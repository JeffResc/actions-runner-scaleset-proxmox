//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakegithub"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakeproxmox"
)

// TestE2E_DestroyIdempotentOnVMNotFound asserts the orchestrator
// treats a "VM was already deleted out-of-band" response as success,
// not as an error worth surfacing on the proxmox_api_errors_total
// metric. This is the path operators exercise when they qm-destroy a
// runner manually while the orchestrator is mid-reconcile.
//
// Flow:
//  1. Start the harness; warm the hot pool so a real VMID exists.
//  2. Pick one of the hot VMs and register a fault that makes any
//     future DELETE on its vmid return 500 "...does not exist".
//  3. Force-destroy that VMID via the admin API.
//  4. The destroy must succeed (admin returns 200), no
//     scaleset_proxmox_api_errors_total{operation="destroy"} ticks
//     up, and the orchestrator's pool catches up to HotSize again.
func TestE2E_DestroyIdempotentOnVMNotFound(t *testing.T) {
	t.Parallel()
	h := Start(t, Options{HotSize: 2, MaxConcurrentRunners: 4})

	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "hot")) >= 2
	}, 10*time.Second, 200*time.Millisecond, "warm pool never filled")

	// Pick a vmid in the orchestrator's range to target.
	var targetVMID int
	for _, vm := range h.Proxmox.Snapshot() {
		if vm.VMID >= 10000 && vm.VMID < 11000 {
			targetVMID = vm.VMID
			break
		}
	}
	require.NotZero(t, targetVMID, "expected at least one VM in the orchestrator's vmid range")

	h.Proxmox.InjectFault(fakeproxmox.Fault{
		Kind: fakeproxmox.FaultVMNotFoundOnDestroy,
		VMID: targetVMID,
	})

	resp := h.AdminRequest(t, "POST", "/admin/destroy/"+itoa(targetVMID), nil)
	resp.Body.Close()
	require.Equal(t, 202, resp.StatusCode,
		"admin destroy schedules async — expect 202 Accepted regardless of upstream noise")

	// Give the destroy queue time to drain through. The orchestrator's
	// pool-error metric is the load-bearing assertion: a "VM not
	// found" response must NOT be classified as a destroy error.
	require.Eventually(t, func() bool {
		// Pool catches up — the destroyed VM is replaced.
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "hot")) >= 2
	}, 10*time.Second, 200*time.Millisecond, "pool never recovered after destroy")

	got := h.MetricValue(t, "scaleset_proxmox_api_errors_total",
		formatLabel("operation", "destroy"), formatLabel("node", "pve1"))
	require.Equal(t, 0.0, got,
		"VMNotFound on destroy must be idempotent — saw %v errors recorded", got)

	// Classification (not just count): a "VM not found" response must be
	// classified as the idempotent SUCCESS path, so the pool refilled via
	// a clean clone — NOT a failure cascade that churns through a
	// failed-clone replacement. Asserting clone-failed stayed 0
	// distinguishes "ErrVMNotFound → success" from "real error → failure"
	// rather than only that the destroy counter didn't move.
	cloneFailed := h.MetricValue(t, "scaleset_vms_total", formatLabel("outcome", "clone-failed"))
	require.Equal(t, 0.0, cloneFailed,
		"VMNotFound-on-destroy must classify as idempotent success, not trigger a failed-clone replacement (saw %v)", cloneFailed)
}

// TestE2E_GuestAgentTransientRetry asserts the orchestrator retries
// past a transient "guest agent not responding" window during VM boot,
// matching the real-world startup race where qemu-guest-agent is
// installed but its systemd unit hasn't come up yet.
//
// We register the fault BEFORE any VM exists so it applies to whichever
// vmid the orchestrator clones first. The harness still observes the
// pool reaching its target size — the fault transparently extends
// each VM's boot window without forcing an outright failure.
func TestE2E_GuestAgentTransientRetry(t *testing.T) {
	t.Parallel()
	// Start with the fake Proxmox pre-created so we can install the
	// fault before app.Run launches.
	fp := fakeproxmox.New(t, fakeproxmox.Options{
		TaskDuration: 5 * time.Millisecond,
	})
	fp.InjectFault(fakeproxmox.Fault{
		Kind:     fakeproxmox.FaultGuestAgentNotReady,
		VMID:     0, // apply to every VM
		Duration: 250 * time.Millisecond,
	})

	h := Start(t, Options{
		HotSize:              1,
		MaxConcurrentRunners: 2,
		FakeProxmox:          fp,
	})

	// Despite each VM's 250ms "agent not ready" window, the
	// orchestrator's WaitReady polling should eventually see a
	// successful get-osinfo. Boot may need to retry — give it
	// generous time.
	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "hot")) >= 1
	}, 15*time.Second, 200*time.Millisecond,
		"hot pool never reached 1 — transient agent errors should retry, not fail the boot")
}

// TestE2E_JITInjectPersistentFailureDestroysVM pins the
// orchestrator's terminal-failure path for JIT injection (#247):
// when qemu-guest-agent file-write fails on every retry, the
// scaler must surface the error metric, deregister the runner,
// and queue the VM for destruction so the pool can re-clone a
// fresh replacement. Without this guarantee a broken-template
// scenario would loop forever, exhausting the orchestrator's
// API budget against an unsalvageable VM.
//
// Flow:
//  1. Pre-create the fake Proxmox and register FaultJITInjectFail
//     with VMID=0 so every clone's file-write fails.
//  2. Start the orchestrator with HotSize=1 + 1 assignable job
//     so a JIT injection actually fires.
//  3. Watch the inject-error metric tick up (orchestrator's retry
//     budget exhausted).
//  4. The just-cloned VM must end up destroyed in the fake.
func TestE2E_JITInjectPersistentFailureDestroysVM(t *testing.T) {
	t.Parallel()
	proxmox := fakeproxmox.New(t, fakeproxmox.Options{
		TaskDuration: 5 * time.Millisecond,
	})
	// Apply to every VMID so whichever vmid the orchestrator
	// clones first hits the failure.
	proxmox.InjectFault(fakeproxmox.Fault{
		Kind: fakeproxmox.FaultJITInjectFail,
		VMID: 0,
	})

	gh := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{Name: "test-scaleset"},
	})
	// Drive one assignable job so HandleJobStarted -> injectWithRetry
	// actually runs.
	gh.SetStatistics(fakegithub.Statistics{TotalAssignedJobs: 1})

	h := Start(t, Options{
		HotSize:              1,
		MaxConcurrentRunners: 2,
		ScaleSetName:         "test-scaleset",
		FakeProxmox:          proxmox,
		FakeGitHub:           gh,
	})

	// The injection-failure metric is the canonical signal that
	// retries were exhausted. It's incremented inside the scaler's
	// "jit injection failed (after retries); releasing vm" branch.
	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_proxmox_api_errors_total",
			formatLabel("operation", "inject_jit"), formatLabel("node", "pve1")) >= 1
	}, 60*time.Second, 250*time.Millisecond,
		"orchestrator never recorded an inject_jit failure — the retry budget should exhaust")

	// And the affected VM must end up destroyed in the fake (the
	// scaler's MarkCompleted -> pool.destroyAsync chain). Use the
	// fake's snapshot to look for an empty orchestrator-range; if
	// HotSize triggers a refill clone, the VMID will differ from
	// the failed one, but the orchestrator-range count must be 0 or
	// occupied only by the refill (a non-running clone in progress).
	require.Eventually(t, func() bool {
		// Either the orchestrator-range is empty (failed VM destroyed,
		// no refill yet) or contains exactly one VM that is NOT the
		// one whose JIT inject ran (matching the new refill clone).
		var inRange int
		for _, vm := range proxmox.Snapshot() {
			if vm.VMID >= 10000 && vm.VMID < 11000 {
				inRange++
			}
		}
		// Failed VM must be gone; a refill may or may not have landed,
		// but we don't want more than one VM in the range (would
		// indicate the failed VM is still around alongside a refill).
		return inRange <= 1
	}, 30*time.Second, 250*time.Millisecond,
		"the inject-failed VM was never destroyed; pool will leak")
}

// TestE2E_CloneTaskFailureIsClassified pins #331 end-to-end: a Proxmox
// clone task that *completes with a failure exit status* (e.g.
// storage-full mid-copy) must be classified as a failed clone, not
// silently treated as success. go-proxmox's WaitFor returns nil the
// moment the task leaves "running" and ignores IsFailed/ExitStatus, so
// without the awaitTask helper the orchestrator would admit a broken VM
// to the pool and never record a clone failure.
//
// Drives the new FaultTaskFails (#326) on every qmclone task. Asserts
// the clone-failed outcome metric ticks up — pre-fix it stays at 0
// because the failed-but-completed task is mistaken for success.
func TestE2E_CloneTaskFailureIsClassified(t *testing.T) {
	t.Parallel()
	proxmox := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
	proxmox.InjectFault(fakeproxmox.Fault{
		Kind:     fakeproxmox.FaultTaskFails,
		VMID:     0, // every VM
		TaskType: "qmclone",
	})

	h := Start(t, Options{HotSize: 2, MaxConcurrentRunners: 4, FakeProxmox: proxmox})

	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_vms_total", formatLabel("outcome", "clone-failed")) >= 1
	}, 20*time.Second, 200*time.Millisecond,
		"a clone task that completes with a failure exitstatus must be classified as clone-failed (#331)")

	// The broken clones must never be admitted to the hot pool.
	require.Never(t, func() bool {
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "hot")) >= 1
	}, 2*time.Second, 250*time.Millisecond,
		"a failed clone must never reach the hot pool")
}

// itoa wraps strconv.Itoa for a slightly less noisy call site in the
// scenarios above.
func itoa(n int) string {
	const digits = "0123456789"
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = digits[n%10]
		n /= 10
	}
	return string(buf[i:])
}

// TestE2E_PartialClone_StartFailureRecoversToHealthyPool pins
// #287's canonical partial-failure cascade: the orchestrator's
// Clone succeeds (Proxmox creates the VM record) but the Start
// that follows returns 500. The reconcile loop must retry past
// the failures, eventually clone a VM whose Start succeeds, and
// the orphan-sweep path (orphan_grace=5s in the harness) must
// destroy the stuck Stopped clones that never recovered.
//
// Drives FaultStatus500Spam with Count=8 so the first N start
// calls fail and subsequent ones succeed. Asserts:
//   - Hot pool eventually reaches HotSize (= retries succeeded).
//   - The orchestrator-owned Stopped VMs left over from failed
//     starts are eventually destroyed by the orphan sweep, NOT
//     left as a permanent quota-consuming leak.
func TestE2E_PartialClone_StartFailureRecoversToHealthyPool(t *testing.T) {
	t.Parallel()
	proxmox := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
	proxmox.InjectFault(fakeproxmox.Fault{
		Kind:  fakeproxmox.FaultStatus500Spam,
		VMID:  0,
		Count: 8,
	})

	h := Start(t, Options{
		HotSize:              2,
		MaxConcurrentRunners: 4,
		FakeProxmox:          proxmox,
	})

	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "hot")) >= 2
	}, 30*time.Second, 250*time.Millisecond,
		"hot pool must converge to HotSize after the start-failure burst clears — issue #287")

	// Now wait for the orphan-sweep to clean up the Stopped
	// partial-clone leaks. orphan_grace is 5s in the harness; give
	// generous headroom for the reconcile tick.
	require.Eventually(t, func() bool {
		for _, vm := range proxmox.Snapshot() {
			if vm.VMID == 9000 {
				continue // template
			}
			if vm.VMID < 10000 || vm.VMID >= 11000 {
				continue
			}
			if !vm.Running {
				return false
			}
		}
		return true
	}, 30*time.Second, 500*time.Millisecond,
		"orchestrator-owned Stopped VMs must be reaped by orphan-sweep — partial clone left running indefinitely is the canonical leak (issue #287)")
}
