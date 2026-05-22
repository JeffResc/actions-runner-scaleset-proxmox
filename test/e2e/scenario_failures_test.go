//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
