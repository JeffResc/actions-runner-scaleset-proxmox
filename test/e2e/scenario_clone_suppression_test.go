//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakegithub"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakeproxmox"
)

// TestE2E_RecycleSuppressesReplacementClone pins the production fix for
// the wasted-replacement-clone problem in snapshot-rollback mode:
//
//	hot_size=1, max_concurrent_runners=2 (headroom for a replacement
//	clone EXISTS — only the returning-VM credit can stop it)
//	job assigned to the single hot VM → Running
//	  → reconcile must NOT dispatch a replacement clone while the job
//	    runs: the VM rolls back into the pool seconds after the job,
//	    so the fake's owner-tagged VM population must stay at exactly 1
//	  → scaleset_clone_suppressed_total must show the suppression
//	job completes (VM powers off) → rollback → the SAME VM re-enters
//	the pool; population still 1
//
// Before the credit, every job on a hot VM triggered an eager
// replacement full-clone (minutes of storage I/O) that was pure waste
// — the job's VM returned ~15s later and the excess piled up as warm
// capacity until vm_max_age.
func TestE2E_RecycleSuppressesReplacementClone(t *testing.T) {
	t.Parallel()
	proxmox := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
	gh := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{Name: "suppress-set"},
	})
	gh.SetStatistics(fakegithub.Statistics{TotalAssignedJobs: 1})

	h := Start(t, Options{
		HotSize:              1,
		MaxConcurrentRunners: 2, // replacement WOULD fit under the cap
		ScaleSetName:         "suppress-set",
		RecycleMode:          "snapshot_rollback",
		FakeProxmox:          proxmox,
		FakeGitHub:           gh,
	})

	// ---- Job assigned to the single hot VM --------------------------
	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "assigned")) >= 1
	}, 30*time.Second, 100*time.Millisecond,
		"orchestrator never transitioned a VM to Assigned")

	vmid, vmName := awaitAssignedVM(t, h, "suppress-set")

	gh.SetRunner(fakegithub.Runner{
		ID:     firstJITRunnerID,
		Name:   vmName,
		Status: "online",
		Busy:   true,
	})
	require.NoError(t, gh.PostJobStarted(vmName, firstJITRunnerID))
	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "running")) >= 1
	}, 30*time.Second, 100*time.Millisecond,
		"orchestrator never transitioned Assigned → Running on JobStarted")

	// ---- The load-bearing negative: NO replacement clone ------------
	// Hold the assertion across ~20 reconcile ticks (100ms interval in
	// the harness). Any replacement dispatch would surface as a second
	// owner-tagged VM in the fake — the clone is the very first thing
	// runClone does after allocating a VMID.
	require.Never(t, func() bool {
		return len(taggedOrchestratorVMIDs(proxmox.Snapshot(), "suppress-set")) > 1
	}, 2*time.Second, 100*time.Millisecond,
		"a replacement clone was dispatched for a busy VM that will return via rollback")

	// The suppression must be observable, not just inferable.
	require.GreaterOrEqual(t, h.MetricValue(t, "scaleset_clone_suppressed_total"), float64(1),
		"scaleset_clone_suppressed_total never incremented while the credit was active")

	// ---- Job completes → rollback → same VM back in the pool --------
	gh.SetStatistics(fakegithub.Statistics{})
	require.NoError(t, proxmox.PowerOff(vmid))

	require.Eventually(t, func() bool {
		return rollbackCount(proxmox, vmid) == 1
	}, 30*time.Second, 100*time.Millisecond,
		"VM %d was never rolled back after its job", vmid)

	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "hot")) >= 1
	}, 30*time.Second, 100*time.Millisecond,
		"recycled VM never returned to the hot pool")

	// Steady state restored with the original VM and zero extra clones.
	ids := taggedOrchestratorVMIDs(proxmox.Snapshot(), "suppress-set")
	require.Equal(t, []int{vmid}, ids,
		"pool must converge back to exactly the recycled VM, no replacements")
}

// TestE2E_RecycleBurstStillClones is the demand-side twin of the
// suppression scenario: a SECOND job arriving while the only VM is
// busy must still get a VM cloned for it. The returning-VM credit
// offsets only the baseline hot/warm top-up; pending demand (GitHub's
// desired count exceeding busy+available) flows through needBurst and
// clones up to max_concurrent_runners exactly as in destroy mode.
func TestE2E_RecycleBurstStillClones(t *testing.T) {
	t.Parallel()
	proxmox := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
	gh := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{Name: "burst-recycle-set"},
	})
	gh.SetStatistics(fakegithub.Statistics{TotalAssignedJobs: 1})

	h := Start(t, Options{
		HotSize:              1,
		MaxConcurrentRunners: 2,
		ScaleSetName:         "burst-recycle-set",
		RecycleMode:          "snapshot_rollback",
		FakeProxmox:          proxmox,
		FakeGitHub:           gh,
	})

	// Job 1 occupies the only VM.
	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "assigned")) >= 1
	}, 30*time.Second, 100*time.Millisecond,
		"orchestrator never transitioned a VM to Assigned")

	_, vmName := awaitAssignedVM(t, h, "burst-recycle-set")

	gh.SetRunner(fakegithub.Runner{
		ID:     firstJITRunnerID,
		Name:   vmName,
		Status: "online",
		Busy:   true,
	})
	// Job 2 arrives while job 1 occupies the only VM. The fake only
	// delivers statistics inside a message envelope (or on session
	// create/refresh), so raise the assigned count BEFORE posting
	// JobStarted — the listener then processes desired=2 while the VM
	// is busy: desired=2, busy=1, available=0 → a clone MUST be
	// dispatched despite the busy VM being credited as returning.
	// Real pending demand always wins over the suppression credit.
	gh.SetStatistics(fakegithub.Statistics{TotalAssignedJobs: 2})
	require.NoError(t, gh.PostJobStarted(vmName, firstJITRunnerID))
	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "running")) >= 1
	}, 30*time.Second, 100*time.Millisecond,
		"orchestrator never transitioned Assigned → Running on JobStarted")

	require.Eventually(t, func() bool {
		return len(taggedOrchestratorVMIDs(proxmox.Snapshot(), "burst-recycle-set")) >= 2
	}, 30*time.Second, 100*time.Millisecond,
		"no clone was dispatched for the second concurrent job — the credit dulled burst response")
}
