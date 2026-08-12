//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakegithub"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakeproxmox"
)

// findVMSnapshot returns the fake's view of one VM, failing the test
// when the VMID is unknown — recycle scenarios assert the VM SURVIVES
// job completion, so "gone" is always a bug here.
func findVMSnapshot(t testing.TB, proxmox *fakeproxmox.Server, vmid int) fakeproxmox.VMSnapshot {
	t.Helper()
	for _, vm := range proxmox.Snapshot() {
		if vm.VMID == vmid {
			return vm
		}
	}
	t.Fatalf("vm %d not found in fake proxmox (destroyed instead of recycled?)", vmid)
	return fakeproxmox.VMSnapshot{}
}

// rollbackCount returns the fake-side rollback counter for vmid, or -1
// when the VM no longer exists. Used inside Eventually polls where a
// mid-poll destroy must read as "not yet" rather than panic.
func rollbackCount(proxmox *fakeproxmox.Server, vmid int) int {
	for _, vm := range proxmox.Snapshot() {
		if vm.VMID == vmid {
			return vm.Rollbacks
		}
	}
	return -1
}

// TestE2E_SnapshotRollbackRecycleLifecycle drives the full
// snapshot-rollback recycle loop (pool.recycle_mode: snapshot_rollback)
// twice through one VM:
//
//	clone → snapshot "scaleset-clean" taken exactly once (pre-boot)
//	job 1 assigned → runs → VM powers itself off
//	  → rollback #1 (not destroy), runner deregistered, row back in Warm
//	same VM promoted Hot again → job 2 assigned (JIT re-injection)
//	  → rollback #2
//
// The load-bearing assertions: the VMID never leaves the fake Proxmox,
// the snapshot-create counter stays at 1 across both jobs, and each
// completion increments the rollback counter instead of the destroy
// path firing.
func TestE2E_SnapshotRollbackRecycleLifecycle(t *testing.T) {
	t.Parallel()
	proxmox := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
	gh := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{Name: "recycle-set"},
	})
	gh.SetStatistics(fakegithub.Statistics{TotalAssignedJobs: 1})

	// MaxConcurrentRunners=1 pins the whole scenario to a single VM:
	// with recycling counting toward availability, the pool must never
	// clone a replacement, so job 2 can only land on the recycled VM.
	h := Start(t, Options{
		HotSize:              1,
		MaxConcurrentRunners: 1,
		ScaleSetName:         "recycle-set",
		RecycleMode:          "snapshot_rollback",
		FakeProxmox:          proxmox,
		FakeGitHub:           gh,
	})

	// ---- Job 1 -----------------------------------------------------
	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "assigned")) >= 1
	}, 30*time.Second, 100*time.Millisecond,
		"orchestrator never transitioned a VM to Assigned")

	vmid, vmName := awaitAssignedVM(t, h, "recycle-set")

	// The clean snapshot was taken exactly once, at clone time, before
	// the first boot dirtied the disk.
	snap := findVMSnapshot(t, proxmox, vmid)
	require.Equal(t, 1, snap.SnapshotCreates, "expected exactly one snapshot create at clone time")
	require.Contains(t, snap.Snapshots, "scaleset-clean")

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

	// Drop desired count before completion so the listener doesn't
	// immediately race a second acquire mid-assertion.
	gh.SetStatistics(fakegithub.Statistics{})

	// The in-VM runner powers itself off; the power poller observes
	// "stopped" and — in snapshot mode — queues a rollback, not a
	// destroy.
	require.NoError(t, proxmox.PowerOff(vmid))

	require.Eventually(t, func() bool {
		return rollbackCount(proxmox, vmid) == 1
	}, 30*time.Second, 100*time.Millisecond,
		"VM %d was never rolled back after its first job", vmid)

	// The recycle deregisters the runner exactly like the destroy path.
	require.Eventually(t, func() bool {
		for _, id := range gh.RunnerDeletions() {
			if id == int64(firstJITRunnerID) {
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond,
		"orchestrator did not deregister the JIT-minted runner on recycle")

	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_recycles_total") >= 1
	}, 10*time.Second, 100*time.Millisecond,
		"scaleset_recycles_total never incremented")

	// ---- Job 2 on the SAME VM ---------------------------------------
	gh.SetStatistics(fakegithub.Statistics{TotalAssignedJobs: 1})

	// The recycled VM promotes Warm → Hot and is re-acquired; a fresh
	// JIT config is injected into the same runner name (a new mint ID
	// proves the re-injection path ran rather than a stale acquire).
	var secondRunnerID int
	require.Eventually(t, func() bool {
		id, ok := gh.JITMintIDForRunner(vmName)
		if !ok || id == firstJITRunnerID {
			return false
		}
		secondRunnerID = id
		return true
	}, 60*time.Second, 100*time.Millisecond,
		"no second JIT mint observed for recycled runner %q", vmName)

	gh.SetRunner(fakegithub.Runner{
		ID:     int64(secondRunnerID),
		Name:   vmName,
		Status: "online",
		Busy:   true,
	})
	require.NoError(t, gh.PostJobStarted(vmName, secondRunnerID))
	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "running")) >= 1
	}, 30*time.Second, 100*time.Millisecond,
		"recycled VM never reached Running for its second job")

	gh.SetStatistics(fakegithub.Statistics{})
	require.NoError(t, proxmox.PowerOff(vmid))

	require.Eventually(t, func() bool {
		return rollbackCount(proxmox, vmid) == 2
	}, 30*time.Second, 100*time.Millisecond,
		"VM %d was never rolled back after its second job", vmid)

	// The VM survived both jobs, was snapshotted exactly once, and the
	// second runner registration was cleaned up too.
	snap = findVMSnapshot(t, proxmox, vmid)
	require.Equal(t, 1, snap.SnapshotCreates, "recycles must reuse the clone-time snapshot, never re-snapshot")
	require.Equal(t, 2, snap.Rollbacks)

	require.Eventually(t, func() bool {
		for _, id := range gh.RunnerDeletions() {
			if id == int64(secondRunnerID) {
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond,
		"orchestrator did not deregister the second JIT-minted runner on recycle")
}
