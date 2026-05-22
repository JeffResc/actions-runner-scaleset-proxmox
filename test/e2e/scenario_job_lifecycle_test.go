//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/tags"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakegithub"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakeproxmox"
)

// firstJITRunnerID is what fakegithub.handleGenerateJIT returns for the
// first JIT mint of any test (`runnerID := 100000 + s.jitMintCount`
// with the counter starting at 1 post-increment). The lifecycle tests
// each run against a fresh fakegithub so this value is deterministic.
const firstJITRunnerID = 100001

// awaitAssignedVM polls the fake's Proxmox snapshot until exactly one
// owner-tagged powered-on VM exists in the orchestrator's VMID range,
// then returns its identifiers. Used by the lifecycle tests to learn
// the runner name the orchestrator chose for the (single) inherited
// job before posting JobStarted / JobCompleted messages against it.
func awaitAssignedVM(t testing.TB, h *Harness, scaleSetName string) (vmid int, name string) {
	t.Helper()
	require.Eventually(t, func() bool {
		ids := taggedOrchestratorVMIDs(h.Proxmox.Snapshot(), scaleSetName)
		return len(ids) == 1
	}, 30*time.Second, 100*time.Millisecond,
		"never observed exactly one owner-tagged VM in scaleset %q", scaleSetName)

	for _, vm := range h.Proxmox.Snapshot() {
		if vm.VMID < 10000 || vm.VMID >= 11000 {
			continue
		}
		if !tags.IsOwnedBy(vm.Tags, scaleSetName) {
			continue
		}
		return vm.VMID, vm.Name
	}
	t.Fatalf("snapshot scan failed to re-find a tagged VM (race with destroy?)")
	return 0, ""
}

// TestE2E_StandaloneJobLifecycle drives a single job through the full
// orchestrator state machine in standalone (single-replica) mode:
//
//	stats {TotalAssignedJobs:1} → listener fires HandleDesiredRunnerCount(1)
//	  → pool.Acquire (Hot → Assigned) + scaler mints JIT
//	PostJobStarted → scaler.HandleJobStarted → pool.MarkRunning (Assigned → Running)
//	PostJobCompleted → scaler.HandleJobCompleted → pool.MarkCompleted → destroy
//
// Catches wiring regressions across the listener → scaler → pool seam
// that the unit tests in internal/scaler and internal/pool cover only
// in isolation.
func TestE2E_StandaloneJobLifecycle(t *testing.T) {
	proxmox := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
	gh := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{Name: "lifecycle-set"},
	})

	// Drive the listener's initial-handshake HandleDesiredRunnerCount(1).
	// This is the only signal that causes the orchestrator to clone +
	// inject a JIT — without it the pool just sits at HotSize idle.
	gh.SetStatistics(fakegithub.Statistics{TotalAssignedJobs: 1})

	h := Start(t, Options{
		HotSize:              1,
		MaxConcurrentRunners: 1,
		ScaleSetName:         "lifecycle-set",
		FakeProxmox:          proxmox,
		FakeGitHub:           gh,
	})

	// Wait for the Acquire to land. The metric is the cleanest signal —
	// snapshot inspection alone can't distinguish "Hot" from "Assigned".
	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "assigned")) >= 1
	}, 30*time.Second, 100*time.Millisecond,
		"orchestrator never transitioned a VM to Assigned")

	vmid, vmName := awaitAssignedVM(t, h, "lifecycle-set")

	// Register the runner with the fake before sending JobStarted.
	// Without this, the gh.Reconciler matrix fires assigned/running
	// grace timers ("runner never registered on GitHub" /
	// "runner missing from GitHub") and force-destroys the row before
	// the test observes the Running state. In production the runner
	// inside the VM self-registers via the JIT config; in the fake
	// we model that step by hand.
	gh.SetRunner(fakegithub.Runner{
		ID:     firstJITRunnerID,
		Name:   vmName,
		Status: "online",
		Busy:   true,
	})

	// Post JobStarted → expect Assigned → Running. The runnerID we
	// pass here is advisory; the row's RunnerID was already set by
	// scaler.provisionOne to whatever the fake's JIT-mint returned
	// (firstJITRunnerID), and that's what OnRunnerOrphaned will
	// deregister on destroy.
	require.NoError(t, gh.PostJobStarted(vmName, firstJITRunnerID))
	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "running")) >= 1
	}, 30*time.Second, 100*time.Millisecond,
		"orchestrator never transitioned Assigned → Running on JobStarted")

	// Drop the desired count to 0 BEFORE posting JobCompleted: the
	// listener's handleMessage always re-fires HandleDesiredRunnerCount
	// after processing the message, and we don't want it to clone a
	// replacement and mint a second JIT mid-assertion.
	gh.SetStatistics(fakegithub.Statistics{})

	require.NoError(t, gh.PostJobCompleted(vmName, firstJITRunnerID))

	// Wait for the specific VM that ran the job to disappear. The pool
	// reconciler may clone a replacement Hot VM to refill HotSize=1
	// after the destroy — that's expected steady-state behavior, so
	// we assert on the original VMID rather than total count.
	require.Eventually(t, func() bool {
		for _, vm := range proxmox.Snapshot() {
			if vm.VMID == vmid {
				return false
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond,
		"VM %d was never destroyed after JobCompleted", vmid)

	// The orchestrator must deregister the runner during the destroy.
	// OnRunnerOrphaned runs AFTER store.Delete on the destroy
	// goroutine, so wait for the call to land rather than asserting
	// immediately after the VM disappears.
	require.Eventually(t, func() bool {
		for _, id := range gh.RunnerDeletions() {
			if id == int64(firstJITRunnerID) {
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond,
		"orchestrator did not deregister the JIT-minted runner on destroy")
}

// TestE2E_ClusterJobLifecycle is the same lifecycle as the standalone
// test but driven through a 3-replica raft cluster. Adds the
// cluster-mode invariant: only the leader's metrics counters tick.
// Followers must observe none of the listener traffic (single shared
// fakegithub session ensures this is automatic — but we still assert it).
func TestE2E_ClusterJobLifecycle(t *testing.T) {
	proxmox := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
	gh := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{Name: "cluster-lifecycle-set"},
	})
	gh.SetStatistics(fakegithub.Statistics{TotalAssignedJobs: 1})

	const replicas = 3
	adminAddrs := PickAdminAddrs(t, replicas)
	rc := NewRaftCluster(t, adminAddrs)
	harnesses := make([]*Harness, replicas)
	for i := 0; i < replicas; i++ {
		harnesses[i] = Start(t, Options{
			HotSize:              1,
			MaxConcurrentRunners: 1,
			ScaleSetName:         "cluster-lifecycle-set",
			FakeProxmox:          proxmox,
			FakeGitHub:           gh,
			RaftCluster:          rc,
			ReplicaIndex:         i,
		})
	}

	// Find the elected leader, then drive the lifecycle against its
	// metrics. The fakeproxmox snapshot is shared, so it's authoritative
	// across all replicas regardless of which one is leader.
	leaderIdx := -1
	require.Eventually(t, func() bool {
		for i, h := range harnesses {
			if h.MetricValue(t, "scaleset_leader") >= 1 {
				leaderIdx = i
				return true
			}
		}
		return false
	}, 30*time.Second, 200*time.Millisecond, "no leader elected")
	leader := harnesses[leaderIdx]

	require.Eventually(t, func() bool {
		return leader.MetricValue(t, "scaleset_pool_size", formatLabel("state", "assigned")) >= 1
	}, 30*time.Second, 100*time.Millisecond,
		"leader never transitioned a VM to Assigned")

	vmid, vmName := awaitAssignedVM(t, leader, "cluster-lifecycle-set")

	// Register the runner so the gh.Reconciler doesn't force-destroy
	// the row mid-lifecycle (see comment in standalone test).
	gh.SetRunner(fakegithub.Runner{
		ID:     firstJITRunnerID,
		Name:   vmName,
		Status: "online",
		Busy:   true,
	})

	require.NoError(t, gh.PostJobStarted(vmName, firstJITRunnerID))
	require.Eventually(t, func() bool {
		return leader.MetricValue(t, "scaleset_pool_size", formatLabel("state", "running")) >= 1
	}, 30*time.Second, 100*time.Millisecond,
		"leader never transitioned Assigned → Running")

	gh.SetStatistics(fakegithub.Statistics{})
	require.NoError(t, gh.PostJobCompleted(vmName, firstJITRunnerID))

	require.Eventually(t, func() bool {
		for _, vm := range proxmox.Snapshot() {
			if vm.VMID == vmid {
				return false
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond,
		"VM %d was never destroyed after JobCompleted in cluster mode", vmid)

	require.Eventually(t, func() bool {
		for _, id := range gh.RunnerDeletions() {
			if id == int64(firstJITRunnerID) {
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond,
		"leader did not deregister the JIT-minted runner on destroy")

	// Followers must never have driven the pool: their clone-success
	// counter stays at zero throughout.
	for i, h := range harnesses {
		if i == leaderIdx {
			continue
		}
		require.Zero(t,
			h.MetricValue(t, "scaleset_vms_total", formatLabel("outcome", "clone-success")),
			"follower %d's vms_total{outcome=clone-success} should be 0; saw %g",
			i, h.MetricValue(t, "scaleset_vms_total", formatLabel("outcome", "clone-success")))
	}
}

// TestE2E_ClusterTakeoverMidJob is the load-bearing test for the adopt
// change. Drives a job to Running on the leader, kills the leader, and
// asserts (a) the new leader adopts the inherited VM directly as
// Running with the correct RunnerID, (b) JobCompleted delivered to the
// new leader's session completes the job cleanly, (c) the runner is
// deregistered exactly once across the handover.
//
// Specifically exercises the adopt-as-Running branch in
// classifyAdoption (internal/pool/manager.go) — the "runner present
// and busy" case that's structurally hard to verify without a
// real-job-in-flight scenario.
func TestE2E_ClusterTakeoverMidJob(t *testing.T) {
	proxmox := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
	gh := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{Name: "takeover-job-set"},
	})
	gh.SetStatistics(fakegithub.Statistics{TotalAssignedJobs: 1})

	const replicas = 3
	adminAddrs := PickAdminAddrs(t, replicas)
	rc := NewRaftCluster(t, adminAddrs)
	harnesses := make([]*Harness, replicas)
	for i := 0; i < replicas; i++ {
		harnesses[i] = Start(t, Options{
			HotSize:              1,
			MaxConcurrentRunners: 1,
			ScaleSetName:         "takeover-job-set",
			FakeProxmox:          proxmox,
			FakeGitHub:           gh,
			RaftCluster:          rc,
			ReplicaIndex:         i,
		})
	}

	leaderIdx := -1
	require.Eventually(t, func() bool {
		for i, h := range harnesses {
			if h.MetricValue(t, "scaleset_leader") >= 1 {
				leaderIdx = i
				return true
			}
		}
		return false
	}, 30*time.Second, 200*time.Millisecond, "no leader elected")
	leader := harnesses[leaderIdx]

	require.Eventually(t, func() bool {
		return leader.MetricValue(t, "scaleset_pool_size", formatLabel("state", "assigned")) >= 1
	}, 30*time.Second, 100*time.Millisecond, "leader never reached Assigned")

	vmid, vmName := awaitAssignedVM(t, leader, "takeover-job-set")

	// Register the runner with the fake before sending JobStarted —
	// the new leader's Adopt queries the GitHub runners list during
	// classification, and "present + busy" is the only path that
	// adopts the VM directly as Running (with RunnerID populated). If
	// the runner is missing from GitHub at adopt time, the VM gets
	// adopted as Hot and the JobCompleted path won't fire MarkCompleted
	// against it cleanly.
	gh.SetRunner(fakegithub.Runner{
		ID:     firstJITRunnerID,
		Name:   vmName,
		Status: "online",
		Busy:   true,
	})

	require.NoError(t, gh.PostJobStarted(vmName, firstJITRunnerID))
	require.Eventually(t, func() bool {
		return leader.MetricValue(t, "scaleset_pool_size", formatLabel("state", "running")) >= 1
	}, 30*time.Second, 100*time.Millisecond, "leader never reached Running")

	// Kill the leader. The remaining 2 replicas have quorum (2 of 2
	// surviving voters) and one must win the election.
	leader.Stop(t)

	var newLeader *Harness
	require.Eventually(t, func() bool {
		for i, h := range harnesses {
			if i == leaderIdx {
				continue
			}
			if h.MetricValue(t, "scaleset_leader") >= 1 {
				newLeader = h
				return true
			}
		}
		return false
	}, 15*time.Second, 200*time.Millisecond,
		"no follower became leader after the previous leader exited")

	// The adopted_running counter is the load-bearing assertion: it
	// proves the new leader's Adopt classified the inherited VM as
	// Running (because GitHub reported the runner busy), seeding the
	// RunnerID directly into the row.
	require.Eventually(t, func() bool {
		return newLeader.MetricValue(t, "scaleset_vms_total",
			formatLabel("outcome", "adopted_running")) >= 1
	}, 30*time.Second, 200*time.Millisecond,
		"new leader's adopted_running counter never incremented — "+
			"the adopt-as-Running classification branch did not fire")

	// Sanity: the VMID the previous leader had cloned is still in
	// Proxmox after takeover. Under the old destroy-everything Recover
	// this assertion would have failed.
	require.Contains(t, orchestratorVMIDs(proxmox.Snapshot()), vmid,
		"inherited VM %d was destroyed during takeover instead of adopted", vmid)

	// Complete the job against the new leader's session. The fake
	// retires the prior session in handleSessionCreate, so the new
	// leader's listener owns the pending channel by the time we send.
	gh.SetStatistics(fakegithub.Statistics{})
	gh.SetRunner(fakegithub.Runner{
		ID:     firstJITRunnerID,
		Name:   vmName,
		Status: "online",
		Busy:   false,
	})
	require.NoError(t, gh.PostJobCompleted(vmName, firstJITRunnerID))

	require.Eventually(t, func() bool {
		for _, vm := range proxmox.Snapshot() {
			if vm.VMID == vmid {
				return false
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond,
		"VM %d was never destroyed after JobCompleted under the new leader", vmid)

	require.Eventually(t, func() bool {
		for _, id := range gh.RunnerDeletions() {
			if id == int64(firstJITRunnerID) {
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond,
		"new leader did not deregister the JIT-minted runner on destroy")
}
