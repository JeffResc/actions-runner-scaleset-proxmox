//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakegithub"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakeproxmox"
)

// TestE2E_ClusterLeaderLossMidConcurrentJobs is the multi-job
// extension of [TestE2E_ClusterTakeoverMidJob] called out in
// issue #249: while the existing single-job test pins the
// adopt-as-Running classification, this scenario exercises the
// same takeover with N concurrent jobs in-flight to surface
// any state-divergence between Adopt's per-VM classification
// loop and the matching pool-side teardown for each row.
//
// Flow:
//  1. 3-replica raft cluster, MaxConcurrentRunners=N.
//  2. Drive N concurrent JobStarted to leader → all reach Running.
//  3. Cancel the leader's ctx (the harness's closest analog to
//     SIGKILL — graceful drain still pins each in-flight row in
//     terminal state, which is what we want to inherit).
//  4. New leader's Adopt classifies all N inherited rows as
//     Running (counter ticks N times).
//  5. JobCompleted each → every VM destroyed, every JIT-minted
//     runner ID deregistered exactly once across the handover.
//
// Pins the load-class bug shape #249 calls out: a per-row
// classification regression that would correctly handle one
// inherited Running VM but silently miss subsequent ones
// (e.g. an iterator bug in classifyAdoption, or a counter race
// on a shared seq under concurrent Adopt classification).
func TestE2E_ClusterLeaderLossMidConcurrentJobs(t *testing.T) {
	const jobs = 3

	proxmox := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
	gh := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{Name: "cluster-loss-set"},
	})
	gh.SetStatistics(fakegithub.Statistics{TotalAssignedJobs: jobs})

	const replicas = 3
	adminAddrs := PickAdminAddrs(t, replicas)
	rc := NewRaftCluster(t, adminAddrs)
	harnesses := make([]*Harness, replicas)
	for i := 0; i < replicas; i++ {
		harnesses[i] = Start(t, Options{
			HotSize:              jobs,
			MaxConcurrentRunners: jobs,
			ScaleSetName:         "cluster-loss-set",
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
	}, 30*time.Second, 200*time.Millisecond, "no initial leader elected")
	leader := harnesses[leaderIdx]

	// Wait for the leader to reach `jobs` Assigned VMs (HotSize
	// + Acquire transitioning them all under the listener's
	// desired-count signal). awaitNAssignedVMs filters by JIT
	// mint so refill-Hot clones don't perturb the count.
	vms := awaitNAssignedVMs(t, leader, "cluster-loss-set", jobs)

	// Resolve each VM's JIT-minted runner ID, register that
	// runner with the fake as online + busy (so Adopt's "runner
	// present and busy" classification fires on the new
	// leader), and post JobStarted for it. The orchestrator's
	// scaler then transitions Assigned → Running.
	runnerIDs := make([]int64, jobs)
	for i, vm := range vms {
		var minted int
		require.Eventuallyf(t, func() bool {
			id, ok := gh.JITMintIDForRunner(vm.name)
			if ok {
				minted = id
			}
			return ok
		}, 30*time.Second, 100*time.Millisecond,
			"no JIT mint observed for runner %q", vm.name)
		runnerIDs[i] = int64(minted)
		gh.SetRunner(fakegithub.Runner{
			ID:     runnerIDs[i],
			Name:   vm.name,
			Status: "online",
			Busy:   true,
		})
	}
	for i, vm := range vms {
		require.NoError(t, gh.PostJobStarted(vm.name, int(runnerIDs[i])))
	}

	require.Eventually(t, func() bool {
		return leader.MetricValue(t, "scaleset_pool_size", formatLabel("state", "running")) >= jobs
	}, 60*time.Second, 200*time.Millisecond,
		"leader never transitioned %d VMs to Running before takeover", jobs)

	// Kill the leader. The other 2 voters retain quorum (2 of 2
	// surviving), so one of them must take over.
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

	// The load-bearing assertion: every inherited Running VM
	// must be classified as Running by the new leader's Adopt.
	// A regression in the per-row classification loop (iterator
	// bug, counter race) would adopt fewer than `jobs` rows or
	// misclassify some as Hot — both would fail this check.
	require.Eventually(t, func() bool {
		return newLeader.MetricValue(t, "scaleset_vms_total",
			formatLabel("outcome", "adopted_running")) >= jobs
	}, 30*time.Second, 200*time.Millisecond,
		"new leader's adopted_running counter never reached %d — "+
			"per-row classification regression?", jobs)

	// Sanity: every original VMID is still present in Proxmox
	// after takeover. Under a pre-Adopt destroy-everything Recover
	// this assertion would have failed.
	postTakeoverVMIDs := orchestratorVMIDs(proxmox.Snapshot())
	for _, vm := range vms {
		require.Contains(t, postTakeoverVMIDs, vm.vmid,
			"inherited VM %d was destroyed during takeover instead of adopted", vm.vmid)
	}

	// Complete each job against the new leader's session.
	gh.SetStatistics(fakegithub.Statistics{})
	for i, vm := range vms {
		gh.SetRunner(fakegithub.Runner{
			ID:     runnerIDs[i],
			Name:   vm.name,
			Status: "online",
			Busy:   false,
		})
		require.NoError(t, gh.PostJobCompleted(vm.name, int(runnerIDs[i])))
	}

	// Every original VM must be destroyed and every test-issued
	// runner ID deregistered. This is the consistency-across-
	// followers assertion: counters of "completed" vs "deregistered"
	// must match across the takeover boundary.
	require.Eventually(t, func() bool {
		live := make(map[int]struct{})
		for _, vm := range proxmox.Snapshot() {
			live[vm.VMID] = struct{}{}
		}
		for _, vm := range vms {
			if _, alive := live[vm.vmid]; alive {
				return false
			}
		}
		return true
	}, 60*time.Second, 200*time.Millisecond,
		"not all %d inherited VMs were destroyed after JobCompleted under the new leader", jobs)

	require.Eventually(t, func() bool {
		deleted := make(map[int64]struct{})
		for _, id := range gh.RunnerDeletions() {
			deleted[id] = struct{}{}
		}
		for _, want := range runnerIDs {
			if _, ok := deleted[want]; !ok {
				return false
			}
		}
		return true
	}, 15*time.Second, 200*time.Millisecond,
		"not all %d runner IDs were deregistered after takeover + completion "+
			"(saw %v, wanted %v)", jobs, gh.RunnerDeletions(), runnerIDs)
}
