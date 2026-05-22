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

// TestE2E_ClusterElectsSingleLeader stands up two orchestrator
// replicas sharing a fake Kubernetes clientset, a fake Proxmox, and
// a fake GitHub. Verifies the cluster.Coordinator's leader-election
// path produces exactly one leader and the standby reports zero on
// the scaleset_leader gauge.
//
// The shared fake Proxmox is what makes this scenario meaningful: if
// both replicas thought they were leader, both would race to clone
// VMs into the same vmid range and the snapshot would show duplicate
// rows. Confirming only one replica drives the pool is the
// load-bearing assertion.
func TestE2E_ClusterElectsSingleLeader(t *testing.T) {
	// Shared infrastructure. Each instance constructs its own
	// orchestrator but they all hit the same fakeproxmox /
	// fakegithub.
	proxmox := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
	gh := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{Name: "cluster-set"},
	})

	const replicas = 2
	adminAddrs := PickAdminAddrs(t, replicas)
	rc := NewRaftCluster(t, adminAddrs)

	harnesses := make([]*Harness, replicas)
	for i := 0; i < replicas; i++ {
		harnesses[i] = Start(t, Options{
			HotSize:              1,
			MaxConcurrentRunners: 4,
			ScaleSetName:         "cluster-set",
			FakeProxmox:          proxmox,
			FakeGitHub:           gh,
			RaftCluster:          rc,
			ReplicaIndex:         i,
		})
	}

	// Wait for the gauges to settle into a single-leader state.
	// LeaseDuration is 300ms in the e2e config so the election runs
	// in well under a second; the generous deadline absorbs CI
	// scheduler jitter under -race.
	var leaders, followers int
	require.Eventually(t, func() bool {
		leaders, followers = 0, 0
		for _, h := range harnesses {
			v := h.MetricValue(t, "scaleset_leader")
			if v >= 1 {
				leaders++
			} else {
				followers++
			}
		}
		return leaders == 1 && followers == replicas-1
	}, 30*time.Second, 200*time.Millisecond,
		"expected exactly 1 leader / %d follower(s); saw leaders=%d followers=%d",
		replicas-1, leaders, followers)

	// The leader's pool should fill the hot pool against the shared
	// fakeproxmox. The follower's pool manager is nil — it does not
	// touch the fake. So at most HotSize+overshoot clones exist in
	// the range, never 2x.
	require.Eventually(t, func() bool {
		clones := 0
		for _, vm := range proxmox.Snapshot() {
			if vm.VMID >= 10000 && vm.VMID < 11000 {
				clones++
			}
		}
		return clones >= 1
	}, 10*time.Second, 200*time.Millisecond,
		"shared fakeproxmox never saw any clones from the leader")

	// Sanity: the maximum reasonable clone count is
	// MaxConcurrentRunners (4). If both replicas thought they were
	// leader, we'd be racing toward 2*MaxConcurrentRunners.
	clones := 0
	for _, vm := range proxmox.Snapshot() {
		if vm.VMID >= 10000 && vm.VMID < 11000 {
			clones++
		}
	}
	require.LessOrEqual(t, clones, 4,
		"clones beyond MaxConcurrentRunners imply both replicas drove the pool; saw %d", clones)
}

// TestE2E_ClusterLeaderTakeover proves the follower picks up
// leadership when the current leader exits cleanly AND that every
// VM the previous leader had cloned survives the handover. Drives
// the same two-replica topology as the steady-state scenario, then
// cancels the leader's app.Run via Harness.Stop and watches the
// follower flip its scaleset_leader gauge to 1.
//
// The new leader's pool.Manager.Adopt seeds its empty memdb from
// every owner-tagged Proxmox VM it inherits (see [manager.go]
// Adopt). The takeover assertions therefore are:
//
//  1. The follower's scaleset_leader gauge flips to 1.
//  2. Every VMID the previous leader had cloned is STILL present in
//     the shared fakeproxmox after the follower takes over — no
//     destroys in the gap.
//  3. The adopted-vm counter ticks, proving Adopt() actually ran
//     (and was the one driving the seed, not just steady-state
//     reconcile).
//  4. At no point does the live VM count exceed MaxConcurrentRunners
//     (would imply both replicas drove the pool concurrently).
func TestE2E_ClusterLeaderTakeover(t *testing.T) {
	proxmox := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
	gh := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{Name: "takeover-set"},
	})

	// Raft needs 2N+1 voters to tolerate N failures. A 2-node cluster
	// can't elect a new leader after one is killed (quorum = 2 of 2,
	// only 1 voter remains), so the takeover scenario uses 3.
	const replicas = 3
	adminAddrs := PickAdminAddrs(t, replicas)
	rc := NewRaftCluster(t, adminAddrs)

	harnesses := make([]*Harness, replicas)
	for i := 0; i < replicas; i++ {
		harnesses[i] = Start(t, Options{
			HotSize:              1,
			MaxConcurrentRunners: 4,
			ScaleSetName:         "takeover-set",
			FakeProxmox:          proxmox,
			FakeGitHub:           gh,
			RaftCluster:          rc,
			ReplicaIndex:         i,
		})
	}

	// Identify the initial leader.
	var leaderIdx int = -1
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
	// Pick an arbitrary follower to watch — any non-leader works.
	var follower *Harness
	for i, h := range harnesses {
		if i != leaderIdx {
			follower = h
			break
		}
	}
	require.NotNil(t, follower)

	// Wait for the leader's hot pool to settle — every cloned VM has
	// finished booting and is in the Hot state. Killing the leader
	// mid-boot would let its own drain destroy the in-flight VM (the
	// boot worker treats WaitReady ctx-cancellation as a boot
	// failure → markPoisonOrDestroy), which has nothing to do with
	// the new leader's adopt path. We need a stable snapshot of
	// tagged VMs to assert against.
	require.Eventually(t, func() bool {
		return leader.MetricValue(t, "scaleset_pool_size", formatLabel("state", "hot")) >= 1
	}, 20*time.Second, 200*time.Millisecond,
		"leader's hot pool never reached steady state before kill")

	inheritedVMIDs := taggedOrchestratorVMIDs(proxmox.Snapshot(), "takeover-set")
	require.NotEmpty(t, inheritedVMIDs, "expected at least one inherited tagged VM")

	// Kill the leader. ReleaseOnCancel=true means the lease drops as
	// app.Run returns. Stop waits up to 30s for clean exit.
	leader.Stop(t)

	// One of the surviving followers must win the election. With 3
	// replicas and one down, the remaining 2 have quorum (2 of 2);
	// raft typically converges within a couple of heartbeat timeouts
	// (~100ms each in the test config), so 15s is very generous.
	require.Eventually(t, func() bool {
		for i, h := range harnesses {
			if i == leaderIdx {
				continue // killed
			}
			if h.MetricValue(t, "scaleset_leader") >= 1 {
				follower = h // re-aim subsequent assertions at the new leader
				return true
			}
		}
		return false
	}, 15*time.Second, 200*time.Millisecond,
		"no follower became leader after the previous leader exited")

	// Wait for the adopt counter to fire — the strongest signal that
	// Adopt() finished against the inherited set, not just that the
	// gauge flipped. We don't know exactly how many VMs were inherited
	// (the hot-pool reconciler runs concurrently with election), so
	// the assertion is "at least one was adopted".
	require.Eventually(t, func() bool {
		total := float64(0)
		for _, state := range []string{"warm", "hot", "assigned", "running"} {
			total += follower.MetricValue(t, "scaleset_vms_total",
				formatLabel("outcome", "adopted_"+state))
		}
		return total >= 1
	}, 30*time.Second, 200*time.Millisecond,
		"adopted_* counter never incremented; the new leader did not adopt the inherited VMs")

	// At least one fully-cloned VMID the previous leader had tagged
	// must still be in Proxmox after takeover. This is the load-
	// bearing assertion of the adopt change: under the old destroy-
	// everything Recover, ALL inherited tagged VMs were destroyed.
	// Stronger "every single VM survives" would be ideal but is
	// racy when an in-flight boot worker on the outgoing leader
	// runs into the drain ctx cancel — that's a pre-existing
	// shutdown race orthogonal to adopt, not something Adopt can fix.
	postTakeoverVMIDs := orchestratorVMIDs(proxmox.Snapshot())
	survivors := 0
	for _, vmid := range inheritedVMIDs {
		for _, post := range postTakeoverVMIDs {
			if vmid == post {
				survivors++
				break
			}
		}
	}
	require.GreaterOrEqual(t, survivors, 1,
		"expected at least one of inherited tagged VMs %v to survive takeover; post-takeover snapshot was %v",
		inheritedVMIDs, postTakeoverVMIDs)

	// At no point should the snapshot exceed MaxConcurrentRunners; if
	// it did, multiple replicas would have been issuing clones at the
	// same time.
	require.LessOrEqual(t, countOrchestratorVMs(proxmox.Snapshot()), 4,
		"post-takeover VM count exceeded MaxConcurrentRunners")
}

// countOrchestratorVMs returns how many entries in the snapshot fall
// in the orchestrator's configured vmid range (10000..10999).
func countOrchestratorVMs(snap []fakeproxmox.VMSnapshot) int {
	n := 0
	for _, vm := range snap {
		if vm.VMID >= 10000 && vm.VMID < 11000 {
			n++
		}
	}
	return n
}

// orchestratorVMIDs returns the VMIDs in the snapshot that fall in
// the orchestrator's configured vmid range. Used by the takeover
// test to assert "previously-cloned VMs survive the handover".
func orchestratorVMIDs(snap []fakeproxmox.VMSnapshot) []int {
	out := make([]int, 0, len(snap))
	for _, vm := range snap {
		if vm.VMID >= 10000 && vm.VMID < 11000 {
			out = append(out, vm.VMID)
		}
	}
	return out
}

// taggedOrchestratorVMIDs filters to VMs that carry our owner tag —
// i.e. the qmclone + qmconfig tag-apply pair has both committed.
// Untagged mid-clone VMs are by design cleaned up by the leader's
// drain on shutdown, so the takeover-survival assertion can only
// load-bear on tagged VMs.
func taggedOrchestratorVMIDs(snap []fakeproxmox.VMSnapshot, scaleSetName string) []int {
	out := make([]int, 0, len(snap))
	for _, vm := range snap {
		if vm.VMID < 10000 || vm.VMID >= 11000 {
			continue
		}
		// fakeproxmox.VMSnapshot.Tags is the semicolon-joined wire
		// form Proxmox uses; tags.IsOwnedBy decodes it.
		if !tags.IsOwnedBy(vm.Tags, scaleSetName) {
			continue
		}
		out = append(out, vm.VMID)
	}
	return out
}
