//go:build e2e

package e2e

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	kubefake "k8s.io/client-go/kubernetes/fake"

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
	kube := kubefake.NewSimpleClientset()

	const replicas = 2
	harnesses := make([]*Harness, replicas)
	for i := 0; i < replicas; i++ {
		harnesses[i] = Start(t, Options{
			HotSize:              1,
			MaxConcurrentRunners: 4,
			ScaleSetName:         "cluster-set",
			FakeProxmox:          proxmox,
			FakeGitHub:           gh,
			KubeClient:           kube,
			Identity:             "replica-" + strconv.Itoa(i),
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
// leadership when the current leader exits cleanly. Drives the same
// two-replica topology as the steady-state scenario, then cancels the
// leader's app.Run via Harness.Stop and watches the follower flip its
// scaleset_leader gauge to 1 within ~2 × LeaseDuration.
//
// The new leader's pool.Manager.Recover treats any tagged Proxmox VMs
// inherited from the previous leader as orphans and destroys them
// (see [manager.go:464](internal/pool/manager.go#L464) — the
// conservative "leftovers can't be trusted" stance protects against
// pre-existing crash state). The takeover assertion therefore is:
//
//  1. The follower's scaleset_leader gauge flips to 1.
//  2. The shared fakeproxmox eventually reaches HotSize again — the
//     new leader cleaned the inherited VMs and re-cloned.
//  3. At no point does the live VM count exceed MaxConcurrentRunners
//     (would imply both replicas drove the pool concurrently).
func TestE2E_ClusterLeaderTakeover(t *testing.T) {
	proxmox := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
	gh := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{Name: "takeover-set"},
	})
	kube := kubefake.NewSimpleClientset()

	const replicas = 2
	harnesses := make([]*Harness, replicas)
	for i := 0; i < replicas; i++ {
		harnesses[i] = Start(t, Options{
			HotSize:              1,
			MaxConcurrentRunners: 4,
			ScaleSetName:         "takeover-set",
			FakeProxmox:          proxmox,
			FakeGitHub:           gh,
			KubeClient:           kube,
			Identity:             "replica-" + strconv.Itoa(i),
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
	follower := harnesses[1-leaderIdx]

	// Wait for the leader to materialise at least one VM so the
	// takeover scenario has inherited state to work against.
	require.Eventually(t, func() bool {
		return countOrchestratorVMs(proxmox.Snapshot()) >= 1
	}, 10*time.Second, 200*time.Millisecond,
		"leader never created any VMs to take over")

	// Kill the leader. ReleaseOnCancel=true means the lease drops as
	// app.Run returns. Stop waits up to 30s for clean exit.
	leader.Stop(t)

	// The follower must win within ~2 × LeaseDuration (LeaseDuration
	// is 1s in the e2e config). Generous deadline absorbs lease-watch
	// jitter through the fake k8s client.
	require.Eventually(t, func() bool {
		return follower.MetricValue(t, "scaleset_leader") >= 1
	}, 15*time.Second, 200*time.Millisecond,
		"follower never became leader after the previous leader exited")

	// The new leader cleans up inherited VMs and reconverges to
	// HotSize. Pool size reflects that even after the destroy/re-clone
	// cycle.
	require.Eventually(t, func() bool {
		return follower.MetricValue(t, "scaleset_pool_size", formatLabel("state", "hot")) >= 1
	}, 30*time.Second, 200*time.Millisecond,
		"new leader never refilled the hot pool after takeover")

	// Recovery counter ticks once per inherited orphan — proves the
	// new leader's Recover() actually ran (and was the one driving
	// the cleanup, not just background reconcile).
	require.GreaterOrEqual(t,
		follower.MetricValue(t, "scaleset_vms_total", formatLabel("outcome", "recovered_orphan")),
		float64(1),
		"recovered_orphan counter should have incremented during takeover")

	// At no point should the snapshot exceed MaxConcurrentRunners; if
	// it did, both replicas would have been issuing clones at the
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
