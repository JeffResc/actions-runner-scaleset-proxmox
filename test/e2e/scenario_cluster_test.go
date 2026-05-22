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
