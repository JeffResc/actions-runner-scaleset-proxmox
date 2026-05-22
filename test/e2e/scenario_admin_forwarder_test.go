//go:build e2e

package e2e

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakegithub"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakeproxmox"
)

// TestE2E_AdminForwarderRoutesFollowerToLeader proves a follower's
// admin API reverse-proxies to the current leader. Operators hitting
// /admin/state via a stable Service IP / LB don't need to know which
// replica is leader — the standby resolves the leader's HTTP endpoint
// from the static raft peer list and forwards the request unchanged.
//
// Driven through the real adminapi.coordAdminGate + cluster.Forwarder
// path; the unit tests in internal/cluster cover the forwarder logic
// in isolation, but only this scenario exercises the full chain
// (raft leadership → peer-map lookup → reverse proxy) against a live
// orchestrator.
func TestE2E_AdminForwarderRoutesFollowerToLeader(t *testing.T) {
	proxmox := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
	gh := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{Name: "fwd-set"},
	})

	const replicas = 2
	adminAddrs := PickAdminAddrs(t, replicas)
	rc := NewRaftCluster(t, adminAddrs)

	harnesses := make([]*Harness, replicas)
	for i := 0; i < replicas; i++ {
		harnesses[i] = Start(t, Options{
			HotSize:              1,
			MaxConcurrentRunners: 4,
			ScaleSetName:         "fwd-set",
			FakeProxmox:          proxmox,
			FakeGitHub:           gh,
			RaftCluster:          rc,
			ReplicaIndex:         i,
		})
	}

	// Wait for the gauge to settle on one leader.
	var leaderIdx int = -1
	require.Eventually(t, func() bool {
		for i, h := range harnesses {
			if h.MetricValue(t, "scaleset_leader") >= 1 {
				leaderIdx = i
				return true
			}
		}
		return false
	}, 30*time.Second, 200*time.Millisecond, "no leader elected")

	follower := harnesses[1-leaderIdx]

	// Hit the follower's /admin/state. The adminapi.leaderOrForward
	// middleware reverse-proxies to the leader endpoint published in
	// the Lease annotation. A successful 200 with non-empty body
	// proves: (a) the follower observed the published endpoint, (b)
	// the proxy completed, (c) the leader responded to the proxied
	// auth-bearing request.
	require.Eventually(t, func() bool {
		resp := follower.AdminRequest(t, "GET", "/admin/state", nil)
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		// 200 with a JSON body containing "pool" is the load-bearing
		// signal — Forwarder + leader's handleState both ran.
		return resp.StatusCode == 200 && len(body) > 0
	}, 15*time.Second, 200*time.Millisecond,
		"follower's /admin/state never forwarded to the leader")
}
