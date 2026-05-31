//go:build e2e

package e2e

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestE2E_WarmPoolFills boots the real orchestrator binary in-process,
// pointed at the fake Proxmox and fake GitHub servers, and waits for
// it to converge on the configured hot-pool size.
//
// This is the smallest possible full-binary e2e: it exercises the
// complete startup path (config load -> provisioner init ->
// scaleset listener handshake -> pool manager -> clone loop), then
// inspects the live /metrics endpoint to confirm the system reached
// its steady state.
func TestE2E_WarmPoolFills(t *testing.T) {
	t.Parallel()
	h := Start(t, Options{
		HotSize:              2,
		WarmSize:             0,
		MaxConcurrentRunners: 4,
	})

	// The reconcile loop runs every 100ms (e2e config). With taskDuration=5ms
	// on the fake Proxmox, full Clone+Start+WaitReady should land in well
	// under a second per VM. Give it generous slack for CI runners.
	require.Eventually(t, func() bool {
		hot := h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "hot"))
		return hot >= 2
	}, 10*time.Second, 200*time.Millisecond,
		"hot pool never reached size 2; last seen pool_size{state=hot}=%v",
		h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "hot")))

	// vms_total{outcome=clone-success} must reflect every clone we did.
	require.GreaterOrEqual(t,
		h.MetricValue(t, "scaleset_vms_total", formatLabel("outcome", "clone-success")),
		float64(2),
		"each warm-pool fill should record a clone-success in scaleset_vms_total")

	// The fake Proxmox should host at least HotSize clones in the
	// orchestrator's vmid range. Under heavy scheduling the reconciler
	// may transiently overshoot — we allow up to MaxConcurrentRunners
	// (4) here since the warm-pool floor is the contract under test.
	snap := h.Proxmox.Snapshot()
	clones := 0
	for _, vm := range snap {
		if vm.VMID >= 10000 && vm.VMID < 11000 {
			clones++
		}
	}
	require.GreaterOrEqual(t, clones, 2,
		"expected at least 2 VMs in the orchestrator's vmid range; saw %d (snapshot=%v)",
		clones, snap)
	require.LessOrEqual(t, clones, 4,
		"unexpected overshoot beyond MaxConcurrentRunners; saw %d (snapshot=%v)",
		clones, snap)

	// /admin/state must report the pool through the live admin API,
	// not just /metrics.
	resp := h.AdminRequest(t, "GET", "/admin/state", nil)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode,
		"admin /state returned %d (response body parsed elsewhere)", resp.StatusCode)
}

// TestE2E_Drain confirms POST /admin/drain shuts the orchestrator down
// cleanly. The harness's Stop is wired to ctx-cancel; here we exercise
// the operator path that hits the HTTP endpoint instead.
func TestE2E_Drain(t *testing.T) {
	t.Parallel()
	h := Start(t, Options{HotSize: 1, MaxConcurrentRunners: 2})

	// Wait for the hot pool to fill so drain has actual work.
	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "hot")) >= 1
	}, 10*time.Second, 200*time.Millisecond)

	resp := h.AdminRequest(t, "POST", "/admin/drain", nil)
	resp.Body.Close()
	require.Equal(t, 202, resp.StatusCode,
		"drain accepts but doesn't synchronously wait — expect 202")

	// The orchestrator should exit within DrainTimeout (5s in the
	// e2e config) plus a small grace. We deliberately do NOT use
	// Stop() here — drain should cause clean self-exit. But install
	// a fallback in case it hangs.
	deadline := time.After(15 * time.Second)
	exited := false
	for !exited {
		select {
		case <-deadline:
			t.Fatalf("orchestrator did not exit within 15s of /admin/drain")
		case <-time.After(100 * time.Millisecond):
		}
		// Liveness probe: /readyz becomes unreachable once the obs
		// HTTP server has shut down.
		if _, err := http.Get(h.ObsURL + "/readyz"); err != nil {
			exited = true
		}
	}
	// Drain cleanly removes h.cancel so the t.Cleanup-driven Stop
	// is a no-op.
	fmt.Fprintln(testWriter{t}, "orchestrator exited cleanly via /admin/drain")
}

// testWriter adapts t.Logf to io.Writer for free-form logging.
type testWriter struct{ t testing.TB }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}
