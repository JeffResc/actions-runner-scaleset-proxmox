//go:build e2e

package e2e

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestE2E_DryRunMakesNoRealClones boots the orchestrator with
// DryRun=true and verifies the destructive-op short-circuit is in
// effect: the fake Proxmox sees zero clones, starts, or destroys
// against its API. Only the seeded template VM appears in the fake's
// snapshot.
//
// Read-only Proxmox calls (Ping, ListOwnedVMs, template discovery)
// still pass through to the inner provisioner — that's how the
// orchestrator can announce /readyz green and reconcile without
// committing to any side effects. The pool's clone path goes through
// the dry-run wrapper, which returns synthetic VM objects without
// touching Proxmox; the boot path's WaitReady then queries the fake
// for VMs that don't exist there, which fails and re-queues the
// boot. That's fine for this scenario — we're proving the absence
// of materialised state, not testing boot semantics in dry-run mode.
func TestE2E_DryRunMakesNoRealClones(t *testing.T) {
	h := Start(t, Options{
		HotSize:              2,
		MaxConcurrentRunners: 4,
		DryRun:               true,
	})

	// Give the reconcile loop a few ticks worth of headroom to attempt
	// clones it would normally have made.
	time.Sleep(1 * time.Second)

	snap := h.Proxmox.Snapshot()
	require.Len(t, snap, 1,
		"dry-run must not materialise any VMs; saw %v", snap)
	require.Equal(t, 9000, snap[0].VMID,
		"only the seeded template should be present after dry-run startup")

	// /readyz should still be green — dry-run is a runtime concern,
	// not a startup gate. The harness's Start already waited on this
	// before returning, but assert here so the test failure mode is
	// obvious if the ready gate ever starts depending on pool state.
	resp, err := http.Get(h.ObsURL + "/readyz")
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode,
		"dry-run is a runtime concern; /readyz should still be green")
}

// TestE2E_DryRunNoStateChangingProxmoxCalls is the stricter
// end-to-end safety property called out by #295: the existing
// snapshot check proves no VMs materialised, but does not prove
// the orchestrator made no state-changing API calls at all.
// A regression that bypassed the dry-run wrapper for, say, a
// PUT /config (tag stamping) or DELETE (destroy reconcile)
// would not surface in Snapshot() if the fake rejected the call
// — but the operator who reads /metrics would still see
// scaleset_proxmox_api_errors_total tick.
//
// This test asserts the absence at the wire level: zero
// POST/PUT/PATCH/DELETE calls reached the fake during a full
// reconcile window. Read-method calls (Ping, ListNodes,
// ListVMs) still pass through as the production code requires
// for /readyz.
func TestE2E_DryRunNoStateChangingProxmoxCalls(t *testing.T) {
	h := Start(t, Options{
		HotSize:              2,
		MaxConcurrentRunners: 4,
		DryRun:               true,
	})

	// Let the reconciler tick a few times so any leaked write
	// calls would have fired.
	time.Sleep(1500 * time.Millisecond)

	writes := h.Proxmox.WriteCalls()
	require.Empty(t, writes,
		"dry-run must not leak state-changing API calls; observed: %v", writes)
}

// TestE2E_DryRunOrchestrationStillRuns locks in the orchestration-
// logic-still-runs side of #295: dry-run is about Proxmox-side
// safety, NOT disabling pool reconcile. The pool manager's
// reconcile loop must still fire so an operator can rehearse a
// new config without committing real side effects. A regression
// that gated reconcile execution behind "real provisioner" would
// make dry-run useless for its primary purpose.
//
// scaleset_reconcile_duration_seconds is a histogram whose
// count tick is observable as the metric base name with a _count
// suffix in the /metrics scrape. We assert >= 1 reconcile pass
// has been recorded — proof the loop ran, regardless of whether
// any individual Clone synthetic succeeded downstream.
func TestE2E_DryRunOrchestrationStillRuns(t *testing.T) {
	h := Start(t, Options{
		HotSize:              2,
		MaxConcurrentRunners: 4,
		DryRun:               true,
	})

	require.Eventually(t, func() bool {
		// Reconcile histogram count is the canonical "loop ran"
		// signal — no other emission depends on Proxmox state.
		return h.MetricValue(t, "scaleset_reconcile_duration_seconds_count") >= 1
	}, 10*time.Second, 200*time.Millisecond,
		"dry-run reconcile loop must still run — the wrapper short-circuits Proxmox writes but does NOT disable orchestration logic (issue #295)")
}

// Log-tagging coverage for the [dry-run] log lines lives in the
// internal/provisioner package (dryrun unit tests). Exercising
// it end-to-end requires injecting a captureable slog handler
// into app.Run, which the orchestrator doesn't expose today —
// adding that seam is a separate refactor.
