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
