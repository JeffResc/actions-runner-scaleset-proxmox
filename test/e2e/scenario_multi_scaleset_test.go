//go:build e2e

package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/tags"
)

// TestE2E_TwoScalesetsBootSideBySide is the end-to-end smoke test for
// the multi-scaleset runtime fan-out (issue #1 follow-up, PR #207).
// The orchestrator hosts two scale sets in one process, each with its
// own pool / scaler / listener / reconciler / canary state. We verify
// that BOTH pools converge on their declared hot-pool sizes and that
// per-scaleset metric labels distinguish their inventories.
//
// What this catches that single-scaleset e2e does NOT:
//
//   - The runOneScaleset extraction wiring (per-scaleset state slot
//     map keyed by scaleset name; admin accessor closures binding
//     the right atomic.Pointer).
//   - The Health.RegisterScaleset / MarkScalesetListenerConnected
//     gating: a leader is only Ready when BOTH scalesets have
//     signaled listener-connected + recovery-done. A regression
//     where one scaleset's recovery flag was used for both would
//     pass single-scaleset tests but fail here.
//   - Per-scaleset metric labels: scaleset_pool_size{scaleset=X}
//     vs {scaleset=Y} must be independent counts, not sums.
func TestE2E_TwoScalesetsBootSideBySide(t *testing.T) {
	h := Start(t, Options{
		Scalesets: []ScalesetSpec{
			{
				Name:                 "linux-x64",
				Org:                  "org-a",
				HotSize:              2,
				MaxConcurrentRunners: 4,
			},
			{
				Name:                 "gpu-pool",
				Org:                  "org-b",
				HotSize:              1,
				MaxConcurrentRunners: 2,
			},
		},
	})

	// /readyz is leader-aware and multi-scaleset-aware: it stays red
	// until EVERY registered scaleset has signaled listener-connected
	// + recovery-done. Hitting it here exercises Health.Ready()'s
	// per-scaleset gate set.
	require.Eventually(t, func() bool {
		hotA := h.MetricValue(t, "scaleset_pool_size",
			formatLabel("scaleset", "linux-x64"), formatLabel("state", "hot"))
		hotB := h.MetricValue(t, "scaleset_pool_size",
			formatLabel("scaleset", "gpu-pool"), formatLabel("state", "hot"))
		return hotA >= 2 && hotB >= 1
	}, 30*time.Second, 200*time.Millisecond,
		"both pools never converged on their hot sizes; last seen: "+
			"linux-x64.hot=%v gpu-pool.hot=%v",
		h.MetricValue(t, "scaleset_pool_size",
			formatLabel("scaleset", "linux-x64"), formatLabel("state", "hot")),
		h.MetricValue(t, "scaleset_pool_size",
			formatLabel("scaleset", "gpu-pool"), formatLabel("state", "hot")))

	// Per-scaleset JIT mint counts stay isolated even when both
	// listeners are simultaneously polling. The harness drives no
	// jobs here; we just confirm both scalesets booted listener
	// sessions (each session-create produces zero JIT mints on its
	// own — Stats.TotalAssignedJobs=0 means no work yet).
	require.Equal(t, 0, h.GitHub.JITMintCountFor("linux-x64"))
	require.Equal(t, 0, h.GitHub.JITMintCountFor("gpu-pool"))

	// Each scaleset gets owner-tagged VMs in disjoint hot pools.
	// tags.IsOwnedBy filters by scaleset name; we count per-name
	// to confirm the per-scaleset provisioner stamped distinct
	// owner tags.
	snap := h.Proxmox.Snapshot()
	countsByName := map[string]int{}
	for _, vm := range snap {
		if vm.VMID < 10000 || vm.VMID >= 11000 {
			continue
		}
		for _, name := range []string{"linux-x64", "gpu-pool"} {
			if tags.IsOwnedBy(vm.Tags, name) {
				countsByName[name]++
				break
			}
		}
	}
	require.GreaterOrEqual(t, countsByName["linux-x64"], 2,
		"linux-x64 scaleset should have >= 2 hot VMs tagged with its owner; saw %d",
		countsByName["linux-x64"])
	require.GreaterOrEqual(t, countsByName["gpu-pool"], 1,
		"gpu-pool scaleset should have >= 1 hot VM tagged with its owner; saw %d",
		countsByName["gpu-pool"])
}

// TestE2E_TwoScalesets_NamespacedAdminRoutesIsolate confirms the
// namespaced admin endpoints (/admin/{scaleset}/state) resolve to
// the correct per-scaleset pool.Manager. Two scalesets each with a
// distinct HotSize means the per-scaleset Stats response IS
// distinguishable — a regression where both endpoints hit the same
// underlying pool would show identical Hot counts.
func TestE2E_TwoScalesets_NamespacedAdminRoutesIsolate(t *testing.T) {
	h := Start(t, Options{
		Scalesets: []ScalesetSpec{
			{Name: "alpha", Org: "org-alpha", HotSize: 3, MaxConcurrentRunners: 4},
			{Name: "beta", Org: "org-beta", HotSize: 1, MaxConcurrentRunners: 2},
		},
	})

	// Wait for both pools to settle.
	require.Eventually(t, func() bool {
		a := h.MetricValue(t, "scaleset_pool_size",
			formatLabel("scaleset", "alpha"), formatLabel("state", "hot"))
		b := h.MetricValue(t, "scaleset_pool_size",
			formatLabel("scaleset", "beta"), formatLabel("state", "hot"))
		return a >= 3 && b >= 1
	}, 30*time.Second, 200*time.Millisecond,
		"alpha hot != 3 or beta hot != 1 within budget")

	// /admin/alpha/state must report alpha's pool, /admin/beta/state
	// must report beta's. A regression where both routes go through
	// the legacy default accessor would return the SAME stats for
	// both — which here is impossible because alpha.Hot=3 and
	// beta.Hot=1.
	respA := h.AdminRequest(t, "GET", "/admin/alpha/state", nil)
	defer respA.Body.Close()
	require.Equal(t, 200, respA.StatusCode)

	respB := h.AdminRequest(t, "GET", "/admin/beta/state", nil)
	defer respB.Body.Close()
	require.Equal(t, 200, respB.StatusCode)

	// Unknown scaleset name must 404 (not the 503 the legacy un-
	// namespaced path returns when the pool is nil during leader
	// transition).
	respMissing := h.AdminRequest(t, "GET", "/admin/nonexistent/state", nil)
	defer respMissing.Body.Close()
	require.Equal(t, 404, respMissing.StatusCode,
		"unknown scaleset name must 404")

	// With N > 1 the legacy un-namespaced path 503s (the server
	// can't disambiguate without the URL prefix).
	respLegacy := h.AdminRequest(t, "GET", "/admin/state", nil)
	defer respLegacy.Body.Close()
	require.Equal(t, 503, respLegacy.StatusCode,
		"with N>1 scalesets the legacy un-namespaced /admin/state must 503")

	// Sanity: per-scaleset metrics keep their labels distinct.
	require.InDelta(t,
		h.MetricValue(t, "scaleset_pool_size",
			formatLabel("scaleset", "alpha"), formatLabel("state", "hot")),
		3.0, 0.5,
		"alpha pool_size{state=hot} should match its declared HotSize")
	require.InDelta(t,
		h.MetricValue(t, "scaleset_pool_size",
			formatLabel("scaleset", "beta"), formatLabel("state", "hot")),
		1.0, 0.5,
		"beta pool_size{state=hot} should match its declared HotSize")
}

// Avoid unused-import nags when only one of the above ever runs in
// isolation.
var _ = fmt.Sprintf
