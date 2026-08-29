package gh

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/observability"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/pool"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/provisioner"
)

// TestCleanupOrphanRunners_TruncatedCapMakesNoDeleteDecision pins the
// delete-or-keep DECISION at the pagination-cap boundary (#358): when the
// runner listing is truncated (a scope over the maxListPages cap whose
// current partial page carries none of our prefix's orphans), the sweep
// must NOT issue a RemoveRunner for a tracked orphan that is already past
// OrphanGrace — a truncated view is not authoritative, so an orphan on an
// un-fetched page merely looks "gone" and must be left tracked, not reaped.
// The contrast leg proves the preserved tracking then reaps the SAME
// orphan the moment a COMPLETE listing shows it, so the truncated window
// only defers (never loses) the decision.
//
// Distinct from TestCleanupOrphanRunners_TruncatedListDoesNotResetGraceClock
// (which asserts the first-seen TIMESTAMP survives) and from the
// truncation-signal test — this one asserts the RemoveRunner call itself
// never fires while truncated.
func TestCleanupOrphanRunners_TruncatedCapMakesNoDeleteDecision(t *testing.T) {
	t.Parallel()
	var deletes atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deletes.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"total_count":0,"runners":[]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mgr := &fakeManager{rows: nil}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)

	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	// The orphan was first seen well over a full OrphanGrace ago, so it is
	// unambiguously past grace and would be reaped on an authoritative tick.
	r.orphanFirstSeen["gh-runner-test-1"] = t0.Add(-2 * baseCfg().OrphanGrace)
	r.now = func() time.Time { return t0 }

	// Truncated tick: the partial page carries a DIFFERENT prefix-matching
	// runner; our tracked orphan is (presumably) on an un-fetched page.
	// Because the view is truncated, the sweep must make no delete decision
	// for the absent-but-tracked orphan.
	r.cleanupOrphanRunners(context.Background(), nil, map[string]pool.RunnerInfo{
		"gh-runner-test-other": {ID: 9, Online: true, Busy: false},
	}, true /* truncated */)

	require.Equal(t, int64(0), deletes.Load(),
		"a truncated listing must not reap a tracked orphan that is merely absent from the partial page")
	require.Contains(t, r.orphanFirstSeen, "gh-runner-test-1",
		"the orphan must remain tracked so an authoritative tick can decide")

	// Contrast: a COMPLETE tick that actually shows the orphan (still past
	// grace) now reaps it — the deferred decision lands exactly once.
	r.cleanupOrphanRunners(context.Background(), nil, map[string]pool.RunnerInfo{
		"gh-runner-test-1": {ID: 1, Online: true, Busy: false},
	}, false)
	require.Equal(t, int64(1), deletes.Load(),
		"once an authoritative listing shows the past-grace orphan, exactly one RemoveRunner must fire")
	require.NotContains(t, r.orphanFirstSeen, "gh-runner-test-1",
		"a successful reap drops the tracking entry")
}

// churnManager overrides ListRows to hand out a scripted sequence of
// snapshots across successive Ticks, modelling the pool worker inserting/
// removing store rows between reconcile passes. All other pool.Manager
// methods come from the embedded *fakeManager.
type churnManager struct {
	*fakeManager
	calls     atomic.Int64
	snapshots [][]pool.RowSnapshot
}

func (c *churnManager) ListRows(context.Context) ([]pool.RowSnapshot, error) {
	i := int(c.calls.Add(1)) - 1
	if i >= len(c.snapshots) {
		i = len(c.snapshots) - 1
	}
	snap := c.snapshots[i]
	out := make([]pool.RowSnapshot, len(snap))
	copy(out, snap)
	return out, nil
}

// TestReconcile_RowChurnAcrossTicks_NoDuplicateRemovalNoPhantomErrors pins
// the snapshot-vs-sweep interleave (#358): a runner whose matching store
// row is present in one Tick's ListRows snapshot but absent in the next
// must be reaped EXACTLY ONCE once its orphan grace elapses, and the churn
// must never spuriously bump github_errors. Within a single Tick the
// snapshot is frozen (ListRows is read once and shared by applyMatrix and
// cleanupOrphanRunners), so the churn window is the tick boundary — this
// test drives a present→absent transition across ticks and asserts the
// removal decision is stable and error-free.
func TestReconcile_RowChurnAcrossTicks_NoDuplicateRemovalNoPhantomErrors(t *testing.T) {
	t.Parallel()
	var (
		deletes atomic.Int64
		deleted atomic.Bool
	)
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deletes.Add(1)
			deleted.Store(true)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if deleted.Load() {
			// After the runner is deregistered the list authoritatively
			// no longer carries it.
			_, _ = w.Write([]byte(`{"total_count":0,"runners":[]}`))
			return
		}
		_, _ = w.Write([]byte(`{"total_count":1,"runners":[{"id":1,"name":"gh-runner-test-1","status":"offline","busy":false}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	metrics := observability.NewMetrics(prometheus.NewRegistry())
	mgr := &churnManager{
		fakeManager: &fakeManager{},
		snapshots: [][]pool.RowSnapshot{
			// Tick 1: the row exists and matches the runner → not an orphan.
			{{VMID: 1, Name: "gh-runner-test-1", State: "hot", StateSince: time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)}},
			// Tick 2+: the row is gone → the runner is now an orphan.
			nil,
		},
	}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), metrics)
	require.NoError(t, err)

	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return t0 }

	// Tick 1: row present → runner known, no orphan tracking, no removal.
	require.NoError(t, r.Tick(context.Background()))
	require.NotContains(t, r.orphanFirstSeen, "gh-runner-test-1",
		"a runner matched to a live row must not be tracked as an orphan")
	require.Equal(t, int64(0), deletes.Load())

	// Tick 2: row churned out → runner becomes an orphan, tracked at t0,
	// still within grace → no removal yet.
	require.NoError(t, r.Tick(context.Background()))
	require.Contains(t, r.orphanFirstSeen, "gh-runner-test-1")
	require.Equal(t, int64(0), deletes.Load(), "within grace, no removal")

	// Tick 3: past grace → reap exactly once.
	r.now = func() time.Time { return t0.Add(baseCfg().OrphanGrace + time.Second) }
	require.NoError(t, r.Tick(context.Background()))
	require.Equal(t, int64(1), deletes.Load(), "the churned-out orphan is reaped once past grace")

	// Tick 4: the runner is now absent from the authoritative list; the
	// tracking entry is already cleared. No second removal, no phantom error.
	require.NoError(t, r.Tick(context.Background()))
	require.Equal(t, int64(1), deletes.Load(),
		"a row present-then-absent across ticks must not trigger a duplicate removal")
	require.NotContains(t, r.orphanFirstSeen, "gh-runner-test-1")
	require.Equal(t, float64(0),
		testutil.ToFloat64(metrics.GitHubErrors.WithLabelValues("test", "remove_runner")),
		"row churn must not spuriously increment github_errors")
}

// TestSweepProxmoxOrphans_ExternallyDeletedVM documents the sweep's
// behaviour when a Proxmox orphan is deleted out-of-band before the sweep
// destroys it: prov.Destroy returns provisioner.ErrVMNotFound. The sweep
// must not wedge (Tick stays nil) and must not surface a permanent error.
//
// NOTE(#358): documents current behavior; possible bug — a not-found
// Destroy is treated exactly like a transient failure (logged, entry
// retained for a next-tick retry). Unlike removeRunner, which maps a 404
// to success and drops the tracking entry, sweepProxmoxOrphans keeps
// re-attempting a Destroy that can never succeed for an already-gone VM,
// so the orphanProxmoxFirstSeen entry lingers until the VM stops being
// reported by ListOwnedVMs. This test pins the current (retain-and-retry)
// behaviour so the discrepancy is visible if the sweep is later taught to
// treat not-found as done.
func TestSweepProxmoxOrphans_ExternallyDeletedVM(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, []fakeRunner{})
	defer srv.Close()

	prov := &stubProv{
		owned:      []*provisioner.VM{{VMID: 4009, Node: "pve1", Name: "gh-runner-test-4009"}},
		destroyErr: provisioner.ErrVMNotFound,
	}
	mgr := &fakeManager{rows: nil}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, prov, silentLogger(), nil)
	require.NoError(t, err)

	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return t0 }

	// First sight: recorded, not destroyed.
	require.NoError(t, r.Tick(context.Background()))
	require.Contains(t, r.orphanProxmoxFirstSeen, 4009)

	// Past grace: Destroy is attempted and returns ErrVMNotFound. The sweep
	// must handle it without wedging — Tick returns nil.
	r.now = func() time.Time { return t0.Add(baseCfg().OrphanGrace + time.Second) }
	require.NoError(t, r.Tick(context.Background()),
		"a not-found Destroy must not surface as a Tick error")
	require.Equal(t, []int{4009}, prov.destroys, "destroy must have been attempted")

	// Current behaviour: the entry is RETAINED (treated like a transient
	// failure), so the next tick re-attempts the doomed Destroy. See
	// NOTE above — a future fix could treat not-found as success and drop
	// the entry here instead.
	require.Contains(t, r.orphanProxmoxFirstSeen, 4009,
		"current behavior: a not-found Destroy retains the tracking entry (see NOTE(#358))")
	require.NoError(t, r.Tick(context.Background()))
	require.Equal(t, []int{4009, 4009}, prov.destroys,
		"current behavior: the sweep re-attempts the doomed Destroy on the next tick")
}
