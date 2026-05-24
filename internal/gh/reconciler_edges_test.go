package gh

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/pool"
)

// flipServer serves a status from `code` on every request until
// the test mutates the value. Lets a test drive a recoverable
// upstream-error scenario without spinning the time-based Run
// loop.
type flipServer struct {
	code   atomic.Int32
	srv    *httptest.Server
	calls  atomic.Int64
	delete atomic.Int32 // status returned by DELETE
}

func newFlipServer(t *testing.T, initialList, initialDelete int32) *flipServer {
	t.Helper()
	fs := &flipServer{}
	fs.code.Store(initialList)
	fs.delete.Store(initialDelete)
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/", func(w http.ResponseWriter, r *http.Request) {
		fs.calls.Add(1)
		if r.Method == http.MethodDelete {
			w.WriteHeader(int(fs.delete.Load()))
			return
		}
		c := int(fs.code.Load())
		w.Header().Set("Content-Type", "application/json")
		if c >= 400 {
			http.Error(w, "boom", c)
			return
		}
		w.WriteHeader(c)
		_, _ = w.Write([]byte(`{"total_count":0,"runners":[]}`))
	})
	fs.srv = httptest.NewServer(mux)
	t.Cleanup(fs.srv.Close)
	return fs
}

// TestReconcile_Tick_PropagatesUpstream5xx pins the primitive
// that Run()'s exponential backoff relies on: a single Tick must
// return an error when the upstream list-runners call returns 5xx,
// and must succeed on the recovery tick once the upstream recovers.
// Without this, Run()'s backoff loop has no signal to back off on.
func TestReconcile_Tick_PropagatesUpstream5xx(t *testing.T) {
	t.Parallel()
	fs := newFlipServer(t, http.StatusInternalServerError, http.StatusNoContent)
	mgr := &fakeManager{}
	r, err := New(baseCfg(), newTestClient(t, fs.srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)

	// First tick: upstream 500 → Tick errors.
	require.Error(t, r.Tick(context.Background()),
		"upstream 5xx must surface as a Tick error so Run's backoff has a signal")

	// Switch the upstream back to a healthy response.
	fs.code.Store(http.StatusOK)

	// Recovery tick: same Reconciler, upstream now OK → Tick succeeds.
	require.NoError(t, r.Tick(context.Background()),
		"once the upstream recovers, the very next Tick must succeed (no internal back-off state pinning the error path)")
}

// TestReconcile_OrphanPrunedWhenRowAppearsBeforeRemoveRunnerRetry
// covers the race the audit (#200) flagged: an orphan was
// observed, RemoveRunner was queued / partially completed, then
// the row's store state changed before the next tick. The retry
// path must not blow up on the now-absent orphan — instead the
// in-memory orphan tracking entry must be pruned cleanly.
func TestReconcile_OrphanPrunedWhenRowAppearsBeforeRemoveRunnerRetry(t *testing.T) {
	t.Parallel()
	// Server returns one runner on the first list, then no
	// runners on subsequent lists. DELETE always errors so the
	// reconciler never gets its own positive ACK that the runner
	// is gone — the cleanup must rely on the upstream list
	// instead.
	fs := newFlipServer(t, http.StatusOK, http.StatusInternalServerError)
	// Override the list handler to return one runner.
	mux := http.NewServeMux()
	listCalls := atomic.Int64{}
	mux.HandleFunc("/repos/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusInternalServerError) // simulated GH 5xx on DELETE
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch listCalls.Add(1) {
		case 1, 2:
			_, _ = w.Write([]byte(`{"total_count":1,"runners":[{"id":999,"name":"gh-runner-test-9999","status":"online","busy":false}]}`))
		default:
			// A different prefix-matching runner shows up but
			// our orphan is gone. The pruning logic
			// deliberately preserves entries across EMPTY
			// runner lists (transient gap), but a non-empty
			// list that does not contain our orphan is the
			// authoritative "upstream forgot about it" signal
			// — that should prune the tracking entry.
			// The replacement runner must share our test
			// prefix; ListRunnersByPrefix filters out anything
			// else, so a foreign-prefix entry would look like
			// an empty list to the cleanup logic.
			_, _ = w.Write([]byte(`{"total_count":1,"runners":[{"id":42,"name":"gh-runner-test-replacement","status":"online","busy":false}]}`))
		}
	})
	fs.srv.Close()
	fs.srv = httptest.NewServer(mux)
	t.Cleanup(fs.srv.Close)

	cfg := baseCfg()
	mgr := &fakeManager{}
	rec, err := New(cfg, newTestClient(t, fs.srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)

	// Anchor "now" so we can drive past the orphan grace window.
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	rec.now = func() time.Time { return t0 }

	// Tick 1: orphan observed, grace started.
	require.NoError(t, rec.Tick(context.Background()))
	require.Contains(t, rec.orphanFirstSeen, "gh-runner-test-9999",
		"orphan must be tracked after the first observation")

	// Tick 2: well past the grace window. The reconciler will
	// attempt RemoveRunner which 500's. Tick itself does NOT
	// return an error from the cleanup branch (best-effort), so
	// it should succeed even though the upstream DELETE failed.
	rec.now = func() time.Time { return t0.Add(time.Minute) }
	require.NoError(t, rec.Tick(context.Background()),
		"cleanup-side 5xx is best-effort; Tick must not surface it as an error")

	// Tick 3: upstream list now returns no runners (the runner
	// was actually deleted out-of-band). The orphan tracking
	// entry must be pruned cleanly so we don't keep retrying
	// against a vanished runner.
	rec.now = func() time.Time { return t0.Add(2 * time.Minute) }
	require.NoError(t, rec.Tick(context.Background()))
	require.NotContains(t, rec.orphanFirstSeen, "gh-runner-test-9999",
		"orphan tracking must be pruned once the runner is no longer in the upstream list, "+
			"even if our DELETE returned 5xx along the way")
}

// TestReconcile_FastOnlineBusyOfflineTransitions pins that the
// matrix classifies a row by the CURRENT snapshot, not by history.
// A row that flips online→busy→offline within one AssignedGrace
// window must produce the right outcome on each tick:
//
//   - tick 1 (online+idle, within grace): noop (waiting)
//   - tick 2 (busy):                        promote to running
//   - tick 3 (offline, within grace):       still noop on the
//     now-Running row
//     (running+offline destroys regardless of grace — pins the
//     matrix's running+offline cell against churn)
func TestReconcile_FastOnlineBusyOfflineTransitions(t *testing.T) {
	t.Parallel()
	// Drive the runners list dynamically so each Tick sees a
	// different state for the same runner.
	state := atomic.Value{}
	state.Store(`{"total_count":1,"runners":[{"id":111,"name":"gh-runner-test-2001","status":"online","busy":false}]}`)
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(state.Load().(string)))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Row starts Assigned, well within AssignedGrace.
	mgr := &fakeManager{rows: []pool.RowSnapshot{{
		VMID: 2001, Node: "pve1", Name: "gh-runner-test-2001",
		State: "assigned", CreatedAt: time.Now(), StateSince: time.Now(),
	}}}
	cfg := baseCfg()
	rec, err := New(cfg, newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)

	// Tick 1: online+idle, within grace → noop.
	require.NoError(t, rec.Tick(context.Background()))
	require.Empty(t, mgr.promoteCalls,
		"assigned+online+idle within grace must not promote")
	require.Empty(t, mgr.destroyCalls,
		"assigned+online+idle within grace must not destroy")

	// Tick 2: runner flips busy → matrix promotes.
	state.Store(`{"total_count":1,"runners":[{"id":111,"name":"gh-runner-test-2001","status":"online","busy":true}]}`)
	require.NoError(t, rec.Tick(context.Background()))
	require.Len(t, mgr.promoteCalls, 1,
		"assigned+busy must promote on the very next tick — matrix reads current snapshot, not history")
	require.Equal(t, 2001, mgr.promoteCalls[0].VMID)

	// Move the row to Running so the matrix's running-row cells
	// apply on the next tick.
	mgr.rows[0].State = "running"
	mgr.rows[0].StateSince = time.Now()

	// Tick 3: runner now offline → running+offline must destroy
	// regardless of grace (the matrix cell has no grace func).
	state.Store(`{"total_count":1,"runners":[{"id":111,"name":"gh-runner-test-2001","status":"offline","busy":false}]}`)
	require.NoError(t, rec.Tick(context.Background()))
	require.Len(t, mgr.destroyCalls, 1,
		"running+offline destroys with no grace — fast offline transition must reach the destroy path on the next tick")
	require.Equal(t, 2001, mgr.destroyCalls[0].VMID)
	require.Contains(t, mgr.destroyCalls[0].Reason, "runner went offline")
}
