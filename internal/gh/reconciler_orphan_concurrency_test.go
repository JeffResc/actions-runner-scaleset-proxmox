package gh

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/githubauth"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/pool"
)

// TestListRunnersByPrefix_SignalsTruncationAtPageCap pins the #337
// detection half: when the runner listing is cut short at the
// maxListPages cap (a scope with >5000 matching runners), the core
// must report truncated=true so the orphan sweep knows its view is
// partial. The server here always advertises a next page, so the
// pagination loop runs to the cap.
func TestListRunnersByPrefix_SignalsTruncationAtPageCap(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	handler := func(w http.ResponseWriter, req *http.Request) {
		// One prefixed runner per page, and always a "next" link so the
		// reconciler's loop never sees NextPage == 0 and runs to the cap.
		next := fmt.Sprintf(`<http://%s%s?page=2&per_page=100>; rel="next"`, req.Host, req.URL.Path)
		w.Header().Set("Link", next)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"total_count":99999,"runners":[{"id":1,"name":"gh-runner-test-1","status":"online","busy":false}]}`))
	}
	mux.HandleFunc("/orgs/", handler)
	mux.HandleFunc("/repos/", handler)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cli := newTestClient(t, srv)
	out, truncated, err := listRunnersByPrefix(context.Background(), cli,
		githubauth.Scope{Org: "octocat"}, "gh-runner-test-", silentLogger())
	require.NoError(t, err)
	require.True(t, truncated, "an over-cap listing must report truncated=true")
	require.NotEmpty(t, out, "the partial page contents are still returned")
}

// TestCleanupOrphanRunners_TruncatedListDoesNotResetGraceClock pins the
// #337 fix: a real orphan whose runner lives on an un-fetched page looks
// "gone" from a truncated listing. The sweep must NOT drop its
// first-seen entry (which resets the grace clock every tick and lets it
// evade the reaper forever) — truncation is treated like the empty-list
// case. The contrast case (complete listing, runner genuinely absent)
// still prunes.
func TestCleanupOrphanRunners_TruncatedListDoesNotResetGraceClock(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, nil)
	defer srv.Close()
	mgr := &fakeManager{rows: nil}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)

	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return t0 }

	// Tick 1: orphan A observed in a complete listing → tracked at t0.
	r.cleanupOrphanRunners(context.Background(), nil, map[string]pool.RunnerInfo{
		"gh-runner-test-1": {ID: 1, Online: true, Busy: false},
	}, false)
	first, ok := r.orphanFirstSeen["gh-runner-test-1"]
	require.True(t, ok)
	require.Equal(t, t0, first)

	// Tick 2: a non-empty but TRUNCATED listing that doesn't include A
	// (A lives on an un-fetched page). The grace clock must survive.
	r.now = func() time.Time { return t0.Add(10 * time.Second) }
	r.cleanupOrphanRunners(context.Background(), nil, map[string]pool.RunnerInfo{
		"gh-runner-test-9": {ID: 9, Online: true, Busy: false},
	}, true /* truncated */)
	preserved, ok := r.orphanFirstSeen["gh-runner-test-1"]
	require.True(t, ok, "a truncated listing must NOT prune an orphan that's merely on an un-fetched page")
	require.Equal(t, t0, preserved, "the orphan-grace clock must not be reset by a truncated tick")

	// Contrast: the SAME absence in a COMPLETE listing is authoritative
	// → prune.
	r.now = func() time.Time { return t0.Add(20 * time.Second) }
	r.cleanupOrphanRunners(context.Background(), nil, map[string]pool.RunnerInfo{
		"gh-runner-test-9": {ID: 9, Online: true, Busy: false},
	}, false)
	_, stillTracked := r.orphanFirstSeen["gh-runner-test-1"]
	require.False(t, stillTracked, "a complete listing that omits the runner is authoritative — prune it")
}

// TestCleanupOrphanRunners_RowAppearingMidGraceIsNotDestroyed pins the
// #329 clone-vs-sweep race deterministically: the pool worker clones VM
// 10001 and the reconciler starts an orphan timer for its runner before
// the store row lands; once the row appears, the sweep must prune the
// tracking entry and must NOT destroy the now-live VM, even past the
// grace window.
func TestCleanupOrphanRunners_RowAppearingMidGraceIsNotDestroyed(t *testing.T) {
	t.Parallel()
	var removed atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodDelete {
			removed.Add(1)
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
	r.now = func() time.Time { return t0 }

	const name = "gh-runner-test-10001"
	runners := map[string]pool.RunnerInfo{name: {ID: 10001, Online: true, Busy: false}}

	// Tick 1: runner present, no row yet → orphan tracked.
	r.cleanupOrphanRunners(context.Background(), nil, runners, false)
	require.Contains(t, r.orphanFirstSeen, name)

	// The pool worker's store row lands. Tick 2 runs well past the grace
	// window, but because the row now matches the runner the sweep must
	// prune tracking and leave the live VM alone.
	rows := []pool.RowSnapshot{{VMID: 10001, Name: name}}
	r.now = func() time.Time { return t0.Add(2 * orphanGrace) }
	r.cleanupOrphanRunners(context.Background(), rows, runners, false)

	require.NotContains(t, r.orphanFirstSeen, name, "a row that appears mid-grace must prune the orphan tracking entry")
	require.Equal(t, int32(0), removed.Load(), "a now-live VM must NOT be destroyed by the orphan sweep (#329)")
}

// TestReconciler_ConcurrentTicksAndRowChurnNoRace stresses the seam
// #329 flags: the reconciler ticking while the pool worker concurrently
// inserts/removes store rows. fakeManager.ListRows is the synchronisation
// point; this asserts repeated ticks under concurrent row churn neither
// panic nor race (run under -race).
func TestReconciler_ConcurrentTicksAndRowChurnNoRace(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, []fakeRunner{
		{id: 10001, name: "gh-runner-test-10001", status: "online"},
		{id: 10002, name: "gh-runner-test-10002", status: "online"},
	})
	defer srv.Close()

	mgr := &fakeManager{rows: nil}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// Churn: flip the matching rows in and out under the manager's lock,
	// racing the reconciler's ListRows snapshot each tick.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}
			mgr.mu.Lock()
			if i%2 == 0 {
				mgr.rows = []pool.RowSnapshot{
					{VMID: 10001, Name: "gh-runner-test-10001"},
					{VMID: 10002, Name: "gh-runner-test-10002"},
				}
			} else {
				mgr.rows = nil
			}
			mgr.mu.Unlock()
		}
	}()

	for i := 0; i < 200; i++ {
		require.NoError(t, r.Tick(context.Background()))
	}
	cancel()
	wg.Wait()
}
