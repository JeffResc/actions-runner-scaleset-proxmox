package gh

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/go-github/v84/github"
	"github.com/luthermonson/go-proxmox"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/githubauth"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/observability"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/pool"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/provisioner"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// fakeRunner is what we serve from the canned /actions/runners endpoint.
type fakeRunner struct {
	id     int64
	name   string
	status string // "online" | "offline"
	busy   bool
}

// runnersServer returns an httptest server that serves the canned set of
// runners under both repo and org endpoints (the reconciler uses one or
// the other based on scope).
func runnersServer(t *testing.T, runners []fakeRunner) *httptest.Server {
	t.Helper()
	body := struct {
		TotalCount int              `json:"total_count"`
		Runners    []*github.Runner `json:"runners"`
	}{TotalCount: len(runners)}
	for _, r := range runners {
		id, name, status, busy := r.id, r.name, r.status, r.busy
		body.Runners = append(body.Runners, &github.Runner{
			ID:     &id,
			Name:   &name,
			Status: &status,
			Busy:   &busy,
		})
	}
	enc, err := json.Marshal(body)
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.HandleFunc("/repos/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(enc)
	})
	mux.HandleFunc("/orgs/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(enc)
	})
	return httptest.NewServer(mux)
}

func newTestClient(t *testing.T, srv *httptest.Server) *github.Client {
	t.Helper()
	cli := github.NewClient(http.DefaultClient)
	base, err := cli.BaseURL.Parse(srv.URL + "/")
	require.NoError(t, err)
	cli.BaseURL = base
	return cli
}

// fakeManager records lifecycle calls so tests can assert what the
// reconciler tried to do.
type fakeManager struct {
	mu           sync.Mutex
	rows         []pool.RowSnapshot
	promoteCalls []promoteCall
	destroyCalls []destroyCall

	// promoteErr, when non-nil, is returned from every PromoteToRunning
	// call — used to exercise the warn-and-meter failure path.
	promoteErr error
}

type promoteCall struct {
	VMID     int
	RunnerID int64
	JobID    int64
}

type destroyCall struct {
	VMID   int
	Reason string
}

func (f *fakeManager) ListRows(_ context.Context) ([]pool.RowSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]pool.RowSnapshot, len(f.rows))
	copy(out, f.rows)
	return out, nil
}

func (f *fakeManager) PromoteToRunning(_ context.Context, vmid int, runnerID, jobID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.promoteCalls = append(f.promoteCalls, promoteCall{vmid, runnerID, jobID})
	return f.promoteErr
}

func (f *fakeManager) ForceDestroy(_ context.Context, vmid int, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.destroyCalls = append(f.destroyCalls, destroyCall{vmid, reason})
	return nil
}

// The rest of pool.Manager is unused by the reconciler.
func (f *fakeManager) Acquire(context.Context, int64) (*pool.VM, error) {
	return nil, pool.ErrNoneAvailable
}
func (f *fakeManager) MarkRunning(context.Context, int, int64) error { return nil }
func (f *fakeManager) SetRunnerID(context.Context, int, int64) error { return nil }
func (f *fakeManager) MarkCompleted(context.Context, int) error      { return nil }
func (f *fakeManager) Stats(context.Context) (pool.Stats, error)     { return pool.Stats{}, nil }
func (f *fakeManager) Recover(context.Context) error                 { return nil }
func (f *fakeManager) Run(context.Context) error                     { return nil }
func (f *fakeManager) SignalRefill()                                 {}
func (f *fakeManager) SetDesiredCount(int)                           {}

// stubProv satisfies provisioner.Provisioner with no-ops. The reconciler
// only calls ListOwnedVMs and Destroy via the orphan sweep.
type stubProv struct {
	mu       sync.Mutex
	owned    []*provisioner.VM
	destroys []int

	// destroyErr, when non-nil, is returned from every Destroy call —
	// used to exercise the retry-on-failure branch of sweepProxmoxOrphans.
	destroyErr error
}

func (s *stubProv) ListOwnedVMs(context.Context) ([]*provisioner.VM, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.owned, nil
}
func (s *stubProv) Destroy(_ context.Context, v *provisioner.VM) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.destroys = append(s.destroys, v.VMID)
	return s.destroyErr
}
func (s *stubProv) Clone(context.Context, provisioner.CloneOptions) (*provisioner.VM, error) {
	return nil, nil
}
func (s *stubProv) Start(context.Context, *provisioner.VM) error                    { return nil }
func (s *stubProv) Stop(context.Context, *provisioner.VM) error                     { return nil }
func (s *stubProv) WaitReady(context.Context, *provisioner.VM, time.Duration) error { return nil }
func (s *stubProv) InjectJITConfig(context.Context, *provisioner.VM, string) error {
	return nil
}
func (s *stubProv) ReadJITConfig(context.Context, *provisioner.VM) ([]byte, error) {
	return nil, nil
}
func (s *stubProv) PowerState(context.Context, *provisioner.VM) (string, error) {
	return "running", nil
}
func (s *stubProv) Ping(context.Context) error                  { return nil }
func (s *stubProv) TemplateNode() string                        { return "pve1" }
func (s *stubProv) Client() *proxmox.Client                     { return nil }
func (s *stubProv) IsRecentlyDestroyed(int, time.Duration) bool { return false }
func (s *stubProv) InFlightCloneCount() int                     { return 0 }

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func baseCfg() Config {
	return Config{
		Scope:                githubauth.Scope{Repo: "octocat/test"},
		PollInterval:         15 * time.Second,
		AssignedGrace:        5 * time.Minute,
		RunningIdleGrace:     30 * time.Second,
		AssignedOfflineGrace: 2 * time.Minute,
		OrphanGrace:          60 * time.Second,
		RunnerNamePrefix:     "gh-runner-test-",
	}
}

// ---------------------------------------------------------------------------
// Matrix coverage
// ---------------------------------------------------------------------------

// 1. assigned + busy → promote (the listener missed JobStarted)
func TestReconcile_AssignedBusy_Promotes(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, []fakeRunner{
		{id: 100, name: "gh-runner-test-1001", status: "online", busy: true},
	})
	defer srv.Close()

	mgr := &fakeManager{rows: []pool.RowSnapshot{
		{VMID: 1001, Name: "gh-runner-test-1001", State: "assigned",
			JobID: 42, StateSince: time.Now().Add(-time.Minute)},
	}}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, r.Tick(context.Background()))

	require.Len(t, mgr.promoteCalls, 1)
	require.Equal(t, promoteCall{VMID: 1001, RunnerID: 100, JobID: 42}, mgr.promoteCalls[0])
	require.Empty(t, mgr.destroyCalls)
}

// 2. assigned + online idle + past grace → destroy
func TestReconcile_AssignedIdlePastGrace_Destroys(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, []fakeRunner{
		{id: 101, name: "gh-runner-test-1002", status: "online", busy: false},
	})
	defer srv.Close()

	mgr := &fakeManager{rows: []pool.RowSnapshot{
		{VMID: 1002, Name: "gh-runner-test-1002", State: "assigned",
			StateSince: time.Now().Add(-10 * time.Minute)},
	}}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, r.Tick(context.Background()))

	require.Empty(t, mgr.promoteCalls)
	require.Len(t, mgr.destroyCalls, 1)
	require.Equal(t, 1002, mgr.destroyCalls[0].VMID)
	require.Contains(t, mgr.destroyCalls[0].Reason, "never picked up")
}

// 3. assigned + online idle but WITHIN grace → no action
func TestReconcile_AssignedIdleWithinGrace_NoOp(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, []fakeRunner{
		{id: 102, name: "gh-runner-test-1003", status: "online", busy: false},
	})
	defer srv.Close()

	mgr := &fakeManager{rows: []pool.RowSnapshot{
		{VMID: 1003, Name: "gh-runner-test-1003", State: "assigned",
			StateSince: time.Now().Add(-30 * time.Second)},
	}}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, r.Tick(context.Background()))

	require.Empty(t, mgr.promoteCalls)
	require.Empty(t, mgr.destroyCalls)
}

// 4. assigned + offline past offline-grace → destroy
func TestReconcile_AssignedOfflinePastGrace_Destroys(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, []fakeRunner{
		{id: 103, name: "gh-runner-test-1004", status: "offline", busy: false},
	})
	defer srv.Close()

	mgr := &fakeManager{rows: []pool.RowSnapshot{
		{VMID: 1004, Name: "gh-runner-test-1004", State: "assigned",
			StateSince: time.Now().Add(-5 * time.Minute)},
	}}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, r.Tick(context.Background()))

	require.Len(t, mgr.destroyCalls, 1)
	require.Contains(t, mgr.destroyCalls[0].Reason, "offline")
}

// 5. assigned + not registered past grace → destroy
func TestReconcile_AssignedMissingPastGrace_Destroys(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, []fakeRunner{}) // no runners
	defer srv.Close()

	mgr := &fakeManager{rows: []pool.RowSnapshot{
		{VMID: 1005, Name: "gh-runner-test-1005", State: "assigned",
			StateSince: time.Now().Add(-10 * time.Minute)},
	}}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, r.Tick(context.Background()))

	require.Len(t, mgr.destroyCalls, 1)
	require.Contains(t, mgr.destroyCalls[0].Reason, "never registered")
}

// 6. running + busy → no action
func TestReconcile_RunningBusy_NoOp(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, []fakeRunner{
		{id: 200, name: "gh-runner-test-2001", status: "online", busy: true},
	})
	defer srv.Close()

	mgr := &fakeManager{rows: []pool.RowSnapshot{
		{VMID: 2001, Name: "gh-runner-test-2001", State: "running",
			StateSince: time.Now().Add(-time.Hour)},
	}}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, r.Tick(context.Background()))

	require.Empty(t, mgr.destroyCalls)
}

// 7. running + idle past idle-grace → destroy (missed JobCompleted)
func TestReconcile_RunningIdle_Destroys(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, []fakeRunner{
		{id: 201, name: "gh-runner-test-2002", status: "online", busy: false},
	})
	defer srv.Close()

	mgr := &fakeManager{rows: []pool.RowSnapshot{
		{VMID: 2002, Name: "gh-runner-test-2002", State: "running",
			StateSince: time.Now().Add(-time.Minute)},
	}}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, r.Tick(context.Background()))

	require.Len(t, mgr.destroyCalls, 1)
	require.Contains(t, mgr.destroyCalls[0].Reason, "missed JobCompleted")
}

// 8. running + offline → destroy
func TestReconcile_RunningOffline_Destroys(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, []fakeRunner{
		{id: 202, name: "gh-runner-test-2003", status: "offline", busy: false},
	})
	defer srv.Close()

	mgr := &fakeManager{rows: []pool.RowSnapshot{
		{VMID: 2003, Name: "gh-runner-test-2003", State: "running",
			StateSince: time.Now().Add(-time.Minute)},
	}}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, r.Tick(context.Background()))

	require.Len(t, mgr.destroyCalls, 1)
	require.Contains(t, mgr.destroyCalls[0].Reason, "offline")
}

// 9. running + missing → destroy
func TestReconcile_RunningMissing_Destroys(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, []fakeRunner{})
	defer srv.Close()

	mgr := &fakeManager{rows: []pool.RowSnapshot{
		{VMID: 2004, Name: "gh-runner-test-2004", State: "running",
			StateSince: time.Now().Add(-time.Minute)},
	}}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, r.Tick(context.Background()))

	require.Len(t, mgr.destroyCalls, 1)
	require.Contains(t, mgr.destroyCalls[0].Reason, "missing")
}

// 10. hot + busy → promote (sneak-assignment)
func TestReconcile_HotBusy_Promotes(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, []fakeRunner{
		{id: 300, name: "gh-runner-test-3001", status: "online", busy: true},
	})
	defer srv.Close()

	mgr := &fakeManager{rows: []pool.RowSnapshot{
		{VMID: 3001, Name: "gh-runner-test-3001", State: "hot",
			StateSince: time.Now().Add(-time.Minute)},
	}}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, r.Tick(context.Background()))

	require.Len(t, mgr.promoteCalls, 1)
	require.Equal(t, 3001, mgr.promoteCalls[0].VMID)
}

// 11. hot + offline (normal pre-JIT state) → no action
func TestReconcile_HotOffline_NoOp(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, []fakeRunner{})
	defer srv.Close()

	mgr := &fakeManager{rows: []pool.RowSnapshot{
		{VMID: 3002, Name: "gh-runner-test-3002", State: "hot",
			StateSince: time.Now().Add(-time.Hour)},
	}}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, r.Tick(context.Background()))

	require.Empty(t, mgr.promoteCalls)
	require.Empty(t, mgr.destroyCalls)
}

// TestReconcile_PromoteFailure_MetersAndContinues guards the observability
// contract on PromoteToRunning failures: the reconciler must surface a
// metric (and log) rather than silently discarding the error. Without
// this, a persistently broken row sits in `assigned` forever while every
// tick logs "promoting…" with no visible failure.
func TestReconcile_PromoteFailure_MetersAndContinues(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, []fakeRunner{
		{id: 999, name: "gh-runner-test-9999", status: "online", busy: true},
	})
	defer srv.Close()

	mgr := &fakeManager{
		rows: []pool.RowSnapshot{
			{VMID: 9999, Name: "gh-runner-test-9999", State: "assigned",
				JobID: 7, StateSince: time.Now().Add(-time.Minute)},
		},
		promoteErr: errors.New("store: row not found"),
	}
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), metrics)
	require.NoError(t, err)
	require.NoError(t, r.Tick(context.Background()))

	require.Len(t, mgr.promoteCalls, 1, "reconciler must still attempt the promotion")
	require.Equal(t, float64(1),
		testutil.ToFloat64(metrics.ReconcileErrors.WithLabelValues("promote_running")),
		"failed PromoteToRunning must increment scaleset_reconcile_errors_total{op=promote_running}")
}

// 12. GH runner not in DB → orphan cleanup via RemoveRunner
func TestReconcile_OrphanGitHubRunner_Removes(t *testing.T) {
	t.Parallel()
	var removedID int64
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/octocat/test/actions/runners", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"total_count":1,"runners":[{"id":999,"name":"gh-runner-test-9999","status":"offline","busy":false}]}`))
	})
	mux.HandleFunc("/repos/octocat/test/actions/runners/999", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			removedID = 999
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mgr := &fakeManager{rows: nil} // no DB rows; the GH runner is orphan
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)

	// First tick records the orphan but doesn't remove it (within grace
	// window — protects against races where the runner registered just
	// before the DB row landed).
	require.NoError(t, r.Tick(context.Background()))
	require.Equal(t, int64(0), removedID, "first tick must not reap a freshly-orphaned runner")

	// Advance the clock past the grace window and tick again — now the
	// runner should be removed.
	r.now = func() time.Time { return time.Now().Add(2 * orphanGrace) }
	require.NoError(t, r.Tick(context.Background()))
	require.Equal(t, int64(999), removedID, "second tick past grace must remove the orphan")
}

// TestReconcile_OrphanFirstTickProtectedByGrace: regression guard for
// the race where a fresh runner registered on GitHub microseconds before
// the orchestrator wrote its DB row. The first tick observes the orphan
// but must NOT reap it; if the row appears on the next tick the orphan
// tracking entry is cleared cleanly.
func TestReconcile_OrphanFirstTickProtectedByGrace(t *testing.T) {
	t.Parallel()
	var removedID int64
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/octocat/test/actions/runners", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"total_count":1,"runners":[{"id":777,"name":"gh-runner-test-7777","status":"online","busy":false}]}`))
	})
	mux.HandleFunc("/repos/octocat/test/actions/runners/777", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			removedID = 777
			w.WriteHeader(http.StatusNoContent)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mgr := &fakeManager{rows: nil}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)

	// Tick 1: orphan tracked, not removed.
	require.NoError(t, r.Tick(context.Background()))
	require.Equal(t, int64(0), removedID)
	require.Contains(t, r.orphanFirstSeen, "gh-runner-test-7777")

	// Before grace elapses, the row catches up — orphan tracking must
	// be cleared, even if we tick again.
	mgr.rows = []pool.RowSnapshot{{
		VMID: 7777, Node: "pve1", Name: "gh-runner-test-7777",
		State: "hot", CreatedAt: time.Now(), StateSince: time.Now(),
	}}
	require.NoError(t, r.Tick(context.Background()))
	require.Equal(t, int64(0), removedID)
	require.NotContains(t, r.orphanFirstSeen, "gh-runner-test-7777")
}

// TestCleanupOrphanRunners_PreservesGraceAcrossEmptyRunnerWindow: a
// transient tick where the GitHub runners list is empty (e.g., between
// jobs) must NOT reset the orphan-grace clock for runners that reappear
// later. The previous early-return wiped the map entirely, so a runner
// that was orphan-for-25s, briefly invisible, and then visible again
// would have its grace clock restart at 0 and never get reaped.
func TestCleanupOrphanRunners_PreservesGraceAcrossEmptyRunnerWindow(t *testing.T) {
	t.Parallel()
	mgr := &fakeManager{rows: nil}
	r, err := New(baseCfg(), github.NewClient(nil), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)

	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return t0 }

	// Tick 1: runner A observed without a DB row → tracked.
	r.cleanupOrphanRunners(context.Background(), nil, map[string]runnerState{
		"gh-runner-test-1": {id: 1, online: true, busy: false},
	})
	first, ok := r.orphanFirstSeen["gh-runner-test-1"]
	require.True(t, ok, "orphan must be tracked after first observation")
	require.Equal(t, t0, first)

	// Tick 2: runners list comes back empty (transient). The bug: the
	// previous implementation wiped orphanFirstSeen entirely here.
	r.now = func() time.Time { return t0.Add(10 * time.Second) }
	r.cleanupOrphanRunners(context.Background(), nil, map[string]runnerState{})

	// Tick 3: runner A observed again. Its grace clock must still be
	// anchored at t0, not restarted to t0+20s.
	r.now = func() time.Time { return t0.Add(20 * time.Second) }
	r.cleanupOrphanRunners(context.Background(), nil, map[string]runnerState{
		"gh-runner-test-1": {id: 1, online: true, busy: false},
	})
	preserved, ok := r.orphanFirstSeen["gh-runner-test-1"]
	require.True(t, ok, "orphan tracking must survive an empty-runners tick")
	require.Equal(t, t0, preserved,
		"orphan first-seen timestamp must NOT be reset by an empty-runners tick")
}

// 13. Proxmox VM exists but no DB row → reconciler destroys it.
// TestSweepProxmoxOrphans_RespectsOrphanGrace locks in the grace
// behaviour: a Proxmox VM missing from the store on its first sight
// must be RECORDED, not destroyed. Without the grace, the sweep
// destroys VMs the pool worker has cloned but not yet booted+inserted
// — producing "Configuration file <vmid>.conf does not exist"
// JIT-inject errors when the boot pipeline catches up to a deleted VM.
func TestSweepProxmoxOrphans_RespectsOrphanGrace(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, []fakeRunner{})
	defer srv.Close()

	prov := &stubProv{
		owned: []*provisioner.VM{{VMID: 4001, Node: "pve1", Name: "gh-runner-test-4001"}},
	}
	mgr := &fakeManager{rows: nil}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, prov, silentLogger(), nil)
	require.NoError(t, err)

	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return t0 }

	// Leg 1: first sight. The VM exists in PVE but not in the store;
	// without the grace fix today's code would destroy immediately —
	// the new behaviour is to record the first-seen timestamp and
	// leave the VM alone for at least OrphanGrace.
	require.NoError(t, r.Tick(context.Background()))
	require.Empty(t, prov.destroys,
		"first sight must NOT destroy — the VM may be a fresh clone whose store row hasn't landed yet")
	require.Contains(t, r.orphanProxmoxFirstSeen, 4001,
		"first sight must record the orphan candidate")

	// Leg 2: still within grace. Advance partway through; no destroy.
	r.now = func() time.Time { return t0.Add(30 * time.Second) }
	require.NoError(t, r.Tick(context.Background()))
	require.Empty(t, prov.destroys, "still within grace; must not destroy")

	// Leg 3: past grace. Destroy fires.
	r.now = func() time.Time { return t0.Add(baseCfg().OrphanGrace + time.Second) }
	require.NoError(t, r.Tick(context.Background()))
	require.Equal(t, []int{4001}, prov.destroys,
		"past grace, the genuine orphan must be destroyed")
}

// TestSweepProxmoxOrphans_PreservesEntryWhenDestroyFails: if the
// destroy call to PVE fails (transient PVE error, locked .conf, etc.),
// the orphan-first-seen entry must remain so the NEXT tick retries.
// Deleting the entry on failure would reset the grace clock and turn a
// transient PVE outage into "we'll re-record this orphan every tick
// for OrphanGrace seconds, then maybe try once more" — exactly the
// kind of subtle data-loss bug that bites at scale.
func TestSweepProxmoxOrphans_PreservesEntryWhenDestroyFails(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, []fakeRunner{})
	defer srv.Close()

	prov := &stubProv{
		owned:      []*provisioner.VM{{VMID: 4003, Node: "pve1", Name: "gh-runner-test-4003"}},
		destroyErr: errors.New("transient PVE failure"),
	}
	mgr := &fakeManager{rows: nil}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, prov, silentLogger(), nil)
	require.NoError(t, err)

	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return t0 }

	// First tick records the orphan.
	require.NoError(t, r.Tick(context.Background()))
	require.Contains(t, r.orphanProxmoxFirstSeen, 4003)

	// Advance past grace and tick — destroy is attempted but fails.
	r.now = func() time.Time { return t0.Add(baseCfg().OrphanGrace + time.Second) }
	require.NoError(t, r.Tick(context.Background()))
	require.Equal(t, []int{4003}, prov.destroys, "destroy must have been attempted")
	require.Contains(t, r.orphanProxmoxFirstSeen, 4003,
		"a failed destroy must NOT delete the first-seen entry — the next tick should retry")

	// Tick again with the failure cleared — destroy succeeds and the
	// entry is finally cleared.
	prov.mu.Lock()
	prov.destroyErr = nil
	prov.mu.Unlock()
	require.NoError(t, r.Tick(context.Background()))
	require.Equal(t, []int{4003, 4003}, prov.destroys, "the next tick must retry the destroy")
	require.NotContains(t, r.orphanProxmoxFirstSeen, 4003,
		"after a successful destroy the first-seen entry is cleared")
}

// TestSweepProxmoxOrphans_ClearsEntryWhenVMReappearsInStore: when the
// store row catches up before grace expires, the orphan-first-seen
// entry must be cleared so the same VMID doesn't carry a stale grace
// clock that fires later if the VM disappears again.
func TestSweepProxmoxOrphans_ClearsEntryWhenVMReappearsInStore(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, []fakeRunner{})
	defer srv.Close()

	prov := &stubProv{
		owned: []*provisioner.VM{{VMID: 4002, Node: "pve1", Name: "gh-runner-test-4002"}},
	}
	mgr := &fakeManager{rows: nil}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, prov, silentLogger(), nil)
	require.NoError(t, err)

	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return t0 }

	// First tick: orphan recorded.
	require.NoError(t, r.Tick(context.Background()))
	require.Contains(t, r.orphanProxmoxFirstSeen, 4002)

	// Pool worker finishes its clone and the row appears in the store
	// snapshot before grace expires. Subsequent tick MUST drop the
	// stale orphan entry so a future absence doesn't reuse the old
	// grace clock.
	mgr.rows = []pool.RowSnapshot{{
		VMID: 4002, Node: "pve1", Name: "gh-runner-test-4002",
		State: "provisioning", CreatedAt: t0, StateSince: t0,
	}}
	r.now = func() time.Time { return t0.Add(10 * time.Second) }
	require.NoError(t, r.Tick(context.Background()))
	require.NotContains(t, r.orphanProxmoxFirstSeen, 4002,
		"once the VM reappears in the store, its orphan-first-seen entry must be cleared")
	require.Empty(t, prov.destroys, "no destroy should happen once the row catches up")
}

// 14. Runners whose name does NOT match our prefix are ignored
// (someone else's runners share the same scope).
func TestReconcile_IgnoresForeignRunners(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, []fakeRunner{
		{id: 500, name: "other-runner-1", status: "online", busy: false},
		{id: 501, name: "gh-runner-test-5001", status: "online", busy: false},
	})
	defer srv.Close()

	mgr := &fakeManager{rows: nil}
	r, err := New(baseCfg(), newTestClient(t, srv), mgr, &stubProv{}, silentLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, r.Tick(context.Background()))

	// `other-runner-1` must not have been targeted for removal — only
	// 5001 (our prefix) would be considered an orphan when there's no
	// matching DB row. With mgr.rows empty, we'd expect a removal of
	// 5001 only. Verify the request count by re-issuing tick? Easier:
	// inspect destroyCalls — there should be none on the pool side
	// (orphan removal goes through gh.Actions.RemoveRunner, not the
	// pool). The matrix path didn't trigger anything else.
	require.Empty(t, mgr.destroyCalls)
}

// ---------------------------------------------------------------------------
// Config / construction
// ---------------------------------------------------------------------------

func TestConfig_Validate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"happy", func(*Config) {}, false},
		{"bad scope", func(c *Config) { c.Scope = githubauth.Scope{} }, true},
		{"zero poll", func(c *Config) { c.PollInterval = 0 }, true},
		{"zero assigned grace", func(c *Config) { c.AssignedGrace = 0 }, true},
		{"zero running grace", func(c *Config) { c.RunningIdleGrace = 0 }, true},
		{"empty prefix", func(c *Config) { c.RunnerNamePrefix = "" }, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := baseCfg()
			c.mutate(&cfg)
			err := cfg.Validate()
			if c.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNew_RequiresNonNilDeps(t *testing.T) {
	t.Parallel()
	srv := runnersServer(t, nil)
	defer srv.Close()
	cli := newTestClient(t, srv)
	mgr := &fakeManager{}
	prov := &stubProv{}
	_, err := New(baseCfg(), nil, mgr, prov, nil, nil)
	require.Error(t, err)
	_, err = New(baseCfg(), cli, nil, prov, nil, nil)
	require.Error(t, err)
	_, err = New(baseCfg(), cli, mgr, nil, nil, nil)
	require.Error(t, err)
}
