package pool

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luthermonson/go-proxmox"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/config"
	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/nodeselector"
	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/observability"
	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/provisioner"
	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/store"
)

// ---------- Fake Provisioner ----------

type fakeProv struct {
	mu sync.Mutex

	cloneErr   error
	cloneDelay time.Duration
	startErr   error
	stopErr    error
	destroyErr error
	waitErr    error
	injectErr  error
	listErr    error

	// destroyErrFor lets a single test drive per-VMID destroy outcomes
	// (e.g. "the orphan on node-A succeeds, the orphan on node-B fails").
	// When set, it takes precedence over destroyErr.
	destroyErrFor map[int]error

	// destroyHang, when true, makes Destroy block until ctx is cancelled
	// — a model of the real provisioner getting stuck on an unreachable
	// Proxmox node. The fake returns ctx.Err once cancellation arrives.
	destroyHang bool

	// destroyEntered is closed the first time Destroy is called; lets
	// tests synchronise on "destroy is now in flight" without sleeping.
	destroyEntered chan struct{}

	// onDestroy, when set, is invoked synchronously inside Destroy after
	// recording the call. Used by concurrency tests to model in-flight
	// destroys (e.g. block on a channel until the test releases).
	onDestroy func()

	// powerStateBy lets tests drive per-VMID PowerState replies for the
	// power-state poller. Default (nil) returns "running" for any VMID,
	// matching the steady-state expectation of an Assigned/Running VM.
	powerStateBy map[int]string

	clones       []provisioner.CloneOptions
	destroys     []int
	starts       []int
	injects      []int
	listOwnedRet []*provisioner.VM
}

func (f *fakeProv) TemplateNode() string         { return "pve1" }
func (f *fakeProv) Client() *proxmox.Client      { return nil }
func (f *fakeProv) Ping(_ context.Context) error { return nil }

func (f *fakeProv) Clone(ctx context.Context, opts provisioner.CloneOptions) (*provisioner.VM, error) {
	if f.cloneDelay > 0 {
		select {
		case <-time.After(f.cloneDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clones = append(f.clones, opts)
	if f.cloneErr != nil {
		return nil, f.cloneErr
	}
	return &provisioner.VM{VMID: opts.NewVMID, Node: opts.Node, Name: opts.Name}, nil
}

func (f *fakeProv) Start(_ context.Context, v *provisioner.VM) error {
	f.mu.Lock()
	f.starts = append(f.starts, v.VMID)
	f.mu.Unlock()
	return f.startErr
}

func (f *fakeProv) Stop(_ context.Context, _ *provisioner.VM) error { return f.stopErr }

func (f *fakeProv) Destroy(ctx context.Context, v *provisioner.VM) error {
	f.mu.Lock()
	f.destroys = append(f.destroys, v.VMID)
	specific, ok := f.destroyErrFor[v.VMID]
	hang := f.destroyHang
	entered := f.destroyEntered
	onDestroy := f.onDestroy
	f.mu.Unlock()
	if entered != nil {
		select {
		case <-entered:
			// already closed
		default:
			close(entered)
		}
	}
	if onDestroy != nil {
		onDestroy()
	}
	if hang {
		<-ctx.Done()
		return ctx.Err()
	}
	if ok {
		return specific
	}
	return f.destroyErr
}

func (f *fakeProv) WaitReady(_ context.Context, _ *provisioner.VM, _ time.Duration) error {
	return f.waitErr
}

func (f *fakeProv) InjectJITConfig(_ context.Context, v *provisioner.VM, _ string) error {
	f.mu.Lock()
	f.injects = append(f.injects, v.VMID)
	f.mu.Unlock()
	return f.injectErr
}

func (f *fakeProv) ReadAgentFile(_ context.Context, _ *provisioner.VM, _ string) ([]byte, error) {
	return nil, nil
}

func (f *fakeProv) ListOwnedVMs(_ context.Context) ([]*provisioner.VM, error) {
	return f.listOwnedRet, f.listErr
}

func (f *fakeProv) PowerState(_ context.Context, v *provisioner.VM) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.powerStateBy[v.VMID]; ok {
		return s, nil
	}
	return "running", nil
}

// testWriter routes slog output to t.Log.
type testWriter struct{ t *testing.T }

func (tw testWriter) Write(p []byte) (int, error) {
	tw.t.Log(string(p))
	return len(p), nil
}

// ---------- Helpers ----------

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.New()
	require.NoError(t, err)
	return s
}

func newTestManager(t *testing.T, st *store.Store, prov provisioner.Provisioner, cfg Config) *manager {
	t.Helper()
	if cfg.MaxConcurrentRunners == 0 {
		cfg.MaxConcurrentRunners = 10
	}
	if cfg.ReconcileInterval == 0 {
		cfg.ReconcileInterval = 50 * time.Millisecond
	}
	if cfg.VMIDRange.Min == 0 {
		cfg.VMIDRange = config.VMIDRange{Min: 10000, Max: 19999}
	}
	if cfg.VMNamePrefix == "" {
		cfg.VMNamePrefix = "gh-runner-test-"
	}
	if cfg.TemplateNode == "" {
		cfg.TemplateNode = "pve1"
	}
	if cfg.BootMaxAttempts == 0 {
		cfg.BootMaxAttempts = 3
	}

	sel, err := nodeselector.NewSingle("pve1")
	require.NoError(t, err)

	metrics := observability.NewMetrics(prometheus.NewRegistry())
	var w io.Writer = io.Discard //nolint:staticcheck // explicit interface type required so reassignment to testWriter compiles
	if testing.Verbose() {
		w = testWriter{t}
	}
	log := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))
	mi, err := NewManager(cfg, st, prov, sel, log, metrics)
	require.NoError(t, err)
	return mi.(*manager)
}

func seedHot(t *testing.T, st *store.Store, count int) {
	t.Helper()
	for i := range count {
		err := st.Insert(&store.VM{
			VMID:     20000 + i,
			Node:     "pve1",
			Name:     "seed-hot",
			PoolKind: store.PoolKindHot,
			State:    store.StateHot,
		})
		require.NoError(t, err)
	}
}

// ---------- Tests ----------

func TestAcquire_PromotesHotToAssigned(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	seedHot(t, st, 1)
	mgr := newTestManager(t, st, &fakeProv{}, Config{HotSize: 1})

	got, err := mgr.Acquire(context.Background(), 4242)
	require.NoError(t, err)
	require.Equal(t, 20000, got.VMID)

	row, err := st.Get(20000)
	require.NoError(t, err)
	require.Equal(t, store.StateAssigned, row.State)
	require.Equal(t, int64(4242), row.JobID)
}

func TestAcquire_NoHotAvailable(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	mgr := newTestManager(t, st, &fakeProv{}, Config{HotSize: 1})

	_, err := mgr.Acquire(context.Background(), 1)
	require.ErrorIs(t, err, ErrNoneAvailable)
}

func TestAcquire_RaceOnlyOneWinner(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	seedHot(t, st, 1)
	mgr := newTestManager(t, st, &fakeProv{}, Config{HotSize: 1})

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		wins int
	)
	for i := range 16 {
		wg.Add(1)
		go func(jobID int64) {
			defer wg.Done()
			if _, err := mgr.Acquire(context.Background(), jobID); err == nil {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}(int64(100 + i))
	}
	wg.Wait()
	require.Equal(t, 1, wins)
}

func TestMarkRunning_AssignedToRunning(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	seedHot(t, st, 1)
	mgr := newTestManager(t, st, &fakeProv{}, Config{HotSize: 1})

	_, err := mgr.Acquire(context.Background(), 42)
	require.NoError(t, err)

	require.NoError(t, mgr.MarkRunning(context.Background(), 20000, 9999))
	row, err := st.Get(20000)
	require.NoError(t, err)
	require.Equal(t, store.StateRunning, row.State)
	require.Equal(t, int64(9999), row.RunnerID)
}

func TestMarkCompleted_DestroysAndSignals(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	seedHot(t, st, 1)
	fp := &fakeProv{}
	mgr := newTestManager(t, st, fp, Config{HotSize: 1})

	_, err := mgr.Acquire(context.Background(), 1)
	require.NoError(t, err)

	require.NoError(t, mgr.MarkCompleted(context.Background(), 20000))

	// Wait for the async destroy.
	require.Eventually(t, func() bool {
		fp.mu.Lock()
		defer fp.mu.Unlock()
		return len(fp.destroys) == 1
	}, time.Second, 10*time.Millisecond)

	// Row should be gone.
	_, err = st.Get(20000)
	require.ErrorIs(t, err, store.ErrNotFound)
}

// TestReconcile_ShrinksHotPoolToTarget verifies that when the hot pool
// has grown beyond HotSize (typically after a burst's demand collapses
// back to 0), the reconcile loop actively destroys the excess. Before
// this behavior was added, extras would sit idle until vm_max_age
// (default 24h) recycled them.
func TestReconcile_ShrinksHotPoolToTarget(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	seedHot(t, st, 5)
	fp := &fakeProv{}
	mgr := newTestManager(t, st, fp, Config{HotSize: 3, MaxConcurrentRunners: 10})

	mgr.SetDesiredCount(0)
	mgr.reconcileOnce(context.Background())

	require.Eventually(t, func() bool {
		fp.mu.Lock()
		defer fp.mu.Unlock()
		return len(fp.destroys) == 2
	}, time.Second, 10*time.Millisecond)

	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.Contains(t, fp.destroys, 20000)
	require.Contains(t, fp.destroys, 20001)
}

// TestReconcile_DoesNotShrinkBelowBurstTarget guards the dangerous race:
// when GitHub still wants more runners (desired > busy), the shrink path
// must NOT kill idle hot VMs that are about to be acquired.
func TestReconcile_DoesNotShrinkBelowBurstTarget(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	seedHot(t, st, 5)
	fp := &fakeProv{}
	mgr := newTestManager(t, st, fp, Config{HotSize: 3, MaxConcurrentRunners: 10})

	// GitHub wants 5 runners and none are busy yet → burst target is 5,
	// floor becomes max(HotSize=3, 5)=5 → no excess to destroy.
	mgr.SetDesiredCount(5)
	mgr.reconcileOnce(context.Background())

	time.Sleep(50 * time.Millisecond)

	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.Empty(t, fp.destroys, "must not shrink while burst demand exceeds HotSize")
}

// TestPromoteToRunning_FromAssigned: the listener missed JobStarted but the
// reconciler observed the runner as busy on GitHub. The catch-up must move
// the row Assigned -> Running and stamp the runner+job IDs.
func TestPromoteToRunning_FromAssigned(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	seedHot(t, st, 1)
	mgr := newTestManager(t, st, &fakeProv{}, Config{HotSize: 1})

	_, err := mgr.Acquire(context.Background(), 0)
	require.NoError(t, err)

	require.NoError(t, mgr.PromoteToRunning(context.Background(), 20000, 555, 9999))

	row, err := st.Get(20000)
	require.NoError(t, err)
	require.Equal(t, store.StateRunning, row.State)
	require.Equal(t, int64(555), row.RunnerID)
	require.Equal(t, int64(9999), row.JobID)
}

// TestPromoteToRunning_FromHot covers the rarer race: GitHub assigned the
// job before our local Hot -> Assigned ran.
func TestPromoteToRunning_FromHot(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	seedHot(t, st, 1)
	mgr := newTestManager(t, st, &fakeProv{}, Config{HotSize: 1})

	require.NoError(t, mgr.PromoteToRunning(context.Background(), 20000, 700, 0))

	row, err := st.Get(20000)
	require.NoError(t, err)
	require.Equal(t, store.StateRunning, row.State)
}

// TestPromoteToRunning_NoopOnRunning: calling promote on a row already in
// Running (duplicate signal) must be a clean no-op, not an error.
func TestPromoteToRunning_NoopOnRunning(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	seedHot(t, st, 1)
	mgr := newTestManager(t, st, &fakeProv{}, Config{HotSize: 1})

	_, err := mgr.Acquire(context.Background(), 0)
	require.NoError(t, err)
	require.NoError(t, mgr.PromoteToRunning(context.Background(), 20000, 1, 1))
	require.NoError(t, mgr.PromoteToRunning(context.Background(), 20000, 1, 1))

	row, err := st.Get(20000)
	require.NoError(t, err)
	require.Equal(t, store.StateRunning, row.State)
}

// TestForceDestroy_FromAssigned simulates the production bug: a row
// stuck in Assigned because the runner never picked up the job. The
// reconciler force-destroys.
func TestForceDestroy_FromAssigned(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	seedHot(t, st, 1)
	fp := &fakeProv{}
	mgr := newTestManager(t, st, fp, Config{HotSize: 1})

	_, err := mgr.Acquire(context.Background(), 0)
	require.NoError(t, err)

	require.NoError(t, mgr.ForceDestroy(context.Background(), 20000, "test: stuck assigned"))

	require.Eventually(t, func() bool {
		fp.mu.Lock()
		defer fp.mu.Unlock()
		return len(fp.destroys) == 1
	}, time.Second, 10*time.Millisecond)
}

// TestForceDestroy_MissingRowIsNoop must not error on rows that the
// reconciler already cleaned up between its scan and the action.
func TestForceDestroy_MissingRowIsNoop(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	mgr := newTestManager(t, st, &fakeProv{}, Config{HotSize: 0, MaxConcurrentRunners: 1})

	require.NoError(t, mgr.ForceDestroy(context.Background(), 99999, "test: missing"))
}

// TestListRows_ExcludesTerminal: the reconciler must not waste a GH API
// call inspecting rows that are already on their way out.
func TestListRows_ExcludesTerminal(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	seedHot(t, st, 2)
	mgr := newTestManager(t, st, &fakeProv{}, Config{HotSize: 2})

	// Put one row into Draining; the other stays Hot.
	_, err := st.Update(20000, func(v *store.VM) {
		v.State = store.StateDraining
		v.StateSince = time.Now()
	})
	require.NoError(t, err)

	rows, err := mgr.ListRows(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, 20001, rows[0].VMID)
	require.Equal(t, "hot", rows[0].State)
	require.False(t, rows[0].StateSince.IsZero())
}

func TestAllocateVMID_AvoidsCollisions(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	seedHot(t, st, 1) // vmid 20000
	mgr := newTestManager(t, st, &fakeProv{}, Config{
		VMIDRange: config.VMIDRange{Min: 19999, Max: 20002},
	})

	// 19999 is free, 20000 is used.
	id, err := mgr.allocateVMID(context.Background())
	require.NoError(t, err)
	require.Equal(t, 19999, id)

	// Seed 19999 too and check we advance to 20001.
	require.NoError(t, st.Insert(&store.VM{
		VMID:     19999,
		Node:     "pve1",
		Name:     "x",
		PoolKind: store.PoolKindWarm,
		State:    store.StateWarm,
	}))

	id, err = mgr.allocateVMID(context.Background())
	require.NoError(t, err)
	require.Equal(t, 20001, id)
}

func TestAllocateVMID_RangeExhausted(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	mgr := newTestManager(t, st, &fakeProv{}, Config{
		VMIDRange: config.VMIDRange{Min: 30000, Max: 30000},
	})
	require.NoError(t, st.Insert(&store.VM{
		VMID:     30000,
		Node:     "pve1",
		Name:     "only",
		PoolKind: store.PoolKindHot,
		State:    store.StateHot,
	}))

	_, err := mgr.allocateVMID(context.Background())
	require.Error(t, err)
}

func TestReconcile_ClonesToFillHot(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{}
	mgr := newTestManager(t, st, fp, Config{HotSize: 2, WarmSize: 0})

	mgr.reconcileOnce(context.Background())
	// Two hot clones should be in flight; wait for them to finish.
	require.Eventually(t, func() bool {
		fp.mu.Lock()
		defer fp.mu.Unlock()
		return len(fp.clones) == 2
	}, 2*time.Second, 10*time.Millisecond)

	// Each clone should have PoweredOn=true (hot path).
	fp.mu.Lock()
	defer fp.mu.Unlock()
	for _, c := range fp.clones {
		require.True(t, c.PoweredOn, "hot-pool clones must be powered on")
	}
}

func TestReconcile_ClonesToFillWarm(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{}
	mgr := newTestManager(t, st, fp, Config{HotSize: 0, WarmSize: 3})

	mgr.reconcileOnce(context.Background())
	require.Eventually(t, func() bool {
		fp.mu.Lock()
		defer fp.mu.Unlock()
		return len(fp.clones) == 3
	}, 2*time.Second, 10*time.Millisecond)

	fp.mu.Lock()
	defer fp.mu.Unlock()
	for _, c := range fp.clones {
		require.False(t, c.PoweredOn, "warm-pool clones must be powered off")
	}
}

func TestReconcile_PromotesWarmToHot(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	// Pre-seed one warm VM.
	require.NoError(t, st.Insert(&store.VM{
		VMID:     11000,
		Node:     "pve1",
		Name:     "seed-warm",
		PoolKind: store.PoolKindWarm,
		State:    store.StateWarm,
	}))

	fp := &fakeProv{}
	mgr := newTestManager(t, st, fp, Config{HotSize: 1, WarmSize: 0})

	mgr.reconcileOnce(context.Background())

	// Promotion should call Start + WaitReady (the fake returns nil for both),
	// and the row should end up Hot.
	require.Eventually(t, func() bool {
		row, err := st.Get(11000)
		if err != nil {
			return false
		}
		return row.State == store.StateHot
	}, 2*time.Second, 10*time.Millisecond)

	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.Contains(t, fp.starts, 11000)
	// No NEW clones — we used the warm one we had.
	require.Empty(t, fp.clones)
}

func TestReconcile_PoisonAfterMaxBootAttempts(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, st.Insert(&store.VM{
		VMID:         11000,
		Node:         "pve1",
		Name:         "warm-broken",
		PoolKind:     store.PoolKindWarm,
		State:        store.StateWarm,
		BootAttempts: 2, // one more failure -> poison
	}))

	fp := &fakeProv{waitErr: errors.New("agent timeout")}
	mgr := newTestManager(t, st, fp, Config{HotSize: 1, WarmSize: 0, BootMaxAttempts: 3})

	mgr.reconcileOnce(context.Background())

	require.Eventually(t, func() bool {
		row, err := st.Get(11000)
		return err == nil && row.State == store.StatePoison
	}, 2*time.Second, 10*time.Millisecond)
}

// TestRecover_DestroysOrphanProxmoxVMs: at startup the (empty) in-memory
// store has no rows, so every Proxmox VM tagged as ours is an orphan from
// a previous process and must be destroyed.
func TestRecover_DestroysOrphanProxmoxVMs(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{
		listOwnedRet: []*provisioner.VM{{VMID: 12345, Node: "pve1", Name: "orphan"}},
	}
	mgr := newTestManager(t, st, fp, Config{})

	require.NoError(t, mgr.Recover(context.Background()))

	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.Contains(t, fp.destroys, 12345)
}

// TestRecover_MultipleOrphansAllDestroyed: a previous process can leak
// several VMs across nodes; recover must destroy every one.
func TestRecover_MultipleOrphansAllDestroyed(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{
		listOwnedRet: []*provisioner.VM{
			{VMID: 10001, Node: "pve1", Name: "orphan-1"},
			{VMID: 10002, Node: "pve2", Name: "orphan-2"},
			{VMID: 10003, Node: "pve1", Name: "orphan-3"},
		},
	}
	mgr := newTestManager(t, st, fp, Config{})

	require.NoError(t, mgr.Recover(context.Background()))

	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.ElementsMatch(t, []int{10001, 10002, 10003}, fp.destroys)
}

// TestRecover_NoProxmoxVMsIsNoop: a clean startup (no leaked VMs) must
// neither destroy anything nor return an error.
func TestRecover_NoProxmoxVMsIsNoop(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{listOwnedRet: nil}
	mgr := newTestManager(t, st, fp, Config{})

	require.NoError(t, mgr.Recover(context.Background()))

	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.Empty(t, fp.destroys)
}

// TestRecover_PropagatesListError: a Proxmox API failure on startup must
// surface as an error so the operator sees something concrete instead of
// the orchestrator silently starting with a stale view.
func TestRecover_PropagatesListError(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{listErr: errors.New("proxmox down")}
	mgr := newTestManager(t, st, fp, Config{})

	err := mgr.Recover(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "proxmox down")
}

// TestRecover_PartialFailureReturnsAggregatedError: even ONE orphan that
// fails to destroy makes Recover return a non-nil error. The orchestrator
// would otherwise start cloning fresh VMs on top of leaked ones — exactly
// the resource exhaustion Recover exists to prevent.
//
// Successful destroys are still applied (we don't roll back); the error
// just signals "the post-condition isn't fully met".
func TestRecover_PartialFailureReturnsAggregatedError(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	boom := errors.New("proxmox 500 on node-B")
	fp := &fakeProv{
		listOwnedRet: []*provisioner.VM{
			{VMID: 10001, Node: "pve1", Name: "ok-1"},
			{VMID: 10002, Node: "pve2", Name: "broken"},
			{VMID: 10003, Node: "pve1", Name: "ok-2"},
		},
		destroyErrFor: map[int]error{10002: boom},
	}
	mgr := newTestManager(t, st, fp, Config{})

	err := mgr.Recover(context.Background())
	require.Error(t, err)
	// Operator-facing message names how many failed.
	require.Contains(t, err.Error(), "1 of 3")
	// The underlying failure is wrapped, not flattened, so errors.Is
	// against the original sentinel still works.
	require.ErrorIs(t, err, boom)
	// The healthy orphans were still destroyed.
	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.ElementsMatch(t, []int{10001, 10002, 10003}, fp.destroys)
}

// TestRecover_AllOrphansFailReportsAll: all orphans failing must surface
// every underlying error so an operator dumping the log sees the full
// scope of the leak.
func TestRecover_AllOrphansFailReportsAll(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{
		listOwnedRet: []*provisioner.VM{
			{VMID: 10001, Node: "pve1"},
			{VMID: 10002, Node: "pve1"},
		},
		destroyErr: errors.New("proxmox unreachable"),
	}
	mgr := newTestManager(t, st, fp, Config{})

	err := mgr.Recover(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "2 of 2")
	require.Contains(t, err.Error(), "vmid=10001")
	require.Contains(t, err.Error(), "vmid=10002")
}

// TestAcquire_OldestHotFirst: when multiple Hot VMs are present, Acquire
// must pick the oldest (closest to max-age recycle) so we don't carry
// stale VMs forever.
func TestAcquire_OldestHotFirst(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	// Insert in reverse age order so we can prove ordering is by CreatedAt,
	// not insertion order.
	now := time.Now()
	require.NoError(t, st.Insert(&store.VM{
		VMID: 20100, Node: "pve1", Name: "newer",
		PoolKind: store.PoolKindHot, State: store.StateHot,
		CreatedAt: now.Add(-1 * time.Minute),
	}))
	require.NoError(t, st.Insert(&store.VM{
		VMID: 20101, Node: "pve1", Name: "oldest",
		PoolKind: store.PoolKindHot, State: store.StateHot,
		CreatedAt: now.Add(-10 * time.Minute),
	}))
	require.NoError(t, st.Insert(&store.VM{
		VMID: 20102, Node: "pve1", Name: "middle",
		PoolKind: store.PoolKindHot, State: store.StateHot,
		CreatedAt: now.Add(-5 * time.Minute),
	}))

	mgr := newTestManager(t, st, &fakeProv{}, Config{HotSize: 3})
	got, err := mgr.Acquire(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 20101, got.VMID, "oldest Hot must be acquired first")
}

// TestReconcile_StuckProvisioningSweptToDestroying: a row stuck in a
// Proxmox-side transient state past the grace window must be force-drained
// and queued for destroy. This is the self-healing path that protects the
// orchestrator against a one-time Proxmox API blip.
func TestReconcile_StuckProvisioningSweptToDestroying(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	stale := time.Now().Add(-10 * time.Minute) // well past the 5-minute grace
	require.NoError(t, st.Insert(&store.VM{
		VMID: 12100, Node: "pve1", Name: "stuck",
		PoolKind: store.PoolKindHot, State: store.StateProvisioning,
		CreatedAt: stale, UpdatedAt: stale, StateSince: stale,
	}))

	fp := &fakeProv{}
	mgr := newTestManager(t, st, fp, Config{HotSize: 0, WarmSize: 0})
	mgr.reconcileOnce(context.Background())

	require.Eventually(t, func() bool {
		fp.mu.Lock()
		defer fp.mu.Unlock()
		for _, v := range fp.destroys {
			if v == 12100 {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond)
}

// TestReconcile_MaxAgeRecyclesIdleVMs: when vm_max_age is set, idle
// Hot/Warm VMs older than the cutoff must be recycled (drained + destroyed)
// so the pool doesn't accumulate stale runners.
func TestReconcile_MaxAgeRecyclesIdleVMs(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	old := time.Now().Add(-30 * time.Minute)
	require.NoError(t, st.Insert(&store.VM{
		VMID: 12200, Node: "pve1", Name: "ancient-hot",
		PoolKind: store.PoolKindHot, State: store.StateHot,
		CreatedAt: old, UpdatedAt: old, StateSince: old,
	}))

	fp := &fakeProv{}
	mgr := newTestManager(t, st, fp, Config{
		HotSize:  0,
		WarmSize: 0,
		VMMaxAge: 5 * time.Minute, // ancient-hot is far past this
	})
	mgr.reconcileOnce(context.Background())

	require.Eventually(t, func() bool {
		fp.mu.Lock()
		defer fp.mu.Unlock()
		for _, v := range fp.destroys {
			if v == 12200 {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond)
}

// TestListRows_ZeroJobIDIsSerialised: the int64-with-0-sentinel boundary
// representation (replacing the previous *int64) must round-trip through
// ListRows unchanged so the GitHub reconciler sees a usable value.
func TestListRows_PreservesJobAndRunnerIDs(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, st.Insert(&store.VM{
		VMID: 12300, Node: "pve1", Name: "with-ids",
		PoolKind: store.PoolKindHot, State: store.StateRunning,
		JobID: 42, RunnerID: 9999,
	}))
	require.NoError(t, st.Insert(&store.VM{
		VMID: 12301, Node: "pve1", Name: "no-ids",
		PoolKind: store.PoolKindHot, State: store.StateHot,
	}))

	mgr := newTestManager(t, st, &fakeProv{}, Config{HotSize: 2})
	rows, err := mgr.ListRows(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 2)

	byID := map[int]RowSnapshot{}
	for _, r := range rows {
		byID[r.VMID] = r
	}
	require.Equal(t, int64(42), byID[12300].JobID)
	require.Equal(t, int64(9999), byID[12300].RunnerID)
	require.Equal(t, int64(0), byID[12301].JobID, "unset job_id round-trips as 0")
	require.Equal(t, int64(0), byID[12301].RunnerID)
}

func TestSignalRefill_Coalesces(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	mgr := newTestManager(t, st, &fakeProv{}, Config{})

	for range 100 {
		mgr.SignalRefill() // must never block
	}
	// Drain the one signal we coalesced into.
	<-mgr.refill
	select {
	case <-mgr.refill:
		t.Fatal("expected exactly one signal after coalesce, got more")
	default:
	}
}

// TestDrain_ForceCancelsInFlightOnTimeout: a destroy stuck on an
// unreachable Proxmox node must not be able to pin the process past
// DrainTimeout. drain() observes the wg-wait timeout, force-cancels the
// worker context, and the in-flight Destroy unwinds via ctx.Err.
//
// This is the load-bearing test for the manager-scoped context plumbing
// — without workerCtx threaded through, Destroy would still be running
// against context.Background and drain would have no way to escalate.
func TestDrain_ForceCancelsInFlightOnTimeout(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	entered := make(chan struct{})
	fp := &fakeProv{destroyHang: true, destroyEntered: entered}

	// Seed an Assigned row so MarkCompleted has something to destroy.
	require.NoError(t, st.Insert(&store.VM{
		VMID:     12345,
		Node:     "pve1",
		Name:     "stuck",
		PoolKind: store.PoolKindHot,
		State:    store.StateAssigned,
	}))

	// Generous-enough DrainTimeout for slow CI (200ms) but short enough
	// to keep the test fast.
	mgr := newTestManager(t, st, fp, Config{
		DrainTimeout: 200 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- mgr.Run(ctx)
	}()

	// Queue a destroy via the public surface — this exercises the
	// MarkCompleted path that previously used context.Background().
	require.NoError(t, mgr.MarkCompleted(context.Background(), 12345))

	// Wait for Destroy to actually start (i.e. the goroutine is in the
	// `<-ctx.Done()` wait).
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Destroy never entered")
	}

	// Trigger drain.
	start := time.Now()
	cancel()
	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after drain timeout + worker cancel")
	}
	// Should complete around DrainTimeout (+ postCancelGrace, but Destroy
	// unwinds immediately on ctx.Done so we expect well under 1s).
	elapsed := time.Since(start)
	require.Less(t, elapsed, 1*time.Second,
		"drain took %s; expected timeout-triggered worker cancel to be near-instant", elapsed)
	require.GreaterOrEqual(t, elapsed, 200*time.Millisecond,
		"drain returned before DrainTimeout elapsed")
}

// TestMarkCompleted_RefusesNonBusyState ensures a stray runner-hook
// "completed" event for a Hot/Warm row doesn't trigger destruction.
// Critical security property: the runner-hook can mark Assigned/Running
// VMs done, nothing else.
func TestMarkCompleted_RefusesNonBusyState(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, st.Insert(&store.VM{
		VMID:     60001,
		Node:     "pve1",
		PoolKind: store.PoolKindHot,
		State:    store.StateHot,
	}))
	fp := &fakeProv{}
	mgr := newTestManager(t, st, fp, Config{})

	require.NoError(t, mgr.MarkCompleted(context.Background(), 60001))
	// Row stayed Hot (no transition), no destroy queued.
	row, err := st.Get(60001)
	require.NoError(t, err)
	require.Equal(t, store.StateHot, row.State)
	// Give async work a moment in case the goroutine fired.
	time.Sleep(50 * time.Millisecond)
	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.Empty(t, fp.destroys)
}

// TestMarkCompleted_IdempotentOnDraining: a duplicate runner-hook event
// for a row already in Draining/Destroying must be a no-op — no second
// destroy goroutine queued.
func TestMarkCompleted_IdempotentOnDraining(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, st.Insert(&store.VM{
		VMID:     60002,
		Node:     "pve1",
		PoolKind: store.PoolKindHot,
		State:    store.StateDraining,
	}))
	fp := &fakeProv{}
	mgr := newTestManager(t, st, fp, Config{})

	require.NoError(t, mgr.MarkCompleted(context.Background(), 60002))
	time.Sleep(50 * time.Millisecond)
	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.Empty(t, fp.destroys, "MarkCompleted on Draining row must not queue another destroy")
}

// TestDestroyAsync_PanicInProvisionerDoesNotKillProcess: a panic inside
// the Proxmox library (nil-deref, race, etc.) used to crash the whole
// orchestrator. The recoverPanic guard now contains it — Run continues,
// wg.Done still fires, and an operator sees the panic in logs instead
// of a process exit.
func TestDestroyAsync_PanicInProvisionerDoesNotKillProcess(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{onDestroy: func() { panic("simulated nil deref inside go-proxmox") }}

	mgr := newTestManager(t, st, fp, Config{DrainTimeout: 1 * time.Second})

	// Drive destroyAsync directly — it spawns a goroutine, panic must
	// be contained by recoverPanic.
	mgr.destroyAsync(50001, "pve1")

	// wg.Done should still fire (deferred in the goroutine), so a
	// Wait completes promptly.
	done := make(chan struct{})
	go func() {
		mgr.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("wg.Wait blocked — recoverPanic likely didn't run / wg.Done missed")
	}
}

// TestDestroyAsync_BoundedByDestroySem: a burst of destroys must respect
// the destroy semaphore — at any instant no more than maxConcurrentDestroys
// (=8) goroutines should be inside prov.Destroy.
//
// We model a slow destroy with destroyHang + an atomic in-flight counter
// the fake bumps on entry. After kicking 50 destroys we observe the max
// observed in-flight count and require it stayed at the cap.
func TestDestroyAsync_BoundedByDestroySem(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	var inFlight, peakInFlight atomic.Int32
	release := make(chan struct{})
	fp := &fakeProv{
		onDestroy: func() {
			cur := inFlight.Add(1)
			for {
				prev := peakInFlight.Load()
				if cur <= prev || peakInFlight.CompareAndSwap(prev, cur) {
					break
				}
			}
			<-release // block until the test lets us go
			inFlight.Add(-1)
		},
	}
	mgr := newTestManager(t, st, fp, Config{
		DrainTimeout: 5 * time.Second,
	})

	// Spawn 50 destroys directly (the semaphore is what we're testing).
	for i := range 50 {
		mgr.destroyAsync(99000+i, "pve1")
	}
	// Give the goroutines a moment to all hit the semaphore.
	require.Eventually(t, func() bool {
		return inFlight.Load() == 8
	}, 2*time.Second, 5*time.Millisecond,
		"in-flight should saturate at the destroy semaphore cap")

	require.LessOrEqual(t, peakInFlight.Load(), int32(8),
		"peak in-flight (%d) must not exceed destroy-sem cap (8)", peakInFlight.Load())

	// Release everyone.
	close(release)
	// Drain wg via the public surface.
	mgr.workerCancel()
	mgr.wg.Wait()
}

// TestMarkCompleted_RespectsDestroySemaphore: a burst of MarkCompleted
// calls (e.g., end-of-CI run with many jobs finishing nearly
// simultaneously) must not spawn more concurrent Destroy calls than the
// destroy semaphore permits. Previously MarkCompleted called destroy()
// directly via `go m.destroy(...)`, bypassing destroySem; under burst,
// the orchestrator could hammer Proxmox with N (e.g. 50) parallel
// destroys.
func TestMarkCompleted_RespectsDestroySemaphore(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	const want = 20 // > maxConcurrentDestroys (8)
	for i := range want {
		require.NoError(t, st.Insert(&store.VM{
			VMID:     72000 + i,
			Node:     "pve1",
			Name:     "burst",
			PoolKind: store.PoolKindHot,
			State:    store.StateRunning,
		}))
	}

	var inFlight, peak atomic.Int32
	release := make(chan struct{})
	fp := &fakeProv{
		onDestroy: func() {
			cur := inFlight.Add(1)
			for {
				prev := peak.Load()
				if cur <= prev || peak.CompareAndSwap(prev, cur) {
					break
				}
			}
			<-release
			inFlight.Add(-1)
		},
	}
	mgr := newTestManager(t, st, fp, Config{DrainTimeout: 5 * time.Second})

	for i := range want {
		require.NoError(t, mgr.MarkCompleted(context.Background(), 72000+i))
	}

	// Let the goroutines saturate the semaphore.
	require.Eventually(t, func() bool {
		return inFlight.Load() == 8
	}, 2*time.Second, 5*time.Millisecond,
		"expected in-flight to saturate at destroy-semaphore cap (8)")

	require.LessOrEqual(t, peak.Load(), int32(8),
		"peak in-flight destroys (%d) must not exceed destroy-sem cap (8)", peak.Load())

	close(release)
	mgr.workerCancel()
	mgr.wg.Wait()
}

// TestPowerPoller_DestroysStoppedRunningVM: a row in Running state whose
// Proxmox VM is observed "stopped" (the in-VM runner powered off after
// the job) must be queued for destruction by the poller. This is the
// replacement for the in-VM runner-hook completed callback.
func TestPowerPoller_DestroysStoppedRunningVM(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, st.Insert(&store.VM{
		VMID:     71001,
		Node:     "pve1",
		Name:     "stopped-on-poll",
		PoolKind: store.PoolKindHot,
		State:    store.StateRunning,
	}))
	fp := &fakeProv{powerStateBy: map[int]string{71001: "stopped"}}
	mgr := newTestManager(t, st, fp, Config{})

	mgr.powerPollOnce(context.Background())

	// MarkCompleted transitions the row to Draining and queues destroy.
	require.Eventually(t, func() bool {
		fp.mu.Lock()
		defer fp.mu.Unlock()
		for _, v := range fp.destroys {
			if v == 71001 {
				return true
			}
		}
		return false
	}, time.Second, 10*time.Millisecond, "stopped VM must be queued for destruction")
}

// TestPowerPoller_NoopOnRunning: a Running row whose VM reports
// "running" is left alone — the poller is the completion signal, not a
// general health probe.
func TestPowerPoller_NoopOnRunning(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, st.Insert(&store.VM{
		VMID:     71002,
		Node:     "pve1",
		Name:     "still-running",
		PoolKind: store.PoolKindHot,
		State:    store.StateRunning,
	}))
	fp := &fakeProv{} // default returns "running"
	mgr := newTestManager(t, st, fp, Config{})

	mgr.powerPollOnce(context.Background())

	// Give any spurious goroutine time to run.
	time.Sleep(50 * time.Millisecond)

	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.Empty(t, fp.destroys, "running VM must NOT be queued for destruction")
}

// TestPowerPoller_IgnoresHotAndWarmRows: the poller only acts on
// Assigned/Running rows. Hot/Warm/Booting/Provisioning VMs are managed
// by the reconciler's own state machine, and a "stopped" status there is
// often normal (Warm rows are stopped by design).
func TestPowerPoller_IgnoresHotAndWarmRows(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, st.Insert(&store.VM{
		VMID: 71010, Node: "pve1", Name: "warm-stopped",
		PoolKind: store.PoolKindWarm, State: store.StateWarm,
	}))
	require.NoError(t, st.Insert(&store.VM{
		VMID: 71011, Node: "pve1", Name: "hot-running",
		PoolKind: store.PoolKindHot, State: store.StateHot,
	}))
	fp := &fakeProv{powerStateBy: map[int]string{
		71010: "stopped",
		71011: "stopped",
	}}
	mgr := newTestManager(t, st, fp, Config{})

	mgr.powerPollOnce(context.Background())
	time.Sleep(50 * time.Millisecond)

	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.Empty(t, fp.destroys, "poller must not act on Hot/Warm rows")
}

// TestSetRunnerID_StampsRowField: the scaler stamps runner_id on the row
// immediately after GenerateJitRunnerConfig so a sub-15s job that
// completes before the gh.Reconciler observes the runner still has a
// runner_id available for OnRunnerOrphaned.
func TestSetRunnerID_StampsRowField(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, st.Insert(&store.VM{
		VMID: 71020, Node: "pve1", Name: "fresh-assigned",
		PoolKind: store.PoolKindHot, State: store.StateAssigned,
	}))
	mgr := newTestManager(t, st, &fakeProv{}, Config{})

	require.NoError(t, mgr.SetRunnerID(context.Background(), 71020, 12345))

	row, err := st.Get(71020)
	require.NoError(t, err)
	require.Equal(t, int64(12345), row.RunnerID)
	require.Equal(t, store.StateAssigned, row.State, "state must not change")
}

// TestSetRunnerID_NoopOnMissingRow: a runner_id stamp for a vmid that
// has already been destroyed (rare end-of-job race) must be a clean
// no-op, not an error.
func TestSetRunnerID_NoopOnMissingRow(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	mgr := newTestManager(t, st, &fakeProv{}, Config{})

	require.NoError(t, mgr.SetRunnerID(context.Background(), 99999, 42))
}

// TestDrain_CompletesNaturallyWhenWorkersFinish: when destroys finish on
// their own, drain returns immediately without escalating to a force
// cancel. The escalation path is an emergency; the happy path stays fast.
func TestDrain_CompletesNaturallyWhenWorkersFinish(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{} // Destroy returns nil immediately

	require.NoError(t, st.Insert(&store.VM{
		VMID:     12346,
		Node:     "pve1",
		Name:     "happy",
		PoolKind: store.PoolKindHot,
		State:    store.StateAssigned,
	}))

	mgr := newTestManager(t, st, fp, Config{
		DrainTimeout: 5 * time.Second, // generous; we expect to never hit it
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- mgr.Run(ctx)
	}()

	require.NoError(t, mgr.MarkCompleted(context.Background(), 12346))

	start := time.Now()
	cancel()
	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	require.Less(t, time.Since(start), 500*time.Millisecond,
		"drain should complete immediately when workers finish on their own")
}
