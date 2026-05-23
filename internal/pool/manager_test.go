package pool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luthermonson/go-proxmox"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/config"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/nodeselector"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/observability"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/provisioner"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/store"
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

	// onClone, when set, is invoked synchronously inside Clone after
	// recording the call but BEFORE returning the result. Used by
	// row-deleted-mid-clone tests to delete a store row at exactly the
	// race window that produced the bug.
	onClone func(opts provisioner.CloneOptions)

	// powerStateBy lets tests drive per-VMID PowerState replies for the
	// power-state poller. Default (nil) returns "running" for any VMID,
	// matching the steady-state expectation of an Assigned/Running VM.
	powerStateBy map[int]string

	// powerStateErrBy lets tests inject per-VMID PowerState errors so
	// adopt tests can exercise the "power query failed" fallback path.
	powerStateErrBy map[int]error

	// powerStateHangBy, when true for a VMID, makes PowerState block
	// until the caller's ctx is cancelled — modelling a stuck Proxmox
	// node. The fake then returns ctx.Err so the poller can move on.
	powerStateHangBy map[int]bool

	// recentlyDestroyedSet drives IsRecentlyDestroyed. Tests set
	// membership directly; the fake ignores the cooldown arg and just
	// consults the set, so toggling membership models "time advanced
	// past the cooldown."
	recentlyDestroyedSet map[int]bool

	// inFlightClones drives InFlightCloneCount.
	inFlightClones int

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
	f.clones = append(f.clones, opts)
	cloneErr := f.cloneErr
	hook := f.onClone
	f.mu.Unlock()
	if cloneErr != nil {
		return nil, cloneErr
	}
	if hook != nil {
		hook(opts)
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

func (f *fakeProv) ReadJITConfig(_ context.Context, _ *provisioner.VM) ([]byte, error) {
	return nil, nil
}

func (f *fakeProv) ListOwnedVMs(_ context.Context) ([]*provisioner.VM, error) {
	return f.listOwnedRet, f.listErr
}

func (f *fakeProv) PowerState(ctx context.Context, v *provisioner.VM) (string, error) {
	f.mu.Lock()
	hang := f.powerStateHangBy[v.VMID]
	f.mu.Unlock()
	if hang {
		<-ctx.Done()
		return "", ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.powerStateErrBy[v.VMID]; ok {
		return "", err
	}
	if s, ok := f.powerStateBy[v.VMID]; ok {
		return s, nil
	}
	return "running", nil
}

// IsRecentlyDestroyed returns whatever the test seeded into
// recentlyDestroyedSet. The cooldown arg is ignored — tests model
// "advance past the cooldown" by toggling map membership.
func (f *fakeProv) IsRecentlyDestroyed(vmid int, _ time.Duration) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.recentlyDestroyedSet[vmid]
}

// InFlightCloneCount returns the value the test seeded.
func (f *fakeProv) InFlightCloneCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inFlightClones
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

func seedWarm(t *testing.T, st *store.Store, count int) {
	t.Helper()
	for i := range count {
		err := st.Insert(&store.VM{
			VMID:     30000 + i,
			Node:     "pve1",
			Name:     "seed-warm",
			PoolKind: store.PoolKindWarm,
			State:    store.StateWarm,
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

	// Wait for the async destroy AND the follow-up row delete.
	// fp.destroys is appended before st.Delete runs, so polling only
	// on the destroy count produced a flaky race in CI where Get(20000)
	// hit the store before the deletion landed.
	require.Eventually(t, func() bool {
		fp.mu.Lock()
		destroys := len(fp.destroys)
		fp.mu.Unlock()
		if destroys != 1 {
			return false
		}
		_, err := st.Get(20000)
		return errors.Is(err, store.ErrNotFound)
	}, time.Second, 10*time.Millisecond)
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

// TestPromoteN_SaturatedBootSemLeavesRowsWarm locks in the #68 fix:
// when bootSem is fully reserved, promoteN must leave Warm rows alone
// instead of CAS'ing them to Booting and then rolling back. The old
// behavior briefly flipped rows to (Booting, PoolKindHot) — which the
// reconciler counts as Available — and rolled them back in a goroutine,
// under-provisioning by one for the racing tick.
func TestPromoteN_SaturatedBootSemLeavesRowsWarm(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	seedWarm(t, st, 3)
	mgr := newTestManager(t, st, &fakeProv{}, Config{HotSize: 3})

	// Pre-saturate bootSem (capacity 16) so TryAcquire fails for every
	// candidate. Real semaphore.Weighted, real reservation — no mock.
	require.True(t, mgr.bootSem.TryAcquire(16),
		"test setup: must be able to drain the entire bootSem budget")
	defer mgr.bootSem.Release(16)

	mgr.promoteN(context.Background(), 3)

	// No goroutines were spawned, so nothing to wait for.
	for vmid := 30000; vmid < 30003; vmid++ {
		row, err := st.Get(vmid)
		require.NoError(t, err)
		require.Equal(t, store.StateWarm, row.State,
			"vmid %d must remain Warm when bootSem is saturated", vmid)
		require.Equal(t, store.PoolKindWarm, row.PoolKind,
			"vmid %d must remain PoolKindWarm (not transiently Hot)", vmid)
	}
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

// TestAdopt_PoweredOff_BecomesWarm: a stopped owner-tagged VM is adopted
// into the warm pool. Adopting (not destroying) is the load-bearing
// invariant of the leader-takeover scenario: an in-progress job on a
// warm slot must survive the handover.
func TestAdopt_PoweredOff_BecomesWarm(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{
		listOwnedRet: []*provisioner.VM{{VMID: 12345, Node: "pve1", Name: "gh-runner-test-12345"}},
		powerStateBy: map[int]string{12345: "stopped"},
	}
	mgr := newTestManager(t, st, fp, Config{})

	require.NoError(t, mgr.Adopt(context.Background()))

	row, err := st.Get(12345)
	require.NoError(t, err)
	require.Equal(t, store.StateWarm, row.State)
	require.Equal(t, store.PoolKindWarm, row.PoolKind)
	require.Zero(t, row.RunnerID)

	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.Empty(t, fp.destroys, "adopt must not destroy any VM")
}

// TestAdopt_PoweredOn_NoRunner_BecomesHot: a powered-on VM with no
// matching GitHub runner is adopted as Hot — the reconciler treats this
// as the normal pre-JIT state.
func TestAdopt_PoweredOn_NoRunner_BecomesHot(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{
		listOwnedRet: []*provisioner.VM{{VMID: 10001, Node: "pve1", Name: "gh-runner-test-10001"}},
		powerStateBy: map[int]string{10001: "running"},
	}
	cfg := Config{RunnerLister: func(context.Context) (map[string]RunnerInfo, error) {
		return map[string]RunnerInfo{}, nil
	}}
	mgr := newTestManager(t, st, fp, cfg)

	require.NoError(t, mgr.Adopt(context.Background()))

	row, err := st.Get(10001)
	require.NoError(t, err)
	require.Equal(t, store.StateHot, row.State)
	require.Equal(t, store.PoolKindHot, row.PoolKind)
	require.Zero(t, row.RunnerID)
}

// TestAdopt_BusyRunner_BecomesRunning: a powered-on VM whose runner is
// busy on GitHub is adopted directly as Running with the right RunnerID.
// This is the critical job-preservation path: the new leader's power-poll
// will then watch the VM and trigger MarkCompleted when the runner powers
// off — exactly the steady-state job-completion flow.
func TestAdopt_BusyRunner_BecomesRunning(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	const vmid = 10002
	const runnerID int64 = 7890
	name := fmt.Sprintf("gh-runner-test-%d", vmid)
	fp := &fakeProv{
		listOwnedRet: []*provisioner.VM{{VMID: vmid, Node: "pve1", Name: name}},
		powerStateBy: map[int]string{vmid: "running"},
	}
	cfg := Config{RunnerLister: func(context.Context) (map[string]RunnerInfo, error) {
		return map[string]RunnerInfo{
			name: {ID: runnerID, Online: true, Busy: true},
		}, nil
	}}
	mgr := newTestManager(t, st, fp, cfg)

	require.NoError(t, mgr.Adopt(context.Background()))

	row, err := st.Get(vmid)
	require.NoError(t, err)
	require.Equal(t, store.StateRunning, row.State)
	require.Equal(t, store.PoolKindHot, row.PoolKind)
	require.Equal(t, runnerID, row.RunnerID)
}

// TestAdopt_OnlineIdleRunner_BecomesAssigned: a runner that registered
// but hasn't picked up a job yet — Assigned is the safe middle ground.
// The reconciler's AssignedGrace will recycle the row if no job arrives.
func TestAdopt_OnlineIdleRunner_BecomesAssigned(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	const vmid = 10003
	const runnerID int64 = 5555
	name := fmt.Sprintf("gh-runner-test-%d", vmid)
	fp := &fakeProv{
		listOwnedRet: []*provisioner.VM{{VMID: vmid, Node: "pve1", Name: name}},
		powerStateBy: map[int]string{vmid: "running"},
	}
	cfg := Config{RunnerLister: func(context.Context) (map[string]RunnerInfo, error) {
		return map[string]RunnerInfo{
			name: {ID: runnerID, Online: true, Busy: false},
		}, nil
	}}
	mgr := newTestManager(t, st, fp, cfg)

	require.NoError(t, mgr.Adopt(context.Background()))

	row, err := st.Get(vmid)
	require.NoError(t, err)
	require.Equal(t, store.StateAssigned, row.State)
	require.Equal(t, store.PoolKindHot, row.PoolKind)
	require.Equal(t, runnerID, row.RunnerID)
}

// TestAdopt_OfflineRunner_BecomesAssigned: a runner registered but
// observed offline — also Assigned, so AssignedOfflineGrace handles it.
func TestAdopt_OfflineRunner_BecomesAssigned(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	const vmid = 10004
	const runnerID int64 = 6666
	name := fmt.Sprintf("gh-runner-test-%d", vmid)
	fp := &fakeProv{
		listOwnedRet: []*provisioner.VM{{VMID: vmid, Node: "pve1", Name: name}},
		powerStateBy: map[int]string{vmid: "running"},
	}
	cfg := Config{RunnerLister: func(context.Context) (map[string]RunnerInfo, error) {
		return map[string]RunnerInfo{
			name: {ID: runnerID, Online: false, Busy: false},
		}, nil
	}}
	mgr := newTestManager(t, st, fp, cfg)

	require.NoError(t, mgr.Adopt(context.Background()))

	row, err := st.Get(vmid)
	require.NoError(t, err)
	require.Equal(t, store.StateAssigned, row.State)
	require.Equal(t, runnerID, row.RunnerID)
}

// TestAdopt_PowerQueryFailure_DefaultsToHot: when Proxmox PowerState
// fails for a single VM, adopt defaults to Hot rather than Warm — Hot
// keeps the row visible to the gh.Reconciler's matrix, which will
// promote to Running if a runner does turn out to be busy. Warm would
// hide the row from the matrix entirely.
func TestAdopt_PowerQueryFailure_DefaultsToHot(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{
		listOwnedRet:    []*provisioner.VM{{VMID: 10005, Node: "pve1", Name: "gh-runner-test-10005"}},
		powerStateErrBy: map[int]error{10005: errors.New("proxmox 500")},
	}
	mgr := newTestManager(t, st, fp, Config{})

	require.NoError(t, mgr.Adopt(context.Background()))

	row, err := st.Get(10005)
	require.NoError(t, err)
	require.Equal(t, store.StateHot, row.State)
	require.Equal(t, store.PoolKindHot, row.PoolKind)
}

// TestAdopt_GitHubListFailure_FallsBackToPowerOnly: a whole-pass GitHub
// API failure must NOT abort adoption — every VM is still classified
// from its power state, and the gh.Reconciler's next tick will
// reclassify Hot rows whose runners turn out to be busy.
func TestAdopt_GitHubListFailure_FallsBackToPowerOnly(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{
		listOwnedRet: []*provisioner.VM{
			{VMID: 10006, Node: "pve1", Name: "gh-runner-test-10006"},
			{VMID: 10007, Node: "pve1", Name: "gh-runner-test-10007"},
		},
		powerStateBy: map[int]string{10006: "running", 10007: "stopped"},
	}
	cfg := Config{RunnerLister: func(context.Context) (map[string]RunnerInfo, error) {
		return nil, errors.New("github 503")
	}}
	mgr := newTestManager(t, st, fp, cfg)

	require.NoError(t, mgr.Adopt(context.Background()))

	hot, err := st.Get(10006)
	require.NoError(t, err)
	require.Equal(t, store.StateHot, hot.State)

	warm, err := st.Get(10007)
	require.NoError(t, err)
	require.Equal(t, store.StateWarm, warm.State)
}

// TestAdopt_NilRunnerLister_OK: a nil lister is treated as "GitHub
// unavailable" — same behavior as a lister returning an error. Allows
// callers (e.g. dry-run mode) to skip GitHub wiring entirely.
func TestAdopt_NilRunnerLister_OK(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{
		listOwnedRet: []*provisioner.VM{{VMID: 10008, Node: "pve1", Name: "gh-runner-test-10008"}},
		powerStateBy: map[int]string{10008: "running"},
	}
	mgr := newTestManager(t, st, fp, Config{})

	require.NoError(t, mgr.Adopt(context.Background()))

	row, err := st.Get(10008)
	require.NoError(t, err)
	require.Equal(t, store.StateHot, row.State)
}

// TestAdopt_NoVMs_IsNoop: a clean startup (no inherited VMs) is a
// successful no-op.
func TestAdopt_NoVMs_IsNoop(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{listOwnedRet: nil}
	mgr := newTestManager(t, st, fp, Config{})

	require.NoError(t, mgr.Adopt(context.Background()))

	rows, err := st.List()
	require.NoError(t, err)
	require.Empty(t, rows)
}

// TestAdopt_PropagatesListError: a Proxmox ListOwnedVMs failure aborts
// adoption — without it, the new leader would start with an empty store
// AND every gh.Reconciler tick would also fail to enumerate, leaving
// inherited VMs effectively invisible. Surfacing the error lets the
// caller log + continue with an empty pool (the reconciler will adopt
// any stranded VMs once the API recovers).
func TestAdopt_PropagatesListError(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{listErr: errors.New("proxmox down")}
	mgr := newTestManager(t, st, fp, Config{})

	err := mgr.Adopt(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "proxmox down")
}

// TestAdopt_MultipleVMs_AllAdopted: every inherited VM, across multiple
// nodes, ends up in the store with no destroys.
func TestAdopt_MultipleVMs_AllAdopted(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{
		listOwnedRet: []*provisioner.VM{
			{VMID: 10010, Node: "pve1", Name: "gh-runner-test-10010"},
			{VMID: 10011, Node: "pve2", Name: "gh-runner-test-10011"},
			{VMID: 10012, Node: "pve1", Name: "gh-runner-test-10012"},
		},
		powerStateBy: map[int]string{10010: "running", 10011: "stopped", 10012: "running"},
	}
	mgr := newTestManager(t, st, fp, Config{})

	require.NoError(t, mgr.Adopt(context.Background()))

	for _, vmid := range []int{10010, 10011, 10012} {
		_, err := st.Get(vmid)
		require.NoError(t, err, "vmid %d should be adopted into the store", vmid)
	}
	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.Empty(t, fp.destroys, "adopt must not destroy any VM")
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

// TestDestroyOrSyncFallback_RunsSynchronouslyWhenWorkerCtxCancelled
// locks the clone-fail VM-leak fix: when workerCtx is already cancelled
// (drain in progress), the async destroy goroutine would bail out
// immediately on its sem.Acquire. The fallback runs prov.Destroy
// against a fresh context so the just-cloned VM still gets cleaned up.
func TestDestroyOrSyncFallback_RunsSynchronouslyWhenWorkerCtxCancelled(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{}

	// Pre-insert a row so the sync destroy path's store.Delete works.
	require.NoError(t, st.Insert(&store.VM{
		VMID:     50050,
		Node:     "pve1",
		Name:     "gh-runner-test-50050",
		PoolKind: store.PoolKindHot,
		State:    store.StateDestroying,
	}))

	mgr := newTestManager(t, st, fp, Config{DrainTimeout: 1 * time.Second})
	// Force the "workerCtx already cancelled" branch.
	mgr.workerCancel()

	mgr.destroyOrSyncFallback(50050, "pve1")

	fp.mu.Lock()
	require.Contains(t, fp.destroys, 50050,
		"synchronous fallback must call prov.Destroy when workerCtx is already cancelled")
	fp.mu.Unlock()

	_, err := st.Get(50050)
	require.Error(t, err, "store row should be deleted after the sync destroy succeeds")
}

// TestDestroyOrSyncFallback_AsyncWhenWorkerCtxLive locks the inverse:
// in normal operation (workerCtx still live) the fallback delegates to
// destroyAsync so we keep the existing concurrency budget semantics.
func TestDestroyOrSyncFallback_AsyncWhenWorkerCtxLive(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{}
	mgr := newTestManager(t, st, fp, Config{DrainTimeout: 1 * time.Second})

	mgr.destroyOrSyncFallback(50051, "pve1")

	// Wait for the async destroy to land.
	done := make(chan struct{})
	go func() {
		mgr.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("destroyAsync goroutine did not exit")
	}

	fp.mu.Lock()
	require.Contains(t, fp.destroys, 50051)
	fp.mu.Unlock()
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

// TestRunClone_PanicReportsAllocatedVMID: when a clone-goroutine panics
// after vmid allocation, the recover log line must carry the real vmid
// — operators need it to know which row to manually clean up. The
// previous `defer m.recoverPanic("clone", 0)` captured 0 by value at
// goroutine entry, before the allocator ran.
func TestRunClone_PanicReportsAllocatedVMID(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	var logBuf strings.Builder
	logMu := sync.Mutex{}
	log := slog.New(slog.NewTextHandler(&syncWriter{w: &logBuf, mu: &logMu}, &slog.HandlerOptions{Level: slog.LevelError}))

	// fakeProv with Clone that panics — simulates a nil-deref in the
	// underlying go-proxmox library.
	fp := &fakeProv{cloneErr: nil}
	fp.cloneDelay = 0
	fp.destroyHang = false
	// Override Clone via the existing hook by patching cloneErr? No — we need to
	// inject a panic. Use a wrapper.
	pp := &panickyProv{inner: fp}

	mi, err := NewManager(Config{
		HotSize:              1,
		MaxConcurrentRunners: 5,
		VMIDRange:            config.VMIDRange{Min: 73000, Max: 73099},
		VMNamePrefix:         "gh-runner-test-",
		TemplateNode:         "pve1",
		BootMaxAttempts:      3,
	}, st, pp, mustSel(t), log, observability.NewMetrics(prometheus.NewRegistry()))
	require.NoError(t, err)
	mgr := mi.(*manager)

	// Trigger a single clone goroutine. The panic happens inside Clone,
	// which is called AFTER allocateVMID — so the log line should
	// reference the allocated id, not 0.
	mgr.kickClone(context.Background(), store.PoolKindHot, true)

	require.Eventually(t, func() bool {
		logMu.Lock()
		defer logMu.Unlock()
		return strings.Contains(logBuf.String(), "panic in async pool worker")
	}, 2*time.Second, 10*time.Millisecond)

	logMu.Lock()
	out := logBuf.String()
	logMu.Unlock()
	require.NotContains(t, out, "vmid=0",
		"panic log must NOT carry vmid=0 once allocation has happened. log: %s", out)
	require.Contains(t, out, "vmid=73000",
		"panic log must reference the allocated vmid. log: %s", out)

	mgr.workerCancel()
	mgr.wg.Wait()
}

// TestRunClone_DeletesOrphanWhenRowVanished simulates the race fixed
// by #63: Clone succeeds on Proxmox, then a concurrent ForceDestroy /
// stuck-state sweep deletes the store row before runClone can flip it
// to Warm/Booting. Without the fix the just-cloned VM lived until
// sweepProxmoxOrphans picked it up (OrphanGrace + reconcile tick).
// The fix must destroy it immediately.
func TestRunClone_DeletesOrphanWhenRowVanished(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{}
	// Inside Clone, delete the store row for the just-allocated vmid.
	// This is the race window the bug exploited.
	fp.onClone = func(opts provisioner.CloneOptions) {
		_ = st.Delete(opts.NewVMID)
	}

	mi, err := NewManager(Config{
		HotSize:              1,
		MaxConcurrentRunners: 5,
		VMIDRange:            config.VMIDRange{Min: 73000, Max: 73099},
		VMNamePrefix:         "gh-runner-test-",
		TemplateNode:         "pve1",
		BootMaxAttempts:      3,
	}, st, fp, mustSel(t), slog.New(slog.NewTextHandler(io.Discard, nil)), observability.NewMetrics(prometheus.NewRegistry()))
	require.NoError(t, err)
	mgr := mi.(*manager)

	mgr.kickClone(context.Background(), store.PoolKindHot, true)

	require.Eventually(t, func() bool {
		fp.mu.Lock()
		defer fp.mu.Unlock()
		return len(fp.destroys) >= 1
	}, 2*time.Second, 10*time.Millisecond,
		"runClone must destroy the just-cloned VM when its row was deleted mid-clone")

	mgr.workerCancel()
	mgr.wg.Wait()

	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.Equal(t, 1, len(fp.clones), "exactly one Clone should have run")
	require.Equal(t, fp.clones[0].NewVMID, fp.destroys[0],
		"destroy must target the vmid just cloned (orphan), not a different id")
}

// panickyProv wraps a Provisioner and panics from Clone. Useful for
// testing recoverPanic's logging.
type panickyProv struct{ inner provisioner.Provisioner }

func (p *panickyProv) Clone(context.Context, provisioner.CloneOptions) (*provisioner.VM, error) {
	panic("simulated nil-deref inside go-proxmox.Clone")
}
func (p *panickyProv) Start(ctx context.Context, vm *provisioner.VM) error {
	return p.inner.Start(ctx, vm)
}
func (p *panickyProv) Stop(ctx context.Context, vm *provisioner.VM) error {
	return p.inner.Stop(ctx, vm)
}
func (p *panickyProv) Destroy(ctx context.Context, vm *provisioner.VM) error {
	return p.inner.Destroy(ctx, vm)
}
func (p *panickyProv) WaitReady(ctx context.Context, vm *provisioner.VM, t time.Duration) error {
	return p.inner.WaitReady(ctx, vm, t)
}
func (p *panickyProv) InjectJITConfig(ctx context.Context, vm *provisioner.VM, jit string) error {
	return p.inner.InjectJITConfig(ctx, vm, jit)
}
func (p *panickyProv) ReadJITConfig(ctx context.Context, vm *provisioner.VM) ([]byte, error) {
	return p.inner.ReadJITConfig(ctx, vm)
}
func (p *panickyProv) ListOwnedVMs(ctx context.Context) ([]*provisioner.VM, error) {
	return p.inner.ListOwnedVMs(ctx)
}
func (p *panickyProv) PowerState(ctx context.Context, vm *provisioner.VM) (string, error) {
	return p.inner.PowerState(ctx, vm)
}
func (p *panickyProv) Ping(ctx context.Context) error { return p.inner.Ping(ctx) }
func (p *panickyProv) TemplateNode() string           { return p.inner.TemplateNode() }
func (p *panickyProv) Client() *proxmox.Client        { return p.inner.Client() }
func (p *panickyProv) IsRecentlyDestroyed(vmid int, c time.Duration) bool {
	return p.inner.IsRecentlyDestroyed(vmid, c)
}
func (p *panickyProv) InFlightCloneCount() int { return p.inner.InFlightCloneCount() }

// syncWriter serialises writes to the shared log buffer so concurrent
// goroutines don't tear the captured output.
type syncWriter struct {
	w  *strings.Builder
	mu *sync.Mutex
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func mustSel(t *testing.T) nodeselector.Selector {
	t.Helper()
	sel, err := nodeselector.NewSingle("pve1")
	require.NoError(t, err)
	return sel
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

// TestPowerPoller_PerVMTimeout_HangingVMDoesNotStallLoop: a single stuck
// Proxmox node previously froze the entire pass for up to the underlying
// HTTP client's 60s timeout. With per-VM bounded context, the hung VM
// returns ctx.Err quickly and the loop proceeds to the next row.
//
// Not run with t.Parallel(): this test mutates the package-level
// powerPollTimeoutPerVM var, and the other power-poll tests read it
// concurrently. Sequential execution avoids the data race.
func TestPowerPoller_PerVMTimeout_HangingVMDoesNotStallLoop(t *testing.T) {
	prev := powerPollTimeoutPerVM
	powerPollTimeoutPerVM = 50 * time.Millisecond
	t.Cleanup(func() { powerPollTimeoutPerVM = prev })

	st := newTestStore(t)
	require.NoError(t, st.Insert(&store.VM{
		VMID: 71030, Node: "pve1", Name: "hung-vm",
		PoolKind: store.PoolKindHot, State: store.StateRunning,
	}))
	require.NoError(t, st.Insert(&store.VM{
		VMID: 71031, Node: "pve1", Name: "completed-vm",
		PoolKind: store.PoolKindHot, State: store.StateRunning,
	}))
	fp := &fakeProv{
		powerStateHangBy: map[int]bool{71030: true},
		powerStateBy:     map[int]string{71031: "stopped"},
	}
	mgr := newTestManager(t, st, fp, Config{})

	done := make(chan struct{})
	go func() {
		mgr.powerPollOnce(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("powerPollOnce did not complete within 2s — per-VM timeout did not unblock the loop")
	}

	// The non-hanging completed VM must still be reaped despite the
	// hung sibling row above it in the iteration.
	require.Eventually(t, func() bool {
		fp.mu.Lock()
		defer fp.mu.Unlock()
		for _, v := range fp.destroys {
			if v == 71031 {
				return true
			}
		}
		return false
	}, time.Second, 10*time.Millisecond, "completed VM must be queued for destruction even when a sibling hangs")
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

// TestAllocateVMID_RespectsRecentlyDestroyedCooldown locks in the
// post-destroy cooldown: after a destroy completes, PVE's qmdestroy
// task and lock-file cleanup may still be settling, so reissuing the
// same VMID immediately produces "VM N is running - destroy failed"
// and lock-file timeouts. allocateVMID must consult
// Provisioner.IsRecentlyDestroyed and skip cooled-down VMIDs.
func TestAllocateVMID_RespectsRecentlyDestroyedCooldown(t *testing.T) {
	st := newTestStore(t)
	fp := &fakeProv{
		// 10000 is "recently destroyed"; 10001 is free.
		recentlyDestroyedSet: map[int]bool{10000: true},
	}
	mgr := newTestManager(t, st, fp, Config{
		HotSize:           1,
		VMIDRange:         config.VMIDRange{Min: 10000, Max: 10005},
		VMIDReuseCooldown: 30 * time.Second,
	})

	// First allocate skips 10000 because it's recently destroyed.
	vmid, err := mgr.allocateVMID(context.Background())
	require.NoError(t, err)
	require.Equal(t, 10001, vmid,
		"allocateVMID must skip 10000 while it's in the cooldown window; reusing it would race with PVE-side teardown")

	// Simulate time-advance past the cooldown: 10000 is no longer
	// recent. The next allocate should pick it again.
	fp.mu.Lock()
	delete(fp.recentlyDestroyedSet, 10000)
	fp.mu.Unlock()

	vmid, err = mgr.allocateVMID(context.Background())
	require.NoError(t, err)
	require.Equal(t, 10000, vmid,
		"after the cooldown expires, the freed VMID becomes eligible again")
}

// TestAllocateVMID_AllRangeRecentlyDestroyedReturnsError covers the
// boundary case where a burst of destroys leaves every VMID in range
// inside the cooldown window. The allocator must surface an error
// rather than block, retry forever, or return a stale ID.
func TestAllocateVMID_AllRangeRecentlyDestroyedReturnsError(t *testing.T) {
	st := newTestStore(t)
	fp := &fakeProv{
		recentlyDestroyedSet: map[int]bool{
			10000: true, 10001: true, 10002: true,
		},
	}
	mgr := newTestManager(t, st, fp, Config{
		HotSize:           1,
		VMIDRange:         config.VMIDRange{Min: 10000, Max: 10002},
		VMIDReuseCooldown: 30 * time.Second,
	})
	_, err := mgr.allocateVMID(context.Background())
	require.Error(t, err)
}

// TestDestroy_RunnerIDSetDuringDestroyIsObserved: SetRunnerID can land
// concurrently while destroy() is inside prov.Destroy (a sub-15s job
// completing before the gh.Reconciler has tagged the row). The previous
// destroy() read the row BEFORE prov.Destroy, so a concurrent stamp was
// invisible and the GitHub registration leaked.
//
// With DeleteAndReturn the runner_id read happens in the same write
// txn as the row delete — the orphan callback sees the latest value.
func TestDestroy_RunnerIDSetDuringDestroyIsObserved(t *testing.T) {
	st := newTestStore(t)

	const lateRunnerID int64 = 99999
	stamped := make(chan struct{})
	fp := &fakeProv{
		onDestroy: func() {
			// Simulate the scaler stamping a runner_id mid-destroy.
			if _, err := st.Update(10060, func(v *store.VM) {
				v.RunnerID = lateRunnerID
			}); err != nil {
				t.Errorf("Update RunnerID failed: %v", err)
			}
			close(stamped)
		},
	}

	var sawRunnerID atomic.Int64
	cb := func(_ context.Context, runnerID int64) error {
		sawRunnerID.Store(runnerID)
		return nil
	}

	mgr := newTestManager(t, st, fp, Config{
		HotSize:          1,
		OnRunnerOrphaned: cb,
		DrainTimeout:     time.Second,
	})

	// Insert a row that has NO runner_id at first — the pre-Destroy
	// Get would have read 0 and the orphan callback would have been
	// skipped entirely. After the in-flight stamp it carries
	// lateRunnerID.
	require.NoError(t, st.Insert(&store.VM{
		VMID: 10060, Node: "pve1", Name: "race-victim",
		PoolKind: store.PoolKindHot, State: store.StateHot,
	}))
	_, err := st.UpdateState(10060, store.StateHot, store.StateDestroying, nil)
	require.NoError(t, err)

	mgr.destroy(context.Background(), 10060, "pve1")

	select {
	case <-stamped:
	case <-time.After(time.Second):
		t.Fatalf("onDestroy hook did not run; test setup is broken")
	}

	require.Equal(t, lateRunnerID, sawRunnerID.Load(),
		"OnRunnerOrphaned must observe the RunnerID stamped during destroy, not a stale pre-Destroy read")

	// Row really is gone.
	_, err = st.Get(10060)
	require.Error(t, err)
}

// TestDestroy_OnRunnerOrphanedErrorDoesNotBlockDestroy: when the
// OnRunnerOrphaned callback (which deregisters the GitHub runner)
// returns an error — common during a GitHub rate-limit or 5xx burst —
// the destroy MUST still complete. Otherwise a single GH outage
// halts VM destruction across the fleet, the pool fills with
// undestroyable runners, and the scaleset wedges. The callback's
// error is logged and discarded.
func TestDestroy_OnRunnerOrphanedErrorDoesNotBlockDestroy(t *testing.T) {
	st := newTestStore(t)
	fp := &fakeProv{}

	var callbackInvocations int32
	cb := func(_ context.Context, runnerID int64) error {
		atomic.AddInt32(&callbackInvocations, 1)
		return errors.New("github rate-limited")
	}

	mgr := newTestManager(t, st, fp, Config{
		HotSize:          1,
		OnRunnerOrphaned: cb,
		DrainTimeout:     time.Second,
	})

	// Seed a Hot row with a runner ID so destroy will invoke the callback.
	require.NoError(t, st.Insert(&store.VM{
		VMID: 10042, Node: "pve1", Name: "x",
		PoolKind: store.PoolKindHot, State: store.StateHot,
		RunnerID: 12345,
	}))
	// Move it to Destroying so destroy() acts on it.
	_, err := st.UpdateState(10042, store.StateHot, store.StateDestroying, nil)
	require.NoError(t, err)

	mgr.destroy(context.Background(), 10042, "pve1")

	// Callback fired exactly once.
	require.Equal(t, int32(1), atomic.LoadInt32(&callbackInvocations))
	// PVE destroy still happened.
	fp.mu.Lock()
	require.Contains(t, fp.destroys, 10042, "Destroy must proceed even when OnRunnerOrphaned errors")
	fp.mu.Unlock()
	// Row removed from the store.
	_, err = st.Get(10042)
	require.Error(t, err, "row must be deleted after destroy regardless of callback outcome")
}

// TestDestroy_OnRunnerOrphanedRunsEvenWhenParentCtxCancelled: a
// force-drain cancels the worker ctx after prov.Destroy returns but
// before OnRunnerOrphaned completes its GitHub round-trip. The fix
// detaches the cleanup ctx from the drain ctx so the idempotent
// deregister still runs and the runner registration isn't leaked.
func TestDestroy_OnRunnerOrphanedRunsEvenWhenParentCtxCancelled(t *testing.T) {
	st := newTestStore(t)
	fp := &fakeProv{}

	var (
		invocations int32
		ctxLive     atomic.Bool // true ⇔ callback saw ctx.Err() == nil
	)
	cb := func(ctx context.Context, runnerID int64) error {
		atomic.AddInt32(&invocations, 1)
		// Record whether the ctx given to the callback is still live.
		// With the fix in place, the cleanup ctx must be independent
		// of the (cancelled) parent.
		ctxLive.Store(ctx.Err() == nil)
		return nil
	}

	mgr := newTestManager(t, st, fp, Config{
		HotSize:          1,
		OnRunnerOrphaned: cb,
		DrainTimeout:     time.Second,
	})

	require.NoError(t, st.Insert(&store.VM{
		VMID: 10050, Node: "pve1", Name: "drain-victim",
		PoolKind: store.PoolKindHot, State: store.StateHot,
		RunnerID: 54321,
	}))
	_, err := st.UpdateState(10050, store.StateHot, store.StateDestroying, nil)
	require.NoError(t, err)

	// Pre-cancelled parent: models worker/drain ctx that was killed
	// mid-destroy after Proxmox already finished.
	parentCtx, cancel := context.WithCancel(context.Background())
	cancel()

	mgr.destroy(parentCtx, 10050, "pve1")

	require.Equal(t, int32(1), atomic.LoadInt32(&invocations),
		"OnRunnerOrphaned must fire exactly once even with a cancelled parent ctx")
	require.True(t, ctxLive.Load(),
		"OnRunnerOrphaned must receive a fresh, non-cancelled cleanup ctx")
}

// TestReconcileOnce_DoesNotOverProvisionWhenClonesInFlight covers the
// inter-tick race: two consecutive reconcile ticks each saw an empty
// store and each dispatched HotSize clones — the pool worker hadn't
// yet inserted the rows from the first tick when the second tick
// snapshotted. The headroom calc must count
// Provisioner.InFlightCloneCount() so a tick sees the previous
// tick's work even before the store rows have caught up.
//
// Setup: empty store + hot_size=3, but the Provisioner reports 3
// clones already in-flight (the previous tick's work). reconcileOnce
// must NOT dispatch any new clones — the in-flight set will become
// Hot soon.
func TestReconcileOnce_DoesNotOverProvisionWhenClonesInFlight(t *testing.T) {
	st := newTestStore(t)
	fp := &fakeProv{
		inFlightClones: 3, // previous tick's work, store rows haven't landed yet
	}
	mgr := newTestManager(t, st, fp, Config{
		HotSize:              3,
		MaxConcurrentRunners: 10,
		VMIDRange:            config.VMIDRange{Min: 10000, Max: 10099},
	})

	mgr.reconcileOnce(context.Background())

	// kickClone spawns goroutines; wait for the manager's wg to drain
	// so we observe the final state. NewManager doesn't expose the
	// wg directly, so we just give the (immediate-return) fake Clone
	// calls time to land — 100ms is generous, the fake returns the
	// instant the goroutine schedules.
	time.Sleep(100 * time.Millisecond)

	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.Empty(t, fp.clones,
		"reconcileOnce must NOT dispatch new clones when prov.InFlightCloneCount() == HotSize — that's the previous tick's work coming through; got %d", len(fp.clones))
}
