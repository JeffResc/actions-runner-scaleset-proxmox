package pool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/config"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/store"
)

// countingAllocator is a fake ipam.Allocator that records Allocate /
// Release calls and can be made to fail either operation. It lets the
// clone-failure tests assert that a lease is never silently leaked: the
// number of Releases must account for every successful Allocate.
type countingAllocator struct {
	mu         sync.Mutex
	allocErr   error
	releaseErr error
	allocs     int
	releases   int
	held       map[int]bool // vmids currently holding a lease
}

func newCountingAllocator() *countingAllocator {
	return &countingAllocator{held: map[int]bool{}}
}

func (a *countingAllocator) Allocate(_ context.Context, vmid int) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.allocs++
	if a.allocErr != nil {
		return "", a.allocErr
	}
	a.held[vmid] = true
	return "10.0.0.1", nil
}

func (a *countingAllocator) Release(_ context.Context, vmid int) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.releases++
	delete(a.held, vmid)
	return a.releaseErr
}

func (a *countingAllocator) snapshot() (allocs, releases, held int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.allocs, a.releases, len(a.held)
}

// storeEmpty reports whether the store has no rows left at all.
func storeEmpty(t *testing.T, st *store.Store) bool {
	t.Helper()
	rows, err := st.ListByState(
		store.StateProvisioning, store.StateWarm, store.StateBooting,
		store.StateHot, store.StateAssigned, store.StateRunning,
		store.StateRecycling, store.StateDraining, store.StateDestroying,
		store.StatePoison,
	)
	require.NoError(t, err)
	return len(rows) == 0
}

// ---------------------------------------------------------------------------
// #356: partial-failure clone paths
// ---------------------------------------------------------------------------

// TestClone_PowerOnFailsAfterRow_LandsTerminal (#356) drives the
// snapshot-rollback clone path where the explicit post-snapshot Start
// fails. The row was already Update'd to Booting (a "will be hot" state);
// the failure must route it through markPoisonOrDestroy to a clean
// terminal outcome (Destroying → removed), never leaving it stuck in a
// Hot/Booting state that warm-reuse or Acquire would trust.
func TestClone_PowerOnFailsAfterRow_LandsTerminal(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{startErr: errors.New("qmstart: node unreachable")}
	mgr := newTestManager(t, st, fp, Config{
		HotSize:     1,
		VMMaxAge:    time.Hour,
		RecycleMode: config.RecycleModeSnapshotRollback,
	})

	// One hot clone dispatched. In snapshot mode the clone is created
	// powered-off, snapshotted, then explicitly started — and that Start
	// fails.
	mgr.reconcileOnce(context.Background())

	require.Eventually(t, func() bool {
		fp.mu.Lock()
		defer fp.mu.Unlock()
		return len(fp.starts) >= 1 && len(fp.destroys) >= 1
	}, 2*time.Second, 10*time.Millisecond,
		"a power-on failure after the row was created must trigger a destroy")

	fp.mu.Lock()
	vmid := fp.destroys[0]
	fp.mu.Unlock()

	// The row must NOT survive as a trusted/ready VM. The failed boot
	// routes through markPoisonOrDestroy → Destroying → destroyAsync,
	// which deletes the row.
	require.Eventually(t, func() bool {
		_, err := st.Get(vmid)
		return errors.Is(err, store.ErrNotFound)
	}, 2*time.Second, 10*time.Millisecond,
		"row must be removed, not left hot/ready")

	// And no row is ever left in a warm-reusable/acquirable state.
	trusted, err := st.ListByState(store.StateHot, store.StateWarm, store.StateBooting)
	require.NoError(t, err)
	require.Empty(t, trusted, "a boot-failed clone must never land in Hot/Warm/Booting")
}

// TestClone_IPAMAllocateFailure_RowDestroyed (#356) makes prepareClone's
// IPAM Allocate fail. The provisioning row already exists, so
// handleCloneFailure must mark it Destroying and queue a destroy — the
// Proxmox Clone call must never fire, and the row must be reaped.
func TestClone_IPAMAllocateFailure_RowDestroyed(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{}
	alloc := newCountingAllocator()
	alloc.allocErr = errors.New("ipam: upstream NetBox 503")
	mgr := newTestManager(t, st, fp, Config{
		Profiles: []ProfileSettings{{Name: defaultProfileName, HotSize: 1, IPAM: alloc}},
	})

	mgr.reconcileOnce(context.Background())

	require.Eventually(t, func() bool {
		fp.mu.Lock()
		defer fp.mu.Unlock()
		return len(fp.destroys) >= 1
	}, 2*time.Second, 10*time.Millisecond,
		"an IPAM allocate failure must queue a destroy for the orphaned row")

	// The Proxmox clone must NOT have been attempted — the failure is
	// upstream of it.
	fp.mu.Lock()
	clones := len(fp.clones)
	fp.mu.Unlock()
	require.Zero(t, clones, "clone must not be called when IPAM allocation fails")

	// Row reaped, and the failed Allocate left nothing held (no lease
	// leak). Release is idempotent and may be called during destroy.
	require.Eventually(t, func() bool { return storeEmpty(t, st) },
		2*time.Second, 10*time.Millisecond, "row must be removed after IPAM failure")
	_, _, held := alloc.snapshot()
	require.Zero(t, held, "a failed Allocate must not leak a lease")
}

// TestClone_IPAMAllocateAndReleaseFail_RowStillReaped (#356) is the
// double-failure variant: Allocate fails (triggering the destroy path)
// and the subsequent Release ALSO errors. The Release error is
// best-effort/logged; the row must still be deleted rather than wedged.
func TestClone_IPAMAllocateAndReleaseFail_RowStillReaped(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{}
	alloc := newCountingAllocator()
	alloc.allocErr = errors.New("ipam: allocate failed")
	alloc.releaseErr = errors.New("ipam: release failed too")
	mgr := newTestManager(t, st, fp, Config{
		Profiles: []ProfileSettings{{Name: defaultProfileName, HotSize: 1, IPAM: alloc}},
	})

	mgr.reconcileOnce(context.Background())

	// destroy() attempts the (idempotent) IPAM release AFTER deleting the
	// row; wait on the release count so we observe the whole path.
	require.Eventually(t, func() bool {
		_, releases, _ := alloc.snapshot()
		return releases >= 1
	}, 2*time.Second, 10*time.Millisecond,
		"destroy must still attempt the (idempotent) release even after an allocate failure")
	require.True(t, storeEmpty(t, st),
		"row must be reaped even when both IPAM Allocate and Release error")
	_, _, held := alloc.snapshot()
	require.Zero(t, held, "no lease may remain held")
}

// TestKickClone_UnknownProfile_CleanNoLeak (#356) exercises the
// prepareClone profile-not-found guard: an unknown profile must abort
// cleanly — no row, no clone, the cloneSem fully released, allocMu never
// wedged, and no pendingClones reservation leaked.
func TestKickClone_UnknownProfile_CleanNoLeak(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{}
	mgr := newTestManager(t, st, fp, Config{HotSize: 1})

	mgr.kickClone(context.Background(), "does-not-exist", store.PoolKindHot, true)

	// The spawned goroutine must release the cloneSem it acquired. When
	// all 8 slots are re-acquirable the goroutine has fully unwound.
	require.Eventually(t, func() bool {
		if !mgr.cloneSem.TryAcquire(8) {
			return false
		}
		mgr.cloneSem.Release(8)
		return true
	}, time.Second, 10*time.Millisecond, "cloneSem must be fully released after an unknown-profile abort")

	require.True(t, storeEmpty(t, st), "no row may be created for an unknown profile")
	fp.mu.Lock()
	require.Empty(t, fp.clones, "no clone may be dispatched for an unknown profile")
	fp.mu.Unlock()

	// The default profile's pendingClones must be untouched (an unknown
	// profile reserves nothing).
	require.Equal(t, int32(0), mgr.profileOf(defaultProfileName).pendingClones.Load())

	// allocMu must not be held.
	require.True(t, mgr.allocMu.TryLock(), "allocMu must not be wedged")
	mgr.allocMu.Unlock()
}

// TestKickClone_CancelledCtx_NoLeaseNoRow (#360) drives a clone with an
// already-cancelled context. kickClone acquires the cloneSem under that
// ctx, so the spawn is dropped before any work: no IPAM lease, no row,
// no clone. Documents that a cancelled reconcile tick cleanly abandons
// the dispatch (no leak).
func TestKickClone_CancelledCtx_NoLeaseNoRow(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{}
	alloc := newCountingAllocator()
	mgr := newTestManager(t, st, fp, Config{
		Profiles: []ProfileSettings{{Name: defaultProfileName, HotSize: 1, IPAM: alloc}},
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mgr.kickClone(ctx, defaultProfileName, store.PoolKindHot, true)

	// Nothing should ever happen; give any erroneous spawn time to land.
	time.Sleep(50 * time.Millisecond)
	require.True(t, storeEmpty(t, st), "cancelled ctx must not create a row")
	fp.mu.Lock()
	require.Empty(t, fp.clones, "cancelled ctx must not dispatch a clone")
	fp.mu.Unlock()
	allocs, _, held := alloc.snapshot()
	require.Zero(t, allocs, "cancelled ctx must not allocate an IP")
	require.Zero(t, held, "cancelled ctx must not hold a lease")
}

// ---------------------------------------------------------------------------
// #359: concurrency correctness
// ---------------------------------------------------------------------------

// TestStampJobMetadata_ConcurrentDistinctJobs_NoHybrid (#359) fires two
// StampJobMetadata writes with distinct org/repo/class at the same VMID
// concurrently. Each write is a single atomic store transaction, so the
// row must reflect exactly ONE job's metadata — never a torn hybrid like
// (org from B, repo from A). Run under -race across many rounds.
func TestStampJobMetadata_ConcurrentDistinctJobs_NoHybrid(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	seedHot(t, st, 1) // vmid 20000
	mgr := newTestManager(t, st, &fakeProv{}, Config{HotSize: 1})

	metaA := JobMetadata{Org: "orgA", Repo: "orgA/repoA", PriorityClass: "classA"}
	metaB := JobMetadata{Org: "orgB", Repo: "orgB/repoB", PriorityClass: "classB"}

	for range 500 {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _ = mgr.StampJobMetadata(context.Background(), 20000, metaA) }()
		go func() { defer wg.Done(); _ = mgr.StampJobMetadata(context.Background(), 20000, metaB) }()
		wg.Wait()

		row, err := st.Get(20000)
		require.NoError(t, err)
		isA := row.Org == "orgA" && row.Repo == "orgA/repoA" && row.PriorityClass == "classA"
		isB := row.Org == "orgB" && row.Repo == "orgB/repoB" && row.PriorityClass == "classB"
		require.True(t, isA || isB,
			"row must reflect exactly one job's metadata, never a hybrid: got org=%q repo=%q class=%q",
			row.Org, row.Repo, row.PriorityClass)

		// Reset for the next round so a hybrid can't be masked by a
		// previous round's value.
		_, err = st.Update(20000, func(v *store.VM) {
			v.Org, v.Repo, v.PriorityClass = "", "", ""
		})
		require.NoError(t, err)
	}
}

// ---------------------------------------------------------------------------
// #360: degenerate configs
// ---------------------------------------------------------------------------

// TestReconcile_WarmShrinkToZero_LeavesWarmRows (#360) documents the
// current behavior when the warm target is lowered to zero at runtime:
// existing warm rows are NOT actively destroyed. Only recycleOldVMs
// (age-based) and shrinkHotPool (hot-only) reap idle capacity; neither
// touches warm rows on a warm-size decrease.
//
// NOTE(#360): documents current behavior; possible silent leak — warm
// VMs provisioned under an old (larger) warm target linger until they
// hit vm_max_age (or the process restarts), rather than being drained
// when warmSize is reduced. A human should decide whether a warm
// shrink-to-target path is warranted.
func TestReconcile_WarmShrinkToZero_LeavesWarmRows(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	seedWarm(t, st, 5) // vmids 30000..30004, no vm_max_age set
	fp := &fakeProv{}
	mgr := newTestManager(t, st, fp, Config{HotSize: 0, WarmSize: 5, MaxConcurrentRunners: 10})

	// Operator (or schedule) drops the warm target to zero.
	require.NoError(t, mgr.SetTargetSizes(defaultProfileName, 0, 0))
	mgr.reconcileOnce(context.Background())

	// Give any (erroneous) async destroy time to land.
	time.Sleep(100 * time.Millisecond)
	fp.mu.Lock()
	require.Empty(t, fp.destroys,
		"warm shrink-to-zero must not (currently) destroy existing warm rows")
	fp.mu.Unlock()

	warm, err := st.ListByState(store.StateWarm)
	require.NoError(t, err)
	require.Len(t, warm, 5, "all 5 warm rows must still be present (current behavior)")
}

// TestValidateConfig_InertPool (#360) pins that validateConfig accepts a
// (0,0,n) pool. The config is internally consistent, so it passes — but
// a pool with hotSize=0 && warmSize=0 never provisions anything on its
// own (only burst demand via SetDesiredCount would).
//
// NOTE(#360): documents current behavior; a (0,0,n) pool is a valid but
// inert config — it will hold zero VMs and never pre-provision. Whether
// that should warn at startup is a product decision.
func TestValidateConfig_InertPool(t *testing.T) {
	t.Parallel()
	require.NoError(t, validateConfig(0, 0, 5),
		"a (hot=0, warm=0) pool is internally consistent and must validate")

	// And end-to-end: such a manager never dispatches a clone at rest.
	st := newTestStore(t)
	fp := &fakeProv{}
	mgr := newTestManager(t, st, fp, Config{HotSize: 0, WarmSize: 0, MaxConcurrentRunners: 5})

	mgr.reconcileOnce(context.Background())
	time.Sleep(100 * time.Millisecond)
	fp.mu.Lock()
	require.Empty(t, fp.clones, "an inert (0,0) pool must never pre-provision")
	fp.mu.Unlock()
	require.True(t, storeEmpty(t, st))
}
