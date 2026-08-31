package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/nodecap"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/nodeselector"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/store"
)

const gib = 1024 * 1024 * 1024

// ---------- Fake CapacityAdmitter ----------

// fakeAdmitter models a fixed pool of bytes per node. It is deliberately
// simple — the real ledger is exercised in internal/nodecap; what these
// tests care about is how the pool REACTS to admit / refuse.
type fakeAdmitter struct {
	mu sync.Mutex
	// free is the remaining admissible bytes per node.
	free map[string]uint64
	// total is what Snapshot reports as the node's size.
	total map[string]uint64
	// err, when set, fails every call — the cold-start "we have no idea
	// what is allocated" case.
	err error

	reserveCalls int
	freeingCalls int
	// lastFreeing records the VMIDs the eviction path promised to
	// destroy, so a test can assert the plan it committed to.
	lastFreeing []int
	released    int
	// vmSizes is the allocated memory of each VM the fake knows about,
	// so ReserveFreeing can credit a victim by its real size — the same
	// arithmetic the accountant does against the cluster snapshot.
	vmSizes map[int]uint64
}

func newFakeAdmitter(freePerNode map[string]uint64) *fakeAdmitter {
	total := make(map[string]uint64, len(freePerNode))
	for n, f := range freePerNode {
		total[n] = f
	}
	return &fakeAdmitter{free: freePerNode, total: total, vmSizes: map[int]uint64{}}
}

type fakeReservation struct {
	a    *fakeAdmitter
	node string
	mem  uint64
	once sync.Once
	vmid int
}

func (r *fakeReservation) Bind(vmid int) {
	r.a.mu.Lock()
	defer r.a.mu.Unlock()
	r.vmid = vmid
}

func (r *fakeReservation) Release() {
	r.once.Do(func() {
		r.a.mu.Lock()
		defer r.a.mu.Unlock()
		r.a.free[r.node] += r.mem
		r.a.released++
	})
}

func (a *fakeAdmitter) Fits(_ context.Context, shape nodecap.Shape, candidates []string) (map[string]bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return nil, a.err
	}
	out := make(map[string]bool, len(candidates))
	for _, c := range candidates {
		out[c] = a.free[c] >= shape.MemoryBytes
	}
	return out, nil
}

func (a *fakeAdmitter) Reserve(ctx context.Context, node string, shape nodecap.Shape) (nodecap.Reservation, bool, error) {
	a.mu.Lock()
	a.reserveCalls++
	a.mu.Unlock()
	return a.reserve(node, shape, nil)
}

func (a *fakeAdmitter) ReserveFreeing(_ context.Context, node string, shape nodecap.Shape, freeing []int) (nodecap.Reservation, bool, error) {
	a.mu.Lock()
	a.freeingCalls++
	a.lastFreeing = append([]int(nil), freeing...)
	a.mu.Unlock()
	return a.reserve(node, shape, freeing)
}

func (a *fakeAdmitter) reserve(node string, shape nodecap.Shape, freeing []int) (nodecap.Reservation, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return nil, false, a.err
	}
	// The eviction path's credit: the victims' memory is already
	// promised to this reservation even though their destroys have not
	// landed, so it counts as available to this caller alone.
	avail := a.free[node]
	for _, vmid := range freeing {
		avail += a.vmSizes[vmid]
	}
	if avail < shape.MemoryBytes {
		return nil, false, nil
	}
	a.free[node] = avail - shape.MemoryBytes
	return &fakeReservation{a: a, node: node, mem: shape.MemoryBytes}, true, nil
}

func (a *fakeAdmitter) Snapshot(_ context.Context) (map[string]nodecap.NodeState, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return nil, a.err
	}
	out := make(map[string]nodecap.NodeState, len(a.total))
	for n, total := range a.total {
		free := a.free[n]
		out[n] = nodecap.NodeState{
			TotalBytes:     total,
			AvailableBytes: free,
			CommittedBytes: total - free,
		}
	}
	return out, nil
}

func (a *fakeAdmitter) snapshotFree(node string) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.free[node]
}

// ---------- shapeOf ----------

func TestShapeOf_ConvertsMiBToBytes(t *testing.T) {
	t.Parallel()
	got := shapeOf(ProfileSettings{MemoryMB: 8192, CPUCores: 4})
	require.Equal(t, uint64(8*gib), got.MemoryBytes)
	require.Equal(t, 4, got.VCPUs)

	require.Zero(t, shapeOf(ProfileSettings{}).MemoryBytes,
		"an undeclared footprint must be zero, never a guess")
}

// ---------- Admission ----------

// TestReserveCapacity_DisabledIsAlwaysAdmitted is the back-compat
// guarantee: with no admitter wired, every clone is admitted exactly as
// it was before this feature existed.
func TestReserveCapacity_DisabledIsAlwaysAdmitted(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	m := newTestManager(t, st, &fakeProv{}, Config{})

	claim, ok := m.reserveCapacity(context.Background(), m.profileOf(""), "pve1", store.PoolKindHot)
	require.True(t, ok)
	require.NotNil(t, claim.release, "the release closure must be safe to call unconditionally")
	claim.release()
	claim.bind(123) // no-op, must not panic
}

// TestReserveCapacity_RefusalIsBackpressure: a full node produces a
// deferral (counted, no row, no error), not a failure.
func TestReserveCapacity_RefusalIsBackpressure(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	adm := newFakeAdmitter(map[string]uint64{"pve1": 8 * gib})
	m := newTestManager(t, st, &fakeProv{}, Config{
		Capacity: adm,
		Profiles: []ProfileSettings{{Name: "big", MemoryMB: 16384, CPUCores: 8, MaxConcurrentRunners: 10}},
	})

	_, ok := m.reserveCapacity(context.Background(), m.profileOf("big"), "pve1", store.PoolKindHot)
	require.False(t, ok, "16 GiB cannot fit in 8 GiB of headroom")
	require.Equal(t, float64(1), testutil.ToFloat64(
		m.metrics.CloneDeferredCapacity.WithLabelValues("test", "big", "pve1", "hot")))
	require.True(t, m.profileOf("big").capacityDeferredHot.Load(),
		"a refused HOT clone must arm the eviction trigger")
}

// TestReserveCapacity_WarmRefusalDoesNotArmEviction: warm is pool
// warmth, not a queued job, so it must never cost a sibling its VM.
func TestReserveCapacity_WarmRefusalDoesNotArmEviction(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	adm := newFakeAdmitter(map[string]uint64{"pve1": 0})
	m := newTestManager(t, st, &fakeProv{}, Config{
		Capacity: adm,
		Profiles: []ProfileSettings{{Name: "big", MemoryMB: 16384, MaxConcurrentRunners: 10}},
	})

	_, ok := m.reserveCapacity(context.Background(), m.profileOf("big"), "pve1", store.PoolKindWarm)
	require.False(t, ok)
	require.False(t, m.profileOf("big").capacityDeferredHot.Load(),
		"only a hot deferral represents a job that cannot run")
}

// TestReserveCapacity_UnknownCapacityDefers: with no snapshot at all we
// cannot promise not to overcommit, so we wait rather than guess.
func TestReserveCapacity_UnknownCapacityDefers(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	adm := newFakeAdmitter(map[string]uint64{"pve1": 64 * gib})
	adm.err = errors.New("proxmox unreachable")
	m := newTestManager(t, st, &fakeProv{}, Config{
		Capacity: adm,
		Profiles: []ProfileSettings{{Name: "p", MemoryMB: 4096, MaxConcurrentRunners: 10}},
	})

	_, ok := m.reserveCapacity(context.Background(), m.profileOf("p"), "pve1", store.PoolKindHot)
	require.False(t, ok, "admission must fail closed when capacity is unknown")
}

// TestClone_DeferredLeavesNoTrace: the whole point of gating BEFORE the
// VMID mint is that a refusal costs nothing — no row, no VMID burned, no
// Proxmox call, nothing for a sweep to clean up.
func TestClone_DeferredLeavesNoTrace(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	prov := &fakeProv{}
	adm := newFakeAdmitter(map[string]uint64{"pve1": 4 * gib})
	m := newTestManager(t, st, prov, Config{
		Capacity: adm,
		Profiles: []ProfileSettings{{Name: "big", MemoryMB: 16384, MaxConcurrentRunners: 10}},
	})

	m.runClone("big", store.PoolKindHot, true, nil, func() {})

	rows, err := st.List()
	require.NoError(t, err)
	require.Empty(t, rows, "a deferred clone must not persist a row")
	require.Zero(t, len(prov.clones), "a deferred clone must not reach Proxmox")
	require.Equal(t, uint64(4*gib), adm.snapshotFree("pve1"),
		"a refused reservation must not consume capacity")
}

// TestClone_SuccessKeepsItsReservation: on success the claim is handed
// to the accountant's observation-based retirement, NOT released. An
// eager release would leave the VM unaccounted for until the lagging
// cluster snapshot catches up — precisely the overcommit window.
func TestClone_SuccessKeepsItsReservation(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	adm := newFakeAdmitter(map[string]uint64{"pve1": 32 * gib})
	m := newTestManager(t, st, &fakeProv{}, Config{
		Capacity: adm,
		Profiles: []ProfileSettings{{Name: "p", MemoryMB: 8192, MaxConcurrentRunners: 10}},
	})

	m.runClone("p", store.PoolKindWarm, false, nil, func() {})

	require.Equal(t, uint64(24*gib), adm.snapshotFree("pve1"),
		"the successful clone's 8 GiB stays committed")
	require.Zero(t, adm.released, "a successful clone must not release its reservation")
}

// TestClone_FailureReturnsItsReservation is the mirror image: when the
// clone dies and its VM is torn down, the memory must go back
// immediately rather than waiting out the reservation TTL.
func TestClone_FailureReturnsItsReservation(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	prov := &fakeProv{}
	prov.cloneErr = errors.New("qmclone exploded")
	adm := newFakeAdmitter(map[string]uint64{"pve1": 32 * gib})
	m := newTestManager(t, st, prov, Config{
		Capacity: adm,
		Profiles: []ProfileSettings{{Name: "p", MemoryMB: 8192, MaxConcurrentRunners: 10}},
	})

	m.runClone("p", store.PoolKindWarm, false, nil, func() {})

	require.Equal(t, uint64(32*gib), adm.snapshotFree("pve1"),
		"a failed clone must hand its capacity straight back")
	require.Equal(t, 1, adm.released)
}

// TestClone_ReservesAgainstTemplateNodeForLinkedClones guards an
// easy-to-miss detail: a linked clone ignores the selector's answer and
// lands on the template's node, so that is where the memory must be
// booked.
func TestClone_ReservesAgainstTemplateNodeForLinkedClones(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	adm := newFakeAdmitter(map[string]uint64{"pve1": 32 * gib, "tpl-node": 32 * gib})
	m := newTestManager(t, st, &fakeProv{}, Config{
		Capacity:     adm,
		LinkedClones: true,
		TemplateNode: "tpl-node",
		Profiles:     []ProfileSettings{{Name: "p", MemoryMB: 8192, MaxConcurrentRunners: 10}},
	})

	m.runClone("p", store.PoolKindWarm, false, nil, func() {})

	require.Equal(t, uint64(24*gib), adm.snapshotFree("tpl-node"),
		"the linked clone's memory belongs to the template's node")
	require.Equal(t, uint64(32*gib), adm.snapshotFree("pve1"),
		"the node the selector picked must not be charged")
}

// TestClone_ConcurrentDispatchesCannotOverspend: the reservation is the
// atomic gate, so N racing clones on a node that fits K produce K VMs.
func TestClone_ConcurrentDispatchesCannotOverspend(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	adm := newFakeAdmitter(map[string]uint64{"pve1": 32 * gib})
	m := newTestManager(t, st, &fakeProv{}, Config{
		Capacity: adm,
		Profiles: []ProfileSettings{{Name: "p", MemoryMB: 8192, MaxConcurrentRunners: 100}},
	})

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.runClone("p", store.PoolKindWarm, false, nil, func() {})
		}()
	}
	wg.Wait()

	rows, err := st.List()
	require.NoError(t, err)
	require.Len(t, rows, 4, "32 GiB / 8 GiB = exactly 4 admitted clones, never 5")
	require.Zero(t, adm.snapshotFree("pve1"))
}

// ---------- Gauges ----------

func TestPublishCapacityGauges(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	adm := newFakeAdmitter(map[string]uint64{"pve1": 8 * gib})
	adm.total["pve1"] = 32 * gib
	m := newTestManager(t, st, &fakeProv{}, Config{Capacity: adm})

	m.publishCapacityGauges(context.Background())

	require.Equal(t, float64(32*gib), testutil.ToFloat64(m.metrics.NodeMemoryTotalBytes.WithLabelValues("pve1")))
	require.Equal(t, float64(24*gib), testutil.ToFloat64(m.metrics.NodeMemoryCommittedBytes.WithLabelValues("pve1")))
	require.Equal(t, float64(8*gib), testutil.ToFloat64(m.metrics.NodeMemoryAvailableBytes.WithLabelValues("pve1")))
}

// ---------- planEviction ----------

func candidate(vmid int, memGiB uint64, warm bool, age time.Duration) evictionCandidate {
	return evictionCandidate{
		row: &store.VM{
			VMID:      vmid,
			Node:      "pve1",
			Profile:   "victim",
			CreatedAt: time.Now().Add(-age),
		},
		memBytes:  memGiB * gib,
		preferred: warm,
	}
}

func TestPlanEviction(t *testing.T) {
	t.Parallel()

	t.Run("nothing to do when the clone already fits", func(t *testing.T) {
		t.Parallel()
		victims, _ := planEviction(
			[]evictionCandidate{candidate(1, 16, true, time.Hour)}, 8*gib, 8*gib)
		require.Empty(t, victims, "capacity for both means evict nothing")
	})

	t.Run("prefers warm over hot", func(t *testing.T) {
		t.Parallel()
		hot := candidate(1, 16, false, 2*time.Hour) // older, but booted
		warm := candidate(2, 16, true, time.Minute) // newer, but parked
		victims, freed := planEviction([]evictionCandidate{hot, warm}, 16*gib, 0)
		require.Len(t, victims, 1)
		require.Equal(t, 2, victims[0].row.VMID, "a parked warm VM is cheaper to lose than a booted hot one")
		require.Equal(t, uint64(16*gib), freed)
	})

	t.Run("oldest first within a kind", func(t *testing.T) {
		t.Parallel()
		newer := candidate(1, 16, true, time.Minute)
		older := candidate(2, 16, true, time.Hour)
		victims, _ := planEviction([]evictionCandidate{newer, older}, 16*gib, 0)
		require.Len(t, victims, 1)
		require.Equal(t, 2, victims[0].row.VMID, "the oldest VM is closest to being recycled anyway")
	})

	t.Run("takes several victims when one is not enough", func(t *testing.T) {
		t.Parallel()
		victims, freed := planEviction([]evictionCandidate{
			candidate(1, 4, true, 3*time.Hour),
			candidate(2, 4, true, 2*time.Hour),
			candidate(3, 4, true, time.Hour),
		}, 12*gib, 2*gib)
		require.Len(t, victims, 3, "a 10 GiB gap needs all three 4 GiB VMs")
		require.Equal(t, uint64(12*gib), freed)
	})

	t.Run("refuses a plan that still would not fit", func(t *testing.T) {
		t.Parallel()
		victims, _ := planEviction([]evictionCandidate{
			candidate(1, 4, true, time.Hour),
		}, 16*gib, 0)
		require.Empty(t, victims,
			"destroying VMs that still leave the job unplaceable is pure loss")
	})

	t.Run("only takes what it needs", func(t *testing.T) {
		t.Parallel()
		victims, _ := planEviction([]evictionCandidate{
			candidate(1, 16, true, 2*time.Hour),
			candidate(2, 16, true, time.Hour),
		}, 16*gib, 0)
		require.Len(t, victims, 1, "one victim closes the gap; the second must survive")
	})
}

// ---------- Eviction ----------

// evictionManager builds a manager with two profiles: `small` holds idle
// VMs on a full node, `big` has queued demand that cannot be placed.
//
// The selector is wrapped in the REAL capacity wrapper, because that
// wrapper sits in front of the reservation on the clone path — a test
// that calls reserveCapacity directly cannot see whether the wrapper
// would have refused the dispatch before it ever got there.
func evictionManager(t *testing.T, adm *fakeAdmitter, evict bool) (*manager, *store.Store) {
	t.Helper()
	st := newTestStore(t)
	single, err := nodeselector.NewSingle("pve1")
	require.NoError(t, err)
	sel, err := nodeselector.NewCapacity(single, adm, []string{"pve1"})
	require.NoError(t, err)
	m := newTestManager(t, st, &fakeProv{}, Config{
		Capacity:           adm,
		EvictIdleForDemand: evict,
		Profiles: []ProfileSettings{
			{Name: "small", MemoryMB: 4096, MaxConcurrentRunners: 10},
			{Name: "big", MemoryMB: 16384, MaxConcurrentRunners: 10},
		},
	}, withSelector(sel))
	return m, st
}

// seedIdle inserts a VM row and tells the fake admitter how big it is,
// so eviction arithmetic in the test matches what the real accountant
// would compute from the cluster snapshot.
func seedIdle(t *testing.T, st *store.Store, adm *fakeAdmitter, vmid int, profile string, memGiB uint64, state store.State, age time.Duration) {
	t.Helper()
	require.NoError(t, st.Insert(&store.VM{
		VMID:      vmid,
		Node:      "pve1",
		Name:      fmt.Sprintf("gh-runner-test-%d", vmid),
		Profile:   profile,
		State:     state,
		PoolKind:  store.PoolKindWarm,
		CreatedAt: time.Now().Add(-age),
	}))
	adm.mu.Lock()
	defer adm.mu.Unlock()
	adm.vmSizes[vmid] = memGiB * gib
}

// TestEvict_DisabledByDefault: eviction is destructive and stays opt-in.
func TestEvict_DisabledByDefault(t *testing.T) {
	t.Parallel()
	// 12 GiB free; `big` needs 16, so the 4 GiB idle VM is genuinely in
	// the way — and must still survive, because eviction is off.
	adm := newFakeAdmitter(map[string]uint64{"pve1": 12 * gib})
	m, st := evictionManager(t, adm, false)
	seedIdle(t, st, adm, 10001, "small", 4, store.StateWarm, time.Hour)

	require.False(t, m.evictForDemand(context.Background(), m.profileOf("big")))
	row, err := st.Get(10001)
	require.NoError(t, err)
	require.Equal(t, store.StateWarm, row.State, "the idle VM must be untouched")
}

// TestEvict_NoopWhenThereIsRoomForBoth is the user-facing rule: only
// evict when the idle VM is actually in the way.
func TestEvict_NoopWhenThereIsRoomForBoth(t *testing.T) {
	t.Parallel()
	adm := newFakeAdmitter(map[string]uint64{"pve1": 16 * gib})
	m, st := evictionManager(t, adm, true)
	seedIdle(t, st, adm, 10001, "small", 4, store.StateWarm, time.Hour)

	require.False(t, m.evictForDemand(context.Background(), m.profileOf("big")),
		"16 GiB is free, so the queued job and the idle VM can coexist")
	row, err := st.Get(10001)
	require.NoError(t, err)
	require.Equal(t, store.StateWarm, row.State)
	require.Zero(t, adm.freeingCalls)
}

// TestEvict_ReclaimsIdleCapacityForQueuedDemand is the headline
// behaviour: an idle VM of one profile makes way for another profile's
// real job, and the freed memory is reserved so the victim's own
// profile cannot immediately take it back.
func TestEvict_ReclaimsIdleCapacityForQueuedDemand(t *testing.T) {
	t.Parallel()
	// 12 GiB free + a 4 GiB idle VM = exactly the 16 GiB `big` needs.
	adm := newFakeAdmitter(map[string]uint64{"pve1": 12 * gib})
	m, st := evictionManager(t, adm, true)
	seedIdle(t, st, adm, 10001, "small", 4, store.StateWarm, time.Hour)

	require.True(t, m.evictForDemand(context.Background(), m.profileOf("big")))

	row, err := st.Get(10001)
	require.NoError(t, err)
	require.Equal(t, store.StateDraining, row.State, "the victim is CAS'd out of the pool")
	require.Equal(t, []int{10001}, adm.lastFreeing,
		"the reservation must name the VM whose destruction pays for it")
	require.Equal(t, float64(1), testutil.ToFloat64(
		m.metrics.CapacityEvictions.WithLabelValues("test", "big", "small")))

	// The claim is parked for `big`, not returned to the general pool.
	require.Len(t, m.profileOf("big").evicted, 1)
	require.Empty(t, m.profileOf("small").evicted)
}

// TestEvict_ParkedReservationIsConsumedByTheNextClone closes the loop:
// the tick after the eviction spends the claim instead of racing for it.
func TestEvict_ParkedReservationIsConsumedByTheNextClone(t *testing.T) {
	t.Parallel()
	adm := newFakeAdmitter(map[string]uint64{"pve1": 12 * gib})
	m, st := evictionManager(t, adm, true)
	seedIdle(t, st, adm, 10001, "small", 4, store.StateWarm, time.Hour)
	require.True(t, m.evictForDemand(context.Background(), m.profileOf("big")))
	require.Zero(t, adm.snapshotFree("pve1"), "the eviction claimed everything it freed")

	before := adm.reserveCalls
	node, claim, ok := m.admitClone(context.Background(), m.profileOf("big"), store.PoolKindHot)
	require.True(t, ok, "the parked claim admits the clone even though the node reports no free bytes")
	require.Equal(t, "pve1", node, "the claim carries the node the eviction freed capacity on")
	require.NotNil(t, claim.release)
	require.Equal(t, before, adm.reserveCalls,
		"a parked claim must be consumed, not re-reserved")
	require.Empty(t, m.profileOf("big").evicted, "the claim is spent exactly once")
}

// TestEvict_ParkedReservationSurvivesTheSelector is the end-to-end
// version: drive a real clone dispatch, not reserveCapacity directly.
//
// The capacity wrapper runs BEFORE the reservation on the clone path,
// and after an eviction the node reports no free bytes (the parked
// claim holds them). If the dispatch consults the wrapper first it is
// refused with ErrNoCapacity, the parked claim is never reached, and
// the deferral re-arms eviction — so the next tick sacrifices another
// idle VM while the queued job still never gets one.
func TestEvict_ParkedReservationSurvivesTheSelector(t *testing.T) {
	t.Parallel()
	adm := newFakeAdmitter(map[string]uint64{"pve1": 12 * gib})
	m, st := evictionManager(t, adm, true)
	seedIdle(t, st, adm, 10001, "small", 4, store.StateWarm, time.Hour)

	require.True(t, m.evictForDemand(context.Background(), m.profileOf("big")))
	require.Zero(t, adm.snapshotFree("pve1"),
		"precondition: the eviction claimed everything it freed, so the node looks full")

	m.runClone("big", store.PoolKindHot, true, nil, func() {})

	rows, err := st.List()
	require.NoError(t, err)
	var forBig int
	for _, r := range rows {
		if r.Profile == "big" {
			forBig++
		}
	}
	require.Equal(t, 1, forBig,
		"the clone the eviction was performed for must actually be dispatched")
	require.Empty(t, m.profileOf("big").evicted, "the parked claim is spent")
	require.False(t, m.profileOf("big").capacityDeferredHot.Load(),
		"a dispatch that succeeded must not re-arm eviction and cost another idle VM")
}

// TestEvict_ParkedReservationIsNotSpentOnWarmRefill: the victim was
// destroyed so a queued JOB could run. Letting the evicting profile's
// own warm refill spend that memory instead would sacrifice the idle VM
// for nothing and leave the job still waiting.
func TestEvict_ParkedReservationIsNotSpentOnWarmRefill(t *testing.T) {
	t.Parallel()
	adm := newFakeAdmitter(map[string]uint64{"pve1": 12 * gib})
	m, st := evictionManager(t, adm, true)
	seedIdle(t, st, adm, 10001, "small", 4, store.StateWarm, time.Hour)
	require.True(t, m.evictForDemand(context.Background(), m.profileOf("big")))

	_, _, ok := m.admitClone(context.Background(), m.profileOf("big"), store.PoolKindWarm)
	require.False(t, ok, "a warm refill must not raid the claim reserved for queued demand")
	require.Len(t, m.profileOf("big").evicted, 1, "the claim must still be parked")

	// ...and the hot clone it was taken for still gets it.
	_, _, ok = m.admitClone(context.Background(), m.profileOf("big"), store.PoolKindHot)
	require.True(t, ok)
}

// TestEvict_SkipsBusyAndTransitionalRows: only genuinely idle VMs may be
// sacrificed. Destroying a VM mid-job — or one a worker is still driving
// — is the destructive behaviour this feature must never exhibit.
func TestEvict_SkipsBusyAndTransitionalRows(t *testing.T) {
	t.Parallel()
	adm := newFakeAdmitter(map[string]uint64{"pve1": 12 * gib})
	m, st := evictionManager(t, adm, true)

	// Every non-idle state, each holding enough memory that eviction
	// would happily take it if the state filter were wrong.
	states := []store.State{
		store.StateRunning, store.StateAssigned, store.StateBooting,
		store.StateProvisioning, store.StateDraining, store.StateDestroying,
		store.StatePoison, store.StateRecycling,
	}
	for i, state := range states {
		seedIdle(t, st, adm, 10100+i, "small", 4, state, time.Hour)
	}

	require.False(t, m.evictForDemand(context.Background(), m.profileOf("big")),
		"no idle candidate exists, so nothing may be destroyed")
	require.Zero(t, adm.freeingCalls, "no eviction plan may even be formed")
	for i, state := range states {
		row, err := st.Get(10100 + i)
		require.NoError(t, err)
		require.Equal(t, state, row.State, "row %d must be left exactly as it was", 10100+i)
	}
}

// TestEvict_RespectsAffinity: with a hard pin, the idle VMs on a node
// the profile can never land on must be left alone. Destroying them
// would free memory the queued job cannot use, and the deferral would
// re-arm eviction — grinding the pool down a VM per tick with no
// progress at all.
func TestEvict_RespectsAffinity(t *testing.T) {
	t.Parallel()
	// Two nodes. `big` is hard-pinned to pve-gpu, which has no idle VMs
	// and no room; pve1 is full of idle `small` VMs that are irrelevant
	// to it.
	adm := newFakeAdmitter(map[string]uint64{"pve1": 12 * gib, "pve-gpu": 0})
	st := newTestStore(t)
	all := []string{"pve1", "pve-gpu"}
	rr, err := nodeselector.NewRoundRobin(all)
	require.NoError(t, err)
	aff, err := nodeselector.NewAffinity(rr, []nodeselector.AffinityRule{{
		Match:       nodeselector.AffinitySelector{Profile: "big"},
		PreferNodes: []string{"pve-gpu"},
		Require:     true,
	}}, all)
	require.NoError(t, err)
	sel, err := nodeselector.NewCapacity(aff, adm, all)
	require.NoError(t, err)

	m := newTestManager(t, st, &fakeProv{}, Config{
		Capacity:           adm,
		EvictIdleForDemand: true,
		Profiles: []ProfileSettings{
			{Name: "small", MemoryMB: 4096, MaxConcurrentRunners: 10},
			{Name: "big", MemoryMB: 16384, MaxConcurrentRunners: 10},
		},
	}, withSelector(sel))
	seedIdle(t, st, adm, 10001, "small", 4, store.StateWarm, time.Hour)

	require.False(t, m.evictForDemand(context.Background(), m.profileOf("big")),
		"nothing on pve1 can help a profile pinned to pve-gpu")
	row, err := st.Get(10001)
	require.NoError(t, err)
	require.Equal(t, store.StateWarm, row.State,
		"an idle VM on an ineligible node must never be sacrificed")
	require.Zero(t, adm.freeingCalls)
}

// TestEvict_ParkedClaimRecheckedAgainstAntiAffinity: consuming a parked
// claim skips node SELECTION but must not skip its rules. Anti-affinity
// is a promise about a node's current occupants ("untrusted-PR runners
// never co-schedule with prod"), and up to parkedReservationTTL passes
// between parking a claim and spending it — long enough for a prod VM
// to arrive. The claim is dropped, not the promise.
func TestEvict_ParkedClaimRecheckedAgainstAntiAffinity(t *testing.T) {
	t.Parallel()
	adm := newFakeAdmitter(map[string]uint64{"pve1": 12 * gib})
	st := newTestStore(t)
	all := []string{"pve1"}
	single, err := nodeselector.NewSingle("pve1")
	require.NoError(t, err)
	aff, err := nodeselector.NewAffinity(single, []nodeselector.AffinityRule{{
		Match:            nodeselector.AffinitySelector{Profile: "big"},
		AntiAffinityWith: nodeselector.AffinitySelector{Profile: "prod"},
	}}, all)
	require.NoError(t, err)
	sel, err := nodeselector.NewCapacity(aff, adm, all)
	require.NoError(t, err)

	m := newTestManager(t, st, &fakeProv{}, Config{
		Capacity:           adm,
		EvictIdleForDemand: true,
		Profiles: []ProfileSettings{
			{Name: "small", MemoryMB: 4096, MaxConcurrentRunners: 10},
			{Name: "big", MemoryMB: 16384, MaxConcurrentRunners: 10},
			{Name: "prod", MemoryMB: 2048, MaxConcurrentRunners: 10},
		},
	}, withSelector(sel))
	seedIdle(t, st, adm, 10001, "small", 4, store.StateWarm, time.Hour)

	require.True(t, m.evictForDemand(context.Background(), m.profileOf("big")),
		"pve1 hosts no prod VM yet, so the eviction is legal")
	require.Len(t, m.profileOf("big").evicted, 1)

	// A prod VM lands on pve1 before the claim is spent.
	seedIdle(t, st, adm, 10002, "prod", 2, store.StateWarm, time.Minute)

	_, _, ok := m.admitClone(context.Background(), m.profileOf("big"), store.PoolKindHot)
	require.False(t, ok,
		"the parked claim must not place an anti-affine VM next to prod")
	require.Equal(t, 1, adm.released, "and the claim is handed back, not leaked")
}

// TestEvict_ParkedReservationsReleasedOnDrain: the accountant is shared
// with sibling scale sets, so a departing manager must not leave memory
// withheld from them.
func TestEvict_ParkedReservationsReleasedOnDrain(t *testing.T) {
	t.Parallel()
	adm := newFakeAdmitter(map[string]uint64{"pve1": 12 * gib})
	m, st := evictionManager(t, adm, true)
	seedIdle(t, st, adm, 10001, "small", 4, store.StateWarm, time.Hour)
	require.True(t, m.evictForDemand(context.Background(), m.profileOf("big")))

	m.releaseParkedReservations()
	require.Equal(t, 1, adm.released)
	require.Empty(t, m.profileOf("big").evicted)
}

// ---------- burstDeficit ----------

// TestBurstDeficit_SeparatesDemandFromTopUp is why the expression was
// extracted: eviction is destructive and must fire ONLY for real queued
// demand, never for baseline pool warmth.
func TestBurstDeficit_SeparatesDemandFromTopUp(t *testing.T) {
	t.Parallel()

	// An empty pool with no demand: hot_size wants VMs, but no job does.
	require.LessOrEqual(t, burstDeficit(Stats{}, 0, 0, 0, returningCredit{}), 0,
		"baseline top-up is not queued demand")

	// Two jobs queued, one VM busy, nothing idle: one job cannot run.
	require.Equal(t, 1, burstDeficit(Stats{Running: 1}, 0, 0, 2, returningCredit{}))

	// The same demand, but a hot VM is already waiting for it.
	require.LessOrEqual(t, burstDeficit(Stats{Running: 1, Hot: 1}, 0, 0, 2, returningCredit{}), 0)

	// ...or a clone is already in flight for it.
	require.LessOrEqual(t, burstDeficit(Stats{Running: 1}, 1, 0, 2, returningCredit{}), 0,
		"an in-flight clone already covers the queued job")
}

// TestBurstDeficit_MatchesComputeCloneNeeds pins the two consumers
// together: if they ever disagree, eviction fires for the wrong reason.
func TestBurstDeficit_MatchesComputeCloneNeeds(t *testing.T) {
	t.Parallel()
	// hotSize 0 removes the idle term, so needHot IS the burst term.
	stats := Stats{Running: 3}
	needHot, _ := computeCloneNeeds(stats, 0, 0, 0, 0, 0, 6, 100, returningCredit{})
	require.Equal(t, burstDeficit(stats, 0, 0, 6, returningCredit{}), needHot)
}
