package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/config"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/provisioner"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/store"
)

// TestSignalRefill_CoalescesBurstsToOnePending pins the
// coalescing contract: when the reconciler is not draining the
// refill channel, a burst of N SignalRefill calls produces at
// most one pending signal — not N. The channel is buffered at
// 1; SignalRefill uses select/default so the (N-1) overflow
// calls drop silently rather than block.
//
// A regression that removed the default branch would deadlock
// the second caller in a burst (the manager's own reconcile
// loop calls SignalRefill from inside the same goroutine that
// drains it). This test guards against that.
func TestSignalRefill_CoalescesBurstsToOnePending(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	mgr := newTestManager(t, st, &fakeProv{}, Config{})

	// Drain whatever the manager's constructor enqueued so the
	// channel starts empty.
	select {
	case <-mgr.refill:
	default:
	}

	// 100 rapid signals.
	for i := 0; i < 100; i++ {
		mgr.SignalRefill()
	}

	// Exactly one signal should be readable; the rest were
	// coalesced.
	select {
	case <-mgr.refill:
	default:
		t.Fatal("expected at least one buffered signal after the burst; SignalRefill must always leave one pending so the reconciler is guaranteed to wake")
	}
	select {
	case <-mgr.refill:
		t.Fatal("expected only ONE buffered signal — the (N-1) overflow calls must drop silently via select/default, not stack up in the channel")
	default:
	}
}

// TestSignalRefill_ConcurrentBurstDoesNotBlock pins that
// SignalRefill is safe to call from many goroutines at once,
// even when nothing is draining the channel. Without the
// select/default fallback, the second goroutine would block on
// the send and deadlock the test.
func TestSignalRefill_ConcurrentBurstDoesNotBlock(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	mgr := newTestManager(t, st, &fakeProv{}, Config{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for i := 0; i < 64; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					mgr.SignalRefill()
				}
			}()
		}
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("SignalRefill deadlocked under concurrent callers; coalescing select/default branch is missing")
	}
}

// TestAllocateVMID_RecoversAfterDestroyClearsSlot covers the
// audit-flagged recovery-after-exhaustion path (#203). Fill the
// VMID range; delete the row that holds the slot; allocateVMID
// must then return the freed VMID (subject to the cooldown
// check, which the fake provisioner trivially passes).
func TestAllocateVMID_RecoversAfterDestroyClearsSlot(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	mgr := newTestManager(t, st, &fakeProv{}, Config{
		VMIDRange: config.VMIDRange{Min: 30000, Max: 30001},
	})

	// Fill both slots.
	for _, vmid := range []int{30000, 30001} {
		require.NoError(t, st.Insert(&store.VM{
			VMID:     vmid,
			Node:     "pve1",
			Name:     "seed",
			Profile:  defaultProfileName,
			PoolKind: store.PoolKindHot,
			State:    store.StateHot,
		}))
	}

	_, err := mgr.allocateVMID(context.Background())
	require.Error(t, err, "with both slots used, allocateVMID must return the exhaustion error")

	// Free 30000 — simulates destroyAsync completing.
	require.NoError(t, st.Delete(30000))

	got, err := mgr.allocateVMID(context.Background())
	require.NoError(t, err, "allocator must recover once the slot is freed")
	require.Equal(t, 30000, got,
		"freed slot must be reused; if this fails the allocator is leaking range capacity after each destroy")
}

// TestAdopt_ConcurrentCallsAreSafe drives Adopt from multiple
// goroutines against the same store + same provisioner. The
// production race is between a leader's startup Adopt() pass
// and reconcile loops that are already firing; the property to
// pin is "no double-insert". The store's unique-VMID constraint
// causes the second insert to error, which adoptOne logs and
// skips — so the first adoption wins and subsequent racers are
// no-ops.
func TestAdopt_ConcurrentCallsAreSafe(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{
		listOwnedRet: []*provisioner.VM{
			{VMID: 40001, Node: "pve1", Name: "gh-runner-test-40001"},
			{VMID: 40002, Node: "pve1", Name: "gh-runner-test-40002"},
			{VMID: 40003, Node: "pve1", Name: "gh-runner-test-40003"},
		},
	}
	mgr := newTestManager(t, st, fp, Config{
		VMIDRange: config.VMIDRange{Min: 40000, Max: 40999},
	})

	const racers = 8
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Adopt itself returns nil on per-VM errors (they
			// are logged + skipped). We only assert the
			// aggregate observation below.
			_ = mgr.Adopt(context.Background())
		}()
	}
	wg.Wait()

	rows, err := mgr.ListRows(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 3,
		"concurrent Adopts must not produce duplicate rows; store unique-VMID constraint catches the second writer and adoptOne swallows it")
	seen := make(map[int]struct{}, len(rows))
	for _, r := range rows {
		require.NotContains(t, seen, r.VMID, "duplicate VMID %d in rows", r.VMID)
		seen[r.VMID] = struct{}{}
	}
}

// TestAllocateVMIDAndInsertRow_ReleasesLockOnPanic pins the fix
// for the allocMu lock-leak bug. Before the fix, runClone took
// allocMu manually and unlocked via three explicit Unlock() calls;
// a panic anywhere between Lock() and the final Unlock() (e.g.
// inside allocateVMID's call to Provisioner.IsRecentlyDestroyed)
// left the mutex held forever, deadlocking every subsequent clone.
//
// The fix is `defer m.allocMu.Unlock()`. This test injects a panic
// via the fake provisioner's IsRecentlyDestroyed, recovers it, and
// asserts the mutex is now free — proving the defer fired even
// though the function exited abnormally.
func TestAllocateVMIDAndInsertRow_ReleasesLockOnPanic(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{isRecentlyDestroyedPanic: true}
	mgr := newTestManager(t, st, fp, Config{
		VMIDRange: config.VMIDRange{Min: 60000, Max: 60099},
	})

	ps := mgr.profileOf("")
	require.NotNil(t, ps, "default profile must exist")

	func() {
		defer func() {
			r := recover()
			require.NotNil(t, r, "fake provisioner must have panicked inside the locked section")
		}()
		_, _, _, _, _ = mgr.allocateVMIDAndInsertRow(context.Background(), ps, store.PoolKindHot, "pve1")
	}()

	require.True(t, mgr.allocMu.TryLock(),
		"allocMu must be free after a panic inside the locked section; the defer in allocateVMIDAndInsertRow is what guarantees this")
	mgr.allocMu.Unlock()
}

// TestLogRecoveredPanic_IncrementsPanicMetric pins #254: every
// panic caught by the pool's recover() guards must bump
// scaleset_panics_recovered_total{op}. Operators alert on
// rate(panics_recovered_total[5m]) > 0 — silent panics in worker
// goroutines historically masked real bugs (the allocMu lock-leak
// regression caught by [TestAllocateVMIDAndInsertRow_ReleasesLockOnPanic]
// is the canonical example).
func TestLogRecoveredPanic_IncrementsPanicMetric(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	mgr := newTestManager(t, st, &fakeProv{}, Config{})

	// Drive the helper directly with a non-nil panic value and
	// assert the counter ticks. Going through a real goroutine +
	// defer + recover sequence is overkill — logRecoveredPanic is
	// the single chokepoint every site funnels through.
	mgr.logRecoveredPanic("test-op", 42, "simulated panic")

	// Counter labels: (scaleset, op). newTestManager defaults
	// ScaleSetName to "test".
	got := promtestutil.ToFloat64(mgr.metrics.PanicsRecovered.WithLabelValues("test", "test-op"))
	require.InEpsilon(t, 1.0, got, 1e-9,
		"a recovered panic must increment scaleset_panics_recovered_total once")

	// A nil panic value (caller's recover() returned nil because
	// no panic was in flight) must NOT increment.
	mgr.logRecoveredPanic("test-op", 42, nil)
	got = promtestutil.ToFloat64(mgr.metrics.PanicsRecovered.WithLabelValues("test", "test-op"))
	require.InEpsilon(t, 1.0, got, 1e-9,
		"nil-panic call must be a no-op; counter must still be 1")
}

// TestRapidStateCycling drives a single VMID through the full
// transient state cycle the audit flagged: Hot → Assigned →
// Running → Draining → Destroyed. Each step is asserted to
// produce the expected store row; a regression that skipped a
// transition silently would surface as a missing or wrong row
// state at one of the checkpoints.
func TestRapidStateCycling(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, st.Insert(&store.VM{
		VMID:     50000,
		Node:     "pve1",
		Name:     "gh-runner-test-50000",
		Profile:  defaultProfileName,
		PoolKind: store.PoolKindHot,
		State:    store.StateHot,
	}))

	// Hot → Assigned (the AcquireHot transition).
	row, err := st.AcquireHot(123, 10, 0)
	require.NoError(t, err)
	require.Equal(t, 50000, row.VMID)
	require.Equal(t, store.StateAssigned, row.State)

	// Assigned → Running.
	ok, err := st.UpdateState(50000, store.StateAssigned, store.StateRunning, nil)
	require.NoError(t, err)
	require.True(t, ok)

	// Running → Draining.
	ok, err = st.UpdateState(50000, store.StateRunning, store.StateDraining, nil)
	require.NoError(t, err)
	require.True(t, ok)

	// Draining → Delete (terminal).
	require.NoError(t, st.Delete(50000))

	// Verify the row is gone.
	_, err = st.Get(50000)
	require.Error(t, err, "row must be deleted after the full cycle")
}

// TestAllocateVMID_NoCollisionAcrossDisjointRanges pins the
// per-scaleset VMID range race fix from PR #223. Two pool managers
// configured with adjacent, disjoint VMID ranges (gap of one) drive
// many concurrent allocateVMID calls. A regression that reintroduced
// shared allocator state across managers would either return out-of-
// range IDs or — worse — mint the same ID from both managers,
// corrupting the Proxmox cluster on the next Clone.
//
// The fix is structural (per-manager allocator scoped to the
// manager's VMIDRange); this test guards against future refactors
// (shared free-list optimization, cross-scaleset rebalance) that
// could silently break it.
func TestAllocateVMID_NoCollisionAcrossDisjointRanges(t *testing.T) {
	t.Parallel()

	const (
		// Two adjacent ranges with a one-id gap to make boundary errors
		// (off-by-one returns from either side) detectable.
		aMin, aMax = 70100, 70199
		bMin, bMax = 70201, 70300
	)
	stA := newTestStore(t)
	stB := newTestStore(t)
	mgrA := newTestManager(t, stA, &fakeProv{}, Config{
		VMIDRange: config.VMIDRange{Min: aMin, Max: aMax},
	})
	mgrB := newTestManager(t, stB, &fakeProv{}, Config{
		VMIDRange: config.VMIDRange{Min: bMin, Max: bMax},
	})

	const workersPerMgr = 8
	const callsPerWorker = 12

	type result struct {
		mgr  string
		vmid int
	}
	results := make(chan result, 2*workersPerMgr*callsPerWorker)

	allocator := func(name string, m *manager, st *store.Store) {
		var wg sync.WaitGroup
		for w := 0; w < workersPerMgr; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < callsPerWorker; i++ {
					id, err := m.allocateVMID(context.Background())
					if err != nil {
						// Range may be momentarily saturated; that's fine,
						// just don't insert and don't record.
						continue
					}
					// Insert into the store so subsequent calls in this
					// manager see the id as taken; this is what the real
					// runClone does immediately after allocateVMID.
					if insErr := st.Insert(&store.VM{
						VMID:     id,
						Node:     "pve1",
						Name:     "stress",
						Profile:  defaultProfileName,
						PoolKind: store.PoolKindHot,
						State:    store.StateProvisioning,
					}); insErr != nil {
						// Lost the race against another worker on the same
						// manager — discard and continue.
						continue
					}
					results <- result{mgr: name, vmid: id}
				}
			}()
		}
		wg.Wait()
	}

	var wgManagers sync.WaitGroup
	wgManagers.Add(2)
	go func() { defer wgManagers.Done(); allocator("A", mgrA, stA) }()
	go func() { defer wgManagers.Done(); allocator("B", mgrB, stB) }()
	wgManagers.Wait()
	close(results)

	seen := make(map[int]string, 2*workersPerMgr*callsPerWorker)
	for r := range results {
		// Owning-range check: A's ids in [aMin..aMax], B's in [bMin..bMax].
		switch r.mgr {
		case "A":
			require.GreaterOrEqual(t, r.vmid, aMin, "manager A returned id %d outside its range", r.vmid)
			require.LessOrEqual(t, r.vmid, aMax, "manager A returned id %d outside its range", r.vmid)
		case "B":
			require.GreaterOrEqual(t, r.vmid, bMin, "manager B returned id %d outside its range", r.vmid)
			require.LessOrEqual(t, r.vmid, bMax, "manager B returned id %d outside its range", r.vmid)
		}
		// No-collision-across-managers check: same vmid must not appear
		// from both managers. Same-vmid same-manager is impossible
		// because the store insert above is the dedup.
		if prev, dup := seen[r.vmid]; dup {
			require.Equal(t, prev, r.mgr,
				"vmid %d minted by both manager %q and manager %q — VMID-range isolation regressed",
				r.vmid, prev, r.mgr)
		}
		seen[r.vmid] = r.mgr
	}
}

// TestDrain_ReleasesDestroySemTokensOnTimeout pins the invariant
// that drain's force-cancel path releases every destroySem token
// held by in-flight workers. Without that release, a subsequent
// drain (or a test that reuses the manager) would block forever
// because the semaphore is full of orphaned tokens.
//
// Before any related future refactor of the destroy worker
// goroutine, this test catches a regression where the deferred
// destroySem.Release isn't reached on ctx-cancel exit paths.
//
// Approach: queue more destroys than the semaphore allows, hang
// them all on a Destroy that blocks on ctx, trigger drain with a
// short timeout, then verify TryAcquire(maxConcurrentDestroys)
// succeeds — proving every token was returned.
func TestDrain_ReleasesDestroySemTokensOnTimeout(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	fp := &fakeProv{destroyHang: true}

	// Seed 20 Assigned rows so MarkCompleted has work to queue.
	const seeded = 20
	for i := 0; i < seeded; i++ {
		require.NoError(t, st.Insert(&store.VM{
			VMID:     80000 + i,
			Node:     "pve1",
			Name:     "stuck",
			Profile:  defaultProfileName,
			PoolKind: store.PoolKindHot,
			State:    store.StateAssigned,
		}))
	}

	mgr := newTestManager(t, st, fp, Config{
		DrainTimeout: 100 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- mgr.Run(ctx) }()

	// Queue all 20 destroys. Semaphore cap is 8, so at most 8 land
	// in `<-ctx.Done()` simultaneously; the rest sit on Acquire.
	for i := 0; i < seeded; i++ {
		require.NoError(t, mgr.MarkCompleted(context.Background(), 80000+i))
	}

	// Give the dispatcher a moment to acquire semaphore tokens for
	// the first batch and launch the hanging Destroy calls.
	require.Eventually(t, func() bool {
		fp.mu.Lock()
		defer fp.mu.Unlock()
		return len(fp.destroys) > 0
	}, time.Second, 10*time.Millisecond, "no destroys ever entered the hang path")

	// Trigger drain. DrainTimeout (100ms) should fire because the
	// hanging Destroys won't return until workerCtx cancels them.
	cancel()
	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after drain timeout + worker cancel")
	}

	// THE invariant: every destroySem token released. If this fails,
	// a future caller / a re-Run of the manager would deadlock on
	// the first Acquire.
	require.True(t, mgr.destroySem.TryAcquire(8),
		"destroySem still holds tokens after drain — workers leaked their slots on the ctx-cancel path")
	mgr.destroySem.Release(8)
}

// TestAcquire_NoDoubleAssignmentOnHeadOfQueueCollision pins
// the canonical double-assignment hazard described in issue #285:
// when N callers race to Acquire from a Hot pool containing
// fewer rows than callers, AcquireHotInProfile's "count busy +
// CAS to Assigned" must happen inside ONE write txn so no two
// callers walk away with the same VMID. The previous stress
// test seeded N random VMIDs and so did not target this collision
// shape; this test seeds exactly K Hot rows and fires N >> K
// concurrent Acquires to maximise head-of-queue contention.
func TestAcquire_NoDoubleAssignmentOnHeadOfQueueCollision(t *testing.T) {
	t.Parallel()
	const (
		hotSeeded  = 8
		acquirers  = 200
		maxRunners = hotSeeded // cap equals seeded count: every slot must land somewhere
	)
	st := newTestStore(t)
	seedHot(t, st, hotSeeded)
	mgr := newTestManager(t, st, &fakeProv{}, Config{
		HotSize:              hotSeeded,
		MaxConcurrentRunners: maxRunners,
	})

	var (
		wg          sync.WaitGroup
		startGate   sync.WaitGroup
		mu          sync.Mutex
		acquiredIDs = map[int]int64{} // VMID → JobID of the winning Acquire
		duplicates  []int
	)
	startGate.Add(1)

	for i := range acquirers {
		wg.Add(1)
		go func(jobID int64) {
			defer wg.Done()
			startGate.Wait()
			got, err := mgr.Acquire(context.Background(), jobID, 0)
			if err != nil {
				return // ErrAtCapacity / ErrNoneAvailable both fine
			}
			mu.Lock()
			defer mu.Unlock()
			if prev, ok := acquiredIDs[got.VMID]; ok {
				duplicates = append(duplicates, got.VMID)
				t.Errorf("VMID %d double-assigned: jobs %d and %d", got.VMID, prev, jobID)
			}
			acquiredIDs[got.VMID] = jobID
		}(int64(1000 + i))
	}
	startGate.Done()
	wg.Wait()

	require.Empty(t, duplicates, "no VMID may be assigned to more than one job (issue #285)")
	require.LessOrEqual(t, len(acquiredIDs), hotSeeded,
		"the number of Acquire winners must not exceed the seeded Hot count")
}

// TestAcquire_GlobalCapNeverOverCommitted pins the
// "no over-commit under contention" invariant for the
// AcquireHotInProfile cap check (issue #286). Seeds Hot rows
// equal to MaxConcurrentRunners and fires N >> cap concurrent
// Acquires — the busy-count + CAS must happen inside one write
// txn so no burst lands more winners than the cap allows.
func TestAcquire_GlobalCapNeverOverCommitted(t *testing.T) {
	t.Parallel()
	const (
		hotSeeded  = 8
		maxRunners = 8 // cap == seeded; every slot must land somewhere
		acquirers  = 200
	)
	st := newTestStore(t)
	seedHot(t, st, hotSeeded)
	mgr := newTestManager(t, st, &fakeProv{}, Config{
		HotSize:              hotSeeded,
		MaxConcurrentRunners: maxRunners,
	})

	var (
		wg        sync.WaitGroup
		startGate sync.WaitGroup
		winners   atomic.Int32
	)
	startGate.Add(1)

	for i := range acquirers {
		wg.Add(1)
		go func(jobID int64) {
			defer wg.Done()
			startGate.Wait()
			got, err := mgr.Acquire(context.Background(), jobID, 0)
			if err == nil && got != nil {
				winners.Add(1)
			}
		}(int64(2000 + i))
	}
	startGate.Done()
	wg.Wait()

	got := int(winners.Load())
	require.Equal(t, maxRunners, got,
		"AcquireHotInProfile must enforce the global cap inside one txn — over-commit under burst is the canonical memdb-isolation bug (issue #286)")
}

// TestAcquire_PerProfileCapNeverOverCommitted exercises the
// per-profile branch of AcquireHotInProfile (the second
// countBusy call): under burst contention, the per-profile
// cap must hold even when the global cap is permissive.
// Without the in-txn re-count, two callers' read-then-write
// could each claim slot K+1 of a K-cap profile (issue #286).
func TestAcquire_PerProfileCapNeverOverCommitted(t *testing.T) {
	t.Parallel()
	const (
		seededPerProfile = 4
		maxBusyProfile   = 4
		acquirers        = 200
	)
	st := newTestStore(t)
	// Seed the "gpu" profile with HotSize == maxBusy so the
	// only ceiling under test is the per-profile cap.
	const targetProfile = "gpu"
	for i := range seededPerProfile {
		require.NoError(t, st.Insert(&store.VM{
			VMID:     90000 + i,
			Node:     "pve1",
			Name:     "gpu-hot",
			Profile:  targetProfile,
			PoolKind: store.PoolKindHot,
			State:    store.StateHot,
		}))
	}
	mgr := newTestManager(t, st, &fakeProv{}, Config{
		HotSize:              seededPerProfile,
		MaxConcurrentRunners: 1000, // global cap permissive; profile cap is what's under test
		Profiles: []ProfileSettings{{
			Name:                 targetProfile,
			HotSize:              seededPerProfile,
			MaxConcurrentRunners: maxBusyProfile,
		}},
	})

	var (
		wg        sync.WaitGroup
		startGate sync.WaitGroup
		winners   atomic.Int32
	)
	startGate.Add(1)
	for i := range acquirers {
		wg.Add(1)
		go func(jobID int64) {
			defer wg.Done()
			startGate.Wait()
			got, err := mgr.AcquireForProfile(context.Background(), jobID, targetProfile, maxBusyProfile)
			if err == nil && got != nil {
				winners.Add(1)
			}
		}(int64(3000 + i))
	}
	startGate.Done()
	wg.Wait()

	got := int(winners.Load())
	require.Equal(t, maxBusyProfile, got,
		"AcquireHotInProfile must enforce the per-profile cap inside one txn — count + CAS happening across txns would allow over-commit under burst (issue #286)")
}

// TestMarkCompletedRacesAcquire_NoStateCorruption pins the
// MarkCompleted vs Acquire contention scenario from issue #285:
// a JobCompleted event arrives for VMID X while concurrent
// Acquire calls are mid-flight. The store's state-transition
// matrix must keep every VMID in exactly one of {Assigned,
// Running, Draining, Destroying, Destroyed} at observation time,
// never two — the bug class where a Destroying row is also
// reachable from Acquire.
func TestMarkCompletedRacesAcquire_NoStateCorruption(t *testing.T) {
	t.Parallel()
	const (
		hotSeeded  = 16
		runningIDs = 8
		acquirers  = 100
		maxRunners = hotSeeded + runningIDs
	)
	st := newTestStore(t)
	seedHot(t, st, hotSeeded)
	// Seed runningIDs rows in StateRunning so MarkCompleted has work.
	for i := range runningIDs {
		require.NoError(t, st.Insert(&store.VM{
			VMID:     70000 + i,
			Node:     "pve1",
			Name:     "running",
			Profile:  defaultProfileName,
			PoolKind: store.PoolKindHot,
			State:    store.StateRunning,
		}))
	}
	mgr := newTestManager(t, st, &fakeProv{destroyHang: false}, Config{
		HotSize:              hotSeeded,
		MaxConcurrentRunners: maxRunners,
		DrainTimeout:         100 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- mgr.Run(ctx) }()

	var (
		wg          sync.WaitGroup
		startGate   sync.WaitGroup
		mu          sync.Mutex
		acquiredIDs = map[int]int64{}
		duplicates  []int
	)
	startGate.Add(1)

	// Acquire racers.
	for i := range acquirers {
		wg.Add(1)
		go func(jobID int64) {
			defer wg.Done()
			startGate.Wait()
			got, err := mgr.Acquire(context.Background(), jobID, 0)
			if err != nil || got == nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if prev, ok := acquiredIDs[got.VMID]; ok {
				duplicates = append(duplicates, got.VMID)
				t.Errorf("VMID %d double-assigned during Acquire/MarkCompleted race: jobs %d and %d",
					got.VMID, prev, jobID)
			}
			acquiredIDs[got.VMID] = jobID
		}(int64(4000 + i))
	}
	// MarkCompleted racers.
	for i := range runningIDs {
		wg.Add(1)
		go func(vmid int) {
			defer wg.Done()
			startGate.Wait()
			require.NoError(t, mgr.MarkCompleted(context.Background(), vmid))
		}(70000 + i)
	}
	startGate.Done()
	wg.Wait()
	cancel()
	<-runDone

	require.Empty(t, duplicates,
		"MarkCompleted concurrent with Acquire must not produce a double-assigned VMID (issue #285)")
	// Every Running row should now be Draining or Destroying — never
	// re-acquired as Hot.
	for i := range runningIDs {
		row, err := st.Get(70000 + i)
		if errors.Is(err, store.ErrNotFound) {
			continue // already destroyed
		}
		require.NoError(t, err)
		require.NotEqual(t, store.StateHot, row.State,
			"VMID %d transitioned back to Hot after MarkCompleted — state machine broken", row.VMID)
		require.NotEqual(t, store.StateAssigned, row.State,
			"VMID %d transitioned back to Assigned after MarkCompleted — would double-assign on next Acquire", row.VMID)
	}
}
