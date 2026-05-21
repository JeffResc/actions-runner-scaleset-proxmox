package store_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/store"
)

func newVM(vmid int, st store.State) *store.VM {
	return &store.VM{
		VMID:     vmid,
		Node:     "pve1",
		Name:     "gh-runner-test",
		PoolKind: store.PoolKindHot,
		State:    st,
	}
}

func TestNew_EmptyStore(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	rows, err := s.List()
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestInsert_StampsTimestamps(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Insert(newVM(10001, store.StateProvisioning)))

	got, err := s.Get(10001)
	require.NoError(t, err)
	require.False(t, got.CreatedAt.IsZero())
	require.False(t, got.UpdatedAt.IsZero())
	require.False(t, got.StateSince.IsZero())
}

func TestInsert_DuplicateVMIDRejected(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Insert(newVM(10001, store.StateProvisioning)))
	err = s.Insert(newVM(10001, store.StateHot))
	require.Error(t, err)
	require.Contains(t, err.Error(), "already exists")
}

func TestGet_NotFoundReturnsErrNotFound(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	_, err = s.Get(99999)
	require.ErrorIs(t, err, store.ErrNotFound)
}

// TestUpdateState_HotToAssigned proves the CAS helper writes the new state
// and runs the mutator when the current state matches.
func TestUpdateState_HotToAssigned(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Insert(newVM(20001, store.StateHot)))

	ok, err := s.UpdateState(20001, store.StateHot, store.StateAssigned, func(v *store.VM) {
		v.JobID = 42
	})
	require.NoError(t, err)
	require.True(t, ok)

	got, err := s.Get(20001)
	require.NoError(t, err)
	require.Equal(t, store.StateAssigned, got.State)
	require.Equal(t, int64(42), got.JobID)
}

// TestUpdateState_RejectedWhenStateMismatched is the losing side of the CAS:
// the second caller observes ok=false (no error) because state already moved.
func TestUpdateState_RejectedWhenStateMismatched(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Insert(newVM(20001, store.StateHot)))

	// First CAS: succeeds.
	ok, err := s.UpdateState(20001, store.StateHot, store.StateAssigned, func(v *store.VM) {
		v.JobID = 42
	})
	require.NoError(t, err)
	require.True(t, ok)

	// Second CAS on the same row: state is no longer Hot, so this loses.
	ok, err = s.UpdateState(20001, store.StateHot, store.StateAssigned, func(v *store.VM) {
		v.JobID = 43
	})
	require.NoError(t, err)
	require.False(t, ok)

	// The losing CAS's mutator must NOT have taken effect.
	got, err := s.Get(20001)
	require.NoError(t, err)
	require.Equal(t, int64(42), got.JobID)
}

// TestUpdateState_RaceConcurrent fires N goroutines all attempting the
// Hot→Assigned transition. Exactly one must win.
func TestUpdateState_RaceConcurrent(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Insert(newVM(30001, store.StateHot)))

	const racers = 32
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		wins int
	)
	wg.Add(racers)
	for i := range racers {
		go func(jobID int64) {
			defer wg.Done()
			ok, err := s.UpdateState(30001, store.StateHot, store.StateAssigned, func(v *store.VM) {
				v.JobID = jobID
			})
			if err == nil && ok {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}(int64(100 + i))
	}
	wg.Wait()
	require.Equal(t, 1, wins, "exactly one CAS racer must win")
}

// TestUpdateState_StampsStateSince checks that successful transitions bump
// state_since (the field gh.Reconciler uses for grace windows).
func TestUpdateState_StampsStateSince(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Insert(newVM(40001, store.StateProvisioning)))

	got, err := s.Get(40001)
	require.NoError(t, err)
	initial := got.StateSince

	time.Sleep(5 * time.Millisecond)
	ok, err := s.UpdateState(40001, store.StateProvisioning, store.StateWarm, nil)
	require.NoError(t, err)
	require.True(t, ok)

	got, err = s.Get(40001)
	require.NoError(t, err)
	require.True(t, got.StateSince.After(initial), "state_since must advance on transition")
}

// TestUpdate_DoesNotTouchStateSince verifies that the non-CAS Update helper
// leaves state_since alone (a non-state mutation should not reset the timer).
func TestUpdate_DoesNotTouchStateSince(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Insert(newVM(40002, store.StateProvisioning)))

	got, err := s.Get(40002)
	require.NoError(t, err)
	initial := got.StateSince

	time.Sleep(5 * time.Millisecond)
	_, err = s.Update(40002, func(v *store.VM) { v.BootAttempts = 1 })
	require.NoError(t, err)

	got, err = s.Get(40002)
	require.NoError(t, err)
	require.Equal(t, 1, got.BootAttempts)
	require.True(t, got.StateSince.Equal(initial), "unrelated mutation must not reset state_since")
}

func TestUpdateStateIn_AcceptsAnyMember(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Insert(newVM(50001, store.StateAssigned)))

	ok, err := s.UpdateStateIn(50001,
		[]store.State{store.StateAssigned, store.StateHot},
		store.StateRunning,
		func(v *store.VM) { v.RunnerID = 9999 },
	)
	require.NoError(t, err)
	require.True(t, ok)

	got, err := s.Get(50001)
	require.NoError(t, err)
	require.Equal(t, store.StateRunning, got.State)
	require.Equal(t, int64(9999), got.RunnerID)
}

func TestUpdateStateIn_RejectedWhenStateNotInSet(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Insert(newVM(50002, store.StateRunning)))

	ok, err := s.UpdateStateIn(50002,
		[]store.State{store.StateAssigned, store.StateHot},
		store.StateRunning,
		nil,
	)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestListByState(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Insert(newVM(60001, store.StateHot)))
	require.NoError(t, s.Insert(newVM(60002, store.StateHot)))
	require.NoError(t, s.Insert(newVM(60003, store.StateWarm)))

	hot, err := s.ListByState(store.StateHot)
	require.NoError(t, err)
	require.Len(t, hot, 2)
}

func TestCountByPoolKindState(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	hot := newVM(60001, store.StateProvisioning)
	hot.PoolKind = store.PoolKindHot
	require.NoError(t, s.Insert(hot))

	warm := newVM(60002, store.StateProvisioning)
	warm.PoolKind = store.PoolKindWarm
	require.NoError(t, s.Insert(warm))

	n, err := s.CountByPoolKindState(store.PoolKindHot, store.StateProvisioning)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	n, err = s.CountByPoolKindState(store.PoolKindWarm, store.StateProvisioning)
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestStats_AllStates(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Insert(newVM(70001, store.StateHot)))
	require.NoError(t, s.Insert(newVM(70002, store.StateHot)))
	require.NoError(t, s.Insert(newVM(70003, store.StateAssigned)))

	stats, err := s.Stats()
	require.NoError(t, err)
	require.Equal(t, 2, stats[store.StateHot])
	require.Equal(t, 1, stats[store.StateAssigned])
	require.Equal(t, 0, stats[store.StateWarm])
	// Every known state has a (possibly zero) entry.
	require.Len(t, stats, len(store.AllStates))
}

func TestListExcludingStates(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Insert(newVM(80001, store.StateHot)))
	require.NoError(t, s.Insert(newVM(80002, store.StateDraining)))
	require.NoError(t, s.Insert(newVM(80003, store.StateDestroying)))

	rows, err := s.ListExcludingStates(store.StateDraining, store.StateDestroying)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, 80001, rows[0].VMID)
}

func TestUsedVMIDs_FiltersToRange(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Insert(newVM(10000, store.StateHot)))
	require.NoError(t, s.Insert(newVM(15000, store.StateHot)))
	require.NoError(t, s.Insert(newVM(20000, store.StateHot)))

	used, err := s.UsedVMIDs(10000, 19999)
	require.NoError(t, err)
	require.Len(t, used, 2)
	_, ok := used[10000]
	require.True(t, ok)
	_, ok = used[15000]
	require.True(t, ok)
}

func TestDelete_MissingIsNoop(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Delete(99999))
}

func TestDelete_RemovesRow(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Insert(newVM(90001, store.StateHot)))
	require.NoError(t, s.Delete(90001))
	_, err = s.Get(90001)
	require.ErrorIs(t, err, store.ErrNotFound)
}

func TestUpdate_MissingReturnsErrNotFound(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	_, err = s.Update(99999, func(v *store.VM) { v.BootAttempts = 1 })
	require.ErrorIs(t, err, store.ErrNotFound)
}

// TestUpdateState_MissingRowReturnsFalse: the CAS helper treats an unknown
// VMID like a state mismatch (ok=false, err=nil). This matches how callers
// (e.g. acquire/promote/markRunning) treat duplicate or already-destroyed
// rows — they expect a quiet no-op rather than a hard error.
func TestUpdateState_MissingRowReturnsFalse(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	ok, err := s.UpdateState(99999, store.StateHot, store.StateAssigned, nil)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestUpdateStateIn_MissingRowReturnsFalse(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	ok, err := s.UpdateStateIn(99999, []store.State{store.StateHot}, store.StateAssigned, nil)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestInsert_RejectsNonPositiveVMID(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.Error(t, s.Insert(&store.VM{VMID: 0, PoolKind: store.PoolKindHot, State: store.StateHot}))
	require.Error(t, s.Insert(&store.VM{VMID: -1, PoolKind: store.PoolKindHot, State: store.StateHot}))
}

// TestInsert_PreservesExplicitTimestamps: the insert helper only stamps
// CreatedAt/UpdatedAt/StateSince when they're zero. Callers (e.g. a future
// recover-from-Proxmox-with-known-ages path) that pre-populate timestamps
// must see them preserved.
func TestInsert_PreservesExplicitTimestamps(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	want := time.Now().Add(-1 * time.Hour)
	require.NoError(t, s.Insert(&store.VM{
		VMID:       11001,
		PoolKind:   store.PoolKindHot,
		State:      store.StateHot,
		CreatedAt:  want,
		UpdatedAt:  want,
		StateSince: want,
	}))
	got, err := s.Get(11001)
	require.NoError(t, err)
	require.True(t, got.CreatedAt.Equal(want))
	require.True(t, got.UpdatedAt.Equal(want))
	require.True(t, got.StateSince.Equal(want))
}

// TestList_ClonesRows: callers may mutate returned slices freely. The
// store must hand back copies, not the indexed objects.
func TestList_ClonesRows(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Insert(newVM(11002, store.StateHot)))

	rows, err := s.List()
	require.NoError(t, err)
	require.Len(t, rows, 1)
	// Mutate the returned slice; the store's internal copy must be unaffected.
	rows[0].JobID = 12345
	rows[0].State = store.StateAssigned

	fresh, err := s.Get(11002)
	require.NoError(t, err)
	require.Equal(t, int64(0), fresh.JobID, "mutating a List() result must not bleed back")
	require.Equal(t, store.StateHot, fresh.State)
}

// TestListByState_MultiStateConcatenates: passing multiple states returns
// every row in any of them (used by the manager's stuck-state sweep).
func TestListByState_MultiStateConcatenates(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Insert(newVM(11010, store.StateProvisioning)))
	require.NoError(t, s.Insert(newVM(11011, store.StateBooting)))
	require.NoError(t, s.Insert(newVM(11012, store.StateHot))) // not in the set

	rows, err := s.ListByState(store.StateProvisioning, store.StateBooting)
	require.NoError(t, err)
	require.Len(t, rows, 2)
}

func TestListByState_EmptyArgsReturnsNil(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Insert(newVM(11020, store.StateHot)))
	rows, err := s.ListByState()
	require.NoError(t, err)
	require.Nil(t, rows)
}

// TestCount_MatchesStateIndex sanity-checks the dedicated Count helper
// against ListByState's length for the same state.
func TestCount_MatchesStateIndex(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Insert(newVM(11030, store.StateHot)))
	require.NoError(t, s.Insert(newVM(11031, store.StateHot)))
	require.NoError(t, s.Insert(newVM(11032, store.StateWarm)))

	n, err := s.Count(store.StateHot)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	rows, err := s.ListByState(store.StateHot)
	require.NoError(t, err)
	require.Len(t, rows, n)
}

// TestUpdate_BumpsUpdatedAt: callers (e.g. the pool's reconcile loop)
// rely on UpdatedAt advancing on every mutation to drive the stuck-state
// sweep. Verify the helper stamps it unconditionally.
func TestUpdate_BumpsUpdatedAt(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)
	require.NoError(t, s.Insert(newVM(11040, store.StateHot)))

	before, err := s.Get(11040)
	require.NoError(t, err)
	time.Sleep(5 * time.Millisecond)

	_, err = s.Update(11040, func(v *store.VM) { v.BootAttempts = 1 })
	require.NoError(t, err)

	after, err := s.Get(11040)
	require.NoError(t, err)
	require.True(t, after.UpdatedAt.After(before.UpdatedAt))
}

// TestAcquireHot_OldestFirst confirms the selection policy.
func TestAcquireHot_OldestFirst(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, s.Insert(&store.VM{
		VMID: 20001, Node: "pve1", Name: "old", PoolKind: store.PoolKindHot,
		State: store.StateHot, CreatedAt: now.Add(-time.Hour),
	}))
	require.NoError(t, s.Insert(&store.VM{
		VMID: 20002, Node: "pve1", Name: "newer", PoolKind: store.PoolKindHot,
		State: store.StateHot, CreatedAt: now.Add(-time.Minute),
	}))

	row, err := s.AcquireHot(42, 10)
	require.NoError(t, err)
	require.Equal(t, 20001, row.VMID, "oldest Hot must be acquired first")
	require.Equal(t, store.StateAssigned, row.State)
	require.Equal(t, int64(42), row.JobID)
}

// TestAcquireHot_AtCapacity blocks new acquires when busy >= cap.
func TestAcquireHot_AtCapacity(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)

	require.NoError(t, s.Insert(&store.VM{
		VMID: 20100, Node: "pve1", PoolKind: store.PoolKindHot, State: store.StateAssigned,
	}))
	require.NoError(t, s.Insert(&store.VM{
		VMID: 20101, Node: "pve1", PoolKind: store.PoolKindHot, State: store.StateRunning,
	}))
	require.NoError(t, s.Insert(&store.VM{
		VMID: 20102, Node: "pve1", PoolKind: store.PoolKindHot, State: store.StateHot,
	}))

	// 2 busy, cap=2 → AtCapacity, no row claimed.
	_, err = s.AcquireHot(1, 2)
	require.ErrorIs(t, err, store.ErrAtCapacity)

	// The Hot row must remain Hot — AcquireHot must not have CAS'd it.
	row, err := s.Get(20102)
	require.NoError(t, err)
	require.Equal(t, store.StateHot, row.State)
}

// TestAcquireHot_NoneAvailable distinguishes "at capacity" from
// "literally no Hot rows".
func TestAcquireHot_NoneAvailable(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)

	_, err = s.AcquireHot(1, 10)
	require.ErrorIs(t, err, store.ErrNoneAvailable)
}

// TestAcquireHot_RaceRespectsCapacity is the load-bearing test for the
// CAS-inside-txn redesign: under concurrent Acquire load against more
// Hot rows than the cap allows, exactly `cap` claimers must win and the
// rest must see ErrAtCapacity. If the cap check ran OUTSIDE the CAS
// (the pre-fix design), all racers could pass the check and claim
// distinct rows — over-provisioning the pool.
func TestAcquireHot_RaceRespectsCapacity(t *testing.T) {
	t.Parallel()
	s, err := store.New()
	require.NoError(t, err)

	const hotRows = 20
	const cap = 5
	for i := range hotRows {
		require.NoError(t, s.Insert(&store.VM{
			VMID: 21000 + i, Node: "pve1", PoolKind: store.PoolKindHot,
			State: store.StateHot, CreatedAt: time.Now().Add(time.Duration(-i) * time.Second),
		}))
	}

	const racers = 50
	var (
		wg          sync.WaitGroup
		mu          sync.Mutex
		acquires    int
		atCapacity  int
		noAvailable int
		otherErrs   []error
	)
	wg.Add(racers)
	for i := range racers {
		go func(jobID int64) {
			defer wg.Done()
			_, err := s.AcquireHot(jobID, cap)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				acquires++
			case errors.Is(err, store.ErrAtCapacity):
				atCapacity++
			case errors.Is(err, store.ErrNoneAvailable):
				noAvailable++
			default:
				otherErrs = append(otherErrs, err)
			}
		}(int64(i))
	}
	wg.Wait()

	require.Empty(t, otherErrs, "unexpected errors: %v", otherErrs)
	require.Equal(t, cap, acquires, "exactly cap=%d racers must succeed; got %d", cap, acquires)
	require.Equal(t, racers-cap, atCapacity+noAvailable,
		"remaining racers must see AtCapacity or NoneAvailable")

	// Post-condition: exactly cap rows are now Assigned.
	assigned, err := s.ListByState(store.StateAssigned)
	require.NoError(t, err)
	require.Len(t, assigned, cap)
}
