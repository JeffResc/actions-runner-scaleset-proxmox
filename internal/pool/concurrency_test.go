package pool

import (
	"context"
	"sync"
	"testing"
	"time"

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
