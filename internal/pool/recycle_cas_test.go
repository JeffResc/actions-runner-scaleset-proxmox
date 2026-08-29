package pool

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/store"
)

// TestRecycleOldVMs_DoesNotDestroyAcquiredRow guards the snapshot-vs-update
// race in recycleOldVMs (#356/#360). The recycler lists Hot/Warm rows, then
// transitions each over-age row to Draining and destroys it. If a listed
// row is acquired for a job (Hot->Running) in the window between the list
// snapshot and the transition, an unconditional Update would clobber the
// running VM to Draining and destroy it mid-job. The CAS guard
// (UpdateState from the observed state) makes recycle skip any row that
// left Hot/Warm — so a row that reaches Running is never destroyed by the
// recycler, and never left in a torn state.
//
// The guarantee is structural: recycle's CAS (Hot->Draining) and the
// acquire's CAS (Hot->Running) are mutually exclusive on from=Hot, so at
// most one wins per row. This test drives the two concurrently across many
// rounds (run under -race) to confirm the invariant holds.
func TestRecycleOldVMs_DoesNotDestroyAcquiredRow(t *testing.T) {
	t.Parallel()
	const rounds = 30
	const n = 20
	for round := 0; round < rounds; round++ {
		st := newTestStore(t)
		fp := &fakeProv{}
		mgr := newTestManager(t, st, fp, Config{HotSize: 0, WarmSize: 0, VMMaxAge: time.Hour})

		old := time.Now().Add(-2 * time.Hour) // older than VMMaxAge
		base := 40000 + round*100
		vmids := make([]int, n)
		for i := range n {
			vmid := base + i
			vmids[i] = vmid
			require.NoError(t, st.Insert(&store.VM{
				VMID: vmid, Node: "pve1", Name: "old-hot", Profile: defaultProfileName,
				PoolKind: store.PoolKindHot, State: store.StateHot, CreatedAt: old,
			}))
		}

		var (
			amu      sync.Mutex
			acquired = map[int]bool{}
			wg       sync.WaitGroup
		)
		wg.Add(2)
		// The recycler.
		go func() {
			defer wg.Done()
			mgr.recycleOldVMs(defaultProfileName, time.Hour)
		}()
		// Concurrent acquires: Hot -> Running, racing the recycler.
		go func() {
			defer wg.Done()
			for _, vmid := range vmids {
				if ok, _ := st.UpdateState(vmid, store.StateHot, store.StateRunning, nil); ok {
					amu.Lock()
					acquired[vmid] = true
					amu.Unlock()
				}
			}
		}()
		wg.Wait()

		// Let any queued destroys drain: every row ends either Running
		// (acquire won) or destroyed (recycle won) — none stays Hot.
		require.Eventually(t, func() bool {
			hot, err := st.ListByProfileAndStates(defaultProfileName, store.StateHot)
			return err == nil && len(hot) == 0
		}, 3*time.Second, 5*time.Millisecond, "every row must resolve to Running or destroyed")

		destroyed := map[int]bool{}
		fp.mu.Lock()
		for _, v := range fp.destroys {
			destroyed[v] = true
		}
		fp.mu.Unlock()

		amu.Lock()
		for vmid := range acquired {
			require.Falsef(t, destroyed[vmid],
				"vmid %d was acquired (Running) but destroyed by the recycler (#356/#360)", vmid)
			row, err := st.Get(vmid)
			require.NoErrorf(t, err, "acquired vmid %d must still exist, not be destroyed", vmid)
			require.Equalf(t, store.StateRunning, row.State,
				"acquired vmid %d must remain Running, not be clobbered to Draining", vmid)
		}
		amu.Unlock()
	}
}
