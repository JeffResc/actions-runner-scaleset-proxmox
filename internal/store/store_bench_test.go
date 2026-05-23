package store_test

import (
	"fmt"
	"testing"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/store"
)

// benchSeed populates count rows split half-Hot, half-Assigned so
// ListByState benchmarks have a non-trivial workload while still
// fitting in memory for large b.N runs.
func benchSeed(b *testing.B, s *store.Store, count int) {
	b.Helper()
	for i := 0; i < count; i++ {
		state := store.StateHot
		if i%2 == 1 {
			state = store.StateAssigned
		}
		err := s.Insert(&store.VM{
			VMID:     20000 + i,
			Node:     "pve1",
			Name:     fmt.Sprintf("bench-%d", i),
			Profile:  "default",
			PoolKind: store.PoolKindHot,
			State:    state,
		})
		if err != nil {
			b.Fatalf("seed: %v", err)
		}
	}
}

// BenchmarkListByState measures the cost of ListByState over a
// populated store. The reconciler reads this on every tick;
// a O(N²) regression here would degrade leader behaviour under
// hundreds of runners.
func BenchmarkListByState(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			s, err := store.New()
			if err != nil {
				b.Fatalf("store.New: %v", err)
			}
			benchSeed(b, s, n)
			b.ResetTimer()
			for range b.N {
				if _, err := s.ListByState(store.StateHot); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkUpdateState measures the cost of a single state
// transition under increasing row count. The Acquire path calls
// this once per acquired VM (Hot → Assigned), so the per-call
// cost matters at burst scale.
func BenchmarkUpdateState(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			s, err := store.New()
			if err != nil {
				b.Fatalf("store.New: %v", err)
			}
			benchSeed(b, s, n)
			b.ResetTimer()
			for i := range b.N {
				vmid := 20000 + (i*2)%n // pick a Hot row (even index)
				// Flip Hot ↔ Assigned so consecutive iterations
				// don't all no-op against the same precondition.
				from, to := store.StateHot, store.StateAssigned
				if i%2 == 1 {
					from, to = store.StateAssigned, store.StateHot
				}
				if _, err := s.UpdateState(vmid, from, to, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkStats measures the cost of Store.Stats — the
// aggregated per-state count, read on every reconcile tick.
func BenchmarkStats(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			s, err := store.New()
			if err != nil {
				b.Fatalf("store.New: %v", err)
			}
			benchSeed(b, s, n)
			b.ResetTimer()
			for range b.N {
				if _, err := s.Stats(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
