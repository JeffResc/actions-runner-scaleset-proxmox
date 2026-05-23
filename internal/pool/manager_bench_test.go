package pool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/config"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/nodeselector"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/observability"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/store"
)

// benchManager builds a *manager wired the same way tests do, sized
// for benchmark workloads (wide VMID range, high concurrency cap).
// The provisioner is fakeProv (defined in manager_test.go) so no
// network or PVE calls happen on the hot path.
func benchManager(b *testing.B, st *store.Store, hotSeed int) *manager {
	b.Helper()
	sel, err := nodeselector.NewSingle("pve1")
	if err != nil {
		b.Fatalf("nodeselector: %v", err)
	}
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := Config{
		MaxConcurrentRunners: 100000,
		ReconcileInterval:    50 * time.Millisecond,
		VMIDRange:            config.VMIDRange{Min: 10000, Max: 999999},
		VMNamePrefix:         "gh-runner-bench-",
		TemplateNode:         "pve1",
		BootMaxAttempts:      3,
		ScaleSetName:         "bench",
	}
	mi, err := NewManager(cfg, st, &fakeProv{}, sel, log, metrics)
	if err != nil {
		b.Fatalf("NewManager: %v", err)
	}
	for i := 0; i < hotSeed; i++ {
		if err := st.Insert(&store.VM{
			VMID:     20000 + i,
			Node:     "pve1",
			Name:     fmt.Sprintf("bench-hot-%d", i),
			Profile:  defaultProfileName,
			PoolKind: store.PoolKindHot,
			State:    store.StateHot,
		}); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}
	return mi.(*manager)
}

// BenchmarkAcquire measures the Hot→Assigned CAS-inside-txn that
// fires on every JobStarted. The N=100 hot rows mirrors the
// production pool shape the issue called out.
func BenchmarkAcquire(b *testing.B) {
	st, err := store.New()
	if err != nil {
		b.Fatalf("store.New: %v", err)
	}
	// Seed enough Hot rows that the benchmark never exhausts the
	// pool. After each successful Acquire we recycle the row back
	// to Hot so the next iteration has work to do.
	mgr := benchManager(b, st, 1000)
	b.ResetTimer()
	for i := range b.N {
		vm, err := mgr.Acquire(context.Background(), int64(i)+1, 0)
		if err != nil {
			b.Fatalf("Acquire: %v", err)
		}
		// Flip the row back to Hot so the next iteration has a
		// candidate. Done via direct store.UpdateState to avoid
		// dragging the manager's destroy/refill paths into the
		// measurement.
		if _, err := st.UpdateState(vm.VMID, store.StateAssigned, store.StateHot, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAcquireParallel measures Acquire under concurrent
// callers — the realistic shape under burst load. Captures store
// contention regressions.
func BenchmarkAcquireParallel(b *testing.B) {
	st, err := store.New()
	if err != nil {
		b.Fatalf("store.New: %v", err)
	}
	mgr := benchManager(b, st, 5000)
	b.ResetTimer()
	var jobID int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			jobID++
			vm, err := mgr.Acquire(context.Background(), jobID, 0)
			if err != nil {
				// At-capacity is acceptable under heavy concurrency;
				// other errors fail the bench.
				if errors.Is(err, ErrAtCapacity) || errors.Is(err, ErrNoneAvailable) {
					continue
				}
				b.Fatalf("Acquire: %v", err)
			}
			if _, err := st.UpdateState(vm.VMID, store.StateAssigned, store.StateHot, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}
