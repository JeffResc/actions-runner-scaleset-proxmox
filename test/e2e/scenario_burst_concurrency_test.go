//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakegithub"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakeproxmox"
)

// TestE2E_SustainedHighConcurrency pushes the orchestrator past
// the existing TestE2E_ConcurrentJobs (3 jobs) to the
// audit-flagged 10+ queued shape. Drives 12 concurrent
// assignments against a 15-runner cap and asserts:
//   - the pool reaches 12 Assigned VMs (no under-provisioning).
//   - the live "assigned" metric never exceeds `jobs` (no
//     over-Acquire under burst).
//   - every job moves Assigned -> Running once we post the
//     per-VM JobStarted, and each runner ID is deregistered on
//     JobCompleted.
//
// Without this, a regression that doubled Acquire calls under
// sustained load (e.g. a refill-coalescing or busy-clamp bug
// regressed) would only surface in production once the queue
// depth exceeded the existing test's tiny N=3.
func TestE2E_SustainedHighConcurrency(t *testing.T) {
	t.Parallel()
	const (
		jobs          = 12
		maxConcurrent = 15
	)
	proxmox := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
	gh := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{Name: "burst-set"},
	})
	gh.SetStatistics(fakegithub.Statistics{TotalAssignedJobs: jobs})

	h := Start(t, Options{
		HotSize:              jobs,
		MaxConcurrentRunners: maxConcurrent,
		ScaleSetName:         "burst-set",
		FakeProxmox:          proxmox,
		FakeGitHub:           gh,
	})

	// While we wait for the pool to land on the desired count,
	// sample the live `assigned` metric and fail loud if it ever
	// exceeds `jobs`. The pool's Acquire CAS is supposed to clamp
	// against MaxConcurrentRunners + the listener's desired count;
	// a regression here would briefly let assigned cross `jobs`
	// before settling — exactly the foot-gun the audit flagged.
	// (Counting tagged VMs in the Proxmox snapshot would be wrong:
	// destroying / refill-Hot VMs also carry the owner tag and
	// briefly inflate the total during steady-state churn.)
	overshoot := make(chan float64, 1)
	stopWatcher := make(chan struct{})
	defer close(stopWatcher)
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopWatcher:
				return
			case <-ticker.C:
				if n := h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "assigned")); n > jobs {
					select {
					case overshoot <- n:
					default:
					}
					return
				}
			}
		}
	}()

	// Wait for the pool to settle at exactly `jobs` Assigned VMs.
	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "assigned")) >= jobs
	}, 60*time.Second, 200*time.Millisecond,
		"orchestrator never transitioned %d VMs to Assigned (saw %g); sustained-concurrency regression?",
		jobs, h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "assigned")))

	// Surface any overshoot the watcher captured.
	select {
	case n := <-overshoot:
		t.Fatalf("assigned overshoot: saw %g Assigned VMs (listener requested %d); Acquire over-fired under sustained load", n, jobs)
	default:
	}

	vms := awaitNAssignedVMs(t, h, "burst-set", jobs)

	// Use each VM's JIT-mint ID as the runner ID. See the matching
	// comment in TestE2E_ConcurrentJobs: scaler.SetRunnerID stamps
	// that value on the row before any test message lands, so
	// RunnerDeletions are deterministic even when the gh.Reconciler
	// wins the Assigned -> Running CAS race against pool.MarkRunning.
	runnerIDs := make([]int64, jobs)
	for i, vm := range vms {
		var minted int
		require.Eventuallyf(t, func() bool {
			id, ok := gh.JITMintIDForRunner(vm.name)
			if ok {
				minted = id
			}
			return ok
		}, 30*time.Second, 100*time.Millisecond,
			"no JIT mint observed for runner %q", vm.name)
		runnerIDs[i] = int64(minted)
		gh.SetRunner(fakegithub.Runner{
			ID:     runnerIDs[i],
			Name:   vm.name,
			Status: "online",
			Busy:   true,
		})
	}
	for i, vm := range vms {
		require.NoError(t, gh.PostJobStarted(vm.name, int(runnerIDs[i])))
	}

	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "running")) >= jobs
	}, 60*time.Second, 200*time.Millisecond,
		"orchestrator never transitioned %d VMs to Running (saw %g)",
		jobs, h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "running")))

	// Drain by clearing the listener's assignment count and
	// posting per-VM JobCompleted. Every original VM must be
	// destroyed and every test-issued runner ID must be
	// deregistered with GitHub.
	gh.SetStatistics(fakegithub.Statistics{})
	for i, vm := range vms {
		require.NoError(t, gh.PostJobCompleted(vm.name, int(runnerIDs[i])))
	}

	require.Eventually(t, func() bool {
		live := make(map[int]struct{})
		for _, vm := range proxmox.Snapshot() {
			live[vm.VMID] = struct{}{}
		}
		for _, vm := range vms {
			if _, alive := live[vm.vmid]; alive {
				return false
			}
		}
		return true
	}, 60*time.Second, 200*time.Millisecond,
		"not all %d original VMs were destroyed after JobCompleted", jobs)

	require.Eventually(t, func() bool {
		deleted := make(map[int64]struct{})
		for _, id := range gh.RunnerDeletions() {
			deleted[id] = struct{}{}
		}
		for _, want := range runnerIDs {
			if _, ok := deleted[want]; !ok {
				return false
			}
		}
		return true
	}, 10*time.Second, 100*time.Millisecond,
		"not all %d job runner IDs were deregistered (saw deletions %v, wanted %v)",
		jobs, gh.RunnerDeletions(), runnerIDs)
}
