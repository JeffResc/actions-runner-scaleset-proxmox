//go:build e2e

package e2e

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakegithub"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakeproxmox"
)

// poolStateView mirrors the GET /admin/state response (pool.Stats has no
// json tags, so the keys are the Go field names).
type poolStateView struct {
	Pool struct {
		Provisioning int
		Warm         int
		Booting      int
		Hot          int
		Assigned     int
		Running      int
		Draining     int
		Destroying   int
		Poison       int
	} `json:"pool"`
}

// statePhases returns the lifecycle states currently occupied (count>0).
func (v poolStateView) statePhases() []string {
	out := make([]string, 0, 4)
	add := func(name string, n int) {
		if n > 0 {
			out = append(out, name)
		}
	}
	add("Provisioning", v.Pool.Provisioning)
	add("Booting", v.Pool.Booting)
	add("Hot", v.Pool.Hot)
	add("Assigned", v.Pool.Assigned)
	add("Running", v.Pool.Running)
	add("Draining", v.Pool.Draining)
	add("Destroying", v.Pool.Destroying)
	add("Poison", v.Pool.Poison)
	return out
}

// stateSampler polls GET /admin/state in the background and records the
// sample index at which each lifecycle state was FIRST observed. For a
// single-VM pool (HotSize=1, MaxConcurrentRunners=1) the aggregate pool
// state IS that one VM's state, so the first-seen ordering is the VM's
// state-transition sequence.
type stateSampler struct {
	mu        sync.Mutex
	firstSeen map[string]int
	next      int
	stop      chan struct{}
	done      chan struct{}
	stopOnce  sync.Once
}

func newStateSampler(t testing.TB, h *Harness) *stateSampler {
	s := &stateSampler{
		firstSeen: map[string]int{},
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	go func() {
		defer close(s.done)
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-tick.C:
				resp := h.AdminRequest(t, "GET", "/admin/state", nil)
				if resp.StatusCode != 200 {
					resp.Body.Close()
					continue
				}
				var v poolStateView
				err := json.NewDecoder(resp.Body).Decode(&v)
				resp.Body.Close()
				if err != nil {
					continue
				}
				s.mu.Lock()
				idx := s.next
				s.next++
				for _, phase := range v.statePhases() {
					if _, ok := s.firstSeen[phase]; !ok {
						s.firstSeen[phase] = idx
					}
				}
				s.mu.Unlock()
			}
		}
	}()
	return s
}

func (s *stateSampler) close() {
	s.stopOnce.Do(func() { close(s.stop) })
	<-s.done
}

func (s *stateSampler) firstSeenIndex(phase string) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, ok := s.firstSeen[phase]
	return i, ok
}

// TestE2E_SingleVMStateTransitionSequence pins #330: aggregate
// pool-size assertions ("assigned >= 1") let a VM that skips or
// transposes a lifecycle state pass clean. With a one-VM pool the
// aggregate state IS the VM's state, so a background sampler over
// /admin/state captures the exact per-VM transition sequence. We assert
// the VM progressed Hot → Assigned → Running (in that order, none
// skipped) and only reached a teardown state AFTER Running — catching
// e.g. an Assigned → Destroying jump that skips Running.
func TestE2E_SingleVMStateTransitionSequence(t *testing.T) {
	t.Parallel()
	proxmox := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
	gh := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{Name: "seq-set"},
	})
	gh.SetStatistics(fakegithub.Statistics{TotalAssignedJobs: 1})

	h := Start(t, Options{
		HotSize:              1,
		MaxConcurrentRunners: 1,
		ScaleSetName:         "seq-set",
		FakeProxmox:          proxmox,
		FakeGitHub:           gh,
	})

	sampler := newStateSampler(t, h)
	defer sampler.close()

	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "assigned")) >= 1
	}, 30*time.Second, 100*time.Millisecond, "VM never reached Assigned")

	vmid, vmName := awaitAssignedVM(t, h, "seq-set")

	gh.SetRunner(fakegithub.Runner{ID: firstJITRunnerID, Name: vmName, Status: "online", Busy: true})
	require.NoError(t, gh.PostJobStarted(vmName, firstJITRunnerID))
	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "running")) >= 1
	}, 30*time.Second, 100*time.Millisecond, "VM never reached Running")

	gh.SetStatistics(fakegithub.Statistics{})
	require.NoError(t, gh.PostJobCompleted(vmName, firstJITRunnerID))
	require.Eventually(t, func() bool {
		for _, vm := range proxmox.Snapshot() {
			if vm.VMID == vmid {
				return false
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond, "VM %d never destroyed", vmid)

	sampler.close() // stop sampling; safe to inspect firstSeen now.

	hotAt, okHot := sampler.firstSeenIndex("Hot")
	assignedAt, okAssigned := sampler.firstSeenIndex("Assigned")
	runningAt, okRunning := sampler.firstSeenIndex("Running")

	require.True(t, okHot, "Hot was never observed for the VM")
	require.True(t, okAssigned, "Assigned was never observed for the VM")
	require.True(t, okRunning, "Running was never observed — the VM skipped Running (Assigned → destroyed)")

	require.Less(t, hotAt, assignedAt, "VM must be Hot before Assigned")
	require.Less(t, assignedAt, runningAt, "VM must be Assigned before Running (no transposition)")
}
