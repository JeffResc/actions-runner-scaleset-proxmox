package scaler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/luthermonson/go-proxmox"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/observability"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/pool"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/priority"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/provisioner"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/quotas"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/router"
)

func TestVMIDFromRunnerName(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		prefix string
		want   int
		ok     bool
	}{
		{"gh-runner-proxmox-10042", "gh-runner-proxmox-", 10042, true},
		{"gh-runner-foo-42", "gh-runner-foo-", 42, true},
		{"gh-runner-proxmox-", "gh-runner-proxmox-", 0, false},
		{"other-name", "gh-runner-proxmox-", 0, false},
		{"gh-runner-proxmox-not-a-number", "gh-runner-proxmox-", 0, false},
		// Trailing garbage after the numeric suffix must be rejected.
		// fmt.Sscanf would accept "10042garbage" → 10042; strconv.Atoi
		// rejects it, which is the correct behavior — a malformed
		// runner name should never map to a real VMID.
		{"gh-runner-proxmox-10042garbage", "gh-runner-proxmox-", 0, false},
		{"gh-runner-proxmox-10042 ", "gh-runner-proxmox-", 0, false},
		{"gh-runner-proxmox--1", "gh-runner-proxmox-", 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got, ok := vmidFromRunnerName(c.name, c.prefix)
			require.Equal(t, c.ok, ok)
			if c.ok {
				require.Equal(t, c.want, got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Fakes for HandleDesiredRunnerCount tests
// ---------------------------------------------------------------------------

// fakePool tracks Acquire calls and reports Busy via Stats. The scaler
// is the only consumer of pool.Manager in this package; we don't need
// to satisfy the rest of the surface accurately.
type fakePool struct {
	mu sync.Mutex

	// available is the number of Hot VMs the next Acquire calls will
	// hand out. Decremented per successful Acquire.
	available int

	// busy is reported as Assigned via Stats. The scaler reads
	// Stats().Busy() to clamp HandleDesiredRunnerCount.
	busy int

	// acquireCalls records every successful Acquire — the count is the
	// observable in the regression test.
	acquireCalls []int

	desiredHistory []int

	// markedRunning records every (vmid, runnerID) pair seen via
	// MarkRunning so the JobStarted tests can assert on the handler's
	// downstream call. markRunningErr lets a single test inject a
	// pool-side failure.
	markedRunning  []markedRunningCall
	markRunningErr error

	// markedCompleted records every vmid seen via MarkCompleted.
	// markCompletedErr injects a pool-side failure.
	markedCompleted  []int
	markCompletedErr error
}

type markedRunningCall struct {
	VMID     int
	RunnerID int64
}

func (f *fakePool) AcquireForProfile(ctx context.Context, jobID int64, _ string, maxBusy int) (*pool.VM, error) {
	return f.Acquire(ctx, jobID, maxBusy)
}

func (f *fakePool) Acquire(_ context.Context, _ int64, maxBusy int) (*pool.VM, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if maxBusy > 0 && f.busy >= maxBusy {
		return nil, pool.ErrAtCapacity
	}
	if f.available <= 0 {
		return nil, pool.ErrNoneAvailable
	}
	f.available--
	// Synthesize a VMID for the row; matches the production "10000+N"
	// allocation shape but the value itself is opaque to the scaler.
	vmid := 10000 + len(f.acquireCalls)
	f.acquireCalls = append(f.acquireCalls, vmid)
	// Acquired VMs count as busy (Hot → Assigned).
	f.busy++
	return &pool.VM{VMID: vmid, Node: "pve1", Name: "gh-runner-test-" + itoa(vmid)}, nil
}

func (f *fakePool) Stats(context.Context) (pool.Stats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return pool.Stats{Assigned: f.busy}, nil
}

func (f *fakePool) SetDesiredCount(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.desiredHistory = append(f.desiredHistory, n)
}

func (f *fakePool) MarkCompleted(_ context.Context, vmid int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.markedCompleted = append(f.markedCompleted, vmid)
	if f.markCompletedErr != nil {
		return f.markCompletedErr
	}
	if f.busy > 0 {
		f.busy--
	}
	// Model the production refill loop: when an Assigned VM is
	// released back, the pool's reconcile loop will clone a fresh Hot
	// VM. From the scaler's vantage point that's just "another Hot
	// VM became available."
	f.available++
	return nil
}

// MarkRunning records the (vmid, runnerID) pair so JobStarted tests
// can assert on the downstream call. markRunningErr lets a single
// test inject a pool-side failure.
func (f *fakePool) MarkRunning(_ context.Context, vmid int, runnerID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.markedRunning = append(f.markedRunning, markedRunningCall{VMID: vmid, RunnerID: runnerID})
	return f.markRunningErr
}
func (f *fakePool) SetRunnerID(context.Context, int, int64) error             { return nil }
func (f *fakePool) PromoteToRunning(context.Context, int, int64, int64) error { return nil }
func (f *fakePool) ForceDestroy(context.Context, int, string) error           { return nil }
func (f *fakePool) Preempt(context.Context, int, string) error                { return nil }
func (f *fakePool) StampJobMetadata(context.Context, int, pool.JobMetadata) error {
	return nil
}
func (f *fakePool) ListRows(context.Context) ([]pool.RowSnapshot, error) { return nil, nil }
func (f *fakePool) Adopt(context.Context) error                          { return nil }
func (f *fakePool) Run(context.Context) error                            { return nil }
func (f *fakePool) SignalRefill()                                        {}
func (f *fakePool) SetTargetSizes(string, int, int) error                { return nil }

func (f *fakePool) acquireCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.acquireCalls)
}

// stubProvForScaler satisfies provisioner.Provisioner with no-ops; the
// scaler only touches it from provisionOne, which we stub out.
type stubProvForScaler struct{}

func (stubProvForScaler) Clone(context.Context, provisioner.CloneOptions) (*provisioner.VM, error) {
	return nil, nil //nolint:nilnil // test stub: scaler tests stub out provisionOne, so Clone isn't exercised
}
func (stubProvForScaler) Start(context.Context, *provisioner.VM) error                    { return nil }
func (stubProvForScaler) Stop(context.Context, *provisioner.VM) error                     { return nil }
func (stubProvForScaler) Destroy(context.Context, *provisioner.VM) error                  { return nil }
func (stubProvForScaler) WaitReady(context.Context, *provisioner.VM, time.Duration) error { return nil }
func (stubProvForScaler) InjectJITConfig(context.Context, *provisioner.VM, string) error  { return nil }
func (stubProvForScaler) ReadJITConfig(context.Context, *provisioner.VM) ([]byte, error) {
	return nil, nil
}
func (stubProvForScaler) ListOwnedVMs(context.Context) ([]*provisioner.VM, error) { return nil, nil }
func (stubProvForScaler) PowerState(context.Context, *provisioner.VM) (string, error) {
	return "running", nil
}
func (stubProvForScaler) Ping(context.Context) error                  { return nil }
func (stubProvForScaler) TemplateNode() string                        { return "pve1" }
func (stubProvForScaler) Client() *proxmox.Client                     { return nil }
func (stubProvForScaler) IsRecentlyDestroyed(int, time.Duration) bool { return false }
func (stubProvForScaler) InFlightCloneCount() int                     { return 0 }

func itoa(n int) string {
	// Small dependency-free implementation to keep this file standalone.
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// quietScaler builds a Scaler with a fake pool and a stub provisionOne
// that succeeds without touching GitHub or Proxmox.
func quietScaler(t *testing.T, fp *fakePool, provisionFn func(context.Context, *pool.VM) bool) *Scaler {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	s := New(Config{ScaleSetID: 1, ScaleSetName: "test", NamePrefix: "gh-runner-test-"}, nil, fp, stubProvForScaler{}, log, nil)
	if provisionFn == nil {
		provisionFn = func(context.Context, *pool.VM) bool { return true }
	}
	s.provisionOneFn = provisionFn
	return s
}

// TestHandleDesiredRunnerCount_RepeatedDesiredDoesNotOverAcquire is the
// regression guard for the over-acquire bug captured in production:
// running 3 GitHub jobs registered 5 runners because
// HandleDesiredRunnerCount(3) fired twice (the actions/scaleset
// listener re-sends the absolute desired count on session refresh) and
// the scaler tried to acquire 3 Hot VMs each time. The fix clamps
// effective need to max(0, count - busy) so a re-asserted same desired
// is a no-op.
func TestHandleDesiredRunnerCount_RepeatedDesiredDoesNotOverAcquire(t *testing.T) {
	t.Parallel()

	// 5 Hot VMs are eligible for acquire — more than the desired
	// count of 3. The first HandleDesiredRunnerCount(3) takes 3 of
	// them. A correctly-behaving scaler will NOT touch the remaining
	// 2 when the same desired count is re-asserted.
	fp := &fakePool{available: 5}
	s := quietScaler(t, fp, nil)

	ctx := context.Background()

	delivered1, err := s.HandleDesiredRunnerCount(ctx, 3)
	require.NoError(t, err)
	require.Equal(t, 3, delivered1, "first call must deliver exactly 3 runners")
	require.Equal(t, 3, fp.acquireCount(), "first call must Acquire exactly 3 Hot VMs")

	// Second call with the SAME desired count. The scaler now sees
	// 3 in-flight (busy=3) and the desired is still 3 — nothing new
	// to acquire. Today this over-acquires; after the fix it is a
	// no-op (returns 0 delivered, no additional Acquires).
	delivered2, err := s.HandleDesiredRunnerCount(ctx, 3)
	require.NoError(t, err)
	require.Equal(t, 0, delivered2, "re-asserted desired count must be a no-op when already satisfied")
	require.Equal(t, 3, fp.acquireCount(),
		"re-asserted desired count must NOT trigger additional Acquires; got %d total Acquires (over-acquire bug)", fp.acquireCount())
}

// TestHandleDesiredRunnerCount_GrowthDeltaActuallyAcquires confirms the
// fix doesn't over-correct: when desired GROWS, the scaler must
// acquire the DELTA, not the absolute number.
func TestHandleDesiredRunnerCount_GrowthDeltaActuallyAcquires(t *testing.T) {
	t.Parallel()
	fp := &fakePool{available: 10}
	s := quietScaler(t, fp, nil)

	ctx := context.Background()

	_, err := s.HandleDesiredRunnerCount(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, 2, fp.acquireCount())

	// Desired grows from 2 to 5. The scaler should acquire 3 more.
	delivered, err := s.HandleDesiredRunnerCount(ctx, 5)
	require.NoError(t, err)
	require.Equal(t, 3, delivered, "growth delta must acquire only count-busy")
	require.Equal(t, 5, fp.acquireCount(), "total Acquires must equal the final desired count")
}

// TestHandleDesiredRunnerCount_ShrinkIsNoop: when desired DECREASES,
// the scaler must not error and must not acquire negative count
// (which the production bug code path can't generate, but a future
// refactor might). The pool's own shrink-to-floor handles the actual
// destruction; HandleDesiredRunnerCount only acquires.
func TestHandleDesiredRunnerCount_ShrinkIsNoop(t *testing.T) {
	t.Parallel()
	fp := &fakePool{available: 5}
	s := quietScaler(t, fp, nil)

	ctx := context.Background()

	_, err := s.HandleDesiredRunnerCount(ctx, 3)
	require.NoError(t, err)
	require.Equal(t, 3, fp.acquireCount())

	// Desired drops to 1. No Acquires should happen.
	delivered, err := s.HandleDesiredRunnerCount(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 0, delivered)
	require.Equal(t, 3, fp.acquireCount(),
		"shrinking desired must NOT acquire more VMs")
}

// TestHandleDesiredRunnerCount_PoolEmptyReturnsZero: even when the
// pool is starved of Hot VMs, HandleDesiredRunnerCount must return
// cleanly without erroring — the next listener message retries.
func TestHandleDesiredRunnerCount_PoolEmptyReturnsZero(t *testing.T) {
	t.Parallel()
	fp := &fakePool{available: 0}
	s := quietScaler(t, fp, nil)

	delivered, err := s.HandleDesiredRunnerCount(context.Background(), 3)
	require.NoError(t, err, "pool exhaustion is back-pressure, not an error")
	require.Equal(t, 0, delivered)
}

// TestHandleDesiredRunnerCount_PartialProvisionFailureReleasesVMs:
// when provisionOne fails for some of the acquired VMs (e.g., a
// transient GitHub 5xx), those VMs must be released back to the
// pool via MarkCompleted so the next tick can retry. The clamp must
// then re-allow acquiring replacements because busy drops.
func TestHandleDesiredRunnerCount_PartialProvisionFailureReleasesVMs(t *testing.T) {
	t.Parallel()
	fp := &fakePool{available: 5}

	// Custom provisionFn: the first 3 calls fail, the rest succeed.
	var calls atomic.Int32
	provisionFn := func(ctx context.Context, vmObj *pool.VM) bool {
		n := calls.Add(1)
		if n <= 3 {
			// Simulate the production failure path: release the VM.
			_ = fp.MarkCompleted(ctx, vmObj.VMID)
			return false
		}
		return true
	}

	s := quietScaler(t, fp, provisionFn)

	ctx := context.Background()
	delivered, err := s.HandleDesiredRunnerCount(ctx, 3)
	require.NoError(t, err)
	require.Equal(t, 0, delivered, "all 3 provisions failed → none delivered")

	// After the failures, busy is back to 0 (failed provisions released
	// their VMs). A retry should acquire 3 more.
	delivered, err = s.HandleDesiredRunnerCount(ctx, 3)
	require.NoError(t, err)
	require.Equal(t, 3, delivered, "retry after failures must succeed; clamp must see busy=0")
}

// TestHandleDesiredRunnerCount_BusyRaceClampsViaMaxBusy locks in the
// #69 fix: the maxBusy parameter must clamp inside the same atomic
// operation that observes busy, so a goroutine bumping busy between
// the scaler's Stats read and its Acquire loop can't sneak past the
// requested target count.
//
// We simulate the race by pre-loading busy=2 in the fakePool BEFORE
// HandleDesiredRunnerCount(count=3) runs. The scaler reads busy=2 and
// would attempt need=1 acquires. With maxBusy=count=3 plumbed in, the
// first acquire succeeds (busy becomes 3); a hypothetical concurrent
// extra acquire by the loop would refuse with ErrAtCapacity.
func TestHandleDesiredRunnerCount_BusyRaceClampsViaMaxBusy(t *testing.T) {
	t.Parallel()
	fp := &fakePool{available: 5, busy: 2}
	s := quietScaler(t, fp, nil)

	// count=3, busy=2, need=1. With maxBusy=3 the loop must STOP after
	// the first acquire because busy becomes 3 inside the fake's check.
	delivered, err := s.HandleDesiredRunnerCount(context.Background(), 3)
	require.NoError(t, err)
	require.Equal(t, 1, delivered, "exactly 1 new runner must be acquired (3 desired - 2 already busy)")
	require.Equal(t, 1, fp.acquireCount(),
		"maxBusy clamp must prevent additional acquires; got %d acquires", fp.acquireCount())

	// Re-asserted desired with already-satisfied busy is a clean no-op.
	delivered, err = s.HandleDesiredRunnerCount(context.Background(), 3)
	require.NoError(t, err)
	require.Equal(t, 0, delivered, "desired already satisfied; no further acquires")
}

// ---------------------------------------------------------------------------
// Routing observability (PR 2 — issue #7)
// ---------------------------------------------------------------------------

// scalerWithRouter wires a Scaler + metrics + router so the routing
// tests can assert on counter increments. The fakePool is unused by
// HandleJobStarted apart from MarkRunning, which we don't care about
// here.
func scalerWithRouter(t *testing.T, profiles []router.Profile) (*Scaler, *observability.Metrics) {
	t.Helper()
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	s := New(Config{ScaleSetID: 1, ScaleSetName: "test", NamePrefix: "gh-runner-test-"}, nil, &fakePool{}, stubProvForScaler{}, log, metrics)
	r, err := router.New(profiles)
	require.NoError(t, err)
	s.SetRouter(r)
	return s, metrics
}

// counterValue reads the current value of a CounterVec sample with
// the given label values. Returns 0 when no matching sample exists.
// Wraps prometheus/testutil so the routing tests don't have to deal
// with the dto.Metric protobuf surface.
func counterValue(t *testing.T, cv *prometheus.CounterVec, labelValues ...string) float64 {
	t.Helper()
	return testutil.ToFloat64(cv.WithLabelValues(labelValues...))
}

func TestHandleJobStarted_RoutedJobDoesNotIncrementUnrouted(t *testing.T) {
	t.Parallel()
	s, metrics := scalerWithRouter(t, []router.Profile{
		{Name: "linux-x64", Labels: []string{"self-hosted", "linux", "x64"}},
	})

	err := s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
		JobMessageBase: scaleset.JobMessageBase{
			RequestLabels: []string{"self-hosted", "linux", "x64"},
		},
		RunnerName: "gh-runner-test-10042",
		RunnerID:   42,
	})
	require.NoError(t, err)

	// Cardinality-capped bucket the recorder would have used IF this
	// were a miss; assert zero increments across all buckets so the
	// matched-job-must-not-count guarantee holds regardless of where
	// the hash lands.
	for i := 0; i < unroutedLabelsBucketCount; i++ {
		bucket := fmt.Sprintf("bucket-%02d", i)
		require.Equal(t, 0.0,
			counterValue(t, metrics.UnroutedJobs, "test", bucket),
			"a matched job must not increment unrouted_jobs_total (bucket=%s)", bucket)
	}
}

func TestHandleJobStarted_UnroutedJobIncrementsCounter(t *testing.T) {
	t.Parallel()
	s, metrics := scalerWithRouter(t, []router.Profile{
		{Name: "linux-x64", Labels: []string{"self-hosted", "linux", "x64"}},
	})

	// Job requests `windows` — no profile satisfies it.
	err := s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
		JobMessageBase: scaleset.JobMessageBase{
			RequestLabels: []string{"self-hosted", "windows"},
		},
		RunnerName: "gh-runner-test-10043",
		RunnerID:   43,
	})
	require.NoError(t, err)

	// Find the bucket the unrouted labels hashed into; cardinality
	// cap means we can't predict it inline without re-deriving the
	// hash, so search across the bucket space.
	wantBucket := joinLabelsForMetric([]string{"self-hosted", "windows"})
	require.Equal(t, 1.0,
		counterValue(t, metrics.UnroutedJobs, "test", wantBucket),
		"unrouted job must increment scaleset_unrouted_jobs_total in its hashed bucket")
}

func TestHandleJobStarted_NoRouterAttachedIsNoop(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	s := New(Config{ScaleSetID: 1, ScaleSetName: "test", NamePrefix: "gh-runner-test-"}, nil, &fakePool{}, stubProvForScaler{}, log, metrics)
	// No SetRouter call — routing is disabled.

	err := s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
		JobMessageBase: scaleset.JobMessageBase{
			RequestLabels: []string{"never", "matches"},
		},
		RunnerName: "gh-runner-test-10044",
		RunnerID:   44,
	})
	require.NoError(t, err)
	for i := 0; i < unroutedLabelsBucketCount; i++ {
		bucket := fmt.Sprintf("bucket-%02d", i)
		require.Equal(t, 0.0,
			counterValue(t, metrics.UnroutedJobs, "test", bucket),
			"nil router must NOT touch the unrouted counter (bucket=%s)", bucket)
	}
}

func TestJoinLabelsForMetric_StableAcrossOrdering(t *testing.T) {
	t.Parallel()
	// Same logical label set in different orders MUST hash to the
	// same Prometheus label value — otherwise repeated occurrences
	// land in different series and bloat cardinality.
	a := joinLabelsForMetric([]string{"self-hosted", "linux", "x64"})
	b := joinLabelsForMetric([]string{"x64", "self-hosted", "linux"})
	require.Equal(t, a, b)
	require.Regexp(t, `^bucket-\d{2}$`, a, "output must be the cardinality-capped bucket form")
	require.Equal(t, "empty", joinLabelsForMetric(nil))
}

// TestJoinLabelsForMetric_BoundedCardinality is the load-bearing
// guard for #240: a workflow author who embeds an ephemeral string
// (PR number, commit SHA, UUID, ...) in `runs-on` must not be able
// to blow up the metrics endpoint with an unbounded series count.
// Drive thousands of distinct inputs and assert the output bucket
// set never exceeds unroutedLabelsBucketCount.
func TestJoinLabelsForMetric_BoundedCardinality(t *testing.T) {
	t.Parallel()
	seen := make(map[string]struct{}, unroutedLabelsBucketCount)
	for i := 0; i < 5000; i++ {
		// Each call gets a unique label set (e.g. a UUID injected
		// into the `runs-on` request labels).
		out := joinLabelsForMetric([]string{"self-hosted", "linux", fmt.Sprintf("uuid-%d", i)})
		seen[out] = struct{}{}
	}
	require.LessOrEqual(t, len(seen), unroutedLabelsBucketCount,
		"5000 distinct inputs must hash to <= %d buckets (cardinality cap)",
		unroutedLabelsBucketCount)
	require.Greater(t, len(seen), unroutedLabelsBucketCount/4,
		"hash distribution should populate at least a quarter of buckets across 5000 inputs (got %d / %d)",
		len(seen), unroutedLabelsBucketCount)
}

// ---------------------------------------------------------------------------
// Quotas + Priority observability (PR 5 — issues #4 + #10)
// ---------------------------------------------------------------------------

// stubQuotaCounter satisfies QuotaCounter with constant return values
// so tests can drive recordQuota's threshold comparison without
// standing up a real store.
type stubQuotaCounter struct {
	repoCounts map[string]int
	orgCounts  map[string]int
}

func (s *stubQuotaCounter) CountByRepo(repo string) (int, error) {
	return s.repoCounts[repo], nil
}
func (s *stubQuotaCounter) CountByOrg(org string) (int, error) {
	return s.orgCounts[org], nil
}

func TestHandleJobStarted_StampsJobMetadata(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	s := New(Config{ScaleSetID: 1, ScaleSetName: "test", NamePrefix: "gh-runner-test-"}, nil, &fakePool{}, stubProvForScaler{}, log, metrics)

	pm, err := priority.New([]priority.Class{
		{Name: "critical", Weight: 100, Match: priority.Match{Org: "acme"}},
	})
	require.NoError(t, err)
	s.SetPriority(pm)

	err = s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
		JobMessageBase: scaleset.JobMessageBase{
			OwnerName:      "acme",
			RepositoryName: "platform",
			RequestLabels:  []string{"self-hosted", "linux"},
		},
		RunnerName: "gh-runner-test-10042",
		RunnerID:   42,
	})
	require.NoError(t, err)

	// priority_acquires_total{class="critical"} must increment.
	require.Equal(t, 1.0,
		counterValue(t, metrics.PriorityAcquires, "test", "critical"),
		"job from matching org must increment its class counter")
}

func TestHandleJobStarted_DefaultPriorityWhenNoMatcher(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	s := New(Config{ScaleSetID: 1, ScaleSetName: "test", NamePrefix: "gh-runner-test-"}, nil, &fakePool{}, stubProvForScaler{}, log, metrics)
	// No SetPriority — every job falls into priority.ZeroClass ("default").

	err := s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
		JobMessageBase: scaleset.JobMessageBase{
			OwnerName: "acme", RepositoryName: "platform",
		},
		RunnerName: "gh-runner-test-10043",
		RunnerID:   43,
	})
	require.NoError(t, err)

	require.Equal(t, 1.0,
		counterValue(t, metrics.PriorityAcquires, "test", "default"),
		"no-priority-config baseline series under 'default'")
}

func TestHandleJobStarted_QuotaOverIncrementsThrottled(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	s := New(Config{ScaleSetID: 1, ScaleSetName: "test", NamePrefix: "gh-runner-test-"}, nil, &fakePool{}, stubProvForScaler{}, log, metrics)

	qr, err := quotas.New(quotas.Config{DefaultPerRepo: 3})
	require.NoError(t, err)
	s.SetQuotas(qr)
	// 4 VMs already stamped for acme/platform — over the cap of 3.
	s.SetQuotaCounter(&stubQuotaCounter{
		repoCounts: map[string]int{"acme/platform": 4},
	})

	err = s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
		JobMessageBase: scaleset.JobMessageBase{
			OwnerName: "acme", RepositoryName: "platform",
		},
		RunnerName: "gh-runner-test-10044",
		RunnerID:   44,
	})
	require.NoError(t, err)

	require.Equal(t, 1.0,
		counterValue(t, metrics.QuotaThrottled, "test", "repo", "acme/platform"),
		"quota over-cap must increment scaleset_quota_throttled_total{repo}")
}

func TestHandleJobStarted_QuotaUnderCapNoThrottle(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	s := New(Config{ScaleSetID: 1, ScaleSetName: "test", NamePrefix: "gh-runner-test-"}, nil, &fakePool{}, stubProvForScaler{}, log, metrics)

	qr, err := quotas.New(quotas.Config{DefaultPerRepo: 10})
	require.NoError(t, err)
	s.SetQuotas(qr)
	s.SetQuotaCounter(&stubQuotaCounter{
		repoCounts: map[string]int{"acme/platform": 2},
	})

	err = s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
		JobMessageBase: scaleset.JobMessageBase{
			OwnerName: "acme", RepositoryName: "platform",
		},
		RunnerName: "gh-runner-test-10045",
		RunnerID:   45,
	})
	require.NoError(t, err)

	require.Equal(t, 0.0,
		counterValue(t, metrics.QuotaThrottled, "test", "repo", "acme/platform"),
		"under-cap job must NOT increment throttled counter")
}

// TestHandleJobStarted_QuotaBoundaries covers the cap edge cases
// previously untested (#250): cap=0 (treated as disabled/no
// enforcement, never throttles), cap=1 (single-slot — throttles
// the moment count>1), and a large overflow (count >> cap; the
// metric is .Inc()'d exactly once per call, NOT once per excess
// VM, so an overflow of 100 still bumps the counter by 1).
func TestHandleJobStarted_QuotaBoundaries(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name             string
		cap              int
		count            int
		wantThrottleHits float64
	}{
		// cap=0 short-circuits in recordQuota — treated as "no
		// enforcement" so even a count over 0 emits no throttle.
		{"cap=0 never throttles", 0, 50, 0},
		// cap=1 single-slot: under cap, no throttle.
		{"cap=1 under cap", 1, 1, 0},
		// cap=1 single-slot: count=2 > cap=1 → throttle.
		{"cap=1 over cap", 1, 2, 1},
		// Large overflow: the throttle is per-call (Inc), NOT per
		// excess VM — so 100 jobs > cap=10 still increments by 1
		// for one observed job.
		{"large overflow increments once", 10, 100, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			metrics := observability.NewMetrics(prometheus.NewRegistry())
			log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
			s := New(Config{ScaleSetID: 1, ScaleSetName: "test", NamePrefix: "gh-runner-test-"}, nil, &fakePool{}, stubProvForScaler{}, log, metrics)

			qr, err := quotas.New(quotas.Config{DefaultPerRepo: tc.cap})
			require.NoError(t, err)
			s.SetQuotas(qr)
			s.SetQuotaCounter(&stubQuotaCounter{
				repoCounts: map[string]int{"acme/platform": tc.count},
			})

			err = s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
				JobMessageBase: scaleset.JobMessageBase{
					OwnerName: "acme", RepositoryName: "platform",
				},
				RunnerName: "gh-runner-test-10047",
				RunnerID:   47,
			})
			require.NoError(t, err)

			got := counterValue(t, metrics.QuotaThrottled, "test", "repo", "acme/platform")
			require.Equal(t, tc.wantThrottleHits, got,
				"cap=%d count=%d → expected %v throttle hits, got %v",
				tc.cap, tc.count, tc.wantThrottleHits, got)
		})
	}
}

// errQuotaCounter satisfies QuotaCounter by always returning an
// error — used to pin the quota-lookup-error path.
type errQuotaCounter struct{ err error }

func (e *errQuotaCounter) CountByRepo(string) (int, error) { return 0, e.err }
func (e *errQuotaCounter) CountByOrg(string) (int, error)  { return 0, e.err }

// TestHandleJobStarted_QuotaLookupErrorDoesNotEmitMetric pins the
// log-only behavior on quota-count failure: a downstream error
// must NOT increment the throttled counter (false signal) and must
// NOT escalate to the listener.
func TestHandleJobStarted_QuotaLookupErrorDoesNotEmitMetric(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	s := New(Config{ScaleSetID: 1, ScaleSetName: "test", NamePrefix: "gh-runner-test-"}, nil, &fakePool{}, stubProvForScaler{}, log, metrics)

	qr, err := quotas.New(quotas.Config{DefaultPerRepo: 5})
	require.NoError(t, err)
	s.SetQuotas(qr)
	s.SetQuotaCounter(&errQuotaCounter{err: errors.New("store unavailable")})

	err = s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
		JobMessageBase: scaleset.JobMessageBase{
			OwnerName: "acme", RepositoryName: "platform",
		},
		RunnerName: "gh-runner-test-10048",
		RunnerID:   48,
	})
	require.NoError(t, err, "lookup errors must NOT escalate to the listener")
	require.Equal(t, 0.0,
		counterValue(t, metrics.QuotaThrottled, "test", "repo", "acme/platform"),
		"lookup error must NOT increment throttled counter — false signal")
}

func TestHandleJobStarted_DisabledQuotaResolverIsNoop(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	s := New(Config{ScaleSetID: 1, ScaleSetName: "test", NamePrefix: "gh-runner-test-"}, nil, &fakePool{}, stubProvForScaler{}, log, metrics)
	// Empty config → Enabled()==false. Even if a counter says we're
	// over 9000, no throttled metric is emitted.
	qr, err := quotas.New(quotas.Config{})
	require.NoError(t, err)
	s.SetQuotas(qr)
	s.SetQuotaCounter(&stubQuotaCounter{
		repoCounts: map[string]int{"acme/platform": 9999},
	})

	err = s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
		JobMessageBase: scaleset.JobMessageBase{
			OwnerName: "acme", RepositoryName: "platform",
		},
		RunnerName: "gh-runner-test-10046",
		RunnerID:   46,
	})
	require.NoError(t, err)

	require.Equal(t, 0.0,
		counterValue(t, metrics.QuotaThrottled, "test", "repo", "acme/platform"),
		"disabled quotas resolver must skip the check entirely")
}

// ---------------------------------------------------------------------------
// JobStarted / JobCompleted handler coverage (issue #136)
// ---------------------------------------------------------------------------

// TestHandleJobStarted_MarksRunning is the happy-path assertion: a
// well-formed JobStarted message routes through to pool.MarkRunning
// with the vmid parsed from RunnerName and the int64-widened
// RunnerID.
func TestHandleJobStarted_MarksRunning(t *testing.T) {
	t.Parallel()
	fp := &fakePool{}
	s := quietScaler(t, fp, nil)

	err := s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
		RunnerName: "gh-runner-test-10042",
		RunnerID:   42,
	})
	require.NoError(t, err)
	require.Equal(t,
		[]markedRunningCall{{VMID: 10042, RunnerID: 42}},
		fp.markedRunning,
		"the vmid in the runner name must be propagated to MarkRunning")
}

// TestHandleJobStarted_MalformedRunnerNameAbsorbed pins the
// silent-absorb contract: a runner name that doesn't match the prefix
// produces a warn log and a nil return, NOT a pool call or an error
// surfaced to the listener. The listener treats handler errors as
// fatal for the worker, so a malformed payload must not take the
// session down.
func TestHandleJobStarted_MalformedRunnerNameAbsorbed(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		runnerName string
	}{
		{"empty", ""},
		{"missing prefix", "totally-unrelated-name"},
		{"non-numeric suffix", "gh-runner-test-notanumber"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fp := &fakePool{}
			s := quietScaler(t, fp, nil)
			err := s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
				RunnerName: tc.runnerName,
				RunnerID:   42,
			})
			require.NoError(t, err, "malformed runner name must NOT surface as an error")
			require.Empty(t, fp.markedRunning, "no MarkRunning call on malformed name")
		})
	}
}

// TestHandleJobStarted_PoolErrorPropagates pins that an error from
// pool.MarkRunning bubbles up to the listener. Stamp failures are
// non-fatal (the in-handler warn-and-continue path), but MarkRunning
// is the load-bearing transition — the listener needs to see a
// failure to re-deliver.
func TestHandleJobStarted_PoolErrorPropagates(t *testing.T) {
	t.Parallel()
	fp := &fakePool{markRunningErr: errors.New("store: row missing")}
	s := quietScaler(t, fp, nil)

	err := s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
		RunnerName: "gh-runner-test-10042",
		RunnerID:   42,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "row missing")
}

// TestHandleJobCompleted_MarksCompleted is the happy-path: a
// well-formed JobCompleted message routes through to
// pool.MarkCompleted with the parsed vmid.
func TestHandleJobCompleted_MarksCompleted(t *testing.T) {
	t.Parallel()
	fp := &fakePool{}
	s := quietScaler(t, fp, nil)

	err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{
		RunnerName: "gh-runner-test-10099",
		RunnerID:   99,
	})
	require.NoError(t, err)
	require.Equal(t, []int{10099}, fp.markedCompleted)
}

// TestHandleJobCompleted_MalformedRunnerNameAbsorbed mirrors the
// JobStarted absorb contract: a runner name we can't parse must be
// dropped at warn-level without surfacing to the listener as an
// error.
func TestHandleJobCompleted_MalformedRunnerNameAbsorbed(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		runnerName string
	}{
		{"empty", ""},
		{"missing prefix", "different-prefix-10099"},
		{"non-numeric suffix", "gh-runner-test-xx"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fp := &fakePool{}
			s := quietScaler(t, fp, nil)
			err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{
				RunnerName: tc.runnerName,
			})
			require.NoError(t, err)
			require.Empty(t, fp.markedCompleted)
		})
	}
}

// TestHandleJobCompleted_PoolErrorPropagates pins error propagation
// from pool.MarkCompleted to the listener so an idempotent re-delivery
// can be requested by the listener layer.
func TestHandleJobCompleted_PoolErrorPropagates(t *testing.T) {
	t.Parallel()
	fp := &fakePool{markCompletedErr: errors.New("store: row missing")}
	s := quietScaler(t, fp, nil)

	err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{
		RunnerName: "gh-runner-test-10099",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "row missing")
}

// TestHandlers_OutOfOrderDelivery: JobCompleted arriving before
// JobStarted for the same runner is observable in production when the
// session refresh re-emits the queue. The handlers must absorb both —
// in order — without aborting on the unexpected sequence. The fake
// pool models the production behaviour (MarkCompleted on a row that
// was never MarkRunning'd is still recorded; production has the same
// idempotent-on-completed semantics).
func TestHandlers_OutOfOrderDelivery(t *testing.T) {
	t.Parallel()
	fp := &fakePool{}
	s := quietScaler(t, fp, nil)
	ctx := context.Background()

	require.NoError(t, s.HandleJobCompleted(ctx, &scaleset.JobCompleted{
		RunnerName: "gh-runner-test-10042",
	}))
	require.NoError(t, s.HandleJobStarted(ctx, &scaleset.JobStarted{
		RunnerName: "gh-runner-test-10042",
		RunnerID:   42,
	}))

	require.Equal(t, []int{10042}, fp.markedCompleted)
	require.Equal(t,
		[]markedRunningCall{{VMID: 10042, RunnerID: 42}},
		fp.markedRunning)
}

func TestClassifyJob_RepoJoinsOwnerSlashRepo(t *testing.T) {
	t.Parallel()
	s := &Scaler{}
	pm, err := priority.New([]priority.Class{
		{Name: "repo-pinned", Match: priority.Match{Repo: "platform"}},
	})
	require.NoError(t, err)
	s.SetPriority(pm)

	org, repo, class := s.classifyJob(&scaleset.JobStarted{
		JobMessageBase: scaleset.JobMessageBase{
			OwnerName:      "acme",
			RepositoryName: "platform",
		},
	})
	require.Equal(t, "acme", org)
	require.Equal(t, "acme/platform", repo, "repo is joined for quota lookup alignment")
	require.Equal(t, "repo-pinned", class.Name,
		"priority matcher receives the bare repo name (not joined) per JobInfo contract")
}

// transientInjectProv always returns the transient sentinel so
// injectWithRetry runs its full retry budget. count records how many
// attempts the scaler made before giving up.
type transientInjectProv struct {
	stubProvForScaler
	count int
}

func (p *transientInjectProv) InjectJITConfig(context.Context, *provisioner.VM, string) error {
	p.count++
	return provisioner.ErrGuestAgentNotReady
}

// TestInjectWithRetry_HonorsCustomConfig pins that the per-Scaler
// injectRetry override controls both the wall-clock budget and the
// attempt cap. A non-default config with a tiny MaxElapsed must bound
// runtime to that budget instead of falling back to the production
// default's 60s.
func TestInjectWithRetry_HonorsCustomConfig(t *testing.T) {
	t.Parallel()
	prov := &transientInjectProv{}
	s := New(
		Config{},
		nil, // no scaleset client needed
		nil, // no pool needed
		prov,
		nil, // default slog
		nil, // no metrics
	)
	// Tiny budget — should give up well under a second.
	s.injectRetry = injectRetryConfig{
		InitialInterval: time.Millisecond,
		MaxInterval:     5 * time.Millisecond,
		Multiplier:      2.0,
		MaxAttempts:     3,
		MaxElapsed:      50 * time.Millisecond,
	}
	start := time.Now()
	err := s.injectWithRetry(t.Context(), &provisioner.VM{VMID: 42, Node: "pve1"}, "jit")
	elapsed := time.Since(start)

	require.Error(t, err)
	require.ErrorIs(t, err, provisioner.ErrGuestAgentNotReady)
	require.LessOrEqual(t, elapsed, 500*time.Millisecond,
		"custom config must bound wall-clock; saw %s", elapsed)
	require.LessOrEqual(t, prov.count, 4,
		"custom config must bound attempts (MaxAttempts=3 + initial); saw %d", prov.count)
	require.GreaterOrEqual(t, prov.count, 2,
		"at least one retry must have run before giving up; saw %d", prov.count)
}

// TestHandleJobCompleted_DuplicateDeliveryIsIdempotent locks in
// the at-least-once delivery contract called out in #283: the
// actions/scaleset listener can deliver the same JobCompleted
// twice (network retry, session refresh). The scaler must
// re-dispatch MarkCompleted but the second call must NOT cause
// an error or a second destroy queue — pool.MarkCompleted's
// state-machine guards (Draining → no-op) carry that contract,
// and this test pins that the scaler doesn't break it by
// returning an error on the duplicate.
func TestHandleJobCompleted_DuplicateDeliveryIsIdempotent(t *testing.T) {
	t.Parallel()
	pool := &fakePool{}
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	s := New(Config{ScaleSetID: 1, ScaleSetName: "test", NamePrefix: "gh-runner-test-"}, nil, pool, stubProvForScaler{}, log, metrics)

	evt := &scaleset.JobCompleted{
		JobMessageBase: scaleset.JobMessageBase{},
		RunnerName:     "gh-runner-test-10042",
		RunnerID:       42,
	}
	require.NoError(t, s.HandleJobCompleted(context.Background(), evt))
	require.NoError(t, s.HandleJobCompleted(context.Background(), evt),
		"duplicate JobCompleted (at-least-once retry) must surface as idempotent success — issue #283")

	// fakePool records both calls so the assertion documents the
	// dispatch surface; the actual idempotency guarantee lives in
	// pool.MarkCompleted's state-transition matrix (pool tests).
	pool.mu.Lock()
	defer pool.mu.Unlock()
	require.Equal(t, []int{10042, 10042}, pool.markedCompleted,
		"scaler must dispatch each delivery; idempotency is enforced downstream in pool.MarkCompleted")
}

// TestHandleJobCompleted_BeforeJobStartedDoesNotError locks in
// the out-of-order-delivery tolerance called out in #283: GitHub's
// at-least-once delivery can stream JobCompleted before
// JobStarted (rare but observed under listener-session refresh).
// The scaler must dispatch MarkCompleted on a row that is still
// Assigned without erroring — pool.MarkCompleted handles the
// Assigned → Draining transition directly.
func TestHandleJobCompleted_BeforeJobStartedDoesNotError(t *testing.T) {
	t.Parallel()
	pool := &fakePool{}
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	s := New(Config{ScaleSetID: 1, ScaleSetName: "test", NamePrefix: "gh-runner-test-"}, nil, pool, stubProvForScaler{}, log, metrics)

	// JobCompleted first — no preceding JobStarted.
	require.NoError(t, s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{
		RunnerName: "gh-runner-test-10042",
		RunnerID:   42,
	}))
	// JobStarted arrives after.
	require.NoError(t, s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
		JobMessageBase: scaleset.JobMessageBase{},
		RunnerName:     "gh-runner-test-10042",
		RunnerID:       42,
	}))

	pool.mu.Lock()
	defer pool.mu.Unlock()
	require.Equal(t, []int{10042}, pool.markedCompleted)
	require.Len(t, pool.markedRunning, 1)
}

// TestHandleJob_MalformedRunnerNameSurfacesAsLogNotError locks in
// the listener-contract surface for #283: a runner name that
// doesn't match the orchestrator's NamePrefix (corrupt payload,
// out-of-band registration) must not propagate as an error from
// the handler — the listener treats handler errors as session
// failures and reconnects, which would amplify the impact of a
// single bad delivery into repeated reconnects.
func TestHandleJob_MalformedRunnerNameSurfacesAsLogNotError(t *testing.T) {
	t.Parallel()
	pool := &fakePool{}
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	s := New(Config{ScaleSetID: 1, ScaleSetName: "test", NamePrefix: "gh-runner-test-"}, nil, pool, stubProvForScaler{}, log, metrics)

	for _, runner := range []string{
		"",                 // empty
		"unrelated-runner", // wrong prefix
		"gh-runner-test-",  // prefix only, no vmid
		"gh-runner-test-NaN",
	} {
		require.NoError(t, s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
			RunnerName: runner, RunnerID: 1,
		}), "malformed RunnerName %q must NOT propagate as handler error — issue #283", runner)
		require.NoError(t, s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{
			RunnerName: runner, RunnerID: 1,
		}), "malformed RunnerName %q must NOT propagate as handler error — issue #283", runner)
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	require.Empty(t, pool.markedRunning, "malformed runner name must not dispatch MarkRunning")
	require.Empty(t, pool.markedCompleted, "malformed runner name must not dispatch MarkCompleted")
}

// stressQuotaCounter is a stress-test counter that hits a real
// sync.Mutex on every read. Used to verify the scaler's quota
// recording path doesn't induce lock starvation under burst load.
type stressQuotaCounter struct {
	mu        sync.Mutex
	orgCalls  int
	repoCalls int
}

func (s *stressQuotaCounter) CountByOrg(_ string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.orgCalls++
	return 1, nil
}

func (s *stressQuotaCounter) CountByRepo(_ string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.repoCalls++
	return 1, nil
}

// TestQuota_BurstAcrossManyReposNoDeadlock pins #288: 1500 jobs
// across many (org, repo) tuples fire HandleJobStarted in
// parallel. The quota recording path must complete every
// dispatch (no deadlock, no lock starvation, no overflow) and
// the QuotaCounter must observe every job's lookup.
//
// The "no over-commit" invariant from the issue is structural,
// not enforced today: quota_throttled_total is observational
// (the scaler emits the metric, but does not refuse the job).
// What's testable today is that the burst completes cleanly.
func TestQuota_BurstAcrossManyReposNoDeadlock(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	s := New(Config{ScaleSetID: 1, ScaleSetName: "test", NamePrefix: "gh-runner-test-"}, nil, &fakePool{}, stubProvForScaler{}, log, metrics)

	qr, err := quotas.New(quotas.Config{DefaultPerRepo: 100, DefaultPerOrg: 1000})
	require.NoError(t, err)
	s.SetQuotas(qr)
	counter := &stressQuotaCounter{}
	s.SetQuotaCounter(counter)

	const N = 1500
	var (
		wg   sync.WaitGroup
		gate sync.WaitGroup
	)
	gate.Add(1)
	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			gate.Wait()
			org := "shared-org"
			repo := fmt.Sprintf("repo-%d", idx)
			err := s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
				JobMessageBase: scaleset.JobMessageBase{
					OwnerName:      org,
					RepositoryName: repo,
				},
				RunnerName: fmt.Sprintf("gh-runner-test-%d", 10000+idx),
				RunnerID:   10000 + idx,
			})
			require.NoError(t, err, "burst job %d must dispatch cleanly under contention (issue #288)", idx)
		}(i)
	}
	gate.Done()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("1500-job burst did not complete within 30s — possible lock starvation in the quota recording path (issue #288)")
	}

	// Per-repo lookups fire for every job since DefaultPerRepo wins
	// over DefaultPerOrg precedence. Counter must have seen every
	// invocation.
	counter.mu.Lock()
	defer counter.mu.Unlock()
	require.Equal(t, N, counter.repoCalls,
		"every burst job must reach the quota counter — observed %d of %d (issue #288)", counter.repoCalls, N)
}

// TestQuotaResolver_ConcurrentResolveIsSafe pins #288 paragraph 1
// at the resolver layer: the Resolver is read-only after New, so
// concurrent Resolve calls across many (org, repo) tuples must
// produce consistent results without data races. Race-detector
// covers no-data-race.
func TestQuotaResolver_ConcurrentResolveIsSafe(t *testing.T) {
	t.Parallel()
	qr, err := quotas.New(quotas.Config{
		DefaultPerRepo: 10,
		DefaultPerOrg:  100,
		Overrides: []quotas.Override{
			{Repo: "shared-org/heavy", MaxConcurrent: 50},
			{Org: "vip-org", MaxConcurrent: 200},
		},
	})
	require.NoError(t, err)

	const N = 2000
	var (
		wg   sync.WaitGroup
		gate sync.WaitGroup
	)
	gate.Add(1)
	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			gate.Wait()
			switch idx % 4 {
			case 0:
				got := qr.Resolve("shared-org", "shared-org/heavy")
				require.Equal(t, 50, got.Cap)
				require.Equal(t, quotas.ScopeRepo, got.Scope)
			case 1:
				got := qr.Resolve("vip-org", "")
				require.Equal(t, 200, got.Cap)
				require.Equal(t, quotas.ScopeOrg, got.Scope)
			case 2:
				got := qr.Resolve("misc-org", fmt.Sprintf("misc-org/repo-%d", idx))
				require.Equal(t, 10, got.Cap, "default_per_repo applies")
				require.Equal(t, quotas.ScopeRepo, got.Scope)
			case 3:
				got := qr.Resolve("misc-org", "")
				require.Equal(t, 100, got.Cap, "default_per_org applies")
				require.Equal(t, quotas.ScopeOrg, got.Scope)
			}
		}(i)
	}
	gate.Done()
	wg.Wait()
}

// TestPriorityMatcher_ConcurrentClassifyIsSafe pins the
// classification-ordering side of #288: the priority Matcher's
// Classify must return the same class for the same JobInfo
// regardless of how many goroutines are calling concurrently.
// Race-detector covers no-data-race.
func TestPriorityMatcher_ConcurrentClassifyIsSafe(t *testing.T) {
	t.Parallel()
	m, err := priority.New([]priority.Class{
		{Name: "platinum", Match: priority.Match{Org: "vip-org"}},
		{Name: "gold", Match: priority.Match{Org: "shared-org", Repo: "shared-org/heavy"}},
		{Name: "default", Match: priority.Match{}}, // wildcard
	})
	require.NoError(t, err)

	const N = 2000
	var (
		wg   sync.WaitGroup
		gate sync.WaitGroup
	)
	gate.Add(1)
	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			gate.Wait()
			switch idx % 3 {
			case 0:
				got := m.Classify(priority.JobInfo{Org: "vip-org"})
				require.Equal(t, "platinum", got.Name)
			case 1:
				got := m.Classify(priority.JobInfo{Org: "shared-org", Repo: "shared-org/heavy"})
				require.Equal(t, "gold", got.Name)
			case 2:
				got := m.Classify(priority.JobInfo{Org: "unknown"})
				require.Equal(t, "default", got.Name)
			}
		}(i)
	}
	gate.Done()
	wg.Wait()
}

// TestMetricCardinality_UnroutedJobsBoundedByBucketCount locks
// in #290: unrouted_jobs_total carries a free-form "labels"
// dimension (user-supplied workflow labels) that must NOT grow
// unbounded under attacker- or operator-controlled label
// diversity. The orchestrator hashes labels into N=64 buckets so
// the metric's cardinality is capped at scalesets * N regardless
// of input. Without this cap, Prometheus would drop the entire
// scrape past ~10k series — monitoring goes dark exactly when
// the orchestrator is under stress.
//
// Drives 5000 unique label sets through the unrouted-recording
// path and asserts the resulting series count stays at-most-N.
func TestMetricCardinality_UnroutedJobsBoundedByBucketCount(t *testing.T) {
	t.Parallel()
	s, metrics := scalerWithRouter(t, []router.Profile{
		{Name: "linux-x64", Labels: []string{"self-hosted", "linux", "x64"}},
	})
	for i := 0; i < 5000; i++ {
		require.NoError(t, s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
			JobMessageBase: scaleset.JobMessageBase{
				RequestLabels: []string{
					"self-hosted",
					fmt.Sprintf("dim-a-%d", i),
					fmt.Sprintf("dim-b-%d", i%97),
				},
			},
			RunnerName: fmt.Sprintf("gh-runner-test-%d", 10000+i),
			RunnerID:   i + 1,
		}))
	}
	count := testutil.CollectAndCount(metrics.UnroutedJobs)
	require.LessOrEqual(t, count, unroutedLabelsBucketCount,
		"unrouted_jobs_total cardinality must stay ≤ %d; saw %d distinct series after 5000 unique label sets (issue #290)",
		unroutedLabelsBucketCount, count)
}

// TestMetricCardinality_QuotaThrottledLabelsBoundedByMatcher
// locks in #290 for the quota path: the (scope, name) label
// pair on quota_throttled_total reflects matcher-resolved
// scopes, not raw job-supplied (org, repo) tuples. Even when
// thousands of jobs from disjoint repos arrive against a single
// per-repo default rule, the emitted series count stays bounded
// by the number of distinct resolved matches, not the input
// cardinality.
//
// With DefaultPerRepo=1 and a stub counter that always reports
// "over cap" for any input repo, the metric records one series
// per (repo) actually observed — bounded here by the test's
// loop, but the load-bearing invariant is that the recording is
// gated on the matcher's resolve, NOT on freeform label space.
func TestMetricCardinality_QuotaThrottledLabelsBoundedByMatcher(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	s := New(Config{ScaleSetID: 1, ScaleSetName: "test", NamePrefix: "gh-runner-test-"}, nil, &fakePool{}, stubProvForScaler{}, log, metrics)

	qr, err := quotas.New(quotas.Config{DefaultPerRepo: 1})
	require.NoError(t, err)
	s.SetQuotas(qr)
	// Counter reports a single fixed repo over cap, so only ONE
	// resolved (scope=repo, name=shared-org/repo-0) series can be
	// emitted regardless of how many distinct jobs we drive.
	s.SetQuotaCounter(&stubQuotaCounter{
		repoCounts: map[string]int{"shared-org/repo-0": 99},
	})

	for i := 0; i < 200; i++ {
		require.NoError(t, s.HandleJobStarted(context.Background(), &scaleset.JobStarted{
			JobMessageBase: scaleset.JobMessageBase{
				OwnerName:      "shared-org",
				RepositoryName: fmt.Sprintf("repo-%d", i),
			},
			RunnerName: fmt.Sprintf("gh-runner-test-%d", 20000+i),
			RunnerID:   10000 + i,
		}))
	}
	count := testutil.CollectAndCount(metrics.QuotaThrottled)
	require.LessOrEqual(t, count, 1,
		"quota_throttled_total series must reflect matcher-resolved scopes, not raw job (org,repo) tuples; saw %d series after 200 distinct repos (issue #290)",
		count)
}

// TestMetricCardinality_EmptyOrgNonEmptyRepoDoesNotCrash locks
// in #290 paragraph 2: metric emission must tolerate mismatched
// row metadata. The classify path produces (org, repo) where
// repo = OwnerName + "/" + RepositoryName only when OwnerName
// is non-empty. A regression that emitted an empty-org series
// with a non-empty repo (or vice versa) could break the
// exporter's "labels are populated consistently" invariant
// when paired with future per-job recording.
func TestMetricCardinality_EmptyOrgNonEmptyRepoDoesNotCrash(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	s := New(Config{ScaleSetID: 1, ScaleSetName: "test", NamePrefix: "gh-runner-test-"}, nil, &fakePool{}, stubProvForScaler{}, log, metrics)

	for _, evt := range []*scaleset.JobStarted{
		{JobMessageBase: scaleset.JobMessageBase{OwnerName: "", RepositoryName: "lonely-repo"}, RunnerName: "gh-runner-test-10001", RunnerID: 1},
		{JobMessageBase: scaleset.JobMessageBase{OwnerName: "", RepositoryName: ""}, RunnerName: "gh-runner-test-10002", RunnerID: 2},
		{JobMessageBase: scaleset.JobMessageBase{OwnerName: "org-only", RepositoryName: ""}, RunnerName: "gh-runner-test-10003", RunnerID: 3},
	} {
		evt := evt
		require.NotPanics(t, func() {
			_ = s.HandleJobStarted(context.Background(), evt)
		}, "missing or empty (org, repo) fields must NOT panic the metric exporter (issue #290)")
	}
}
