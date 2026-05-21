package scaler

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/pool"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/provisioner"
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
}

func (f *fakePool) Acquire(_ context.Context, _ int64) (*pool.VM, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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

func (f *fakePool) MarkCompleted(context.Context, int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
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

// Unused by HandleDesiredRunnerCount.
func (f *fakePool) MarkRunning(context.Context, int, int64) error             { return nil }
func (f *fakePool) SetRunnerID(context.Context, int, int64) error             { return nil }
func (f *fakePool) PromoteToRunning(context.Context, int, int64, int64) error { return nil }
func (f *fakePool) ForceDestroy(context.Context, int, string) error           { return nil }
func (f *fakePool) ListRows(context.Context) ([]pool.RowSnapshot, error)      { return nil, nil }
func (f *fakePool) Recover(context.Context) error                             { return nil }
func (f *fakePool) Run(context.Context) error                                 { return nil }
func (f *fakePool) SignalRefill()                                             {}

func (f *fakePool) acquireCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.acquireCalls)
}

// stubProvForScaler satisfies provisioner.Provisioner with no-ops; the
// scaler only touches it from provisionOne, which we stub out.
type stubProvForScaler struct{}

func (stubProvForScaler) Clone(context.Context, provisioner.CloneOptions) (*provisioner.VM, error) {
	return nil, nil
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
	s := New(Config{ScaleSetID: 1, NamePrefix: "gh-runner-test-"}, nil, fp, stubProvForScaler{}, log, nil)
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
	provisionFn := func(_ context.Context, vmObj *pool.VM) bool {
		n := calls.Add(1)
		if n <= 3 {
			// Simulate the production failure path: release the VM.
			_ = fp.MarkCompleted(context.Background(), vmObj.VMID)
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
