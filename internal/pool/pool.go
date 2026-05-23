// Package pool owns the lifecycle of every Proxmox VM the orchestrator
// manages.
//
// The pool exists in three tiers:
//   - Hot: VMs that are fully booted, idle, and waiting to receive a JIT
//     runner config (sub-10s job start).
//   - Warm: VMs that have been cloned from the template but are powered
//     off. Promoting one to Hot costs only the boot time (~20-30s).
//   - Cold: VMs that don't yet exist. When the hot and warm pools are
//     exhausted, the reconcile loop clones-on-demand into the hot pool.
//
// State is authoritative in the in-memory store (hashicorp/go-memdb);
// Proxmox tags are used at startup to find this orchestrator's VMs and
// rebuild the empty in-memory view. Every state transition is a single
// atomic CAS via the store's write transaction.
//
// The reconcile goroutine is the single owner of the pool. It wakes on a
// ticker and on buffered-channel refill signals; Acquire is the only entry
// point for callers wanting a Hot VM.
package pool

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Errors returned from Manager methods.
var (
	// ErrNoneAvailable means no Hot VM is ready to be acquired right now.
	// The caller (scaler) should kick a refill and try again on the next
	// listener message.
	ErrNoneAvailable = errors.New("pool: no hot VM available")

	// ErrAtCapacity means the orchestrator is already at MaxConcurrent
	// runners; the GitHub side should queue jobs server-side until a
	// running job completes.
	ErrAtCapacity = errors.New("pool: at MaxConcurrentRunners")
)

// Stats summarises the pool's current population.
type Stats struct {
	Provisioning int
	Warm         int
	Booting      int
	Hot          int
	Assigned     int
	Running      int
	Draining     int
	Destroying   int
	Poison       int
}

// Total returns the sum of all non-terminal states.
func (s Stats) Total() int {
	return s.Provisioning + s.Warm + s.Booting + s.Hot +
		s.Assigned + s.Running + s.Draining + s.Destroying + s.Poison
}

// Available returns the count of VMs that ARE or WILL BE acquirable from
// the hot pool. Excludes Assigned/Running so that consuming a hot VM
// triggers an eager replacement clone in the next reconcile pass.
//
// Note: provisioning-bound-for-hot is added in manager.reconcileOnce via
// a separate query — Stats alone can't differentiate provisioning rows
// by pool_kind without an extra count.
func (s Stats) Available() int {
	return s.Hot + s.Booting
}

// Busy returns the count of VMs currently serving a job.
func (s Stats) Busy() int {
	return s.Assigned + s.Running
}

// LiveWarm returns the count of VMs in the warm budget.
func (s Stats) LiveWarm() int { return s.Warm }

// VM is the pool's external view of a managed VM. It is intentionally
// thin; the full state row lives in the ent store.
type VM struct {
	VMID int
	Node string
	Name string
}

// RowSnapshot is the reconciler's view of a single VM row. It excludes
// the timestamps the storage layer carries internally so the reconciler
// doesn't accidentally depend on storage layout. JobID and RunnerID are
// int64 with 0 meaning "unset".
type RowSnapshot struct {
	VMID       int
	Node       string
	Name       string
	State      string
	JobID      int64
	RunnerID   int64
	StateSince time.Time
	CreatedAt  time.Time
}

// RunnerInfo is the subset of GitHub's per-runner state that the
// orchestrator needs for adoption + reconciliation. ID == 0 only ever
// appears in synthesised "unknown" entries; production callers receive
// the GitHub-assigned positive int64.
type RunnerInfo struct {
	ID     int64
	Online bool
	Busy   bool
}

// RunnerLister returns every GitHub runner registered against this
// scaleset's scope whose name matches the orchestrator's prefix, keyed
// by runner name (which is also the VM name). Used by both the gh
// reconciler's polling loop and the leader's one-shot Adopt pass.
//
// Implementations MUST be safe for concurrent calls. A nil lister is
// allowed — Adopt treats it as "GitHub unavailable" and falls back to
// power-state-only classification.
type RunnerLister func(ctx context.Context) (map[string]RunnerInfo, error)

// Manager is the entry point for the rest of the orchestrator.
type Manager interface {
	// Acquire atomically transitions one Hot VM to Assigned, associates
	// it with the given job ID, and returns it. Returns ErrNoneAvailable
	// if no Hot VM is ready; ErrAtCapacity if the orchestrator is at its
	// max-concurrent ceiling or if the per-call maxBusy cap is hit.
	//
	// maxBusy is a per-call clamp on total busy (Assigned + Running)
	// enforced inside the same write transaction that performs the
	// Hot -> Assigned CAS. Callers that already know how many runners
	// they want to satisfy in this burst pass it as maxBusy so the
	// store can refuse mid-burst when a concurrent goroutine has
	// pushed busy past the requested target. Pass 0 to disable the
	// per-call clamp (only the orchestrator-wide
	// MaxConcurrentRunners cap applies).
	Acquire(ctx context.Context, jobID int64, maxBusy int) (*VM, error)

	// MarkRunning transitions a VM from Assigned to Running. Called from
	// the scaleset listener's HandleJobStarted callback.
	MarkRunning(ctx context.Context, vmid int, runnerID int64) error

	// SetRunnerID stamps the GitHub runner ID on the row without
	// changing its state. Called by the scaler immediately after
	// GenerateJitRunnerConfig returns so a sub-15s job that completes
	// before the gh.Reconciler observes the runner still has a
	// runner_id available for OnRunnerOrphaned to deregister.
	// Idempotent — a no-op if the row is missing or already has the
	// same id.
	SetRunnerID(ctx context.Context, vmid int, runnerID int64) error

	// MarkCompleted transitions a VM out of Running, queues it for
	// destruction, and signals a refill. Called from HandleJobCompleted.
	MarkCompleted(ctx context.Context, vmid int) error

	// PromoteToRunning is the reconciler-side equivalent of MarkRunning:
	// when GitHub reports a runner as busy but our DB still shows the row
	// as Assigned (because the listener-side JobStarted was lost), this
	// catches us up. Also accepts Hot → Running for the case where a job
	// got assigned before we even saw the transition. Idempotent.
	PromoteToRunning(ctx context.Context, vmid int, runnerID, jobID int64) error

	// ForceDestroy unconditionally transitions a row to Draining and
	// kicks destruction. Reason is logged for forensics — typical
	// callers are the reconciler's stuck-row sweeper and admin API.
	ForceDestroy(ctx context.Context, vmid int, reason string) error

	// ListRows returns a point-in-time snapshot of every non-terminal row.
	// Used by the GitHub reconciler to join DB state against the runners
	// API response. The result is a copy; callers may not mutate it back.
	ListRows(ctx context.Context) ([]RowSnapshot, error)

	// Stats returns a snapshot of the pool.
	Stats(ctx context.Context) (Stats, error)

	// Adopt seeds the (empty) in-memory state from Proxmox + GitHub
	// observations on startup, preserving every owner-tagged VM and its
	// in-flight job across leader transitions. For each VM returned by
	// Provisioner.ListOwnedVMs, it queries the VM's Proxmox power state
	// and (when a RunnerLister is wired) the corresponding GitHub
	// runner, then inserts a row with the best-inferred PoolKind, State,
	// and RunnerID. The continuous gh.Reconciler matrix converges any
	// approximations on its next tick. Must be called before Run.
	//
	// Adopt is best-effort by design: a transient Proxmox or GitHub API
	// blip skips an individual VM (logged at warn) rather than aborting
	// leader startup. Returns a non-nil error only when ListOwnedVMs
	// itself fails — the caller is expected to log it and continue with
	// an empty pool (the orphan-sweep in gh.Reconciler will reap any
	// stranded VMs once the API recovers).
	Adopt(ctx context.Context) error

	// Run is the reconcile loop. Blocks until ctx is cancelled, then
	// performs a graceful drain.
	Run(ctx context.Context) error

	// SignalRefill wakes the reconcile loop. Safe to call from any
	// goroutine; coalesces concurrent calls.
	SignalRefill()

	// SetDesiredCount records GitHub's most recent "total assigned jobs"
	// signal. The reconcile loop uses max(HotSize, desiredCount) as the
	// effective hot-pool floor, so when GitHub has more jobs queued than
	// our steady-state HotSize, we scale up — capped by
	// MaxConcurrentRunners. Setting back to 0 (or below HotSize) drops
	// us back to the steady-state floor on the next reconcile pass.
	SetDesiredCount(n int)
}

// validateConfig returns a descriptive error if poolConfig + maxConcurrent
// are internally inconsistent.
func validateConfig(hotSize, warmSize, maxConcurrent int) error {
	if hotSize < 0 || warmSize < 0 {
		return fmt.Errorf("pool: hot/warm sizes must be non-negative (hot=%d warm=%d)", hotSize, warmSize)
	}
	if maxConcurrent <= 0 {
		return fmt.Errorf("pool: max_concurrent_runners must be > 0 (got %d)", maxConcurrent)
	}
	if hotSize+warmSize > maxConcurrent {
		return fmt.Errorf("pool: hot+warm (%d) exceeds max_concurrent (%d)", hotSize+warmSize, maxConcurrent)
	}
	return nil
}
