// Package scaler implements the scaleset.Scaler contract by translating
// GitHub-side demand signals into pool operations.
//
// The package is small on purpose — most of the heavy lifting lives in the
// pool manager and the provisioner. The Scaler:
//
//   - Receives HandleDesiredRunnerCount from the listener and drives the
//     pool to provision enough Hot VMs to cover it.
//   - For each VM it acquires, asks GitHub for a JIT runner config, injects
//     it into the VM via the guest agent, and lets the in-VM runner
//     self-register.
//   - Records job-lifecycle callbacks (JobStarted, JobCompleted) by
//     transitioning the corresponding VM row.
package scaler

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
	"github.com/cenkalti/backoff/v5"
	"golang.org/x/sync/semaphore"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/observability"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/pool"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/provisioner"
)

// Compile-time assertion that *Scaler satisfies the listener.Scaler contract.
var _ listener.Scaler = (*Scaler)(nil)

// Config bundles the static information the Scaler needs.
type Config struct {
	ScaleSetID int
	WorkFolder string // "_work" by default
	NamePrefix string // matches the pool's VM name prefix
}

// Scaler implements scaleset.Scaler against the orchestrator's pool.
type Scaler struct {
	cfg     Config
	gh      *scaleset.Client
	pool    pool.Manager
	prov    provisioner.Provisioner
	log     *slog.Logger
	metrics *observability.Metrics

	// provisionOneFn is the per-VM mint+inject worker invoked from
	// HandleDesiredRunnerCount. Defaults to the production
	// provisionOne; tests override it to isolate the acquire/clamp
	// logic from the GitHub + Proxmox call paths.
	provisionOneFn func(ctx context.Context, vmObj *pool.VM) bool
}

// New constructs a Scaler.
func New(cfg Config, gh *scaleset.Client, p pool.Manager, prov provisioner.Provisioner, log *slog.Logger, metrics *observability.Metrics) *Scaler {
	if log == nil {
		log = slog.Default()
	}
	if cfg.WorkFolder == "" {
		cfg.WorkFolder = "_work"
	}
	s := &Scaler{cfg: cfg, gh: gh, pool: p, prov: prov, log: log, metrics: metrics}
	s.provisionOneFn = s.provisionOne
	return s
}

// HandleJobStarted is called when GitHub assigns a queued job to one of our
// JIT runners. We transition the matching VM row Assigned -> Running.
func (s *Scaler) HandleJobStarted(ctx context.Context, info *scaleset.JobStarted) error {
	if s.metrics != nil {
		s.metrics.ListenerMessages.WithLabelValues("job_started").Inc()
	}
	vmid, ok := vmidFromRunnerName(info.RunnerName, s.cfg.NamePrefix)
	if !ok {
		s.log.Warn("job_started: cannot derive vmid from runner name", "runner_name", info.RunnerName)
		return nil
	}
	return s.pool.MarkRunning(ctx, vmid, int64(info.RunnerID))
}

// HandleJobCompleted is called when a job finishes. We destroy the VM.
func (s *Scaler) HandleJobCompleted(ctx context.Context, info *scaleset.JobCompleted) error {
	if s.metrics != nil {
		s.metrics.ListenerMessages.WithLabelValues("job_completed").Inc()
		// JobDuration intentionally NOT observed here — the listener
		// payload doesn't carry the runner's start time, and the
		// orchestrator's clock differs from the runner VM's. Track
		// duration via traces (Acquire→destroy span) instead.
	}
	vmid, ok := vmidFromRunnerName(info.RunnerName, s.cfg.NamePrefix)
	if !ok {
		s.log.Warn("job_completed: cannot derive vmid from runner name", "runner_name", info.RunnerName)
		return nil
	}
	return s.pool.MarkCompleted(ctx, vmid)
}

// maxConcurrentProvisions caps how many per-runner JIT generation +
// inject operations run in parallel. Sized to keep total GitHub API
// throughput within the rate limit while still finishing a 50-runner
// burst in seconds instead of tens of seconds.
const maxConcurrentProvisions = 8

// HandleDesiredRunnerCount is called when GitHub tells us how many runners
// it wants. We attempt to acquire that many Hot VMs and inject JIT configs
// into them in parallel (bounded by maxConcurrentProvisions). The returned
// int reports how many we actually delivered; callers (the listener) use
// this as the new "max capacity" hint to GitHub.
//
// Why parallel: each per-runner provision is ~300-400ms of GitHub
// round-trips + Proxmox guest-agent calls. A serial loop over 50
// requested runners can blow past the listener's response deadline.
// Per-runner work is embarrassingly parallel — Acquire's CAS-inside-txn
// (see store.AcquireHot) keeps concurrency safe.
//
// If the hot pool is depleted we kick a refill and return what we managed
// to acquire — the next listener message will retry.
func (s *Scaler) HandleDesiredRunnerCount(ctx context.Context, count int) (int, error) {
	if s.metrics != nil {
		s.metrics.ListenerMessages.WithLabelValues("desired_count").Inc()
	}
	// Drive the pool's effective floor BEFORE we try to acquire — if
	// count > HotSize we want reconcile to clone the difference asap,
	// even for the runners we can't satisfy from the hot pool right now.
	s.pool.SetDesiredCount(count)
	if count <= 0 {
		return 0, nil
	}

	// Clamp to (count - already-busy). The actions/scaleset listener
	// re-sends the absolute desired count on session refresh; without
	// this clamp a repeated `desired=N` message acquires N MORE Hot
	// VMs every time, which produces dead runners that wait the full
	// assigned_grace before the GH reconciler force-destroys them. The
	// contract is "I want N runners total active right now," not "give
	// me N more."
	busy := 0
	if stats, err := s.pool.Stats(ctx); err == nil {
		busy = stats.Busy()
	} else {
		s.log.Warn("acquire: stats lookup failed; proceeding without clamp", "err", err)
	}
	need := count - busy
	if need <= 0 {
		return 0, nil
	}

	// Pre-acquire serially: Acquire is cheap (single store write
	// transaction), and serialising it surfaces pool exhaustion fast so
	// we don't spin up worker goroutines for VMs we'll never get.
	// Pass maxBusy=count so the store refuses an Acquire that would
	// push total busy past the listener's requested count — guards
	// against the race where another goroutine bumps busy between our
	// Stats read above and this loop, which would otherwise let us
	// over-acquire (bounded by MaxConcurrentRunners but still wasteful).
	vms := make([]*pool.VM, 0, need)
	for range need {
		const jobID int64 = 0 // not yet known; JobStarted callback updates
		vmObj, err := s.pool.Acquire(ctx, jobID, count)
		if err != nil {
			if errors.Is(err, pool.ErrNoneAvailable) || errors.Is(err, pool.ErrAtCapacity) {
				// Expected back-pressure; refill is already signalled.
				break
			}
			s.log.Error("acquire failed", "err", err)
			break
		}
		vms = append(vms, vmObj)
	}
	if len(vms) == 0 {
		return 0, nil
	}

	// Provision each acquired VM in parallel, bounded by the semaphore.
	// Errors during per-runner provisioning are LOGGED but NEVER
	// returned to the listener — returning an error would terminate the
	// scaleset listener loop and crash the orchestrator. Individual
	// failures are recoverable on the next listener message.
	var (
		delivered atomic.Int32
		wg        sync.WaitGroup
		sem       = semaphore.NewWeighted(maxConcurrentProvisions)
	)
	for _, vmObj := range vms {
		vmObj := vmObj
		if err := sem.Acquire(ctx, 1); err != nil {
			// ctx cancelled mid-burst — release the rest back to the pool.
			if mcErr := s.pool.MarkCompleted(ctx, vmObj.VMID); mcErr != nil {
				s.log.Warn("mark completed failed during cancel", "vmid", vmObj.VMID, "err", mcErr)
			}
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer sem.Release(1)
			if s.provisionOneFn(ctx, vmObj) {
				delivered.Add(1)
			}
		}()
	}
	wg.Wait()
	return int(delivered.Load()), nil
}

// provisionOne handles the per-runner GitHub-side mint + Proxmox-side
// inject for a single acquired VM. Returns true iff the VM was
// successfully delivered to GitHub.
func (s *Scaler) provisionOne(ctx context.Context, vmObj *pool.VM) bool {
	// Idempotent self-heal: if a runner with this name already exists
	// on GitHub (from a previous attempt that failed mid-flight),
	// deregister it before asking for a fresh JIT config. Without this,
	// repeated retries against the same VMID accumulate orphan runner
	// registrations and every subsequent GenerateJitRunnerConfig call
	// returns 409 "runner already exists", permanently breaking the pool.
	s.cleanupStaleRunnerByName(vmObj.Name) //nolint:contextcheck // deliberately detached; see function comment

	jitCfg, err := s.gh.GenerateJitRunnerConfig(ctx, &scaleset.RunnerScaleSetJitRunnerSetting{
		Name:       vmObj.Name,
		WorkFolder: s.cfg.WorkFolder,
	}, s.cfg.ScaleSetID)
	if err != nil {
		s.log.Error("jit config generation failed; releasing vm", "vmid", vmObj.VMID, "err", err)
		if s.metrics != nil {
			s.metrics.GitHubErrors.WithLabelValues("generate_jit").Inc()
		}
		if mcErr := s.pool.MarkCompleted(ctx, vmObj.VMID); mcErr != nil {
			s.log.Warn("mark completed failed after jit generation error", "vmid", vmObj.VMID, "err", mcErr)
		}
		return false
	}

	var runnerID int64
	if jitCfg.Runner != nil {
		runnerID = int64(jitCfg.Runner.ID)
	}
	// Stamp the runner_id on the row before injection. This closes the
	// race where a sub-15s job completes and is destroyed before the
	// gh.Reconciler observes the runner — without an id on the row,
	// OnRunnerOrphaned has nothing to deregister and the GitHub-side
	// registration leaks.
	if runnerID > 0 {
		if err := s.pool.SetRunnerID(ctx, vmObj.VMID, runnerID); err != nil {
			// Non-fatal: the gh.Reconciler will set it on its next pass —
			// for jobs longer than one reconcile interval. Sub-15s jobs
			// that complete before the next tick will be destroyed before
			// the reconciler ever observes their runner_id, and
			// OnRunnerOrphaned will have nothing to deregister. The
			// GitHub-side registration then leaks until the orphan-runner
			// sweep (cleanupOrphanRunners) reaps it on a subsequent tick.
			// We accept this — SetRunnerID failures here are typically a
			// row-was-destroyed-mid-flight signal, in which case the VM
			// is already on its way out and the leak is bounded.
			s.log.Warn("set runner id failed", "vmid", vmObj.VMID, "runner_id", runnerID, "err", err)
		}
	}

	if err := s.injectWithRetry(ctx, &provisioner.VM{
		VMID: vmObj.VMID, Node: vmObj.Node, Name: vmObj.Name,
	}, jitCfg.EncodedJITConfig); err != nil {
		s.log.Error("jit injection failed (after retries); releasing vm", "vmid", vmObj.VMID, "err", err)
		// Helper enforces the closed enum on `op` so a future caller
		// can't blow up Prometheus cardinality silently.
		s.metrics.RecordProxmoxError("inject_jit", vmObj.Node)
		// Also deregister the runner we just minted; otherwise the
		// next clone of this VMID will hit a 409.
		s.cleanupStaleRunnerByName(vmObj.Name) //nolint:contextcheck // deliberately detached; see function comment
		if mcErr := s.pool.MarkCompleted(ctx, vmObj.VMID); mcErr != nil {
			s.log.Warn("mark completed failed after jit injection error", "vmid", vmObj.VMID, "err", mcErr)
		}
		return false
	}
	return true
}

// cleanupStaleRunnerByName best-effort removes a runner registration
// matching the given name. Used both before generating a new JIT (to
// avoid 409 conflicts) and after a failed inject (to avoid leaking).
//
// A persistent failure to deregister stale runners can permanently
// break the pool (every clone of the same VMID hits 409), so we surface
// failures via the GitHubErrors metric — operators see the rate climb
// even though we don't return the error.
func (s *Scaler) cleanupStaleRunnerByName(name string) {
	// Detach from the listener's ctx: this cleanup is idempotent and
	// purely defensive (avoid 409 conflicts on retry / avoid leaking a
	// registration after a failed inject). A cancelled listener ctx
	// must not abort the in-flight GitHub deregister and leave a stale
	// registration behind.
	ctx, cancel := context.WithTimeout(context.Background(), staleRunnerCleanupTimeout)
	defer cancel()

	existing, err := s.gh.GetRunnerByName(ctx, name)
	if err != nil {
		if s.metrics != nil {
			s.metrics.GitHubErrors.WithLabelValues("get_runner_by_name").Inc()
		}
		s.log.Debug("stale runner cleanup: lookup failed", "name", name, "err", err)
		return
	}
	if existing == nil {
		// "not found" is the common case.
		return
	}
	if err := s.gh.RemoveRunner(ctx, int64(existing.ID)); err != nil {
		if s.metrics != nil {
			s.metrics.GitHubErrors.WithLabelValues("remove_stale_runner").Inc()
		}
		s.log.Warn("stale runner cleanup: remove failed", "name", name, "id", existing.ID, "err", err)
		return
	}
	s.log.Info("removed stale runner registration", "name", name, "id", existing.ID)
}

// staleRunnerCleanupTimeout bounds the detached GitHub round-trip for
// cleanupStaleRunnerByName. Long enough for a real GH API call, short
// enough that pathological GH unavailability can't pin the caller.
// Tests may override this.
var staleRunnerCleanupTimeout = 15 * time.Second

// injectWithRetry calls InjectJITConfig with a longer retry budget than
// the underlying HTTP transport for the specific "VM is not running"
// transient error. This error is misleading — Proxmox returns it when
// the qemu-guest-agent socket is briefly unresponsive (e.g., when
// in-VM firstboot scripts churn systemd). The VM is usually fine
// within 10-30s; we retry the inject so an unlucky timing window
// doesn't burn a VM.
func (s *Scaler) injectWithRetry(ctx context.Context, vm *provisioner.VM, jit string) error {
	// Bound by both attempts (6) and wall-clock (60s) so a stuck VM
	// can't pin the scaler past the listener's response deadline.
	const (
		maxAttempts  = 6
		maxWallClock = 60 * time.Second
	)
	eb := backoff.NewExponentialBackOff()
	eb.InitialInterval = 2 * time.Second
	eb.MaxInterval = 10 * time.Second
	eb.Multiplier = 2.0
	eb.RandomizationFactor = 0 // deterministic; matches the prior fixed-step schedule
	var attempts int
	_, err := backoff.Retry(ctx, func() (struct{}, error) {
		attempts++
		err := s.prov.InjectJITConfig(ctx, vm, jit)
		if err == nil {
			return struct{}{}, nil
		}
		// Non-transient errors fail fast via Permanent so Retry stops immediately.
		if !isTransientInjectError(err) {
			return struct{}{}, backoff.Permanent(err)
		}
		return struct{}{}, err
	},
		backoff.WithBackOff(eb),
		backoff.WithMaxTries(maxAttempts),
		backoff.WithMaxElapsedTime(maxWallClock),
		backoff.WithNotify(func(err error, d time.Duration) {
			s.log.Warn("jit inject failed; retrying", "vmid", vm.VMID, "attempt", attempts, "backoff", d, "err", err)
		}),
	)
	if err != nil {
		return err
	}
	if attempts > 1 {
		s.log.Info("jit inject recovered", "vmid", vm.VMID, "attempts", attempts)
	}
	return nil
}

// isTransientInjectError recognises the "agent socket briefly
// unreachable" class via the typed sentinel the provisioner exposes.
// Centralising the detection there means a Proxmox version that changes
// the error wording is a one-line fix in the provisioner, not a
// scattered grep-and-replace across consumers.
func isTransientInjectError(err error) bool {
	return errors.Is(err, provisioner.ErrGuestAgentNotReady)
}

// vmidFromRunnerName extracts the VMID we encoded into the runner name at
// clone time. Naming convention is "<prefix><vmid>", e.g.
// "gh-runner-proxmox-ubuntu-x64-10042". strconv.Atoi (not fmt.Sscanf %d)
// rejects trailing garbage — a malformed name like "<prefix>10042xyz"
// must not map to vmid 10042.
func vmidFromRunnerName(name, prefix string) (int, bool) {
	if !strings.HasPrefix(name, prefix) {
		return 0, false
	}
	rest := strings.TrimPrefix(name, prefix)
	vmid, err := strconv.Atoi(rest)
	if err != nil || vmid <= 0 {
		return 0, false
	}
	return vmid, true
}
