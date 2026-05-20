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
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
	"golang.org/x/sync/semaphore"

	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/observability"
	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/pool"
	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/provisioner"
	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/runnertoken"
)

// Compile-time assertion that *Scaler satisfies the listener.Scaler contract.
var _ listener.Scaler = (*Scaler)(nil)

// Config bundles the static information the Scaler needs.
type Config struct {
	ScaleSetID int
	WorkFolder string // "_work" by default
	NamePrefix string // matches the pool's VM name prefix

	// HookCallbackURL is the URL the in-VM runner-hook script POSTs
	// lifecycle events to. Empty means hooks are disabled and the
	// per-job token is not minted/injected.
	HookCallbackURL string

	// HookTokenMinter mints a per-job HMAC JWT bound to the VM's vmid
	// and the runner's runner_id. The minted token is injected into the
	// VM as SCALESET_HOOK_TOKEN and presented by the in-VM hook script
	// when calling back the runner_hook receiver. Required when
	// HookCallbackURL is set.
	HookTokenMinter *runnertoken.Minter
}

// Scaler implements scaleset.Scaler against the orchestrator's pool.
type Scaler struct {
	cfg     Config
	gh      *scaleset.Client
	pool    pool.Manager
	prov    provisioner.Provisioner
	log     *slog.Logger
	metrics *observability.Metrics
}

// New constructs a Scaler.
func New(cfg Config, gh *scaleset.Client, p pool.Manager, prov provisioner.Provisioner, log *slog.Logger, metrics *observability.Metrics) *Scaler {
	if log == nil {
		log = slog.Default()
	}
	if cfg.WorkFolder == "" {
		cfg.WorkFolder = "_work"
	}
	return &Scaler{cfg: cfg, gh: gh, pool: p, prov: prov, log: log, metrics: metrics}
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

	// Pre-acquire serially: Acquire is cheap (single store write
	// transaction), and serialising it surfaces pool exhaustion fast so
	// we don't spin up worker goroutines for VMs we'll never get.
	vms := make([]*pool.VM, 0, count)
	for range count {
		const jobID int64 = 0 // not yet known; JobStarted callback updates
		vmObj, err := s.pool.Acquire(ctx, jobID)
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
			_ = s.pool.MarkCompleted(ctx, vmObj.VMID)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer sem.Release(1)
			if s.provisionOne(ctx, vmObj) {
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
	s.cleanupStaleRunnerByName(ctx, vmObj.Name)

	jitCfg, err := s.gh.GenerateJitRunnerConfig(ctx, &scaleset.RunnerScaleSetJitRunnerSetting{
		Name:       vmObj.Name,
		WorkFolder: s.cfg.WorkFolder,
	}, s.cfg.ScaleSetID)
	if err != nil {
		s.log.Error("jit config generation failed; releasing vm", "vmid", vmObj.VMID, "err", err)
		if s.metrics != nil {
			s.metrics.GitHubErrors.WithLabelValues("generate_jit").Inc()
		}
		_ = s.pool.MarkCompleted(ctx, vmObj.VMID) // destroy and refill
		return false
	}

	var runnerID int64
	if jitCfg.Runner != nil {
		runnerID = int64(jitCfg.Runner.ID)
	}
	if err := s.injectWithRetry(ctx, &provisioner.VM{
		VMID: vmObj.VMID, Node: vmObj.Node, Name: vmObj.Name,
	}, jitCfg.EncodedJITConfig, runnerID); err != nil {
		s.log.Error("jit injection failed (after retries); releasing vm", "vmid", vmObj.VMID, "err", err)
		if s.metrics != nil {
			s.metrics.ProxmoxErrors.WithLabelValues("inject_jit", vmObj.Node).Inc()
		}
		// Also deregister the runner we just minted; otherwise the
		// next clone of this VMID will hit a 409.
		s.cleanupStaleRunnerByName(ctx, vmObj.Name)
		_ = s.pool.MarkCompleted(ctx, vmObj.VMID)
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
func (s *Scaler) cleanupStaleRunnerByName(ctx context.Context, name string) {
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

// hookEnv returns the extra env-file lines to inject alongside the JIT
// config, or nil when in-VM runner hooks are not configured. Each call
// mints a fresh per-job HMAC-signed JWT bound to (vmid, runnerID) — see
// internal/runnertoken for the threat model.
func (s *Scaler) hookEnv(vmid int, runnerID int64) (map[string]string, error) {
	if s.cfg.HookCallbackURL == "" || s.cfg.HookTokenMinter == nil {
		return nil, nil
	}
	token, err := s.cfg.HookTokenMinter.Mint(vmid, runnerID)
	if err != nil {
		return nil, fmt.Errorf("mint runner-hook token: %w", err)
	}
	return map[string]string{
		"SCALESET_HOOK_URL":   s.cfg.HookCallbackURL,
		"SCALESET_HOOK_TOKEN": token,
	}, nil
}

// injectWithRetry calls InjectJITConfig with a longer retry budget than
// the underlying HTTP transport for the specific "VM is not running"
// transient error. This error is misleading — Proxmox returns it when
// the qemu-guest-agent socket is briefly unresponsive (e.g., when
// in-VM firstboot scripts churn systemd). The VM is usually fine
// within 10-30s; we retry the inject so an unlucky timing window
// doesn't burn a VM.
func (s *Scaler) injectWithRetry(ctx context.Context, vm *provisioner.VM, jit string, runnerID int64) error {
	extraEnv, err := s.hookEnv(vm.VMID, runnerID)
	if err != nil {
		return err
	}
	// Bound by both attempts (6) and wall-clock (60s) so a stuck VM
	// can't pin the scaler past the listener's response deadline.
	const (
		maxAttempts  = 6
		maxWallClock = 60 * time.Second
	)
	retryCtx, cancel := context.WithTimeout(ctx, maxWallClock)
	defer cancel()
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := s.prov.InjectJITConfig(retryCtx, vm, jit, extraEnv)
		if err == nil {
			if attempt > 1 {
				s.log.Info("jit inject recovered", "vmid", vm.VMID, "attempts", attempt)
			}
			return nil
		}
		lastErr = err
		// Non-transient errors fail fast.
		if !isTransientInjectError(err) {
			return err
		}
		if attempt < maxAttempts {
			// 2, 4, 6, 8, 10s = 30s cumulative beyond HTTP transport retries
			backoff := time.Duration(attempt*2) * time.Second
			s.log.Warn("jit inject failed; retrying", "vmid", vm.VMID, "attempt", attempt, "backoff", backoff, "err", err)
			select {
			case <-time.After(backoff):
			case <-retryCtx.Done():
				return fmt.Errorf("inject: %w (last: %w)", retryCtx.Err(), lastErr)
			}
		}
	}
	return lastErr
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
// "gh-runner-proxmox-ubuntu-x64-10042".
func vmidFromRunnerName(name, prefix string) (int, bool) {
	if !strings.HasPrefix(name, prefix) {
		return 0, false
	}
	rest := strings.TrimPrefix(name, prefix)
	if rest == "" {
		return 0, false
	}
	var vmid int
	if _, err := fmt.Sscanf(rest, "%d", &vmid); err != nil {
		return 0, false
	}
	return vmid, vmid > 0
}
