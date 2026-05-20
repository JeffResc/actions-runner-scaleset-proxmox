package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/semaphore"

	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/config"
	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/nodeselector"
	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/observability"
	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/provisioner"
	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/store"
)

// tracer is the package-level OpenTelemetry tracer. When tracing isn't
// initialised (no endpoint configured) this resolves to a no-op tracer
// so all instrumented paths stay cheap.
var tracer = otel.Tracer(observability.TracerName)

// logRecoveredPanic logs a recovered panic. Callers MUST invoke recover()
// directly in their deferred closure (Go spec: recover only returns
// non-nil when called directly by a deferred function — a nested call
// through this helper would silently no-op). The closure then passes
// the recovered value here so it can log alongside the op-name and the
// current vmid (which the closure can capture by reference, letting
// the log reflect the vmid AT PANIC TIME rather than at defer time).
func (m *manager) logRecoveredPanic(opName string, vmid int, r any) {
	if r == nil {
		return
	}
	m.log.Error("panic in async pool worker",
		"op", opName, "vmid", vmid, "panic", fmt.Sprintf("%v", r))
}

// Config bundles everything the manager needs at construction time.
type Config struct {
	HotSize              int
	WarmSize             int
	MaxConcurrentRunners int
	ReconcileInterval    time.Duration
	VMMaxAge             time.Duration
	DrainTimeout         time.Duration
	BootMaxAttempts      int

	// PowerPollInterval is the cadence at which the manager polls
	// Proxmox for the power state of Assigned/Running VMs. When a row's
	// VM appears stopped, the row is queued for destruction — this is
	// the orchestrator's "job completed" signal in lieu of the deleted
	// in-VM runner-hook. Zero falls back to a sane default (3s).
	PowerPollInterval time.Duration

	ScaleSetName string
	VMNamePrefix string // e.g. "gh-runner-<scaleset>-"
	VMIDRange    config.VMIDRange
	LinkedClones bool
	TemplateNode string // returned by Provisioner.TemplateNode()
	GuestAgentTO time.Duration

	// OnRunnerOrphaned is invoked when the manager destroys a VM whose
	// row had a runner_id set, i.e. a runner that was registered with
	// GitHub. The callback is expected to deregister the runner. Best
	// effort — errors are logged but don't block destruction. Nil is OK
	// and treated as a no-op (e.g. in tests).
	OnRunnerOrphaned func(ctx context.Context, runnerID int64) error
}

// manager is the in-process Manager implementation.
type manager struct {
	cfg     Config
	store   *store.Store
	prov    provisioner.Provisioner
	sel     nodeselector.Selector
	log     *slog.Logger
	metrics *observability.Metrics

	refill chan struct{}

	// Per-operation goroutine governance. Three independent semaphores
	// so a burst of one op-class can't starve another: a hot pool can
	// still destroy excess capacity while clones are saturating their
	// budget. Each spawn site (kickClone, destroyAsync, runBoot) takes
	// from the matching semaphore; if it can't acquire under the
	// caller's ctx, the spawn is logged and dropped (a future reconcile
	// tick will retry, since the underlying state isn't lost).
	cloneSem   *semaphore.Weighted // concurrent Clone ops
	destroySem *semaphore.Weighted // concurrent Destroy ops
	bootSem    *semaphore.Weighted // concurrent Start/WaitReady ops
	wg         sync.WaitGroup      // tracks in-flight async operations

	// workerCtx is the parent for every async Proxmox operation (clone,
	// destroy, boot). It is created once in NewManager rooted at
	// context.Background and cancelled by drain() when:
	//   (a) drain's wg-wait exceeds DrainTimeout, or
	//   (b) drain completes naturally (so any racing spawn after
	//       wg.Wait returns is cancelled too).
	// The reason for using Background as the root rather than Run's ctx
	// is that we deliberately want async workers to OUTLIVE Run's ctx
	// briefly — drain() wants to wait for them to finish naturally
	// before force-cancelling. workerCtx is created once and never
	// reassigned, so it can be read without synchronisation.
	workerCtx    context.Context
	workerCancel context.CancelFunc

	// allocMu serialises VMID allocation + the matching row insert so
	// concurrent reconcile goroutines can't pick the same VMID. The
	// store's unique constraint would catch the dup on insert, but
	// holding this lock means we fail-fast in the allocator instead of
	// wasting work on Proxmox clone calls that would have to be undone.
	allocMu sync.Mutex

	// desiredCount is GitHub's most recent "total assigned jobs" signal.
	// Read/written via atomic ops so the reconcile loop doesn't need to
	// take a lock on every pass.
	desiredCount atomic.Int32
}

// NewManager constructs a Manager.
func NewManager(cfg Config, st *store.Store, prov provisioner.Provisioner, sel nodeselector.Selector, log *slog.Logger, metrics *observability.Metrics) (Manager, error) {
	if err := validateConfig(cfg.HotSize, cfg.WarmSize, cfg.MaxConcurrentRunners); err != nil {
		return nil, err
	}
	if cfg.ReconcileInterval <= 0 {
		cfg.ReconcileInterval = 10 * time.Second
	}
	if cfg.GuestAgentTO <= 0 {
		cfg.GuestAgentTO = 90 * time.Second
	}
	if cfg.PowerPollInterval <= 0 {
		cfg.PowerPollInterval = 3 * time.Second
	}
	if cfg.BootMaxAttempts <= 0 {
		cfg.BootMaxAttempts = 3
	}
	if log == nil {
		log = slog.Default()
	}
	// Concurrent op caps. Chosen so each class can drive the Proxmox
	// API at reasonable parallelism without saturating the others:
	//   - clones: heavy (disk I/O + multi-second tasks); cap at 8
	//   - destroys: medium (single API + cleanup); cap at 8
	//   - boots: light (mostly WaitReady poll); cap at 16 so the
	//     pool can recover quickly after a burst completes
	// These are deliberately separate from MaxConcurrentRunners: that
	// caps how many VMs CAN exist; these cap how fast we change state.
	const (
		maxConcurrentClones   = 8
		maxConcurrentDestroys = 8
		maxConcurrentBoots    = 16
	)
	// workerCtx is rooted at Background so async workers can outlive
	// Run's ctx briefly during drain. drain() cancels it once it has
	// either observed clean completion or hit DrainTimeout.
	wctx, wcancel := context.WithCancel(context.Background())
	return &manager{
		cfg:          cfg,
		store:        st,
		prov:         prov,
		sel:          sel,
		log:          log,
		metrics:      metrics,
		refill:       make(chan struct{}, 1),
		cloneSem:     semaphore.NewWeighted(maxConcurrentClones),
		destroySem:   semaphore.NewWeighted(maxConcurrentDestroys),
		bootSem:      semaphore.NewWeighted(maxConcurrentBoots),
		workerCtx:    wctx,
		workerCancel: wcancel,
	}, nil
}

// SignalRefill nudges the reconcile loop without blocking.
func (m *manager) SignalRefill() {
	select {
	case m.refill <- struct{}{}:
	default:
	}
}

// SetDesiredCount records the listener-side "total assigned jobs" so
// reconcile can scale up beyond HotSize when the burst calls for it.
func (m *manager) SetDesiredCount(n int) {
	if n < 0 {
		n = 0
	}
	prev := m.desiredCount.Swap(int32(n))
	if int(prev) != n {
		m.log.Debug("desired count updated", "from", prev, "to", n)
	}
	m.SignalRefill()
}

// Stats returns a pool-population snapshot.
func (m *manager) Stats(_ context.Context) (Stats, error) {
	raw, err := m.store.Stats()
	if err != nil {
		return Stats{}, fmt.Errorf("stats: %w", err)
	}
	stats := Stats{
		Provisioning: raw[store.StateProvisioning],
		Warm:         raw[store.StateWarm],
		Booting:      raw[store.StateBooting],
		Hot:          raw[store.StateHot],
		Assigned:     raw[store.StateAssigned],
		Running:      raw[store.StateRunning],
		Draining:     raw[store.StateDraining],
		Destroying:   raw[store.StateDestroying],
		Poison:       raw[store.StatePoison],
	}
	if m.metrics != nil {
		for st, n := range raw {
			m.metrics.PoolSize.WithLabelValues(string(st)).Set(float64(n))
		}
	}
	return stats, nil
}

// Acquire atomically transitions one Hot VM to Assigned. Selection is by
// oldest-Hot-first (preferring VMs near max-age recycle so we don't carry
// stale VMs forever).
//
// The cap check (busy < MaxConcurrentRunners) and the Hot→Assigned CAS
// happen inside the same store write transaction, so concurrent Acquire
// callers cannot over-provision past the cap.
func (m *manager) Acquire(ctx context.Context, jobID int64) (*VM, error) {
	ctx, span := tracer.Start(ctx, "pool.Acquire",
		trace.WithAttributes(attribute.Int64("job.id", jobID)))
	defer span.End()
	_ = ctx
	row, err := m.store.AcquireHot(jobID, m.cfg.MaxConcurrentRunners)
	switch {
	case errors.Is(err, store.ErrAtCapacity):
		span.SetStatus(codes.Ok, "at_capacity")
		if m.metrics != nil {
			m.metrics.AtCapacityTotal.Inc()
		}
		return nil, ErrAtCapacity
	case errors.Is(err, store.ErrNoneAvailable):
		span.SetStatus(codes.Ok, "none_available")
		m.SignalRefill()
		return nil, ErrNoneAvailable
	case err != nil:
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("acquire: %w", err)
	}
	span.SetAttributes(
		attribute.Int("vm.id", row.VMID),
		attribute.String("vm.node", row.Node),
	)
	m.SignalRefill()
	return &VM{VMID: row.VMID, Node: row.Node, Name: row.Name}, nil
}

// SetRunnerID stamps RunnerID on the row without changing state. Used by
// the scaler right after GenerateJitRunnerConfig so the row carries the
// id before any job/runner-side transition has a chance to fire — closes
// the race where a sub-15s job completes before MarkRunning/PromoteToRunning
// runs and OnRunnerOrphaned then leaks the GitHub registration.
func (m *manager) SetRunnerID(_ context.Context, vmid int, runnerID int64) error {
	if runnerID <= 0 {
		return nil
	}
	_, err := m.store.Update(vmid, func(v *store.VM) {
		v.RunnerID = runnerID
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("set runner id: %w", err)
	}
	return nil
}

// MarkRunning transitions Assigned → Running and stamps the runner ID.
func (m *manager) MarkRunning(_ context.Context, vmid int, runnerID int64) error {
	ok, err := m.store.UpdateState(vmid, store.StateAssigned, store.StateRunning, func(v *store.VM) {
		v.RunnerID = runnerID
	})
	if err != nil {
		return fmt.Errorf("mark running: %w", err)
	}
	if !ok {
		// Row might already be Running (duplicate callback) or further along.
		m.log.Debug("mark running: no state change applied", "vmid", vmid)
	}
	return nil
}

// PromoteToRunning catches up a row to Running when the listener-side
// JobStarted callback was lost. Accepts both Assigned → Running (the
// common case) and Hot → Running (the rare case where GitHub assigned a
// job before we even observed the assignment). A row already past Running
// is left alone — this method is idempotent.
func (m *manager) PromoteToRunning(_ context.Context, vmid int, runnerID, jobID int64) error {
	ok, err := m.store.UpdateStateIn(vmid,
		[]store.State{store.StateAssigned, store.StateHot},
		store.StateRunning,
		func(v *store.VM) {
			v.RunnerID = runnerID
			if jobID != 0 {
				v.JobID = jobID
			}
		},
	)
	if err != nil {
		return fmt.Errorf("promote to running: %w", err)
	}
	if !ok {
		m.log.Debug("promote to running: no state change applied", "vmid", vmid)
	}
	return nil
}

// ForceDestroy unconditionally transitions a row to Draining and kicks
// the destroy goroutine. Used by the reconciler when GitHub tells us the
// runner is gone but the store still thinks it's busy. Reason is logged
// so the forensic trail is preserved.
func (m *manager) ForceDestroy(_ context.Context, vmid int, reason string) error {
	target, err := m.store.Get(vmid)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil // already gone
		}
		return fmt.Errorf("force destroy: lookup: %w", err)
	}
	if target.State == store.StateDraining || target.State == store.StateDestroying {
		// Already on the way out; just make sure the destroy goroutine
		// is in flight in case the previous attempt died with the process.
		m.destroyAsync(target.VMID, target.Node)
		return nil
	}
	m.log.Warn("force destroy", "vmid", vmid, "from_state", target.State, "reason", reason)
	if _, err := m.store.Update(vmid, func(v *store.VM) {
		v.State = store.StateDraining
		v.StateSince = time.Now()
	}); err != nil {
		return fmt.Errorf("force destroy: drain: %w", err)
	}
	m.destroyAsync(target.VMID, target.Node)
	return nil
}

// ListRows returns a snapshot of every non-terminal VM row for the
// GitHub reconciler. Terminal rows (Draining, Destroying) are excluded
// because the reconciler shouldn't second-guess in-flight destruction.
func (m *manager) ListRows(_ context.Context) ([]RowSnapshot, error) {
	rows, err := m.store.ListExcludingStates(store.StateDraining, store.StateDestroying)
	if err != nil {
		return nil, fmt.Errorf("list rows: %w", err)
	}
	out := make([]RowSnapshot, 0, len(rows))
	for _, r := range rows {
		out = append(out, RowSnapshot{
			VMID:       r.VMID,
			Node:       r.Node,
			Name:       r.Name,
			State:      string(r.State),
			JobID:      r.JobID,
			RunnerID:   r.RunnerID,
			StateSince: r.StateSince,
			CreatedAt:  r.CreatedAt,
		})
	}
	return out, nil
}

// MarkCompleted transitions a busy VM (Assigned or Running) → Draining
// and kicks destruction in the background. Refuses to act on rows in
// any other state — this prevents a stray runner-hook event from
// destroying a Hot/Booting VM, and a race with ForceDestroy from
// reverting an already-destroying row.
//
// Idempotent: a row already in Draining/Destroying gets a no-op return
// (the existing destroy goroutine handles cleanup).
func (m *manager) MarkCompleted(_ context.Context, vmid int) error {
	target, err := m.store.Get(vmid)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil // already gone
		}
		return fmt.Errorf("mark completed: lookup: %w", err)
	}
	switch target.State {
	case store.StateAssigned, store.StateRunning:
		// proceed
	case store.StateDraining, store.StateDestroying:
		// Already on the way out — just signal a refill in case the
		// previous destroy is mid-flight and the freed slot hasn't been
		// announced yet.
		m.SignalRefill()
		return nil
	default:
		// Hot / Warm / Booting / Provisioning / Poison: a runner-hook
		// "completed" event for a row in these states is either a
		// spoof or a wildly stale retry. Refuse.
		m.log.Warn("mark completed: refused for non-busy row",
			"vmid", vmid, "state", target.State)
		return nil
	}
	// CAS Assigned/Running → Draining inside one txn so a concurrent
	// ForceDestroy can't revert us.
	ok, err := m.store.UpdateStateIn(vmid,
		[]store.State{store.StateAssigned, store.StateRunning},
		store.StateDraining,
		func(v *store.VM) { v.StateSince = time.Now() },
	)
	if err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}
	if !ok {
		// Lost the race to another writer — treat as already handled.
		return nil
	}
	// destroyAsync (not a raw `go m.destroy(...)`): the latter would
	// bypass destroySem, and a burst of completions could fire many
	// parallel Destroy calls against Proxmox.
	m.destroyAsync(target.VMID, target.Node)
	m.SignalRefill()
	return nil
}

// Recover reconciles the (empty) in-memory store against Proxmox reality
// on startup. With no persistent state to load, this collapses to
// "destroy any Proxmox VM tagged as ours that has no in-memory row" —
// i.e. clean up orphans left by a previous process crash.
//
// Returns a non-nil error if any orphan destruction failed; the caller
// should refuse to start unless an explicit override is in effect.
// Partial recovery is the worst possible state: the orchestrator would
// start cloning fresh VMs on top of leaked ones, leading to the very
// resource exhaustion Recover exists to prevent.
func (m *manager) Recover(ctx context.Context) error {
	pmoxVMs, err := m.prov.ListOwnedVMs(ctx)
	if err != nil {
		return fmt.Errorf("recover: list owned vms: %w", err)
	}
	var failed []error
	for _, pv := range pmoxVMs {
		m.log.Warn("recover: orphan proxmox vm; destroying", "vmid", pv.VMID, "node", pv.Node)
		if err := m.prov.Destroy(ctx, pv); err != nil {
			m.log.Warn("recover: destroy orphan failed", "vmid", pv.VMID, "err", err)
			failed = append(failed, fmt.Errorf("vmid=%d node=%s: %w", pv.VMID, pv.Node, err))
			continue
		}
		if m.metrics != nil {
			m.metrics.VMsTotal.WithLabelValues("recovered_orphan").Inc()
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("recover: %d of %d orphans failed to destroy: %w",
			len(failed), len(pmoxVMs), errors.Join(failed...))
	}
	return nil
}

// Run is the reconcile loop.
//
// Async workers (clone, destroy, boot) derive their context from
// workerCtx (set in NewManager and never reassigned). drain() observes
// ctx cancellation: it waits up to DrainTimeout for natural completion,
// then force-cancels workerCtx so in-flight Proxmox calls see ctx.Done
// and unwind — without that escalation a single stuck destroy could pin
// the process well past DrainTimeout.
//
// A second goroutine runs the power-state poller — it watches every
// Assigned/Running row and treats a Proxmox-side "stopped" power state
// as the JobCompleted signal (the in-VM gh-runner.service powers off
// when the runner exits). The poller exits when ctx is cancelled; like
// the reconcile loop it's bounded by the manager's lifetime.
func (m *manager) Run(ctx context.Context) error {
	tick := time.NewTicker(m.cfg.ReconcileInterval)
	defer tick.Stop()

	// Kick once on entry.
	m.SignalRefill()

	// Power-state poller. Runs independently of the reconcile loop so
	// a slow Proxmox reply doesn't delay reconcile and vice versa.
	pollerDone := make(chan struct{})
	go func() {
		defer close(pollerDone)
		m.runPowerPoll(ctx)
	}()
	defer func() { <-pollerDone }()

	for {
		select {
		case <-ctx.Done():
			m.drain()
			return nil
		case <-tick.C:
			m.reconcileOnce(ctx)
		case <-m.refill:
			m.reconcileOnce(ctx)
		}
	}
}

// runPowerPoll observes Proxmox-side power state for Assigned/Running
// VMs and queues a MarkCompleted on any that have flipped to "stopped".
// This is the orchestrator's job-completion signal: the runner unit's
// ExecStopPost is `systemctl poweroff`, so a stopped VM means the job
// finished and the runner exited.
//
// Errors are logged and skipped per-VM — one Proxmox API blip mustn't
// short-circuit the whole pass. The next tick will pick up rows we
// missed.
func (m *manager) runPowerPoll(ctx context.Context) {
	tick := time.NewTicker(m.cfg.PowerPollInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			m.powerPollOnce(ctx)
		}
	}
}

// powerPollOnce does a single pass over Assigned/Running rows. Exposed
// (lower-case) so tests can drive it deterministically without spinning
// the time-based Run loop.
func (m *manager) powerPollOnce(ctx context.Context) {
	rows, err := m.store.ListByState(store.StateAssigned, store.StateRunning)
	if err != nil {
		m.log.Warn("power-poll: list rows failed", "err", err)
		return
	}
	for _, row := range rows {
		state, err := m.prov.PowerState(ctx, &provisioner.VM{
			VMID: row.VMID, Node: row.Node, Name: row.Name,
		})
		if err != nil {
			m.log.Debug("power-poll: query failed; will retry", "vmid", row.VMID, "err", err)
			continue
		}
		// Empty string means "unknown" (VM not found). Skip — the
		// stuck-state sweep will reap genuinely-missing rows.
		if state == "" || state == "running" {
			continue
		}
		// Anything else ("stopped", "paused", ...) signals the job is
		// no longer executing. MarkCompleted is idempotent and refuses
		// rows already in Draining/Destroying, so a duplicate poll
		// observation is harmless.
		m.log.Info("power-poll: vm not running; marking completed",
			"vmid", row.VMID, "state", state, "db_state", row.State)
		if err := m.MarkCompleted(ctx, row.VMID); err != nil {
			m.log.Warn("power-poll: mark completed failed", "vmid", row.VMID, "err", err)
		}
	}
}

// reconcileOnce computes the desired pool population and dispatches the
// async operations to reach it.
func (m *manager) reconcileOnce(ctx context.Context) {
	start := time.Now()
	defer func() {
		if m.metrics != nil {
			m.metrics.ReconcileDuration.Observe(time.Since(start).Seconds())
		}
	}()

	stats, err := m.Stats(ctx)
	if err != nil {
		m.log.Warn("reconcile: stats failed", "err", err)
		return
	}

	hotProv, err := m.store.CountByPoolKindState(store.PoolKindHot, store.StateProvisioning)
	if err != nil {
		m.log.Warn("reconcile: count hot-provisioning failed", "err", err)
		return
	}
	warmProv, err := m.store.CountByPoolKindState(store.PoolKindWarm, store.StateProvisioning)
	if err != nil {
		m.log.Warn("reconcile: count warm-provisioning failed", "err", err)
		return
	}
	// Two reasons to clone a hot VM:
	//   (a) Eager replacement: keep `available >= HotSize` so consuming
	//       a hot VM (Assigned) immediately triggers a refill clone.
	//   (b) Burst response: when GitHub's desiredCount exceeds the
	//       current in-flight runner count, scale up immediately.
	// Effective need is the larger of the two.
	available := stats.Available() + hotProv
	busy := stats.Busy()
	desired := int(m.desiredCount.Load())

	needIdle := m.cfg.HotSize - available
	needBurst := desired - (available + busy)
	needHot := needIdle
	if needBurst > needHot {
		needHot = needBurst
	}
	if needHot < 0 {
		needHot = 0
	}
	// Cap by remaining room under MaxConcurrentRunners.
	if room := m.cfg.MaxConcurrentRunners - (available + busy); room < needHot {
		needHot = room
	}
	if needHot < 0 {
		needHot = 0
	}

	warmInflight := stats.LiveWarm() + warmProv
	needWarm := m.cfg.WarmSize - warmInflight
	if needWarm < 0 {
		needWarm = 0
	}

	// Promote warm -> hot first (cheap).
	promoteN := needHot
	if promoteN > stats.Warm {
		promoteN = stats.Warm
	}
	if promoteN > 0 {
		m.promoteN(ctx, promoteN)
		needHot -= promoteN
	}

	// Clone whatever's left.
	for range needHot {
		m.kickClone(ctx, store.PoolKindHot, true)
	}
	for range needWarm {
		m.kickClone(ctx, store.PoolKindWarm, false)
	}

	// Shrink-to-floor: when the hot pool has grown beyond what we need
	// (typically after a burst completes and demand collapses back to
	// 0), destroy the excess. Target floor is max(HotSize, current
	// burst demand) — never go below HotSize, and never below the
	// shortfall the burst path is still trying to satisfy. Oldest hot
	// VMs are destroyed first so younger ones get full vm_max_age.
	//
	// We use a CAS (Hot -> Draining) so an in-flight Acquire can't
	// snipe a VM we just decided to destroy.
	hotTarget := m.cfg.HotSize
	if burstTarget := desired - busy; burstTarget > hotTarget {
		hotTarget = burstTarget
	}
	if stats.Hot > hotTarget {
		excess := stats.Hot - hotTarget
		hotRows, err := m.store.ListByState(store.StateHot)
		if err == nil {
			// Oldest first.
			sort.Slice(hotRows, func(i, j int) bool {
				return hotRows[i].CreatedAt.Before(hotRows[j].CreatedAt)
			})
			killed := 0
			for _, row := range hotRows {
				if killed >= excess {
					break
				}
				ok, err := m.store.UpdateState(row.VMID, store.StateHot, store.StateDraining, func(v *store.VM) {
					v.StateSince = time.Now()
				})
				if err != nil || !ok {
					continue
				}
				m.log.Info("shrink: hot pool over target; destroying excess",
					"vmid", row.VMID, "hot_size", m.cfg.HotSize, "target", hotTarget, "current_hot", stats.Hot)
				m.destroyAsync(row.VMID, row.Node)
				killed++
			}
		}
	}

	// Stuck-state sweep: rows that have been in a Proxmox-side
	// transient state for too long (typically because Proxmox returned
	// a transient error during clone/start/destroy) get re-queued.
	// This keeps the orchestrator self-healing — a one-time API blip
	// can't leave the pool in a permanently degraded state.
	//
	// Division of labor: this sweep ONLY covers the Proxmox-driven
	// states (provisioning/booting/draining/destroying). The
	// GitHub-driven states (assigned/running) are owned by the
	// gh.Reconciler, which has the runner-side ground truth needed to
	// distinguish "stuck" from "legitimately waiting on a long job".
	const stuckGrace = 5 * time.Minute
	stuckCutoff := time.Now().Add(-stuckGrace)
	stuckCandidates, err := m.store.ListByState(
		store.StateProvisioning, store.StateBooting,
		store.StateDraining, store.StateDestroying,
	)
	if err == nil {
		for _, s := range stuckCandidates {
			if !s.UpdatedAt.Before(stuckCutoff) {
				continue
			}
			m.log.Warn("sweep: row stuck in transient state; re-queueing for destroy",
				"vmid", s.VMID, "state", s.State, "age", time.Since(s.UpdatedAt))
			// Force-transition to draining (idempotent) and kick destroy.
			// The destroy path is idempotent on the Proxmox side too.
			if _, err := m.store.Update(s.VMID, func(v *store.VM) {
				v.State = store.StateDraining
				v.StateSince = time.Now()
			}); err == nil {
				m.destroyAsync(s.VMID, s.Node)
			}
		}
	}

	// VM-max-age recycle: destroy idle Hot/Warm VMs older than the limit.
	if m.cfg.VMMaxAge > 0 {
		cutoff := time.Now().Add(-m.cfg.VMMaxAge)
		olds, err := m.store.ListByState(store.StateHot, store.StateWarm)
		if err == nil {
			for _, o := range olds {
				if !o.CreatedAt.Before(cutoff) {
					continue
				}
				m.log.Info("recycle: vm exceeded max age", "vmid", o.VMID, "age", time.Since(o.CreatedAt))
				if _, err := m.store.Update(o.VMID, func(v *store.VM) {
					v.State = store.StateDraining
					v.StateSince = time.Now()
				}); err == nil {
					m.destroyAsync(o.VMID, o.Node)
				}
			}
		}
	}
}

// promoteN moves up to n Warm VMs to Booting and kicks Start+WaitReady in
// the background for each. Oldest-Warm-first so warm VMs near max-age
// recycle get used before the recycler reaps them.
func (m *manager) promoteN(_ context.Context, n int) {
	warms, err := m.store.ListByState(store.StateWarm)
	if err != nil {
		m.log.Warn("promote: list warm failed", "err", err)
		return
	}
	sort.Slice(warms, func(i, j int) bool { return warms[i].CreatedAt.Before(warms[j].CreatedAt) })
	if n < len(warms) {
		warms = warms[:n]
	}
	for _, w := range warms {
		// CAS warm -> booting; if lost, skip.
		ok, err := m.store.UpdateState(w.VMID, store.StateWarm, store.StateBooting, func(v *store.VM) {
			v.PoolKind = store.PoolKindHot // promoted to hot budget
		})
		if err != nil || !ok {
			continue
		}
		row := w
		// Bound concurrent boots via bootSem — Acquire inside the
		// goroutine so promoteN (called from reconcileOnce) doesn't
		// stall the whole reconcile pass under a burst.
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			defer func() { m.logRecoveredPanic("promote", row.VMID, recover()) }()
			if err := m.bootSem.Acquire(m.workerCtx, 1); err != nil {
				m.log.Debug("promote: cancelled before sem acquired", "vmid", row.VMID, "err", err)
				// Roll the CAS back so the next pass can try again.
				_, _ = m.store.UpdateState(row.VMID, store.StateBooting, store.StateWarm, func(v *store.VM) {
					v.PoolKind = store.PoolKindWarm
				})
				return
			}
			defer m.bootSem.Release(1)
			m.runBoot(row)
		}()
	}
}

// kickClone dispatches a single async clone operation, bounded by the
// concurrency semaphore. If the semaphore can't be acquired (ctx done
// during shutdown, or burst already saturated) the spawn is dropped —
// the next reconcile pass will retry.
//
// The deferred panic-recover closure captures vmid by reference so a
// panic that fires AFTER allocateVMID succeeded logs the real vmid,
// not the goroutine-entry value of 0.
func (m *manager) kickClone(ctx context.Context, kind store.PoolKind, poweredOn bool) {
	if err := m.cloneSem.Acquire(ctx, 1); err != nil {
		m.log.Debug("clone: dropping spawn (semaphore unavailable)", "kind", kind, "err", err)
		return
	}
	m.wg.Add(1)
	var vmid int
	go func() {
		defer m.wg.Done()
		defer func() { m.logRecoveredPanic("clone", vmid, recover()) }()
		defer m.cloneSem.Release(1)
		m.runClone(kind, poweredOn, &vmid)
	}()
}

// runClone is the body of an async clone goroutine. The caller passes
// a *int that runClone writes the allocated vmid into as soon as
// allocation succeeds — so the surrounding goroutine's panic-recover
// closure can log the real vmid if a panic fires later in the body.
func (m *manager) runClone(kind store.PoolKind, poweredOn bool, vmidRef *int) {
	// Derived from workerCtx so SIGTERM (and drain timeout) propagate
	// into in-flight Proxmox calls. 15-minute deadline caps a single
	// stuck call.
	ctx, cancel := context.WithTimeout(m.workerCtx, 15*time.Minute)
	defer cancel()
	ctx, span := tracer.Start(ctx, "pool.runClone", trace.WithAttributes(
		attribute.String("pool.kind", string(kind)),
		attribute.Bool("powered_on", poweredOn),
	))
	defer span.End()

	hint := nodeselector.Hint{}
	node, err := m.sel.Select(ctx, hint)
	if err != nil {
		m.log.Warn("clone: node selection failed", "err", err)
		return
	}
	if m.cfg.LinkedClones {
		node = m.cfg.TemplateNode
	}

	// Allocate VMID and insert the row under allocMu so concurrent
	// goroutines don't collide on the same id.
	m.allocMu.Lock()
	vmid, err := m.allocateVMID(ctx)
	if err != nil {
		m.allocMu.Unlock()
		m.log.Warn("clone: allocate vmid failed", "err", err)
		return
	}
	// Publish the allocated id to the caller so a panic later in this
	// function logs the real vmid rather than 0.
	if vmidRef != nil {
		*vmidRef = vmid
	}
	name := fmt.Sprintf("%s%d", m.cfg.VMNamePrefix, vmid)
	row := &store.VM{
		VMID:     vmid,
		Node:     node,
		Name:     name,
		PoolKind: kind,
		State:    store.StateProvisioning,
	}
	if err := m.store.Insert(row); err != nil {
		m.allocMu.Unlock()
		m.log.Warn("clone: create row failed", "vmid", vmid, "err", err)
		return
	}
	m.allocMu.Unlock()

	span.SetAttributes(attribute.Int("vm.id", vmid), attribute.String("vm.node", node))
	cloneStart := time.Now()
	pv, err := m.prov.Clone(ctx, provisioner.CloneOptions{
		NewVMID:   vmid,
		Node:      node,
		Name:      name,
		Linked:    m.cfg.LinkedClones,
		PoweredOn: poweredOn,
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "clone failed")
		m.log.Warn("clone: provisioner failed", "vmid", vmid, "err", err)
		if m.metrics != nil {
			m.metrics.VMsTotal.WithLabelValues("clone-failed").Inc()
		}
		// Mark the row destroying and let the destroy path clean up.
		_, _ = m.store.Update(vmid, func(v *store.VM) {
			v.State = store.StateDestroying
			v.StateSince = time.Now()
		})
		m.destroyAsync(vmid, node)
		return
	}
	if m.metrics != nil {
		m.metrics.CloneDuration.WithLabelValues(fmt.Sprintf("%t", m.cfg.LinkedClones), node).Observe(time.Since(cloneStart).Seconds())
		m.metrics.VMsTotal.WithLabelValues("clone-success").Inc()
	}

	// Transition row to warm (if not powered on) or booting (if powered on).
	target := store.StateWarm
	if poweredOn {
		target = store.StateBooting
	}
	updated, err := m.store.Update(vmid, func(v *store.VM) {
		v.State = target
		v.StateSince = time.Now()
	})
	if err != nil {
		m.log.Warn("clone: update row state failed", "vmid", vmid, "err", err)
		return
	}

	if poweredOn {
		m.runBootInline(ctx, pv, updated)
	}
	m.SignalRefill()
}

// runBoot is the body of a warm->hot promotion goroutine.
func (m *manager) runBoot(row *store.VM) {
	ctx, cancel := context.WithTimeout(m.workerCtx, 5*time.Minute)
	defer cancel()
	pv := &provisioner.VM{VMID: row.VMID, Node: row.Node, Name: row.Name}
	if err := m.prov.Start(ctx, pv); err != nil {
		m.log.Warn("boot: start failed", "vmid", row.VMID, "err", err)
		m.markPoisonOrDestroy(row)
		return
	}
	m.runBootInline(ctx, pv, row)
}

// runBootInline waits for the guest agent and transitions Booting → Hot.
// Shared by the clone(poweredOn=true) and warm-promotion paths.
func (m *manager) runBootInline(ctx context.Context, pv *provisioner.VM, row *store.VM) {
	ctx, span := tracer.Start(ctx, "pool.runBoot", trace.WithAttributes(
		attribute.Int("vm.id", row.VMID),
		attribute.String("vm.node", row.Node),
	))
	defer span.End()
	bootStart := time.Now()
	if err := m.prov.WaitReady(ctx, pv, m.cfg.GuestAgentTO); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "wait-ready failed")
		m.log.Warn("boot: wait-ready failed", "vmid", row.VMID, "err", err)
		m.markPoisonOrDestroy(row)
		return
	}
	if m.metrics != nil {
		m.metrics.BootDuration.WithLabelValues(row.Node).Observe(time.Since(bootStart).Seconds())
	}
	if _, err := m.store.Update(row.VMID, func(v *store.VM) {
		v.State = store.StateHot
		v.StateSince = time.Now()
	}); err != nil {
		m.log.Warn("boot: set hot state failed", "vmid", row.VMID, "err", err)
	}
	m.SignalRefill()
}

// markPoisonOrDestroy increments boot_attempts; if past the threshold,
// tags the VM as poison and stops touching it; otherwise schedules
// destruction so the next reconcile can clone a fresh one.
func (m *manager) markPoisonOrDestroy(row *store.VM) {
	updated, err := m.store.Update(row.VMID, func(v *store.VM) {
		v.BootAttempts++
	})
	if err != nil {
		m.log.Warn("poison: inc attempts failed", "vmid", row.VMID, "err", err)
		return
	}
	if updated.BootAttempts >= m.cfg.BootMaxAttempts {
		_, _ = m.store.Update(row.VMID, func(v *store.VM) {
			v.State = store.StatePoison
			v.StateSince = time.Now()
		})
		m.log.Warn("vm marked poison; manual intervention required", "vmid", row.VMID, "attempts", updated.BootAttempts)
		return
	}
	_, _ = m.store.Update(row.VMID, func(v *store.VM) {
		v.State = store.StateDestroying
		v.StateSince = time.Now()
	})
	m.destroyAsync(updated.VMID, updated.Node)
}

// destroyAsync queues a destruction in the background. The Proxmox-side
// call is bounded by the destroy semaphore, but the goroutine queue
// itself is unbounded (acquire happens inside the goroutine) so that
// hot-path callers like MarkCompleted and the runner-hook handler never
// block on the semaphore.
//
// Trade-off: a burst can spawn many goroutines that sit waiting for a
// destroy slot. With max_concurrent_runners on the order of 50-100,
// goroutine overhead is negligible — what matters is the cap on
// concurrent Proxmox API calls, which the sem inside the goroutine
// enforces.
func (m *manager) destroyAsync(vmid int, node string) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer func() { m.logRecoveredPanic("destroyAsync", vmid, recover()) }()
		if err := m.destroySem.Acquire(m.workerCtx, 1); err != nil {
			m.log.Debug("destroy: cancelled before sem acquired", "vmid", vmid, "err", err)
			return
		}
		defer m.destroySem.Release(1)
		m.destroy(m.workerCtx, vmid, node)
	}()
}

// destroy invokes the provisioner to delete the VM and removes the
// in-memory row. If the row carried a runner_id, the orphan-cleanup
// callback is also invoked so the orchestrator can deregister the runner
// on GitHub.
//
// On transient Proxmox failure (network blip, mid-shutdown, etc.) the
// row is LEFT in its current state — the reconcile loop's stuck-state
// sweep will re-queue it on the next pass. We don't retry in-line here
// because we don't want to hold a goroutine + wg.Wait() entry for many
// minutes during graceful drain.
func (m *manager) destroy(ctx context.Context, vmid int, node string) {
	dctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	dctx, span := tracer.Start(dctx, "pool.destroy", trace.WithAttributes(
		attribute.Int("vm.id", vmid),
		attribute.String("vm.node", node),
	))
	defer span.End()

	// Capture the runner_id BEFORE destruction so we can ask the
	// orphan-cleanup hook to deregister it even after the row is gone.
	var runnerID int64
	if row, err := m.store.Get(vmid); err == nil {
		runnerID = row.RunnerID
	}

	if err := m.prov.Destroy(dctx, &provisioner.VM{VMID: vmid, Node: node}); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "destroy failed")
		m.log.Warn("destroy: provisioner failed", "vmid", vmid, "err", err)
		return
	}
	if err := m.store.Delete(vmid); err != nil {
		m.log.Warn("destroy: delete row failed", "vmid", vmid, "err", err)
	}
	if m.metrics != nil {
		m.metrics.VMsTotal.WithLabelValues("destroyed").Inc()
	}

	if runnerID != 0 && m.cfg.OnRunnerOrphaned != nil {
		if err := m.cfg.OnRunnerOrphaned(dctx, runnerID); err != nil {
			m.log.Warn("destroy: orphan-runner cleanup failed", "vmid", vmid, "runner_id", runnerID, "err", err)
		} else {
			m.log.Debug("destroy: deregistered github runner", "vmid", vmid, "runner_id", runnerID)
		}
	}
}

// allocateVMID returns the lowest VMID in the configured range that is not
// already claimed by an existing row.
func (m *manager) allocateVMID(_ context.Context) (int, error) {
	used, err := m.store.UsedVMIDs(m.cfg.VMIDRange.Min, m.cfg.VMIDRange.Max)
	if err != nil {
		return 0, err
	}
	for id := m.cfg.VMIDRange.Min; id <= m.cfg.VMIDRange.Max; id++ {
		if _, taken := used[id]; !taken {
			return id, nil
		}
	}
	return 0, errors.New("vmid range exhausted")
}

// drain is invoked when Run's ctx is cancelled. It waits for in-flight
// operations to finish up to DrainTimeout. On timeout, the worker
// context is force-cancelled so in-flight Proxmox calls observe
// ctx.Done and unwind — without this, a single 5-minute destroy under a
// hung Proxmox node could pin the process well past DrainTimeout.
//
// In all exit paths workerCancel is called so no goroutine outlives Run
// in a "phantom alive" state. drain blocks until either the wg drains
// or a post-cancel grace period elapses.
func (m *manager) drain() {
	m.log.Info("pool: draining")
	// Always cancel the worker context on the way out, so any goroutine
	// that spawns racing with drain (e.g. a late-arriving destroyAsync
	// from a state-sweep that ran on the last reconcile tick) doesn't
	// outlive us.
	defer m.workerCancel()

	timeout := m.cfg.DrainTimeout
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		m.log.Info("pool: drain completed cleanly")
		return
	case <-time.After(timeout):
		m.log.Warn("pool: drain timed out; cancelling in-flight Proxmox calls", "timeout", timeout)
	}
	// Escalate: cancel workers so they see ctx.Done. The defer will
	// fire too, but cancelling here is idempotent and the order matters
	// (we want workers to observe cancellation BEFORE we re-wait).
	m.workerCancel()
	const postCancelGrace = 10 * time.Second
	select {
	case <-done:
		m.log.Info("pool: drain completed after worker cancellation")
	case <-time.After(postCancelGrace):
		m.log.Error("pool: workers did not unwind after cancellation; abandoning",
			"grace", postCancelGrace)
	}
}
