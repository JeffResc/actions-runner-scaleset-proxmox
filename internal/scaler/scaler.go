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
	"sort"
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
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/priority"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/provisioner"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/quotas"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/router"
)

// Compile-time assertion that *Scaler satisfies the listener.Scaler contract.
var _ listener.Scaler = (*Scaler)(nil)

// Config bundles the static information the Scaler needs.
type Config struct {
	ScaleSetID int
	// ScaleSetName is the human-readable scaleset identifier
	// recorded as the `scaleset` label on every metric this
	// scaler emits (issue #1). Required.
	ScaleSetName string
	WorkFolder   string // "_work" by default
	NamePrefix   string // matches the pool's VM name prefix
}

// Scaler implements scaleset.Scaler against the orchestrator's pool.
type Scaler struct {
	cfg     Config
	gh      *scaleset.Client
	pool    pool.Manager
	prov    provisioner.Provisioner
	log     *slog.Logger
	metrics *observability.Metrics

	// router maps a job's RequestLabels to the profile the scaler
	// would ideally serve it from. Nil disables routing entirely —
	// the scaler then records every JobStarted as "routed to default
	// profile" (single-profile back-compat).
	router *router.Router

	// quotas resolves the effective per-(org|repo) concurrency cap
	// for a given job. When the resolver is non-nil and Enabled,
	// HandleJobStarted compares the matching bucket's existing
	// count against the cap and emits scaleset_quota_throttled_total
	// when the cap is breached. Per the PR's premise note this is
	// observational; at-acquire-time enforcement is deferred until
	// the listener integration exposes per-job pre-assignment
	// metadata.
	quotas *quotas.Resolver

	// priority classifies jobs into operator-declared priority
	// classes for the scaleset_priority_acquires_total counter.
	// Nil = every job lands in priority.ZeroClass.
	priority *priority.Matcher

	// quotaCounter looks up the current per-org / per-repo VM
	// count for recordQuota. Decoupled from pool.Manager so unit
	// tests can stub it without faking the entire interface.
	// Production wires the store-backed implementation in
	// app.Run.
	quotaCounter QuotaCounter

	// provisionOneFn is the per-VM mint+inject worker invoked from
	// HandleDesiredRunnerCount. Defaults to the production
	// provisionOne; tests override it to isolate the acquire/clamp
	// logic from the GitHub + Proxmox call paths.
	provisionOneFn func(ctx context.Context, vmObj *pool.VM) bool

	// injectRetry overrides the inject backoff policy. The zero value
	// means "use defaultInjectRetry"; tests fill this in to shorten
	// the schedule (e.g. assert MaxElapsed actually bounds runtime).
	injectRetry injectRetryConfig
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

// SetRouter attaches a label router. Pass nil to disable routing
// observations. Called once at construction by the caller (app.Run)
// after both the scaler and the router have been built.
func (s *Scaler) SetRouter(r *router.Router) { s.router = r }

// SetQuotas attaches a quotas resolver. Pass nil (or an empty
// config's resolver) to disable quota observations.
func (s *Scaler) SetQuotas(q *quotas.Resolver) { s.quotas = q }

// SetPriority attaches a priority matcher. Pass nil to treat every
// job as priority.ZeroClass.
func (s *Scaler) SetPriority(p *priority.Matcher) { s.priority = p }

// HandleJobStarted is called when GitHub assigns a queued job to one of our
// JIT runners. We transition the matching VM row Assigned -> Running and,
// when a label router is configured, record the routing decision for the
// job's RequestLabels. Routing here is observational — by the time
// JobStarted fires GitHub has already paired the job with a specific VM,
// so we can't redirect; what we CAN do is alert operators via the
// unrouted-jobs metric when a job arrives whose labels no profile
// satisfies (a config gap they need to fix). The quotas + priority
// machinery (issues #4 + #10) follows the same observational
// pattern for the same reason — per-job pre-assignment metadata
// requires a deeper listener integration, deferred to a future PR.
func (s *Scaler) HandleJobStarted(ctx context.Context, info *scaleset.JobStarted) error {
	if s.metrics != nil {
		s.metrics.ListenerMessages.WithLabelValues(s.cfg.ScaleSetName, "job_started").Inc()
	}
	s.recordRouting(info.RequestLabels)
	vmid, ok := vmidFromRunnerName(info.RunnerName, s.cfg.NamePrefix)
	if !ok {
		s.log.Warn("job_started: cannot derive vmid from runner name", "runner_name", info.RunnerName)
		return nil
	}
	// Stamp the row with per-job metadata + record priority class
	// + observe quotas BEFORE transitioning to Running. The metric
	// emissions are cheap; the StampJobMetadata write happens in
	// the same goroutine as MarkRunning so the row is fully
	// described before any other consumer (admin /state, GH
	// reconciler ListRows) can observe it.
	org, repo, class := s.classifyJob(info)
	if err := s.pool.StampJobMetadata(ctx, vmid, pool.JobMetadata{
		Org: org, Repo: repo, PriorityClass: class.Name,
	}); err != nil {
		// Non-fatal: stamping is observability scaffolding. Log
		// and move on so the actual state transition still
		// happens.
		s.log.Warn("job_started: stamp metadata failed", "vmid", vmid, "err", err)
	}
	s.recordPriority(class)
	s.recordQuota(ctx, org, repo)
	return s.pool.MarkRunning(ctx, vmid, int64(info.RunnerID))
}

// classifyJob projects the JobStarted message into the dimensions
// the quotas + priority machinery consults. Repo is joined into
// owner/repo form to align with the lookup keys.
func (s *Scaler) classifyJob(info *scaleset.JobStarted) (org, repo string, class priority.Class) {
	org = info.OwnerName
	if info.OwnerName != "" && info.RepositoryName != "" {
		repo = info.OwnerName + "/" + info.RepositoryName
	}
	class = s.priority.Classify(priority.JobInfo{
		Org:            org,
		Repo:           info.RepositoryName,
		WorkflowLabels: info.RequestLabels,
	})
	return org, repo, class
}

// recordPriority bumps scaleset_priority_acquires_total{class} for
// the class the job was paired into. A nil matcher resolves to
// priority.ZeroClass, so the counter is always populated under
// "default" — operators get a baseline series even without
// priority config.
func (s *Scaler) recordPriority(class priority.Class) {
	if s.metrics == nil {
		return
	}
	s.metrics.PriorityAcquires.WithLabelValues(s.cfg.ScaleSetName, class.Name).Inc()
}

// recordQuota looks up the effective per-(org|repo) cap for the
// job, counts the bucket's existing stamped VMs, and bumps
// scaleset_quota_throttled_total when the cap is exceeded. The
// observation is post-facto (GitHub already paired the job with a
// VM) — the metric tells operators which quota is being violated
// and by how much, but the orchestrator does not refuse the
// already-assigned job here.
func (s *Scaler) recordQuota(_ context.Context, org, repo string) {
	if s.quotas == nil || !s.quotas.Enabled() {
		return
	}
	res := s.quotas.Resolve(org, repo)
	if res.Scope == quotas.ScopeNone || res.Cap == 0 {
		return
	}
	// Counter logic intentionally lives in QuotaCount; defer the
	// actual store query so unit tests can stub it without paying
	// the cost of a fake store. In the production path it queries
	// the manager-backed store via the scaler's hook below.
	count, err := s.quotaCount(res.Scope, res.Name)
	if err != nil {
		s.log.Warn("quota: count lookup failed", "scope", res.Scope, "name", res.Name, "err", err)
		return
	}
	if count > res.Cap {
		s.log.Warn("quota: bucket over cap",
			"scope", res.Scope, "name", res.Name, "count", count, "cap", res.Cap)
		if s.metrics != nil {
			s.metrics.QuotaThrottled.WithLabelValues(s.cfg.ScaleSetName, string(res.Scope), res.Name).Inc()
		}
	}
}

// QuotaCounter is the abstraction the scaler uses to look up the
// current per-org / per-repo VM count. Production wires the store-
// backed implementation; tests can plug in a stub without faking
// the entire pool.Manager surface.
type QuotaCounter interface {
	CountByOrg(org string) (int, error)
	CountByRepo(repo string) (int, error)
}

// SetQuotaCounter attaches the per-bucket count source. Nil
// disables the lookup (recordQuota then short-circuits, which is
// safe even with a non-nil resolver — the metric just stays at 0).
func (s *Scaler) SetQuotaCounter(c QuotaCounter) { s.quotaCounter = c }

// quotaCount dispatches to the configured QuotaCounter. Returns 0
// when nothing is wired so the caller's threshold comparison
// always trivially passes (we don't want missing wiring to spam
// false throttled events).
func (s *Scaler) quotaCount(scope quotas.Scope, name string) (int, error) {
	if s.quotaCounter == nil {
		return 0, nil
	}
	switch scope {
	case quotas.ScopeRepo:
		return s.quotaCounter.CountByRepo(name)
	case quotas.ScopeOrg:
		return s.quotaCounter.CountByOrg(name)
	case quotas.ScopeNone:
		// Caller already short-circuited on ScopeNone; defensive
		// no-op so the exhaustive lint stays happy if recordQuota
		// ever forgets the guard.
	}
	return 0, nil
}

// recordRouting consults the router (when configured) and either logs
// the resolved profile or emits the unrouted-jobs counter. Cheap
// enough to run on every JobStarted; no I/O.
func (s *Scaler) recordRouting(jobLabels []string) {
	if s.router == nil {
		return
	}
	profile, err := s.router.Route(jobLabels)
	if err != nil {
		s.log.Warn("router: no profile satisfies job labels", "labels", jobLabels, "err", err)
		if s.metrics != nil {
			s.metrics.UnroutedJobs.WithLabelValues(s.cfg.ScaleSetName, joinLabelsForMetric(jobLabels)).Inc()
		}
		return
	}
	s.log.Debug("router: job routed", "profile", profile, "labels", jobLabels)
}

// joinLabelsForMetric renders a job's RequestLabels into a single
// stable string suitable as a Prometheus label value. Sort + join so
// the same logical label set always hashes to the same series — and
// so the cardinality is bounded by distinct label sets, not by
// arrival order.
func joinLabelsForMetric(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	cp := make([]string, len(labels))
	copy(cp, labels)
	sort.Strings(cp)
	return strings.Join(cp, "|")
}

// HandleJobCompleted is called when a job finishes. We destroy the VM.
func (s *Scaler) HandleJobCompleted(ctx context.Context, info *scaleset.JobCompleted) error {
	if s.metrics != nil {
		s.metrics.ListenerMessages.WithLabelValues(s.cfg.ScaleSetName, "job_completed").Inc()
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
		s.metrics.ListenerMessages.WithLabelValues(s.cfg.ScaleSetName, "desired_count").Inc()
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
			s.metrics.GitHubErrors.WithLabelValues(s.cfg.ScaleSetName, "generate_jit").Inc()
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
		s.metrics.RecordProxmoxError(s.cfg.ScaleSetName, "inject_jit", vmObj.Node)
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
			s.metrics.GitHubErrors.WithLabelValues(s.cfg.ScaleSetName, "get_runner_by_name").Inc()
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
			s.metrics.GitHubErrors.WithLabelValues(s.cfg.ScaleSetName, "remove_stale_runner").Inc()
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

// injectRetryConfig is the tunable retry policy for [Scaler.injectWithRetry].
// Hoisting it to a struct keeps the retry-budget concerns out of the
// inject body and gives tests a knob to dial the policy without rewriting
// the call site.
type injectRetryConfig struct {
	InitialInterval     time.Duration
	MaxInterval         time.Duration
	Multiplier          float64
	RandomizationFactor float64
	MaxAttempts         uint
	MaxElapsed          time.Duration
}

// build returns a fresh ExponentialBackOff configured from c. Each
// injectWithRetry call needs its own backoff instance (it carries
// internal state); the helper just stamps the policy onto one.
func (c injectRetryConfig) build() *backoff.ExponentialBackOff {
	eb := backoff.NewExponentialBackOff()
	eb.InitialInterval = c.InitialInterval
	eb.MaxInterval = c.MaxInterval
	eb.Multiplier = c.Multiplier
	eb.RandomizationFactor = c.RandomizationFactor
	return eb
}

// defaultInjectRetry is the policy applied unless a test overrides
// s.injectRetry. Bound by both attempts and wall-clock so a stuck VM
// can't pin the scaler past the listener's response deadline.
var defaultInjectRetry = injectRetryConfig{
	InitialInterval:     2 * time.Second,
	MaxInterval:         10 * time.Second,
	Multiplier:          2.0,
	RandomizationFactor: 0, // deterministic; matches the prior fixed-step schedule
	MaxAttempts:         6,
	MaxElapsed:          60 * time.Second,
}

// injectWithRetry calls InjectJITConfig with a longer retry budget than
// the underlying HTTP transport for the specific "VM is not running"
// transient error. This error is misleading — Proxmox returns it when
// the qemu-guest-agent socket is briefly unresponsive (e.g., when
// in-VM firstboot scripts churn systemd). The VM is usually fine
// within 10-30s; we retry the inject so an unlucky timing window
// doesn't burn a VM.
//
// Retry policy comes from s.injectRetry when non-zero, otherwise
// defaultInjectRetry.
func (s *Scaler) injectWithRetry(ctx context.Context, vm *provisioner.VM, jit string) error {
	cfg := s.injectRetry
	if cfg == (injectRetryConfig{}) {
		cfg = defaultInjectRetry
	}
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
		backoff.WithBackOff(cfg.build()),
		backoff.WithMaxTries(cfg.MaxAttempts),
		backoff.WithMaxElapsedTime(cfg.MaxElapsed),
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
