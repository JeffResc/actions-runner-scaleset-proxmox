// Package gh implements the GitHub-side reconciler: a polling loop that
// joins the GitHub Runners API against our DB and drives lifecycle
// transitions to converge the two. It is the backstop for missed
// scaleset-listener messages (the production failure mode that motivates
// this package: VMs stuck in `assigned` because JobStarted/JobCompleted
// never fired or arrived with empty fields).
//
// The reconciler is intentionally separate from the pool's own reconcile
// loop. The pool decides how many VMs SHOULD exist; this package decides
// whether each existing VM is doing what we think it is.
package gh

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/google/go-github/v88/github"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/githubauth"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/observability"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/pool"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/provisioner"
)

var tracer = otel.Tracer(observability.TracerName)

// Config configures the reconciler.
type Config struct {
	// Scope identifies the GitHub registration target (mirrors
	// scaleset's Scope; required so we know which runners-list endpoint
	// to hit — repo vs org).
	Scope githubauth.Scope

	// PollInterval is how often a non-failing tick fires.
	PollInterval time.Duration

	// FailureBackoffMax caps the backoff when consecutive ticks fail
	// (GitHub 5xx, network errors). The backoff starts at PollInterval
	// and doubles on each failure up to this ceiling.
	FailureBackoffMax time.Duration

	// AssignedGrace is the minimum time a row may stay in `assigned`
	// before the reconciler considers force-destroying it. Tuned to
	// "JIT inject + runner connect + GitHub assignment" worst case.
	AssignedGrace time.Duration

	// RunningIdleGrace is the minimum time a row stays in `running`
	// (with the runner observed as online + idle on GitHub) before
	// destruction. Catches missed JobCompleted callbacks.
	RunningIdleGrace time.Duration

	// AssignedOfflineGrace is the minimum time for an `assigned` row
	// whose runner is observed offline before destruction. Shorter than
	// AssignedGrace because an offline runner is a stronger signal of
	// failure than an idle one.
	AssignedOfflineGrace time.Duration

	// OrphanGrace is how long a Proxmox VM may exist without a matching
	// store row before sweepProxmoxOrphans destroys it. Must exceed the
	// typical Clone → guest-agent-ready → JIT-inject worst case;
	// otherwise the reconciler will destroy VMs the pool worker is
	// still booting. Wired from config's pool.orphan_grace.
	OrphanGrace time.Duration

	// RunnerNamePrefix is the prefix our scaleset stamps on every
	// runner name (e.g. "gh-runner-proxmox-ubuntu-x64-"). Runners NOT
	// matching this prefix are ignored.
	RunnerNamePrefix string

	// ScaleSetName is the human-readable identifier recorded as
	// the `scaleset` label on every metric this reconciler emits
	// (issue #1). Required.
	ScaleSetName string
}

// Validate returns nil iff the config is internally consistent.
func (c Config) Validate() error {
	if err := c.Scope.Validate(); err != nil {
		return err
	}
	if c.PollInterval <= 0 {
		return errors.New("gh: poll_interval must be > 0")
	}
	if c.AssignedGrace <= 0 {
		return errors.New("gh: assigned_grace must be > 0")
	}
	if c.RunningIdleGrace <= 0 {
		return errors.New("gh: running_idle_grace must be > 0")
	}
	if c.OrphanGrace <= 0 {
		return errors.New("gh: orphan_grace must be > 0")
	}
	if c.RunnerNamePrefix == "" {
		return errors.New("gh: runner_name_prefix must be set")
	}
	return nil
}

// Reconciler is the polling loop + state matrix.
type Reconciler struct {
	cfg     Config
	gh      *github.Client
	pool    pool.Manager
	prov    provisioner.Provisioner
	log     *slog.Logger
	metrics *observability.Metrics

	// orphanFirstSeen tracks when each currently-orphaned runner name
	// was first observed without a matching DB row. We only remove a
	// runner once it's been an orphan for >= orphanGrace, so a runner
	// that registered on GitHub microseconds before the orchestrator
	// wrote its store row doesn't get reaped. Entries are pruned
	// whenever the runner is matched to a row.
	//
	// Tick is single-threaded (called from Run), so no mutex needed.
	orphanFirstSeen map[string]time.Time

	// orphanProxmoxFirstSeen mirrors orphanFirstSeen but for Proxmox
	// VMs that the orchestrator sees in PVE without a matching store
	// row. Without this grace, sweepProxmoxOrphans destroys VMs the
	// pool worker has just cloned but hasn't yet booted+inserted —
	// producing the "Configuration file does not exist" JIT-inject
	// errors. Keys are VMIDs; entries are pruned when the VM
	// reappears in the store rows snapshot.
	orphanProxmoxFirstSeen map[int]time.Time

	now func() time.Time // injected for tests
}

// New builds a Reconciler. The github.Client must already be authenticated
// (built via githubauth.Auth.NewRESTClient).
func New(cfg Config, gh *github.Client, p pool.Manager, prov provisioner.Provisioner, log *slog.Logger, metrics *observability.Metrics) (*Reconciler, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if gh == nil {
		return nil, errors.New("gh: nil github client")
	}
	if p == nil {
		return nil, errors.New("gh: nil pool manager")
	}
	if prov == nil {
		return nil, errors.New("gh: nil provisioner")
	}
	if log == nil {
		log = slog.Default()
	}
	if cfg.FailureBackoffMax == 0 {
		cfg.FailureBackoffMax = 5 * time.Minute
	}
	if cfg.AssignedOfflineGrace == 0 {
		cfg.AssignedOfflineGrace = 2 * time.Minute
	}
	return &Reconciler{
		cfg:                    cfg,
		gh:                     gh,
		pool:                   p,
		prov:                   prov,
		log:                    log,
		metrics:                metrics,
		orphanFirstSeen:        make(map[string]time.Time),
		orphanProxmoxFirstSeen: make(map[int]time.Time),
		now:                    time.Now,
	}, nil
}

// orphanGrace is how long a runner must have been observed unmatched
// against the DB before cleanup destroys its GitHub registration. Set
// just above one PollInterval so a fresh runner that registered mid-tick
// (before its DB row landed) gets a second tick to be matched.
const orphanGrace = 30 * time.Second

// cleanupTimeoutPerRunner caps an individual GitHub Actions
// RemoveRunner call. A GitHub-side outage previously stalled the
// reconcile tick for the full http.Client timeout (~60s) per orphan
// candidate, multiplied across the orphan set. With a per-call
// timeout the slow runner is logged + skipped and the tick keeps
// moving — the next tick retries.
//
// var (not const) so the regression test can dial it down to a few
// hundred milliseconds without holding CI hostage for 10 seconds.
var cleanupTimeoutPerRunner = 10 * time.Second

// Run drives ticks until ctx is cancelled. Returns ctx.Err() on shutdown.
//
// On consecutive tick failures the next-tick delay doubles up to
// FailureBackoffMax, then collapses back to PollInterval on the first
// successful tick. This keeps API budget intact during GitHub outages
// without abandoning recovery once the API comes back.
//
// backoff.ExponentialBackOff owns the doubling + cap; we call its
// NextBackOff/Reset around the infinite-poll loop. backoff.Retry isn't
// a fit because Run never "succeeds and returns" — it runs forever
// until ctx is cancelled.
func (r *Reconciler) Run(ctx context.Context) error {
	eb := backoff.NewExponentialBackOff()
	eb.InitialInterval = r.cfg.PollInterval
	eb.MaxInterval = r.cfg.FailureBackoffMax
	eb.Multiplier = 2.0
	eb.RandomizationFactor = 0 // deterministic to keep operator-facing log timings predictable
	eb.Reset()
	delay := r.cfg.PollInterval
	for {
		// time.NewTimer (not time.After) so the timer is reclaimed
		// immediately on ctx cancellation. With time.After a cancellation
		// at the start of a 5-minute backoff leaves a runtime timer alive
		// for the full duration — harmless but noisy under SIGTERM at
		// scale.
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if err := r.Tick(ctx); err != nil {
			delay = eb.NextBackOff()
			r.log.Warn("reconciler tick failed; backing off", "err", err, "next_in", delay)
			continue
		}
		eb.Reset()
		delay = r.cfg.PollInterval
	}
}

// Tick performs a single reconcile pass. Exported so tests can drive it
// deterministically without spinning the time-based Run loop.
func (r *Reconciler) Tick(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "gh.Reconciler.Tick")
	defer span.End()

	runners, err := r.listOurRunners(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "list runners failed")
		return fmt.Errorf("list runners: %w", err)
	}

	rows, err := r.pool.ListRows(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "list rows failed")
		return fmt.Errorf("list rows: %w", err)
	}
	span.SetAttributes(
		attribute.Int("gh.runners", len(runners)),
		attribute.Int("db.rows", len(rows)),
	)

	r.applyMatrix(ctx, rows, runners)
	r.cleanupOrphanRunners(ctx, rows, runners)

	if r.prov != nil {
		r.sweepProxmoxOrphans(ctx, rows)
	}
	return nil
}

// listOurRunners returns every runner on the scope whose name matches
// our prefix, keyed by runner name. Delegates to ListRunnersByPrefix
// so the leader's one-shot Adopt pass and this reconcile loop share a
// single implementation (pagination cap, prefix filter, scope dispatch).
func (r *Reconciler) listOurRunners(ctx context.Context) (map[string]pool.RunnerInfo, error) {
	return ListRunnersByPrefix(ctx, r.gh, r.cfg.Scope, r.cfg.RunnerNamePrefix, r.log)
}

// ListRunnersByPrefix returns every runner on the scope whose name
// matches the given prefix, keyed by runner name (which is also the VM
// name produced by pool.Manager).
//
// Pagination is capped at maxListPages (50 × 100 = 5000 runners) so a
// scope that's accumulated a stale runner backlog (e.g. an operator
// resetting the orchestrator several times) can't blow our API budget
// or memory in one call. Anything past the cap is processed on the
// next tick — eventual consistency is fine because the reconciler
// itself is a backstop, not a primary signal.
//
// Exported so pool.Manager.Adopt can call it without owning a
// *Reconciler — its result is wrapped by NewRunnerLister into a
// pool.RunnerLister suitable for pool.Config.RunnerLister.
func ListRunnersByPrefix(ctx context.Context, gh *github.Client, scope githubauth.Scope, prefix string, log *slog.Logger) (map[string]pool.RunnerInfo, error) {
	const maxListPages = 50
	out := make(map[string]pool.RunnerInfo)
	opt := &github.ListRunnersOptions{ListOptions: github.ListOptions{PerPage: 100}}
	for page := 0; page < maxListPages; page++ {
		runnersPage, resp, err := listRunnersPageWithRetry(ctx, gh, scope, opt, log)
		if err != nil {
			return nil, err
		}
		for _, gr := range runnersPage.Runners {
			name := gr.GetName()
			if !strings.HasPrefix(name, prefix) {
				continue
			}
			out[name] = pool.RunnerInfo{
				ID:     gr.GetID(),
				Online: gr.GetStatus() == "online",
				Busy:   gr.GetBusy(),
			}
		}
		if resp == nil || resp.NextPage == 0 {
			return out, nil
		}
		opt.Page = resp.NextPage
	}
	if log != nil {
		log.Warn("ListRunnersByPrefix: hit max-pages cap; deferring rest to next call",
			"max_pages", maxListPages, "matched", len(out))
	}
	return out, nil
}

// listRunnersPage fetches one page of runners from the API endpoint
// that matches scope. Centralising the org-vs-repo dispatch here means
// ListRunnersByPrefix's pagination loop carries no branching on scope
// per iteration; a future enterprise endpoint becomes a one-line
// addition in this function.
func listRunnersPage(ctx context.Context, gh *github.Client, scope githubauth.Scope, opt *github.ListRunnersOptions) (*github.Runners, *github.Response, error) {
	if scope.Org != "" {
		return gh.Actions.ListOrganizationRunners(ctx, scope.Org, opt)
	}
	owner, repo := splitRepo(scope.Repo)
	return gh.Actions.ListRunners(ctx, owner, repo, opt)
}

// listPageRetryConfig sets a small per-page retry budget. With 50
// max pages a tick can issue up to 50 calls; capping retries at 3
// keeps the worst-case API budget bounded at 150 calls even if
// every page transiently fails, which is well under GitHub's
// per-installation rate limit.
const (
	listPageMaxTries        = 3
	listPageInitialInterval = 200 * time.Millisecond
	listPageMaxInterval     = 2 * time.Second
)

// listRunnersPageWithRetry wraps listRunnersPage with bounded
// retry/backoff. GitHub's runner-list endpoint returns 502/503 and
// network errors with non-trivial frequency under deploys + rate-
// limit pressure; before this wrapper a single page transient
// failed the whole pagination run, discarding partial progress and
// re-issuing every prior page on the next tick. With 50 dependent
// pages, the per-page transient compounds quickly.
//
// Retry policy: 3 attempts per page (so worst case 150 API calls
// per tick rather than 50), 200ms initial backoff capped at 2s.
// Treats 4xx-other-than-429 as Permanent so a missing scope /
// revoked token fails fast — only 429, 5xx, and network errors are
// retried. ctx cancellation propagates through to backoff.Retry.
func listRunnersPageWithRetry(ctx context.Context, gh *github.Client, scope githubauth.Scope, opt *github.ListRunnersOptions, log *slog.Logger) (*github.Runners, *github.Response, error) {
	eb := backoff.NewExponentialBackOff()
	eb.InitialInterval = listPageInitialInterval
	eb.MaxInterval = listPageMaxInterval
	eb.Multiplier = 2.0
	eb.RandomizationFactor = 0.1

	type pageResult struct {
		runners *github.Runners
		resp    *github.Response
	}
	var attempts int
	res, err := backoff.Retry(ctx, func() (pageResult, error) {
		attempts++
		runners, resp, err := listRunnersPage(ctx, gh, scope, opt)
		if err == nil {
			return pageResult{runners: runners, resp: resp}, nil
		}
		if !isTransientListError(err, resp) {
			return pageResult{}, backoff.Permanent(err)
		}
		return pageResult{}, err
	},
		backoff.WithBackOff(eb),
		backoff.WithMaxTries(listPageMaxTries),
		backoff.WithNotify(func(err error, d time.Duration) {
			if log != nil {
				log.Warn("ListRunners page transient failure; retrying",
					"attempt", attempts, "backoff", d, "err", err)
			}
		}),
	)
	if err != nil {
		return nil, nil, err
	}
	return res.runners, res.resp, nil
}

// isTransientListError classifies a list-runners failure for the
// retry wrapper. 429 (rate-limited), 5xx, and any error without an
// HTTP response (network failure, ctx-deadline before the request)
// are transient and worth retrying. Any other 4xx (401, 403, 404)
// is permanent — retrying won't help and would burn budget.
func isTransientListError(err error, resp *github.Response) bool {
	if err == nil {
		return false
	}
	// Network errors or other failures with no response — retry.
	if resp == nil || resp.Response == nil {
		return true
	}
	status := resp.StatusCode
	if status == http.StatusTooManyRequests {
		return true
	}
	if status >= 500 && status <= 599 {
		return true
	}
	return false
}

// NewRunnerLister returns a pool.RunnerLister that pages the GitHub
// runners API for the given scope and prefix. Adopt uses it once at
// leader startup; the reconciler's polling loop builds its own snapshot
// independently via listOurRunners.
func NewRunnerLister(gh *github.Client, scope githubauth.Scope, prefix string, log *slog.Logger) pool.RunnerLister {
	return func(ctx context.Context) (map[string]pool.RunnerInfo, error) {
		return ListRunnersByPrefix(ctx, gh, scope, prefix, log)
	}
}

// transitionOp is the dispatcher's per-cell action.
type transitionOp int

const (
	opNoop transitionOp = iota
	opPromote
	opDestroy
)

// transitionKey indexes [stateTransitionTable]. ghLabel is the
// low-cardinality string ghStateLabel produces (busy / idle / offline
// / missing).
type transitionKey struct {
	dbState string
	ghLabel string
}

// transitionAction captures everything the dispatcher needs to fire
// for one (dbState, ghLabel) cell.
//
// grace returns the row-age threshold the action waits on; nil means
// "no time gate". Reading it through Config means a per-cell decision
// lives next to the rest of the cell definition.
//
// promoteMsg and promoteWarn shape the log line emitted on promote —
// preserved verbatim from the prior reconcile{Assigned,Hot} so log
// consumers see no churn.
type transitionAction struct {
	op           transitionOp
	destroyMsg   string
	mismatchKind string
	promoteMsg   string
	promoteWarn  bool
	grace        func(Config) time.Duration
}

// stateTransitionTable is the authoritative (dbState, ghLabel) → action
// map. Adding a new state or ghLabel is a one-line addition here; the
// dispatcher walks rows without per-state branches.
//
// Every cell is enumerated explicitly (no "any" sentinel) so a missing
// combination is grep-visible rather than a silent fall-through.
var stateTransitionTable = map[transitionKey]transitionAction{
	// assigned: JIT injected, waiting for the runner to pick up work.
	{dbState: "assigned", ghLabel: "busy"}: {
		op:           opPromote,
		promoteMsg:   "reconcile: promoting assigned->running (missed JobStarted)",
		mismatchKind: "promote_running",
	},
	{dbState: "assigned", ghLabel: "offline"}: {
		op:         opDestroy,
		destroyMsg: "assigned: runner registered then went offline",
		grace:      func(c Config) time.Duration { return c.AssignedOfflineGrace },
	},
	{dbState: "assigned", ghLabel: "idle"}: {
		op:         opDestroy,
		destroyMsg: "assigned: runner online but never picked up a job",
		grace:      func(c Config) time.Duration { return c.AssignedGrace },
	},
	{dbState: "assigned", ghLabel: "missing"}: {
		op:         opDestroy,
		destroyMsg: "assigned: runner never registered on GitHub",
		grace:      func(c Config) time.Duration { return c.AssignedGrace },
	},

	// running: a job is in flight.
	{dbState: "running", ghLabel: "busy"}: {op: opNoop},
	{dbState: "running", ghLabel: "idle"}: {
		op:         opDestroy,
		destroyMsg: "running: runner went idle (missed JobCompleted)",
		grace:      func(c Config) time.Duration { return c.RunningIdleGrace },
	},
	{dbState: "running", ghLabel: "offline"}: {
		op:         opDestroy,
		destroyMsg: "running: runner went offline",
	},
	{dbState: "running", ghLabel: "missing"}: {
		op:         opDestroy,
		destroyMsg: "running: runner missing from GitHub",
	},

	// hot: pre-JIT pool. Only the busy-without-promote case is
	// actionable; missing/offline/idle are the expected pre-handshake
	// states and the pool's own age-based recycle handles a stalled
	// hot row.
	{dbState: "hot", ghLabel: "busy"}: {
		op:           opPromote,
		promoteMsg:   "reconcile: hot row observed as busy on GitHub; promoting",
		promoteWarn:  true,
		mismatchKind: "promote_running",
	},
	{dbState: "hot", ghLabel: "idle"}:    {op: opNoop},
	{dbState: "hot", ghLabel: "offline"}: {op: opNoop},
	{dbState: "hot", ghLabel: "missing"}: {op: opNoop},
}

// applyMatrix walks each row, looks up its (dbState, ghLabel) cell in
// [stateTransitionTable], and fires the cell's action. The transition
// table is the documentation for the reconciler's behaviour.
func (r *Reconciler) applyMatrix(ctx context.Context, rows []pool.RowSnapshot, runners map[string]pool.RunnerInfo) {
	now := time.Now()
	for _, row := range rows {
		gr, present := runners[row.Name]
		ghLabel := ghStateLabel(gr, present)
		age := now.Sub(row.StateSince)

		action, ok := stateTransitionTable[transitionKey{dbState: row.State, ghLabel: ghLabel}]
		if !ok || action.op == opNoop {
			continue
		}
		if action.grace != nil && age < action.grace(r.cfg) {
			continue
		}
		switch action.op { //nolint:exhaustive // opNoop is short-circuited above
		case opPromote:
			if action.promoteWarn {
				r.log.Warn(action.promoteMsg, "vmid", row.VMID, "runner_id", gr.ID)
			} else {
				r.log.Info(action.promoteMsg, "vmid", row.VMID, "runner_id", gr.ID)
			}
			r.promoteToRunning(ctx, row, gr.ID)
			if action.mismatchKind != "" {
				r.recordMismatch(row.State, ghLabel, action.mismatchKind)
			}
		case opDestroy:
			r.forceDestroy(ctx, row.VMID, action.destroyMsg, row.State, ghLabel)
		}
	}
}

// promoteToRunning is the shared error-handling wrapper for
// pool.PromoteToRunning. A silent error here means a persistently stuck
// row that the reconciler will keep "promoting" every tick with no
// effect — so we surface it via warn-level log AND a metric (which is
// suitable for on-call alerting) instead of discarding the error.
func (r *Reconciler) promoteToRunning(ctx context.Context, row pool.RowSnapshot, runnerID int64) {
	if err := r.pool.PromoteToRunning(ctx, row.VMID, runnerID, row.JobID); err != nil {
		r.log.Warn("reconcile: promote to running failed",
			"vmid", row.VMID, "runner_id", runnerID, "db_state", row.State, "err", err)
		if r.metrics != nil {
			r.metrics.ReconcileErrors.WithLabelValues(r.cfg.ScaleSetName, "promote_running").Inc()
		}
	}
}

// cleanupOrphanRunners removes GitHub runner registrations that match
// our prefix but have no corresponding DB row AND have been in that
// state for at least orphanGrace. The grace window prevents reaping a
// runner that registered on GitHub microseconds before the orchestrator
// wrote its store row — a real production race during burst scaling.
//
// State for the grace logic lives in r.orphanFirstSeen, which is pruned
// here as runners reappear matched to rows.
func (r *Reconciler) cleanupOrphanRunners(ctx context.Context, rows []pool.RowSnapshot, runners map[string]pool.RunnerInfo) {
	known := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		known[row.Name] = struct{}{}
	}
	now := r.now()
	// Prune entries that are no longer orphaned. Two cases:
	//   - matched to a DB row this tick: drop unconditionally.
	//   - not in `runners` AND `runners` is non-empty: drop, because
	//     GitHub authoritatively says the runner is gone.
	// We deliberately do NOT drop entries when `runners` is empty —
	// that's almost always a transient state between jobs, and resetting
	// the grace clock here is the bug this code path is guarding
	// against. Entries left behind by a genuinely-removed runner will
	// be pruned on the next tick that returns a non-empty list.
	for name := range r.orphanFirstSeen {
		if _, ok := known[name]; ok {
			delete(r.orphanFirstSeen, name)
			continue
		}
		if len(runners) == 0 {
			continue
		}
		if _, ok := runners[name]; !ok {
			delete(r.orphanFirstSeen, name)
		}
	}
	for name, gr := range runners {
		if _, ok := known[name]; ok {
			continue
		}
		firstSeen, ok := r.orphanFirstSeen[name]
		if !ok {
			r.orphanFirstSeen[name] = now
			r.log.Debug("reconcile: tracking new orphan candidate", "name", name, "grace", orphanGrace)
			continue
		}
		if now.Sub(firstSeen) < orphanGrace {
			// Still within grace; give the next tick a chance to match.
			continue
		}
		r.log.Info("reconcile: orphan github runner; removing",
			"name", name, "runner_id", gr.ID, "orphan_age", now.Sub(firstSeen))
		rmCtx, cancel := context.WithTimeout(ctx, cleanupTimeoutPerRunner)
		var err error
		if r.cfg.Scope.Org != "" {
			_, err = r.gh.Actions.RemoveOrganizationRunner(rmCtx, r.cfg.Scope.Org, gr.ID)
		} else {
			owner, repo := splitRepo(r.cfg.Scope.Repo)
			_, err = r.gh.Actions.RemoveRunner(rmCtx, owner, repo, gr.ID)
		}
		cancel()
		if err != nil {
			r.log.Warn("reconcile: orphan runner removal failed", "name", name, "err", err)
			if r.metrics != nil {
				r.metrics.GitHubErrors.WithLabelValues(r.cfg.ScaleSetName, "remove_runner").Inc()
			}
			// Leave in tracking so the next tick retries.
			continue
		}
		delete(r.orphanFirstSeen, name)
	}
}

// sweepProxmoxOrphans finds VMs that Proxmox knows about (and that carry
// our owner tags) but our DB does not. These are the inverse of the
// orphan-runner case: a process restart left a VM running with no DB row
// to drive it. The recovery flow at startup handles this once; the
// reconciler does it every tick so out-of-band VMs (e.g., manual `qm
// clone` of our template) get cleaned up too.
//
// The grace mirror of [cleanupOrphanRunners]: a Proxmox VM missing from
// the store on its FIRST sight is recorded in orphanProxmoxFirstSeen
// and left alone. Only after OrphanGrace elapses does the destroy
// fire. Without the grace, the reconciler races the pool worker's
// boot+inject pipeline and destroys VMs it just cloned (the captured
// production failure: VM 10006 cloned at T=0, destroyed by this sweep
// at T+70s, JIT inject at T+115s then failed with "Configuration file
// does not exist"). Entries are pruned when the VM reappears in
// `known` on a later tick.
func (r *Reconciler) sweepProxmoxOrphans(ctx context.Context, rows []pool.RowSnapshot) {
	pmoxVMs, err := r.prov.ListOwnedVMs(ctx)
	if err != nil {
		r.log.Warn("reconcile: list-owned-vms failed", "err", err)
		return
	}
	known := make(map[int]struct{}, len(rows))
	for _, row := range rows {
		known[row.VMID] = struct{}{}
	}

	// Drop tracking for any VMID that has reappeared in the store so a
	// future re-disappearance doesn't reuse the stale first-seen clock.
	for vmid := range r.orphanProxmoxFirstSeen {
		if _, ok := known[vmid]; ok {
			delete(r.orphanProxmoxFirstSeen, vmid)
		}
	}

	now := r.now()
	for _, pv := range pmoxVMs {
		if _, ok := known[pv.VMID]; ok {
			continue
		}
		first, seen := r.orphanProxmoxFirstSeen[pv.VMID]
		if !seen {
			r.orphanProxmoxFirstSeen[pv.VMID] = now
			r.log.Debug("reconcile: tracking new proxmox orphan candidate",
				"vmid", pv.VMID, "node", pv.Node, "grace", r.cfg.OrphanGrace)
			continue
		}
		if now.Sub(first) < r.cfg.OrphanGrace {
			continue
		}
		r.log.Warn("reconcile: orphan proxmox vm; destroying",
			"vmid", pv.VMID, "node", pv.Node, "orphan_age", now.Sub(first))
		if err := r.prov.Destroy(ctx, pv); err != nil {
			r.log.Warn("reconcile: destroy orphan failed", "vmid", pv.VMID, "err", err)
			continue
		}
		delete(r.orphanProxmoxFirstSeen, pv.VMID)
	}
}

func (r *Reconciler) forceDestroy(ctx context.Context, vmid int, reason, dbState, ghLabel string) {
	if err := r.pool.ForceDestroy(ctx, vmid, reason); err != nil {
		r.log.Warn("reconcile: force destroy failed", "vmid", vmid, "err", err)
		return
	}
	r.recordMismatch(dbState, ghLabel, "destroy")
}

func (r *Reconciler) recordMismatch(dbState, ghState, action string) {
	// Helper enforces the closed enum on ghState/action so a typo or
	// future caller can't blow up Prometheus cardinality silently.
	r.metrics.RecordGHStateMismatch(r.cfg.ScaleSetName, dbState, ghState, action)
}

// ghStateLabel collapses the (present, online, busy) tuple into a single
// low-cardinality label suitable for Prometheus.
func ghStateLabel(gr pool.RunnerInfo, present bool) string {
	if !present {
		return "missing"
	}
	if !gr.Online {
		return "offline"
	}
	if gr.Busy {
		return "busy"
	}
	return "idle"
}

// splitRepo splits "owner/repo" into its halves. Inputs are validated
// upstream by Scope.Validate, so we don't return an error — a malformed
// slug here is a programmer error.
func splitRepo(slug string) (owner, repo string) {
	parts := strings.SplitN(slug, "/", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}
