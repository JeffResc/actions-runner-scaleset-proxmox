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

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/canary"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/config"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/ipam"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/nodeselector"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/observability"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/provisioner"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/store"
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

// ErrUnknownProfile is returned by SetTargetSizes (and any future
// per-profile API) when the operator names a profile the manager
// has no state for.
var ErrUnknownProfile = errors.New("pool: unknown profile")

// ProfileSettings is the per-profile sizing and clone-shape the
// manager applies. Zero / empty fields fall back to the top-level
// Config defaults (HotSize / WarmSize / MaxConcurrentRunners /
// BootMaxAttempts / VMMaxAge) so existing single-profile callers
// don't need to populate the slice.
type ProfileSettings struct {
	// Name is the profile identifier. Must be non-empty when
	// Profiles is set; pool.NewManager synthesises a default
	// "default" profile when the slice is empty.
	Name string

	// HotSize / WarmSize / MaxConcurrentRunners / BootMaxAttempts
	// override the top-level Config fields when > 0.
	HotSize              int
	WarmSize             int
	MaxConcurrentRunners int
	BootMaxAttempts      int

	// VMMaxAge overrides the top-level VMMaxAge when > 0. Zero
	// inherits.
	VMMaxAge time.Duration

	// Per-clone hardware shape applied to provisioner.CloneOptions.
	// Zero / empty inherits the template's default.
	TemplateVMID int
	CPUCores     int
	MemoryMB     int
	DiskGB       int
	Storage      string

	// NICs sets the cloned VM's network interfaces. Empty leaves
	// the template's NICs in place — production typically lists
	// at least net0 even for the default profile to disambiguate
	// the "use template default" path.
	NICs []provisioner.CloneNIC

	// IPAM allocates a static IP for each clone (and releases it
	// on destroy). Nil falls back to ipam.Noop — clones boot via
	// DHCP.
	IPAM ipam.Allocator
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

	// Profiles enables multi-profile operation. When empty, the
	// manager synthesises a single "default" profile from the
	// fields above so existing single-profile callers (and tests)
	// continue to work unchanged.
	Profiles []ProfileSettings

	// Canary, when non-nil, drives the canary template selection
	// for new clones and tracks boot failures by template class.
	// Nil = every clone uses the profile's TemplateVMID (no
	// canary).
	Canary *canary.Controller

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

	// VMIDReuseCooldown gates how soon after a destroy completes the
	// allocator may reissue the same VMID. allocateVMID consults
	// Provisioner.IsRecentlyDestroyed with this duration to skip
	// VMIDs whose PVE-side qmdestroy task is still settling — without
	// it, a fresh clone targets a VMID Proxmox is still tearing down
	// and produces "VM N is running - destroy failed" + lock-file
	// contention. Zero falls back to 30s.
	VMIDReuseCooldown time.Duration

	// OnRunnerOrphaned is invoked when the manager destroys a VM whose
	// row had a runner_id set, i.e. a runner that was registered with
	// GitHub. The callback is expected to deregister the runner. Best
	// effort — errors are logged but don't block destruction. Nil is OK
	// and treated as a no-op (e.g. in tests).
	OnRunnerOrphaned func(ctx context.Context, runnerID int64) error

	// RunnerLister is consulted by Adopt to classify owner-tagged
	// Proxmox VMs more precisely: a VM whose runner is busy on GitHub
	// is adopted directly as Running with the right RunnerID, skipping
	// the Hot → reconcile → promote round-trip the gh.Reconciler would
	// otherwise do. Nil is OK — Adopt falls back to power-state-only
	// classification (the gh.Reconciler's first tick converges anyway).
	RunnerLister RunnerLister
}

// profileState is the per-profile runtime state.
type profileState struct {
	settings     ProfileSettings
	desiredCount atomic.Int32
	// hotSize and warmSize are the live target sizes the
	// reconcile loop reads each tick. Initialised from
	// settings.{HotSize,WarmSize} and mutated at runtime by
	// SetTargetSizes (driven by the schedule package, issue #9).
	// Atomic int32 so the reconcile reader and the schedule
	// goroutine writer don't contend on a mutex.
	hotSize  atomic.Int32
	warmSize atomic.Int32
	// refill is a per-profile signal channel so a profile's
	// reconcile loop can be nudged without waking sibling loops.
	refill chan struct{}
}

// manager is the in-process Manager implementation.
type manager struct {
	cfg     Config
	store   *store.Store
	prov    provisioner.Provisioner
	sel     nodeselector.Selector
	log     *slog.Logger
	metrics *observability.Metrics

	// profiles is the per-profile state keyed by ProfileSettings.Name.
	// At least one entry — NewManager synthesises a default profile
	// when the operator didn't declare any.
	profiles map[string]*profileState

	// profileOrder preserves the declaration order from the config so
	// callers that don't pass an explicit profile name (the
	// backwards-compatible Acquire path) get deterministic selection.
	profileOrder []string

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

	// destroyQueue is the bounded backlog for destroyAsync. A single
	// dispatcher goroutine reads from it, acquires destroySem, and
	// spawns the actual destroy worker. The bound keeps a destroy
	// storm (SIGTERM during a poison sweep, mass-recycle, etc.) from
	// spawning unbounded goroutines that sit waiting for sem slots.
	// Overflow is surfaced as PoolDestroyBacklogFull; the row stays in
	// a transient state and the stuck-state sweep re-enqueues it.
	destroyQueue chan destroyRequest

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

	// desiredCount mirrors the per-profile desiredCount in the
	// single-profile / "default" case so existing callers of
	// SetDesiredCount (which is fleet-wide) still observe the
	// classic semantics. With multiple profiles it represents the
	// fleet-wide aggregate; per-profile demand split is owed by the
	// caller (PR 2 / #7 — label routing).
	desiredCount atomic.Int32

	// powerPollErrLastLog tracks, per-VMID, when we last emitted a
	// Debug line for a PowerState query failure. Used by powerPollOnce
	// to throttle the per-VM error log to at most one line per VMID
	// per powerPollErrLogInterval — without this, a flapping Proxmox
	// endpoint produces one Debug line per VM per tick (for fleets
	// where Booting/WaitingReady is a sizeable share of the population,
	// that's thousands of identical lines per minute that drown out
	// the real signal). Only the power-poll goroutine touches this
	// map, so no synchronisation is needed.
	powerPollErrLastLog map[int]time.Time
}

// normaliseProfiles returns the profile slice the manager will
// operate on, synthesising a single default profile from the
// top-level Config fields when none were declared. Per-profile
// fields left at zero inherit the top-level value.
func normaliseProfiles(cfg Config) []ProfileSettings {
	if len(cfg.Profiles) == 0 {
		return []ProfileSettings{{
			Name:                 defaultProfileName,
			HotSize:              cfg.HotSize,
			WarmSize:             cfg.WarmSize,
			MaxConcurrentRunners: cfg.MaxConcurrentRunners,
			BootMaxAttempts:      cfg.BootMaxAttempts,
			VMMaxAge:             cfg.VMMaxAge,
		}}
	}
	out := make([]ProfileSettings, len(cfg.Profiles))
	for i, p := range cfg.Profiles {
		if p.HotSize == 0 {
			p.HotSize = cfg.HotSize
		}
		if p.WarmSize == 0 {
			p.WarmSize = cfg.WarmSize
		}
		if p.MaxConcurrentRunners == 0 {
			p.MaxConcurrentRunners = cfg.MaxConcurrentRunners
		}
		if p.BootMaxAttempts == 0 {
			p.BootMaxAttempts = cfg.BootMaxAttempts
		}
		if p.VMMaxAge == 0 {
			p.VMMaxAge = cfg.VMMaxAge
		}
		out[i] = p
	}
	return out
}

// defaultProfileName must match tags.DefaultProfile. Duplicated here
// (rather than imported) so the pool package keeps its short import
// list and doesn't take a new dependency on tags solely for a string
// literal.
const defaultProfileName = "default"

// destroyQueueCap bounds the destroy dispatcher backlog. Chosen as 2x
// the destroy semaphore cap so steady-state bursts have slack without
// the goroutine count growing unboundedly. Overflow surfaces as
// PoolDestroyBacklogFull; the row stays in a transient state and the
// stuck-state sweep re-enqueues it on the next reconcile pass.
const destroyQueueCap = 16

// NewManager constructs a Manager.
func NewManager(cfg Config, st *store.Store, prov provisioner.Provisioner, sel nodeselector.Selector, log *slog.Logger, metrics *observability.Metrics) (Manager, error) {
	cfg.Profiles = normaliseProfiles(cfg)
	for _, p := range cfg.Profiles {
		if p.Name == "" {
			return nil, fmt.Errorf("pool: profile name must be non-empty")
		}
		if err := validateConfig(p.HotSize, p.WarmSize, p.MaxConcurrentRunners); err != nil {
			return nil, fmt.Errorf("pool: profile %q: %w", p.Name, err)
		}
	}
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
	if cfg.VMIDReuseCooldown <= 0 {
		cfg.VMIDReuseCooldown = 30 * time.Second
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
	m := &manager{
		cfg:                 cfg,
		store:               st,
		prov:                prov,
		sel:                 sel,
		log:                 log,
		metrics:             metrics,
		refill:              make(chan struct{}, 1),
		cloneSem:            semaphore.NewWeighted(maxConcurrentClones),
		destroySem:          semaphore.NewWeighted(maxConcurrentDestroys),
		bootSem:             semaphore.NewWeighted(maxConcurrentBoots),
		workerCtx:           wctx,
		workerCancel:        wcancel,
		destroyQueue:        make(chan destroyRequest, destroyQueueCap),
		profiles:            make(map[string]*profileState, len(cfg.Profiles)),
		profileOrder:        make([]string, 0, len(cfg.Profiles)),
		powerPollErrLastLog: make(map[int]time.Time),
	}
	for _, p := range cfg.Profiles {
		ps := &profileState{
			settings: p,
			refill:   make(chan struct{}, 1),
		}
		ps.hotSize.Store(int32(p.HotSize))   //nolint:gosec // pool sizes are config-bounded
		ps.warmSize.Store(int32(p.WarmSize)) //nolint:gosec // pool sizes are config-bounded
		m.profiles[p.Name] = ps
		m.profileOrder = append(m.profileOrder, p.Name)
	}
	go m.destroyDispatcher()
	return m, nil
}

// defaultProfile returns the profile name to use when a caller doesn't
// specify one — the first declared profile (which is the synthesised
// "default" profile in single-profile configs).
func (m *manager) defaultProfile() string {
	return m.profileOrder[0]
}

// profileOf returns the profileState for the given name, falling back
// to the default profile when name is empty. Returns nil for an
// unknown explicit name so callers can surface a clear error.
func (m *manager) profileOf(name string) *profileState {
	if name == "" {
		return m.profiles[m.defaultProfile()]
	}
	return m.profiles[name]
}

// signalProfileRefill nudges one profile's reconcile loop without
// waking sibling loops.
func (m *manager) signalProfileRefill(profile string) {
	ps := m.profileOf(profile)
	if ps == nil {
		return
	}
	select {
	case ps.refill <- struct{}{}:
	default:
	}
}

// SignalRefill nudges every per-profile reconcile loop. The
// fleet-wide channel is also signalled so callers waiting on it
// (legacy single-profile path) wake up.
func (m *manager) SignalRefill() {
	select {
	case m.refill <- struct{}{}:
	default:
	}
	for _, name := range m.profileOrder {
		m.signalProfileRefill(name)
	}
}

// SetDesiredCount records the listener-side "total assigned jobs" so
// reconcile can scale up beyond HotSize when the burst calls for it.
// Fleet-wide aggregate. In single-profile configs (the only
// production shape today) the value flows through to the default
// profile's desiredCount. With multi-profile configs the per-profile
// split is the caller's responsibility (PR 2 / #7 — label routing);
// until that lands, the value is also applied to the default profile.
func (m *manager) SetDesiredCount(n int) {
	if n < 0 {
		n = 0
	}
	prev := m.desiredCount.Swap(int32(n)) // #nosec G115 -- n is bounded by GitHub's signal; clamped above
	if int(prev) != n {
		m.log.Debug("desired count updated", "from", prev, "to", n)
	}
	if ps := m.profileOf(""); ps != nil {
		ps.desiredCount.Store(int32(n)) // #nosec G115 -- see above
	}
	m.SignalRefill()
}

// SetTargetSizes updates the live hot/warm target sizes for the
// named profile (issue #9). Called from the schedule runner on
// each cron fire / window expiry; the reconcile loop picks up
// the new values on its next tick and converges (draining
// excess hot/warm capacity if the new targets are smaller,
// cloning more if larger).
//
// Returns ErrUnknownProfile when name doesn't match a
// configured profile. Negative sizes are clamped to zero, and
// the sum is clamped to the profile's MaxConcurrentRunners so a
// runaway schedule can't permanently exceed the operator's
// concurrency cap.
func (m *manager) SetTargetSizes(name string, hot, warm int) error {
	ps, ok := m.profiles[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownProfile, name)
	}
	if hot < 0 {
		hot = 0
	}
	if warm < 0 {
		warm = 0
	}
	if cap := ps.settings.MaxConcurrentRunners; hot+warm > cap {
		// Trim warm first — hot is the latency-sensitive pool
		// operators tend to care about more.
		if hot > cap {
			hot = cap
			warm = 0
		} else {
			warm = cap - hot
		}
	}
	prevHot := ps.hotSize.Swap(int32(hot))    //nolint:gosec // bounded by MaxConcurrentRunners
	prevWarm := ps.warmSize.Swap(int32(warm)) //nolint:gosec // bounded above
	if int(prevHot) != hot || int(prevWarm) != warm {
		m.log.Info("pool: target sizes updated",
			"profile", name,
			"hot_from", prevHot, "hot_to", hot,
			"warm_from", prevWarm, "warm_to", warm)
		m.SignalRefill()
	}
	return nil
}

// Stats returns the orchestrator-wide pool-population snapshot.
// PoolSize gauge is emitted PER PROFILE rather than fleet-wide so
// dashboards can slice by hardware shape; the returned aggregate
// Stats keeps the previous shape for backwards-compatible callers
// (admin API, tests).
func (m *manager) Stats(_ context.Context) (Stats, error) {
	raw, err := m.store.Stats()
	if err != nil {
		return Stats{}, fmt.Errorf("stats: %w", err)
	}
	stats := statsFromRaw(raw)
	if m.metrics != nil {
		for _, name := range m.profileOrder {
			perProfile, perr := m.store.StatsByProfile(name)
			if perr != nil {
				continue
			}
			for st, n := range perProfile {
				m.metrics.PoolSize.WithLabelValues(m.cfg.ScaleSetName, name, string(st)).Set(float64(n))
			}
		}
	}
	return stats, nil
}

// statsForProfile returns the population snapshot scoped to a single
// profile. Used by the per-profile reconcile loop. store.Insert
// stamps every row with a non-empty Profile (default-profile name
// when unset), so this is a single profile-scoped query.
func (m *manager) statsForProfile(profile string) (Stats, error) {
	raw, err := m.store.StatsByProfile(profile)
	if err != nil {
		return Stats{}, fmt.Errorf("stats: %w", err)
	}
	if m.metrics != nil {
		for st, n := range raw {
			m.metrics.PoolSize.WithLabelValues(m.cfg.ScaleSetName, profile, string(st)).Set(float64(n))
		}
	}
	return statsFromRaw(raw), nil
}

// statsFromRaw projects a store-state map into the Stats struct.
func statsFromRaw(raw map[store.State]int) Stats {
	return Stats{
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
}

// Acquire atomically transitions one Hot VM to Assigned. Selection
// is by oldest-Hot-first (preferring VMs near max-age recycle so we
// don't carry stale VMs forever).
//
// Profile-agnostic: picks the oldest Hot VM across ALL profiles. The
// per-call maxBusy clamp applies to the orchestrator-wide busy count.
// This is the backwards-compatible single-profile entry point;
// multi-profile callers that want per-profile scoping should use
// AcquireForProfile.
//
// The cap check (busy < MaxConcurrentRunners; additionally busy <
// maxBusy when maxBusy > 0) and the Hot→Assigned CAS happen inside
// the same store write transaction, so concurrent Acquire callers
// cannot over-provision past either ceiling.
func (m *manager) Acquire(ctx context.Context, jobID int64, maxBusy int) (*VM, error) {
	ctx, span := tracer.Start(ctx, "pool.Acquire",
		trace.WithAttributes(attribute.Int64("job.id", jobID)))
	defer span.End()
	_ = ctx
	row, err := m.store.AcquireHot(jobID, m.cfg.MaxConcurrentRunners, maxBusy)
	switch {
	case errors.Is(err, store.ErrAtCapacity):
		span.SetStatus(codes.Ok, "at_capacity")
		if m.metrics != nil {
			m.metrics.AtCapacityTotal.WithLabelValues(m.cfg.ScaleSetName).Inc()
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
	return &VM{VMID: row.VMID, Node: row.Node, Name: row.Name, Profile: row.Profile}, nil
}

// AcquireForProfile is the profile-aware variant of Acquire. An
// explicit profile name scopes both the Hot-VM search and the
// maxBusy clamp to that profile (so one profile can't starve
// another). An empty profile name falls back to Acquire's
// profile-agnostic behaviour for backwards compatibility. Unknown
// explicit names return an error.
func (m *manager) AcquireForProfile(ctx context.Context, jobID int64, profile string, maxBusy int) (*VM, error) {
	if profile == "" {
		return m.Acquire(ctx, jobID, maxBusy)
	}
	ps := m.profileOf(profile)
	if ps == nil {
		return nil, fmt.Errorf("acquire: unknown profile %q", profile)
	}
	resolved := ps.settings.Name
	ctx, span := tracer.Start(ctx, "pool.Acquire", trace.WithAttributes(
		attribute.Int64("job.id", jobID),
		attribute.String("pool.profile", resolved),
	))
	defer span.End()
	_ = ctx
	row, err := m.store.AcquireHotInProfile(resolved, jobID, m.cfg.MaxConcurrentRunners, maxBusy)
	switch {
	case errors.Is(err, store.ErrAtCapacity):
		span.SetStatus(codes.Ok, "at_capacity")
		if m.metrics != nil {
			m.metrics.AtCapacityTotal.WithLabelValues(m.cfg.ScaleSetName).Inc()
		}
		return nil, ErrAtCapacity
	case errors.Is(err, store.ErrNoneAvailable):
		span.SetStatus(codes.Ok, "none_available")
		m.signalProfileRefill(resolved)
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
	m.signalProfileRefill(resolved)
	return &VM{VMID: row.VMID, Node: row.Node, Name: row.Name, Profile: resolved}, nil
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

// StampJobMetadata records per-job context (org/repo/class) on the
// row without changing state. Idempotent — empty fields are
// skipped so a partial second call doesn't blank fields the first
// call set. A missing row is a no-op (the runner may have been
// destroyed between JobStarted observation and this call).
func (m *manager) StampJobMetadata(_ context.Context, vmid int, meta JobMetadata) error {
	if meta.Org == "" && meta.Repo == "" && meta.PriorityClass == "" {
		return nil
	}
	_, err := m.store.Update(vmid, func(v *store.VM) {
		if meta.Org != "" {
			v.Org = meta.Org
		}
		if meta.Repo != "" {
			v.Repo = meta.Repo
		}
		if meta.PriorityClass != "" {
			v.PriorityClass = meta.PriorityClass
		}
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stamp job metadata: %w", err)
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
// the destroy goroutine. Used by the reconciler when GitHub tells us
// the runner is gone but the store still thinks it's busy. Reason is
// logged so the forensic trail is preserved.
//
// Concurrent ForceDestroy calls for the same VMID are de-duplicated by
// CAS: exactly one caller wins the transition into Draining and spawns
// destroyAsync. Subsequent callers (and callers racing a row that's
// already Draining/Destroying) observe ok == false and return cleanly,
// so we don't double-call prov.Destroy or burn duplicate Proxmox /
// GitHub API budget.
func (m *manager) ForceDestroy(_ context.Context, vmid int, reason string) error {
	from := []store.State{
		store.StateProvisioning,
		store.StateWarm,
		store.StateBooting,
		store.StateHot,
		store.StateAssigned,
		store.StateRunning,
		store.StatePoison,
	}
	var node, profile string
	ok, err := m.store.UpdateStateIn(vmid, from, store.StateDraining, func(v *store.VM) {
		node = v.Node
		profile = v.Profile
	})
	if err != nil {
		return fmt.Errorf("force destroy: cas: %w", err)
	}
	if !ok {
		// Either the row is gone, or it's already Draining/Destroying
		// — another caller owns its teardown. Nothing more to do.
		return nil
	}
	m.log.Warn("force destroy", "vmid", vmid, "reason", reason)
	m.destroyAsync(vmid, node, profile)
	return nil
}

// Preempt destroys an Assigned-but-not-yet-Running VM to free
// capacity for a higher-priority job (issue #10). Refuses any row
// not in Assigned — never preempts Running (interrupting an
// actively-executing job is the destructive behaviour we promise
// never to do); Hot/Warm/Booting/Provisioning rows are released
// through the natural shrink-to-floor reconcile path, not here.
//
// On success the row is CAS-transitioned to Draining and an async
// destroy is queued. Returns ErrPreemptRefused with a descriptive
// reason (lookup miss, wrong state) when the transition is not
// applied.
func (m *manager) Preempt(_ context.Context, vmid int, reason string) error {
	target, err := m.store.Get(vmid)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("%w: vmid %d not found", ErrPreemptRefused, vmid)
		}
		return fmt.Errorf("preempt: lookup: %w", err)
	}
	if target.State != store.StateAssigned {
		return fmt.Errorf("%w: vmid %d is in state %s", ErrPreemptRefused, vmid, target.State)
	}
	ok, err := m.store.UpdateState(vmid, store.StateAssigned, store.StateDraining, func(v *store.VM) {
		v.StateSince = time.Now()
	})
	if err != nil {
		return fmt.Errorf("preempt: cas: %w", err)
	}
	if !ok {
		// CAS lost — between our Get and the UpdateState the row
		// transitioned to Running (the runner picked up work) or
		// another caller force-destroyed it. Either way the right
		// answer is "don't act"; surface as a refusal so the
		// caller's metric reflects the no-op.
		return fmt.Errorf("%w: vmid %d state changed during preempt", ErrPreemptRefused, vmid)
	}
	m.log.Warn("preempt", "vmid", vmid, "reason", reason, "from_class", target.PriorityClass)
	m.destroyAsync(vmid, target.Node, target.Profile)
	return nil
}

// ListRows returns a snapshot of every non-terminal VM row for the
// GitHub reconciler. Terminal rows (Draining, Destroying) are excluded
// because the reconciler shouldn't second-guess in-flight destruction.
// snapshotExistingVMsForAffinity returns the live VM rows projected
// down to the (Node, Profile) tuples the nodeselector's affinity
// wrapper needs. Excludes terminal states (Draining, Destroying):
// a row on its way out shouldn't keep its node off the candidate
// list for an anti-affinity rule. Errors are swallowed — the
// affinity wrapper degrades to "no exclusions" if the store
// momentarily misbehaves, which is preferable to failing every
// clone over a transient list error.
func (m *manager) snapshotExistingVMsForAffinity() []nodeselector.ExistingVM {
	rows, err := m.store.ListExcludingStates(store.StateDraining, store.StateDestroying)
	if err != nil {
		m.log.Warn("affinity: snapshot list failed; selector will see empty existing set", "err", err)
		return nil
	}
	out := make([]nodeselector.ExistingVM, 0, len(rows))
	for _, r := range rows {
		out = append(out, nodeselector.ExistingVM{Node: r.Node, Profile: r.Profile})
	}
	return out
}

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
			Profile:    r.Profile,
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
	case store.StateProvisioning,
		store.StateWarm,
		store.StateBooting,
		store.StateHot,
		store.StatePoison:
		// A runner-hook "completed" event for a row in any of these
		// states is either a spoof or a wildly stale retry. Refuse.
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
	m.destroyAsync(target.VMID, target.Node, target.Profile)
	m.SignalRefill()
	return nil
}

// Recover reconciles the (empty) in-memory store against Proxmox reality
// on startup. With no persistent state to load, this collapses to
// Adopt seeds the in-memory store from Proxmox + GitHub observations
// for every owner-tagged VM the previous leader (or this process's
// prior incarnation) left behind. The classification matrix is:
//
//	power=stopped                          → Warm   / PoolKindWarm
//	power=running, no GitHub runner        → Hot    / PoolKindHot
//	power=running, runner busy             → Running  / PoolKindHot (with RunnerID)
//	power=running, runner online & idle    → Assigned / PoolKindHot (with RunnerID)
//	power=running, runner offline          → Assigned / PoolKindHot (with RunnerID)
//	power query failed                     → Hot    / PoolKindHot (safe fallback)
//
// The gh.Reconciler's matrix converges any misclassifications on its
// next tick (Hot+busy → promote to Running, Assigned/Running grace
// timers, etc.), so adoption only needs to be approximately right.
//
// Best-effort by design: a per-VM PowerState or store.Insert failure
// is logged and the loop continues. The whole-pass error is returned
// only when ListOwnedVMs itself fails — Proxmox is unreachable, no
// classification is possible. The caller (app.runLeaderPlane) logs
// and continues with an empty pool; the orphan-sweep in gh.Reconciler
// will reap any stranded VMs once Proxmox recovers.
//
// JobID is intentionally left 0: we don't know which GitHub job an
// in-flight runner is executing, and the power-poller / reconciler
// don't need it — they key on RunnerID and VM power state.
func (m *manager) Adopt(ctx context.Context) error {
	pmoxVMs, err := m.prov.ListOwnedVMs(ctx)
	if err != nil {
		return fmt.Errorf("adopt: list owned vms: %w", err)
	}

	// Best-effort GitHub runner snapshot. A whole-pass failure here is
	// non-fatal: power-state-only classification still preserves every
	// inherited VM; the next gh.Reconciler tick reclassifies Hot rows
	// whose runners turn out to be busy.
	var runners map[string]RunnerInfo
	if m.cfg.RunnerLister != nil {
		runners, err = m.cfg.RunnerLister(ctx)
		if err != nil {
			m.log.Warn("adopt: github runner list failed; classifying by power-state only", "err", err)
			runners = nil
		}
	}

	for _, pv := range pmoxVMs {
		m.adoptOne(ctx, pv, runners)
	}
	return nil
}

// adoptOne classifies a single inherited VM and inserts it into the
// store. Extracted so the per-VM error paths can `return` cleanly
// without leaking the loop control structure into the classification.
func (m *manager) adoptOne(ctx context.Context, pv *provisioner.VM, runners map[string]RunnerInfo) {
	state, kind, runnerID := m.classifyAdoption(ctx, pv, runners)
	// Route the adopted VM into a known profile. An unrecognised
	// profile name (operator removed a profile config but VMs from
	// it still exist on Proxmox) is force-routed to the default
	// profile so the destroyer can still recycle the row instead of
	// leaving it orphaned.
	profile := pv.Profile
	if profile == "" || m.profileOf(profile) == nil {
		if profile != "" {
			m.log.Warn("adopt: unknown profile on inherited vm; routing to default",
				"vmid", pv.VMID, "profile", profile, "default", m.defaultProfile())
		}
		profile = m.defaultProfile()
	}
	row := &store.VM{
		VMID:     pv.VMID,
		Node:     pv.Node,
		Name:     pv.Name,
		Profile:  profile,
		PoolKind: kind,
		State:    state,
		RunnerID: runnerID,
	}
	if err := m.store.Insert(row); err != nil {
		m.log.Warn("adopt: insert failed; skipping",
			"vmid", pv.VMID, "node", pv.Node, "state", state, "err", err)
		return
	}
	m.log.Info("adopt: inherited vm",
		"vmid", pv.VMID, "node", pv.Node, "name", pv.Name, "profile", profile,
		"state", state, "pool_kind", kind, "runner_id", runnerID)
	if m.metrics != nil {
		m.metrics.VMsTotal.WithLabelValues(m.cfg.ScaleSetName, profile, "adopted_"+string(state)).Inc()
	}
}

// adoptionKey is the explicit state matrix axis: each cell maps a
// (Proxmox-running, runner-present, runner-busy) combination to one
// adoptionClass. classifyAdoption walks the inputs into this key and
// looks up the result — the matrix is the documentation.
type adoptionKey struct {
	powerRunning  bool
	runnerPresent bool
	runnerBusy    bool
}

// adoptionClass is the per-cell outcome. withRunnerID selects whether
// the GitHub runner's ID is preserved on the inherited row; for Hot /
// Warm cells the row carries no runner association so we explicitly
// zero it.
type adoptionClass struct {
	state        store.State
	kind         store.PoolKind
	withRunnerID bool
}

// adoptionMatrix exhaustively enumerates every (powerRunning,
// runnerPresent, runnerBusy) triple. Listing all eight cells (over an
// "any" sentinel) keeps the table grep-friendly and forces a new
// classification to be a single-line addition.
var adoptionMatrix = map[adoptionKey]adoptionClass{
	// power != "running" → Warm regardless of runner snapshot.
	{powerRunning: false, runnerPresent: false, runnerBusy: false}: {store.StateWarm, store.PoolKindWarm, false},
	{powerRunning: false, runnerPresent: false, runnerBusy: true}:  {store.StateWarm, store.PoolKindWarm, false},
	{powerRunning: false, runnerPresent: true, runnerBusy: false}:  {store.StateWarm, store.PoolKindWarm, false},
	{powerRunning: false, runnerPresent: true, runnerBusy: true}:   {store.StateWarm, store.PoolKindWarm, false},
	// powerRunning && !runnerPresent → Hot (no registered runner).
	{powerRunning: true, runnerPresent: false, runnerBusy: false}: {store.StateHot, store.PoolKindHot, false},
	{powerRunning: true, runnerPresent: false, runnerBusy: true}:  {store.StateHot, store.PoolKindHot, false}, // logically unreachable; included for completeness
	// powerRunning && runnerPresent && !busy → Assigned (online-idle
	// or offline-registered). Reconciler's grace timers will recycle.
	{powerRunning: true, runnerPresent: true, runnerBusy: false}: {store.StateAssigned, store.PoolKindHot, true},
	// powerRunning && runnerPresent && busy → Running (in flight).
	{powerRunning: true, runnerPresent: true, runnerBusy: true}: {store.StateRunning, store.PoolKindHot, true},
}

// classifyAdoption picks (State, PoolKind, RunnerID) for one inherited
// VM from observable Proxmox power state and the GitHub runner snapshot
// (which may be nil when GitHub was unreachable).
//
// The PowerState query failure path defaults to Hot (NOT Warm) so the
// reconciler's hot+busy → promote-to-Running rule can recover any
// in-flight job if a runner does turn out to be registered. Defaulting
// to Warm would hide the VM from the reconciler's matrix entirely.
func (m *manager) classifyAdoption(ctx context.Context, pv *provisioner.VM, runners map[string]RunnerInfo) (store.State, store.PoolKind, int64) {
	// Per-VM timeout so one stuck node doesn't pin the leader-plane
	// startup. The default-to-hot fallback below absorbs the timeout
	// cleanly — the gh.Reconciler reclassifies on its first tick.
	vmCtx, cancel := context.WithTimeout(ctx, adoptPowerStateTimeoutPerVM)
	defer cancel()
	power, err := m.prov.PowerState(vmCtx, pv)
	if err != nil {
		m.log.Warn("adopt: power-state query failed; defaulting to hot",
			"vmid", pv.VMID, "node", pv.Node, "err", err)
		return store.StateHot, store.PoolKindHot, 0
	}
	gr, present := runners[pv.Name]
	key := adoptionKey{
		powerRunning:  power == "running",
		runnerPresent: present,
		runnerBusy:    present && gr.Busy,
	}
	class := adoptionMatrix[key]
	var runnerID int64
	if class.withRunnerID {
		runnerID = gr.ID
	}
	return class.state, class.kind, runnerID
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
	// Kick once on entry so every profile reconciles immediately.
	m.SignalRefill()

	// Power-state poller. Runs independently of the reconcile loops
	// so a slow Proxmox reply doesn't delay reconcile and vice versa.
	pollerDone := make(chan struct{})
	go func() {
		defer close(pollerDone)
		m.runPowerPoll(ctx)
	}()

	// Per-profile reconcile loops. Each profile's reconcile is
	// independent — one profile's saturated clone budget doesn't
	// delay another's. Async workers still share workerCtx and the
	// concurrency semaphores so total Proxmox parallelism is
	// fleet-wide bounded.
	loopsDone := make(chan struct{})
	go func() {
		defer close(loopsDone)
		var wg sync.WaitGroup
		for _, name := range m.profileOrder {
			wg.Add(1)
			go func(profile string) {
				defer wg.Done()
				m.runProfileLoop(ctx, profile)
			}(name)
		}
		wg.Wait()
	}()

	<-ctx.Done()
	// CRITICAL: wait for the producers of destroyAsync (power
	// poller + profile reconcile loops) to fully exit BEFORE
	// entering drain(). Both can call m.wg.Add(1) from their
	// in-flight iteration after ctx.Done() fires; if drain()'s
	// internal wg.Wait() observes wg=0 in that window and then a
	// late Add(1) lands, sync.WaitGroup panics with
	// "WaitGroup is reused before previous Wait has returned" — and
	// even when it doesn't panic the race detector flags it
	// (Add after Wait hit zero is undefined per sync docs).
	<-pollerDone
	<-loopsDone
	m.drain()
	return nil
}

// runProfileLoop is one profile's reconcile loop. Exits when ctx is
// cancelled; drain is fleet-wide and handled by Run after loops
// return.
func (m *manager) runProfileLoop(ctx context.Context, profile string) {
	ps := m.profileOf(profile)
	if ps == nil {
		return
	}
	tick := time.NewTicker(m.cfg.ReconcileInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			m.reconcileProfileOnce(ctx, profile)
		case <-ps.refill:
			m.reconcileProfileOnce(ctx, profile)
		case <-m.refill:
			// Fleet-wide refill — every profile loop wakes.
			m.reconcileProfileOnce(ctx, profile)
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

// powerPollTimeoutPerVM caps how long a single per-VM PowerState call
// may block. Without it, one stuck Proxmox node would pin the entire
// poll pass for up to the underlying HTTP client's 60s timeout per VM,
// stalling job-completion detection across the rest of the fleet.
// Mirrors templateDiscoveryTimeoutPerNode in the provisioner package.
// Tests may override this.
var powerPollTimeoutPerVM = 5 * time.Second

// powerPollErrLogInterval is the minimum wall-clock gap between two
// Debug log lines for the same VMID's PowerState query failure. A
// flapping Proxmox endpoint would otherwise produce one line per VM
// per tick — at HotSize=50 and PowerPollInterval=3s that's 1000
// identical lines per minute. With a 30s gap the same flap surfaces
// twice per minute per VM, which is enough to diagnose without
// drowning the log stream. Variable so tests can compress the
// interval.
var powerPollErrLogInterval = 30 * time.Second

// adoptPowerStateTimeoutPerVM caps how long Adopt's per-VM PowerState
// query may block during leader-plane startup. A bit looser than
// powerPollTimeoutPerVM since adopt is a one-off cost paid before the
// reconciler starts running, but still tight enough that a single
// hung Proxmox node can't drag startup out for the full HTTP client
// timeout times the inherited-VM count.
var adoptPowerStateTimeoutPerVM = 10 * time.Second

// powerPollOnce does a single pass over Assigned/Running rows. Exposed
// (lower-case) so tests can drive it deterministically without spinning
// the time-based Run loop. Each per-VM PowerState call is bounded by
// powerPollTimeoutPerVM so a single hung node can't freeze the poller.
func (m *manager) powerPollOnce(ctx context.Context) {
	rows, err := m.store.ListByState(store.StateAssigned, store.StateRunning)
	if err != nil {
		m.log.Warn("power-poll: list rows failed", "err", err)
		return
	}
	now := time.Now()
	seen := make(map[int]struct{}, len(rows))
	suppressed := 0
	for _, row := range rows {
		seen[row.VMID] = struct{}{}
		vmCtx, cancel := context.WithTimeout(ctx, powerPollTimeoutPerVM)
		state, err := m.prov.PowerState(vmCtx, &provisioner.VM{
			VMID: row.VMID, Node: row.Node, Name: row.Name, Profile: row.Profile,
		})
		cancel()
		if err != nil {
			// Per-VMID rate-limited Debug. A flapping Proxmox endpoint
			// would otherwise emit one line per VM per tick — see the
			// powerPollErrLogInterval doc for the rationale.
			if last, ok := m.powerPollErrLastLog[row.VMID]; !ok || now.Sub(last) >= powerPollErrLogInterval {
				m.log.Debug("power-poll: query failed; will retry", "vmid", row.VMID, "err", err)
				m.powerPollErrLastLog[row.VMID] = now
			} else {
				suppressed++
			}
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
	// One summary line per tick when the per-VMID throttle swallowed
	// any errors. Keeps observability that a flap is in progress
	// without the per-VM line storm.
	if suppressed > 0 {
		m.log.Debug("power-poll: suppressed repeated error logs", "count", suppressed)
	}
	// Prune entries whose VMID is no longer in the poll set so the
	// map can't grow unboundedly as VMIDs are recycled. Touched only
	// by this goroutine, so no synchronisation needed.
	for vmid := range m.powerPollErrLastLog {
		if _, ok := seen[vmid]; !ok {
			delete(m.powerPollErrLastLog, vmid)
		}
	}
}

// reconcileOnce is kept as a thin wrapper that delegates to the
// default profile's reconcile. Retained so existing tests that drive
// reconcile directly continue to work; new code should call
// reconcileProfileOnce.
func (m *manager) reconcileOnce(ctx context.Context) {
	for _, name := range m.profileOrder {
		m.reconcileProfileOnce(ctx, name)
	}
}

// reconcileProfileOnce computes the desired pool population for a
// single profile and dispatches the async operations to reach it.
// Profile-scoped: per-profile hot/warm sizes, per-profile busy
// counts, per-profile shrink/recycle. Shared resources (semaphores,
// VMID allocator) remain fleet-wide so total Proxmox concurrency is
// bounded.
func (m *manager) reconcileProfileOnce(ctx context.Context, profile string) {
	ps := m.profileOf(profile)
	if ps == nil {
		return
	}
	start := time.Now()
	defer func() {
		if m.metrics != nil {
			m.metrics.ReconcileDuration.WithLabelValues(m.cfg.ScaleSetName).Observe(time.Since(start).Seconds())
		}
	}()

	stats, err := m.statsForProfile(profile)
	if err != nil {
		m.log.Warn("reconcile: stats failed", "profile", profile, "err", err)
		return
	}
	// Per-profile Hot/Warm Provisioning counts. Fleet-wide counts here
	// would let sibling profiles' in-flight clones bleed into this
	// profile's headroom — under heavy multi-profile load that produces
	// "X under-dispatched because Y's clones were still landing" misses
	// that converge after a tick but break single-pass test setups.
	profileRows, err := m.store.ListByProfile(profile)
	if err != nil {
		m.log.Warn("reconcile: list profile rows failed", "profile", profile, "err", err)
		return
	}
	hotProv, warmProv := 0, 0
	for _, r := range profileRows {
		if r.State != store.StateProvisioning {
			continue
		}
		switch r.PoolKind {
		case store.PoolKindHot:
			hotProv++
		case store.PoolKindWarm:
			warmProv++
		}
	}
	// Two reasons to clone a hot VM:
	//   (a) Eager replacement: keep `available >= HotSize` so consuming
	//       a hot VM (Assigned) immediately triggers a refill clone.
	//   (b) Burst response: when GitHub's desiredCount exceeds the
	//       current in-flight runner count, scale up immediately.
	// Effective need is the larger of the two.
	//
	// We add Provisioner.InFlightCloneCount() so a tick sees in-flight
	// clones from PREVIOUS ticks whose store rows haven't landed yet
	// (the gap between PVE qmclone returning and the manager's
	// store.Insert call). Without this, two consecutive ticks each
	// see an empty store and each spawn HotSize clones — producing
	// the "current_hot=4 target=3" over-provision the production
	// reproducer captured.
	// Provisioner.InFlightCloneCount() is fleet-wide; using it for a
	// single profile's headroom over-counts when other profiles are
	// cloning concurrently, but only in the safe direction (we under-
	// dispatch this profile's needed clones for one tick).
	inflight := m.prov.InFlightCloneCount()
	available := stats.Available() + hotProv + inflight
	busy := stats.Busy()
	desired := int(ps.desiredCount.Load())

	hotSize := int(ps.hotSize.Load())
	warmSize := int(ps.warmSize.Load())
	profileMax := ps.settings.MaxConcurrentRunners

	needIdle := hotSize - available
	needBurst := desired - (available + busy)
	needHot := needIdle
	if needBurst > needHot {
		needHot = needBurst
	}
	if needHot < 0 {
		needHot = 0
	}
	// Cap by remaining room under this profile's MaxConcurrentRunners.
	if room := profileMax - (available + busy); room < needHot {
		needHot = room
	}
	if needHot < 0 {
		needHot = 0
	}

	warmInflight := stats.LiveWarm() + warmProv
	needWarm := warmSize - warmInflight
	if needWarm < 0 {
		needWarm = 0
	}

	// Promote warm -> hot first (cheap).
	promoteCount := needHot
	if promoteCount > stats.Warm {
		promoteCount = stats.Warm
	}
	if promoteCount > 0 {
		m.promoteN(ctx, profile, promoteCount)
		needHot -= promoteCount
	}

	// Clone whatever's left.
	for range needHot {
		m.kickClone(ctx, profile, store.PoolKindHot, true)
	}
	for range needWarm {
		m.kickClone(ctx, profile, store.PoolKindWarm, false)
	}

	// Shrink-to-floor: when the hot pool has grown beyond what we
	// need (typically after a burst completes and demand collapses
	// back to 0), destroy the excess. Per-profile scope so a quiet
	// profile shrinks even while a busy sibling stays at cap.
	hotTarget := hotSize
	if burstTarget := desired - busy; burstTarget > hotTarget {
		hotTarget = burstTarget
	}
	freshStats, statsErr := m.statsForProfile(profile)
	if statsErr != nil {
		m.log.Warn("reconcile: re-stats failed; skipping shrink this tick", "profile", profile, "err", statsErr)
	} else if freshStats.Hot > hotTarget {
		excess := freshStats.Hot - hotTarget
		hotRows, err := m.store.ListByProfileAndStates(profile, store.StateHot)
		if err == nil {
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
					"vmid", row.VMID, "profile", profile, "hot_size", hotSize, "target", hotTarget, "current_hot", freshStats.Hot)
				m.destroyAsync(row.VMID, row.Node, profile)
				killed++
			}
		}
	}

	// Stuck-state sweep is fleet-wide (transient-state issues aren't
	// profile-specific). Only one profile's loop needs to run it
	// each tick — pin it to the default profile to avoid N parallel
	// sweeps doing the same work.
	if profile == m.defaultProfile() {
		m.sweepStuckRows()
	}

	// VM-max-age recycle: destroy idle Hot/Warm VMs in THIS profile
	// older than the profile's max-age. Per-profile because the
	// profile setting can override the global default.
	maxAge := ps.settings.VMMaxAge
	if maxAge > 0 {
		cutoff := time.Now().Add(-maxAge)
		olds, err := m.store.ListByProfileAndStates(profile, store.StateHot, store.StateWarm)
		if err == nil {
			for _, o := range olds {
				if !o.CreatedAt.Before(cutoff) {
					continue
				}
				m.log.Info("recycle: vm exceeded max age",
					"vmid", o.VMID, "profile", profile, "age", time.Since(o.CreatedAt))
				if _, err := m.store.Update(o.VMID, func(v *store.VM) {
					v.State = store.StateDraining
					v.StateSince = time.Now()
				}); err == nil {
					m.destroyAsync(o.VMID, o.Node, profile)
				}
			}
		}
	}
}

// sweepStuckRows is the fleet-wide transient-state sweep extracted
// from the old reconcileOnce so a single profile's loop can run it
// without forcing N concurrent sweeps in multi-profile configs.
func (m *manager) sweepStuckRows() {
	const stuckGrace = 5 * time.Minute
	stuckCutoff := time.Now().Add(-stuckGrace)
	stuckCandidates, err := m.store.ListByState(
		store.StateProvisioning, store.StateBooting,
		store.StateDraining, store.StateDestroying,
	)
	if err != nil {
		return
	}
	for _, s := range stuckCandidates {
		if !s.UpdatedAt.Before(stuckCutoff) {
			continue
		}
		m.log.Warn("sweep: row stuck in transient state; re-queueing for destroy",
			"vmid", s.VMID, "state", s.State, "age", time.Since(s.UpdatedAt))
		if _, err := m.store.Update(s.VMID, func(v *store.VM) {
			v.State = store.StateDraining
			v.StateSince = time.Now()
		}); err == nil {
			m.destroyAsync(s.VMID, s.Node, s.Profile)
		}
	}
}

// promoteN moves up to n Warm VMs in the given profile to Booting
// and kicks Start+WaitReady in the background for each. Oldest-Warm-
// first so warm VMs near max-age recycle get used before the
// recycler reaps them. An empty profile name selects warms across
// every profile (used only by legacy code paths in tests).
func (m *manager) promoteN(_ context.Context, profile string, n int) {
	var warms []*store.VM
	var err error
	if profile == "" {
		warms, err = m.store.ListByState(store.StateWarm)
	} else {
		warms, err = m.store.ListByProfileAndStates(profile, store.StateWarm)
	}
	if err != nil {
		m.log.Warn("promote: list warm failed", "profile", profile, "err", err)
		return
	}
	sort.Slice(warms, func(i, j int) bool { return warms[i].CreatedAt.Before(warms[j].CreatedAt) })
	if n < len(warms) {
		warms = warms[:n]
	}
	for _, w := range warms {
		// Reserve the boot-concurrency slot BEFORE flipping the row to
		// Booting. TryAcquire is non-blocking so promoteN (called from
		// reconcileOnce) doesn't stall the whole reconcile pass when the
		// semaphore is saturated. If we can't reserve, leave the row at
		// Warm — the next reconcile tick will retry. Previously the
		// acquire was inside the goroutine and on cancellation we
		// rolled the CAS back, which left a brief window where the row
		// was visibly Booting (and PoolKindHot) and counted toward
		// Available — under-provisioning by one for that tick.
		if !m.bootSem.TryAcquire(1) {
			m.log.Debug("promote: bootSem saturated, deferring to next tick", "vmid", w.VMID)
			continue
		}
		// CAS warm -> booting; if lost, release the slot we just took.
		ok, err := m.store.UpdateState(w.VMID, store.StateWarm, store.StateBooting, func(v *store.VM) {
			v.PoolKind = store.PoolKindHot // promoted to hot budget
		})
		if err != nil || !ok {
			m.bootSem.Release(1)
			continue
		}
		row := w
		m.wg.Add(1)
		// runBoot derives its own context from m.workerCtx; the caller's
		// ctx is for the reconcile tick, not the boot lifetime.
		go func() { //nolint:contextcheck // boot outlives the reconcile-tick ctx
			defer m.wg.Done()
			defer func() { m.logRecoveredPanic("promote", row.VMID, recover()) }()
			defer m.bootSem.Release(1)
			m.runBoot(row)
		}()
	}
}

// kickClone dispatches a single async clone operation, bounded by
// the concurrency semaphore. If the semaphore can't be acquired (ctx
// done during shutdown, or burst already saturated) the spawn is
// dropped — the next reconcile pass will retry.
//
// The deferred panic-recover closure captures vmid by reference so a
// panic that fires AFTER allocateVMID succeeded logs the real vmid,
// not the goroutine-entry value of 0.
//
// profile names the runner profile the clone is being made for; the
// name is stamped on the store row and threaded into CloneOptions so
// the provisioner can apply per-profile tags and hardware overrides.
// An empty profile name selects the default profile.
func (m *manager) kickClone(ctx context.Context, profile string, kind store.PoolKind, poweredOn bool) {
	if err := m.cloneSem.Acquire(ctx, 1); err != nil {
		m.log.Debug("clone: dropping spawn (semaphore unavailable)", "profile", profile, "kind", kind, "err", err)
		return
	}
	m.wg.Add(1)
	var vmid int
	// runClone derives its own context from m.workerCtx; the caller's
	// ctx scopes the reconcile pass, not the clone lifetime.
	go func() { //nolint:contextcheck // clone outlives the reconcile-tick ctx
		defer m.wg.Done()
		defer func() { m.logRecoveredPanic("clone", vmid, recover()) }()
		defer m.cloneSem.Release(1)
		m.runClone(profile, kind, poweredOn, &vmid)
	}()
}

// runClone is the body of an async clone goroutine. The caller
// passes a *int that runClone writes the allocated vmid into as soon
// as allocation succeeds — so the surrounding goroutine's panic-
// recover closure can log the real vmid if a panic fires later in
// the body.
//
// profile selects the per-profile hardware shape and labels the
// clone with the matching tag. An empty profile name maps to the
// manager's default profile.
func (m *manager) runClone(profile string, kind store.PoolKind, poweredOn bool, vmidRef *int) {
	ps := m.profileOf(profile)
	if ps == nil {
		m.log.Warn("clone: unknown profile; aborting", "profile", profile)
		return
	}
	profileName := ps.settings.Name
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

	// Build the Hint with profile + existing-row context so the
	// affinity wrapper (issue #8) can apply prefer_nodes /
	// anti_affinity_with rules. The non-affinity selectors ignore
	// these fields; the snapshot is small (bounded by
	// MaxConcurrentRunners) so building it on every clone is cheap.
	hint := nodeselector.Hint{
		Profile:     profileName,
		ExistingVMs: m.snapshotExistingVMsForAffinity(),
	}
	node, err := m.sel.Select(ctx, hint)
	if err != nil {
		m.log.Warn("clone: node selection failed", "profile", profileName, "err", err)
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
	// Canary template selection. Pre-computed before the row
	// Insert so the Template field is set in one atomic write —
	// avoiding a second Update between Insert and Clone that
	// could race with reconcile-loop accounting.
	templateVMID := ps.settings.TemplateVMID
	templateClass := string(canary.Stable)
	if m.cfg.Canary != nil {
		if pr, perr := m.cfg.Canary.Pick(profileName, vmid); perr == nil {
			if pr.TemplateVMID > 0 {
				templateVMID = pr.TemplateVMID
			}
			templateClass = string(pr.Template)
		}
	}
	row := &store.VM{
		VMID:     vmid,
		Node:     node,
		Name:     name,
		Profile:  profileName,
		Template: templateClass,
		PoolKind: kind,
		State:    store.StateProvisioning,
	}
	if err := m.store.Insert(row); err != nil {
		m.allocMu.Unlock()
		m.log.Warn("clone: create row failed", "vmid", vmid, "err", err)
		return
	}
	m.allocMu.Unlock()

	span.SetAttributes(
		attribute.Int("vm.id", vmid),
		attribute.String("vm.node", node),
		attribute.String("pool.profile", profileName),
	)

	// Allocate a static IP (if the profile's IPAM is non-noop)
	// BEFORE the Clone call so the IPConfig can be stamped into
	// the same Config request the provisioner makes for tags +
	// hardware. Allocation failures fail the clone — the
	// orchestrator's "skip and let the next reconcile tick retry"
	// behaviour applies via the standard runClone exit path.
	allocator := ps.settings.IPAM
	if allocator == nil {
		allocator = ipam.Noop{}
	}
	ipAssignment, err := allocator.Allocate(ctx, vmid)
	if err != nil {
		m.log.Warn("clone: ipam allocate failed", "vmid", vmid, "profile", profileName, "err", err)
		// Treat as a clone failure — destroy the freshly-inserted
		// row so the next reconcile pass can retry.
		if _, updErr := m.store.Update(vmid, func(v *store.VM) {
			v.State = store.StateDestroying
			v.StateSince = time.Now()
		}); updErr != nil {
			m.log.Warn("clone: ipam-fail mark destroying failed", "vmid", vmid, "err", updErr)
		}
		m.destroyOrSyncFallback(vmid, node, profileName)
		return
	}
	ipConfig := ""
	if ipAssignment != "" {
		ipConfig = "ip=" + ipAssignment
	}

	cloneStart := time.Now()
	pv, err := m.prov.Clone(ctx, provisioner.CloneOptions{
		NewVMID:       vmid,
		Node:          node,
		Name:          name,
		Linked:        m.cfg.LinkedClones,
		PoweredOn:     poweredOn,
		Profile:       profileName,
		TemplateVMID:  templateVMID,
		TemplateClass: templateClass,
		CPUCores:      ps.settings.CPUCores,
		MemoryMB:      ps.settings.MemoryMB,
		DiskGB:        ps.settings.DiskGB,
		Storage:       ps.settings.Storage,
		NICs:          ps.settings.NICs,
		IPConfig:      ipConfig,
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "clone failed")
		m.log.Warn("clone: provisioner failed", "vmid", vmid, "err", err)
		if m.metrics != nil {
			m.metrics.VMsTotal.WithLabelValues(m.cfg.ScaleSetName, profileName, "clone-failed").Inc()
		}
		// Mark the row destroying and let the destroy path clean up.
		if _, updErr := m.store.Update(vmid, func(v *store.VM) {
			v.State = store.StateDestroying
			v.StateSince = time.Now()
		}); updErr != nil {
			m.log.Warn("clone: mark destroying failed", "vmid", vmid, "err", updErr)
		}
		m.destroyOrSyncFallback(vmid, node, profileName)
		return
	}
	if m.metrics != nil {
		m.metrics.CloneDuration.WithLabelValues(m.cfg.ScaleSetName, profileName, fmt.Sprintf("%t", m.cfg.LinkedClones), node).Observe(time.Since(cloneStart).Seconds())
		m.metrics.VMsTotal.WithLabelValues(m.cfg.ScaleSetName, profileName, "clone-success").Inc()
	}
	// Feed the canary controller's clone counter (only the
	// candidate class actually counts; the controller ignores
	// stable for failure-rate purposes).
	if m.cfg.Canary != nil {
		m.cfg.Canary.RecordClone(profileName, canary.Template(templateClass))
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
		if errors.Is(err, store.ErrNotFound) {
			// Row was deleted while Clone was in flight — typically by
			// admin ForceDestroy or a stuck-state sweep that didn't see
			// the in-flight clone. The Proxmox VM is real; without an
			// immediate destroy it lives until sweepProxmoxOrphans picks
			// it up (OrphanGrace + reconcile tick). Destroy it now.
			m.log.Info("clone: row deleted mid-clone, destroying orphan vm",
				"vmid", vmid, "node", node)
			m.destroyOrSyncFallback(vmid, node, profileName)
			return
		}
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
	pv := &provisioner.VM{VMID: row.VMID, Node: row.Node, Name: row.Name, Profile: row.Profile}
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
		m.metrics.BootDuration.WithLabelValues(m.cfg.ScaleSetName, row.Profile, row.Node).Observe(time.Since(bootStart).Seconds())
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
// destruction so the next reconcile can clone a fresh one. The
// threshold is read from the row's profile so per-profile overrides
// (e.g. a flaky GPU image that needs 5 retries) actually take effect;
// rows whose profile no longer exists in config fall back to the
// fleet-wide cfg value.
func (m *manager) markPoisonOrDestroy(row *store.VM) {
	updated, err := m.store.Update(row.VMID, func(v *store.VM) {
		v.BootAttempts++
	})
	if err != nil {
		m.log.Warn("poison: inc attempts failed", "vmid", row.VMID, "profile", row.Profile, "err", err)
		return
	}
	// Feed the canary controller's failure counter — only fires
	// when Template was stamped as "candidate" at clone time.
	// When the cumulative rate trips the operator's threshold the
	// controller returns true and we emit the
	// scaleset_canary_reverted_total counter.
	if m.cfg.Canary != nil && row.Template != "" {
		if reverted := m.cfg.Canary.RecordFailure(row.Profile, canary.Template(row.Template)); reverted {
			m.log.Warn("canary: auto-reverted percent to 0; investigate canary template",
				"profile", row.Profile, "vmid", row.VMID)
			if m.metrics != nil {
				m.metrics.CanaryReverts.WithLabelValues(m.cfg.ScaleSetName, row.Profile).Inc()
			}
		}
	}
	threshold := m.cfg.BootMaxAttempts
	if ps := m.profileOf(row.Profile); ps != nil {
		threshold = ps.settings.BootMaxAttempts
	}
	if updated.BootAttempts >= threshold {
		if _, updErr := m.store.Update(row.VMID, func(v *store.VM) {
			v.State = store.StatePoison
			v.StateSince = time.Now()
		}); updErr != nil {
			m.log.Warn("poison: mark poison failed", "vmid", row.VMID, "profile", row.Profile, "err", updErr)
		}
		// Page-worthy: a VM has exhausted its boot-retry budget and is
		// quarantined until an operator triages the underlying failure
		// (template image regression, networking misconfig, etc.).
		m.log.Error("vm marked poison; manual intervention required",
			"vmid", row.VMID, "profile", row.Profile, "attempts", updated.BootAttempts)
		return
	}
	if _, updErr := m.store.Update(row.VMID, func(v *store.VM) {
		v.State = store.StateDestroying
		v.StateSince = time.Now()
	}); updErr != nil {
		m.log.Warn("poison: mark destroying failed", "vmid", row.VMID, "profile", row.Profile, "err", updErr)
	}
	m.destroyAsync(updated.VMID, updated.Node, updated.Profile)
}

// destroyRequest is the unit of work the destroy dispatcher consumes.
// profile is the row's Profile (or empty, which the backlog-full metric
// and the destroyer's downstream emit both clamp to defaultProfileName).
type destroyRequest struct {
	vmid    int
	node    string
	profile string
}

// destroyAsync enqueues a destruction. A single dispatcher goroutine
// (destroyDispatcher) reads from the queue, acquires destroySem, and
// spawns the actual destroy worker — so the goroutine backlog is bounded
// by destroyQueueCap rather than by caller burst.
//
// On a full queue the request is dropped: the row remains in a
// transient state (caller has already transitioned to Draining /
// Destroying) and the stuck-state sweep re-enqueues it on the next
// reconcile pass. PoolDestroyBacklogFull surfaces the overload.
//
// m.wg is incremented up-front so drain() waits for queued-but-not-yet-
// processed work in addition to in-flight destroys.
func (m *manager) destroyAsync(vmid int, node, profile string) {
	m.wg.Add(1)
	select {
	case m.destroyQueue <- destroyRequest{vmid: vmid, node: node, profile: profile}:
		if m.metrics != nil {
			m.metrics.PoolDestroyBacklogDepth.WithLabelValues(m.cfg.ScaleSetName).Set(float64(len(m.destroyQueue)))
		}
	default:
		m.wg.Done()
		label := profile
		if label == "" {
			label = defaultProfileName
		}
		if m.metrics != nil {
			m.metrics.PoolDestroyBacklogFull.WithLabelValues(m.cfg.ScaleSetName, label).Inc()
		}
		m.log.Warn("destroy: backlog full; dropping request, sweep will re-enqueue",
			"vmid", vmid, "profile", label, "cap", cap(m.destroyQueue))
	}
}

// destroyDispatcher is the single consumer of destroyQueue. It serialises
// destroySem acquisition so the goroutine count is bounded by the sem
// (active destroys) plus the queue depth (waiting destroys), never by
// caller burst.
//
// On workerCtx cancellation it drains any queued requests so the
// matching m.wg counters held by destroyAsync get released — without
// that, drain() would block on m.wg.Wait() for the lifetime of any
// in-flight queue items even though no worker will ever pick them up.
func (m *manager) destroyDispatcher() {
	defer func() { m.logRecoveredPanic("destroyDispatcher", 0, recover()) }()
	for {
		select {
		case req := <-m.destroyQueue:
			if m.metrics != nil {
				m.metrics.PoolDestroyBacklogDepth.WithLabelValues(m.cfg.ScaleSetName).Set(float64(len(m.destroyQueue)))
			}
			if err := m.destroySem.Acquire(m.workerCtx, 1); err != nil {
				m.log.Debug("destroy: cancelled before sem acquired", "vmid", req.vmid, "err", err)
				m.wg.Done()
				m.drainDestroyQueueLocked()
				return
			}
			go func(req destroyRequest) {
				defer m.wg.Done()
				defer m.destroySem.Release(1)
				defer func() { m.logRecoveredPanic("destroyWorker", req.vmid, recover()) }()
				m.destroy(m.workerCtx, req.vmid, req.node)
			}(req)
		case <-m.workerCtx.Done():
			m.drainDestroyQueueLocked()
			return
		}
	}
}

// drainDestroyQueueLocked releases the wg counter for every queued-
// but-not-yet-dispatched request. Only safe to call from the dispatcher
// itself (no other goroutine reads from destroyQueue), since multiple
// concurrent drainers would race over the per-request counter.
func (m *manager) drainDestroyQueueLocked() {
	for {
		select {
		case <-m.destroyQueue:
			m.wg.Done()
		default:
			if m.metrics != nil {
				m.metrics.PoolDestroyBacklogDepth.WithLabelValues(m.cfg.ScaleSetName).Set(0)
			}
			return
		}
	}
}

// destroyOrSyncFallback is the clone-failure destroy path. The async
// route used by every other site bails out fast if m.workerCtx is
// already cancelled (the destroyAsync goroutine's destroySem.Acquire
// returns immediately on a cancelled ctx), which leaks the just-cloned
// VM during a hard drain. The clone-fail case is the only one where
// the VM was created by THIS goroutine in THIS tick — if we don't
// destroy it now, no other code path will. So when we observe
// workerCtx already cancelled, fall back to a synchronous destroy
// against a fresh background context with a short cap, accepting the
// extra drain wait against a guaranteed-leak.
func (m *manager) destroyOrSyncFallback(vmid int, node, profile string) {
	if m.workerCtx.Err() == nil {
		m.destroyAsync(vmid, node, profile)
		return
	}
	m.log.Warn("clone-fail destroy: workerCtx already cancelled; running synchronous fallback",
		"vmid", vmid, "node", node)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := m.prov.Destroy(ctx, &provisioner.VM{VMID: vmid, Node: node}); err != nil {
		m.log.Warn("clone-fail destroy: synchronous fallback failed; vm may leak", "vmid", vmid, "err", err)
		return
	}
	if err := m.store.Delete(vmid); err != nil {
		m.log.Warn("clone-fail destroy: row delete failed after sync destroy", "vmid", vmid, "err", err)
	}
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

	if err := m.prov.Destroy(dctx, &provisioner.VM{VMID: vmid, Node: node}); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "destroy failed")
		m.log.Warn("destroy: provisioner failed", "vmid", vmid, "err", err)
		return
	}
	// Delete the row and capture its runner_id in the same write txn.
	// A separate Get-then-Delete would race with concurrent
	// SetRunnerID writes that land during prov.Destroy: a stale read
	// here would call OnRunnerOrphaned with the wrong (or zero) id
	// and the registration would leak.
	deleted, err := m.store.DeleteAndReturn(vmid)
	if err != nil {
		m.log.Warn("destroy: delete row failed", "vmid", vmid, "err", err)
	}
	var runnerID int64
	destroyedProfile := defaultProfileName
	if deleted != nil {
		runnerID = deleted.RunnerID
		if deleted.Profile != "" {
			destroyedProfile = deleted.Profile
		}
	}
	if m.metrics != nil {
		m.metrics.VMsTotal.WithLabelValues(m.cfg.ScaleSetName, destroyedProfile, "destroyed").Inc()
	}

	// Release the IPAM allocation BEFORE the runner-orphan
	// cleanup so a slow GitHub round-trip doesn't hold the IP.
	// Best-effort — a release failure is logged but doesn't
	// block destroy completion; the orphan-IP gets reclaimed by
	// the operator's IPAM (or the static allocator's next
	// process restart).
	if ps := m.profileOf(destroyedProfile); ps != nil && ps.settings.IPAM != nil {
		// Use a fresh context: dctx may have been cancelled by
		// drain. The release is idempotent so re-running on the
		// next reconcile pass is safe, but losing the release
		// here leaks the IP until process restart.
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := ps.settings.IPAM.Release(releaseCtx, vmid); err != nil { //nolint:contextcheck // deliberately detached; see comment above
			m.log.Warn("destroy: ipam release failed", "vmid", vmid, "profile", destroyedProfile, "err", err)
		}
		releaseCancel()
	}

	if runnerID != 0 && m.cfg.OnRunnerOrphaned != nil {
		// Detach from dctx (which is derived from the worker/drain ctx)
		// so a force-drain cancellation that arrives after Proxmox
		// destroy succeeded doesn't abort the idempotent GitHub
		// deregister already in flight and leak the registration.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), orphanCleanupTimeout)
		err := m.cfg.OnRunnerOrphaned(cleanupCtx, runnerID) //nolint:contextcheck // deliberately detached; see comment above
		cleanupCancel()
		if err != nil {
			m.log.Warn("destroy: orphan-runner cleanup failed", "vmid", vmid, "runner_id", runnerID, "err", err)
		} else {
			m.log.Debug("destroy: deregistered github runner", "vmid", vmid, "runner_id", runnerID)
		}
	}
}

// orphanCleanupTimeout bounds the detached GitHub-deregister call made
// from destroy(). It must outlive a typical GH API round-trip while
// still being short enough to not pin process shutdown. Tests may
// override this.
var orphanCleanupTimeout = 15 * time.Second

// allocateVMID returns the lowest VMID in the configured range that is
// not already claimed by an existing row AND has not been destroyed
// within VMIDReuseCooldown. The cooldown check protects against a
// fresh clone colliding with PVE's still-settling qmdestroy task on
// the same ID (manifests as "VM N is running - destroy failed" and
// lock-file timeouts).
//
// Honours ctx — a cancelled drain (or any other ctx cancellation) is
// observed at the start of each iteration so a wide range with many
// recently-destroyed VMIDs can't pin a draining caller.
func (m *manager) allocateVMID(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	used, err := m.store.UsedVMIDs(m.cfg.VMIDRange.Min, m.cfg.VMIDRange.Max)
	if err != nil {
		return 0, err
	}
	for id := m.cfg.VMIDRange.Min; id <= m.cfg.VMIDRange.Max; id++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if _, taken := used[id]; taken {
			continue
		}
		if m.prov.IsRecentlyDestroyed(id, m.cfg.VMIDReuseCooldown) {
			continue
		}
		return id, nil
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
