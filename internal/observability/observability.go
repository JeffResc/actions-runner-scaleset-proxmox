// Package observability bundles the orchestrator's cross-cutting concerns:
// structured logging via log/slog, Prometheus metrics, and HTTP health
// probes. It is intentionally small — most of it is library glue.
package observability

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

// NewLogger returns a slog.Logger configured per the config strings.
// format is one of "json" or "text"; level is one of "debug", "info",
// "warn", or "error".
func NewLogger(level, format string) (*slog.Logger, error) {
	return NewLoggerTo(os.Stderr, level, format)
}

// NewLoggerTo writes to an explicit destination. Useful for tests.
func NewLoggerTo(w io.Writer, level, format string) (*slog.Logger, error) {
	lvl, err := parseLevel(level)
	if err != nil {
		return nil, err
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	switch strings.ToLower(format) {
	case "", "json":
		h = slog.NewJSONHandler(w, opts)
	case "text":
		h = slog.NewTextHandler(w, opts)
	default:
		return nil, fmt.Errorf("observability: unknown log format %q (expected json or text)", format)
	}
	return slog.New(h), nil
}

func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("observability: unknown log level %q", s)
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

// Metrics is the orchestrator's Prometheus instrument set. Construct via
// [NewMetrics] which registers all instruments on a provided registry.
//
// Every per-scaleset metric carries `scaleset` as the FIRST
// label (issue #1) so dashboards can slice cleanly when one
// binary hosts multiple scale sets. Fleet-wide instruments
// (Leader) keep their original signature.
type Metrics struct {
	PoolSize             *prometheus.GaugeVec
	VMsTotal             *prometheus.CounterVec
	CloneDuration        *prometheus.HistogramVec
	BootDuration         *prometheus.HistogramVec
	AcquireDuration      *prometheus.HistogramVec
	ProxmoxErrors        *prometheus.CounterVec
	GitHubErrors         *prometheus.CounterVec
	ListenerMessages     *prometheus.CounterVec
	ReconcileDuration    *prometheus.HistogramVec
	AtCapacityTotal      *prometheus.CounterVec
	GHAPICalls           *prometheus.CounterVec
	GHRateLimitRemaining *prometheus.GaugeVec
	GHStateMismatch      *prometheus.CounterVec
	RunnerHookEvents     *prometheus.CounterVec
	ReconcileErrors      *prometheus.CounterVec
	UnroutedJobs         *prometheus.CounterVec
	QuotaThrottled       *prometheus.CounterVec
	PriorityAcquires     *prometheus.CounterVec
	Preemptions          *prometheus.CounterVec
	CanaryReverts        *prometheus.CounterVec
	ScheduleFires        *prometheus.CounterVec
	ScheduleActive       *prometheus.GaugeVec
	Leader               prometheus.Gauge

	// PoolDestroyBacklogFull counts destroy requests dropped because the
	// pool's destroy dispatcher queue was at capacity. Each drop is
	// recoverable — the next reconcile pass re-finds the row in a
	// transient state and re-enqueues — but a non-zero rate is a signal
	// that destroy throughput is the bottleneck.
	PoolDestroyBacklogFull *prometheus.CounterVec
	// PoolDestroyBacklogDepth tracks the live depth of the destroy
	// dispatcher queue. The bound is 2 * the destroy semaphore cap.
	PoolDestroyBacklogDepth *prometheus.GaugeVec
}

// ProxmoxOpUnknown is the fallback for off-list values of the
// ProxmoxErrors.operation label. Adding a new value to validProxmoxOps
// (rather than passing a fresh string at the call site) is
// intentional friction — every new entry adds a Prometheus time
// series, and RecordProxmoxError substitutes ProxmoxOpUnknown for
// anything off-list so a typo or new caller never blows up
// cardinality silently.
const ProxmoxOpUnknown = "unknown"

var validProxmoxOps = map[string]struct{}{
	"inject_jit":     {},
	"clone":          {},
	"destroy":        {},
	"start":          {},
	"stop":           {},
	"power_state":    {},
	"list_vms":       {},
	"wait_ready":     {},
	ProxmoxOpUnknown: {},
}

// GHStateUnknown is the fallback for off-list values of the
// GHStateMismatch.gh_state label. Kept in sync with gh.ghStateLabel —
// every value that function can emit must appear in validGHStates.
const GHStateUnknown = "unknown"

var validGHStates = map[string]struct{}{
	"missing":      {},
	"offline":      {},
	"busy":         {},
	"idle":         {},
	GHStateUnknown: {},
}

// GHActionUnknown is the fallback for off-list values of the
// GHStateMismatch.action label. Same closed-enum guarantee as
// validGHStates.
const GHActionUnknown = "unknown"

var validGHActions = map[string]struct{}{
	"promote_running": {},
	"destroy":         {},
	GHActionUnknown:   {},
}

// RecordProxmoxError increments ProxmoxErrors with the op clamped to
// validProxmoxOps. Off-list values become "unknown" so a future
// caller passing an unbounded string (per-VMID, per-task-id, etc.)
// can't blow up Prometheus cardinality silently. Returns the op the
// counter was incremented with so callers (typically tests) can
// observe the substitution.
func (m *Metrics) RecordProxmoxError(scaleset, op, node string) string {
	if m == nil {
		return op
	}
	if _, ok := validProxmoxOps[op]; !ok {
		op = ProxmoxOpUnknown
	}
	m.ProxmoxErrors.WithLabelValues(scaleset, op, node).Inc()
	return op
}

// RecordGHStateMismatch increments GHStateMismatch with ghState and
// action clamped to their respective closed enums. db_state is
// expected to be a store.State value; we leave it unchanged because
// store.State is a typed enum at the call site (no risk of unbounded
// input). Off-list ghState / action become "unknown".
func (m *Metrics) RecordGHStateMismatch(scaleset, dbState, ghState, action string) (string, string) {
	if m == nil {
		return ghState, action
	}
	if _, ok := validGHStates[ghState]; !ok {
		ghState = GHStateUnknown
	}
	if _, ok := validGHActions[action]; !ok {
		action = GHActionUnknown
	}
	m.GHStateMismatch.WithLabelValues(scaleset, dbState, ghState, action).Inc()
	return ghState, action
}

// NewMetrics creates and registers the orchestrator's metric set on reg.
// Panics on duplicate registration; pass a fresh registry per process.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	const ns = "scaleset"
	m := &Metrics{
		// Every per-scaleset metric carries `scaleset` as the
		// FIRST label so dashboards can slice cleanly when one
		// binary hosts multiple scale sets (issue #1). Profile-
		// keyed metrics carry `profile` next.
		PoolSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "pool_size",
			Help: "Number of VMs in each lifecycle state, by scaleset and profile.",
		}, []string{"scaleset", "profile", "state"}),
		VMsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "vms_total",
			Help: "Cumulative count of VMs created, partitioned by scaleset, profile and outcome.",
		}, []string{"scaleset", "profile", "outcome"}),
		CloneDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "clone_duration_seconds",
			Help:    "Time to clone a VM from template.",
			Buckets: prometheus.ExponentialBuckets(0.5, 2, 12),
		}, []string{"scaleset", "profile", "linked", "node"}),
		BootDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "boot_duration_seconds",
			Help:    "Time from VM start to guest-agent ready.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 11),
		}, []string{"scaleset", "profile", "node"}),
		AcquireDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "acquire_duration_seconds",
			Help:    "Time from Acquire call to a ready VM, by scaleset, profile and source tier.",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
		}, []string{"scaleset", "profile", "tier"}),
		ProxmoxErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "proxmox_api_errors_total",
			Help: "Errors from Proxmox API calls, partitioned by scaleset and operation.",
		}, []string{"scaleset", "operation", "node"}),
		GitHubErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "github_api_errors_total",
			Help: "Errors from GitHub API calls.",
		}, []string{"scaleset", "endpoint"}),
		ListenerMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "listener_messages_total",
			Help: "Inbound listener messages by kind.",
		}, []string{"scaleset", "kind"}),
		ReconcileDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "reconcile_duration_seconds",
			Help:    "Wall-clock time of one pool reconciliation pass.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
		}, []string{"scaleset"}),
		AtCapacityTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "at_capacity_total",
			Help: "Times an Acquire was rejected because the orchestrator was at MaxConcurrentRunners.",
		}, []string{"scaleset"}),
		GHAPICalls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "gh_api_calls_total",
			Help: "Outbound GitHub REST API calls, by scaleset, endpoint group and HTTP status class.",
		}, []string{"scaleset", "endpoint", "status"}),
		GHRateLimitRemaining: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "gh_rate_limit_remaining",
			Help: "Most recent X-RateLimit-Remaining value observed on a GitHub REST response.",
		}, []string{"scaleset"}),
		GHStateMismatch: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "gh_runner_state_mismatch_total",
			Help: "Reconciler events where DB state diverged from GitHub runner state, by action taken.",
		}, []string{"scaleset", "db_state", "gh_state", "action"}),
		RunnerHookEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "runner_hook_events_total",
			Help: "Inbound events from in-VM runner-side lifecycle hooks.",
		}, []string{"scaleset", "phase", "result"}),
		ReconcileErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "reconcile_errors_total",
			Help: "Errors raised inside the gh.Reconciler when applying state transitions, by operation.",
		}, []string{"scaleset", "op"}),
		UnroutedJobs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "unrouted_jobs_total",
			Help: "Jobs whose requested labels did not match any configured runner profile.",
		}, []string{"scaleset", "labels"}),
		QuotaThrottled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "quota_throttled_total",
			Help: "Jobs observed exceeding their configured per-org or per-repo quota.",
		}, []string{"scaleset", "scope", "name"}),
		PriorityAcquires: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "priority_acquires_total",
			Help: "Jobs paired with a runner VM, partitioned by their priority class.",
		}, []string{"scaleset", "class"}),
		Preemptions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "preemptions_total",
			Help: "Assigned VMs preempted to free capacity, partitioned by from_class and to_class.",
		}, []string{"scaleset", "from_class", "to_class"}),
		CanaryReverts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "canary_reverted_total",
			Help: "Canary template rollouts auto-reverted to 0% due to failure rate exceeding threshold.",
		}, []string{"scaleset", "profile"}),
		ScheduleFires: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "schedule_fires_total",
			Help: "Schedule cron fires that applied a pool-size override.",
		}, []string{"scaleset", "profile", "schedule"}),
		ScheduleActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "schedule_active",
			Help: "Active schedule override per profile (1 = currently applying).",
		}, []string{"scaleset", "profile", "schedule"}),
		Leader: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Name: "leader",
			Help: "1 when this replica holds cluster leadership, 0 when standby. Always 1 in standalone mode.",
		}),
		PoolDestroyBacklogFull: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "pool_destroy_backlog_full_total",
			Help: "Destroy requests dropped because the dispatcher queue was at capacity, by scaleset and profile.",
		}, []string{"scaleset", "profile"}),
		PoolDestroyBacklogDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "pool_destroy_backlog_depth",
			Help: "Current depth of the pool destroy dispatcher queue, by scaleset.",
		}, []string{"scaleset"}),
	}
	reg.MustRegister(
		m.PoolSize, m.VMsTotal, m.CloneDuration, m.BootDuration,
		m.AcquireDuration, m.ProxmoxErrors, m.GitHubErrors,
		m.ListenerMessages, m.ReconcileDuration, m.AtCapacityTotal,
		m.GHAPICalls, m.GHRateLimitRemaining, m.GHStateMismatch, m.RunnerHookEvents,
		m.ReconcileErrors, m.UnroutedJobs,
		m.QuotaThrottled, m.PriorityAcquires, m.Preemptions, m.CanaryReverts,
		m.ScheduleFires, m.ScheduleActive,
		m.PoolDestroyBacklogFull, m.PoolDestroyBacklogDepth,
		m.Leader,
	)
	return m
}

// ScopedMetrics binds a scaleset name so callers that observe
// the githubauth.RateLimitObserver contract can record metrics
// against the right scaleset (issue #1). Obtain via
// Metrics.ForScaleset.
type ScopedMetrics struct {
	m        *Metrics
	scaleset string
}

// ForScaleset returns a ScopedMetrics view that pre-binds the
// scaleset label on every per-scaleset metric. With one scope
// per scaleset, callers that don't otherwise know the scaleset
// name (e.g. the githubauth round-tripper) can observe correctly.
func (m *Metrics) ForScaleset(scaleset string) *ScopedMetrics {
	if m == nil {
		return nil
	}
	return &ScopedMetrics{m: m, scaleset: scaleset}
}

// ObserveRateLimit satisfies the githubauth.RateLimitObserver contract.
// Stores the most recent X-RateLimit-Remaining value emitted by GitHub
// for this scope's scaleset.
func (s *ScopedMetrics) ObserveRateLimit(remaining int) {
	if s == nil {
		return
	}
	s.m.GHRateLimitRemaining.WithLabelValues(s.scaleset).Set(float64(remaining))
}

// ObserveCall satisfies the githubauth.RateLimitObserver contract by
// counting GitHub REST calls partitioned by endpoint group and status
// class (2xx/3xx/4xx/5xx/transport_error).
func (s *ScopedMetrics) ObserveCall(endpoint, statusClass string) {
	if s == nil {
		return
	}
	s.m.GHAPICalls.WithLabelValues(s.scaleset, endpoint, statusClass).Inc()
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

// Health tracks readiness state observed across the orchestrator. All
// methods are safe for concurrent use.
//
// In single-process / standalone deployments Leader is set true at
// startup and never changes. In Kubernetes multi-replica deployments
// the cluster.Coordinator drives [Health.MarkLeader] as leadership
// transitions, which in turn shifts /readyz's gate set: standbys are
// ready as long as Proxmox is reachable (so they can take over);
// leaders additionally require every registered scaleset to have its
// per-scaleset listener-connected + recovery-done flags set.
//
// Multi-scaleset (issue #1): the leader plane runs one stack per
// scale set, and any one of them not being ready is enough to keep
// /readyz red. Call RegisterScaleset(name) once per declared scale
// set at startup; the leader's per-scaleset workers call
// MarkScalesetListenerConnected and MarkScalesetRecoveryDone as
// they progress, and ClearScalesetState on deposal so the
// next-elected replica's gates start clean.
type Health struct {
	leader          atomic.Bool
	proxmoxLastSeen atomic.Pointer[time.Time]
	proxmoxMaxStale time.Duration

	mu        sync.RWMutex
	scalesets map[string]*scalesetReady
}

type scalesetReady struct {
	listenerConnected atomic.Bool
	recoveryDone      atomic.Bool
}

// NewHealth returns a Health tracker. proxmoxMaxStale defines how long the
// last successful Proxmox call may be in the past before Ready returns
// false; defaults to 30s if zero.
func NewHealth(proxmoxMaxStale time.Duration) *Health {
	if proxmoxMaxStale == 0 {
		proxmoxMaxStale = 30 * time.Second
	}
	return &Health{proxmoxMaxStale: proxmoxMaxStale}
}

// MarkProxmoxOK records that a Proxmox API call just succeeded.
func (h *Health) MarkProxmoxOK() {
	now := time.Now()
	h.proxmoxLastSeen.Store(&now)
}

// RegisterScaleset records that the named scale set exists. Ready()
// gates leader readiness on every registered scaleset having its
// listener-connected + recovery-done flags set. Idempotent —
// calling twice for the same name is a no-op.
func (h *Health) RegisterScaleset(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.scalesets == nil {
		h.scalesets = make(map[string]*scalesetReady)
	}
	if _, ok := h.scalesets[name]; !ok {
		h.scalesets[name] = &scalesetReady{}
	}
}

// MarkScalesetListenerConnected records that the named scale set's
// listener has accepted at least one message session — proof that
// GitHub credentials and connectivity are working for that scale
// set. Unregistered names are silently ignored.
func (h *Health) MarkScalesetListenerConnected(name string) {
	h.mu.RLock()
	r := h.scalesets[name]
	h.mu.RUnlock()
	if r != nil {
		r.listenerConnected.Store(true)
	}
}

// MarkScalesetRecoveryDone records that the initial crash-recovery
// pass completed for the named scale set.
func (h *Health) MarkScalesetRecoveryDone(name string) {
	h.mu.RLock()
	r := h.scalesets[name]
	h.mu.RUnlock()
	if r != nil {
		r.recoveryDone.Store(true)
	}
}

// ClearScalesetState resets both gates for the named scale set.
// Called on deposal so a freshly demoted replica stops claiming a
// working scaleset session, and the next-elected replica's /readyz
// only flips green after IT has finished its own Recover().
// Unregistered names are silently ignored.
func (h *Health) ClearScalesetState(name string) {
	h.mu.RLock()
	r := h.scalesets[name]
	h.mu.RUnlock()
	if r != nil {
		r.listenerConnected.Store(false)
		r.recoveryDone.Store(false)
	}
}

// MarkLeader records the current leadership state for this replica.
// Pass true when leadership is acquired, false when it is lost.
func (h *Health) MarkLeader(leader bool) { h.leader.Store(leader) }

// Ready reports readiness with leader-aware gates:
//
//   - All replicas require Proxmox reachable within the staleness window
//     (so a standby that has lost Proxmox cannot safely take over).
//   - Leaders additionally require every registered scale set's
//     listener-connected + recovery-done flags to be set.
//
// Standbys whose Proxmox is reachable are ready even though they have
// never connected any scaleset listener — that work is leader-only.
func (h *Health) Ready() bool {
	last := h.proxmoxLastSeen.Load()
	if last == nil {
		return false
	}
	if time.Since(*last) > h.proxmoxMaxStale {
		return false
	}
	if h.leader.Load() {
		h.mu.RLock()
		defer h.mu.RUnlock()
		if len(h.scalesets) == 0 {
			// A leader with no registered scalesets has nothing to
			// be ready about. Defensive — production always
			// registers at least one before promotion.
			return false
		}
		for _, r := range h.scalesets {
			if !r.listenerConnected.Load() || !r.recoveryDone.Load() {
				return false
			}
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// HTTP server
// ---------------------------------------------------------------------------

// Serve runs an HTTP server exposing /metrics, /healthz, and /readyz on
// the configured address until ctx is cancelled. Wraps ServeOn with a
// listener bound to addr.
func Serve(ctx context.Context, addr string, reg *prometheus.Registry, h *Health, log *slog.Logger) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("observability listen %s: %w", addr, err)
	}
	return ServeOn(ctx, ln, reg, h, log)
}

// ServeOn runs the observability HTTP server on a caller-supplied
// listener. Lets tests bind to ":0" and discover the bound address via
// ln.Addr() so they can run in parallel without colliding on a shared
// hardcoded port.
func ServeOn(ctx context.Context, ln net.Listener, reg *prometheus.Registry, h *Health, log *slog.Logger) error {
	r := chi.NewRouter()
	r.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	// Liveness is intentionally "process exists and serves HTTP" —
	// /healthz always returns 200. Readiness ("process can serve real
	// traffic": leader role + Proxmox reachable + recovery done) lives
	// on /readyz. This split is deliberate: a k8s livenessProbe wired
	// to a Proxmox-dependent check would restart every pod during a
	// Proxmox outage, turning a degraded-but-recoverable cluster into a
	// restart storm. Operators who DO want pod restarts on prolonged
	// Proxmox outages should wire that behavior into /readyz +
	// readinessProbe + a separate watchdog, not into /healthz.
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if _, err := io.WriteString(w, "ok"); err != nil {
			log.Debug("healthz write failed", "err", err)
		}
	})
	r.Get("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if h.Ready() {
			if _, err := io.WriteString(w, "ready"); err != nil {
				log.Debug("readyz write failed", "err", err)
			}
			return
		}
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	})

	srv := &http.Server{
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("observability http listening", "addr", ln.Addr().String())
		err := srv.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		// Shutdown intentionally uses a fresh context: ctx is already
		// cancelled, so deriving from it would short-circuit Shutdown
		// before in-flight handlers finish their drain budget.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil { //nolint:contextcheck // see comment above
			return fmt.Errorf("observability shutdown: %w", err)
		}
		return nil
	case err := <-errCh:
		return err
	}
}
