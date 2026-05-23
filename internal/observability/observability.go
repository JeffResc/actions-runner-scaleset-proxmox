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
type Metrics struct {
	PoolSize             *prometheus.GaugeVec
	VMsTotal             *prometheus.CounterVec
	CloneDuration        *prometheus.HistogramVec
	BootDuration         *prometheus.HistogramVec
	AcquireDuration      *prometheus.HistogramVec
	ProxmoxErrors        *prometheus.CounterVec
	GitHubErrors         *prometheus.CounterVec
	ListenerMessages     *prometheus.CounterVec
	ReconcileDuration    prometheus.Histogram
	AtCapacityTotal      prometheus.Counter
	GHAPICalls           *prometheus.CounterVec
	GHRateLimitRemaining prometheus.Gauge
	GHStateMismatch      *prometheus.CounterVec
	RunnerHookEvents     *prometheus.CounterVec
	ReconcileErrors      *prometheus.CounterVec
	Leader               prometheus.Gauge
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
func (m *Metrics) RecordProxmoxError(op, node string) string {
	if m == nil {
		return op
	}
	if _, ok := validProxmoxOps[op]; !ok {
		op = ProxmoxOpUnknown
	}
	m.ProxmoxErrors.WithLabelValues(op, node).Inc()
	return op
}

// RecordGHStateMismatch increments GHStateMismatch with ghState and
// action clamped to their respective closed enums. db_state is
// expected to be a store.State value; we leave it unchanged because
// store.State is a typed enum at the call site (no risk of unbounded
// input). Off-list ghState / action become "unknown".
func (m *Metrics) RecordGHStateMismatch(dbState, ghState, action string) (string, string) {
	if m == nil {
		return ghState, action
	}
	if _, ok := validGHStates[ghState]; !ok {
		ghState = GHStateUnknown
	}
	if _, ok := validGHActions[action]; !ok {
		action = GHActionUnknown
	}
	m.GHStateMismatch.WithLabelValues(dbState, ghState, action).Inc()
	return ghState, action
}

// NewMetrics creates and registers the orchestrator's metric set on reg.
// Panics on duplicate registration; pass a fresh registry per process.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	const ns = "scaleset"
	m := &Metrics{
		PoolSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "pool_size",
			Help: "Number of VMs in each lifecycle state.",
		}, []string{"state"}),
		VMsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "vms_total",
			Help: "Cumulative count of VMs created, partitioned by outcome.",
		}, []string{"outcome"}),
		// CloneDuration label set is intentionally bounded by node count
		// (a handful) × linked-bool (2). Do NOT add pool.kind or vm.id
		// here — pool.kind only adds a fixed factor today but vm.id would
		// produce one series per VM per scrape, blowing up Prometheus
		// cardinality. If you need per-VM clone latency, emit a trace
		// span (we already do — see provisioner.Clone) rather than a
		// metric.
		CloneDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "clone_duration_seconds",
			Help: "Time to clone a VM from template.",
			// 0.5s .. ~1024s — wide enough to keep p99 meaningful when
			// Proxmox is slow. The narrower 9-bucket range used to top
			// out at ~128s, collapsing high-end percentiles into +Inf
			// exactly when operators most needed to see the latency.
			Buckets: prometheus.ExponentialBuckets(0.5, 2, 12),
		}, []string{"linked", "node"}),
		BootDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "boot_duration_seconds",
			Help: "Time from VM start to guest-agent ready.",
			// 1s .. ~1024s — same widening as CloneDuration. Real VM
			// boots can run several minutes when Proxmox is loaded.
			Buckets: prometheus.ExponentialBuckets(1, 2, 11),
		}, []string{"node"}),
		AcquireDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "acquire_duration_seconds",
			Help:    "Time from Acquire call to a ready VM, by source tier.",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
		}, []string{"tier"}),
		ProxmoxErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "proxmox_api_errors_total",
			Help: "Errors from Proxmox API calls, partitioned by operation.",
		}, []string{"operation", "node"}),
		GitHubErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "github_api_errors_total",
			Help: "Errors from GitHub API calls.",
		}, []string{"endpoint"}),
		ListenerMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "listener_messages_total",
			Help: "Inbound listener messages by kind.",
		}, []string{"kind"}),
		ReconcileDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Name: "reconcile_duration_seconds",
			Help:    "Wall-clock time of one pool reconciliation pass.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
		}),
		AtCapacityTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Name: "at_capacity_total",
			Help: "Times an Acquire was rejected because the orchestrator was at MaxConcurrentRunners.",
		}),
		GHAPICalls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "gh_api_calls_total",
			Help: "Outbound GitHub REST API calls, by endpoint group and HTTP status class.",
		}, []string{"endpoint", "status"}),
		GHRateLimitRemaining: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Name: "gh_rate_limit_remaining",
			Help: "Most recent X-RateLimit-Remaining value observed on a GitHub REST response.",
		}),
		GHStateMismatch: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "gh_runner_state_mismatch_total",
			Help: "Reconciler events where DB state diverged from GitHub runner state, by action taken.",
		}, []string{"db_state", "gh_state", "action"}),
		RunnerHookEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "runner_hook_events_total",
			Help: "Inbound events from in-VM runner-side lifecycle hooks.",
		}, []string{"phase", "result"}),
		ReconcileErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "reconcile_errors_total",
			Help: "Errors raised inside the gh.Reconciler when applying state transitions, by operation.",
		}, []string{"op"}),
		Leader: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Name: "leader",
			Help: "1 when this replica holds cluster leadership, 0 when standby. Always 1 in standalone mode.",
		}),
	}
	reg.MustRegister(
		m.PoolSize, m.VMsTotal, m.CloneDuration, m.BootDuration,
		m.AcquireDuration, m.ProxmoxErrors, m.GitHubErrors,
		m.ListenerMessages, m.ReconcileDuration, m.AtCapacityTotal,
		m.GHAPICalls, m.GHRateLimitRemaining, m.GHStateMismatch, m.RunnerHookEvents,
		m.ReconcileErrors, m.Leader,
	)
	return m
}

// ObserveRateLimit satisfies the githubauth.RateLimitObserver contract.
// Stores the most recent X-RateLimit-Remaining value emitted by GitHub.
func (m *Metrics) ObserveRateLimit(remaining int) {
	m.GHRateLimitRemaining.Set(float64(remaining))
}

// ObserveCall satisfies the githubauth.RateLimitObserver contract by
// counting GitHub REST calls partitioned by endpoint group and status
// class (2xx/3xx/4xx/5xx/transport_error).
func (m *Metrics) ObserveCall(endpoint, statusClass string) {
	m.GHAPICalls.WithLabelValues(endpoint, statusClass).Inc()
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
// leaders also need listenerConnected and recoveryDone.
type Health struct {
	listenerConnected atomic.Bool
	recoveryDone      atomic.Bool
	leader            atomic.Bool
	proxmoxLastSeen   atomic.Pointer[time.Time]
	proxmoxMaxStale   time.Duration
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

// MarkListenerConnected records that the scaleset Listener has accepted at
// least one message session — proof that GitHub credentials and
// connectivity are working.
func (h *Health) MarkListenerConnected() { h.listenerConnected.Store(true) }

// MarkRecoveryDone records that the initial crash-recovery pass completed.
func (h *Health) MarkRecoveryDone() { h.recoveryDone.Store(true) }

// ClearListenerConnected resets the listener-connected gate. Called by
// the cluster.Coordinator's OnDeposed so a freshly demoted replica
// stops claiming a working scaleset-listener session.
func (h *Health) ClearListenerConnected() { h.listenerConnected.Store(false) }

// ClearRecoveryDone resets the recovery gate. Called on deposal so the
// next-elected replica's /readyz only flips green after IT has finished
// its own Recover() pass.
func (h *Health) ClearRecoveryDone() { h.recoveryDone.Store(false) }

// MarkLeader records the current leadership state for this replica.
// Pass true when leadership is acquired, false when it is lost.
func (h *Health) MarkLeader(leader bool) { h.leader.Store(leader) }

// Ready reports readiness with leader-aware gates:
//
//   - All replicas require Proxmox reachable within the staleness window
//     (so a standby that has lost Proxmox cannot safely take over).
//   - Leaders additionally require listenerConnected and recoveryDone.
//
// Standbys whose Proxmox is reachable are ready even though they have
// never connected the scaleset listener — that work is leader-only.
func (h *Health) Ready() bool {
	last := h.proxmoxLastSeen.Load()
	if last == nil {
		return false
	}
	if time.Since(*last) > h.proxmoxMaxStale {
		return false
	}
	if h.leader.Load() {
		if !h.listenerConnected.Load() || !h.recoveryDone.Load() {
			return false
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
