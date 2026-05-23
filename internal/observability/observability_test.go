package observability_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/observability"
)

func TestNewLogger_JSONAndText(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	log, err := observability.NewLoggerTo(&buf, "info", "json")
	require.NoError(t, err)
	log.Info("hello", "key", "value")
	require.Contains(t, buf.String(), `"msg":"hello"`)
	require.Contains(t, buf.String(), `"key":"value"`)

	buf.Reset()
	log, err = observability.NewLoggerTo(&buf, "info", "text")
	require.NoError(t, err)
	log.Info("hi", "k", "v")
	require.Contains(t, buf.String(), "msg=hi")
}

func TestNewLogger_LevelFilters(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	log, err := observability.NewLoggerTo(&buf, "error", "json")
	require.NoError(t, err)
	log.Info("info-line")
	log.Error("error-line")
	out := buf.String()
	require.NotContains(t, out, "info-line")
	require.Contains(t, out, "error-line")
}

func TestNewLogger_RejectsUnknownFormat(t *testing.T) {
	t.Parallel()
	_, err := observability.NewLoggerTo(io.Discard, "info", "xml")
	require.Error(t, err)
}

func TestNewLogger_RejectsUnknownLevel(t *testing.T) {
	t.Parallel()
	_, err := observability.NewLoggerTo(io.Discard, "loud", "json")
	require.Error(t, err)
}

func TestNewMetrics_RegistersAll(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := observability.NewMetrics(reg)
	require.NotNil(t, m)

	// Touch each instrument so it shows up in /metrics output.
	m.PoolSize.WithLabelValues("hot").Set(2)
	m.VMsTotal.WithLabelValues("success").Inc()
	m.CloneDuration.WithLabelValues("true", "pve1").Observe(1.5)
	m.BootDuration.WithLabelValues("pve1").Observe(7.0)
	m.AcquireDuration.WithLabelValues("hot").Observe(0.5)
	m.ProxmoxErrors.WithLabelValues("clone", "pve1").Inc()
	m.GitHubErrors.WithLabelValues("list-runners").Inc()
	m.ListenerMessages.WithLabelValues("job_started").Inc()
	m.ReconcileDuration.Observe(0.05)
	m.AtCapacityTotal.Inc()
	m.GHAPICalls.WithLabelValues("runners", "2xx").Inc()
	m.GHRateLimitRemaining.Set(4999)
	m.GHStateMismatch.WithLabelValues("assigned", "missing", "destroy").Inc()
	m.RunnerHookEvents.WithLabelValues("started", "ok").Inc()
	m.Leader.Set(1)

	families, err := reg.Gather()
	require.NoError(t, err)

	names := make(map[string]bool, len(families))
	for _, f := range families {
		names[*f.Name] = true
	}
	for _, want := range []string{
		"scaleset_pool_size",
		"scaleset_vms_total",
		"scaleset_clone_duration_seconds",
		"scaleset_boot_duration_seconds",
		"scaleset_acquire_duration_seconds",
		"scaleset_proxmox_api_errors_total",
		"scaleset_github_api_errors_total",
		"scaleset_listener_messages_total",
		"scaleset_reconcile_duration_seconds",
		"scaleset_at_capacity_total",
		"scaleset_gh_api_calls_total",
		"scaleset_gh_rate_limit_remaining",
		"scaleset_gh_runner_state_mismatch_total",
		"scaleset_runner_hook_events_total",
		"scaleset_leader",
	} {
		require.Truef(t, names[want], "missing metric %s", want)
	}
}

// TestCloneBootBuckets_CoverSlowProxmox locks in the #74 fix: the
// CloneDuration and BootDuration histogram buckets must extend high
// enough that p99 stays meaningful when Proxmox is slow. The previous
// upper bucket was ~128s for CloneDuration and ~256s for BootDuration;
// both collapsed into +Inf on the production failure mode operators
// most needed to dashboard.
func TestCloneBootBuckets_CoverSlowProxmox(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := observability.NewMetrics(reg)

	// Single observation past the old upper bucket but inside the new
	// one. If the buckets are too tight this observation lands in +Inf;
	// if they're wide enough, the largest non-Inf bucket includes it.
	m.CloneDuration.WithLabelValues("true", "pve1").Observe(600)
	m.BootDuration.WithLabelValues("pve1").Observe(600)

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, f := range families {
		name := *f.Name
		if name != "scaleset_clone_duration_seconds" && name != "scaleset_boot_duration_seconds" {
			continue
		}
		require.NotEmpty(t, f.Metric, "no metric for %s", name)
		// Find the largest finite bucket and confirm it covers 600.
		var maxFinite float64
		for _, b := range f.Metric[0].Histogram.Bucket {
			if b.UpperBound != nil && *b.UpperBound > maxFinite {
				maxFinite = *b.UpperBound
			}
		}
		require.Greaterf(t, maxFinite, 600.0,
			"%s upper bucket %g must be > 600 so p99 is meaningful when proxmox is slow",
			name, maxFinite)
	}
}

// TestLeaderGauge_Transitions confirms the gauge flips 0 → 1 → 0 as
// the app's leader callbacks fire. The gauge is the assertable signal
// e2e tests use to identify which replica currently holds the lease.
func TestLeaderGauge_Transitions(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := observability.NewMetrics(reg)

	read := func() float64 {
		families, err := reg.Gather()
		require.NoError(t, err)
		for _, f := range families {
			if *f.Name == "scaleset_leader" {
				return *f.Metric[0].Gauge.Value
			}
		}
		t.Fatal("scaleset_leader metric not found")
		return 0
	}

	require.Equal(t, 0.0, read(), "initial value must be zero (standby)")
	m.Leader.Set(1)
	require.Equal(t, 1.0, read(), "after acquiring leadership")
	m.Leader.Set(0)
	require.Equal(t, 0.0, read(), "after losing leadership")
}

// TestMetrics_RateLimitObserver verifies *Metrics satisfies the
// githubauth.RateLimitObserver contract (a structural interface match —
// the package can't import githubauth without an import cycle, so we
// assert via the actual exported methods).
func TestMetrics_RateLimitObserver(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := observability.NewMetrics(reg)

	m.ObserveRateLimit(1234)
	m.ObserveCall("runners", "5xx")
	m.ObserveCall("jobs", "2xx")

	families, err := reg.Gather()
	require.NoError(t, err)

	got := map[string]float64{}
	for _, f := range families {
		for _, mt := range f.Metric {
			key := *f.Name
			for _, l := range mt.Label {
				key += ":" + *l.Name + "=" + *l.Value
			}
			switch {
			case mt.Counter != nil:
				got[key] = *mt.Counter.Value
			case mt.Gauge != nil:
				got[key] = *mt.Gauge.Value
			}
		}
	}
	require.InDelta(t, 1234.0, got["scaleset_gh_rate_limit_remaining"], 0.001)
	require.InDelta(t, 1.0, got["scaleset_gh_api_calls_total:endpoint=runners:status=5xx"], 0.001)
	require.InDelta(t, 1.0, got["scaleset_gh_api_calls_total:endpoint=jobs:status=2xx"], 0.001)
}

func TestHealth_LeaderTransitionsToReady(t *testing.T) {
	t.Parallel()
	h := observability.NewHealth(time.Minute)
	h.MarkLeader(true)
	require.False(t, h.Ready())

	h.MarkListenerConnected()
	require.False(t, h.Ready())

	h.MarkRecoveryDone()
	require.False(t, h.Ready(), "still missing Proxmox liveness")

	h.MarkProxmoxOK()
	require.True(t, h.Ready())
}

func TestHealth_StaleProxmox(t *testing.T) {
	t.Parallel()
	h := observability.NewHealth(10 * time.Millisecond)
	h.MarkLeader(true)
	h.MarkListenerConnected()
	h.MarkRecoveryDone()
	h.MarkProxmoxOK()
	require.True(t, h.Ready())

	time.Sleep(20 * time.Millisecond)
	require.False(t, h.Ready())
}

// Standby readiness: a non-leader doesn't need listener/recovery — it
// just needs Proxmox reachable so it can take over cleanly. Without
// this, /readyz on a standby pod would flap and the K8s Service would
// drop it from its endpoint pool, defeating the purpose of warm
// standbys.
func TestHealth_StandbyReadyOnProxmoxAlone(t *testing.T) {
	t.Parallel()
	h := observability.NewHealth(time.Minute)
	require.False(t, h.Ready(), "no Proxmox observed yet → not ready")

	h.MarkProxmoxOK()
	require.True(t, h.Ready(), "standby with Proxmox reachable should be ready")
}

// On deposal the listener-connected and recovery-done gates are
// cleared so a freshly demoted replica doesn't carry stale leader-only
// gates into its standby life.
func TestHealth_ClearGatesOnDeposal(t *testing.T) {
	t.Parallel()
	h := observability.NewHealth(time.Minute)
	h.MarkLeader(true)
	h.MarkListenerConnected()
	h.MarkRecoveryDone()
	h.MarkProxmoxOK()
	require.True(t, h.Ready())

	// Simulate cluster.Coordinator's OnDeposed work.
	h.MarkLeader(false)
	h.ClearListenerConnected()
	h.ClearRecoveryDone()
	// Standby readiness only requires Proxmox, which is still fresh.
	require.True(t, h.Ready())

	// Re-elect: leader-only gates must be re-asserted before /readyz
	// flips green again under leader semantics.
	h.MarkLeader(true)
	require.False(t, h.Ready())
}

// TestRecordProxmoxError_ClampsUnknownOp locks in the #72 fix:
// off-list `op` values become "unknown" so a future caller passing
// per-VMID or per-task-id strings can't blow up Prometheus
// cardinality. Returns the substituted value so callers can observe.
func TestRecordProxmoxError_ClampsUnknownOp(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := observability.NewMetrics(reg)

	// Known op passes through.
	require.Equal(t, "inject_jit", m.RecordProxmoxError("inject_jit", "pve1"))

	// Unknown op is clamped to "unknown".
	require.Equal(t, "unknown", m.RecordProxmoxError("destroy-vmid-12345", "pve1"))

	// Assert the metric output only contains known + unknown label values.
	families, err := reg.Gather()
	require.NoError(t, err)
	var ops []string
	for _, f := range families {
		if *f.Name != "scaleset_proxmox_api_errors_total" {
			continue
		}
		for _, mt := range f.Metric {
			for _, l := range mt.Label {
				if *l.Name == "operation" {
					ops = append(ops, *l.Value)
				}
			}
		}
	}
	require.ElementsMatch(t, []string{"inject_jit", "unknown"}, ops,
		"unbounded op string must be clamped to unknown, not emitted verbatim")
}

// TestRecordGHStateMismatch_ClampsUnknownLabels guarantees the same
// closed-enum protection for ghState and action; db_state is left
// alone because callers pass it as a typed store.State value.
func TestRecordGHStateMismatch_ClampsUnknownLabels(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := observability.NewMetrics(reg)

	// Known values pass through.
	gh, ac := m.RecordGHStateMismatch("assigned", "busy", "promote_running")
	require.Equal(t, "busy", gh)
	require.Equal(t, "promote_running", ac)

	// Unknown ghState and action both clamp to "unknown".
	gh, ac = m.RecordGHStateMismatch("assigned", "stale-gh-runner-id-9999", "retry-7")
	require.Equal(t, "unknown", gh)
	require.Equal(t, "unknown", ac)
}

func TestServe_RespondsToProbes(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := prometheus.NewRegistry()
	m := observability.NewMetrics(reg)
	// Labelled metrics don't appear in /metrics output until at least one
	// label combination has been observed, so touch one explicitly.
	m.PoolSize.WithLabelValues("hot").Set(0)
	h := observability.NewHealth(time.Minute)

	log, _ := observability.NewLoggerTo(io.Discard, "error", "text")

	// Bind ":0" → kernel-assigned free port. Discovered via ln.Addr() so
	// the test can run in parallel with any other Serve test without
	// colliding on a fixed port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = observability.ServeOn(ctx, ln, reg, h, log)
	}()

	// Give Serve a moment to bind.
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode == http.StatusOK && strings.Contains(string(body), "ok")
	}, time.Second, 20*time.Millisecond)

	// /readyz should be 503 until health flips.
	resp, err := http.Get("http://" + addr + "/readyz")
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	h.MarkLeader(true)
	h.MarkListenerConnected()
	h.MarkRecoveryDone()
	h.MarkProxmoxOK()

	resp, err = http.Get("http://" + addr + "/readyz")
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// /metrics returns Prometheus text format.
	resp, err = http.Get("http://" + addr + "/metrics")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.Contains(t, string(body), "scaleset_pool_size")

	cancel()
	wg.Wait()
}
