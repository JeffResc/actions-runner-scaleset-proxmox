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
	} {
		require.Truef(t, names[want], "missing metric %s", want)
	}
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
