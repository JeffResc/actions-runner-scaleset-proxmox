package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/golang-jwt/jwt/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/cluster"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/config"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/githubauth"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/ipam"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/observability"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakegithub"
)

// ---------------------------------------------------------------------------
// #355 bullet 1: buildGitHubAuth
// ---------------------------------------------------------------------------

// TestBuildGitHubAuth_OverrideWins pins the test-hook contract: a
// non-nil AuthOverride is returned verbatim, bypassing cfg.GitHub
// entirely (the seam every e2e test relies on to point at fakegithub).
func TestBuildGitHubAuth_OverrideWins(t *testing.T) {
	t.Parallel()
	override, err := githubauth.NewPAT("token")
	require.NoError(t, err)
	got, err := buildGitHubAuth(&config.Config{}, override)
	require.NoError(t, err)
	require.Same(t, override, got, "a non-nil override must be returned verbatim, ignoring cfg.GitHub")
}

// TestBuildGitHubAuth_PATConstructs pins the happy pat path: a valid
// pat config yields a non-nil Auth and no error.
func TestBuildGitHubAuth_PATConstructs(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{GitHub: config.GitHubConfig{
		AuthMode: "pat",
		PAT:      &config.GitHubPATConfig{Token: "testtoken"},
	}}
	got, err := buildGitHubAuth(cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
}

// TestBuildGitHubAuth_AppConstructs pins the happy app path: a valid
// app config (client_id + installation_id + a readable PEM file)
// yields a non-nil Auth and no error.
func TestBuildGitHubAuth_AppConstructs(t *testing.T) {
	t.Parallel()
	pemPath := filepath.Join(t.TempDir(), "key.pem")
	// The auth layer only checks that the file is 0600, owned by us,
	// and contains a PEM "BEGIN" marker — no real key parse happens at
	// construction time.
	require.NoError(t, os.WriteFile(pemPath,
		[]byte("-----BEGIN RSA PRIVATE KEY-----\nZmFrZQ==\n-----END RSA PRIVATE KEY-----\n"), 0o600))
	cfg := &config.Config{GitHub: config.GitHubConfig{
		AuthMode: "app",
		App: &config.GitHubAppConfig{
			ClientID:       "Iv23liTestClientID",
			InstallationID: 12345,
			PrivateKeyPath: pemPath,
		},
	}}
	got, err := buildGitHubAuth(cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
}

// TestBuildGitHubAuth_UnknownModeErrors pins the flagged error path
// (#355): an unset or invalid auth_mode must surface a non-nil error
// rather than a nil Auth the caller would later deref.
//
// NOTE(#355): the returned error names only the bad auth_mode value,
// not which scaleset/config produced it. With N scalesets sharing one
// process an operator can't tell from the message which entry is
// misconfigured. Documented here, not fixed (production change).
func TestBuildGitHubAuth_UnknownModeErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		mode string
	}{
		{name: "unset", mode: ""},
		{name: "invalid", mode: "oauth"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := &config.Config{GitHub: config.GitHubConfig{AuthMode: tc.mode}}
			got, err := buildGitHubAuth(cfg, nil)
			require.Error(t, err, "unknown auth_mode must fail loud, not return a nil Auth")
			require.Nil(t, got)
			require.Contains(t, err.Error(), "auth_mode")
		})
	}
}

// ---------------------------------------------------------------------------
// #355 bullet 2: buildCoordinator
// ---------------------------------------------------------------------------

// TestBuildCoordinator_StandaloneIsAlwaysLeader pins the default-mode
// selection: a non-raft config yields a non-nil Coordinator whose
// Run() elects it leader (standalone is always leader). We drive Run
// briefly and assert IsLeader() flips true inside the OnElected
// callback, then that cancellation returns cleanly.
func TestBuildCoordinator_StandaloneIsAlwaysLeader(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{
		Cluster:  config.ClusterConfig{Mode: "standalone"},
		AdminAPI: config.AdminAPIConfig{HTTPAddr: "127.0.0.1:9101"},
	}
	elected := make(chan struct{})
	cb := cluster.Callbacks{OnElected: func(context.Context) { close(elected) }}
	coord, err := buildCoordinator(cfg, cb, silentLogger(), Options{})
	require.NoError(t, err)
	require.NotNil(t, coord)
	require.False(t, coord.IsLeader(), "standalone is not leader until Run drives election")

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- coord.Run(ctx) }()

	select {
	case <-elected:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("standalone coordinator never fired OnElected")
	}
	require.True(t, coord.IsLeader(), "standalone must report leader while Run is active")

	cancel()
	select {
	case err := <-runErr:
		require.NoError(t, err, "standalone Run must return nil on ctx cancel")
	case <-time.After(5 * time.Second):
		t.Fatal("standalone Run did not return after ctx cancel")
	}
}

// TestBuildCoordinator_RaftMisconfigErrors pins that a raft-mode
// config that fails cluster.NewRaft validation surfaces an error
// rather than silently degrading to standalone. Two independent
// misconfigurations are exercised, both hermetic (no port binding):
//   - a malformed admin addr (port extraction fails before NewRaft)
//   - a valid admin addr but an empty peer list (NewRaft.validate fails)
func TestBuildCoordinator_RaftMisconfigErrors(t *testing.T) {
	t.Parallel()

	t.Run("bad_admin_addr", func(t *testing.T) {
		t.Parallel()
		cfg := &config.Config{
			Cluster:  config.ClusterConfig{Mode: "raft"},
			AdminAPI: config.AdminAPIConfig{HTTPAddr: "no-port-here"},
		}
		coord, err := buildCoordinator(cfg, cluster.Callbacks{}, silentLogger(), Options{})
		require.Error(t, err, "raft mode with an unparseable admin addr must fail, not fall back to standalone")
		require.Nil(t, coord)
		require.Contains(t, err.Error(), "admin port")
	})

	t.Run("empty_peers", func(t *testing.T) {
		t.Parallel()
		cfg := &config.Config{
			Cluster: config.ClusterConfig{Mode: "raft", Raft: config.ClusterRaftConfig{
				NodeID:   "node-1",
				BindAddr: "127.0.0.1:0",
				DataDir:  t.TempDir(),
				Peers:    nil, // validate() rejects: at least one peer required
			}},
			AdminAPI: config.AdminAPIConfig{HTTPAddr: "127.0.0.1:9101"},
		}
		coord, err := buildCoordinator(cfg, cluster.Callbacks{}, silentLogger(), Options{})
		require.Error(t, err, "raft mode with an invalid peer list must fail, not silently degrade")
		require.Nil(t, coord)
	})
}

// ---------------------------------------------------------------------------
// #355 bullet 3: ensureScaleSetForEntry
// ---------------------------------------------------------------------------

// newFakeScaleSetClient builds a real scaleset.Client pointed at the
// given config URL (a fakegithub or custom handshake server). retryMax
// 0 keeps failure-path tests from waiting on the library's exponential
// backoff.
func newFakeScaleSetClient(t *testing.T, configURL string) *scaleset.Client {
	t.Helper()
	auth, err := githubauth.NewPATWithConfig(githubauth.PATConfig{Token: "t", ConfigURL: configURL})
	require.NoError(t, err)
	cli, err := auth.NewScaleSetClient(t.Context(), githubauth.Scope{Org: "my-org"},
		scaleset.SystemInfo{}, scaleset.WithRetryMax(0))
	require.NoError(t, err)
	return cli
}

// TestEnsureScaleSetForEntry_FoundWithRunnerGroup pins the happy path
// with an explicit runner group: the group is resolved by name, then
// the named scale set is found within it and returned.
func TestEnsureScaleSetForEntry_FoundWithRunnerGroup(t *testing.T) {
	t.Parallel()
	srv := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{ID: 77, Name: "linux-x64", RunnerGroupID: 3},
	})
	cli := newFakeScaleSetClient(t, srv.ConfigURL("my-org"))
	entry := config.ScaleSetEntry{Name: "linux-x64", RunnerGroup: "prod-group"}

	rss, err := ensureScaleSetForEntry(t.Context(), cli, entry, nil, silentLogger())
	require.NoError(t, err)
	require.NotNil(t, rss)
	require.Equal(t, 77, rss.ID, "the found scale set's ID must be returned")
	require.Equal(t, "linux-x64", rss.Name)
}

// TestEnsureScaleSetForEntry_FoundDefaultGroup pins the happy path
// without a runner group: groupID defaults to 1 and the lookup still
// resolves the existing scale set.
func TestEnsureScaleSetForEntry_FoundDefaultGroup(t *testing.T) {
	t.Parallel()
	srv := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{ID: 42, Name: "test-scaleset"},
	})
	cli := newFakeScaleSetClient(t, srv.ConfigURL("my-org"))
	entry := config.ScaleSetEntry{Name: "test-scaleset"} // no RunnerGroup

	rss, err := ensureScaleSetForEntry(t.Context(), cli, entry, nil, silentLogger())
	require.NoError(t, err)
	require.NotNil(t, rss)
	require.Equal(t, 42, rss.ID)
}

// TestEnsureScaleSetForEntry_LabelDriftRepaired drives the case the
// early return used to skip: the scale set already exists on GitHub
// with fewer labels than the config now lists. The orchestrator must
// PATCH GitHub into agreement, otherwise jobs asking for the new label
// queue forever with every health signal green.
func TestEnsureScaleSetForEntry_LabelDriftRepaired(t *testing.T) {
	t.Parallel()
	srv := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{
			ID:     77,
			Name:   "linux-x64",
			Labels: []string{"self-hosted", "linux", "x64", "proxmox"},
		},
	})
	cli := newFakeScaleSetClient(t, srv.ConfigURL("my-org"))
	entry := config.ScaleSetEntry{
		Name:   "linux-x64",
		Labels: []string{"self-hosted", "linux", "x64", "proxmox", "mem-4g"},
	}
	metrics := observability.NewMetrics(prometheus.NewRegistry())

	rss, err := ensureScaleSetForEntry(t.Context(), cli, entry, metrics, silentLogger())
	require.NoError(t, err)
	require.NotNil(t, rss)
	require.ElementsMatch(t, entry.Labels, srv.ScaleSetLabels(),
		"the configured labels must reach GitHub on an already-existing scale set")
	require.Zero(t, testutil.ToFloat64(metrics.LabelDrift.WithLabelValues("linux-x64")),
		"a repaired difference is not drift")
}

// TestEnsureScaleSetForEntry_LabelDriftUpdateFailureWarns pins the
// fallback: when the PATCH is rejected, startup continues with the
// labels GitHub already knows (they still route jobs) and the
// unrepaired difference is published for alerting.
func TestEnsureScaleSetForEntry_LabelDriftUpdateFailureWarns(t *testing.T) {
	t.Parallel()
	srv := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{ID: 77, Name: "linux-x64", Labels: []string{"proxmox"}},
	})
	srv.InjectScaleSetUpdateFailure(http.StatusInternalServerError, 1)
	cli := newFakeScaleSetClient(t, srv.ConfigURL("my-org"))
	entry := config.ScaleSetEntry{Name: "linux-x64", Labels: []string{"proxmox", "mem-4g"}}
	metrics := observability.NewMetrics(prometheus.NewRegistry())

	rss, err := ensureScaleSetForEntry(t.Context(), cli, entry, metrics, silentLogger())
	require.NoError(t, err, "a failed label update must not stop the scale set from starting")
	require.NotNil(t, rss)
	require.Equal(t, 77, rss.ID)
	require.Equal(t, []string{"proxmox"}, srv.ScaleSetLabels())
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.LabelDrift.WithLabelValues("linux-x64")),
		"an unrepaired difference must be visible on scaleset_labels_drift")
}

// TestEnsureScaleSetForEntry_LabelsInSyncNoUpdate pins that matching
// labels issue no PATCH — the injected failure would surface as drift
// if the reconciliation fired on every start.
func TestEnsureScaleSetForEntry_LabelsInSyncNoUpdate(t *testing.T) {
	t.Parallel()
	srv := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{ID: 77, Name: "linux-x64", Labels: []string{"proxmox", "linux"}},
	})
	srv.InjectScaleSetUpdateFailure(http.StatusInternalServerError, 1)
	cli := newFakeScaleSetClient(t, srv.ConfigURL("my-org"))
	// Same set, different order — order is not drift.
	entry := config.ScaleSetEntry{Name: "linux-x64", Labels: []string{"linux", "proxmox"}}
	metrics := observability.NewMetrics(prometheus.NewRegistry())

	_, err := ensureScaleSetForEntry(t.Context(), cli, entry, metrics, silentLogger())
	require.NoError(t, err)
	require.Zero(t, testutil.ToFloat64(metrics.LabelDrift.WithLabelValues("linux-x64")))
}

// TestEnsureScaleSetForEntry_NoConfiguredLabelsLeavesGitHubAlone pins
// that an entry without `labels:` does not manage labels at all.
// Reconciling one to "just the scale set's name" would wipe the label
// set of a scale set adopted from ARC or created by hand — the failure
// this reconciliation exists to prevent, in the opposite direction.
func TestEnsureScaleSetForEntry_NoConfiguredLabelsLeavesGitHubAlone(t *testing.T) {
	t.Parallel()
	adopted := []string{"self-hosted", "linux", "x64", "proxmox"}
	srv := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{ID: 77, Name: "linux-x64", Labels: adopted},
	})
	srv.InjectScaleSetUpdateFailure(http.StatusInternalServerError, 1)
	cli := newFakeScaleSetClient(t, srv.ConfigURL("my-org"))
	entry := config.ScaleSetEntry{Name: "linux-x64"} // no Labels
	metrics := observability.NewMetrics(prometheus.NewRegistry())

	_, err := ensureScaleSetForEntry(t.Context(), cli, entry, metrics, silentLogger())
	require.NoError(t, err)
	require.Equal(t, adopted, srv.ScaleSetLabels(),
		"an empty labels: must leave GitHub's label set untouched")
	require.Zero(t, testutil.ToFloat64(metrics.LabelDrift.WithLabelValues("linux-x64")))
}

// TestEnsureScaleSetForEntry_SystemLabelPreserved pins that GitHub's
// own "System" label survives reconciliation. A scale set created
// without labels carries one named after itself; deleting it would
// break routing by scale-set name, and a service that re-added it
// would leave the reconciler patching on every restart.
func TestEnsureScaleSetForEntry_SystemLabelPreserved(t *testing.T) {
	t.Parallel()
	// No seeded labels — the fake mirrors GitHub's create-path default
	// of one System label named after the scale set.
	srv := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{ID: 77, Name: "linux-x64"},
	})
	cli := newFakeScaleSetClient(t, srv.ConfigURL("my-org"))
	entry := config.ScaleSetEntry{Name: "linux-x64", Labels: []string{"proxmox", "mem-4g"}}
	metrics := observability.NewMetrics(prometheus.NewRegistry())

	_, err := ensureScaleSetForEntry(t.Context(), cli, entry, metrics, silentLogger())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"linux-x64", "proxmox", "mem-4g"}, srv.ScaleSetLabels(),
		"the configured labels must be added without dropping GitHub's System label")

	// Second pass over the reconciled state must be a no-op: the
	// injected failure would surface as drift if it patched again.
	srv.InjectScaleSetUpdateFailure(http.StatusInternalServerError, 1)
	_, err = ensureScaleSetForEntry(t.Context(), cli, entry, metrics, silentLogger())
	require.NoError(t, err)
	require.Zero(t, testutil.ToFloat64(metrics.LabelDrift.WithLabelValues("linux-x64")),
		"reconciliation must converge, not patch on every start")
}

// TestEnsureScaleSetForEntry_CreateFailureSurfaces pins that a scale
// set the fake does not host (lookup returns "not found", then the
// create is rejected) surfaces as an error rather than a nil scale set
// the caller would deref.
func TestEnsureScaleSetForEntry_CreateFailureSurfaces(t *testing.T) {
	t.Parallel()
	srv := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{Name: "configured"},
	})
	cli := newFakeScaleSetClient(t, srv.ConfigURL("my-org"))
	entry := config.ScaleSetEntry{Name: "not-configured", Labels: []string{"self-hosted"}}

	rss, err := ensureScaleSetForEntry(t.Context(), cli, entry, nil, silentLogger())
	require.Error(t, err, "a create rejection must surface, not return a nil scale set")
	require.Nil(t, rss)
	require.Contains(t, err.Error(), "create runner scale set")
}

// TestEnsureScaleSetForEntry_RunnerGroupLookupErrorSurfaces drives a
// runner-group lookup failure: the handshake succeeds but the
// runnergroups endpoint returns 500. ensureScaleSetForEntry must
// surface the error wrapped with the group name.
//
// NOTE(#355): the surfaced error does not distinguish a transient 5xx
// (retry-worthy) from a permanent 404/misconfiguration — both collapse
// into the same "get runner group" wrap, so the caller's supervisor
// retries a permanent failure forever. Documented, not fixed.
func TestEnsureScaleSetForEntry_RunnerGroupLookupErrorSurfaces(t *testing.T) {
	t.Parallel()
	srv := newHandshakeServer(t, http.StatusInternalServerError)
	cli := newFakeScaleSetClient(t, srv.URL+"/my-org")
	entry := config.ScaleSetEntry{Name: "linux-x64", RunnerGroup: "prod-group"}

	rss, err := ensureScaleSetForEntry(t.Context(), cli, entry, nil, silentLogger())
	require.Error(t, err, "a runner-group lookup failure must surface")
	require.Nil(t, rss)
	require.Contains(t, err.Error(), "get runner group")
	require.Contains(t, err.Error(), "prod-group", "the error must name the group so the operator can locate it")
}

// newHandshakeServer stands up a minimal GitHub-shaped server that
// completes the scaleset library's auth handshake (registration-token
// exchange + runner-registration → actions service URL) and then
// answers the runnergroups endpoint with runnerGroupsStatus. It exists
// because fakegithub always answers runnergroups with 200; forcing the
// failure branch of ensureScaleSetForEntry needs a controllable status.
func newHandshakeServer(t *testing.T, runnerGroupsStatus int) *httptest.Server {
	t.Helper()
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/actions/runners/registration-token"):
			writeJSONResp(w, http.StatusCreated, map[string]any{
				"token":      "reg-token",
				"expires_at": time.Now().Add(time.Hour),
			})
		case strings.HasSuffix(r.URL.Path, "/actions/runner-registration"):
			writeJSONResp(w, http.StatusOK, map[string]any{
				"url":   srv.URL, // actions service base points back at us
				"token": mintTestAdminJWT(t),
			})
		case strings.Contains(r.URL.Path, "/_apis/runtime/runnergroups"):
			http.Error(w, "boom", runnerGroupsStatus)
		default:
			http.Error(w, "unexpected path "+r.URL.Path, http.StatusNotImplemented)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func writeJSONResp(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// mintTestAdminJWT returns a parseable JWT with a future exp claim —
// the scaleset library extracts the expiry via ParseUnverified, so the
// signing key is irrelevant.
func mintTestAdminJWT(t *testing.T) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		IssuedAt:  jwt.NewNumericDate(time.Now()),
	})
	s, err := tok.SignedString([]byte("test-admin-secret"))
	require.NoError(t, err)
	return s
}

// ---------------------------------------------------------------------------
// #355 bullet 4: leader-plane fan-out contract (via the existing
// superviseScaleset `run` seam — no new production seam)
// ---------------------------------------------------------------------------

// TestSuperviseScaleset_FanOutCancelCleansUpAll composes superviseScaleset
// exactly as Run's inline runLeaderPlane does — one supervisor per
// scaleset under a shared errgroup — and asserts the drain contract:
// when the shared context is cancelled while every worker is running,
// each worker's cleanup executes, every supervisor returns nil (a
// ctx-cancel is a clean exit), and g.Wait() returns without leaking a
// goroutine.
func TestSuperviseScaleset_FanOutCancelCleansUpAll(t *testing.T) {
	t.Parallel()
	const n = 4
	entries := make([]config.ScaleSetEntry, n)
	states := make([]*scalesetState, n)
	for i := range entries {
		entries[i] = config.ScaleSetEntry{Name: "ss-" + string(rune('a'+i)), Scope: config.GitHubScope{Org: "o"}}
		states[i] = &scalesetState{name: entries[i].Name}
	}

	var started, cleaned atomic.Int64
	var wg sync.WaitGroup
	wg.Add(n)
	run := func(ctx context.Context, _ config.ScaleSetEntry, _ *scalesetState) error {
		started.Add(1)
		wg.Done()
		<-ctx.Done() // stay up until drain
		cleaned.Add(1)
		return ctx.Err() // context.Canceled → supervise returns nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	g, ctxg := errgroup.WithContext(ctx)
	for i := range entries {
		entry, state := entries[i], states[i]
		g.Go(func() error {
			return superviseScaleset(ctxg, entry, state, silentLogger(),
				testRetryInitial, testRetryMax, run)
		})
	}

	// Wait until every worker is up, then drain.
	waitCh := make(chan struct{})
	go func() { wg.Wait(); close(waitCh) }()
	select {
	case <-waitCh:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("not all fan-out workers started")
	}
	require.Equal(t, int64(n), started.Load(), "every scaleset worker must be started in the fan-out")

	cancel()
	require.NoError(t, g.Wait(), "a drain (ctx cancel) must yield a clean nil result for the whole fan-out")
	require.Equal(t, int64(n), cleaned.Load(), "every started worker's cleanup must run on drain (no orphaned goroutine)")
}

// ---------------------------------------------------------------------------
// #355 bullet 5: pure config-mapping helpers (0% coverage)
// ---------------------------------------------------------------------------

func intp(v int) *int { return &v }

// TestEntryVMIDRange pins the per-scaleset-vs-fallback selection.
func TestEntryVMIDRange(t *testing.T) {
	t.Parallel()
	fallback := config.VMIDRange{Min: 100, Max: 200}

	t.Run("inherits_fallback_when_nil", func(t *testing.T) {
		t.Parallel()
		got := entryVMIDRange(config.ScaleSetEntry{VMIDRange: nil}, fallback)
		require.Equal(t, fallback, got)
	})
	t.Run("uses_entry_range_when_set", func(t *testing.T) {
		t.Parallel()
		own := config.VMIDRange{Min: 300, Max: 400}
		got := entryVMIDRange(config.ScaleSetEntry{VMIDRange: &own}, fallback)
		require.Equal(t, own, got)
	})
}

// TestFirstNonEmpty pins the override-layering helper.
func TestFirstNonEmpty(t *testing.T) {
	t.Parallel()
	require.Equal(t, "a", firstNonEmpty("a", "b"))
	require.Equal(t, "b", firstNonEmpty("", "b"))
	require.Equal(t, "", firstNonEmpty("", ""))
}

// TestAffinityNodeUniverse pins both universe shapes: single-node
// collapses to a one-element slice; multi-node returns the member list.
func TestAffinityNodeUniverse(t *testing.T) {
	t.Parallel()
	single := affinityNodeUniverse(&config.Config{Nodes: config.NodesConfig{
		Strategy: "single", SingleNode: "pve1",
	}})
	require.Equal(t, []string{"pve1"}, single)

	multi := affinityNodeUniverse(&config.Config{Nodes: config.NodesConfig{
		Strategy: "round_robin", Members: []string{"pve1", "pve2"},
	}})
	require.Equal(t, []string{"pve1", "pve2"}, multi)
}

// TestProfileSettingsForScaleset pins the YAML→pool.ProfileSettings
// projection: per-profile sizes override global pool defaults, and the
// no-network profile projects a nil NIC list plus a noop IPAM
// allocator.
func TestProfileSettingsForScaleset(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{Pool: config.PoolConfig{HotSize: 2, WarmSize: 5, BootMaxAttempts: 3}}
	entry := config.ScaleSetEntry{
		Name:                 "linux-x64",
		MaxConcurrentRunners: 10,
		Profiles: []config.ProfileConfig{
			{Name: "default"}, // inherits global sizes
			{Name: "gpu", HotSize: intp(0), WarmSize: intp(1), TemplateVMID: 9001, CPUCores: 8},
		},
	}
	got := profileSettingsForScaleset(entry, cfg)
	require.Len(t, got, 2)

	require.Equal(t, "default", got[0].Name)
	require.Equal(t, 2, got[0].HotSize, "default profile inherits global hot_size")
	require.Equal(t, 5, got[0].WarmSize, "default profile inherits global warm_size")
	require.Equal(t, 10, got[0].MaxConcurrentRunners, "profile inherits the entry's concurrency cap")
	require.Nil(t, got[0].NICs, "a profile with no network block projects no NIC overrides")
	require.IsType(t, ipam.Noop{}, got[0].IPAM, "a profile with no IPAM block gets the noop allocator")

	require.Equal(t, "gpu", got[1].Name)
	require.Equal(t, 0, got[1].HotSize, "explicit hot_size:0 overrides the global default")
	require.Equal(t, 1, got[1].WarmSize)
	require.Equal(t, 9001, got[1].TemplateVMID)
	require.Equal(t, 8, got[1].CPUCores)
}

// TestNicsFromProfileNetwork pins the NIC layering: no network block
// yields nil; a network block layers profile bridge over the global
// default and appends extra NICs.
func TestNicsFromProfileNetwork(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{Proxmox: config.ProxmoxConfig{Network: config.ProxmoxNetwork{
		Bridge: "vmbr0", VLANTag: 100,
	}}}

	require.Nil(t, nicsFromProfileNetwork(cfg, config.ProfileConfig{Network: nil}),
		"no network block must leave the template's NICs untouched")

	p := config.ProfileConfig{Network: &config.ProfileNetworkConfig{
		Bridge:    "", // inherit global vmbr0
		ExtraNICs: []config.ProfileNICConfig{{Bridge: "vmbr9", VLANTag: 200}},
	}}
	nics := nicsFromProfileNetwork(cfg, p)
	require.Len(t, nics, 2)
	require.Equal(t, "vmbr0", nics[0].Bridge, "empty profile bridge inherits the global default")
	require.Equal(t, 100, nics[0].VLANTag, "unset profile VLAN inherits the global tag")
	require.Equal(t, "vmbr9", nics[1].Bridge)
	require.Equal(t, 200, nics[1].VLANTag)
}

// TestIpamFromProfileNetwork pins backend selection: no block / noop /
// unknown all fall back to the noop allocator; a valid static block
// yields a non-noop allocator.
func TestIpamFromProfileNetwork(t *testing.T) {
	t.Parallel()
	require.IsType(t, ipam.Noop{},
		ipamFromProfileNetwork(config.ProfileConfig{Network: nil}, silentLogger()))

	require.IsType(t, ipam.Noop{},
		ipamFromProfileNetwork(config.ProfileConfig{
			Network: &config.ProfileNetworkConfig{IPAM: &config.ProfileIPAMConfig{Backend: "noop"}},
		}, silentLogger()))

	require.IsType(t, ipam.Noop{},
		ipamFromProfileNetwork(config.ProfileConfig{
			Network: &config.ProfileNetworkConfig{IPAM: &config.ProfileIPAMConfig{Backend: "made-up"}},
		}, silentLogger()), "an unknown backend must degrade to noop, not panic")

	static := ipamFromProfileNetwork(config.ProfileConfig{
		Network: &config.ProfileNetworkConfig{IPAM: &config.ProfileIPAMConfig{
			Backend: "static", Pool: []string{"10.0.0.10/24", "10.0.0.11/24"},
		}},
	}, silentLogger())
	require.NotNil(t, static)
	_, isNoop := static.(ipam.Noop)
	require.False(t, isNoop, "a valid static block must yield a real allocator, not noop")
}

// TestRouterForScaleset pins that a profile catalog builds a non-nil
// router without error.
func TestRouterForScaleset(t *testing.T) {
	t.Parallel()
	r, err := routerForScaleset(config.ScaleSetEntry{Profiles: []config.ProfileConfig{
		{Name: "default", Labels: []string{"self-hosted"}},
		{Name: "gpu", Labels: []string{"gpu"}},
	}})
	require.NoError(t, err)
	require.NotNil(t, r)
}

// TestCanaryControllerForScaleset pins that per-profile canary fields
// project into a non-nil controller; a profile with no canary template
// falls back to the global stable template.
func TestCanaryControllerForScaleset(t *testing.T) {
	t.Parallel()
	c, err := canaryControllerForScaleset(config.ScaleSetEntry{Profiles: []config.ProfileConfig{
		{Name: "default"}, // stable-only, inherits global template
		{Name: "gpu", TemplateVMID: 9001, CanaryTemplateVMID: 9002, CanaryPercent: 10},
	}}, 9000)
	require.NoError(t, err)
	require.NotNil(t, c)
}

// TestQuotasResolverFromConfig pins that a quotas block (defaults +
// one override) builds a non-nil resolver.
func TestQuotasResolverFromConfig(t *testing.T) {
	t.Parallel()
	r, err := quotasResolverFromConfig(&config.Config{Quotas: config.QuotasConfig{
		DefaultPerRepo: 5,
		DefaultPerOrg:  20,
		Overrides: []config.QuotaOverride{
			{Match: config.QuotaMatch{Org: "acme"}, MaxConcurrent: 50},
		},
	}})
	require.NoError(t, err)
	require.NotNil(t, r)
}

// TestPriorityMatcherFromConfig pins that a priority class list builds
// a non-nil matcher.
func TestPriorityMatcherFromConfig(t *testing.T) {
	t.Parallel()
	m, err := priorityMatcherFromConfig(&config.Config{Priority: config.PriorityConfig{
		Classes: []config.PriorityClassConfig{
			{Name: "high", Weight: 100, Preempt: true, Match: config.PriorityMatchConfig{Org: "acme"}},
		},
	}})
	require.NoError(t, err)
	require.NotNil(t, m)
}

// TestScheduleRunnerForScaleset_NoSchedulesReturnsNil pins the
// no-schedule signal: when no profile declares a schedule the helper
// returns (nil, nil) so the caller skips the goroutine spawn.
func TestScheduleRunnerForScaleset_NoSchedulesReturnsNil(t *testing.T) {
	t.Parallel()
	r, err := scheduleRunnerForScaleset(
		config.ScaleSetEntry{Profiles: []config.ProfileConfig{{Name: "default"}}},
		config.PoolConfig{}, nil, silentLogger(), nil)
	require.NoError(t, err)
	require.Nil(t, r, "no declared schedules must signal via (nil, nil)")
}
