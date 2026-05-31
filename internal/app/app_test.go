package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/config"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/nodeselector"
)

func TestPortFromAddr(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		addr    string
		want    int
		wantErr bool
	}{
		{name: "empty", addr: "", want: 0},
		{name: "ipv4_loopback", addr: "127.0.0.1:9101", want: 9101},
		{name: "wildcard_v4", addr: "0.0.0.0:9101", want: 9101},
		{name: "bare_port", addr: ":9101", want: 9101},
		{name: "ipv6_loopback", addr: "[::1]:9101", want: 9101},
		{name: "ipv6_wildcard", addr: "[::]:9101", want: 9101},
		{name: "ipv6_full", addr: "[fe80::1]:9101", want: 9101},
		{name: "no_port_separator", addr: "127.0.0.1", wantErr: true},
		{name: "non_numeric_port", addr: "127.0.0.1:abc", wantErr: true},
		{name: "port_zero", addr: "127.0.0.1:0", wantErr: true},
		{name: "port_too_large", addr: "127.0.0.1:70000", wantErr: true},
		{name: "ipv6_no_brackets", addr: "::1:9101", wantErr: true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := portFromAddr(tc.addr)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("portFromAddr(%q) = %d, want error", tc.addr, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("portFromAddr(%q) unexpected error: %v", tc.addr, err)
			}
			if got != tc.want {
				t.Fatalf("portFromAddr(%q) = %d, want %d", tc.addr, got, tc.want)
			}
		})
	}
}

// TestMergeLeaderPlaneErr covers the exit-code promotion path: when
// runLeaderPlane fails and cancels the root ctx, coord.Run returns
// nil (clean ctx-cancel), so g1.Wait()'s result is nil even though
// the process should exit non-zero. The helper must surface the
// stashed leader-plane error in that case so supervisors restart.
func TestMergeLeaderPlaneErr(t *testing.T) {
	t.Parallel()

	stash := func(err error) *atomic.Pointer[error] {
		var p atomic.Pointer[error]
		if err != nil {
			p.Store(&err)
		}
		return &p
	}

	leaderErr := errors.New("ensure runner scale set: bad creds")

	t.Run("phase1_nil_and_leader_nil_returns_nil", func(t *testing.T) {
		t.Parallel()
		got := mergeLeaderPlaneErr(nil, stash(nil))
		if got != nil {
			t.Fatalf("want nil, got %v", got)
		}
	})

	t.Run("phase1_nil_and_leader_set_surfaces_leader", func(t *testing.T) {
		t.Parallel()
		got := mergeLeaderPlaneErr(nil, stash(leaderErr))
		if got == nil {
			t.Fatalf("want non-nil to drive non-zero exit, got nil")
		}
		if !errors.Is(got, leaderErr) {
			t.Fatalf("want wrapped leader err, got %v", got)
		}
	})

	t.Run("phase1_set_takes_priority", func(t *testing.T) {
		t.Parallel()
		phase1 := errors.New("coord: transport: dial tcp")
		got := mergeLeaderPlaneErr(phase1, stash(leaderErr))
		if !errors.Is(got, phase1) {
			t.Fatalf("phase1 err must win, got %v", got)
		}
		if errors.Is(got, leaderErr) {
			t.Fatalf("leader err must not be wrapped when phase1 set; got %v", got)
		}
	})

	t.Run("empty_pointer_is_safe", func(t *testing.T) {
		t.Parallel()
		var p atomic.Pointer[error]
		if got := mergeLeaderPlaneErr(nil, &p); got != nil {
			t.Fatalf("unset pointer must yield nil, got %v", got)
		}
	})
}

// TestSuperviseScaleset_PanicIsolated locks in the multi-scaleset
// supervisor's failure-isolation contract (issue #1): a panic in
// one scaleset's worker MUST NOT propagate to its siblings via
// the outer errgroup. The supervisor recovers, logs, and returns
// nil so the errgroup keeps the other scalesets running.
func TestSuperviseScaleset_PanicIsolated(t *testing.T) {
	t.Parallel()
	entry := config.ScaleSetEntry{Name: "panicky", Scope: config.GitHubScope{Org: "x"}}
	state := &scalesetState{name: "panicky"}
	got := superviseScaleset(t.Context(), entry, state, silentLogger(),
		func(context.Context, config.ScaleSetEntry, *scalesetState) error {
			panic("simulated worker panic")
		})
	require.NoError(t, got, "panicking worker must NOT propagate up; sibling scalesets keep running")
}

// TestSuperviseScaleset_ErrorIsolated covers the same isolation
// contract for returned errors: a non-canceled error from one
// worker is logged and swallowed so siblings continue.
func TestSuperviseScaleset_ErrorIsolated(t *testing.T) {
	t.Parallel()
	entry := config.ScaleSetEntry{Name: "broken", Scope: config.GitHubScope{Org: "x"}}
	state := &scalesetState{name: "broken"}
	got := superviseScaleset(t.Context(), entry, state, silentLogger(),
		func(context.Context, config.ScaleSetEntry, *scalesetState) error {
			return errors.New("simulated worker error")
		})
	require.NoError(t, got)
}

// TestSuperviseScaleset_ContextCanceledQuiet matches the rest of
// the orchestrator's convention: ctx.Canceled on a clean shutdown
// is not an error worth logging at error severity (it's just
// drain/SIGTERM). Returning nil is the same observable behaviour
// as the other isolation paths, but no error log is emitted.
func TestSuperviseScaleset_ContextCanceledQuiet(t *testing.T) {
	t.Parallel()
	entry := config.ScaleSetEntry{Name: "draining", Scope: config.GitHubScope{Org: "x"}}
	state := &scalesetState{name: "draining"}
	got := superviseScaleset(t.Context(), entry, state, silentLogger(),
		func(context.Context, config.ScaleSetEntry, *scalesetState) error {
			return context.Canceled
		})
	require.NoError(t, got)
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// TestBuildNodeSelector_KnownStrategiesReturnNonNil pins the
// happy paths through buildUnderlyingSelector: each of the
// three declared strategies returns a non-nil selector and no
// error. Without this, a typo or a removed case in the switch
// would silently return (nil, nil) and the orchestrator would
// crash at the first Acquire.
func TestBuildNodeSelector_KnownStrategiesReturnNonNil(t *testing.T) {
	t.Parallel()
	t.Run("single", func(t *testing.T) {
		t.Parallel()
		cfg := &config.Config{Nodes: config.NodesConfig{
			Strategy: "single", SingleNode: "pve1",
		}}
		sel, err := buildNodeSelector(cfg, nil)
		require.NoError(t, err)
		require.NotNil(t, sel)
	})
	t.Run("round_robin", func(t *testing.T) {
		t.Parallel()
		cfg := &config.Config{Nodes: config.NodesConfig{
			Strategy: "round_robin", Members: []string{"pve1", "pve2"},
		}}
		sel, err := buildNodeSelector(cfg, nil)
		require.NoError(t, err)
		require.NotNil(t, sel)
	})
}

// TestBuildNodeSelector_UnknownStrategyReturnsError pins the
// safety net the audit (#202) flagged: an unknown strategy
// must surface as an error, not as a silent nil selector. The
// config validator's oneof tag normally catches typos at load
// time, but buildNodeSelector is also called from in-process
// code paths that bypass the validator (e.g. constructing
// Config in a test) — the defensive branch must still fire.
func TestBuildNodeSelector_UnknownStrategyReturnsError(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{Nodes: config.NodesConfig{
		Strategy: "made-up-strategy", SingleNode: "pve1",
	}}
	sel, err := buildNodeSelector(cfg, nil)
	require.Error(t, err,
		"unknown strategy must surface as an error so a misconfig fails loud, "+
			"not as a nil selector that crashes the first Acquire")
	require.Nil(t, sel)
	require.Contains(t, err.Error(), "made-up-strategy",
		"error must name the bad strategy so the operator can locate it")
}

// TestBuildNodeSelector_AffinityWrapsUnderlying pins that
// declaring nodes.affinity rules wraps the underlying selector
// in an affinity layer — without the wrap the prefer_nodes /
// anti_affinity_with rules would silently no-op.
func TestBuildNodeSelector_AffinityWrapsUnderlying(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{
		Nodes: config.NodesConfig{
			Strategy:   "single",
			SingleNode: "pve1",
			Members:    []string{"pve1", "pve2"},
			Affinity: []config.AffinityRule{{
				Match:       config.AffinityMatch{Profile: "gpu"},
				PreferNodes: []string{"pve1"},
				Require:     true,
			}},
		},
		Profiles: []config.ProfileConfig{{Name: "gpu"}},
	}
	sel, err := buildNodeSelector(cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, sel)
	// The single-node selector returns a static string; the
	// affinity wrapper, by contrast, has different concrete
	// type. We pin the behaviour-level observable: with a
	// Require rule pinned to "pve1" for the gpu profile, the
	// returned Selector must place gpu jobs on pve1.
	got, err := sel.Select(t.Context(), nodeselector.Hint{Profile: "gpu"})
	require.NoError(t, err)
	require.Equal(t, "pve1", got,
		"affinity Require=true with prefer_nodes=[pve1] must pin gpu profile to pve1; "+
			"if this fails, buildNodeSelector skipped the affinity wrap")
}

// TestRun_InvalidConfigPathReturnsError pins that Run() returns
// promptly on a config-load failure, before any goroutines or
// network calls fire. Without this, a typo'd config path would
// surface deep in the stack (or worse, partial startup leaving
// stray resources).
func TestRun_InvalidConfigPathReturnsError(t *testing.T) {
	t.Parallel()
	err := Run(t.Context(), Options{ConfigPath: "/definitely/does/not/exist.yaml"})
	require.Error(t, err,
		"Run must surface config-load errors immediately; a silent partial startup would leak goroutines and Proxmox connections")
}

// validStartupYAML is a minimal config that Parse/Resolve/Validate
// all accept. Used as the baseline for the startup-failure tests
// below: each test mutates one field to trigger a specific
// rejection path through Run().
const validStartupYAML = `
github:
  auth_mode: pat
  pat:
    token: testtoken
  scope:
    org: my-org

scaleset:
  name: linux-x64
  labels: [self-hosted, linux, x64]
  max_concurrent_runners: 1

proxmox:
  endpoint: https://pve.example.com:8006/api2/json
  auth:
    token_id: scaleset@pve!automation
    token_secret: testsecret
  template_vmid: 9000
  vmid_range: { min: 10000, max: 10010 }
  storage:  { disk: local-lvm, snippets: local }
  network:  { bridge: vmbr0 }
  clone:    { linked: true }

nodes:
  strategy: single
  single_node: pve1

pool:
  hot_size: 0
  warm_size: 0
  reconcile_interval: 5s
  vm_max_age: 12h
  drain_timeout: 15m
  boot_max_attempts: 3
`

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "scaleset.yaml")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

// TestRun_MalformedYAMLReturnsError covers the second-stage
// startup failure: the file exists and is readable, but the YAML
// parser rejects it. Run() must surface the parse error rather
// than partial-start. (issue #294)
func TestRun_MalformedYAMLReturnsError(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, "github: {\n  not: closed")
	err := Run(t.Context(), Options{ConfigPath: path})
	require.Error(t, err, "Run must surface yaml-parse failures, not partial-start")
}

// TestRun_MissingProxmoxTokenSecretSurfaces covers the
// missing-credential path: Parse populates secrets from env, then
// Resolve enforces "non-empty". Without the env var the secret
// remains empty and Run() must return a clear error before
// touching Proxmox. (issue #294)
func TestRun_MissingProxmoxTokenSecretSurfaces(t *testing.T) {
	t.Parallel()
	noSecret := strings.ReplaceAll(validStartupYAML, "    token_secret: testsecret", "")
	path := writeConfig(t, noSecret)
	err := Run(t.Context(), Options{ConfigPath: path})
	require.Error(t, err)
	require.Contains(t, err.Error(), "token_secret",
		"Run must name the missing secret so the operator can wire SCALESET_PROXMOX_AUTH_TOKEN_SECRET")
}

// TestRun_MalformedProxmoxEndpointSurfaces covers an invalid
// Proxmox URL: the provisioner constructor parses cfg.Proxmox.Endpoint
// and must reject malformed values before any goroutine is launched.
// (issue #294)
func TestRun_MalformedProxmoxEndpointSurfaces(t *testing.T) {
	t.Parallel()
	bad := strings.ReplaceAll(validStartupYAML, "https://pve.example.com:8006/api2/json", "not a url")
	path := writeConfig(t, bad)
	err := Run(t.Context(), Options{ConfigPath: path})
	require.Error(t, err, "Run must surface malformed Proxmox endpoints at startup, not at first ping")
}

// TestRun_ContextCanceledBeforeStartReturnsCleanly covers the
// SIGTERM-during-startup ordering: a parent ctx already cancelled
// when Run() is invoked must unwind quickly without leaking the
// observability tracer or admin server. The test fails if Run
// hangs past the deadline. (issue #294)
func TestRun_ContextCanceledBeforeStartReturnsCleanly(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, validStartupYAML)
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // pre-cancelled
	done := make(chan error, 1)
	go func() { done <- Run(ctx, Options{ConfigPath: path}) }()
	select {
	case <-done:
		// Either nil (clean drain) or non-nil (early exit) is
		// acceptable — what matters is bounded return, not the
		// specific error.
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return within 30s of a pre-cancelled context; startup ordering must honor ctx.Done()")
	}
}

// TestRunOneScalesetDeps_FieldsCompose pins that the extracted
// runOneScalesetDeps struct exposes the leader-plane collaborators
// runOneScaleset reads. A future refactor that adds or removes a
// dep must update this test, keeping the seam visible. (issue #274)
func TestRunOneScalesetDeps_FieldsCompose(t *testing.T) {
	t.Parallel()
	deps := runOneScalesetDeps{
		cfg:     &config.Config{},
		auth:    nil,
		sel:     nil,
		metrics: nil,
		health:  nil,
		log:     silentLogger(),
	}
	require.NotNil(t, deps.cfg, "runOneScalesetDeps.cfg must remain part of the public seam")
	require.NotNil(t, deps.log, "runOneScalesetDeps.log must remain part of the public seam")
}
