//go:build e2e

// Package e2e drives the orchestrator binary in-process against fake
// Proxmox and GitHub HTTP servers. The test files in this package are
// gated by the `e2e` build tag so they're excluded from the default
// `go test ./...` run — invoke via `task e2e` instead.
package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/app"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/githubauth"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakegithub"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakeproxmox"
)

// Harness owns one running orchestrator instance plus the fake servers
// it talks to. Construct with Start; the harness registers a t.Cleanup
// hook that cancels the orchestrator context and waits for clean exit.
type Harness struct {
	Proxmox *fakeproxmox.Server
	GitHub  *fakegithub.Server

	// HTTP endpoints discovered after the orchestrator binds.
	ObsURL   string // http://127.0.0.1:N (observability + /metrics + /readyz)
	AdminURL string // http://127.0.0.1:M (admin API)

	// AdminSecret is the bearer token configured for the admin API.
	// Use SignAdminRequest to attach it to outbound requests.
	AdminSecret string

	cancel context.CancelFunc
	done   <-chan error
	t      testing.TB
}

// Options configures a single Start call. Zero values give a
// well-behaved orchestrator: hot pool 2 / warm 0 / max-concurrent 4,
// org-scoped, polling intervals dropped to ~hundreds-of-ms so tests
// run in seconds rather than tens of seconds.
type Options struct {
	// HotSize is the always-booted runner count. Defaults to 2.
	HotSize int
	// WarmSize is the additional pre-cloned-but-stopped budget. Defaults to 0.
	WarmSize int
	// MaxConcurrentRunners gates total runner provisioning. Defaults to 8.
	MaxConcurrentRunners int

	// Org is the GitHub org slug the orchestrator registers under.
	// Defaults to "octocat". The fake accepts any value.
	Org string

	// ScaleSetName is the runner-scale-set name (must match the fake's
	// scale-set name; both default to "test-scaleset").
	ScaleSetName string

	// FakeProxmox lets tests pre-construct (and pre-seed) the fake
	// Proxmox server. When nil, Start creates one with defaults.
	FakeProxmox *fakeproxmox.Server
	// FakeGitHub mirrors FakeProxmox for the GitHub side.
	FakeGitHub *fakegithub.Server

	// DryRun, when true, wraps the orchestrator's provisioner so all
	// destructive Proxmox operations log instead of executing. Mirrors
	// the binary's --dry-run flag. The fake Proxmox should therefore
	// see no Clone / Start / Destroy traffic — read calls
	// (template discovery, ping, list) still pass through.
	DryRun bool

	// RaftCluster, when non-nil, enables raft leader election. The
	// caller creates one RaftCluster shared between every replica
	// (call NewRaftCluster), then passes (RaftCluster, ReplicaIndex)
	// in each per-replica Start. The cluster owns the InmemTransport
	// network that wires replicas to each other; without that wiring
	// raft election would never converge in-process.
	RaftCluster *RaftCluster

	// ReplicaIndex identifies which slot in RaftCluster.Peers this
	// replica occupies. Required when RaftCluster is set; ignored
	// otherwise. Must be unique per replica in the same cluster.
	ReplicaIndex int

	// Identity is the replica's NodeID in raft mode. Defaults to
	// RaftCluster.Peers[ReplicaIndex].NodeID when empty.
	Identity string
}

// RaftCluster manages the in-process raft transport network shared
// across N replicas in a single e2e scenario. Construct once with
// NewRaftCluster, then pass it to each per-replica Start.
//
// Internally it pre-allocates N InmemTransports + synthetic addresses
// and wires every pair via Connect, so the moment each replica's
// raft.NewRaft call lands the membership protocol can converge
// without a real network hop.
type RaftCluster struct {
	Transports []*raft.InmemTransport
	Addrs      []raft.ServerAddress
	Peers      []RaftPeerSpec
}

// RaftPeerSpec is one entry in the shared peer list. The orchestrator
// config marshals these into cluster.raft.peers; the harness also
// injects the matching transport into app.Options.
type RaftPeerSpec struct {
	NodeID   string
	RaftAddr string // matches Addrs[i] — the InmemTransport synthetic addr
	HTTPAddr string // 127.0.0.1:<adminPort> — used by Forwarder
}

// NewRaftCluster builds the shared transport network for n replicas.
// Caller-supplied adminAddrs match each replica's expected admin
// HTTPAddr so leader-endpoint resolution lines up with the harness's
// pre-bound admin port. Order matters: adminAddrs[i] is the i-th
// replica.
func NewRaftCluster(t testing.TB, adminAddrs []string) *RaftCluster {
	t.Helper()
	n := len(adminAddrs)
	rc := &RaftCluster{
		Transports: make([]*raft.InmemTransport, n),
		Addrs:      make([]raft.ServerAddress, n),
		Peers:      make([]RaftPeerSpec, n),
	}
	for i := 0; i < n; i++ {
		addr, tr := raft.NewInmemTransport("")
		rc.Transports[i] = tr
		rc.Addrs[i] = addr
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			rc.Transports[i].Connect(rc.Addrs[j], rc.Transports[j])
		}
	}
	for i := 0; i < n; i++ {
		rc.Peers[i] = RaftPeerSpec{
			NodeID:   "replica-" + strconv.Itoa(i),
			RaftAddr: string(rc.Addrs[i]),
			HTTPAddr: adminAddrs[i],
		}
	}
	return rc
}

// Start spins up the fakes (if not supplied) and launches app.Run in a
// goroutine. It blocks until /readyz reports green or 30s elapses,
// then returns the Harness. t.Cleanup unwinds everything; tests do
// not need to call Stop explicitly unless they want to inspect the
// post-shutdown state.
func Start(t testing.TB, opts Options) *Harness {
	t.Helper()
	if opts.HotSize == 0 {
		opts.HotSize = 2
	}
	if opts.MaxConcurrentRunners == 0 {
		opts.MaxConcurrentRunners = 8
	}
	if opts.Org == "" {
		opts.Org = "octocat"
	}
	if opts.ScaleSetName == "" {
		opts.ScaleSetName = "test-scaleset"
	}

	proxmox := opts.FakeProxmox
	if proxmox == nil {
		proxmox = fakeproxmox.New(t, fakeproxmox.Options{
			TaskDuration: 5 * time.Millisecond,
		})
	}
	gh := opts.FakeGitHub
	if gh == nil {
		gh = fakegithub.New(t, fakegithub.Options{
			ScaleSet: fakegithub.ScaleSetOptions{Name: opts.ScaleSetName},
		})
	}

	// Pre-bind two ports so we know where to point readiness probes
	// and admin client traffic. There's a tiny TOCTOU race between
	// closing the probe listener and the orchestrator binding — small
	// enough not to matter in a single-process test runner.
	var (
		obsAddr   string
		adminAddr string
	)
	if opts.RaftCluster != nil {
		// In raft mode the admin port must match the peer list the
		// shared RaftCluster already committed to — the test scenario
		// builds the cluster with each replica's adminAddr declared
		// upfront, then we just look it up here.
		adminAddr = opts.RaftCluster.Peers[opts.ReplicaIndex].HTTPAddr
		obsAddr = pickAddr(t)
	} else {
		obsAddr = pickAddr(t)
		adminAddr = pickAddr(t)
	}

	const adminSecret = "fake-admin-secret"
	const proxmoxTokenSecret = "fake-proxmox-secret"
	const ghToken = "ghp_fake"
	t.Setenv("GITHUB_PAT", ghToken)
	t.Setenv("PROXMOX_TOKEN_SECRET", proxmoxTokenSecret)
	t.Setenv("SCALESET_ADMIN_SECRET", adminSecret)

	cv := configValues{
		ProxmoxURL:           proxmox.URL,
		Org:                  opts.Org,
		ScaleSetName:         opts.ScaleSetName,
		HotSize:              opts.HotSize,
		WarmSize:             opts.WarmSize,
		MaxConcurrentRunners: opts.MaxConcurrentRunners,
		ObsAddr:              obsAddr,
		AdminAddr:            adminAddr,
	}
	var (
		raftTransport raft.Transport
		raftLocalAddr raft.ServerAddress
	)
	if opts.RaftCluster != nil {
		identity := opts.Identity
		if identity == "" {
			identity = opts.RaftCluster.Peers[opts.ReplicaIndex].NodeID
		}
		cv.ClusterMode = "raft"
		cv.NodeID = identity
		// BindAddr is a placeholder — the in-mem TestTransport takes
		// over before any TCP listener is ever built — but the config
		// validator still requires a non-empty value, so we give it
		// one that's obviously synthetic.
		cv.BindAddr = "test-inmem://" + string(opts.RaftCluster.Addrs[opts.ReplicaIndex])
		// DataDir is required by config validation even though the
		// in-mem TestTransport keeps the cluster from ever touching
		// disk. Give it a per-replica temp dir so config.Parse is
		// satisfied; the in-mem store path inside NewRaft bypasses
		// it.
		cv.DataDir = filepath.Join(t.TempDir(), "raft")
		cv.Peers = opts.RaftCluster.Peers
		// Bootstrap the first replica only; the rest discover the
		// bootstrapped configuration via the transport.
		cv.Bootstrap = opts.ReplicaIndex == 0
		// Aggressive timings keep election fast for short-lived tests
		// while still satisfying raft's internal invariant that
		// LeaderLease <= Heartbeat.
		cv.HeartbeatTimeout = "100ms"
		cv.ElectionTimeout = "100ms"
		cv.CommitTimeout = "20ms"

		raftTransport = opts.RaftCluster.Transports[opts.ReplicaIndex]
		raftLocalAddr = opts.RaftCluster.Addrs[opts.ReplicaIndex]
	}
	configPath := writeConfig(t, cv)

	auth, err := githubauth.NewPATWithConfig(githubauth.PATConfig{
		Token:       ghToken,
		ConfigURL:   gh.ConfigURL(opts.Org),
		RESTBaseURL: gh.RESTBaseURL(),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- app.Run(ctx, app.Options{
			ConfigPath:    configPath,
			DryRun:        opts.DryRun,
			Version:       "e2e",
			AuthOverride:  auth,
			RaftTransport: raftTransport,
			RaftLocalAddr: raftLocalAddr,
		})
	}()

	h := &Harness{
		Proxmox:     proxmox,
		GitHub:      gh,
		ObsURL:      "http://" + obsAddr,
		AdminURL:    "http://" + adminAddr,
		AdminSecret: adminSecret,
		cancel:      cancel,
		done:        errCh,
		t:           t,
	}
	t.Cleanup(func() { h.Stop(t) })

	h.WaitReady(t, 30*time.Second)
	return h
}

// Stop cancels the orchestrator context and waits up to 30s for clean
// exit. Calling Stop more than once is a no-op.
func (h *Harness) Stop(t testing.TB) {
	t.Helper()
	if h.cancel == nil {
		return
	}
	h.cancel()
	h.cancel = nil
	select {
	case err := <-h.done:
		if err != nil && !strings.Contains(err.Error(), "context canceled") {
			// Surface unexpected exit reasons; don't fail an already-
			// passing test on shutdown noise.
			t.Logf("orchestrator exited with: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Errorf("orchestrator did not exit within 30s of cancel")
	}
}

// WaitReady polls /readyz until it returns 200 or the deadline expires.
func (h *Harness) WaitReady(t testing.TB, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		resp, err := http.Get(h.ObsURL + "/readyz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("orchestrator did not become ready within %s", within)
}

// MetricValue scrapes /metrics and returns the float value of the
// named metric with the given labels. Labels are key=value pairs;
// order-sensitive and must match the label set exactly. Returns 0
// (no error) when the metric is registered but has no samples yet —
// distinguishing "missing" from "zero" requires inspecting the raw
// /metrics text and isn't needed for current assertions.
func (h *Harness) MetricValue(t testing.TB, name string, labels ...string) float64 {
	t.Helper()
	resp, err := http.Get(h.ObsURL + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return parseMetric(string(body), name, labels)
}

// AdminRequest sends an authenticated request to the admin API. The
// caller is responsible for closing the response body.
func (h *Harness) AdminRequest(t testing.TB, method, path string, body io.Reader) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, h.AdminURL+path, body)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+h.AdminSecret)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// pickAddr returns "127.0.0.1:N" with a port the OS just told us is
// free. There's a TOCTOU race: another process could grab the port
// between the close and the orchestrator's listen. In a single-process
// test runner that's vanishingly rare.
func pickAddr(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// PickAdminAddrs returns n free 127.0.0.1 ports — used by raft cluster
// tests that need to commit to each replica's admin address before
// any harness has started.
func PickAdminAddrs(t testing.TB, n int) []string {
	t.Helper()
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = pickAddr(t)
	}
	return out
}

type configValues struct {
	ProxmoxURL           string
	Org                  string
	ScaleSetName         string
	HotSize              int
	WarmSize             int
	MaxConcurrentRunners int
	ObsAddr              string
	AdminAddr            string

	// Cluster mode plumbing. When ClusterMode is "raft" the template
	// emits the cluster.raft block; otherwise it's omitted
	// (default = standalone).
	ClusterMode      string // "standalone" or "raft"
	NodeID           string
	BindAddr         string
	DataDir          string
	Peers            []RaftPeerSpec
	Bootstrap        bool
	HeartbeatTimeout string
	ElectionTimeout  string
	CommitTimeout    string
}

const configTmpl = `
github:
  auth_mode: pat
  pat:
    token_env: GITHUB_PAT
  scope:
    org: {{.Org}}
  poll_interval: 200ms
  assigned_grace: 5s
  running_idle_grace: 1s
  assigned_offline_grace: 5s
scaleset:
  name: {{.ScaleSetName}}
  labels: [self-hosted, linux, x64, e2e]
  runner_group: default
  max_concurrent_runners: {{.MaxConcurrentRunners}}
proxmox:
  endpoint: {{.ProxmoxURL}}
  insecure_skip_verify: true
  auth:
    token_id: scaleset@pve!automation
    token_secret_env: PROXMOX_TOKEN_SECRET
  template_vmid: 9000
  vmid_range: { min: 10000, max: 10999 }
  storage:
    disk: local-lvm
    snippets: local
  network:
    bridge: vmbr0
    vlan_tag: 0
  clone:
    linked: true
nodes:
  strategy: single
  single_node: pve1
pool:
  hot_size: {{.HotSize}}
  warm_size: {{.WarmSize}}
  reconcile_interval: 100ms
  vm_max_age: 24h
  drain_timeout: 5s
  boot_max_attempts: 3
  power_poll_interval: 100ms
  vmid_reuse_cooldown: 1s
  orphan_grace: 5s
  clone_inflight_grace: 1m
observability:
  http_addr: "{{.ObsAddr}}"
  log_level: warn
  log_format: text
admin_api:
  http_addr: "{{.AdminAddr}}"
  shared_secret_env: SCALESET_ADMIN_SECRET
cluster:
  mode: {{if .ClusterMode}}{{.ClusterMode}}{{else}}standalone{{end}}
{{- if eq .ClusterMode "raft" }}
  raft:
    node_id: {{.NodeID}}
    bind_addr: "{{.BindAddr}}"
    data_dir: "{{.DataDir}}"
    bootstrap: {{.Bootstrap}}
    heartbeat_timeout: {{.HeartbeatTimeout}}
    election_timeout: {{.ElectionTimeout}}
    commit_timeout: {{.CommitTimeout}}
    peers:
{{- range .Peers }}
      - node_id: {{.NodeID}}
        raft_addr: "{{.RaftAddr}}"
        http_addr: "{{.HTTPAddr}}"
{{- end }}
{{- end }}
`

func writeConfig(t testing.TB, v configValues) string {
	t.Helper()
	tmpl, err := template.New("cfg").Parse(configTmpl)
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, tmpl.Execute(&buf, v))
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o600))
	return path
}

// parseMetric does a minimal text-format scan looking for `name{labels...} value`.
// labels is a slice of "k=v" strings (order matters — Prometheus emits
// labels alphabetically, which is what we feed in). When labels is
// empty, the unlabelled sample is returned. Returns 0 if no matching
// sample is found.
func parseMetric(body, name string, labels []string) float64 {
	wantLabels := ""
	if len(labels) > 0 {
		wantLabels = "{" + strings.Join(labels, ",") + "}"
	}
	prefix := name + wantLabels + " "
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		valStr := strings.TrimPrefix(line, prefix)
		// Strip trailing timestamp if present.
		if i := strings.IndexByte(valStr, ' '); i >= 0 {
			valStr = valStr[:i]
		}
		v, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			return 0
		}
		return v
	}
	return 0
}

// formatLabel builds a "k=\"v\"" snippet suitable for label-matched
// MetricValue calls. Saves callers from sprintf'ing the quotes inline.
func formatLabel(k, v string) string { return fmt.Sprintf(`%s="%s"`, k, v) }

var _ = formatLabel // exported via callers in scenario tests
