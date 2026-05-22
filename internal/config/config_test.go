package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/config"
)

const validPATYAML = `
github:
  auth_mode: pat
  pat:
    token_env: TEST_GH_TOKEN
  scope:
    org: my-org

scaleset:
  name: proxmox-ubuntu-x64
  labels: [self-hosted, linux, proxmox]
  max_concurrent_runners: 10

proxmox:
  endpoint: https://pve.example.com:8006/api2/json
  auth:
    token_id: scaleset@pve!automation
    token_secret_env: TEST_PVE_TOKEN
  template_vmid: 9000
  vmid_range: { min: 10000, max: 19999 }
  storage:  { disk: local-lvm, snippets: local }
  network:  { bridge: vmbr0 }
  clone:    { linked: true }

nodes:
  strategy: single
  single_node: pve1

pool:
  hot_size: 2
  warm_size: 3
  reconcile_interval: 5s
  vm_max_age: 12h
  drain_timeout: 15m
  boot_max_attempts: 3
`

// keyPath returns the path to a temp PEM-shaped file so the file validator
// is satisfied for App auth tests.
func keyPath(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "app.pem")
	require.NoError(t, os.WriteFile(p, []byte("-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----\n"), 0o600))
	return p
}

func setEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for k, v := range kv {
		t.Setenv(k, v)
	}
}

func TestParse_ValidPAT(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})

	cfg, err := config.Parse([]byte(validPATYAML))
	require.NoError(t, err)

	// Env vars resolved into secret fields.
	require.Equal(t, "ghp_fake", cfg.GitHub.PAT.Token)
	require.Equal(t, "pve-secret", cfg.Proxmox.Auth.TokenSecret)

	// Durations parsed.
	require.Equal(t, 5*time.Second, cfg.Pool.ReconcileIntervalD)
	require.Equal(t, 12*time.Hour, cfg.Pool.VMMaxAgeD)
	require.Equal(t, 15*time.Minute, cfg.Pool.DrainTimeoutD)
}

func TestParse_MissingEnvVar(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN": "x",
		// TEST_PVE_TOKEN intentionally unset
	})
	_, err := config.Parse([]byte(validPATYAML))
	require.Error(t, err)
	require.Contains(t, err.Error(), `"TEST_PVE_TOKEN"`)
}

func TestParse_UnknownYAMLField(t *testing.T) {
	bad := validPATYAML + "\nextra_garbage: 1\n"
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "extra_garbage")
}

func TestParse_BothOrgAndRepo(t *testing.T) {
	bad := strings.Replace(validPATYAML, "    org: my-org", "    org: my-org\n    repo: my-org/my-repo", 1)
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "both are present")
}

func TestParse_NeitherOrgNorRepo(t *testing.T) {
	bad := strings.Replace(validPATYAML, "    org: my-org", "", 1)
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "both are empty")
}

func TestParse_TemplateInsideVMIDRange(t *testing.T) {
	bad := strings.Replace(validPATYAML, "template_vmid: 9000", "template_vmid: 15000", 1)
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be outside vmid_range")
}

func TestParse_PoolSizesExceedMaxConcurrent(t *testing.T) {
	bad := strings.Replace(validPATYAML, "max_concurrent_runners: 10", "max_concurrent_runners: 3", 1)
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "must not exceed")
}

func TestParse_NodesStrategyRequiresMembers(t *testing.T) {
	bad := strings.Replace(validPATYAML,
		"  strategy: single\n  single_node: pve1",
		"  strategy: round_robin", 1)
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "nodes.members is required")
}

func TestParse_SingleStrategyRequiresSingleNode(t *testing.T) {
	bad := strings.Replace(validPATYAML, "  single_node: pve1", "", 1)
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "nodes.single_node is required")
}

func TestParse_InvalidDuration(t *testing.T) {
	bad := strings.Replace(validPATYAML, "reconcile_interval: 5s", "reconcile_interval: notaduration", 1)
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "pool.reconcile_interval")
}

func TestApplyDefaults_FillsMissingFields(t *testing.T) {
	minimal := `
github:
  auth_mode: pat
  pat: { token_env: TEST_GH_TOKEN }
  scope: { org: o }
scaleset:
  name: x
  max_concurrent_runners: 5
proxmox:
  endpoint: https://h:8006/api2/json
  auth: { token_id: a!b, token_secret_env: TEST_PVE_TOKEN }
  template_vmid: 9000
  vmid_range: { min: 10000, max: 19999 }
  storage: { disk: d, snippets: s }
  network: { bridge: br0 }
nodes:
  strategy: single
  single_node: n1
pool: {}
`
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
	cfg, err := config.Parse([]byte(minimal))
	require.NoError(t, err)

	require.Equal(t, 10*time.Second, cfg.Pool.ReconcileIntervalD)
	require.Equal(t, 24*time.Hour, cfg.Pool.VMMaxAgeD)
	require.Equal(t, 30*time.Minute, cfg.Pool.DrainTimeoutD)
	require.Equal(t, 3, cfg.Pool.BootMaxAttempts)
	require.Equal(t, ":9100", cfg.Observability.HTTPAddr)
	require.Equal(t, "info", cfg.Observability.LogLevel)
	require.Equal(t, "json", cfg.Observability.LogFormat)
	require.True(t, cfg.Proxmox.Clone.LinkedOrDefault(), "linked clones default to true when omitted")
	require.Nil(t, cfg.Proxmox.Clone.Linked, "default leaves the underlying field nil")

	// GitHub reconciler defaults: the production failure mode this whole
	// package was built for (over-provisioned runners that GitHub never
	// assigns) only resolves cleanly if these have sensible fallbacks.
	require.Equal(t, 15*time.Second, cfg.GitHub.PollIntervalD)
	require.Equal(t, 5*time.Minute, cfg.GitHub.AssignedGraceD)
	require.Equal(t, 30*time.Second, cfg.GitHub.RunningIdleGraceD)
	require.Equal(t, 2*time.Minute, cfg.GitHub.AssignedOfflineGraceD)

	// Power-state poller default.
	require.Equal(t, 3*time.Second, cfg.Pool.PowerPollIntervalD)

	// Race-fix grace/cooldown defaults (pre-existing race bug fixes).
	require.Equal(t, 30*time.Second, cfg.Pool.VMIDReuseCooldownD)
	require.Equal(t, 60*time.Second, cfg.Pool.OrphanGraceD)
	require.Equal(t, 5*time.Minute, cfg.Pool.CloneInflightGraceD)
}

// TestPool_RaceGraceKnobs_AcceptOverrides confirms the three pool grace
// knobs introduced for the over-provision / VMID-reuse / orphan-sweep
// races round-trip through ApplyDefaults + Resolve.
func TestPool_RaceGraceKnobs_AcceptOverrides(t *testing.T) {
	yaml := `
github:
  auth_mode: pat
  pat: { token_env: TEST_GH_TOKEN }
  scope: { org: o }
scaleset: { name: x, max_concurrent_runners: 5 }
proxmox:
  endpoint: https://h:8006/api2/json
  auth: { token_id: a!b, token_secret_env: TEST_PVE_TOKEN }
  template_vmid: 9000
  vmid_range: { min: 10000, max: 19999 }
  storage: { disk: d, snippets: s }
  network: { bridge: br0 }
nodes: { strategy: single, single_node: n1 }
pool:
  vmid_reuse_cooldown: 45s
  orphan_grace: 90s
  clone_inflight_grace: 10m
`
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
	cfg, err := config.Parse([]byte(yaml))
	require.NoError(t, err)
	require.Equal(t, 45*time.Second, cfg.Pool.VMIDReuseCooldownD)
	require.Equal(t, 90*time.Second, cfg.Pool.OrphanGraceD)
	require.Equal(t, 10*time.Minute, cfg.Pool.CloneInflightGraceD)
}

// TestPool_RaceGraceKnobs_RejectZeroOrNegative locks down the
// validation: each of the three knobs must be > 0 or Parse fails. The
// race fixes assume strictly positive durations.
func TestPool_RaceGraceKnobs_RejectZeroOrNegative(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field string
		val   string
	}{
		{"vmid_reuse_cooldown zero", "vmid_reuse_cooldown", "0s"},
		{"orphan_grace zero", "orphan_grace", "0s"},
		{"clone_inflight_grace zero", "clone_inflight_grace", "0s"},
		{"vmid_reuse_cooldown negative", "vmid_reuse_cooldown", "-1s"},
		{"orphan_grace negative", "orphan_grace", "-1s"},
		{"clone_inflight_grace negative", "clone_inflight_grace", "-1s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			yaml := `
github:
  auth_mode: pat
  pat: { token_env: TEST_GH_TOKEN }
  scope: { org: o }
scaleset: { name: x, max_concurrent_runners: 5 }
proxmox:
  endpoint: https://h:8006/api2/json
  auth: { token_id: a!b, token_secret_env: TEST_PVE_TOKEN }
  template_vmid: 9000
  vmid_range: { min: 10000, max: 19999 }
  storage: { disk: d, snippets: s }
  network: { bridge: br0 }
nodes: { strategy: single, single_node: n1 }
pool:
  ` + tc.field + `: ` + tc.val + `
`
			setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
			_, err := config.Parse([]byte(yaml))
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.field)
		})
	}
}

// TestParse_GitHubReconcilerOverrides confirms the YAML keys map to the
// resolved durations. Regression guard against typos in the yaml tags.
func TestParse_GitHubReconcilerOverrides(t *testing.T) {
	src := `
github:
  auth_mode: pat
  pat: { token_env: TEST_GH_TOKEN }
  scope: { org: o }
  poll_interval: 7s
  assigned_grace: 99m
  running_idle_grace: 11s
  assigned_offline_grace: 45s
scaleset: { name: x, max_concurrent_runners: 5 }
proxmox:
  endpoint: https://h:8006/api2/json
  auth: { token_id: a!b, token_secret_env: TEST_PVE_TOKEN }
  template_vmid: 9000
  vmid_range: { min: 10000, max: 19999 }
  storage: { disk: d, snippets: s }
  network: { bridge: br0 }
nodes:
  strategy: single
  single_node: n1
pool: {}
`
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
	cfg, err := config.Parse([]byte(src))
	require.NoError(t, err)
	require.Equal(t, 7*time.Second, cfg.GitHub.PollIntervalD)
	require.Equal(t, 99*time.Minute, cfg.GitHub.AssignedGraceD)
	require.Equal(t, 11*time.Second, cfg.GitHub.RunningIdleGraceD)
	require.Equal(t, 45*time.Second, cfg.GitHub.AssignedOfflineGraceD)
}

func TestParse_AppAuthRequiresAppBlock(t *testing.T) {
	// AuthMode=app but no `app:` block — must fail validation.
	bad := `
github:
  auth_mode: app
  scope: { org: o }
scaleset: { name: x, max_concurrent_runners: 5 }
proxmox:
  endpoint: https://h:8006/api2/json
  auth: { token_id: a!b, token_secret_env: TEST_PVE_TOKEN }
  template_vmid: 9000
  vmid_range: { min: 10000, max: 19999 }
  storage: { disk: d, snippets: s }
  network: { bridge: br0 }
nodes: { strategy: single, single_node: n1 }
pool: {}
`
	setEnv(t, map[string]string{"TEST_PVE_TOKEN": "y"})
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
}

func TestParse_AppAuth_BothClientIDAndAppID(t *testing.T) {
	pem := keyPath(t)
	bad := `
github:
  auth_mode: app
  app:
    client_id: "Iv23likB94"
    app_id: 1
    installation_id: 2
    private_key_path: ` + pem + `
  scope: { repo: o/r }
scaleset: { name: x, max_concurrent_runners: 5 }
proxmox:
  endpoint: https://h:8006/api2/json
  auth: { token_id: a!b, token_secret_env: TEST_PVE_TOKEN }
  template_vmid: 9000
  vmid_range: { min: 10000, max: 19999 }
  storage: { disk: d, snippets: s }
  network: { bridge: br0 }
nodes: { strategy: single, single_node: n1 }
pool: {}
`
	setEnv(t, map[string]string{"TEST_PVE_TOKEN": "y"})
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "both are present")
}

func TestParse_AppAuth_NeitherClientIDNorAppID(t *testing.T) {
	pem := keyPath(t)
	bad := `
github:
  auth_mode: app
  app:
    installation_id: 2
    private_key_path: ` + pem + `
  scope: { repo: o/r }
scaleset: { name: x, max_concurrent_runners: 5 }
proxmox:
  endpoint: https://h:8006/api2/json
  auth: { token_id: a!b, token_secret_env: TEST_PVE_TOKEN }
  template_vmid: 9000
  vmid_range: { min: 10000, max: 19999 }
  storage: { disk: d, snippets: s }
  network: { bridge: br0 }
nodes: { strategy: single, single_node: n1 }
pool: {}
`
	setEnv(t, map[string]string{"TEST_PVE_TOKEN": "y"})
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "both are empty")
}

func TestParse_AppAuth_ClientIDIsIssuer(t *testing.T) {
	pem := keyPath(t)
	good := `
github:
  auth_mode: app
  app:
    client_id: "Iv23likB94"
    installation_id: 2
    private_key_path: ` + pem + `
  scope: { org: o }
scaleset: { name: x, max_concurrent_runners: 5 }
proxmox:
  endpoint: https://h:8006/api2/json
  auth: { token_id: a!b, token_secret_env: TEST_PVE_TOKEN }
  template_vmid: 9000
  vmid_range: { min: 10000, max: 19999 }
  storage: { disk: d, snippets: s }
  network: { bridge: br0 }
nodes: { strategy: single, single_node: n1 }
pool: {}
`
	setEnv(t, map[string]string{"TEST_PVE_TOKEN": "y"})
	cfg, err := config.Parse([]byte(good))
	require.NoError(t, err)
	require.Equal(t, "Iv23likB94", cfg.GitHub.App.Issuer())
}

func TestParse_AppAuthLoadsValidApp(t *testing.T) {
	pem := keyPath(t)
	good := `
github:
  auth_mode: app
  app:
    app_id: 1
    installation_id: 2
    private_key_path: ` + pem + `
  scope: { repo: o/r }
scaleset: { name: x, max_concurrent_runners: 5 }
proxmox:
  endpoint: https://h:8006/api2/json
  auth: { token_id: a!b, token_secret_env: TEST_PVE_TOKEN }
  template_vmid: 9000
  vmid_range: { min: 10000, max: 19999 }
  storage: { disk: d, snippets: s }
  network: { bridge: br0 }
nodes: { strategy: single, single_node: n1 }
pool: {}
`
	setEnv(t, map[string]string{"TEST_PVE_TOKEN": "y"})
	cfg, err := config.Parse([]byte(good))
	require.NoError(t, err)
	require.Equal(t, int64(1), cfg.GitHub.App.AppID)
	require.Equal(t, "o/r", cfg.GitHub.Scope.Repo)
	require.Empty(t, cfg.GitHub.Scope.Org)
}

func TestLoad_ReadsFromDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(validPATYAML), 0o600))
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
	cfg, err := config.Load(path)
	require.NoError(t, err)
	require.Equal(t, "proxmox-ubuntu-x64", cfg.ScaleSet.Name)
}

// Cluster defaults: omitted cluster block falls back to standalone with
// sane raft timing defaults applied even though they're unused in
// standalone mode.
func TestCluster_DefaultsToStandalone(t *testing.T) {
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
	cfg, err := config.Parse([]byte(validPATYAML))
	require.NoError(t, err)
	require.Equal(t, "standalone", cfg.Cluster.Mode)
	require.Equal(t, time.Second, cfg.Cluster.Raft.HeartbeatTimeoutD)
	require.Equal(t, time.Second, cfg.Cluster.Raft.ElectionTimeoutD)
	require.Equal(t, 50*time.Millisecond, cfg.Cluster.Raft.CommitTimeoutD)
}

// Raft mode parses a complete peer list and confirms this replica is
// part of it. NodeID defaults to $HOSTNAME / os.Hostname() when YAML
// leaves it empty.
func TestCluster_RaftResolvesNodeIDFromHostname(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "x",
		"TEST_PVE_TOKEN": "y",
		"HOSTNAME":       "replica-a",
	})
	yaml := validPATYAML + `cluster:
  mode: raft
  raft:
    bind_addr: "0.0.0.0:7000"
    data_dir: "/var/lib/scaleset/raft"
    peers:
      - node_id: replica-a
        raft_addr: "10.0.0.1:7000"
        http_addr: "10.0.0.1:8080"
      - node_id: replica-b
        raft_addr: "10.0.0.2:7000"
        http_addr: "10.0.0.2:8080"
`
	cfg, err := config.Parse([]byte(yaml))
	require.NoError(t, err)
	require.Equal(t, "raft", cfg.Cluster.Mode)
	require.Equal(t, "replica-a", cfg.Cluster.Raft.NodeID)
	require.Equal(t, "/var/lib/scaleset/raft", cfg.Cluster.Raft.DataDir)
	require.Len(t, cfg.Cluster.Raft.Peers, 2)
}

// Raft mode without data_dir refuses to start: persistent raft stores
// are required for election safety. The check runs AFTER the topology
// validations so missing-peer / wrong-self errors surface first.
func TestCluster_RaftRequiresDataDir(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "x",
		"TEST_PVE_TOKEN": "y",
		"HOSTNAME":       "replica-a",
	})
	yaml := validPATYAML + `cluster:
  mode: raft
  raft:
    bind_addr: "0.0.0.0:7000"
    peers:
      - node_id: replica-a
        raft_addr: "10.0.0.1:7000"
        http_addr: "10.0.0.1:8080"
`
	_, err := config.Parse([]byte(yaml))
	require.Error(t, err)
	require.Contains(t, err.Error(), "data_dir")
}

// Raft mode without bind_addr refuses to start.
func TestCluster_RaftRequiresBindAddr(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "x",
		"TEST_PVE_TOKEN": "y",
		"HOSTNAME":       "replica-a",
	})
	yaml := validPATYAML + `cluster:
  mode: raft
  raft:
    peers:
      - node_id: replica-a
        raft_addr: "10.0.0.1:7000"
`
	_, err := config.Parse([]byte(yaml))
	require.Error(t, err)
	require.Contains(t, err.Error(), "bind_addr")
}

// Raft mode without an entry for this replica's NodeID is rejected.
func TestCluster_RaftRequiresSelfInPeers(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "x",
		"TEST_PVE_TOKEN": "y",
		"HOSTNAME":       "replica-a",
	})
	yaml := validPATYAML + `cluster:
  mode: raft
  raft:
    bind_addr: "0.0.0.0:7000"
    peers:
      - node_id: replica-b
        raft_addr: "10.0.0.2:7000"
`
	_, err := config.Parse([]byte(yaml))
	require.Error(t, err)
	require.Contains(t, err.Error(), "node_id")
}

// Raft mode with an empty peers list is rejected.
func TestCluster_RaftRequiresPeers(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "x",
		"TEST_PVE_TOKEN": "y",
		"HOSTNAME":       "replica-a",
	})
	yaml := validPATYAML + `cluster:
  mode: raft
  raft:
    bind_addr: "0.0.0.0:7000"
`
	_, err := config.Parse([]byte(yaml))
	require.Error(t, err)
	require.Contains(t, err.Error(), "peers")
}

// An unknown mode is rejected by the validator tags.
func TestCluster_RejectsUnknownMode(t *testing.T) {
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
	_, err := config.Parse([]byte(validPATYAML + "cluster:\n  mode: bogus\n"))
	require.Error(t, err)
}
