package config_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
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

func TestParse_ProxmoxEndpointScheme(t *testing.T) {
	cases := []struct {
		name      string
		endpoint  string
		wantError string // substring; empty means must succeed
	}{
		{"https ok", "https://pve.example.com:8006/api2/json", ""},
		{"https plain host", "https://pve.example.com", ""},
		{"http rejected", "http://pve.example.com:8006/api2/json", "must use https://"},
		{"ftp rejected", "ftp://pve.example.com/", "must use https://"},
		{"scheme-less rejected", "pve.example.com:8006", "must use https://"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
			yamlSrc := strings.Replace(validPATYAML,
				"endpoint: https://pve.example.com:8006/api2/json",
				"endpoint: "+tc.endpoint, 1)
			_, err := config.Parse([]byte(yamlSrc))
			if tc.wantError == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantError)
		})
	}
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

// TestParse_AdminAPI_TLSValidation: when admin_api.tls is set, the
// referenced files must exist; absent files surface as a Config.Validate
// error with the file path included for ops.
func TestParse_AdminAPI_TLSValidation(t *testing.T) {
	certPath, keyPath := writeSelfSignedKeypair(t)
	caPath := certPath // a self-signed cert doubles as its own CA bundle

	cases := []struct {
		name      string
		yamlBlock string
		wantErr   string
	}{
		{
			name:      "valid one-way tls",
			yamlBlock: "admin_api:\n  tls:\n    cert_file: " + certPath + "\n    key_file: " + keyPath + "\n",
		},
		{
			name:      "valid mtls",
			yamlBlock: "admin_api:\n  tls:\n    cert_file: " + certPath + "\n    key_file: " + keyPath + "\n    ca_file: " + caPath + "\n",
		},
		{
			name:      "missing cert_file",
			yamlBlock: "admin_api:\n  tls:\n    cert_file: /no/such/cert.pem\n    key_file: " + keyPath + "\n",
			wantErr:   "admin_api.tls.cert_file",
		},
		{
			name:      "missing key_file",
			yamlBlock: "admin_api:\n  tls:\n    cert_file: " + certPath + "\n    key_file: /no/such/key.pem\n",
			wantErr:   "admin_api.tls.key_file",
		},
		{
			name:      "missing ca_file",
			yamlBlock: "admin_api:\n  tls:\n    cert_file: " + certPath + "\n    key_file: " + keyPath + "\n    ca_file: /no/such/ca.pem\n",
			wantErr:   "admin_api.tls.ca_file",
		},
		{
			name:      "empty cert_file rejected",
			yamlBlock: "admin_api:\n  tls:\n    cert_file: \"\"\n    key_file: " + keyPath + "\n",
			wantErr:   "CertFile",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
			src := validPATYAML + tc.yamlBlock
			_, err := config.Parse([]byte(src))
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestTLSConfig_BuildServerAndClient: BuildServerTLS / BuildClientTLS
// must load a real keypair, and BuildServerTLS must enforce
// RequireAndVerifyClientCert when CAFile is set.
func TestTLSConfig_BuildServerAndClient(t *testing.T) {
	certPath, keyPath := writeSelfSignedKeypair(t)
	caPath := certPath // self-signed cert is its own root for this test

	t.Run("oneway", func(t *testing.T) {
		tc := config.TLSConfig{CertFile: certPath, KeyFile: keyPath}
		s, err := tc.BuildServerTLS()
		require.NoError(t, err)
		require.Equal(t, tls.NoClientCert, s.ClientAuth)
		require.NotEmpty(t, s.Certificates)
		c, err := tc.BuildClientTLS()
		require.NoError(t, err)
		require.Nil(t, c.RootCAs, "RootCAs unset when no CAFile")
	})
	t.Run("mtls", func(t *testing.T) {
		tc := config.TLSConfig{CertFile: certPath, KeyFile: keyPath, CAFile: caPath}
		s, err := tc.BuildServerTLS()
		require.NoError(t, err)
		require.Equal(t, tls.RequireAndVerifyClientCert, s.ClientAuth,
			"mTLS mode must require + verify the client cert")
		c, err := tc.BuildClientTLS()
		require.NoError(t, err)
		require.NotNil(t, c.RootCAs, "RootCAs populated when CAFile set")
	})
	t.Run("rejects bad pem", func(t *testing.T) {
		bad := filepath.Join(t.TempDir(), "junk.pem")
		require.NoError(t, os.WriteFile(bad, []byte("not a pem"), 0o600))
		tc := config.TLSConfig{CertFile: bad, KeyFile: bad}
		_, err := tc.BuildServerTLS()
		require.Error(t, err)
	})
}

// ---------------------------------------------------------------------------
// Profile parsing / validation tests (PR 1 — issues #2 + #3)
// ---------------------------------------------------------------------------

const validProfileYAML = `
github:
  auth_mode: pat
  pat:
    token_env: TEST_GH_TOKEN
  scope:
    org: my-org

scaleset:
  name: proxmox-ubuntu-x64
  labels: [self-hosted, linux, proxmox]
  max_concurrent_runners: 30

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
  global_max: 30
  reconcile_interval: 5s
  vm_max_age: 12h
  drain_timeout: 15m
  boot_max_attempts: 3

profiles:
  - name: linux-x64
    labels: [self-hosted, linux, proxmox, x64]
    template_vmid: 9001
    cpu: 4
    memory_mb: 8192
    hot_size: 5
    warm_size: 10
    max_concurrent_runners: 20
  - name: gpu
    labels: [self-hosted, linux, proxmox, gpu]
    template_vmid: 9100
    cpu: 8
    memory_mb: 32768
    hot_size: 0
    warm_size: 1
    max_concurrent_runners: 4
    vm_max_age: 6h
`

func TestProfiles_NoBlockSynthesizesDefault(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	cfg, err := config.Parse([]byte(validPATYAML))
	require.NoError(t, err)

	// The single-profile back-compat path must materialise one profile
	// inheriting the global pool/scaleset values.
	require.Len(t, cfg.Profiles, 1)
	p := cfg.Profiles[0]
	require.Equal(t, "default", p.Name)
	require.Equal(t, cfg.Pool.HotSize, p.HotSizeOrDefault(0))
	require.Equal(t, cfg.Pool.WarmSize, p.WarmSizeOrDefault(0))
	require.Equal(t, cfg.ScaleSet.MaxConcurrentRunners, p.MaxConcurrentRunnersOrDefault(0))
	require.Equal(t, cfg.Pool.BootMaxAttempts, p.BootMaxAttemptsOrDefault(0))
	require.Equal(t, cfg.ScaleSet.Labels, p.Labels)
}

func TestProfiles_ExplicitBlockParses(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	cfg, err := config.Parse([]byte(validProfileYAML))
	require.NoError(t, err)

	require.Len(t, cfg.Profiles, 2)
	x64 := cfg.Profiles[0]
	require.Equal(t, "linux-x64", x64.Name)
	require.Equal(t, []string{"self-hosted", "linux", "proxmox", "x64"}, x64.Labels)
	require.Equal(t, 9001, x64.TemplateVMID)
	require.Equal(t, 4, x64.CPUCores)
	require.Equal(t, 8192, x64.MemoryMB)
	require.Equal(t, 5, x64.HotSizeOrDefault(-1))
	require.Equal(t, 10, x64.WarmSizeOrDefault(-1))
	require.Equal(t, 20, x64.MaxConcurrentRunnersOrDefault(-1))
	// Inherited from global pool (omitted on profile).
	require.Equal(t, cfg.Pool.BootMaxAttempts, x64.BootMaxAttemptsOrDefault(0))
	require.Equal(t, cfg.Pool.VMMaxAge, x64.VMMaxAge)

	gpu := cfg.Profiles[1]
	require.Equal(t, "gpu", gpu.Name)
	// Explicit 0 stays 0 — not overwritten by the global default of 2.
	require.Equal(t, 0, gpu.HotSizeOrDefault(99))
	require.Equal(t, 1, gpu.WarmSizeOrDefault(99))
	// Per-profile override takes precedence over global pool.vm_max_age.
	require.Equal(t, "6h", gpu.VMMaxAge)
	require.Equal(t, 6*time.Hour, gpu.VMMaxAgeD)
}

func TestProfiles_GlobalMaxRejectsOversum(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	// linux-x64.max + gpu.max = 20 + 4 = 24; tightening global_max
	// below that must fail at validation time.
	oversum := strings.Replace(validProfileYAML, "global_max: 30", "global_max: 10", 1)
	_, err := config.Parse([]byte(oversum))
	require.Error(t, err)
	require.Contains(t, err.Error(), "global_max")
}

func TestProfiles_DuplicateNameRejected(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	dup := strings.Replace(validProfileYAML, "name: gpu", "name: linux-x64", 1)
	_, err := config.Parse([]byte(dup))
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate name")
}

func TestProfiles_InvalidNameRejected(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	bad := strings.Replace(validProfileYAML, "name: gpu", `name: "GPU 1"`, 1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "must match")
}

func TestProfiles_UncoveredScaleSetLabelRejected(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	// Strip "proxmox" off both profiles so neither covers the
	// scaleset's declared `proxmox` label. The router-coverage
	// invariant must reject this at config load time rather than
	// surfacing it as a per-job ErrNoMatchingProfile in production.
	uncovered := strings.ReplaceAll(validProfileYAML,
		"[self-hosted, linux, proxmox, x64]", "[self-hosted, linux, x64]")
	uncovered = strings.ReplaceAll(uncovered,
		"[self-hosted, linux, proxmox, gpu]", "[self-hosted, linux, gpu]")
	_, err := config.Parse([]byte(uncovered))
	require.Error(t, err)
	require.Contains(t, err.Error(), "no profile covers scaleset labels")
	require.Contains(t, err.Error(), "proxmox")
}

func TestProfiles_HotPlusWarmExceedsMaxRejected(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	// linux-x64 hot=5 warm=10; lower its max to 12.
	bad := strings.Replace(validProfileYAML,
		"max_concurrent_runners: 20", "max_concurrent_runners: 12", 1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "hot_size+warm_size")
}

func TestProfiles_VMMaxAgeZeroRejected(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	// gpu profile sets vm_max_age: 6h; flip it to 0s.
	bad := strings.Replace(validProfileYAML, "vm_max_age: 6h", `vm_max_age: "0s"`, 1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "vm_max_age must be positive")
	require.Contains(t, err.Error(), "gpu")
}

func TestProfiles_VMMaxAgeNegativeRejected(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	bad := strings.Replace(validProfileYAML, "vm_max_age: 6h", `vm_max_age: "-5m"`, 1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "vm_max_age must be positive")
}

func TestProfiles_VMMaxAgePositiveAccepted(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	ok := strings.Replace(validProfileYAML, "vm_max_age: 6h", "vm_max_age: 30m", 1)
	cfg, err := config.Parse([]byte(ok))
	require.NoError(t, err)
	require.Equal(t, 30*time.Minute, cfg.Profiles[1].VMMaxAgeD)
}

// writeSelfSignedKeypair generates a fresh self-signed cert + key on
// disk and returns their paths. The cert is valid for "localhost" only
// — enough for our admin-API tests, which dial loopback.
func writeSelfSignedKeypair(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)

	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")

	cfh, err := os.Create(certPath)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(cfh, &pem.Block{Type: "CERTIFICATE", Bytes: der}))
	require.NoError(t, cfh.Close())

	keyDER, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)
	kfh, err := os.Create(keyPath)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(kfh, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	require.NoError(t, kfh.Close())

	return certPath, keyPath
}
