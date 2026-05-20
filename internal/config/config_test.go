package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/config"
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
