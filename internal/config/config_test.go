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
	"reflect"
	"runtime"
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
    token: testtoken
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
    token_secret: testsecret
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
	cfg, err := config.Parse([]byte(validPATYAML))
	require.NoError(t, err)

	// Secrets populated from the yaml-side test value.
	require.Equal(t, "testtoken", cfg.GitHub.PAT.Token)
	require.Equal(t, "testsecret", cfg.Proxmox.Auth.TokenSecret)

	// Durations parsed.
	require.Equal(t, 5*time.Second, cfg.Pool.ReconcileInterval.D())
	require.Equal(t, 12*time.Hour, cfg.Pool.VMMaxAge.D())
	require.Equal(t, 15*time.Minute, cfg.Pool.DrainTimeout.D())
}

// TestParse_EnvOverridesYAML covers the koanf env layer winning over a
// yaml-supplied value. Sets a non-secret yaml field, overrides it via
// the canonical SCALESET_* env var, asserts env wins.
func TestParse_EnvOverridesYAML(t *testing.T) {
	// validPATYAML has scaleset.name=proxmox-ubuntu-x64.
	setEnv(t, map[string]string{"SCALESET_SCALESET_NAME": "from-env"})
	cfg, err := config.Parse([]byte(validPATYAML))
	require.NoError(t, err)
	require.Equal(t, "from-env", cfg.ScaleSet.Name,
		"SCALESET_SCALESET_NAME must override the yaml-supplied scaleset.name")
}

// TestParse_SecretFromEnvOnly covers the canonical production shape:
// yaml omits the secret entirely, env supplies it via SCALESET_*. The
// resolved Config carries the env value.
func TestParse_SecretFromEnvOnly(t *testing.T) {
	noSecretsYAML := strings.ReplaceAll(validPATYAML, "    token: testtoken", "")
	noSecretsYAML = strings.ReplaceAll(noSecretsYAML, "    token_secret: testsecret", "")
	setEnv(t, map[string]string{
		"SCALESET_GITHUB_PAT_TOKEN":          "ghp_from_env",
		"SCALESET_PROXMOX_AUTH_TOKEN_SECRET": "pve_from_env",
	})
	cfg, err := config.Parse([]byte(noSecretsYAML))
	require.NoError(t, err)
	require.Equal(t, "ghp_from_env", cfg.GitHub.PAT.Token)
	require.Equal(t, "pve_from_env", cfg.Proxmox.Auth.TokenSecret)
}

// TestParse_MissingSecret covers the "neither yaml nor env supplied
// the secret" path: Resolve must reject with a clear error naming the
// koanf key and the canonical SCALESET_* env var operators should set.
func TestParse_MissingSecret(t *testing.T) {
	noProxmoxSecretYAML := strings.ReplaceAll(validPATYAML, "    token_secret: testsecret", "")
	_, err := config.Parse([]byte(noProxmoxSecretYAML))
	require.Error(t, err)
	require.Contains(t, err.Error(), "proxmox.auth.token_secret")
	require.Contains(t, err.Error(), "SCALESET_PROXMOX_AUTH_TOKEN_SECRET")
}

// TestParse_UnknownEnvVarSurfaced covers the typo-detection path: a
// SCALESET_*-prefixed env var that doesn't map to any schema key
// doesn't fail the load (so host env pollution can't crash the
// orchestrator), but Config.UnknownEnvOverrides surfaces it so the
// caller can warn. Operators who typo SCALESET_POOL_HOTSIZE instead of
// SCALESET_POOL_HOT_SIZE see the mistake at startup.
//
// Pairs real and bogus keys in the same Setenv batch so the negative
// case (a valid override must NOT appear in the unknowns list) is
// exercised alongside the positive cases.
func TestParse_UnknownEnvVarSurfaced(t *testing.T) {
	setEnv(t, map[string]string{
		// Bogus — should appear in the unknowns list.
		"SCALESET_TOTALLY_MADE_UP_KEY": "whatever",
		"SCALESET_POOL_HOTSIZE":        "10", // typo: missing _ between hot and size
		// Real — should be applied and NOT appear in the unknowns list.
		"SCALESET_POOL_HOT_SIZE":           "7",
		"SCALESET_OBSERVABILITY_LOG_LEVEL": "warn",
	})
	cfg, err := config.Parse([]byte(validPATYAML))
	require.NoError(t, err)

	unknowns := cfg.UnknownEnvOverrides()
	require.Contains(t, unknowns, "SCALESET_TOTALLY_MADE_UP_KEY")
	require.Contains(t, unknowns, "SCALESET_POOL_HOTSIZE")
	require.NotContains(t, unknowns, "SCALESET_POOL_HOT_SIZE",
		"a real override must not be reported as unknown")
	require.NotContains(t, unknowns, "SCALESET_OBSERVABILITY_LOG_LEVEL",
		"a real override must not be reported as unknown")

	// Sanity: the real overrides also took effect.
	require.Equal(t, 7, cfg.Pool.HotSize)
	require.Equal(t, "warn", cfg.Observability.LogLevel)
}

// TestParse_ProfileCanaryFieldsRoundTripThroughKoanf locks in that the
// canary-rollout fields added under ProfileConfig (canary_template_vmid,
// canary_percent, canary_max_failure_rate) are recognised by the koanf
// strict-mode walk and unmarshal into the struct. A regression here
// would silently swallow operator-specified canary settings.
func TestParse_ProfileCanaryFieldsRoundTripThroughKoanf(t *testing.T) {
	src := validPATYAML + `
profiles:
  - name: linux-x64
    labels: [self-hosted, linux, proxmox]
    template_vmid: 9000
    cpu: 2
    memory_mb: 4096
    max_concurrent_runners: 5
    canary_template_vmid: 9500
    canary_percent: 25
    canary_max_failure_rate: 0.15
`
	cfg, err := config.Parse([]byte(src))
	require.NoError(t, err)
	require.Len(t, cfg.Profiles, 1)
	p := cfg.Profiles[0]
	require.Equal(t, 9500, p.CanaryTemplateVMID)
	require.Equal(t, 25, p.CanaryPercent)
	require.InDelta(t, 0.15, p.CanaryMaxFailureRate, 1e-9)
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
  pat: { token: testtoken }
  scope: { org: o }
scaleset:
  name: x
  max_concurrent_runners: 5
proxmox:
  endpoint: https://h:8006/api2/json
  auth: { token_id: a!b, token_secret: testsecret }
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

	require.Equal(t, 10*time.Second, cfg.Pool.ReconcileInterval.D())
	require.Equal(t, 24*time.Hour, cfg.Pool.VMMaxAge.D())
	require.Equal(t, 30*time.Minute, cfg.Pool.DrainTimeout.D())
	require.Equal(t, 3, cfg.Pool.BootMaxAttempts)
	require.Equal(t, ":9100", cfg.Observability.HTTPAddr)
	require.Equal(t, "info", cfg.Observability.LogLevel)
	require.Equal(t, "json", cfg.Observability.LogFormat)
	require.True(t, cfg.Proxmox.Clone.LinkedOrDefault(), "linked clones default to true when omitted")
	require.Nil(t, cfg.Proxmox.Clone.Linked, "default leaves the underlying field nil")

	// GitHub reconciler defaults: the production failure mode this whole
	// package was built for (over-provisioned runners that GitHub never
	// assigns) only resolves cleanly if these have sensible fallbacks.
	require.Equal(t, 15*time.Second, cfg.GitHub.PollInterval.D())
	require.Equal(t, 5*time.Minute, cfg.GitHub.AssignedGrace.D())
	require.Equal(t, 30*time.Second, cfg.GitHub.RunningIdleGrace.D())
	require.Equal(t, 2*time.Minute, cfg.GitHub.AssignedOfflineGrace.D())

	// Power-state poller default.
	require.Equal(t, 3*time.Second, cfg.Pool.PowerPollInterval.D())

	// Race-fix grace/cooldown defaults (pre-existing race bug fixes).
	require.Equal(t, 30*time.Second, cfg.Pool.VMIDReuseCooldown.D())
	require.Equal(t, 60*time.Second, cfg.Pool.OrphanGrace.D())
	require.Equal(t, 5*time.Minute, cfg.Pool.CloneInflightGrace.D())
}

// TestPool_RaceGraceKnobs_AcceptOverrides confirms the three pool grace
// knobs introduced for the over-provision / VMID-reuse / orphan-sweep
// races round-trip through ApplyDefaults + Resolve.
func TestPool_RaceGraceKnobs_AcceptOverrides(t *testing.T) {
	yaml := `
github:
  auth_mode: pat
  pat: { token: testtoken }
  scope: { org: o }
scaleset: { name: x, max_concurrent_runners: 5 }
proxmox:
  endpoint: https://h:8006/api2/json
  auth: { token_id: a!b, token_secret: testsecret }
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
	require.Equal(t, 45*time.Second, cfg.Pool.VMIDReuseCooldown.D())
	require.Equal(t, 90*time.Second, cfg.Pool.OrphanGrace.D())
	require.Equal(t, 10*time.Minute, cfg.Pool.CloneInflightGrace.D())
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
  pat: { token: testtoken }
  scope: { org: o }
scaleset: { name: x, max_concurrent_runners: 5 }
proxmox:
  endpoint: https://h:8006/api2/json
  auth: { token_id: a!b, token_secret: testsecret }
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
  pat: { token: testtoken }
  scope: { org: o }
  poll_interval: 7s
  assigned_grace: 99m
  running_idle_grace: 11s
  assigned_offline_grace: 45s
scaleset: { name: x, max_concurrent_runners: 5 }
proxmox:
  endpoint: https://h:8006/api2/json
  auth: { token_id: a!b, token_secret: testsecret }
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
	require.Equal(t, 7*time.Second, cfg.GitHub.PollInterval.D())
	require.Equal(t, 99*time.Minute, cfg.GitHub.AssignedGrace.D())
	require.Equal(t, 11*time.Second, cfg.GitHub.RunningIdleGrace.D())
	require.Equal(t, 45*time.Second, cfg.GitHub.AssignedOfflineGrace.D())
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
  auth: { token_id: a!b, token_secret: testsecret }
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
  auth: { token_id: a!b, token_secret: testsecret }
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
  auth: { token_id: a!b, token_secret: testsecret }
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
  auth: { token_id: a!b, token_secret: testsecret }
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
  auth: { token_id: a!b, token_secret: testsecret }
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

// TestLoad_RejectsWorldReadablePerms pins the perm check Load applies to
// the config file. The file holds Proxmox tokens, the webhook secret,
// and the admin bearer — same class as the PEM file we already protect.
// World/group-readable mode bits must refuse at startup, not log-and-go.
func TestLoad_RejectsWorldReadablePerms(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows mode bits don't map onto POSIX r/w/x")
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(validPATYAML), 0o600))
	require.NoError(t, os.Chmod(path, 0o644))
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
	_, err := config.Load(path)
	require.Error(t, err)
	require.Contains(t, err.Error(), "insecure mode")
	require.Contains(t, err.Error(), "0644")
}

// TestLoad_AcceptsTight pins the happy-path: mode 0600 owned by us
// passes both checks.
func TestLoad_AcceptsTight(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(validPATYAML), 0o600))
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
	_, err := config.Load(path)
	require.NoError(t, err)
}

// Cluster defaults: omitted cluster block falls back to standalone with
// sane raft timing defaults applied even though they're unused in
// standalone mode.
func TestCluster_DefaultsToStandalone(t *testing.T) {
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "x", "TEST_PVE_TOKEN": "y"})
	cfg, err := config.Parse([]byte(validPATYAML))
	require.NoError(t, err)
	require.Equal(t, "standalone", cfg.Cluster.Mode)
	require.Equal(t, time.Second, cfg.Cluster.Raft.HeartbeatTimeout.D())
	require.Equal(t, time.Second, cfg.Cluster.Raft.ElectionTimeout.D())
	require.Equal(t, 50*time.Millisecond, cfg.Cluster.Raft.CommitTimeout.D())
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
    token: testtoken
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
    token_secret: testsecret
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
	require.Equal(t, 6*time.Hour, gpu.VMMaxAge.D())
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
	require.Contains(t, err.Error(), "no profile covers labels")
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
	require.Equal(t, 30*time.Minute, cfg.Profiles[1].VMMaxAge.D())
}

// TestProfiles_BootMaxAttemptsZeroRejected pins the per-profile
// boot_max_attempts >= 1 validator. Accepting 0 would silently poison
// every VM in the affected profile on its first failed boot once the
// poisoning decision honors the per-profile threshold.
func TestProfiles_BootMaxAttemptsZeroRejected(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	bad := strings.Replace(validProfileYAML,
		"max_concurrent_runners: 4",
		"max_concurrent_runners: 4\n    boot_max_attempts: 0", 1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "boot_max_attempts")
	require.Contains(t, err.Error(), "must be >= 1")
}

// ---------------------------------------------------------------------------
// Quotas + Priority parsing/validation tests (PR 5 — issues #4 + #10)
// ---------------------------------------------------------------------------

func TestQuotas_ParsedFromYAML(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	withQuotas := validPATYAML + `
quotas:
  default_per_repo: 5
  default_per_org: 20
  overrides:
    - match: { repo: "acme/heavy-ci" }
      max_concurrent: 15
    - match: { org: "acme-platform" }
      max_concurrent: 30
`
	cfg, err := config.Parse([]byte(withQuotas))
	require.NoError(t, err)
	require.Equal(t, 5, cfg.Quotas.DefaultPerRepo)
	require.Equal(t, 20, cfg.Quotas.DefaultPerOrg)
	require.Len(t, cfg.Quotas.Overrides, 2)
	require.Equal(t, "acme/heavy-ci", cfg.Quotas.Overrides[0].Match.Repo)
	require.Equal(t, 15, cfg.Quotas.Overrides[0].MaxConcurrent)
	require.Equal(t, "acme-platform", cfg.Quotas.Overrides[1].Match.Org)
	require.Equal(t, 30, cfg.Quotas.Overrides[1].MaxConcurrent)
}

func TestQuotas_RejectsAmbiguousOverride(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	bad := validPATYAML + `
quotas:
  overrides:
    - match: { org: "acme", repo: "acme/platform" }
      max_concurrent: 5
`
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "exactly one of org or repo")
}

func TestQuotas_RejectsBothEmptyOverride(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	bad := validPATYAML + `
quotas:
  overrides:
    - match: {}
      max_concurrent: 5
`
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "exactly one of org or repo")
}

func TestQuotas_RejectsNegativeDefaults(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	bad := validPATYAML + `
quotas:
  default_per_repo: -1
`
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
}

func TestPriority_ParsedFromYAML(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	withPriority := validPATYAML + `
priority:
  classes:
    - name: critical
      match: { workflow_label: "priority:critical" }
      weight: 100
      preempt: true
    - name: standard
      weight: 10
    - name: best_effort
      match: { repo: "acme/research" }
      weight: 1
`
	cfg, err := config.Parse([]byte(withPriority))
	require.NoError(t, err)
	require.Len(t, cfg.Priority.Classes, 3)

	critical := cfg.Priority.Classes[0]
	require.Equal(t, "critical", critical.Name)
	require.Equal(t, "priority:critical", critical.Match.WorkflowLabel)
	require.Equal(t, 100, critical.Weight)
	require.True(t, critical.Preempt)

	require.Equal(t, "standard", cfg.Priority.Classes[1].Name)
	require.False(t, cfg.Priority.Classes[1].Preempt, "preempt defaults to false")

	require.Equal(t, "acme/research", cfg.Priority.Classes[2].Match.Repo)
}

func TestPriority_RejectsEmptyName(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	bad := validPATYAML + `
priority:
  classes:
    - weight: 100
`
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
}

func TestPriority_RejectsDuplicateName(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	bad := validPATYAML + `
priority:
  classes:
    - name: critical
      weight: 100
    - name: critical
      weight: 50
`
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")
}

// ---------------------------------------------------------------------------
// Node affinity (PR 6 — issue #8)
// ---------------------------------------------------------------------------

func TestNodeAffinity_ParsedFromYAML(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	withAffinity := validProfileYAML + `
nodes:
  strategy: round_robin
  members: [pve1, pve2, pve-gpu-1]
  affinity:
    - match: { profile: gpu }
      prefer_nodes: [pve-gpu-1]
      require: true
    - match: { profile: linux-x64 }
      anti_affinity_with: { profile: gpu }
`
	// validProfileYAML already sets nodes.strategy: single — strip
	// it so the appended block doesn't duplicate the key.
	withAffinity = strings.Replace(withAffinity,
		"nodes:\n  strategy: single\n  single_node: pve1\n",
		"", 1)
	cfg, err := config.Parse([]byte(withAffinity))
	require.NoError(t, err)
	require.Len(t, cfg.Nodes.Affinity, 2)

	gpu := cfg.Nodes.Affinity[0]
	require.Equal(t, "gpu", gpu.Match.Profile)
	require.Equal(t, []string{"pve-gpu-1"}, gpu.PreferNodes)
	require.True(t, gpu.Require)

	x64 := cfg.Nodes.Affinity[1]
	require.Equal(t, "linux-x64", x64.Match.Profile)
	require.Equal(t, "gpu", x64.AntiAffinityWith.Profile)
}

func TestNodeAffinity_RejectsUnknownProfileInMatch(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	bad := validPATYAML + `
nodes:
  strategy: round_robin
  members: [pve1]
  affinity:
    - match: { profile: not-a-real-profile }
      prefer_nodes: [pve1]
      require: true
`
	bad = strings.Replace(bad, "nodes:\n  strategy: single\n  single_node: pve1\n", "", 1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "not-a-real-profile")
	require.Contains(t, err.Error(), "not a declared profile")
}

func TestNodeAffinity_RejectsUnknownNodeInPreferNodes(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	// Use validProfileYAML so "gpu" is a declared profile, then
	// reference a node that isn't in members.
	bad := validProfileYAML + `
nodes:
  strategy: round_robin
  members: [pve1, pve2]
  affinity:
    - match: { profile: gpu }
      prefer_nodes: [pve-nonexistent]
      require: true
`
	bad = strings.Replace(bad, "nodes:\n  strategy: single\n  single_node: pve1\n", "", 1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "pve-nonexistent")
}

func TestNodeAffinity_RequireTrueWithoutPreferNodesRejected(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	bad := validProfileYAML + `
nodes:
  strategy: round_robin
  members: [pve1, pve2]
  affinity:
    - match: { profile: gpu }
      require: true
`
	bad = strings.Replace(bad, "nodes:\n  strategy: single\n  single_node: pve1\n", "", 1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "require=true is only meaningful with prefer_nodes")
}

func TestNodeAffinity_EmptyAffinityIsValid(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	// Pre-PR-6 configs (no affinity block) must keep loading.
	_, err := config.Parse([]byte(validPATYAML))
	require.NoError(t, err)
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

// ---------------------------------------------------------------------------
// Per-profile network + IPAM (PR 3 — issue #6)
// ---------------------------------------------------------------------------

func TestProfileNetwork_ParsedFromYAML(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	withNet := strings.Replace(validProfileYAML,
		"    template_vmid: 9100",
		`    template_vmid: 9100
    network:
      bridge: vmbr1
      vlan_tag: 30
      mtu: 9000
      extra_nics:
        - bridge: vmbr-storage
          vlan_tag: 100
          mtu: 9000
      ipam:
        backend: static
        pool:
          - 10.0.0.10/24
          - 10.0.0.11/24`, 1)
	cfg, err := config.Parse([]byte(withNet))
	require.NoError(t, err)
	require.Len(t, cfg.Profiles, 2)

	gpu := cfg.Profiles[1]
	require.NotNil(t, gpu.Network)
	require.Equal(t, "vmbr1", gpu.Network.Bridge)
	require.Equal(t, 30, gpu.Network.VLANTag)
	require.Equal(t, 9000, gpu.Network.MTU)
	require.Len(t, gpu.Network.ExtraNICs, 1)
	require.Equal(t, "vmbr-storage", gpu.Network.ExtraNICs[0].Bridge)
	require.NotNil(t, gpu.Network.IPAM)
	require.Equal(t, "static", gpu.Network.IPAM.Backend)
	require.Equal(t, []string{"10.0.0.10/24", "10.0.0.11/24"}, gpu.Network.IPAM.Pool)
}

func TestProfileNetwork_StaticBackendRequiresPool(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	bad := strings.Replace(validProfileYAML,
		"    template_vmid: 9100",
		`    template_vmid: 9100
    network:
      ipam:
        backend: static`, 1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "ipam.pool is required when backend=static")
}

func TestProfileNetwork_NoopBackendRejectsPool(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	bad := strings.Replace(validProfileYAML,
		"    template_vmid: 9100",
		`    template_vmid: 9100
    network:
      ipam:
        backend: noop
        pool: ["10.0.0.10/24"]`, 1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "ipam.pool must be empty when backend=noop")
}

func TestProfileNetwork_RejectsInvalidBackend(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	bad := strings.Replace(validProfileYAML,
		"    template_vmid: 9100",
		`    template_vmid: 9100
    network:
      ipam:
        backend: netbox`, 1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err, "backend not in oneof must fail validation")
}

func TestProfileNetwork_RejectsOutOfRangeVLAN(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	bad := strings.Replace(validProfileYAML,
		"    template_vmid: 9100",
		`    template_vmid: 9100
    network:
      bridge: vmbr0
      vlan_tag: 5000`, 1) // > 4094
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
}

func TestProfileNetwork_OmittedIsValid(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	// validProfileYAML doesn't set network anywhere; back-compat.
	cfg, err := config.Parse([]byte(validProfileYAML))
	require.NoError(t, err)
	for _, p := range cfg.Profiles {
		require.Nil(t, p.Network, "no network block: profile.Network must remain nil")
	}
}

func TestSchedules_ProfileScheduleParses(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	withSched := strings.Replace(validProfileYAML,
		"    max_concurrent_runners: 20",
		`    max_concurrent_runners: 20
    schedules:
      - name: business-hours
        cron: "0 8 * * 1-5"
        duration: 10h
        timezone: America/New_York
        hot_size: 5
        warm_size: 10`, 1)
	cfg, err := config.Parse([]byte(withSched))
	require.NoError(t, err)

	x64 := cfg.Profiles[0]
	require.Len(t, x64.Schedules, 1)
	s := x64.Schedules[0]
	require.Equal(t, "business-hours", s.Name)
	require.Equal(t, "0 8 * * 1-5", s.Cron)
	require.Equal(t, 10*time.Hour, s.Duration.D())
	require.NotNil(t, s.Location)
	require.Equal(t, "America/New_York", s.Location.String())
	require.Equal(t, 5, s.HotSize)
	require.Equal(t, 10, s.WarmSize)
}

func TestSchedules_RejectsInvalidCron(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	bad := strings.Replace(validProfileYAML,
		"    max_concurrent_runners: 20",
		`    max_concurrent_runners: 20
    schedules:
      - name: bad
        cron: "garbage spec"
        duration: 1h
        hot_size: 1
        warm_size: 1`, 1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "cron")
}

func TestSchedules_RejectsSizesExceedingMaxConcurrent(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	bad := strings.Replace(validProfileYAML,
		"    max_concurrent_runners: 4",
		`    max_concurrent_runners: 4
    schedules:
      - name: oversized
        cron: "0 8 * * *"
        duration: 1h
        hot_size: 10
        warm_size: 10`, 1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "max_concurrent_runners")
}

func TestSchedules_RejectsDuplicateNames(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	bad := strings.Replace(validProfileYAML,
		"    max_concurrent_runners: 20",
		`    max_concurrent_runners: 20
    schedules:
      - name: dup
        cron: "0 8 * * *"
        duration: 1h
        hot_size: 1
        warm_size: 1
      - name: dup
        cron: "0 18 * * *"
        duration: 1h
        hot_size: 1
        warm_size: 1`, 1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")
}

func TestSchedules_PoolSchedulesInheritsIntoDefaultProfile(t *testing.T) {
	setEnv(t, map[string]string{
		"TEST_GH_TOKEN":  "ghp_fake",
		"TEST_PVE_TOKEN": "pve-secret",
	})
	withGlobal := strings.Replace(validPATYAML,
		"  boot_max_attempts: 3",
		`  boot_max_attempts: 3
  schedules:
    - name: night
      cron: "0 22 * * *"
      duration: 8h
      hot_size: 0
      warm_size: 1`, 1)
	cfg, err := config.Parse([]byte(withGlobal))
	require.NoError(t, err)
	require.Len(t, cfg.Profiles, 1)
	require.Len(t, cfg.Profiles[0].Schedules, 1)
	require.Equal(t, "night", cfg.Profiles[0].Schedules[0].Name)
}

const validMultiScalesetYAML = `
github:
  auth_mode: pat
  pat:
    token: testtoken

scalesets:
  - name: linux-x64
    labels: [self-hosted, linux, proxmox, x64]
    max_concurrent_runners: 10
    scope:
      org: org-a
    vmid_range: { min: 10000, max: 14999 }
  - name: gpu-pool
    labels: [self-hosted, linux, gpu]
    max_concurrent_runners: 4
    scope:
      org: org-b
    vmid_range: { min: 15000, max: 19999 }
    profiles:
      - name: gpu
        labels: [self-hosted, linux, gpu]
        template_vmid: 9100
        cpu: 8
        memory_mb: 32768
        hot_size: 0
        warm_size: 1
        max_concurrent_runners: 4

proxmox:
  endpoint: https://pve.example.com:8006/api2/json
  auth:
    token_id: scaleset@pve!automation
    token_secret: testsecret
  template_vmid: 9000
  vmid_range: { min: 10000, max: 19999 }
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

func TestScalesets_PluralFormParses(t *testing.T) {
	cfg, err := config.Parse([]byte(validMultiScalesetYAML))
	require.NoError(t, err)
	require.Len(t, cfg.Scalesets, 2)
	require.Equal(t, "linux-x64", cfg.Scalesets[0].Name)
	require.Equal(t, "org-a", cfg.Scalesets[0].Scope.Org)
	require.Equal(t, "gpu-pool", cfg.Scalesets[1].Name)
	require.Equal(t, "org-b", cfg.Scalesets[1].Scope.Org)
	// linux-x64 omits profiles; ApplyDefaults synthesised one.
	require.Len(t, cfg.Scalesets[0].Profiles, 1)
	require.Equal(t, "default", cfg.Scalesets[0].Profiles[0].Name)
	// gpu-pool declared its own profile.
	require.Len(t, cfg.Scalesets[1].Profiles, 1)
	require.Equal(t, "gpu", cfg.Scalesets[1].Profiles[0].Name)
}

func TestScalesets_SingularFormFoldsToPlural(t *testing.T) {
	cfg, err := config.Parse([]byte(validPATYAML))
	require.NoError(t, err)
	require.Len(t, cfg.Scalesets, 1, "singular scaleset must normalise to 1-element Scalesets")
	s := cfg.Scalesets[0]
	require.Equal(t, "proxmox-ubuntu-x64", s.Name)
	require.Equal(t, "my-org", s.Scope.Org)
	require.Len(t, s.Profiles, 1)
	// Backwards-compat projection: legacy fields stay populated.
	require.Equal(t, s.Name, cfg.ScaleSet.Name)
	require.Equal(t, s.Scope, cfg.GitHub.Scope)
}

func TestScalesets_MixingLegacyAndPluralRejected(t *testing.T) {
	mixed := strings.Replace(validMultiScalesetYAML,
		"github:\n  auth_mode: pat\n  pat:\n    token: testtoken\n",
		"github:\n  auth_mode: pat\n  pat:\n    token: testtoken\n  scope:\n    org: leftover\n",
		1)
	_, err := config.Parse([]byte(mixed))
	require.Error(t, err)
	require.Contains(t, err.Error(), "scalesets:")
	require.Contains(t, err.Error(), "github.scope")
}

func TestScalesets_DuplicateScopeRejected(t *testing.T) {
	dup := strings.Replace(validMultiScalesetYAML, "org: org-b", "org: org-a", 1)
	_, err := config.Parse([]byte(dup))
	require.Error(t, err)
	require.Contains(t, err.Error(), "share the same scope")
}

func TestScalesets_DuplicateNameRejected(t *testing.T) {
	dup := strings.Replace(validMultiScalesetYAML, "name: gpu-pool", "name: linux-x64", 1)
	_, err := config.Parse([]byte(dup))
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate name")
}

func TestScalesets_BothOrgAndRepoInScopeRejected(t *testing.T) {
	bad := strings.Replace(validMultiScalesetYAML,
		"    scope:\n      org: org-a",
		"    scope:\n      org: org-a\n      repo: org-a/repo",
		1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "exactly one of org or repo")
}

func TestScalesets_ProfileNamespacedPerScaleset(t *testing.T) {
	// Both scalesets get a "default" profile from ApplyDefaults
	// (no profiles block declared) — must not collide.
	bare := `
github:
  auth_mode: pat
  pat:
    token: testtoken
scalesets:
  - name: a
    labels: [a]
    max_concurrent_runners: 5
    scope: { org: org-a }
    vmid_range: { min: 10000, max: 14999 }
  - name: b
    labels: [b]
    max_concurrent_runners: 5
    scope: { org: org-b }
    vmid_range: { min: 15000, max: 19999 }
proxmox:
  endpoint: https://pve.example.com:8006/api2/json
  auth:
    token_id: scaleset@pve!automation
    token_secret: testsecret
  template_vmid: 9000
  vmid_range: { min: 10000, max: 19999 }
  storage: { disk: local-lvm, snippets: local }
  network: { bridge: vmbr0 }
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
	cfg, err := config.Parse([]byte(bare))
	require.NoError(t, err)
	require.Len(t, cfg.Scalesets, 2)
	require.Equal(t, "default", cfg.Scalesets[0].Profiles[0].Name)
	require.Equal(t, "default", cfg.Scalesets[1].Profiles[0].Name)
}

// TestDuration_UnmarshalText pins the parsing semantics: empty leaves
// the value unset, a valid string sets it, a malformed string returns
// a wrapped error naming the offending input. Tests the type itself,
// independent of the full Parse pipeline, so a regression in the
// koanf wiring is easy to bisect.
func TestDuration_UnmarshalText(t *testing.T) {
	t.Run("empty leaves unset", func(t *testing.T) {
		var d config.Duration
		require.NoError(t, d.UnmarshalText([]byte("")))
		require.False(t, d.Set())
		require.Equal(t, time.Duration(0), d.D())
	})
	t.Run("whitespace leaves unset", func(t *testing.T) {
		var d config.Duration
		require.NoError(t, d.UnmarshalText([]byte("  \t\n")))
		require.False(t, d.Set())
	})
	t.Run("valid duration sets value", func(t *testing.T) {
		var d config.Duration
		require.NoError(t, d.UnmarshalText([]byte("15s")))
		require.True(t, d.Set())
		require.Equal(t, 15*time.Second, d.D())
	})
	t.Run("malformed duration returns wrapped error", func(t *testing.T) {
		var d config.Duration
		err := d.UnmarshalText([]byte("nope"))
		require.Error(t, err)
		require.Contains(t, err.Error(), `"nope"`)
		require.False(t, d.Set())
	})
}

// TestDuration_OrDefault pins the read-side helper: unset returns the
// supplied default, set returns the parsed value (even when the parsed
// value is the zero duration).
func TestDuration_OrDefault(t *testing.T) {
	var unset config.Duration
	require.Equal(t, 7*time.Second, unset.OrDefault(7*time.Second))

	var set config.Duration
	require.NoError(t, set.UnmarshalText([]byte("3s")))
	require.Equal(t, 3*time.Second, set.OrDefault(7*time.Second))

	// Explicit "0s" reports Set() and D() == 0 — the orchestrator uses
	// this to reject explicit zero on knobs that must be positive.
	var zero config.Duration
	require.NoError(t, zero.UnmarshalText([]byte("0s")))
	require.True(t, zero.Set())
	require.Equal(t, time.Duration(0), zero.D())
}

// TestParse_MalformedDuration covers the load-level error path: a bad
// duration string in YAML produces an error wrapping the offending
// input value so the operator can locate it.
func TestParse_MalformedDuration(t *testing.T) {
	bad := strings.Replace(validPATYAML, "reconcile_interval: 5s", "reconcile_interval: nope", 1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), `"nope"`)
}

// TestParse_VMIDRangeEdgeCases pins the validator's behaviour at
// VMIDRange boundaries. min == max is the "single-VM range" edge
// (validate tag `gtfield=Min` rejects it — the orchestrator needs
// at least one allocatable slot ABOVE Min for a fresh clone before
// the destroyed VM's cooldown expires). zero and negative bounds
// trip the `required,gt=0` chain.
func TestParse_VMIDRangeEdgeCases(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		yamlEdit   string // replaces the `vmid_range: { min: 10000, max: 19999 }` line
		wantErrSub string // empty = must succeed; non-empty = must error and contain this substring
	}{
		{
			name:       "min equals max is rejected",
			yamlEdit:   "vmid_range: { min: 10000, max: 10000 }",
			wantErrSub: "Max",
		},
		{
			name:       "min greater than max is rejected",
			yamlEdit:   "vmid_range: { min: 20000, max: 10000 }",
			wantErrSub: "Max",
		},
		{
			name:       "min equals zero is rejected",
			yamlEdit:   "vmid_range: { min: 0, max: 100 }",
			wantErrSub: "Min",
		},
		{
			name:       "negative min is rejected",
			yamlEdit:   "vmid_range: { min: -1, max: 100 }",
			wantErrSub: "Min",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			y := strings.Replace(validPATYAML,
				"vmid_range: { min: 10000, max: 19999 }", c.yamlEdit, 1)
			_, err := config.Parse([]byte(y))
			require.Error(t, err, "VMID range %q must be rejected", c.yamlEdit)
			require.Contains(t, err.Error(), c.wantErrSub,
				"error must name the offending field so the operator can find it")
		})
	}
}

// TestParse_VMIDRangeAcceptsProxmoxReservedRange documents the
// current policy on VMIDs Proxmox reserves for cluster internals
// (typically <100): the orchestrator accepts them. The validator
// only enforces gt=0 and Max > Min; it does NOT carve out the
// reserved band.
//
// This is a deliberate pin, not an assertion of best practice —
// operators who care about avoiding Proxmox's reserved IDs should
// configure min >= 100 themselves. Surfacing this as a test means
// any future change to the policy (e.g. rejecting min < 100) will
// fail loudly here and force an intentional decision.
func TestParse_VMIDRangeAcceptsProxmoxReservedRange(t *testing.T) {
	t.Parallel()
	y := strings.Replace(validPATYAML,
		"vmid_range: { min: 10000, max: 19999 }",
		"vmid_range: { min: 50, max: 99 }", 1)
	// Template VMID stays at 9000 in validPATYAML — already outside
	// the [50, 99] range, so the cross-field validator passes.

	_, err := config.Parse([]byte(y))
	require.NoError(t, err,
		"current policy: VMIDs in Proxmox's reserved range (<100) are accepted; "+
			"if this test starts failing, document the new rejection rule explicitly")
}

// TestParse_VMIDRangeAcceptsSingleSlotPlusOne pins the smallest
// valid range. min=1000 max=1001 is a two-VMID range — enough for
// the orchestrator's cooldown semantics (one destroyed VMID can
// still settle while the next clone targets the alternative).
// Anything tighter (min == max) is rejected by gtfield=Min, as
// covered in TestParse_VMIDRangeEdgeCases above.
func TestParse_VMIDRangeAcceptsSingleSlotPlusOne(t *testing.T) {
	t.Parallel()
	y := strings.Replace(validPATYAML,
		"vmid_range: { min: 10000, max: 19999 }",
		"vmid_range: { min: 1000, max: 1001 }", 1)
	// Pool sizing must fit within the 2-slot range to avoid the
	// HotSize+WarmSize > MaxConcurrentRunners cross-field check.
	y = strings.Replace(y, "max_concurrent_runners: 10", "max_concurrent_runners: 2", 1)
	y = strings.Replace(y, "hot_size: 2", "hot_size: 1", 1)
	y = strings.Replace(y, "warm_size: 3", "warm_size: 1", 1)

	_, err := config.Parse([]byte(y))
	require.NoError(t, err,
		"a 2-slot range must parse cleanly; this is the operational floor")
}

// TestTLS_InsecureSkipVerifyAndCAFileBothHonored pins the current
// contract of [TLSConfig.BuildClientTLS]: when both
// InsecureSkipVerify=true and a CAFile are set, BOTH are applied —
// the CA pool is loaded into RootCAs AND Insecure is left true on
// the resulting *tls.Config. The CA is therefore present but
// unused (Insecure short-circuits verification).
//
// This is a foot-gun: an operator who set CAFile expecting Insecure
// to be overridden gets neither pinning nor a config-load failure.
// Pinning the behaviour means a future change has to make the
// trade-off explicitly — either reject the combination at validate,
// or have one take precedence. Without this test, a refactor could
// silently flip the semantics.
func TestTLS_InsecureSkipVerifyAndCAFileBothHonored(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")

	// Generate a self-signed cert/key pair to serve as the
	// cert+key. tls.LoadX509KeyPair must accept them.
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:         true,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(certFile,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	keyBytes, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(keyFile,
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}), 0o600))
	// Reuse the same cert as the CA bundle — the contract test
	// just needs both files to load.
	require.NoError(t, os.WriteFile(caFile,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))

	tcfg := &config.TLSConfig{
		CertFile:           certFile,
		KeyFile:            keyFile,
		CAFile:             caFile,
		InsecureSkipVerify: true,
	}
	built, err := tcfg.BuildClientTLS()
	require.NoError(t, err)
	require.True(t, built.InsecureSkipVerify,
		"InsecureSkipVerify must reach the *tls.Config — the operator opted into it")
	require.NotNil(t, built.RootCAs,
		"CAFile must STILL be loaded into RootCAs even when Insecure is set; pinning the current behaviour catches a silent refactor")
	require.NotNil(t, built.Certificates,
		"keypair must be loaded regardless of Insecure")
}

// TestParse_EmptyTokenSecretEnvRejected pins the empty-env
// fallback semantics: SCALESET_PROXMOX_AUTH_TOKEN_SECRET set to
// an empty string must be rejected with the same error as the
// fully-missing case. Without this, an operator who templated
// an empty secret into their deployment manifest would silently
// start the orchestrator with no Proxmox auth and only discover
// the failure at the first PVE API call.
//
// No t.Parallel — setEnv uses t.Setenv which forbids parallel.
func TestParse_EmptyTokenSecretEnvRejected(t *testing.T) {
	noSecretYAML := strings.Replace(validPATYAML, "    token_secret: testsecret", "", 1)
	setEnv(t, map[string]string{"SCALESET_PROXMOX_AUTH_TOKEN_SECRET": ""})
	_, err := config.Parse([]byte(noSecretYAML))
	require.Error(t, err, "empty env-var must be rejected — silent acceptance leads to auth failure deep in the call stack")
	require.Contains(t, err.Error(), "proxmox.auth.token_secret",
		"the error must name the missing field so the operator can find it")
}

// TestParse_PATConfigURLAndBaseURLMutuallyExclusive locks in the
// config-side mutual-exclusion validation (issue #214). The two
// override modes have incompatible per-scope behaviour; silently
// picking one would be a footgun in a production GHES rollout.
func TestParse_PATConfigURLAndBaseURLMutuallyExclusive(t *testing.T) {
	bad := strings.Replace(validPATYAML,
		"  pat:\n    token: testtoken",
		"  pat:\n    token: testtoken\n    config_url: https://ghes.example.com/my-org\n    config_base_url: https://ghes.example.com",
		1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "mutually exclusive")
}

// TestParse_PATConfigURLRejectedWithMultiScaleset confirms the
// orchestrator refuses to start when the operator declares
// multiple scalesets but pins github.pat.config_url to a single
// org/repo URL. config_url forces every per-scaleset client to
// handshake against the same scope — a silent bug that this
// validation surfaces at load time.
func TestParse_PATConfigURLRejectedWithMultiScaleset(t *testing.T) {
	// Build a YAML with scalesets: [...] and github.pat.config_url
	// set. Start from the multi-scaleset fixture (already exercised
	// by TestScalesets_PluralFormParses).
	bad := strings.Replace(validMultiScalesetYAML,
		"  pat:\n    token: testtoken",
		"  pat:\n    token: testtoken\n    config_url: https://ghes.example.com/org-a",
		1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "config_url")
	require.Contains(t, err.Error(), "multi-scaleset")
}

// TestParse_PATConfigBaseURLAcceptedWithMultiScaleset confirms the
// inverse: a multi-scaleset config with github.pat.config_base_url
// set loads cleanly. This is the supported GHES multi-scaleset
// shape.
func TestParse_PATConfigBaseURLAcceptedWithMultiScaleset(t *testing.T) {
	ok := strings.Replace(validMultiScalesetYAML,
		"  pat:\n    token: testtoken",
		"  pat:\n    token: testtoken\n    config_base_url: https://ghes.example.com",
		1)
	cfg, err := config.Parse([]byte(ok))
	require.NoError(t, err)
	require.Equal(t, "https://ghes.example.com", cfg.GitHub.PAT.ConfigBaseURL)
}

// TestScalesets_RejectsMissingVMIDRangeWithMulti confirms the
// issue #222 guard: with N > 1 scalesets, every entry must
// declare its own vmid_range so per-scaleset pool.Manager
// allocators don't race on the same shared global range.
func TestScalesets_RejectsMissingVMIDRangeWithMulti(t *testing.T) {
	bad := strings.Replace(validMultiScalesetYAML,
		"    vmid_range: { min: 10000, max: 14999 }\n", "", 1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "vmid_range is required with multi-scaleset")
	require.Contains(t, err.Error(), "issue #222")
}

// TestScalesets_RejectsOverlappingVMIDRanges locks in the
// pairwise overlap rejection. Two scalesets that BOTH declare
// ranges but overlap them would still race; the validator
// catches that at load.
func TestScalesets_RejectsOverlappingVMIDRanges(t *testing.T) {
	bad := strings.Replace(validMultiScalesetYAML,
		"    vmid_range: { min: 15000, max: 19999 }",
		"    vmid_range: { min: 14500, max: 19999 }", // overlaps 10000-14999
		1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "overlaps")
}

// TestScalesets_AcceptsDisjointVMIDRanges is the happy-path
// counterpart — disjoint declared ranges parse and validate
// cleanly. validMultiScalesetYAML already declares disjoint
// ranges so this is redundant with TestScalesets_PluralFormParses
// today, but locks the expectation in case future schema work
// changes the default fixture.
func TestScalesets_AcceptsDisjointVMIDRanges(t *testing.T) {
	cfg, err := config.Parse([]byte(validMultiScalesetYAML))
	require.NoError(t, err)
	require.Equal(t, 10000, cfg.Scalesets[0].VMIDRange.Min)
	require.Equal(t, 14999, cfg.Scalesets[0].VMIDRange.Max)
	require.Equal(t, 15000, cfg.Scalesets[1].VMIDRange.Min)
	require.Equal(t, 19999, cfg.Scalesets[1].VMIDRange.Max)
}

// TestScalesets_RejectsMalformedVMIDRange covers the per-entry
// well-formedness checks: min > 0 and max > min.
func TestScalesets_RejectsMalformedVMIDRange(t *testing.T) {
	t.Run("min_must_be_positive", func(t *testing.T) {
		bad := strings.Replace(validMultiScalesetYAML,
			"    vmid_range: { min: 10000, max: 14999 }",
			"    vmid_range: { min: 0, max: 14999 }", 1)
		_, err := config.Parse([]byte(bad))
		require.Error(t, err)
		require.Contains(t, err.Error(), "min must be > 0")
	})
	t.Run("max_must_exceed_min", func(t *testing.T) {
		bad := strings.Replace(validMultiScalesetYAML,
			"    vmid_range: { min: 10000, max: 14999 }",
			"    vmid_range: { min: 10000, max: 10000 }", 1)
		_, err := config.Parse([]byte(bad))
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be greater than min")
	})
}

// TestScalesets_SingleScalesetInheritsGlobalRange documents the
// back-compat behaviour: N == 1 scalesets are allowed to inherit
// cfg.Proxmox.VMIDRange (there's no sibling to collide with).
// The singular-form projection used by every existing single-
// scaleset config exercises this path.
func TestScalesets_SingleScalesetInheritsGlobalRange(t *testing.T) {
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "ghp_fake", "TEST_PVE_TOKEN": "pve-secret"})
	cfg, err := config.Parse([]byte(validPATYAML))
	require.NoError(t, err)
	require.Len(t, cfg.Scalesets, 1)
	// Inherited path: entry's VMIDRange is nil; app.entryVMIDRange
	// falls back to cfg.Proxmox.VMIDRange.
	require.Nil(t, cfg.Scalesets[0].VMIDRange)
}

// TestParse_AppConfigURLAndBaseURLMutuallyExclusive locks in the
// App-side mutual-exclusion validation (mirrors the PAT-side
// rule from #214; same per-scope semantics).
func TestParse_AppConfigURLAndBaseURLMutuallyExclusive(t *testing.T) {
	pem := keyPath(t)
	bad := `
github:
  auth_mode: app
  app:
    client_id: "Iv23likB94"
    installation_id: 2
    private_key_path: ` + pem + `
    config_url: https://ghes.example.com/myorg
    config_base_url: https://ghes.example.com
  scope: { org: o }
scaleset: { name: x, max_concurrent_runners: 5 }
proxmox:
  endpoint: https://h:8006/api2/json
  auth: { token_id: a!b, token_secret: testsecret }
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
	require.Contains(t, err.Error(), "mutually exclusive")
}

// TestParse_AppConfigURLRejectedWithMultiScaleset locks in the
// App-side multi-scaleset rule: config_url forces every per-
// scaleset client to handshake against the same scope, so we
// reject it loudly at load time rather than silently mis-routing
// at runtime. The multi-scaleset fixture also needs per-entry
// vmid_range blocks (issue #222) to reach the config_url check
// — the missing-range guard fires before the auth check
// otherwise.
func TestParse_AppConfigURLRejectedWithMultiScaleset(t *testing.T) {
	pem := keyPath(t)
	// Build a multi-scaleset config with App auth + config_url.
	bad := `
github:
  auth_mode: app
  app:
    client_id: "Iv23likB94"
    installation_id: 2
    private_key_path: ` + pem + `
    config_url: https://ghes.example.com/org-a

scalesets:
  - name: linux-x64
    labels: [self-hosted, linux, proxmox, x64]
    max_concurrent_runners: 10
    scope:
      org: org-a
    vmid_range: { min: 10000, max: 14999 }
  - name: gpu-pool
    labels: [self-hosted, linux, gpu]
    max_concurrent_runners: 4
    scope:
      org: org-b
    vmid_range: { min: 15000, max: 19999 }

proxmox:
  endpoint: https://h:8006/api2/json
  auth:
    token_id: a!b
    token_secret: testsecret
  template_vmid: 9000
  vmid_range: { min: 10000, max: 19999 }
  storage: { disk: d, snippets: s }
  network: { bridge: br0 }

nodes: { strategy: single, single_node: n1 }

pool:
  hot_size: 0
  warm_size: 0
  reconcile_interval: 5s
  vm_max_age: 12h
  drain_timeout: 15m
  boot_max_attempts: 3
`
	setEnv(t, map[string]string{"TEST_PVE_TOKEN": "y"})
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "config_url")
	require.Contains(t, err.Error(), "multi-scaleset")
}

// ---------------------------------------------------------------------------
// NIC bridge alphabet (issue #284)
// ---------------------------------------------------------------------------

// TestProxmoxNetwork_RejectsBridgeWithCommaOrEquals locks in the
// alphabet check on proxmox.network.bridge. Without it, a value
// like "vmbr0,firewall=1" silently injects extra NIC attributes
// when encodeNIC concatenates the field into Proxmox's net<idx>
// config (issue #284).
func TestProxmoxNetwork_RejectsBridgeWithCommaOrEquals(t *testing.T) {
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "ghp_fake", "TEST_PVE_TOKEN": "pve-secret"})
	for name, bridge := range map[string]string{
		"comma":      "vmbr0,firewall=1",
		"equals":     "vmbr0=evil",
		"whitespace": "vmbr0 vmbr1",
		"newline":    "vmbr0\nbridge=other",
	} {
		t.Run(name, func(t *testing.T) {
			bad := strings.Replace(validPATYAML,
				"  network:  { bridge: vmbr0 }",
				`  network:  { bridge: "`+bridge+`" }`, 1)
			_, err := config.Parse([]byte(bad))
			require.Error(t, err)
			require.Contains(t, err.Error(), "proxmox.network.bridge")
		})
	}
}

// TestProfileNetwork_RejectsBridgeWithComma covers the per-profile
// network override path (#284). Same alphabet, same failure mode.
func TestProfileNetwork_RejectsBridgeWithComma(t *testing.T) {
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "ghp_fake", "TEST_PVE_TOKEN": "pve-secret"})
	bad := strings.Replace(validProfileYAML,
		"    template_vmid: 9100",
		`    template_vmid: 9100
    network:
      bridge: "vmbr1,model=e1000,firewall=1"`, 1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "bridge")
}

// TestProfileNetwork_RejectsExtraNICBridgeWithComma covers the
// extra-NIC path (#284). Same alphabet on every NIC bridge.
func TestProfileNetwork_RejectsExtraNICBridgeWithComma(t *testing.T) {
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "ghp_fake", "TEST_PVE_TOKEN": "pve-secret"})
	bad := strings.Replace(validProfileYAML,
		"    template_vmid: 9100",
		`    template_vmid: 9100
    network:
      bridge: vmbr1
      extra_nics:
        - bridge: "vmbr-storage,firewall=1"`, 1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "extra_nics")
	require.Contains(t, err.Error(), "bridge")
}

// TestProxmoxNetwork_AcceptsCanonicalBridgeNames covers the happy
// path for the alphabet check: real-world bridge identifiers
// (vmbr0, vmbr-storage, br_dmz, br.42) must still parse.
func TestProxmoxNetwork_AcceptsCanonicalBridgeNames(t *testing.T) {
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "ghp_fake", "TEST_PVE_TOKEN": "pve-secret"})
	for _, bridge := range []string{"vmbr0", "vmbr-storage", "br_dmz", "br.42", "VMBR0"} {
		t.Run(bridge, func(t *testing.T) {
			good := strings.Replace(validPATYAML,
				"  network:  { bridge: vmbr0 }",
				"  network:  { bridge: "+bridge+" }", 1)
			_, err := config.Parse([]byte(good))
			require.NoError(t, err)
		})
	}
}

// ---------------------------------------------------------------------------
// Required-but-zero / runtime reload (issue #291)
// ---------------------------------------------------------------------------

// TestParse_ExplicitZeroPoolDurationsRejected locks in that an
// operator writing `pool.power_poll_interval: 0s` (literal zero)
// is rejected even though the Go zero value of time.Duration is
// also 0. ApplyDefaults only substitutes when the field is
// *unset*; an explicit 0 surfaces through to Resolve and must
// fail loudly rather than degrade into a busy-loop. (issue #291)
func TestParse_ExplicitZeroPoolDurationsRejected(t *testing.T) {
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "ghp_fake", "TEST_PVE_TOKEN": "pve-secret"})
	for field, msg := range map[string]string{
		"power_poll_interval":  "pool.power_poll_interval must be positive",
		"vmid_reuse_cooldown":  "pool.vmid_reuse_cooldown must be positive",
		"orphan_grace":         "pool.orphan_grace must be positive",
		"clone_inflight_grace": "pool.clone_inflight_grace must be positive",
	} {
		t.Run(field, func(t *testing.T) {
			bad := strings.Replace(validPATYAML,
				"  hot_size: 2",
				"  "+field+": 0s\n  hot_size: 2", 1)
			_, err := config.Parse([]byte(bad))
			require.Error(t, err)
			require.Contains(t, err.Error(), msg)
		})
	}
}

// TestParse_RejectsZeroMaxConcurrentInMultiScaleset confirms a
// multi-scaleset entry with `max_concurrent_runners: 0` is rejected
// at Parse time — the field's "required,gt=0" tag prevents a
// no-op scaleset that consumes orchestration cycles but never
// provisions a runner. (issue #291)
func TestParse_RejectsZeroMaxConcurrentInMultiScaleset(t *testing.T) {
	bad := strings.Replace(validMultiScalesetYAML,
		"max_concurrent_runners: 10", "max_concurrent_runners: 0", 1)
	_, err := config.Parse([]byte(bad))
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "max_concurrent_runners")
}

// TestParse_DoesNotSupportRuntimeReload documents the current
// behaviour: the Config type has no live-reload API, so adding
// or removing a scaleset requires a full process restart. The
// test exists so a future commit that adds reload semantics has
// to explicitly delete it. (issue #291)
//
// The signal is structural: there is no Config.Reload, no
// fsnotify import in this package, and Parse always returns a
// freshly-constructed value. Asserting "no such method" via
// reflection keeps the contract explicit.
func TestParse_DoesNotSupportRuntimeReload(t *testing.T) {
	setEnv(t, map[string]string{"TEST_GH_TOKEN": "ghp_fake", "TEST_PVE_TOKEN": "pve-secret"})
	cfg, err := config.Parse([]byte(validPATYAML))
	require.NoError(t, err)
	v := reflect.ValueOf(cfg)
	for _, name := range []string{"Reload", "Watch", "Refresh"} {
		require.False(t, v.MethodByName(name).IsValid(),
			"Config exposes a %s method — runtime reload appears to be implemented; update or delete this test", name)
	}
}
