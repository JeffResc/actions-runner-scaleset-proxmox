package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/config"
)

// capacityYAML is the homelab shape from the feature's motivating case:
// three heterogeneous profiles on one node. `%s` takes the pool.capacity
// block (and anything else) each test wants to append.
const capacityYAML = `
github:
  auth_mode: pat
  pat:
    token: testtoken
  scope:
    org: my-org

scaleset:
  name: homelab
  labels: [self-hosted, linux, proxmox]
  max_concurrent_runners: %s

proxmox:
  endpoint: https://pve.example.com:8006/api2/json
  auth:
    token_id: scaleset@pve!automation
    token_secret: testsecret
  template_vmid: 9000
  vmid_range: { min: 10000, max: 19999 }
  storage:  { disk: local-lvm, snippets: local }
  network:  { bridge: vmbr0 }

nodes:
  strategy: single
  single_node: pve1

pool:
  hot_size: 0
  warm_size: 0
%s

profiles:
  - name: mem-4g
    labels: [self-hosted, linux, proxmox]
    cpu: %s
    memory_mb: %s
  - name: mem-16g
    labels: [self-hosted, linux, proxmox, big]
    cpu: 8
    memory_mb: 16384
`

// capacityCfg renders capacityYAML. maxConc is spliced verbatim so a
// test can write `0` or omit the value entirely.
func capacityCfg(maxConc, poolExtra, cpu, memoryMB string) []byte {
	y := capacityYAML
	for _, sub := range []string{maxConc, poolExtra, cpu, memoryMB} {
		y = strings.Replace(y, "%s", sub, 1)
	}
	return []byte(y)
}

const capacityOn = `  capacity:
    enabled: true
    reserve_memory_mb: 4096`

// TestCapacity_DisabledByDefault is the back-compat contract: a config
// that has never heard of this feature must load exactly as before.
func TestCapacity_DisabledByDefault(t *testing.T) {
	cfg, err := config.Parse([]byte(validPATYAML))
	require.NoError(t, err)
	require.False(t, cfg.Pool.Capacity.Enabled)
	require.False(t, cfg.Pool.Capacity.EvictIdleForDemand)
	require.Zero(t, cfg.Pool.Capacity.CPUOvercommitRatio,
		"CPU must stay ungated unless the operator asks for it")
}

func TestCapacity_Defaults(t *testing.T) {
	cfg, err := config.Parse(capacityCfg("10", "  capacity:\n    enabled: true", "2", "4096"))
	require.NoError(t, err)
	require.True(t, cfg.Pool.Capacity.Enabled)
	require.Equal(t, 15*time.Second, cfg.Pool.Capacity.RefreshInterval.D())
	require.Equal(t, 2048, cfg.Pool.Capacity.ReserveMemoryMB,
		"a bare `capacity: {enabled: true}` must still withhold host headroom")
}

// TestCapacity_ExplicitReserveIsNotOverridden guards the default from
// clobbering an operator who deliberately configured only a fraction.
func TestCapacity_ExplicitReserveIsNotOverridden(t *testing.T) {
	cfg, err := config.Parse(capacityCfg("10",
		"  capacity:\n    enabled: true\n    reserve_memory_fraction: 0.1", "2", "4096"))
	require.NoError(t, err)
	require.Zero(t, cfg.Pool.Capacity.ReserveMemoryMB)
	require.InDelta(t, 0.1, cfg.Pool.Capacity.ReserveMemoryFraction, 1e-9)
}

// TestCapacity_RequiresProfileMemory is the load-time refusal that keeps
// admission honest: a profile inheriting the template's RAM has no
// footprint the orchestrator can reserve against, and guessing would
// silently overcommit the node.
func TestCapacity_RequiresProfileMemory(t *testing.T) {
	_, err := config.Parse(capacityCfg("10", capacityOn, "2", "0"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "memory_mb must be > 0")
	require.Contains(t, err.Error(), "mem-4g", "the error must name the offending profile")
}

// TestCapacity_MemoryOptionalWhenDisabled: the requirement is scoped to
// the feature, so existing profiles keep inheriting the template.
func TestCapacity_MemoryOptionalWhenDisabled(t *testing.T) {
	_, err := config.Parse(capacityCfg("10", "", "2", "0"))
	require.NoError(t, err)
}

// TestCapacity_CPURequiredOnlyWhenGated mirrors the memory rule for the
// opt-in CPU gate.
func TestCapacity_CPURequiredOnlyWhenGated(t *testing.T) {
	ok := capacityOn
	gated := capacityOn + "\n    cpu_overcommit_ratio: 4.0"

	_, err := config.Parse(capacityCfg("10", ok, "0", "4096"))
	require.NoError(t, err, "cpu may be inherited while CPU gating is off")

	_, err = config.Parse(capacityCfg("10", gated, "0", "4096"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "cpu must be > 0")
}

// TestCapacity_MaxConcurrentOptionalOnlyUnderCapacity is the relaxation
// that lets memory be the sole gate — and its guard rail: without
// capacity admission the static cap is the ONLY bound on the fleet, so
// it stays mandatory.
func TestCapacity_MaxConcurrentOptionalOnlyUnderCapacity(t *testing.T) {
	t.Run("permitted with capacity on", func(t *testing.T) {
		cfg, err := config.Parse(capacityCfg("0", capacityOn, "2", "4096"))
		require.NoError(t, err)
		require.Zero(t, cfg.Scalesets[0].MaxConcurrentRunners,
			"zero survives into the resolved config as the unlimited marker")
	})

	t.Run("still required with capacity off", func(t *testing.T) {
		_, err := config.Parse(capacityCfg("0", "", "2", "4096"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "max_concurrent_runners")
	})
}

// TestCapacity_UnlimitedSkipsSizingArithmetic: the hot+warm and schedule
// checks bound sizes against the static cap, so they are vacuous when
// there isn't one — they must not degrade into "hot+warm > 0".
func TestCapacity_UnlimitedSkipsSizingArithmetic(t *testing.T) {
	y := strings.Replace(
		string(capacityCfg("0", capacityOn, "2", "4096")),
		"  hot_size: 0\n  warm_size: 0", "  hot_size: 4\n  warm_size: 4", 1)
	_, err := config.Parse([]byte(y))
	require.NoError(t, err, "non-zero pool sizes must be fine when there is no cap to exceed")
}

// TestCapacity_GlobalMaxStillEnforcedWithStaticCaps: relaxing the cap
// must not quietly disable global_max for configs that still use it.
func TestCapacity_GlobalMaxStillEnforcedWithStaticCaps(t *testing.T) {
	// Both profiles inherit the scaleset's cap of 10, summing to 20.
	_, err := config.Parse(capacityCfg("10", capacityOn+"\n  global_max: 5", "2", "4096"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "global_max")
}

// TestCapacity_GlobalMaxSkippedWhenUnlimited: with no per-profile caps
// to sum there is nothing to compare, so the assertion is skipped rather
// than evaluated against a meaningless partial total.
func TestCapacity_GlobalMaxSkippedWhenUnlimited(t *testing.T) {
	_, err := config.Parse(capacityCfg("0", capacityOn+"\n  global_max: 5", "2", "4096"))
	require.NoError(t, err)
}

func TestCapacity_RejectsOutOfRangeKnobs(t *testing.T) {
	for name, extra := range map[string]string{
		"reserve fraction above 0.9": capacityOn + "\n    reserve_memory_fraction: 0.95",
		"negative reserve":           capacityOn + "\n    reserve_memory_mb: -1",
		"absurd overcommit":          capacityOn + "\n    cpu_overcommit_ratio: 999",
		"zero refresh interval":      capacityOn + "\n    refresh_interval: 0s",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := config.Parse(capacityCfg("10", extra, "2", "4096"))
			require.Error(t, err)
		})
	}
}
