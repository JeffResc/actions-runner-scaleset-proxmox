//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakeproxmox"
)

// TestE2E_FirewallAppliedToEveryClone drives the full orchestrator with
// proxmox.firewall enabled and asserts the invariant the feature
// exists for: every runner VM the pool clones carries the
// security-group rule plus enable+dhcp firewall options, and every VM
// that ever booted had that sandbox active BEFORE its first start.
//
// Background: Proxmox does not copy /etc/pve/firewall/<vmid>.fw on
// clone, so template-side firewall config never reaches the clones —
// the orchestrator must write the rules per clone via the PVE
// firewall API, and hot-fill clones are started inside Clone, so the
// ordering (rules → options → first start) is the load-bearing part.
func TestE2E_FirewallAppliedToEveryClone(t *testing.T) {
	t.Parallel()
	h := Start(t, Options{
		HotSize:              2,
		WarmSize:             1,
		MaxConcurrentRunners: 4,
		FirewallEnabled:      true,
	})

	require.Eventually(t, func() bool {
		hot := h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "hot"))
		warm := h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "warm"))
		return hot >= 2 && warm >= 1
	}, 15*time.Second, 200*time.Millisecond,
		"pool never converged to hot=2 warm=1")

	clones := 0
	booted := 0
	for _, vm := range h.Proxmox.Snapshot() {
		if vm.VMID < 10000 || vm.VMID > 10999 {
			continue // template / non-orchestrator VMs
		}
		clones++
		require.Equal(t, []fakeproxmox.FirewallRuleRecord{
			{Type: "group", Action: "gh-runner", Enable: 1},
		}, vm.FirewallRules,
			"vm %d must carry exactly the configured security-group rule", vm.VMID)
		require.EqualValues(t, 1, vm.FirewallOptions["enable"],
			"vm %d firewall must be enabled", vm.VMID)
		require.EqualValues(t, 1, vm.FirewallOptions["dhcp"],
			"vm %d dhcp option must default on", vm.VMID)
		if vm.EverStarted {
			booted++
			require.True(t, vm.FirewallActiveAtFirstStart,
				"vm %d booted before its firewall was applied — sandbox ordering violated", vm.VMID)
		}
	}
	require.GreaterOrEqual(t, clones, 3, "expected hot(2)+warm(1) clones in range")
	require.GreaterOrEqual(t, booted, 2, "hot VMs must have been started")
}
