package provisioner

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/config"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakeproxmox"
)

// These tests pin the per-clone VM firewall feature: Proxmox does not
// copy /etc/pve/firewall/<vmid>.fw on clone, so when
// proxmox.firewall.enabled is set the provisioner must (1) attach the
// security-group rule, (2) enable the VM firewall — both BEFORE the
// VM's first start — and (3) fail the whole clone when either call
// fails, so a runner that was supposed to be sandboxed never runs
// unsandboxed.

// newFirewallProvisioner builds a *pmox against the fake with the
// firewall block enabled. The client is unaffected by cfg.Firewall so
// mutating it after newTestProvisioner is safe.
func newFirewallProvisioner(t *testing.T, fp *fakeproxmox.Server, fw config.FirewallConfig) *pmox {
	t.Helper()
	p := newTestProvisioner(t, fp.Server, "pve1")
	p.cfg.Firewall = fw
	return p
}

// findVM plucks one VM out of the fake's snapshot by VMID.
func findVM(t *testing.T, fp *fakeproxmox.Server, vmid int) fakeproxmox.VMSnapshot {
	t.Helper()
	for _, v := range fp.Snapshot() {
		if v.VMID == vmid {
			return v
		}
	}
	t.Fatalf("vm %d not found in fake snapshot", vmid)
	return fakeproxmox.VMSnapshot{}
}

// TestClone_FirewallAppliedBeforeFirstStart: a hot-fill clone
// (PoweredOn=true starts the VM inside Clone) must have the group rule
// and the enable+dhcp options recorded BEFORE its first qmstart.
func TestClone_FirewallAppliedBeforeFirstStart(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	p := newFirewallProvisioner(t, fp, config.FirewallConfig{
		Enabled:       true,
		SecurityGroup: "gh-runner",
	})

	vm, err := p.Clone(context.Background(), CloneOptions{
		NewVMID: 10042, Node: "pve1", Name: "gh-runner-test-10042", PoweredOn: true,
	})
	require.NoError(t, err)

	got := findVM(t, fp, vm.VMID)
	require.Equal(t, []fakeproxmox.FirewallRuleRecord{
		{Type: "group", Action: "gh-runner", Enable: 1},
	}, got.FirewallRules, "exactly one security-group rule must be attached")
	require.EqualValues(t, 1, got.FirewallOptions["enable"], "vm firewall must be enabled")
	require.EqualValues(t, 1, got.FirewallOptions["dhcp"], "dhcp defaults true → wire value 1")
	require.True(t, got.Running)
	require.True(t, got.EverStarted)
	require.True(t, got.FirewallActiveAtFirstStart,
		"the sandbox must be active before the VM's first boot, not applied afterwards")
}

// TestClone_FirewallDHCPFalseOmitsDhcpFlag: dhcp:false marshals to an
// omitted field (go-proxmox IntOrBool + omitempty), which is
// wire-equivalent to PVE's dhcp default 0. Enable must still land.
func TestClone_FirewallDHCPFalseOmitsDhcpFlag(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	dhcp := false
	p := newFirewallProvisioner(t, fp, config.FirewallConfig{
		Enabled:       true,
		SecurityGroup: "gh-runner",
		DHCP:          &dhcp,
	})

	vm, err := p.Clone(context.Background(), CloneOptions{
		NewVMID: 10043, Node: "pve1", Name: "gh-runner-test-10043",
	})
	require.NoError(t, err)

	got := findVM(t, fp, vm.VMID)
	require.EqualValues(t, 1, got.FirewallOptions["enable"])
	require.NotContains(t, got.FirewallOptions, "dhcp",
		"dhcp:false is sent as an omitted field (PVE default 0)")
}

// TestClone_FirewallFailureFailsClone: when the firewall API errors the
// clone must FAIL — never warn-and-continue — and the VM must never
// have been started. The pool's existing failed-clone path then
// destroys the leftover VM.
func TestClone_FirewallFailureFailsClone(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	fp.InjectFault(fakeproxmox.Fault{Kind: fakeproxmox.FaultFirewallFail})
	p := newFirewallProvisioner(t, fp, config.FirewallConfig{
		Enabled:       true,
		SecurityGroup: "gh-runner",
	})

	_, err := p.Clone(context.Background(), CloneOptions{
		NewVMID: 10044, Node: "pve1", Name: "gh-runner-test-10044", PoweredOn: true,
	})
	require.Error(t, err, "a runner that was supposed to be sandboxed must not run unsandboxed")
	require.Contains(t, err.Error(), "apply firewall")

	// The clone exists in Proxmox (pool cleanup handles it) but must
	// never have booted.
	got := findVM(t, fp, 10044)
	require.False(t, got.EverStarted, "firewall failure must abort Clone before Start")
	require.False(t, got.Running)
}

// TestClone_FirewallDisabledMakesNoFirewallCalls: with the block absent
// (zero value) the clone path is byte-for-byte the pre-feature one — no
// firewall endpoints are ever touched.
func TestClone_FirewallDisabledMakesNoFirewallCalls(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	p := newTestProvisioner(t, fp.Server, "pve1")

	vm, err := p.Clone(context.Background(), CloneOptions{
		NewVMID: 10045, Node: "pve1", Name: "gh-runner-test-10045", PoweredOn: true,
	})
	require.NoError(t, err)

	got := findVM(t, fp, vm.VMID)
	require.Empty(t, got.FirewallRules)
	require.Nil(t, got.FirewallOptions)
	require.True(t, got.Running)
}

// TestEncodeNIC_FirewallFlagForced: a profile network override rebuilds
// net<N> from scratch, replacing the template's NIC string — so when
// the firewall feature is on, encodeNIC must re-add firewall=1 or the
// override would silently detach the VM firewall from the bridge.
func TestEncodeNIC_FirewallFlagForced(t *testing.T) {
	t.Parallel()
	require.Equal(t, "virtio,bridge=vmbr0,tag=42,firewall=1",
		encodeNIC(CloneNIC{Bridge: "vmbr0", VLANTag: 42}, true))
	require.Equal(t, "e1000,bridge=vmbr1,mtu=9000,firewall=1",
		encodeNIC(CloneNIC{Bridge: "vmbr1", MTU: 9000, Model: "e1000"}, true))
}

// TestBuildCloneConfig_NICOverridePreservesFirewallFlag: end-to-end
// through buildCloneConfig — every rebuilt net<N> option carries
// firewall=1 when the feature is enabled, and none do when disabled.
func TestBuildCloneConfig_NICOverridePreservesFirewallFlag(t *testing.T) {
	t.Parallel()
	opts := CloneOptions{
		NewVMID: 10042,
		Profile: "default",
		NICs: []CloneNIC{
			{Bridge: "vmbr0", VLANTag: 10},
			{Bridge: "vmbr1", VLANUntagged: true},
		},
	}

	on, err := buildCloneConfig("test-scaleset", opts, true)
	require.NoError(t, err)
	collected := map[string]any{}
	for _, o := range on {
		collected[o.Name] = o.Value
	}
	require.Equal(t, "virtio,bridge=vmbr0,tag=10,firewall=1", collected["net0"])
	require.Equal(t, "virtio,bridge=vmbr1,firewall=1", collected["net1"])

	off, err := buildCloneConfig("test-scaleset", opts, false)
	require.NoError(t, err)
	for _, o := range off {
		if o.Name == "net0" || o.Name == "net1" {
			require.NotContains(t, o.Value.(string), "firewall",
				"disabled feature must not inject the flag (%s)", o.Name)
		}
	}
}

// ---------------------------------------------------------------------------
// Template-inherited NIC firewall flag (nicFirewallPatches)
// ---------------------------------------------------------------------------
//
// PVE only routes a NIC through the VM firewall bridge when its net
// string carries firewall=1. A template NIC that no profile override
// rebuilds is copied verbatim on clone — without patching, a template
// whose NIC lacks the flag would produce runners with enable=1 and
// rules attached but zero actual filtering, silently.

func TestForceNICFirewallFlag(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in      string
		want    string
		changed bool
	}{
		// No flag → appended, everything else (MAC, bridge) untouched.
		{"virtio=AA:BB:CC:DD:EE:FF,bridge=vmbr0",
			"virtio=AA:BB:CC:DD:EE:FF,bridge=vmbr0,firewall=1", true},
		// Explicit firewall=0 → corrected in place, position kept.
		{"virtio=AA:BB:CC:DD:EE:FF,bridge=vmbr0,firewall=0,mtu=9000",
			"virtio=AA:BB:CC:DD:EE:FF,bridge=vmbr0,firewall=1,mtu=9000", true},
		// Already firewall=1 → untouched, reported unchanged.
		{"virtio=AA:BB:CC:DD:EE:FF,bridge=vmbr0,tag=42,firewall=1",
			"virtio=AA:BB:CC:DD:EE:FF,bridge=vmbr0,tag=42,firewall=1", false},
		// Exotic attrs survive verbatim.
		{"e1000=00:11:22:33:44:55,bridge=vmbr1,tag=30,trunks=10;20,queues=4",
			"e1000=00:11:22:33:44:55,bridge=vmbr1,tag=30,trunks=10;20,queues=4,firewall=1", true},
	}
	for _, tc := range cases {
		got, changed := forceNICFirewallFlag(tc.in)
		require.Equal(t, tc.want, got, "input %q", tc.in)
		require.Equal(t, tc.changed, changed, "input %q", tc.in)
	}
}

// TestClone_TemplateNICsGetFirewallFlag: with no profile NIC override,
// the template's net strings are cloned verbatim — the provisioner
// must patch firewall=1 onto every one of them (preserving MAC, tag,
// and any other attribute) or the sandbox never engages.
func TestClone_TemplateNICsGetFirewallFlag(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	require.NoError(t, fp.SetVMConfig(9000, "net0", "virtio=AA:BB:CC:DD:EE:F0,bridge=vmbr0"))
	require.NoError(t, fp.SetVMConfig(9000, "net1", "virtio=AA:BB:CC:DD:EE:F1,bridge=vmbr1,tag=30,firewall=0"))
	p := newFirewallProvisioner(t, fp, config.FirewallConfig{
		Enabled:       true,
		SecurityGroup: "gh-runner",
	})

	vm, err := p.Clone(context.Background(), CloneOptions{
		NewVMID: 10046, Node: "pve1", Name: "gh-runner-test-10046",
	})
	require.NoError(t, err)

	got := findVM(t, fp, vm.VMID)
	require.Equal(t, "virtio=AA:BB:CC:DD:EE:F0,bridge=vmbr0,firewall=1",
		got.Config["net0"], "missing flag must be appended, MAC preserved")
	require.Equal(t, "virtio=AA:BB:CC:DD:EE:F1,bridge=vmbr1,tag=30,firewall=1",
		got.Config["net1"], "explicit firewall=0 must be corrected, tag preserved")
}

// TestClone_OverriddenNICNotDoublePatched: a profile override rebuilds
// net0 via encodeNIC (which appends firewall=1 itself); only the
// template NICs beyond the override list are patched.
func TestClone_OverriddenNICNotDoublePatched(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	require.NoError(t, fp.SetVMConfig(9000, "net0", "virtio=AA:BB:CC:DD:EE:F0,bridge=vmbr0"))
	require.NoError(t, fp.SetVMConfig(9000, "net1", "virtio=AA:BB:CC:DD:EE:F1,bridge=vmbr1"))
	p := newFirewallProvisioner(t, fp, config.FirewallConfig{
		Enabled:       true,
		SecurityGroup: "gh-runner",
	})

	vm, err := p.Clone(context.Background(), CloneOptions{
		NewVMID: 10047, Node: "pve1", Name: "gh-runner-test-10047",
		NICs: []CloneNIC{{Bridge: "vmbr9", VLANTag: 7}},
	})
	require.NoError(t, err)

	got := findVM(t, fp, vm.VMID)
	require.Equal(t, "virtio,bridge=vmbr9,tag=7,firewall=1", got.Config["net0"],
		"overridden NIC comes from encodeNIC — exactly one firewall=1, no template leftovers")
	require.Equal(t, "virtio=AA:BB:CC:DD:EE:F1,bridge=vmbr1,firewall=1", got.Config["net1"],
		"non-overridden template NIC must still be patched")
}

// TestClone_FirewallDisabledLeavesTemplateNICsAlone: the disabled path
// must remain byte-for-byte pre-feature — inherited NIC strings are
// not rewritten.
func TestClone_FirewallDisabledLeavesTemplateNICsAlone(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	require.NoError(t, fp.SetVMConfig(9000, "net0", "virtio=AA:BB:CC:DD:EE:F0,bridge=vmbr0"))
	p := newTestProvisioner(t, fp.Server, "pve1")

	vm, err := p.Clone(context.Background(), CloneOptions{
		NewVMID: 10048, Node: "pve1", Name: "gh-runner-test-10048",
	})
	require.NoError(t, err)

	got := findVM(t, fp, vm.VMID)
	require.Equal(t, "virtio=AA:BB:CC:DD:EE:F0,bridge=vmbr0", got.Config["net0"])
}

// ---------------------------------------------------------------------------
// Pre-start firewall enforcement (ensureFirewall)
// ---------------------------------------------------------------------------
//
// Clone sandboxes fresh VMs, but a VM can reach Start without ever
// passing through this process's applyFirewall: crash-recovery
// adoption of a clone that died between the qmclone task and
// applyFirewall (the pool adopts the stopped VM as Warm and later
// boots it), or a pre-feature fleet adopted after the operator enables
// proxmox.firewall. Start must apply the missing sandbox or fail —
// never boot unfiltered.

// TestStart_FirewallEnsuredOnAdoptedVM: a seeded (adopted) VM with no
// firewall state and a flag-less NIC gets the full sandbox — rule,
// options, NIC flag — applied before its first boot.
func TestStart_FirewallEnsuredOnAdoptedVM(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	fp.SeedVM("pve1", 10060, "gh-runner-test-10060", false, []string{"gh-scaleset"})
	require.NoError(t, fp.SetVMConfig(10060, "net0", "virtio=AA:BB:CC:DD:EE:60,bridge=vmbr0"))
	p := newFirewallProvisioner(t, fp, config.FirewallConfig{
		Enabled:       true,
		SecurityGroup: "gh-runner",
	})

	require.NoError(t, p.Start(context.Background(), &VM{VMID: 10060, Node: "pve1"}))

	got := findVM(t, fp, 10060)
	require.Equal(t, []fakeproxmox.FirewallRuleRecord{
		{Type: "group", Action: "gh-runner", Enable: 1},
	}, got.FirewallRules)
	require.EqualValues(t, 1, got.FirewallOptions["enable"])
	require.EqualValues(t, 1, got.FirewallOptions["dhcp"])
	require.Equal(t, "virtio=AA:BB:CC:DD:EE:60,bridge=vmbr0,firewall=1", got.Config["net0"],
		"adopted VM's NIC must be attached to the firewall bridge too")
	require.True(t, got.Running)
	require.True(t, got.FirewallActiveAtFirstStart,
		"the sandbox must land before the adopted VM's first boot")
}

// TestStart_FirewallAlreadySandboxedIsIdempotent: a VM that Clone
// already sandboxed must not accumulate duplicate group rules across
// starts (warm->hot promotion, stop/start cycles).
func TestStart_FirewallAlreadySandboxedIsIdempotent(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	p := newFirewallProvisioner(t, fp, config.FirewallConfig{
		Enabled:       true,
		SecurityGroup: "gh-runner",
	})

	vm, err := p.Clone(context.Background(), CloneOptions{
		NewVMID: 10061, Node: "pve1", Name: "gh-runner-test-10061",
	})
	require.NoError(t, err)
	require.NoError(t, p.Start(context.Background(), &VM{VMID: vm.VMID, Node: vm.Node}))

	got := findVM(t, fp, vm.VMID)
	require.Len(t, got.FirewallRules, 1, "re-verification must not stack duplicate group rules")
	require.True(t, got.Running)
	require.True(t, got.FirewallActiveAtFirstStart)
}

// TestStart_FirewallFailureBlocksBoot: when the firewall API is broken
// Start must FAIL and the VM must never power on — the fail-safe
// direction is "no boot", not "boot unsandboxed".
func TestStart_FirewallFailureBlocksBoot(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	fp.SeedVM("pve1", 10062, "gh-runner-test-10062", false, []string{"gh-scaleset"})
	fp.InjectFault(fakeproxmox.Fault{Kind: fakeproxmox.FaultFirewallFail})
	p := newFirewallProvisioner(t, fp, config.FirewallConfig{
		Enabled:       true,
		SecurityGroup: "gh-runner",
	})

	err := p.Start(context.Background(), &VM{VMID: 10062, Node: "pve1"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "ensure firewall before start")

	got := findVM(t, fp, 10062)
	require.False(t, got.Running)
	require.False(t, got.EverStarted)
}

// TestStart_FirewallDisabledMakesNoFirewallCalls: with the feature off
// Start stays byte-for-byte the pre-feature path.
func TestStart_FirewallDisabledMakesNoFirewallCalls(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	fp.SeedVM("pve1", 10063, "gh-runner-test-10063", false, []string{"gh-scaleset"})
	p := newTestProvisioner(t, fp.Server, "pve1")

	require.NoError(t, p.Start(context.Background(), &VM{VMID: 10063, Node: "pve1"}))

	got := findVM(t, fp, 10063)
	require.Empty(t, got.FirewallRules)
	require.Nil(t, got.FirewallOptions)
	require.True(t, got.Running)
}

// ---------------------------------------------------------------------------
// Startup security-group existence check (issue #419)
// ---------------------------------------------------------------------------
//
// When proxmox.firewall is enabled the provisioner attaches a
// type=group rule naming security_group on every clone. If that group
// does not exist in the datacenter the rule is inert: fail-closed under
// a DROP input policy, silently unfiltered otherwise. The provisioner
// confirms the group exists once at startup and fails loud when it does
// not, rather than shipping an inert rule per clone.

// firewallProxmoxConfig builds the ProxmoxConfig New() needs to reach
// the fake, with the firewall block applied.
func firewallProxmoxConfig(url string, fw config.FirewallConfig) config.ProxmoxConfig {
	return config.ProxmoxConfig{
		Endpoint:           url,
		InsecureSkipVerify: true,
		Auth: config.ProxmoxAuth{
			TokenID:     "scaleset@pve!automation",
			TokenSecret: "fake-secret",
		},
		TemplateVMID: 9000,
		Firewall:     fw,
	}
}

// TestValidateFirewallSecurityGroup_PresentPasses: the configured group
// exists in the datacenter, so validation succeeds.
func TestValidateFirewallSecurityGroup_PresentPasses(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	fp.SeedSecurityGroup("gh-runner")
	p := newFirewallProvisioner(t, fp, config.FirewallConfig{Enabled: true, SecurityGroup: "gh-runner"})
	require.NoError(t, p.validateFirewallSecurityGroup(context.Background()))
}

// TestValidateFirewallSecurityGroup_MissingFails: a configured group
// absent from the datacenter must fail with ErrFirewallGroupNotFound and
// name the offending group.
func TestValidateFirewallSecurityGroup_MissingFails(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	fp.SeedSecurityGroup("some-other-group") // a different group exists, not ours
	p := newFirewallProvisioner(t, fp, config.FirewallConfig{Enabled: true, SecurityGroup: "gh-runner"})
	err := p.validateFirewallSecurityGroup(context.Background())
	require.ErrorIs(t, err, ErrFirewallGroupNotFound)
	require.Contains(t, err.Error(), "gh-runner")
}

// TestValidateFirewallSecurityGroup_DisabledSkips: with the firewall
// feature off, the check is a no-op even when no groups exist.
func TestValidateFirewallSecurityGroup_DisabledSkips(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	p := newFirewallProvisioner(t, fp, config.FirewallConfig{Enabled: false})
	require.NoError(t, p.validateFirewallSecurityGroup(context.Background()))
}

// TestNew_FirewallGroupMissingAbortsStartup proves New wires the check:
// an enabled firewall whose group is missing must fail construction.
func TestNew_FirewallGroupMissingAbortsStartup(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	cfg := firewallProxmoxConfig(fp.Server.URL, config.FirewallConfig{Enabled: true, SecurityGroup: "gh-runner"})
	_, err := New(context.Background(), cfg, "test-scaleset", "gh-runner-test-", Options{}, quietLogger())
	require.ErrorIs(t, err, ErrFirewallGroupNotFound)
}

// TestNew_FirewallGroupPresentSucceeds: with the group seeded, New
// completes construction as normal.
func TestNew_FirewallGroupPresentSucceeds(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	fp.SeedSecurityGroup("gh-runner")
	cfg := firewallProxmoxConfig(fp.Server.URL, config.FirewallConfig{Enabled: true, SecurityGroup: "gh-runner"})
	p, err := New(context.Background(), cfg, "test-scaleset", "gh-runner-test-", Options{}, quietLogger())
	require.NoError(t, err)
	require.NotNil(t, p)
}
