//go:build e2e

package e2e

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakegithub"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakeproxmox"
)

// The fake records QGA calls but does not execute Windows or a runner.
func TestE2E_WindowsLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name      string
		hot, warm int
	}{{"cold", 0, 0}, {"warm", 0, 1}, {"hot", 1, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			p := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
			if tc.name == "hot" {
				p.InjectFault(fakeproxmox.Fault{Kind: fakeproxmox.FaultGuestAgentNotReady, Duration: 250 * time.Millisecond})
			}
			gh := fakegithub.New(t, fakegithub.Options{ScaleSet: fakegithub.ScaleSetOptions{Name: "windows-set"}})
			recycle := "snapshot_rollback"
			if tc.name == "cold" {
				recycle = "destroy"
			}
			h := Start(t, Options{ScaleSetName: "windows-set", MaxConcurrentRunners: 1, RecycleMode: recycle, FakeProxmox: p, FakeGitHub: gh, Profiles: []ProfileSpec{{Name: "default", GuestOS: "windows", CPUCores: 2, MemoryMB: 2048, HotSize: tc.hot, WarmSize: tc.warm}}})
			if tc.hot > 0 || tc.warm > 0 {
				state := "hot"
				if tc.warm > 0 {
					state = "warm"
				}
				require.Eventually(t, func() bool { return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", state)) >= 1 }, 30*time.Second, 100*time.Millisecond)
			}
			gh.SetStatistics(fakegithub.Statistics{TotalAssignedJobs: 1})
			// A listener message refreshes the desired count after the initial handshake.
			require.NoError(t, gh.PostJobStarted("unrelated", 999999))
			require.Eventually(t, func() bool { return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "assigned")) >= 1 }, 30*time.Second, 100*time.Millisecond)
			assigned := awaitNAssignedVMs(t, h, "windows-set", 1)[0]
			vmid, name := assigned.vmid, assigned.name
			require.Eventually(t, func() bool {
				n := 0
				for _, call := range p.AgentCalls() {
					if call.VMID == vmid && (call.Kind == "write" || call.Kind == "exec") {
						n++
					}
				}
				return n >= 2
			}, 10*time.Second, 100*time.Millisecond)
			calls := p.AgentCalls()
			var matching []fakeproxmox.AgentCall
			for _, c := range calls {
				if c.VMID == vmid {
					matching = append(matching, c)
				}
			}
			require.GreaterOrEqual(t, len(matching), 2)
			require.Equal(t, "write", matching[0].Kind)
			require.Equal(t, `C:\ProgramData\ActionsRunnerScaleSet\jitconfig.tmp`, matching[0].Body["file"])
			content, ok := matching[0].Body["content"].(string)
			require.True(t, ok)
			decoded, err := base64.StdEncoding.DecodeString(content)
			require.NoError(t, err)
			var object map[string]any
			require.NoError(t, json.Unmarshal(decoded, &object))
			require.NotEmpty(t, object)
			require.Equal(t, "exec", matching[1].Kind)
			cmd, ok := matching[1].Body["command"].([]any)
			require.True(t, ok)
			require.Len(t, cmd, 5)
			require.Equal(t, "powershell.exe", cmd[0])
			require.Equal(t, "-EncodedCommand", cmd[3])
			raw, err := base64.StdEncoding.DecodeString(cmd[4].(string))
			require.NoError(t, err)
			require.Zero(t, len(raw)%2)
			units := make([]uint16, len(raw)/2)
			for i := range units {
				units[i] = binary.LittleEndian.Uint16(raw[i*2:])
			}
			script := string(utf16.Decode(units))
			require.Contains(t, script, "[System.IO.File]::Move")
			require.Contains(t, script, `C:\ProgramData\ActionsRunnerScaleSet\jitconfig.tmp`)
			require.Contains(t, script, `C:\ProgramData\ActionsRunnerScaleSet\jitconfig`)
			require.False(t, strings.Contains(script, "/opt/actions-runner"))
			// A repeated demand notification while the VM is assigned must not mint a second runner.
			require.NoError(t, gh.PostJobStarted("unrelated", 999999))
			require.Never(t, func() bool { return gh.JITMintCount() > 1 }, 400*time.Millisecond, 50*time.Millisecond)
			gh.SetRunner(fakegithub.Runner{ID: firstJITRunnerID, Name: name, Status: "online", Busy: true})
			require.NoError(t, gh.PostJobStarted(name, firstJITRunnerID))
			require.Eventually(t, func() bool { return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "running")) >= 1 }, 30*time.Second, 100*time.Millisecond)
			gh.SetStatistics(fakegithub.Statistics{})
			require.NoError(t, p.PowerOff(vmid))
			if recycle == "destroy" {
				require.Eventually(t, func() bool {
					for _, v := range p.Snapshot() {
						if v.VMID == vmid {
							return false
						}
					}
					return true
				}, 30*time.Second, 100*time.Millisecond)
			} else {
				require.Eventually(t, func() bool { return rollbackCount(p, vmid) == 1 }, 30*time.Second, 100*time.Millisecond)
			}
			require.Eventually(t, func() bool {
				for _, id := range gh.RunnerDeletions() {
					if id == firstJITRunnerID {
						return true
					}
				}
				return false
			}, 10*time.Second, 100*time.Millisecond)
		})
	}
}

// The injected file-write fails permanently; the minted runner and its VM must be cleaned up.
func TestE2E_WindowsInjectionFailure(t *testing.T) {
	p := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
	p.InjectFault(fakeproxmox.Fault{Kind: fakeproxmox.FaultJITInjectFail})
	gh := fakegithub.New(t, fakegithub.Options{ScaleSet: fakegithub.ScaleSetOptions{Name: "windows-fail"}})
	gh.SetStatistics(fakegithub.Statistics{TotalAssignedJobs: 1})
	h := Start(t, Options{ScaleSetName: "windows-fail", MaxConcurrentRunners: 1, FakeProxmox: p, FakeGitHub: gh, Profiles: []ProfileSpec{{Name: "default", GuestOS: "windows", CPUCores: 2, MemoryMB: 2048, HotSize: 1}}})
	require.Eventually(t, func() bool { return gh.JITMintCount() >= 1 }, 30*time.Second, 100*time.Millisecond)
	vm := awaitNAssignedVMs(t, h, "windows-fail", 1)[0]
	gh.SetStatistics(fakegithub.Statistics{})
	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_proxmox_api_errors_total", formatLabel("operation", "inject_jit"), formatLabel("node", "pve1")) >= 1
	}, 60*time.Second, 200*time.Millisecond)
	require.Eventually(t, func() bool {
		for _, v := range p.Snapshot() {
			if v.VMID == vm.vmid {
				return false
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool {
		for _, id := range gh.RunnerDeletions() {
			if id == firstJITRunnerID {
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond)
}
