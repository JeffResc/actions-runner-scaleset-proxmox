package provisioner

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakeproxmox"
)

// These tests pin issue #331: a Proxmox task that *completes with a
// failure exit status* (rather than hanging) must surface as an error,
// not be mistaken for success. go-proxmox v0.7.0's Task.WaitFor returns
// nil the instant the task leaves "running" and never inspects
// IsFailed/ExitStatus, so the awaitTask helper is what closes the gap.
// fakeproxmox's FaultTaskFails (issue #326) lets us drive a
// failed-but-completed task end-to-end through the provisioner.

func newFaultProvisioner(t *testing.T, fp *fakeproxmox.Server) *pmox {
	t.Helper()
	return newTestProvisioner(t, fp.Server, "pve1")
}

func TestAwaitTask_CloneTaskFailureSurfacesError(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	fp.InjectFault(fakeproxmox.Fault{Kind: fakeproxmox.FaultTaskFails, TaskType: "qmclone"})

	p := newFaultProvisioner(t, fp)
	_, err := p.Clone(context.Background(), CloneOptions{NewVMID: 10042, Node: "pve1", Name: "x"})
	require.Error(t, err, "a clone task that completes with a failure exitstatus must surface as an error")
	require.Contains(t, err.Error(), "clone")
}

func TestAwaitTask_StartTaskFailureSurfacesError(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	fp.SeedVM("pve1", 10042, "x", false /* stopped */, nil)
	fp.InjectFault(fakeproxmox.Fault{Kind: fakeproxmox.FaultTaskFails, TaskType: "qmstart"})

	p := newFaultProvisioner(t, fp)
	err := p.Start(context.Background(), &VM{VMID: 10042, Node: "pve1"})
	require.Error(t, err, "a start task that completes with a failure exitstatus must surface as an error")
	require.Contains(t, err.Error(), "start")
}

func TestAwaitTask_DestroyTaskFailureSurfacesError(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	// Seed running so the pre-destroy stop step succeeds and the failure
	// lands on the qmdestroy task itself.
	fp.SeedVM("pve1", 10042, "x", true /* running */, nil)
	fp.InjectFault(fakeproxmox.Fault{Kind: fakeproxmox.FaultTaskFails, TaskType: "qmdestroy"})

	p := newFaultProvisioner(t, fp)
	err := p.Destroy(context.Background(), &VM{VMID: 10042, Node: "pve1"})
	require.Error(t, err, "a failed qmdestroy task must NOT be recorded as a successful destroy (#331: VM would leak)")
	require.True(t, strings.Contains(err.Error(), "delete") || strings.Contains(err.Error(), "destroy"),
		"destroy error should mention the failed delete/destroy task, got: %v", err)
}
