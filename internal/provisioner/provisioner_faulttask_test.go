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

// ---------------------------------------------------------------------------
// Snapshot create / rollback (recycle_mode: snapshot_rollback support)
// ---------------------------------------------------------------------------

// TestSnapshotCreateAndRollback_RoundTrip drives both snapshot methods
// through the real go-proxmox client against fakeproxmox: create the
// named snapshot, then roll back to it. Verifies the wire paths, the
// task-await handling, and the fake's counters end-to-end.
func TestSnapshotCreateAndRollback_RoundTrip(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	fp.SeedVM("pve1", 10050, "gh-runner-test-10050", false /* stopped */, nil)

	p := newTestProvisioner(t, fp.Server, "pve1")
	vm := &VM{VMID: 10050, Node: "pve1", Name: "gh-runner-test-10050"}

	require.NoError(t, p.SnapshotCreate(context.Background(), vm, "scaleset-clean"))
	require.NoError(t, p.SnapshotRollback(context.Background(), vm, "scaleset-clean"))

	for _, v := range fp.Snapshot() {
		if v.VMID != 10050 {
			continue
		}
		require.Equal(t, []string{"scaleset-clean"}, v.Snapshots)
		require.Equal(t, 1, v.SnapshotCreates)
		require.Equal(t, 1, v.Rollbacks)
		return
	}
	t.Fatal("vm 10050 disappeared from the fake")
}

// TestSnapshotRollback_MissingSnapshotFails: rolling back to a snapshot
// that was never created must return an error carrying the vmid/node
// context — the pool's rollback worker keys its destroy fallback on it.
func TestSnapshotRollback_MissingSnapshotFails(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	fp.SeedVM("pve1", 10051, "gh-runner-test-10051", false, nil)

	p := newTestProvisioner(t, fp.Server, "pve1")
	err := p.SnapshotRollback(context.Background(), &VM{VMID: 10051, Node: "pve1"}, "scaleset-clean")
	require.Error(t, err)
	require.Contains(t, err.Error(), "10051")
	require.Contains(t, err.Error(), "pve1")
}

// TestSnapshotCreate_TaskFailureSurfacesError: a snapshot task that
// completes with a failure exit status (e.g. storage without snapshot
// support) must surface as an error, mirroring the other awaitTask
// call sites pinned above.
func TestSnapshotCreate_TaskFailureSurfacesError(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	fp.SeedVM("pve1", 10052, "gh-runner-test-10052", false, nil)
	fp.InjectFault(fakeproxmox.Fault{Kind: fakeproxmox.FaultTaskFails, TaskType: "qmsnapshot"})

	p := newTestProvisioner(t, fp.Server, "pve1")
	err := p.SnapshotCreate(context.Background(), &VM{VMID: 10052, Node: "pve1"}, "scaleset-clean")
	require.Error(t, err, "a snapshot task that completes with a failure exitstatus must surface as an error")
	require.Contains(t, err.Error(), "snapshot")
}
