package provisioner

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakeproxmox"
)

// TestPowerState_MissingVM_ReturnsEmpty (#360) pins the contract that
// PowerState on a VM Proxmox no longer knows about returns ("", nil) —
// NOT ("stopped", nil). Callers (the power-state poller) treat "" as
// "unknown" and skip it, letting the stuck-state sweep reap a genuinely
// missing row; treating "" as "stopped" would instead misfire a
// job-completed destroy against a VM that may simply be mid-migration.
//
// The absent VM is modelled with a 400 "does not exist" body (the same
// technique the WaitReady not-found test uses): go-proxmox flattens 5xx
// responses to just "500 Internal Server Error" — dropping the body the
// message classifier needs — and nil-derefs on a 404 for this call path,
// so a 400 whose body carries "does not exist" is the reliable way to
// drive PowerState's not-found branch. The poller's error branch also
// treats a raw error as "skip", so callers must never interpret "" as a
// definitive "stopped".
func TestPowerState_MissingVM_ReturnsEmpty(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	// The node exists (go-proxmox's Client.Node hits /nodes/{node}/status).
	mux.HandleFunc("/nodes/pve1/status", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{}}`)
	})
	// The VM itself is absent — respond with the "does not exist" shape
	// the not-found classifier recognises.
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"errors":{"vm":"VM does not exist"}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")

	state, err := p.PowerState(context.Background(), &VM{VMID: 12345, Node: "pve1"})
	require.NoError(t, err, "a missing VM is not an error — it is 'unknown'")
	require.Equal(t, "", state,
		"PowerState must return the empty string (unknown) for a missing VM, never \"stopped\"")
}

// TestPowerState_RunningVM_ReturnsRunning (#360) is the positive twin:
// a live, running VM reports "running" so the poller leaves it alone.
func TestPowerState_RunningVM_ReturnsRunning(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	fp.SeedVM("pve1", 10060, "gh-runner-test-10060", true /* running */, nil)
	p := newTestProvisioner(t, fp.Server, "pve1")

	state, err := p.PowerState(context.Background(), &VM{VMID: 10060, Node: "pve1"})
	require.NoError(t, err)
	require.Equal(t, "running", state)
}

// TestInFlightCloneTracker_ClearedOnCloneError (#356) proves the
// in-flight clone tracker is not leaked when Clone returns an error. The
// tracker entry is Set at the top of Clone and removed by an
// UNCONDITIONAL `defer p.inFlightClones.Delete(...)`, so every return
// path — success, error, and (by Go's defer-runs-during-panic
// guarantee) panic — clears it. A leaked entry would permanently
// suppress the untagged-orphan warning for that VMID.
//
// Note: a panic cannot be injected into the real Clone without modifying
// production code; the error path exercised here shares the same
// unconditional defer, and the panic case is covered by the language
// guarantee that deferred calls run while a panic unwinds.
func TestInFlightCloneTracker_ClearedOnCloneError(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	// Make the clone task complete with a failure exit status so Clone
	// returns an error after the in-flight entry was Set.
	fp.InjectFault(fakeproxmox.Fault{Kind: fakeproxmox.FaultTaskFails, TaskType: "qmclone"})
	p := newTestProvisioner(t, fp.Server, "pve1")

	_, err := p.Clone(context.Background(), CloneOptions{NewVMID: 10061, Node: "pve1", Name: "x"})
	require.Error(t, err, "a failed clone task must surface an error")

	require.Equal(t, 0, p.InFlightCloneCount(),
		"the in-flight clone entry must be cleared on the error return path — no leak")
	require.False(t, p.inFlightClones.Has(10061),
		"the specific VMID must not remain registered as in-flight")
}

// TestInFlightCloneTracker_ClearedOnCloneSuccess (#356) is the
// success-path twin: after a clean clone the tracker returns to empty,
// so InFlightCloneCount never over-reports once the clone completes.
func TestInFlightCloneTracker_ClearedOnCloneSuccess(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{})
	p := newTestProvisioner(t, fp.Server, "pve1")

	_, err := p.Clone(context.Background(), CloneOptions{NewVMID: 10062, Node: "pve1", Name: "x"})
	require.NoError(t, err)

	require.Equal(t, 0, p.InFlightCloneCount(),
		"a completed clone must leave no in-flight entry behind")
}
