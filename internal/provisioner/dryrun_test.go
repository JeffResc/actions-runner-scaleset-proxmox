package provisioner

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
)

// recordingProv counts calls to each Provisioner method so dryRun
// tests can assert which verbs short-circuit and which pass through.
// Read-only return values are canned so passthrough tests can also
// assert the wrapper returns the inner result unchanged.
type recordingProv struct {
	clone, start, stop, destroy, inject     int
	waitReady, readJIT, listOwned           int
	powerState, ping                        int
	templateNode, client                    int
	isRecentlyDestroyed, inFlightCloneCount int

	// Canned read-only return values.
	powerStateOut string
	ownedOut      []*VM
	jitOut        []byte
	pingErr       error
}

func (r *recordingProv) Clone(context.Context, CloneOptions) (*VM, error) {
	r.clone++
	return &VM{}, nil
}
func (r *recordingProv) Start(context.Context, *VM) error   { r.start++; return nil }
func (r *recordingProv) Stop(context.Context, *VM) error    { r.stop++; return nil }
func (r *recordingProv) Destroy(context.Context, *VM) error { r.destroy++; return nil }
func (r *recordingProv) WaitReady(context.Context, *VM, time.Duration) error {
	r.waitReady++
	return nil
}
func (r *recordingProv) InjectJITConfig(context.Context, *VM, string) error {
	r.inject++
	return nil
}
func (r *recordingProv) ReadJITConfig(context.Context, *VM) ([]byte, error) {
	r.readJIT++
	return r.jitOut, nil
}
func (r *recordingProv) ListOwnedVMs(context.Context) ([]*VM, error) {
	r.listOwned++
	return r.ownedOut, nil
}
func (r *recordingProv) PowerState(context.Context, *VM) (string, error) {
	r.powerState++
	return r.powerStateOut, nil
}
func (r *recordingProv) Ping(context.Context) error { r.ping++; return r.pingErr }
func (r *recordingProv) TemplateNode() string       { r.templateNode++; return "pve-bench" }
func (r *recordingProv) Client() *proxmox.Client    { r.client++; return nil }
func (r *recordingProv) IsRecentlyDestroyed(int, time.Duration) bool {
	r.isRecentlyDestroyed++
	return false
}
func (r *recordingProv) InFlightCloneCount() int { r.inFlightCloneCount++; return 0 }

// newDryRunWithLog wires a dryRun whose log writes into the returned
// buffer so callers can assert specific log-line content.
func newDryRunWithLog(t *testing.T, inner Provisioner) (Provisioner, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return NewDryRun(inner, log), &buf
}

// compile-time assertion that the test recorder satisfies Provisioner.
var _ Provisioner = (*recordingProv)(nil)

// Compile-time assertion that NewDryRun's return value satisfies
// Provisioner — if the interface grows a method, this catches a
// missing dryRun method at build time rather than at the first
// runtime call.
var _ Provisioner = (*dryRun)(nil)

// TestDryRun_DestructiveVerbsShortCircuit pins the central
// invariant: each mutating verb must log and NOT call the inner
// provisioner. A leak here would let a refactor accidentally make
// the dry-run wrapper destructive.
func TestDryRun_DestructiveVerbsShortCircuit(t *testing.T) {
	t.Parallel()
	rec := &recordingProv{}
	dr, buf := newDryRunWithLog(t, rec)
	ctx := t.Context()
	vm := &VM{VMID: 10042, Node: "pve1", Name: "gh-runner-test"}

	gotVM, err := dr.Clone(ctx, CloneOptions{NewVMID: 10042, Node: "pve1", Name: "gh-runner-test"})
	require.NoError(t, err)
	require.NotNil(t, gotVM)
	require.Equal(t, 10042, gotVM.VMID, "Clone must synthesise a VM mirroring the requested options")

	require.NoError(t, dr.Start(ctx, vm))
	require.NoError(t, dr.Stop(ctx, vm))
	require.NoError(t, dr.Destroy(ctx, vm))
	require.NoError(t, dr.InjectJITConfig(ctx, vm, "junk-jit"))

	require.Equal(t, 0, rec.clone, "inner Clone must not run in dry-run mode")
	require.Equal(t, 0, rec.start, "inner Start must not run in dry-run mode")
	require.Equal(t, 0, rec.stop, "inner Stop must not run in dry-run mode")
	require.Equal(t, 0, rec.destroy, "inner Destroy must not run in dry-run mode")
	require.Equal(t, 0, rec.inject, "inner InjectJITConfig must not run in dry-run mode")

	// Every destructive verb must log something with the [dry-run]
	// prefix so operators reading the log can see the no-op.
	logs := buf.String()
	for _, want := range []string{"[dry-run] Clone", "[dry-run] Start", "[dry-run] Stop", "[dry-run] Destroy", "[dry-run] InjectJITConfig"} {
		require.Contains(t, logs, want, "expected log line for %q", want)
	}
	// The dry_run=true field is added at the wrapper level.
	require.Contains(t, logs, "dry_run=true",
		"dryRun must tag every log line with dry_run=true so a grep can find dry-run activity")
}

// TestDryRun_ReadOpsPassThrough confirms read-only verbs delegate
// to the inner provisioner unchanged. A regression here would
// silently hide real-world state from the rest of the orchestrator
// during a --dry-run.
func TestDryRun_ReadOpsPassThrough(t *testing.T) {
	t.Parallel()
	wantOwned := []*VM{{VMID: 1, Node: "pve1"}, {VMID: 2, Node: "pve2"}}
	wantJIT := []byte("jit-payload")
	rec := &recordingProv{
		powerStateOut: "running",
		ownedOut:      wantOwned,
		jitOut:        wantJIT,
	}
	dr, _ := newDryRunWithLog(t, rec)
	ctx := t.Context()
	vm := &VM{VMID: 10042, Node: "pve1"}

	require.NoError(t, dr.WaitReady(ctx, vm, time.Second))
	require.Equal(t, 1, rec.waitReady, "WaitReady must passthrough to inner")

	got, err := dr.ReadJITConfig(ctx, vm)
	require.NoError(t, err)
	require.Equal(t, wantJIT, got, "ReadJITConfig must return the inner value unchanged")
	require.Equal(t, 1, rec.readJIT)

	gotOwned, err := dr.ListOwnedVMs(ctx)
	require.NoError(t, err)
	require.Equal(t, wantOwned, gotOwned)
	require.Equal(t, 1, rec.listOwned)

	gotPower, err := dr.PowerState(ctx, vm)
	require.NoError(t, err)
	require.Equal(t, "running", gotPower)
	require.Equal(t, 1, rec.powerState)

	require.NoError(t, dr.Ping(ctx))
	require.Equal(t, 1, rec.ping)

	require.Equal(t, "pve-bench", dr.TemplateNode())
	require.Equal(t, 1, rec.templateNode)

	// Client returns nil from the recorder; the assertion is just
	// that the call landed on inner.
	require.Nil(t, dr.Client())
	require.Equal(t, 1, rec.client)

	require.False(t, dr.IsRecentlyDestroyed(10042, time.Minute))
	require.Equal(t, 1, rec.isRecentlyDestroyed)

	require.Equal(t, 0, dr.InFlightCloneCount())
	require.Equal(t, 1, rec.inFlightCloneCount)
}

// TestDryRun_ReadOpsPropagateErrors covers the inner-returns-error
// path on a read passthrough so a regression that swallows errors
// would surface here.
func TestDryRun_ReadOpsPropagateErrors(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("upstream unavailable")
	rec := &recordingProv{pingErr: wantErr}
	dr, _ := newDryRunWithLog(t, rec)

	err := dr.Ping(t.Context())
	require.ErrorIs(t, err, wantErr, "inner errors must propagate through the dry-run wrapper")
}

// TestNewDryRun_NilLoggerDefaults ensures a nil logger argument
// doesn't panic — the constructor must fall back to slog.Default().
func TestNewDryRun_NilLoggerDefaults(t *testing.T) {
	t.Parallel()
	rec := &recordingProv{}
	require.NotPanics(t, func() {
		dr := NewDryRun(rec, nil)
		// Drive a destructive verb so the logger is actually used.
		_ = dr.Destroy(t.Context(), &VM{VMID: 1, Node: "pve1"})
	})
}

// TestDryRun_CloneReturnsCallerVMID locks in the contract that the
// synthetic VM returned by dry-run Clone carries the operator's
// requested VMID. Callers (the pool manager) feed this back into
// the store, so a different VMID would corrupt downstream state.
func TestDryRun_CloneReturnsCallerVMID(t *testing.T) {
	t.Parallel()
	dr, buf := newDryRunWithLog(t, &recordingProv{})
	vm, err := dr.Clone(t.Context(), CloneOptions{
		NewVMID: 12345,
		Node:    "pve3",
		Name:    "gh-runner-x",
	})
	require.NoError(t, err)
	require.Equal(t, 12345, vm.VMID)
	require.Equal(t, "pve3", vm.Node)
	require.Equal(t, "gh-runner-x", vm.Name)
	// The log line must carry the same VMID for operator
	// traceability.
	require.True(t, strings.Contains(buf.String(), "vmid=12345"),
		"log line must include vmid=12345; got %q", buf.String())
}
