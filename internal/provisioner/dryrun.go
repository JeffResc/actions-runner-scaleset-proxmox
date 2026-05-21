package provisioner

import (
	"context"
	"log/slog"
	"time"

	"github.com/luthermonson/go-proxmox"
)

// dryRun wraps a real Provisioner and logs intended side effects instead
// of executing them. Read-only operations (Ping, ListOwnedVMs,
// ReadAgentFile, WaitReady, TemplateNode, Client) pass through to the
// real provisioner so the rest of the orchestrator can still observe
// real-world state.
//
// Enable via `scaleset run --dry-run`.
type dryRun struct {
	inner Provisioner
	log   *slog.Logger
}

// NewDryRun returns a Provisioner that logs destructive intentions
// instead of executing them. The inner Provisioner is used for all
// read-only calls.
func NewDryRun(inner Provisioner, log *slog.Logger) Provisioner {
	if log == nil {
		log = slog.Default()
	}
	return &dryRun{inner: inner, log: log.With("dry_run", true)}
}

func (d *dryRun) Clone(_ context.Context, opts CloneOptions) (*VM, error) {
	d.log.Info("[dry-run] Clone",
		"vmid", opts.NewVMID,
		"node", opts.Node,
		"name", opts.Name,
		"linked", opts.Linked,
		"powered_on", opts.PoweredOn,
	)
	return &VM{VMID: opts.NewVMID, Node: opts.Node, Name: opts.Name}, nil
}

func (d *dryRun) Start(_ context.Context, vm *VM) error {
	d.log.Info("[dry-run] Start", "vmid", vm.VMID, "node", vm.Node)
	return nil
}

func (d *dryRun) Stop(_ context.Context, vm *VM) error {
	d.log.Info("[dry-run] Stop", "vmid", vm.VMID, "node", vm.Node)
	return nil
}

func (d *dryRun) Destroy(_ context.Context, vm *VM) error {
	d.log.Info("[dry-run] Destroy", "vmid", vm.VMID, "node", vm.Node)
	return nil
}

func (d *dryRun) InjectJITConfig(_ context.Context, vm *VM, jitConfig string) error {
	d.log.Info("[dry-run] InjectJITConfig", "vmid", vm.VMID, "node", vm.Node,
		"jit_bytes", len(jitConfig))
	return nil
}

// Read-only passthroughs.
func (d *dryRun) WaitReady(ctx context.Context, vm *VM, timeout time.Duration) error {
	return d.inner.WaitReady(ctx, vm, timeout)
}
func (d *dryRun) ReadAgentFile(ctx context.Context, vm *VM, path string) ([]byte, error) {
	return d.inner.ReadAgentFile(ctx, vm, path)
}
func (d *dryRun) ListOwnedVMs(ctx context.Context) ([]*VM, error) {
	return d.inner.ListOwnedVMs(ctx)
}
func (d *dryRun) PowerState(ctx context.Context, vm *VM) (string, error) {
	return d.inner.PowerState(ctx, vm)
}
func (d *dryRun) Ping(ctx context.Context) error { return d.inner.Ping(ctx) }
func (d *dryRun) TemplateNode() string           { return d.inner.TemplateNode() }
func (d *dryRun) Client() *proxmox.Client        { return d.inner.Client() }

// IsRecentlyDestroyed and InFlightCloneCount pass through to the inner
// provisioner. In dry-run mode we never actually destroy or clone, so
// the inner trackers stay empty and both accessors return zero — but
// the methods exist so the interface is satisfied and the read-side
// stays consistent if a future code path populates the inner trackers
// out-of-band.
func (d *dryRun) IsRecentlyDestroyed(vmid int, cooldown time.Duration) bool {
	return d.inner.IsRecentlyDestroyed(vmid, cooldown)
}
func (d *dryRun) InFlightCloneCount() int { return d.inner.InFlightCloneCount() }
