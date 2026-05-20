// Package provisioner is the Proxmox-facing side of the orchestrator. It
// turns a high-level intent ("clone a VM for the warm pool", "inject a JIT
// config into VM 10042", "destroy this VM") into the corresponding Proxmox
// VE API calls.
//
// All Provisioner methods accept a context.Context and propagate it to
// every underlying call. Network errors are wrapped with %w so callers can
// errors.Is them against package-level sentinel errors where needed.
package provisioner

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/luthermonson/go-proxmox"

	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/config"
	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/tags"
)

// ErrTemplateNotFound is returned by [New] when the configured template
// VMID cannot be located on any node in the cluster.
var ErrTemplateNotFound = errors.New("provisioner: template VMID not found on any node")

// ErrLinkedCloneCrossNode indicates a Clone with Linked=true that targets a
// node different from the template's node. Linked clones must stay on the
// template's node because they reference its disks.
var ErrLinkedCloneCrossNode = errors.New("provisioner: linked clones must target the template's node")

// Operational sentinels callers can errors.Is against. These wrap the
// underlying go-proxmox / HTTP-status error so the detection live
// inside this package — callers never have to string-match Proxmox
// response text.
var (
	// ErrVMNotFound means the Proxmox API responded with "vm not
	// found" (typically HTTP 500 wrapping "Configuration file
	// 'nodes/.../qemu-server/<vmid>.conf' does not exist" or HTTP 404).
	// Destroy/Stop treat this as idempotent success.
	ErrVMNotFound = errors.New("provisioner: vm not found")

	// ErrVMAlreadyRunning is returned by Start when the VM is already
	// powered on. The caller's desired post-condition is already met.
	ErrVMAlreadyRunning = errors.New("provisioner: vm already running")

	// ErrGuestAgentNotReady is the transient class returned during the
	// brief window where the VM is up but the qemu-guest-agent socket
	// isn't responsive yet (firstboot scripts, systemd churn, etc.).
	// Callers (e.g. scaler.injectWithRetry) should retry with backoff
	// rather than burning the VM.
	ErrGuestAgentNotReady = errors.New("provisioner: qemu-guest-agent not ready")
)

// VM is the orchestrator's view of a Proxmox VM. It is intentionally tiny —
// the persistent store (ent) carries the richer state.
type VM struct {
	VMID int
	Node string
	Name string
}

// CloneOptions are passed to [Provisioner.Clone]. NewVMID is allocated by
// the caller (pool manager); Node is chosen by the NodeSelector.
type CloneOptions struct {
	NewVMID   int
	Node      string
	Name      string
	Linked    bool
	PoweredOn bool
}

// Provisioner is the contract the rest of the orchestrator uses to talk to
// Proxmox. The proxmox-backed implementation is the default; a dry-run
// implementation (see [NewDryRun]) is selected by the `--dry-run` flag.
type Provisioner interface {
	Clone(ctx context.Context, opts CloneOptions) (*VM, error)
	Start(ctx context.Context, vm *VM) error
	Stop(ctx context.Context, vm *VM) error
	Destroy(ctx context.Context, vm *VM) error
	WaitReady(ctx context.Context, vm *VM, timeout time.Duration) error
	InjectJITConfig(ctx context.Context, vm *VM, jitConfig string) error
	ReadAgentFile(ctx context.Context, vm *VM, path string) ([]byte, error)
	ListOwnedVMs(ctx context.Context) ([]*VM, error)

	// PowerState returns the Proxmox status string for the VM —
	// typically "running", "stopped", or "paused". Returns an empty
	// string when the VM cannot be located (callers should treat that
	// as "unknown" and skip — not as "stopped"). Used by the pool
	// manager's power-state poller to detect job completion: the
	// in-VM gh-runner.service powers off after the runner exits, and
	// observing "stopped" on an Assigned/Running row is the orchestrator's
	// completion signal in lieu of an in-VM hook.
	PowerState(ctx context.Context, vm *VM) (string, error)

	// Ping does the cheapest possible Proxmox API call (GET /version) so
	// callers can drive readiness probes. Returns nil iff the API is
	// reachable + the configured credentials still work.
	Ping(ctx context.Context) error

	// TemplateNode reports the node the template VM lives on. Useful for
	// the pool manager when deciding where to place a linked clone.
	TemplateNode() string

	// Client returns the underlying Proxmox client. Exposed for code
	// that needs to issue calls outside the Provisioner's typed surface
	// (e.g. the least-loaded NodeSelector). Callers must not retain the
	// pointer past the Provisioner's lifetime.
	Client() *proxmox.Client
}

// pmox is the production Provisioner backed by github.com/luthermonson/go-proxmox.
type pmox struct {
	cfg          config.ProxmoxConfig
	cli          *proxmox.Client
	scaleSetName string
	vmNamePrefix string // e.g. "gh-runner-<scaleset>-" — used to detect untagged orphans
	templateNode string
	log          *slog.Logger
}

// New constructs a Proxmox-backed Provisioner. It performs a one-time
// scan of the cluster to locate the template VMID and caches the result.
//
// vmNamePrefix is used during ListOwnedVMs as a belt-and-suspenders
// fallback to detect orphans whose tag-apply step failed mid-clone (the
// canonical "Proxmox clone returned but the orchestrator crashed before
// applying our owner tag" failure mode). The go-proxmox library v0.5.1
// doesn't support tags-at-clone-time, so we can't make clone+tag fully
// atomic — name-prefix + VMID-range matching plugs the gap.
func New(ctx context.Context, cfg config.ProxmoxConfig, scaleSetName, vmNamePrefix string, log *slog.Logger) (Provisioner, error) {
	if log == nil {
		log = slog.Default()
	}
	cli := newProxmoxClient(cfg)
	p := &pmox{cfg: cfg, cli: cli, scaleSetName: scaleSetName, vmNamePrefix: vmNamePrefix, log: log}
	if err := p.discoverTemplateNode(ctx); err != nil {
		return nil, err
	}
	log.Info("provisioner ready", "template_vmid", cfg.TemplateVMID, "template_node", p.templateNode)
	return p, nil
}

// newProxmoxClient builds the underlying HTTP+API-token client.
//
// The HTTP transport is wrapped by hashicorp/go-retryablehttp so transient
// Proxmox API hiccups (502/503/504 during pveproxy restarts, DNS blips,
// short-lived connection errors) are retried with exponential backoff
// before surfacing to the caller. Idempotent operations (GET / inspect)
// retry up to 4 times; the underlying library only retries on its
// CheckRetry predicate, which treats 5xx + connection errors as retryable
// but NOT 4xx (e.g. 403/404 fail-fast).
func newProxmoxClient(cfg config.ProxmoxConfig) *proxmox.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	// Pin TLS 1.2 floor (RFC 8996 deprecates TLS 1.0/1.1) regardless of
	// the InsecureSkipVerify opt-in — skipping cert verification doesn't
	// mean we should also accept deprecated protocol versions.
	if tr.TLSClientConfig == nil {
		tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	} else {
		tr.TLSClientConfig.MinVersion = tls.VersionTLS12
	}
	if cfg.InsecureSkipVerify {
		tr.TLSClientConfig.InsecureSkipVerify = true //nolint:gosec // user-opt-in
	}

	retry := retryablehttp.NewClient()
	retry.HTTPClient = &http.Client{
		Transport: tr,
		Timeout:   60 * time.Second,
	}
	retry.RetryMax = 4
	retry.RetryWaitMin = 500 * time.Millisecond
	retry.RetryWaitMax = 8 * time.Second
	// Silence the retryable-http default logger; orchestrator logs the
	// final outcome via the provisioner's slog.
	retry.Logger = nil
	retry.ErrorHandler = func(resp *http.Response, _ error, _ int) (*http.Response, error) {
		// Returning the last response (not an error) lets the typed
		// Proxmox client handle the HTTP status normally, including its
		// own 4xx/5xx error decoding.
		return resp, nil
	}

	hc := retry.StandardClient()
	return proxmox.NewClient(cfg.Endpoint,
		proxmox.WithHTTPClient(hc),
		proxmox.WithAPIToken(cfg.Auth.TokenID, cfg.Auth.TokenSecret),
	)
}

func (p *pmox) TemplateNode() string    { return p.templateNode }
func (p *pmox) Client() *proxmox.Client { return p.cli }

// Ping issues a GET /version against the Proxmox API. This is the
// canonical cheapest call and is the basis for readiness signalling.
func (p *pmox) Ping(ctx context.Context) error {
	if _, err := p.cli.Version(ctx); err != nil {
		return fmt.Errorf("proxmox ping: %w", err)
	}
	return nil
}

// discoverTemplateNode walks the cluster to find the node hosting the
// configured template VMID. If a node has the VMID but the VM isn't a
// template, the scan continues (the VMID might appear on multiple nodes
// in some HA configurations) and the non-template hit is reported at
// end if no real template was found.
func (p *pmox) discoverTemplateNode(ctx context.Context) error {
	statuses, err := p.cli.Nodes(ctx)
	if err != nil {
		return fmt.Errorf("provisioner: list nodes: %w", err)
	}
	var nonTemplateHits []string
	for _, ns := range statuses {
		node, err := p.cli.Node(ctx, ns.Node)
		if err != nil {
			p.log.Warn("provisioner: get node failed; continuing scan", "node", ns.Node, "err", err)
			continue
		}
		vm, err := node.VirtualMachine(ctx, p.cfg.TemplateVMID)
		if err != nil {
			continue
		}
		if !isTemplate(vm) {
			p.log.Warn("provisioner: VMID found but not a template; continuing scan",
				"vmid", p.cfg.TemplateVMID, "node", ns.Node)
			nonTemplateHits = append(nonTemplateHits, ns.Node)
			continue
		}
		p.templateNode = ns.Node
		return nil
	}
	if len(nonTemplateHits) > 0 {
		return fmt.Errorf("%w: vmid=%d found on %v but none are templates",
			ErrTemplateNotFound, p.cfg.TemplateVMID, nonTemplateHits)
	}
	return fmt.Errorf("%w: vmid=%d", ErrTemplateNotFound, p.cfg.TemplateVMID)
}

func isTemplate(vm *proxmox.VirtualMachine) bool {
	return bool(vm.Template)
}

// Clone clones the template VM into NewVMID on opts.Node, applies our owner
// tags, and optionally starts it.
func (p *pmox) Clone(ctx context.Context, opts CloneOptions) (*VM, error) {
	if opts.Linked && opts.Node != p.templateNode {
		return nil, fmt.Errorf("%w: requested node=%s template_node=%s", ErrLinkedCloneCrossNode, opts.Node, p.templateNode)
	}

	templateNode, err := p.cli.Node(ctx, p.templateNode)
	if err != nil {
		return nil, fmt.Errorf("get template node: %w", err)
	}
	templateVM, err := templateNode.VirtualMachine(ctx, p.cfg.TemplateVMID)
	if err != nil {
		return nil, fmt.Errorf("get template vm: %w", err)
	}

	cloneOpts := &proxmox.VirtualMachineCloneOptions{
		NewID: opts.NewVMID,
		Name:  opts.Name,
	}
	if opts.Linked {
		cloneOpts.Full = 0
	} else {
		cloneOpts.Full = 1
		// Target only takes effect for full clones.
		if opts.Node != "" && opts.Node != p.templateNode {
			cloneOpts.Target = opts.Node
		}
	}

	newID, task, err := templateVM.Clone(ctx, cloneOpts)
	if err != nil {
		return nil, fmt.Errorf("issue clone: %w", err)
	}
	if err := task.WaitFor(ctx, 600); err != nil {
		return nil, fmt.Errorf("await clone task: %w", err)
	}

	// Compute the resulting node; linked clones land on the template node.
	resultNode := opts.Node
	if opts.Linked {
		resultNode = p.templateNode
	}
	if resultNode == "" {
		resultNode = p.templateNode
	}

	newNode, err := p.cli.Node(ctx, resultNode)
	if err != nil {
		return nil, fmt.Errorf("get new node: %w", err)
	}
	newVM, err := newNode.VirtualMachine(ctx, newID)
	if err != nil {
		return nil, fmt.Errorf("fetch cloned vm: %w", err)
	}

	initial, err := tags.Initial(p.scaleSetName)
	if err != nil {
		return nil, fmt.Errorf("compute initial tags: %w", err)
	}
	if _, err := newVM.Config(ctx, proxmox.VirtualMachineOption{Name: "tags", Value: tags.Encode(initial)}); err != nil {
		return nil, fmt.Errorf("set owner tags: %w", err)
	}

	if opts.PoweredOn {
		if err := p.startInternal(ctx, newVM); err != nil {
			return nil, err
		}
	}

	return &VM{VMID: newID, Node: resultNode, Name: opts.Name}, nil
}

// Start powers on an existing VM and waits up to 5 minutes for the task to
// settle. The VM may not yet have a working guest agent on return; call
// WaitReady to confirm.
func (p *pmox) Start(ctx context.Context, vm *VM) error {
	pVM, err := p.getVM(ctx, vm)
	if err != nil {
		return err
	}
	return p.startInternal(ctx, pVM)
}

func (p *pmox) startInternal(ctx context.Context, pVM *proxmox.VirtualMachine) error {
	task, err := pVM.Start(ctx)
	if err != nil {
		// If the VM is already running, Proxmox returns an error. Treat as
		// success since the desired post-condition is met.
		if isAlreadyRunning(err) {
			return nil
		}
		return fmt.Errorf("start vm: %w", err)
	}
	if err := task.WaitFor(ctx, 300); err != nil {
		return fmt.Errorf("await start: %w", err)
	}
	return nil
}

// Stop attempts a graceful Shutdown first; if that doesn't settle within
// 60s it falls back to a hard Stop.
func (p *pmox) Stop(ctx context.Context, vm *VM) error {
	pVM, err := p.getVM(ctx, vm)
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	return p.stopInternal(ctx, pVM)
}

// stopInternal does the graceful-then-hard stop with an already-resolved
// *proxmox.VirtualMachine. Lets callers (notably Destroy) reuse a handle
// they already fetched instead of paying the GET-node + GET-vm round
// trips a second time.
func (p *pmox) stopInternal(ctx context.Context, pVM *proxmox.VirtualMachine) error {
	// Best-effort graceful shutdown.
	task, err := pVM.Shutdown(ctx)
	if err == nil {
		if err := task.WaitFor(ctx, 60); err == nil {
			return nil
		}
		p.log.Warn("graceful shutdown timed out; falling back to hard stop", "vmid", pVM.VMID, "node", pVM.Node)
	}
	task, err = pVM.Stop(ctx)
	if err != nil {
		return fmt.Errorf("hard stop: %w", err)
	}
	if err := task.WaitFor(ctx, 60); err != nil {
		return fmt.Errorf("await hard stop: %w", err)
	}
	return nil
}

// Destroy stops (if needed) and deletes a VM. Idempotent: a missing VM is
// treated as success.
func (p *pmox) Destroy(ctx context.Context, vm *VM) error {
	pVM, err := p.getVM(ctx, vm)
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	// Reuse the resolved handle for the stop step — Destroy is on the
	// hot drain path so an extra round trip per VM matters at scale.
	if err := p.stopInternal(ctx, pVM); err != nil {
		p.log.Warn("stop before destroy failed; proceeding to delete anyway", "vmid", vm.VMID, "err", err)
	}
	task, err := pVM.Delete(ctx)
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return fmt.Errorf("delete vm: %w", err)
	}
	if err := task.WaitFor(ctx, 120); err != nil {
		return fmt.Errorf("await delete: %w", err)
	}
	return nil
}

// PowerState returns the Proxmox status string ("running"/"stopped"/...)
// for the VM. A missing VM returns ("", nil) — callers treat that as
// "unknown" and skip the row rather than confuse it with "stopped".
func (p *pmox) PowerState(ctx context.Context, vm *VM) (string, error) {
	if vm == nil {
		return "", fmt.Errorf("power state: nil vm")
	}
	pVM, err := p.getVM(ctx, vm)
	if err != nil {
		if isNotFound(err) {
			return "", nil
		}
		return "", err
	}
	return pVM.Status, nil
}

// WaitReady blocks until the qemu-guest-agent inside the VM is responsive
// or the timeout elapses.
func (p *pmox) WaitReady(ctx context.Context, vm *VM, timeout time.Duration) error {
	pVM, err := p.getVM(ctx, vm)
	if err != nil {
		return err
	}
	seconds := int(timeout.Seconds())
	if seconds < 1 {
		seconds = 60
	}
	if err := pVM.WaitForAgent(ctx, seconds); err != nil {
		return fmt.Errorf("await guest agent: %w", err)
	}
	return nil
}

// ListOwnedVMs returns every VM the orchestrator should consider its
// own, scanned across all nodes. A VM is considered ours when EITHER:
//
//   - It carries our owner tag (the normal case), OR
//   - It has our VM-name prefix AND its VMID is inside our configured
//     range. This catches the rare "clone returned but tag-apply
//     crashed" case where the VM exists in Proxmox but is missing the
//     tag the first detection layer relies on.
//
// Used by the crash-recovery pass on startup; the manager destroys
// every result whose VMID isn't already tracked in the in-memory store.
func (p *pmox) ListOwnedVMs(ctx context.Context) ([]*VM, error) {
	statuses, err := p.cli.Nodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	var out []*VM
	for _, ns := range statuses {
		node, err := p.cli.Node(ctx, ns.Node)
		if err != nil {
			p.log.Warn("list-owned: get node failed; skipping", "node", ns.Node, "err", err)
			continue
		}
		vms, err := node.VirtualMachines(ctx)
		if err != nil {
			p.log.Warn("list-owned: list vms failed; skipping", "node", ns.Node, "err", err)
			continue
		}
		for _, v := range vms {
			vmid := int(v.VMID)
			owned := tags.IsOwnedBy(v.Tags, p.scaleSetName)
			// Untagged orphan detection — both predicates must hold so
			// we never reap a human-created VM that just happens to
			// sit in our range.
			untaggedOrphan := !owned &&
				p.vmNamePrefix != "" &&
				strings.HasPrefix(v.Name, p.vmNamePrefix) &&
				vmid >= p.cfg.VMIDRange.Min &&
				vmid <= p.cfg.VMIDRange.Max
			if !owned && !untaggedOrphan {
				continue
			}
			if untaggedOrphan {
				p.log.Warn("list-owned: untagged orphan detected (likely crash mid-clone)",
					"vmid", vmid, "node", ns.Node, "name", v.Name)
			}
			out = append(out, &VM{VMID: vmid, Node: ns.Node, Name: v.Name})
		}
	}
	return out, nil
}

// getVM resolves *VM into the library's *proxmox.VirtualMachine.
func (p *pmox) getVM(ctx context.Context, vm *VM) (*proxmox.VirtualMachine, error) {
	node, err := p.cli.Node(ctx, vm.Node)
	if err != nil {
		return nil, fmt.Errorf("get node %s: %w", vm.Node, err)
	}
	pVM, err := node.VirtualMachine(ctx, vm.VMID)
	if err != nil {
		return nil, fmt.Errorf("get vm %d on %s: %w", vm.VMID, vm.Node, err)
	}
	return pVM, nil
}

// classifyProxmoxError wraps err with our typed sentinels when the
// underlying go-proxmox / HTTP error matches a known operational
// condition. Detection priority:
//
//  1. Library-typed sentinels (proxmox.ErrNotFound, ErrNotAuthorized,
//     ErrTimeout) — the most stable layer.
//  2. HTTP status codes parsed from the standard "%d Status Text"
//     format the library uses for unhandled 5xx (proxmox.go handleResponse).
//  3. Response-body text patterns ("does not exist", "already running")
//     — least preferred, but kept because Proxmox returns 500s with the
//     real failure in the body and the library passes the body through.
//
// The function returns the (possibly wrapped) error unchanged when no
// known pattern matches — callers still see the original.
func classifyProxmoxError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, proxmox.ErrNotFound) {
		return fmt.Errorf("%w: %w", ErrVMNotFound, err)
	}
	switch httpStatusFromError(err) {
	case http.StatusNotFound:
		return fmt.Errorf("%w: %w", ErrVMNotFound, err)
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "does not exist"):
		return fmt.Errorf("%w: %w", ErrVMNotFound, err)
	case strings.Contains(s, "already running"):
		return fmt.Errorf("%w: %w", ErrVMAlreadyRunning, err)
	}
	return err
}

// httpStatusFromError extracts an HTTP status code from a go-proxmox
// error formatted as "%d Status Text" (e.g. "500 Internal Server
// Error", "404 Not Found"). Returns 0 if no recognisable status prefix
// is present.
func httpStatusFromError(err error) int {
	if err == nil {
		return 0
	}
	s := err.Error()
	// Pull the leading number out of the canonical "NNN Status..." format.
	// We avoid a full regex to keep this hot path cheap.
	end := 0
	for end < len(s) && s[end] >= '0' && s[end] <= '9' {
		end++
	}
	if end < 3 || end > 3 || (end < len(s) && s[end] != ' ') {
		return 0
	}
	n, perr := strconv.Atoi(s[:end])
	if perr != nil {
		return 0
	}
	if n < 100 || n > 599 {
		return 0
	}
	return n
}

// isNotFound is kept as a thin adapter so internal call sites (Stop,
// Destroy, getVM) read naturally. Use the typed ErrVMNotFound externally.
func isNotFound(err error) bool {
	return errors.Is(classifyProxmoxError(err), ErrVMNotFound)
}

// isAlreadyRunning is the equivalent thin adapter for the "start an
// already-running VM" case.
func isAlreadyRunning(err error) bool {
	return errors.Is(classifyProxmoxError(err), ErrVMAlreadyRunning)
}
