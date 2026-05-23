// Package ipam abstracts allocation of static IPs to runner VMs.
//
// The orchestrator's clone path consults an Allocator before
// invoking Proxmox: Allocate hands back an IP string the
// provisioner stamps onto the VM (via Proxmox's built-in cloud-init
// `ipconfig0` field) so the VM boots with a known address;
// Release is called on destroy so the IP returns to the pool.
//
// The default implementation is [Noop], which returns an empty IP
// (no static-IP assignment — Proxmox cloud-init falls back to
// DHCP) and a no-op Release. Operators integrating with an
// external IPAM (NetBox, Infoblox, phpIPAM, ...) ship their own
// Allocator and wire it via the per-profile config.
//
// All Allocators must be safe for concurrent use — the pool's
// runClone goroutines may call Allocate / Release at the
// fleet-wide cloneSem / destroySem cap.
package ipam

import (
	"context"
	"errors"
	"fmt"
)

// ErrNoAllocation is returned by Release when the allocator has no
// record of an IP for the given VMID. Implementations should treat
// this as success (the row may have been destroyed before any IP
// was stamped) and the pool manager logs without escalating.
var ErrNoAllocation = errors.New("ipam: no allocation for vmid")

// Allocator hands out and reclaims static IPs by VMID. Empty IP
// strings are allowed and mean "no static assignment for this
// VM" — the provisioner skips the ipconfig0 write and Proxmox
// cloud-init falls back to DHCP.
type Allocator interface {
	// Allocate reserves an IP for vmid. Returns the IP (empty for
	// no-op allocators) plus any allocator-side error. Errors
	// fail the clone — callers can degrade to DHCP by configuring
	// the noop allocator instead.
	Allocate(ctx context.Context, vmid int) (string, error)

	// Release returns the IP previously allocated for vmid to
	// the pool. Idempotent — a double release (or a release for
	// a vmid that never allocated) returns nil; implementations
	// that surface "not found" as a typed error wrap
	// ErrNoAllocation so callers can errors.Is past it.
	Release(ctx context.Context, vmid int) error
}

// Noop is the zero-cost default Allocator. Operators that don't
// integrate with an external IPAM use this implicitly; it lets the
// rest of the orchestrator call Allocate / Release unconditionally
// without paying any cost.
type Noop struct{}

// Allocate always returns the empty string. The provisioner skips
// the ipconfig0 write when the IP is empty, falling back to DHCP.
func (Noop) Allocate(context.Context, int) (string, error) { return "", nil }

// Release is a no-op for the noop allocator. Returns nil
// unconditionally.
func (Noop) Release(context.Context, int) error { return nil }

// Static is a tiny in-memory Allocator that hands out IPs from a
// fixed list. Intended for e2e tests and small homelab setups
// where the operator knows the address range in advance; it
// rejects allocations once the list is exhausted. Production
// users should plug in a real IPAM (NetBox, etc.).
type Static struct {
	pool []string
	in   map[int]string // vmid -> ip
}

// NewStatic builds a Static allocator from a slice of IPs. The
// slice is defensively copied. Empty slices are rejected — an
// empty pool would deadlock the clone path the first time
// Allocate is called.
func NewStatic(pool []string) (*Static, error) {
	if len(pool) == 0 {
		return nil, errors.New("ipam: static pool must be non-empty")
	}
	cp := make([]string, len(pool))
	copy(cp, pool)
	return &Static{pool: cp, in: make(map[int]string, len(cp))}, nil
}

// Allocate hands out the next free IP from the pool. Returns an
// error when the pool is exhausted — callers (the orchestrator)
// surface this as a clone failure that the next reconcile tick
// will retry against the (possibly now-released) pool.
func (s *Static) Allocate(_ context.Context, vmid int) (string, error) {
	if existing, ok := s.in[vmid]; ok {
		// Idempotent: repeated Allocate for the same vmid returns
		// the already-assigned IP. Useful when the pool's clone
		// retry path re-runs Allocate on a row whose previous
		// attempt errored after allocation.
		return existing, nil
	}
	for _, ip := range s.pool {
		assigned := false
		for _, taken := range s.in {
			if taken == ip {
				assigned = true
				break
			}
		}
		if !assigned {
			s.in[vmid] = ip
			return ip, nil
		}
	}
	return "", fmt.Errorf("ipam: static pool exhausted (%d IPs)", len(s.pool))
}

// Release returns the IP for vmid to the free pool. A double
// release (vmid not present) is a no-op — matches the noop
// allocator's contract.
func (s *Static) Release(_ context.Context, vmid int) error {
	delete(s.in, vmid)
	return nil
}
