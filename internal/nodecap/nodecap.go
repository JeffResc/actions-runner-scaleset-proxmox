// Package nodecap is the orchestrator's capacity accountant: it decides
// whether a Proxmox node has room for one more VM of a given shape, and
// hands out reservations so concurrent clone dispatches cannot spend the
// same megabyte twice.
//
// The hard constraint is ALLOCATED memory, not used memory. A booted-but-
// idle 16 GiB VM reports a low RSS while owning its full allocation, so
// admission must count what is promised to the guests on a node, never
// what they happen to be touching. (The least_loaded node selector's
// score deliberately uses USED memory — that is the right input for
// tie-breaking placement and the wrong input for admission. The two are
// not interchangeable.)
//
// Data source is a single GET /cluster/resources, which returns both the
// node rows (physical RAM / cores) and every guest row (qemu and lxc,
// including guests this orchestrator does not own) with the guest's
// configured maxmem and vcpu count. Proxmox derives those two fields from
// the guest CONFIG rather than from a running process, so they are
// correct for stopped guests too — exactly the "promised" number
// admission needs.
//
// All exported methods are safe for concurrent use.
package nodecap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/luthermonson/go-proxmox"
	"golang.org/x/sync/singleflight"
)

// Shape is the resource footprint of one VM the orchestrator is about to
// clone. Built from the runner profile's declared cpu / memory_mb.
type Shape struct {
	MemoryBytes uint64
	VCPUs       int
}

// NodeState is the externally observable capacity of one node. Byte
// counts are absolute; Available is already net of both the host reserve
// and every outstanding reservation, so a caller can compare a Shape
// against it directly.
type NodeState struct {
	TotalBytes     uint64
	ReserveBytes   uint64 // host headroom withheld from admission
	CommittedBytes uint64 // guest allocations + outstanding reservations
	AvailableBytes uint64

	// CPUGated reports whether cpu_overcommit_ratio is configured. When
	// false, CPU is never a reason to refuse a clone and the three
	// fields below are informational only.
	CPUGated bool
	// LimitVCPU is the admission ceiling — physical cores x
	// cpu_overcommit_ratio — and AvailableVCPU is what is left of it.
	// Both are zero when CPUGated is false. (LimitVCPU is deliberately
	// not the raw core count: a consumer comparing a Shape against it
	// must see the same ceiling admission applies.)
	LimitVCPU     int
	CommittedVCPU int
	AvailableVCPU int
}

// Reservation is a claim on a node's capacity held across a clone
// dispatch. The caller Binds it to the VMID once one has been allocated
// and Releases it if the clone fails before the VM exists. A reservation
// that reaches a live VM is never released explicitly — it retires
// itself once the accountant observes the VM at its full configured
// size (see the ledger notes on Accountant).
type Reservation interface {
	// Bind associates the reservation with the VMID that was minted for
	// it. Until this is called the reservation counts in full, because
	// no guest row can possibly account for it yet.
	Bind(vmid int)

	// Release drops the reservation. Idempotent — callers wire it into
	// a sync.OnceFunc and a defer, so the failure paths and the panic
	// guard can all call it.
	Release()
}

// Options configures an Accountant.
type Options struct {
	// Nodes restricts accounting to the operator-declared node list.
	// Empty means "every node the cluster reports".
	Nodes []string

	// Refresh is the TTL on the /cluster/resources snapshot.
	Refresh time.Duration

	// ReserveBytes and ReserveFraction are the host headroom withheld
	// from admission. The effective reserve for a node is the LARGER of
	// the two — the conservative reading of "absolute and/or fraction".
	ReserveBytes    uint64
	ReserveFraction float64

	// CPUOvercommit gates vCPU admission at (physical cores * ratio).
	// Zero disables CPU gating entirely: CPU is time-shared and
	// overcommittable, memory is not, so the default is permissive.
	CPUOvercommit float64

	// ReservationTTL bounds how long a single reservation may survive
	// without retiring. It is a leak guard for a clone that hangs or
	// dies past the pool's panic recovery, not a normal expiry path.
	ReservationTTL time.Duration

	// OwnedVMIDs are the VMID spans this orchestrator allocates clones
	// from — every scale set's range. Guests inside them count against
	// capacity even when powered off, because the warm tier is stopped
	// by design; guests outside them count only while running. See
	// holdsMemory.
	//
	// Leaving this empty makes every stopped guest look foreign, which
	// would stop the pool's own warm VMs from counting. Callers that
	// run a warm pool must populate it.
	OwnedVMIDs []VMIDRange

	Log *slog.Logger
}

// VMIDRange is an inclusive span of Proxmox VMIDs. Mirrors
// config.VMIDRange; redeclared here so this package stays free of the
// config import.
type VMIDRange struct {
	Min int
	Max int
}

// guestStopped is the /cluster/resources status of a powered-off guest.
// Anything else — running, paused, or a status this version of PVE
// reports that we don't recognise — is treated as holding memory.
const guestStopped = "stopped"

// ErrNoSnapshot is returned when the accountant has never successfully
// read /cluster/resources. Admission fails closed in that window: with
// no idea what is allocated we cannot promise not to overcommit. It is a
// cold-start-only condition (once a snapshot lands, a later fetch error
// falls back to the stale one), and cloning is impossible anyway while
// the Proxmox API is unreachable.
var ErrNoSnapshot = errors.New("nodecap: no cluster resource snapshot available")

// defaults applied to zero-valued Options.
const (
	defaultRefresh        = 15 * time.Second
	defaultReservationTTL = 5 * time.Minute
	// fetchTimeout bounds the detached, singleflight-shared fetch. The
	// call is deliberately decoupled from any single caller's context
	// (so one caller's cancellation can't fail every waiter), so it
	// needs its own deadline. Mirrors nodeselector.fetchTimeout.
	fetchTimeout = 30 * time.Second
	// snapshotCacheKey is the single key in the snapshot cache. The
	// cache only ever holds one entry; ttlcache owns the TTL accounting.
	snapshotCacheKey = "resources"
)

// Accountant tracks per-node allocated capacity and hands out
// reservations.
//
// # The ledger
//
// One expression covers every phase of a clone's life:
//
//	committed(N) = Σ observedMem(g) for each non-template guest g on N
//	             + Σ max(0, r.mem - observedMem(r.vmid)) for each reservation r on N
//	             - Σ observedMem(v) for each v in r.freeing, for each r on N
//
// where observedMem(vmid) is the snapshot's maxmem for that VMID, or 0
// when it is absent. The three reservation phases fall out of the middle
// term automatically:
//
//   - unbound (no VMID minted yet): counts in full;
//   - bound, but the post-clone memory override has not landed: counts
//     the difference. This case is why a naive "drop the reservation
//     once the VMID appears" rule would undercount — qmclone copies the
//     TEMPLATE's memory and the provisioner raises it in a later Config
//     call, so for a few seconds the guest row understates the VM;
//   - bound and fully applied: counts zero, and is deleted on the next
//     refresh.
//
// Retirement is by OBSERVATION, not on clone success. Because
// /cluster/resources lags (pvestatd broadcasts on its own cadence),
// releasing a successful clone's reservation eagerly would open a window
// where the VM is absent from both the snapshot and the ledger. Deleting
// the reservation on retirement — rather than merely letting its term go
// to zero — matters too: once the VM is eventually destroyed its
// observedMem returns to 0, which would resurrect a merely-zeroed
// reservation.
type Accountant struct {
	cli  *proxmox.Client
	opts Options
	log  *slog.Logger

	// cache holds the most recent /cluster/resources read. Expired
	// reads return nil from Get; on miss we singleflight a fetch.
	cache *ttlcache.Cache[string, *snapshot]

	// lastFresh is the most recent successful snapshot regardless of
	// TTL. Consulted on the fetch-error path so a transient Proxmox
	// blip degrades to stale data — which, with reservations stacking
	// on top of it, drifts conservative — instead of stalling every
	// clone in the fleet.
	lastFresh *snapshot

	// sf collapses concurrent cache-miss fetches into one API call.
	sf singleflight.Group

	// mu guards lastFresh and res. It is also what makes the
	// fit-then-reserve sequence in reserve() atomic, which is the whole
	// no-double-spend guarantee: two concurrent dispatches for the last
	// 8 GiB cannot both observe room.
	mu sync.Mutex
	// res holds every outstanding reservation, keyed by a monotonic id.
	res     map[uint64]*reservation
	nextRes uint64
}

// snapshot is one immutable read of /cluster/resources.
type snapshot struct {
	// nodes maps node name to its physical totals and the sum of the
	// allocations of the guests it hosts.
	nodes map[string]nodeTotals
	// guestMem / guestVCPU map VMID to the guest's CONFIGURED footprint.
	// Keyed by VMID because that is what the reservation terms need to
	// look up. Templates are excluded — a template is a disk image, not
	// a live allocation.
	guestMem  map[int]uint64
	guestVCPU map[int]int
	takenAt   time.Time
}

type nodeTotals struct {
	memBytes uint64
	vcpus    int
	// guestMem / guestVCPU are the sums over this node's guests,
	// accumulated during fetch so evaluating the ledger is O(reservations)
	// rather than O(guests) per node.
	guestMem  uint64
	guestVCPU int
}

// reservation is one outstanding claim.
type reservation struct {
	id    uint64
	node  string
	shape Shape
	// vmid is 0 until Bind is called.
	vmid int
	// freeing lists VMIDs the caller has committed to destroying in
	// order to make this reservation fit. Their allocation is credited
	// back while they are still visible in the snapshot; each entry
	// drops out once its VMID leaves the snapshot (at which point the
	// snapshot already reflects the freed memory and crediting again
	// would double-count).
	freeing  []int
	expires  time.Time
	released bool
	acct     *Accountant
}

func (r *reservation) Bind(vmid int) {
	r.acct.mu.Lock()
	defer r.acct.mu.Unlock()
	r.vmid = vmid
}

func (r *reservation) Release() {
	r.acct.mu.Lock()
	defer r.acct.mu.Unlock()
	if r.released {
		return
	}
	r.released = true
	delete(r.acct.res, r.id)
}

// New constructs an Accountant over the given Proxmox client.
func New(cli *proxmox.Client, opts Options) (*Accountant, error) {
	if cli == nil {
		return nil, errors.New("nodecap: requires a non-nil proxmox client")
	}
	if opts.Refresh <= 0 {
		opts.Refresh = defaultRefresh
	}
	if opts.ReservationTTL <= 0 {
		opts.ReservationTTL = defaultReservationTTL
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	return &Accountant{
		cli:  cli,
		opts: opts,
		log:  opts.Log,
		cache: ttlcache.New[string, *snapshot](
			ttlcache.WithTTL[string, *snapshot](opts.Refresh),
			ttlcache.WithDisableTouchOnHit[string, *snapshot](),
		),
		res: make(map[uint64]*reservation),
	}, nil
}

// Snapshot returns the current per-node capacity state, net of the host
// reserve and every outstanding reservation. Callers use it to publish
// gauges and to size an eviction plan.
func (a *Accountant) Snapshot(ctx context.Context) (map[string]NodeState, error) {
	snap, err := a.resources(ctx)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sweepLocked(snap)
	out := make(map[string]NodeState, len(snap.nodes))
	for node := range snap.nodes {
		out[node] = a.stateLocked(snap, node)
	}
	return out, nil
}

// Fits reports, for each candidate node, whether shape would be admitted
// there right now. Best effort and immediately stale — it narrows the
// node selector's candidate set, while Reserve remains the single atomic
// gate. A candidate the snapshot does not know (offline, removed, or
// outside the operator's declared list) is reported as not fitting.
func (a *Accountant) Fits(ctx context.Context, shape Shape, candidates []string) (map[string]bool, error) {
	snap, err := a.resources(ctx)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sweepLocked(snap)
	out := make(map[string]bool, len(candidates))
	for _, node := range candidates {
		out[node] = a.fitsLocked(snap, node, shape, nil)
	}
	return out, nil
}

// Reserve atomically claims shape on node. The bool reports whether the
// shape fit; false is ordinary backpressure, not an error. An error means
// the accountant could not establish what is allocated at all.
func (a *Accountant) Reserve(ctx context.Context, node string, shape Shape) (Reservation, bool, error) {
	return a.ReserveFreeing(ctx, node, shape, nil)
}

// ReserveFreeing is Reserve with the allocation of freeing treated as
// already released. The pool's idle-eviction path uses it: it has just
// CAS'd those rows to Draining and queued their destroy, so their memory
// is committed to this reservation even though the snapshot will keep
// reporting them for a few more seconds.
//
// Taking the reservation at eviction time — rather than letting the freed
// memory return to the general pool — is what stops the victim's own
// profile from immediately reclaiming what it just lost.
func (a *Accountant) ReserveFreeing(ctx context.Context, node string, shape Shape, freeing []int) (Reservation, bool, error) {
	snap, err := a.resources(ctx)
	if err != nil {
		return nil, false, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sweepLocked(snap)
	if !a.fitsLocked(snap, node, shape, freeing) {
		return nil, false, nil
	}
	a.nextRes++
	r := &reservation{
		id:      a.nextRes,
		node:    node,
		shape:   shape,
		freeing: append([]int(nil), freeing...),
		expires: time.Now().Add(a.opts.ReservationTTL),
		acct:    a,
	}
	a.res[r.id] = r
	return r, true, nil
}

// fitsLocked evaluates shape against node's remaining capacity. extraFreeing
// is credited on top of the ledger for the caller's own prospective
// reservation (the eviction path); pass nil for a plain fit check.
// Caller must hold a.mu.
func (a *Accountant) fitsLocked(snap *snapshot, node string, shape Shape, extraFreeing []int) bool {
	totals, known := snap.nodes[node]
	if !known {
		// Offline, removed, or outside the declared node list. Never
		// place onto a node we cannot account for.
		return false
	}
	st := a.stateLocked(snap, node)
	credit := uint64(0)
	for _, vmid := range extraFreeing {
		credit += snap.guestMem[vmid]
	}
	avail := st.AvailableBytes + credit
	if shape.MemoryBytes > avail {
		return false
	}
	if a.opts.CPUOvercommit > 0 {
		vcpuCredit := 0
		for _, vmid := range extraFreeing {
			vcpuCredit += snap.guestVCPU[vmid]
		}
		if st.CommittedVCPU-vcpuCredit+shape.VCPUs > a.vcpuLimit(totals.vcpus) {
			return false
		}
	}
	return true
}

// stateLocked evaluates the ledger for one node. Caller must hold a.mu.
func (a *Accountant) stateLocked(snap *snapshot, node string) NodeState {
	totals := snap.nodes[node]
	committed := totals.guestMem
	committedVCPU := totals.guestVCPU

	// Reservation terms. Signed arithmetic: the freeing credits can in
	// principle exceed the pending claims, and unsigned wraparound there
	// would report a wildly empty node.
	delta := int64(0)
	deltaVCPU := 0
	for _, r := range a.res {
		if r.node != node {
			continue
		}
		observed := uint64(0)
		observedVCPU := 0
		if r.vmid != 0 {
			observed = snap.guestMem[r.vmid]
			observedVCPU = snap.guestVCPU[r.vmid]
		}
		if r.shape.MemoryBytes > observed {
			//nolint:gosec // G115: a guest's configured memory is bounded by physical RAM, many orders of magnitude below 2^63 bytes (8 EiB).
			delta += int64(r.shape.MemoryBytes - observed)
		}
		if r.shape.VCPUs > observedVCPU {
			deltaVCPU += r.shape.VCPUs - observedVCPU
		}
		for _, v := range r.freeing {
			//nolint:gosec // G115: as above — guest memory is bounded by physical RAM.
			delta -= int64(snap.guestMem[v])
			deltaVCPU -= snap.guestVCPU[v]
		}
	}
	committed = addClamped(committed, delta)
	committedVCPU += deltaVCPU
	if committedVCPU < 0 {
		committedVCPU = 0
	}

	reserve := a.reserveFor(totals.memBytes)
	st := NodeState{
		TotalBytes:     totals.memBytes,
		ReserveBytes:   reserve,
		CommittedBytes: committed,
		CPUGated:       a.opts.CPUOvercommit > 0,
		CommittedVCPU:  committedVCPU,
	}
	if usable := saturatingSub(totals.memBytes, reserve); usable > committed {
		st.AvailableBytes = usable - committed
	}
	if st.CPUGated {
		st.LimitVCPU = a.vcpuLimit(totals.vcpus)
		if st.LimitVCPU > committedVCPU {
			st.AvailableVCPU = st.LimitVCPU - committedVCPU
		}
	}
	return st
}

// vcpuLimit is the vCPU admission ceiling for a node with the given
// physical core count. Sole definition, shared by the fit check and the
// reported NodeState so the two cannot disagree.
func (a *Accountant) vcpuLimit(cores int) int {
	return int(float64(cores) * a.opts.CPUOvercommit)
}

// holdsMemory reports whether a guest in this state occupies host
// memory that admission must plan around.
//
// A POWERED-OFF guest occupies nothing. Proxmox reserves nothing for
// it, and counting its configured memory would withhold capacity that
// physically exists — on a host with a few dormant VMs that is enough
// to refuse every clone forever. "Allocated, not used" is about a
// running guest that is idle (it owns its full allocation while
// touching little of it); a stopped guest owns nothing at all.
//
// The orchestrator's OWN guests are the exception and always count,
// whatever their power state, because the warm tier is stopped BY
// DESIGN — a warm VM is pre-cloned and powered off precisely so it can
// boot on demand. Skipping those would let the pool clone warm VMs
// without limit, none of them counting, and then oversubscribe the node
// the moment they start.
//
// Ownership is decided by VMID range rather than by owner tag: a fresh
// clone carries its VMID from the instant it exists, whereas its tags
// land in a follow-up API call, so a tag test would misread a clone
// during that window — and, worse, would keep misreading it after its
// reservation retired.
//
// The residual risk is an operator starting a large dormant VM while
// runners hold the memory it wants. That is what reserve_memory_mb is
// for: withhold headroom for guests you know may wake up.
func (a *Accountant) holdsMemory(status string, vmid int) bool {
	if status != guestStopped {
		return true
	}
	for _, r := range a.opts.OwnedVMIDs {
		if vmid >= r.Min && vmid <= r.Max {
			return true
		}
	}
	return false
}

// reserveFor is the host headroom withheld on a node with the given
// physical memory: the larger of the absolute and fractional settings.
func (a *Accountant) reserveFor(total uint64) uint64 {
	frac := uint64(float64(total) * a.opts.ReserveFraction)
	if frac > a.opts.ReserveBytes {
		return frac
	}
	return a.opts.ReserveBytes
}

// sweepLocked retires reservations against a fresh snapshot and drops
// expired ones. Caller must hold a.mu.
//
// A reservation retires once its VM is observed at (or above) its full
// configured size — at that point the guest row carries the allocation
// and keeping the reservation would double-count it. Expiry is the leak
// guard for clones that never got that far.
func (a *Accountant) sweepLocked(snap *snapshot) {
	now := time.Now()
	for id, r := range a.res {
		if now.After(r.expires) {
			a.log.Warn("nodecap: reservation expired without retiring; releasing",
				"node", r.node, "vmid", r.vmid, "memory_bytes", r.shape.MemoryBytes)
			r.released = true
			delete(a.res, id)
			continue
		}
		if r.vmid != 0 && snap.guestMem[r.vmid] >= r.shape.MemoryBytes {
			r.released = true
			delete(a.res, id)
			continue
		}
		// Drop freeing credits whose VMID has left the snapshot: the
		// snapshot now reflects the freed memory on its own, and
		// crediting it a second time would over-report capacity.
		if len(r.freeing) > 0 {
			kept := r.freeing[:0]
			for _, v := range r.freeing {
				if _, still := snap.guestMem[v]; still {
					kept = append(kept, v)
				}
			}
			r.freeing = kept
		}
	}
}

// resources returns the current snapshot, fetching on cache miss.
// Mirrors nodeselector.leastLoaded.scores: singleflight so a burst of
// concurrent dispatches makes one API call, and a stale fallback so a
// transient Proxmox error doesn't stall admission fleet-wide.
func (a *Accountant) resources(ctx context.Context) (*snapshot, error) {
	if item := a.cache.Get(snapshotCacheKey); item != nil {
		return item.Value(), nil
	}
	v, err, _ := a.sf.Do(snapshotCacheKey, func() (any, error) {
		// Detached from the triggering caller's ctx: this fetch is
		// shared by every concurrent waiter, so one caller abandoning
		// it must not fail the rest. WithoutCancel keeps ctx values
		// (tracing) but drops cancellation; the timeout bounds it.
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
		defer cancel()
		fresh, err := a.fetch(fetchCtx)
		if err != nil {
			return nil, err
		}
		a.cache.Set(snapshotCacheKey, fresh, ttlcache.DefaultTTL)
		a.mu.Lock()
		a.lastFresh = fresh
		a.mu.Unlock()
		return fresh, nil
	})
	if err != nil {
		a.mu.Lock()
		prev := a.lastFresh
		a.mu.Unlock()
		if prev != nil {
			a.log.Debug("nodecap: resource fetch failed; using last known-good snapshot",
				"age", time.Since(prev.takenAt), "err", err)
			return prev, nil
		}
		return nil, fmt.Errorf("%w: %w", ErrNoSnapshot, err)
	}
	return v.(*snapshot), nil
}

// fetch reads GET /cluster/resources and projects it into a snapshot.
//
// The raw client Get is used rather than Client.Cluster().Resources()
// because the latter issues an extra /cluster/status round-trip per call
// that carries nothing this package needs.
func (a *Accountant) fetch(ctx context.Context) (*snapshot, error) {
	var rs proxmox.ClusterResources
	if err := a.cli.Get(ctx, "/cluster/resources", &rs); err != nil {
		return nil, fmt.Errorf("list cluster resources: %w", err)
	}
	snap := &snapshot{
		nodes:     map[string]nodeTotals{},
		guestMem:  map[int]uint64{},
		guestVCPU: map[int]int{},
		takenAt:   time.Now(),
	}
	want := func(name string) bool {
		return len(a.opts.Nodes) == 0 || slices.Contains(a.opts.Nodes, name)
	}
	// Guest rows and node rows arrive in one undifferentiated list with
	// no ordering guarantee, so guests are accumulated separately and
	// folded into the node totals afterwards.
	guestSums := map[string]nodeTotals{}
	for _, r := range rs {
		switch r.Type {
		case "node":
			// An offline node reports a non-online status (and stale or
			// zero totals). Leaving it out of snap.nodes means
			// fitsLocked refuses to place there, which is what we want.
			if !want(r.Node) || (r.Status != "" && r.Status != "online") {
				continue
			}
			snap.nodes[r.Node] = nodeTotals{
				memBytes: r.MaxMem,
				vcpus:    int(r.MaxCPU), //nolint:gosec // physical core counts are far below int range
			}
		case "qemu", "lxc":
			// Templates own no memory — they are disk images. Foreign
			// guests that DO hold memory still count: a node's real
			// capacity is its total minus everything allocated on it,
			// not just minus this orchestrator's own VMs.
			if !want(r.Node) || r.Template != 0 {
				continue
			}
			vmid := int(r.VMID) //nolint:gosec // VMIDs are bounded well below int range
			if !a.holdsMemory(r.Status, vmid) {
				continue
			}
			vcpu := int(r.MaxCPU) //nolint:gosec // vcpu counts are far below int range
			snap.guestMem[vmid] = r.MaxMem
			snap.guestVCPU[vmid] = vcpu
			sum := guestSums[r.Node]
			sum.guestMem += r.MaxMem
			sum.guestVCPU += vcpu
			guestSums[r.Node] = sum
		}
	}
	for node, totals := range snap.nodes {
		totals.guestMem = guestSums[node].guestMem
		totals.guestVCPU = guestSums[node].guestVCPU
		snap.nodes[node] = totals
	}
	return snap, nil
}

// addClamped adds a signed delta to an unsigned base, flooring at 0.
func addClamped(base uint64, delta int64) uint64 {
	if delta >= 0 {
		return base + uint64(delta)
	}
	return saturatingSub(base, uint64(-delta))
}

func saturatingSub(a, b uint64) uint64 {
	if b > a {
		return 0
	}
	return a - b
}
