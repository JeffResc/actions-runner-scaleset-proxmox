package pool

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/nodecap"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/nodeselector"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/store"
)

// This file holds the pool's half of resource-aware admission: turning a
// profile into a Shape, taking and releasing capacity reservations, and
// the idle-eviction pass that lets queued demand reclaim memory from an
// idle pool VM.
//
// The accountant (internal/nodecap) owns the ledger and the atomicity
// guarantee; everything here is policy about WHEN to ask it.

// parkedReservationTTL bounds how long an eviction's pre-taken
// reservation is worth popping. Sized as a handful of reconcile ticks:
// long enough for the victim's destroy to land and the next tick to
// consume the handle, short enough that a wedged destroy doesn't hold
// memory hostage. The accountant enforces its own (longer) TTL on the
// underlying reservation as the real leak guard.
const parkedReservationTTL = 90 * time.Second

// shapeOf projects a profile's declared hardware into the accountant's
// Shape. Config validation guarantees MemoryMB > 0 whenever capacity
// admission is enabled, so a zero here can only mean the feature is off
// — in which case the shape is never consulted.
func shapeOf(s ProfileSettings) nodecap.Shape {
	return nodecap.Shape{
		MemoryBytes: uint64(max(s.MemoryMB, 0)) * 1024 * 1024,
		VCPUs:       s.CPUCores,
	}
}

// capacityClaim is one clone's admission handle. When admission is
// disabled every field is inert and both methods are no-ops, so the
// clone path carries no branches of its own.
type capacityClaim struct {
	res nodecap.Reservation // nil when admission is disabled
	// release is idempotent (sync.OnceFunc) and never nil, so the
	// several failure paths can all call it without coordinating.
	release func()
}

func (c capacityClaim) bind(vmid int) {
	if c.res != nil {
		c.res.Bind(vmid)
	}
}

// admitClone decides where one clone of ps goes and claims the capacity
// for it. It is the whole admission decision for a dispatch: a false
// bool means "not now", and the caller abandons the dispatch before
// anything is persisted.
//
// Order matters. A reservation parked by an earlier eviction is
// consumed BEFORE the node selector is consulted, because that claim
// already names a node and is itself holding that node's free bytes.
// Asking the capacity wrapper first would get ErrNoCapacity for the
// very dispatch the eviction was performed for — and the resulting
// deferral would re-arm eviction, so each tick would sacrifice another
// idle VM while the queued job still never got one.
//
// Skipping the selector does NOT skip its rules: eviction picked the
// parked node through the same selector (see evictionNode), and
// stillEligible re-checks it here, because up to parkedReservationTTL
// passes in between and anti-affinity is a promise about the node's
// CURRENT occupants ("untrusted-PR runners never co-schedule with
// prod"). A node that stopped being eligible in that window loses the
// claim rather than the promise.
func (m *manager) admitClone(ctx context.Context, ps *profileState, kind store.PoolKind) (string, capacityClaim, bool) {
	// Only a HOT clone may consume a parked reservation. Eviction
	// destroys a sibling's idle VM specifically so a queued job can run;
	// letting this profile's own warm refill spend that memory instead
	// would sacrifice the victim for nothing and leave the job waiting.
	if m.cfg.Capacity != nil && kind == store.PoolKindHot {
		if node, claim, ok := m.claimParked(ctx, ps); ok {
			return node, claim, true
		}
	}

	node, ok := m.selectNode(ctx, ps, kind)
	if !ok {
		return "", capacityClaim{}, false
	}
	claim, ok := m.reserveCapacity(ctx, ps, node, kind)
	if !ok {
		return "", capacityClaim{}, false
	}
	return node, claim, true
}

// claimParked consumes one reservation parked by an earlier eviction,
// if any is still usable.
//
// Every parked claim is considered, not just the first: they may sit on
// different nodes, and one having gone stale must not hide a good one
// behind it for a whole reconcile tick. A claim whose node is
// definitively no longer eligible is handed back; one the selector
// cannot answer for is kept parked and retried next tick, because
// releasing it would throw away memory an idle VM was destroyed to free.
func (m *manager) claimParked(ctx context.Context, ps *profileState) (string, capacityClaim, bool) {
	parked := ps.drainEvicted()
	if len(parked) == 0 {
		return "", capacityClaim{}, false
	}
	var (
		chosen *parkedReservation
		keep   []*parkedReservation
	)
	for _, p := range parked {
		if chosen != nil {
			keep = append(keep, p)
			continue
		}
		eligible, known := m.nodeEligible(ctx, ps, p.node)
		switch {
		case eligible:
			chosen = p
		case known:
			m.log.Info("clone: discarding parked eviction claim; node is no longer an eligible placement",
				"profile", ps.settings.Name, "node", p.node)
			p.res.Release()
		default:
			m.log.Debug("clone: keeping parked eviction claim; node eligibility unknown this tick",
				"profile", ps.settings.Name, "node", p.node)
			keep = append(keep, p)
		}
	}
	ps.parkAll(keep)
	if chosen == nil {
		return "", capacityClaim{}, false
	}
	m.log.Debug("clone: consuming reservation parked by an earlier eviction",
		"profile", ps.settings.Name, "node", chosen.node)
	return chosen.node, claimFor(chosen.res), true
}

// nodeEligible reports whether node is a legal placement for a clone of
// ps, ignoring capacity.
//
// It asks the ordinary selector with every OTHER node in the avoid list,
// so the verdict comes from the same affinity rules and strategy that
// govern a real dispatch — no second copy of that logic to drift. The
// Shape is left zero, which makes the capacity wrapper a pass-through:
// whether there is ROOM is a separate question, and for both callers
// here it is either already answered or beside the point.
//
// known distinguishes "the selector says no" from "the selector could
// not say". They must not be conflated: for a parked eviction claim,
// treating a transient least_loaded read failure as a refusal would
// throw away memory an idle VM was just destroyed to free.
func (m *manager) nodeEligible(ctx context.Context, ps *profileState, node string) (eligible, known bool) {
	if m.cfg.LinkedClones {
		// Placement isn't the selector's to make in this mode.
		return node == m.cfg.TemplateNode, true
	}
	states, err := m.cfg.Capacity.Snapshot(ctx)
	if err != nil {
		return false, false
	}
	avoid := make([]string, 0, len(states))
	for n := range states {
		if n != node {
			avoid = append(avoid, n)
		}
	}
	got, err := m.sel.Select(ctx, nodeselector.Hint{
		Profile:     ps.settings.Name,
		ExistingVMs: m.snapshotExistingVMsForAffinity(),
		Avoid:       avoid,
	})
	switch {
	case errors.Is(err, nodeselector.ErrAffinityRequireUnsatisfiable):
		// Definitive. A hard pin is operator config: it will not resolve
		// on its own, so a claim on this node is never coming good and
		// its memory should go back to the fleet now.
		return false, true
	case err != nil:
		// Everything else is treated as "can't say", and callers hold
		// rather than discard. The refusals that land here — a node
		// currently hosting an anti-affinity violator, a transient
		// least_loaded read failure — are the kind that resolve
		// themselves once a job finishes or the API recovers, and the
		// avoid list here deliberately names every other node, so the
		// error shapes are not reliably distinguishable anyway.
		return false, false
	}
	return got == node, true
}

// selectNode runs node selection for a clone of ps, translating a
// capacity refusal into a counted deferral rather than an error.
func (m *manager) selectNode(ctx context.Context, ps *profileState, kind store.PoolKind) (string, bool) {
	// Profile + existing-row context lets the affinity wrapper apply
	// prefer_nodes / anti_affinity_with; Shape lets the capacity wrapper
	// skip nodes without room. The plain single / round_robin /
	// least_loaded selectors ignore all three. The row snapshot is
	// bounded by MaxConcurrentRunners, so building it per clone is cheap.
	node, err := m.sel.Select(ctx, nodeselector.Hint{
		Profile:     ps.settings.Name,
		ExistingVMs: m.snapshotExistingVMsForAffinity(),
		Shape:       shapeOf(ps.settings),
	})
	if err != nil {
		// A capacity refusal is expected backpressure, not a fault: the
		// nodes are simply full. Log at Debug and count it, so a
		// saturated fleet doesn't drown the log in warnings.
		if errors.Is(err, nodeselector.ErrNoCapacity) {
			m.recordCapacityDeferral(ps, "none", kind)
			m.log.Debug("clone: deferred, no node has capacity",
				"profile", ps.settings.Name, "kind", kind, "err", err)
			return "", false
		}
		m.log.Warn("clone: node selection failed", "profile", ps.settings.Name, "err", err)
		return "", false
	}
	if m.cfg.LinkedClones {
		// A linked clone must stay on the template's node, so the
		// selector's answer is discarded — and the reservation is taken
		// against THIS node, since charging the node we selected but
		// will not use would account for the clone in the wrong place.
		node = m.cfg.TemplateNode
	}
	return node, true
}

// reserveCapacity claims capacity for one clone of ps on node.
//
// The returned bool is the admission decision: false means "no room
// right now", which is ordinary backpressure — the caller abandons this
// dispatch, nothing is persisted, and the next reconcile tick retries.
// It is never an error the operator needs to see, so it is logged at
// Debug and counted rather than warned.
func (m *manager) reserveCapacity(ctx context.Context, ps *profileState, node string, kind store.PoolKind) (capacityClaim, bool) {
	if m.cfg.Capacity == nil {
		return capacityClaim{release: func() {}}, true
	}
	profileName := ps.settings.Name

	res, ok, err := m.cfg.Capacity.Reserve(ctx, node, shapeOf(ps.settings))
	if err != nil {
		// Only reachable before the accountant's first successful
		// snapshot — after that it serves stale data rather than
		// failing. Admitting blind here would be exactly the
		// overcommit this feature exists to prevent, so defer.
		m.recordCapacityDeferral(ps, node, kind)
		m.log.Warn("clone: capacity unknown; deferring", "profile", profileName, "node", node, "err", err)
		return capacityClaim{}, false
	}
	if !ok {
		m.recordCapacityDeferral(ps, node, kind)
		m.log.Debug("clone: deferred, node is at capacity",
			"profile", profileName, "node", node, "kind", kind,
			"memory_mb", ps.settings.MemoryMB)
		return capacityClaim{}, false
	}
	return claimFor(res), true
}

func claimFor(res nodecap.Reservation) capacityClaim {
	return capacityClaim{res: res, release: sync.OnceFunc(res.Release)}
}

// recordCapacityDeferral counts one clone the capacity gate turned away
// and, for a hot clone, arms the idle-eviction trigger. node is "none"
// when no candidate node had room at all.
func (m *manager) recordCapacityDeferral(ps *profileState, node string, kind store.PoolKind) {
	if kind == store.PoolKindHot {
		ps.capacityDeferredHot.Store(true)
	}
	if m.metrics == nil {
		return
	}
	m.metrics.CloneDeferredCapacity.
		WithLabelValues(m.cfg.ScaleSetName, metricsProfile(ps.settings.Name), node, string(kind)).Inc()
}

// drainEvicted removes and returns every unexpired parked reservation.
// Each handle carries the node its eviction freed capacity on, so the
// caller takes the node from the claim rather than choosing one first.
//
// Draining rather than popping one keeps the eligibility checks — which
// hit the selector and the accountant — outside evictMu. Whatever the
// caller doesn't use goes back via parkAll.
//
// Expired handles are released on the way past; the capacity they
// covered has long since been reclaimed by the accountant's own TTL.
func (ps *profileState) drainEvicted() []*parkedReservation {
	ps.evictMu.Lock()
	defer ps.evictMu.Unlock()
	now := time.Now()
	out := make([]*parkedReservation, 0, len(ps.evicted))
	for _, p := range ps.evicted {
		if now.After(p.expires) {
			p.res.Release()
			continue
		}
		out = append(out, p)
	}
	ps.evicted = nil
	return out
}

// parkAll returns unconsumed handles to the profile, preserving their
// original expiry.
func (ps *profileState) parkAll(rs []*parkedReservation) {
	if len(rs) == 0 {
		return
	}
	ps.evictMu.Lock()
	defer ps.evictMu.Unlock()
	ps.evicted = append(ps.evicted, rs...)
}

// parkEvicted stores a reservation for a later tick to consume.
func (ps *profileState) parkEvicted(node string, res nodecap.Reservation) {
	ps.evictMu.Lock()
	defer ps.evictMu.Unlock()
	ps.evicted = append(ps.evicted, &parkedReservation{
		node:    node,
		res:     res,
		expires: time.Now().Add(parkedReservationTTL),
	})
}

// releaseParkedReservations drops every parked handle. Called on drain so
// a shutting-down manager doesn't leave phantom claims in a shared
// accountant for the rest of their TTL.
func (m *manager) releaseParkedReservations() {
	for _, ps := range m.profiles {
		ps.evictMu.Lock()
		for _, p := range ps.evicted {
			p.res.Release()
		}
		ps.evicted = nil
		ps.evictMu.Unlock()
	}
}

// publishCapacityGauges exports the accountant's per-node ledger. Cheap:
// Snapshot is served from the same TTL cache the admission path uses, so
// a reconcile tick costs at most one shared Proxmox call.
func (m *manager) publishCapacityGauges(ctx context.Context) {
	if m.cfg.Capacity == nil || m.metrics == nil {
		return
	}
	states, err := m.cfg.Capacity.Snapshot(ctx)
	if err != nil {
		m.log.Debug("capacity: snapshot failed; gauges not refreshed this tick", "err", err)
		return
	}
	for node, st := range states {
		m.metrics.NodeMemoryTotalBytes.WithLabelValues(node).Set(float64(st.TotalBytes))
		m.metrics.NodeMemoryCommittedBytes.WithLabelValues(node).Set(float64(st.CommittedBytes))
		m.metrics.NodeMemoryAvailableBytes.WithLabelValues(node).Set(float64(st.AvailableBytes))
	}
}

// ---------------------------------------------------------------------------
// Idle eviction
// ---------------------------------------------------------------------------

// evictionCandidate is one idle VM whose allocation could be reclaimed.
type evictionCandidate struct {
	row       *store.VM
	memBytes  uint64
	preferred bool // Warm: cheaper to lose than a booted Hot VM
}

// evictForDemand tries to free enough capacity on some node for one
// clone of ps, by destroying this scale set's own idle VMs.
//
// It is called only when a DEMAND-driven clone was refused — a real job
// is queued on GitHub and cannot start. Baseline hot/warm top-up never
// evicts: two profiles trading idle VMs back and forth would thrash
// forever and never converge, whereas a queued job is a claim that
// outranks speculative warmth.
//
// The clone is deliberately NOT dispatched here. The victim's qmdestroy
// takes seconds, and cloning a replacement before it lands would put the
// node genuinely over its memory for that window. Instead the freed
// capacity is reserved for ps and parked, and the next reconcile tick —
// which still sees the same unmet demand — consumes it.
//
// Returns true when an eviction was started.
func (m *manager) evictForDemand(ctx context.Context, ps *profileState) bool {
	if m.cfg.Capacity == nil || !m.cfg.EvictIdleForDemand {
		return false
	}
	shape := shapeOf(ps.settings)
	states, err := m.cfg.Capacity.Snapshot(ctx)
	if err != nil {
		m.log.Warn("evict: capacity snapshot failed", "profile", ps.settings.Name, "err", err)
		return false
	}

	// Every node this profile could legally be placed on, in a stable
	// order so a tie between two equally viable nodes doesn't oscillate
	// between reconcile passes.
	//
	// Eligibility is what scopes the whole pass. Without it a profile
	// pinned by an affinity rule (or by linked clones) would destroy
	// idle VMs on a node it can never land on, every tick, with no
	// progress and a steadily shrinking pool.
	eligible := m.eligibleNodes(ctx, ps, states)
	if len(eligible) == 0 {
		// Typically an unsatisfiable hard pin: no amount of freed memory
		// would make this profile placeable anywhere.
		m.log.Debug("evict: no placeable node for profile; nothing to evict for",
			"profile", ps.settings.Name)
		return false
	}

	// If an eligible node can already take the clone, evict nothing: the
	// idle VM and the queued job can coexist, and destroying warmth we
	// don't need to destroy is pure loss. Restricted to ELIGIBLE nodes —
	// room on a node this profile is pinned away from is room it can
	// never use, and letting that veto the eviction would stall the
	// queued job indefinitely rather than for a tick.
	for _, node := range eligible {
		if states[node].AvailableBytes >= shape.MemoryBytes {
			return false
		}
	}

	byNode, err := m.idleCandidatesByNode()
	if err != nil {
		m.log.Warn("evict: listing idle rows failed", "profile", ps.settings.Name, "err", err)
		return false
	}

	// Take the first eligible node that has a viable victim set. Scanning
	// rather than committing to the selector's single top pick matters:
	// under least_loaded that pick is whichever node currently scores
	// best, which may hold none of this scale set's idle VMs while a
	// sibling node — an equally legal placement — is full of them.
	for _, node := range eligible {
		victims, freed := planEviction(byNode[node], shape.MemoryBytes, states[node].AvailableBytes)
		if len(victims) == 0 {
			continue
		}
		if m.startEviction(ctx, ps, node, shape, victims, freed) {
			return true
		}
	}
	return false
}

// eligibleNodes returns, in sorted order, the subset of the snapshot's
// nodes that this profile could legally be placed on. Nodes the selector
// couldn't answer for are omitted — for eviction, "don't know" and "no"
// lead to the same safe action.
func (m *manager) eligibleNodes(ctx context.Context, ps *profileState, states map[string]nodecap.NodeState) []string {
	nodes := make([]string, 0, len(states))
	for node := range states {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)

	out := nodes[:0]
	for _, node := range nodes {
		if eligible, known := m.nodeEligible(ctx, ps, node); known && eligible {
			out = append(out, node)
		}
	}
	return out
}

// planEviction picks the smallest set of idle VMs whose combined memory
// closes the gap between what is available on a node and what the clone
// needs. Returns nil when the node's idle VMs cannot close it — evicting
// a partial set would destroy useful capacity and still not admit the
// job.
func planEviction(candidates []evictionCandidate, need, available uint64) ([]evictionCandidate, uint64) {
	if need <= available {
		return nil, 0 // nothing to do; the clone should have fit
	}
	gap := need - available

	// Warm before Hot (no boot invested, and Hot serves job-start
	// latency), then oldest first within a kind — the same preference
	// order shrinkHotPool / shrinkWarmPool already apply, and the oldest
	// VM is closest to being recycled by vm_max_age anyway.
	ordered := make([]evictionCandidate, len(candidates))
	copy(ordered, candidates)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].preferred != ordered[j].preferred {
			return ordered[i].preferred
		}
		return ordered[i].row.CreatedAt.Before(ordered[j].row.CreatedAt)
	})

	var picked []evictionCandidate
	var freed uint64
	for _, c := range ordered {
		picked = append(picked, c)
		freed += c.memBytes
		if freed >= gap {
			return picked, freed
		}
	}
	return nil, 0
}

// startEviction commits an eviction plan: CAS each victim out of its
// idle state, reserve the capacity they release for ps, and queue their
// destroys.
//
// The CAS is the commit point. A victim that lost its CAS was acquired
// (or destroyed) by someone else in the meantime, so the plan no longer
// holds — the whole attempt is abandoned rather than half-applied, and
// the next tick re-plans against fresh state. Abandoning is safe: any
// victims already CAS'd in this pass are still destroyed, which only
// frees more capacity than intended for one tick.
func (m *manager) startEviction(ctx context.Context, ps *profileState, node string, shape nodecap.Shape, victims []evictionCandidate, freed uint64) bool {
	committed := make([]evictionCandidate, 0, len(victims))
	for _, v := range victims {
		from := store.StateHot
		if v.preferred {
			from = store.StateWarm
		}
		ok, err := m.store.UpdateState(v.row.VMID, from, store.StateDraining, func(row *store.VM) {
			row.StateSince = time.Now()
		})
		if err != nil || !ok {
			m.log.Debug("evict: victim changed state under us; abandoning plan",
				"vmid", v.row.VMID, "from", from, "err", err)
			break
		}
		committed = append(committed, v)
	}
	if len(committed) == 0 {
		return false
	}

	vmids := make([]int, 0, len(committed))
	for _, v := range committed {
		vmids = append(vmids, v.row.VMID)
	}

	// Reserve against the capacity the victims are about to release.
	// ReserveFreeing credits their allocation even though the cluster
	// snapshot will keep reporting them until the destroys land — and
	// holding the reservation is what stops the victims' own profile
	// from re-cloning into the hole on its next tick.
	res, ok, err := m.cfg.Capacity.ReserveFreeing(ctx, node, shape, vmids)
	if err != nil || !ok {
		// The victims are already committed to destruction, so the
		// capacity still frees up — we just lose the exclusive claim on
		// it. Log and let the ordinary admission path race for it.
		m.log.Warn("evict: could not reserve the freed capacity; destroying victims anyway",
			"profile", ps.settings.Name, "node", node, "err", err)
	} else {
		ps.parkEvicted(node, res)
	}

	for _, v := range committed {
		m.log.Info("evict: destroying idle vm to admit queued demand",
			"vmid", v.row.VMID, "victim_profile", v.row.Profile, "victim_state", v.row.State,
			"for_profile", ps.settings.Name, "node", node,
			"freed_bytes", freed, "needed_bytes", shape.MemoryBytes)
		if m.metrics != nil {
			m.metrics.CapacityEvictions.WithLabelValues(
				m.cfg.ScaleSetName, metricsProfile(ps.settings.Name), metricsProfile(v.row.Profile)).Inc()
		}
		m.destroyAsync(v.row.VMID, v.row.Node, v.row.Profile)
	}
	return true
}

// idleCandidatesByNode groups this scale set's idle Hot/Warm rows by the
// node they occupy, sized by their profile's declared memory.
//
// Only Hot and Warm qualify. Everything else is either serving a job
// (Assigned/Running), already on its way out (Draining/Destroying), or
// mid-transition (Provisioning/Booting/Recycling) where destroying it
// would race the worker that owns it.
//
// Rows whose profile this manager doesn't know are skipped: without a
// declared footprint there is no honest number for how much destroying
// them would free.
func (m *manager) idleCandidatesByNode() (map[string][]evictionCandidate, error) {
	rows, err := m.store.ListByState(store.StateHot, store.StateWarm)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]evictionCandidate)
	for _, r := range rows {
		ps := m.profiles[r.Profile]
		if ps == nil || ps.settings.MemoryMB <= 0 {
			continue
		}
		out[r.Node] = append(out[r.Node], evictionCandidate{
			row:       r,
			memBytes:  shapeOf(ps.settings).MemoryBytes,
			preferred: r.State == store.StateWarm,
		})
	}
	return out, nil
}
