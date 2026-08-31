//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakeproxmox"
)

// Idle eviction (pool.capacity.evict_idle_for_demand) is NOT covered
// here. Driving it end-to-end needs a queued-demand signal to arrive
// after the pool has already filled the node, and this harness's demand
// model fires on the listener's initial handshake — so any such
// scenario would turn on which profile's reconcile loop won a race.
// Eviction is instead covered deterministically by the unit tests in
// internal/pool (planEviction, evictForDemand, the parked-reservation
// handoff).

const (
	gib = 1024 * 1024 * 1024
	// vmidLo / vmidHi bracket the orchestrator's clone range in the e2e
	// config, so a snapshot scan can tell our VMs from the template and
	// from any seeded foreign guests.
	vmidLo = 10000
	vmidHi = 11000
)

// homelabProfiles is the shape from the feature's motivating case:
// three sizes whose static maxima would reserve a 28 GiB worst case
// regardless of what is actually running.
func homelabProfiles() []ProfileSpec {
	return []ProfileSpec{
		{Name: "mem-4g", CPUCores: 2, MemoryMB: 4096, WarmSize: 1},
		{Name: "mem-8g", CPUCores: 4, MemoryMB: 8192, WarmSize: 1},
		{Name: "mem-16g", CPUCores: 8, MemoryMB: 16384, WarmSize: 1},
	}
}

// ownedAllocationMB returns how much memory the orchestrator's own VMs
// hold on the node, read back out of the fake's qemu config — the same
// place the capacity accountant reads it. Asserting against what Proxmox
// believes, rather than what the orchestrator intended, is what makes
// this a real overcommit check.
func ownedAllocationMB(t testing.TB, h *Harness) (count, allocatedMB int) {
	t.Helper()
	for _, vm := range h.Proxmox.Snapshot() {
		if vm.VMID < vmidLo || vm.VMID >= vmidHi {
			continue
		}
		count++
		mb := fakeproxmox.DefaultGuestMemoryMB
		switch n := vm.Config["memory"].(type) {
		case int:
			mb = n
		case float64:
			mb = int(n)
		}
		allocatedMB += mb
	}
	return count, allocatedMB
}

// TestE2E_CapacityAdmission_RefusesToOvercommit is the core promise. The
// three warm pools want 28 GiB on a node that can admit 16 GiB. Without
// capacity admission the orchestrator would clone all three — each
// profile is individually well under its static cap — and oversubscribe
// the node by 12 GiB.
//
// The assertion is deliberately two-sided: the node must never be
// overcommitted, AND it must not be left mostly empty. A gate that
// simply refused everything would satisfy the first half alone.
func TestE2E_CapacityAdmission_RefusesToOvercommit(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
	// 20 GiB node, 4 GiB withheld for the host => 16 GiB admissible.
	fp.SetNodeCapacity("pve1", 20*gib, 16)

	h := Start(t, Options{
		FakeProxmox:          fp,
		HotSize:              0,
		MaxConcurrentRunners: 8,
		Profiles:             homelabProfiles(),
		Capacity:             &CapacitySpec{ReserveMemoryMB: 4096},
	})

	// Whichever profiles win the race, the stable outcomes on a 16 GiB
	// budget are {16g} or {4g + 8g} — so 12 GiB is the floor for "the
	// node actually got used".
	require.Eventually(t, func() bool {
		_, mb := ownedAllocationMB(t, h)
		return mb >= 12*1024
	}, 20*time.Second, 200*time.Millisecond,
		"the pool should have filled the node with whatever mix fits")

	// Hold still: a mis-accounted clone would have had many reconcile
	// ticks (100ms each) to slip through by now.
	time.Sleep(2 * time.Second)

	count, allocatedMB := ownedAllocationMB(t, h)
	require.LessOrEqual(t, allocatedMB, 16*1024,
		"allocated memory (%d MiB across %d VMs) must never exceed the node's admissible 16 GiB",
		allocatedMB, count)

	// The refusals are observable rather than silent — this is the
	// signal an operator alerts on when the fleet wants more RAM than
	// the nodes have.
	require.Positive(t,
		h.MetricValue(t, "scaleset_clone_deferred_capacity_total"),
		"a clone that could not be admitted must be counted, so the wait is visible")

	// The ledger is exported and self-consistent.
	total := h.MetricValue(t, "scaleset_node_memory_total_bytes", formatLabel("node", "pve1"))
	committed := h.MetricValue(t, "scaleset_node_memory_committed_bytes", formatLabel("node", "pve1"))
	available := h.MetricValue(t, "scaleset_node_memory_available_bytes", formatLabel("node", "pve1"))
	require.Equal(t, float64(20*gib), total)
	require.LessOrEqual(t, committed+available, total,
		"committed + available must stay within the node, net of the host reserve")
}

// TestE2E_CapacityAdmission_CountsForeignVMs: the node hosts guests this
// orchestrator does not own, and their allocation is just as real as
// ours. Planning against only our own store would oversubscribe the node
// the moment somebody else put a VM on it.
func TestE2E_CapacityAdmission_CountsForeignVMs(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
	fp.SetNodeCapacity("pve1", 32*gib, 16)
	// Somebody else's 24 GiB VM: outside our VMID range, untagged, and
	// running. 32 - 4 (reserve) - 24 = 4 GiB left, room for exactly one
	// mem-4g and nothing else.
	fp.SeedVM("pve1", 500, "someone-elses-database", true, nil)
	require.NoError(t, fp.SetVMConfig(500, "memory", 24*1024))

	h := Start(t, Options{
		FakeProxmox:          fp,
		HotSize:              0,
		MaxConcurrentRunners: 8,
		Profiles:             homelabProfiles(),
		Capacity:             &CapacitySpec{ReserveMemoryMB: 4096},
	})

	require.Eventually(t, func() bool {
		_, mb := ownedAllocationMB(t, h)
		return mb >= 4*1024
	}, 20*time.Second, 200*time.Millisecond,
		"the 4 GiB of remaining headroom should still be used")
	time.Sleep(2 * time.Second)

	count, allocatedMB := ownedAllocationMB(t, h)
	require.LessOrEqual(t, allocatedMB, 4*1024,
		"must plan around the foreign VM: %d MiB across %d owned VMs exceeds the 4 GiB left over",
		allocatedMB, count)

	require.GreaterOrEqual(t,
		h.MetricValue(t, "scaleset_node_memory_committed_bytes", formatLabel("node", "pve1")),
		float64(24*gib),
		"the foreign VM's allocation must appear in the committed gauge")
}

// TestE2E_CapacityAdmission_DisabledKeepsLegacyBehaviour is the
// back-compat guard: with the feature off, node size is irrelevant and
// the orchestrator provisions to its static counts exactly as it always
// has.
func TestE2E_CapacityAdmission_DisabledKeepsLegacyBehaviour(t *testing.T) {
	t.Parallel()
	fp := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 5 * time.Millisecond})
	fp.SetNodeCapacity("pve1", 1*gib, 1) // absurdly small, and ignored

	h := Start(t, Options{
		FakeProxmox:          fp,
		HotSize:              2,
		MaxConcurrentRunners: 4,
		// No Capacity block at all.
	})

	require.Eventually(t, func() bool {
		return h.MetricValue(t, "scaleset_pool_size", formatLabel("state", "hot")) >= 2
	}, 20*time.Second, 200*time.Millisecond,
		"with capacity admission off, node size must not gate provisioning")

	require.Zero(t, h.MetricValue(t, "scaleset_clone_deferred_capacity_total"),
		"nothing may be deferred when the feature is disabled")
	require.Zero(t, h.MetricValue(t, "scaleset_node_memory_total_bytes", formatLabel("node", "pve1")),
		"the node gauges stay unpublished when no accountant is wired")
}
