package fakeproxmox

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// vmRecord is the in-memory representation of a single Proxmox VM. The
// fake stores VMs by (node, vmid). Tags are kept as a semicolon-joined
// string to match go-proxmox's wire format.
type vmRecord struct {
	VMID      int
	Node      string
	Name      string
	Tags      string // semicolon-joined, sorted, deduped
	Template  bool
	Running   bool
	StartedAt time.Time // wall-clock start so guest-agent delay can be enforced
	Config    map[string]any

	// Snapshots holds the VM's snapshot names in creation order.
	// SnapshotCreates / Rollbacks count the respective API calls so
	// tests can assert "snapshotted exactly once, rolled back twice"
	// without scraping the task table.
	Snapshots       []string
	SnapshotCreates int
	Rollbacks       int

	// FirewallRules / FirewallOptions record what the orchestrator
	// applied via POST .../firewall/rules and PUT .../firewall/options.
	// Mirrors real PVE: this state is per-VM and (like the real
	// /etc/pve/firewall/<vmid>.fw file) is NOT copied on clone.
	FirewallRules   []FirewallRuleRecord
	FirewallOptions map[string]any

	// FirewallActiveAtFirstStart captures whether the VM firewall was
	// enabled (options enable=1 with at least one rule) at the moment
	// of the VM's FIRST qmstart. Tests assert on it to pin the
	// "sandbox before first boot" ordering contract. Meaningless until
	// EverStarted is true.
	EverStarted                bool
	FirewallActiveAtFirstStart bool
}

// FirewallRuleRecord is one recorded firewall rule in wire-field form.
type FirewallRuleRecord struct {
	Type   string
	Action string
	Enable int
}

// taskRecord backs the asynchronous task model. Real Proxmox returns an
// UPID from any "do work" POST and the caller polls
// /nodes/{node}/tasks/{upid}/status until status=="stopped". We model the
// same lifecycle with a simple time-based transition.
type taskRecord struct {
	UPID      string
	Node      string
	Type      string // "qmclone", "qmstart", "qmstop", "qmshutdown", "qmdestroy"
	ID        string // typically the affected vmid
	StartedAt time.Time
	Duration  time.Duration // status becomes "stopped" after this elapses
}

// store is the fake's mutable state behind a single mutex. The fake is
// intended for single-process tests; a single lock keeps the
// implementation honest about race conditions in the orchestrator.
type store struct {
	mu        sync.Mutex
	vms       map[int]*vmRecord // keyed by vmid
	tasks     map[string]*taskRecord
	nextTask  uint64
	taskDur   time.Duration
	agentWait time.Duration
	faults    []Fault
	// securityGroups is the set of datacenter firewall security group
	// names the fake claims to host, served at GET
	// /cluster/firewall/groups. Seed via Server.SeedSecurityGroup.
	securityGroups []string
}

// snapshot returns a deep copy of the VM set suitable for assertion.
// Caller does not need to hold the lock — snapshot acquires it.
func (s *store) snapshot() []VMSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]VMSnapshot, 0, len(s.vms))
	for _, v := range s.vms {
		var fwOpts map[string]any
		if v.FirewallOptions != nil {
			fwOpts = make(map[string]any, len(v.FirewallOptions))
			for k, val := range v.FirewallOptions {
				fwOpts[k] = val
			}
		}
		var cfgCopy map[string]any
		if len(v.Config) > 0 {
			cfgCopy = make(map[string]any, len(v.Config))
			for k, val := range v.Config {
				cfgCopy[k] = val
			}
		}
		out = append(out, VMSnapshot{
			VMID:                       v.VMID,
			Node:                       v.Node,
			Name:                       v.Name,
			Tags:                       v.Tags,
			Running:                    v.Running,
			Snapshots:                  append([]string(nil), v.Snapshots...),
			SnapshotCreates:            v.SnapshotCreates,
			Rollbacks:                  v.Rollbacks,
			Config:                     cfgCopy,
			FirewallRules:              append([]FirewallRuleRecord(nil), v.FirewallRules...),
			FirewallOptions:            fwOpts,
			EverStarted:                v.EverStarted,
			FirewallActiveAtFirstStart: v.FirewallActiveAtFirstStart,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].VMID < out[j].VMID })
	return out
}

// VMSnapshot is the externally observable view of one VM. Tags are kept
// in semicolon-joined wire form so assertion code matches the
// orchestrator's own format. Snapshots / SnapshotCreates / Rollbacks
// expose the VM's Proxmox-snapshot bookkeeping for recycle-mode tests.
type VMSnapshot struct {
	VMID    int
	Node    string
	Name    string
	Tags    string
	Running bool

	Snapshots       []string
	SnapshotCreates int
	Rollbacks       int

	// Config is a copy of the VM's qemu config keys as set via the
	// config endpoint (or inherited from the template on clone). Nil
	// when no keys are set. Tests assert on net<N> strings here.
	Config map[string]any

	// Firewall state as applied via the per-VM firewall endpoints.
	// FirewallActiveAtFirstStart is only meaningful when EverStarted.
	FirewallRules              []FirewallRuleRecord
	FirewallOptions            map[string]any
	EverStarted                bool
	FirewallActiveAtFirstStart bool
}

// seedVM inserts a VM directly into the store, bypassing the API. Tests
// use this to set up the template VM before constructing the orchestrator.
func (s *store) seedVM(node string, vmid int, name string, template, running bool, tags []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vms[vmid] = &vmRecord{
		VMID:     vmid,
		Node:     node,
		Name:     name,
		Tags:     joinTags(tags),
		Template: template,
		Running:  running,
		Config:   map[string]any{},
	}
}

// findVM returns (record, ok). The caller must hold s.mu.
func (s *store) findVMLocked(vmid int) (*vmRecord, bool) {
	v, ok := s.vms[vmid]
	return v, ok
}

// cloneVM creates a new VM by copying the template, returning a task
// record the caller can poll. Caller must hold s.mu.
func (s *store) cloneVMLocked(templateVMID, newVMID int, targetNode, name string) (*vmRecord, *taskRecord, error) {
	tpl, ok := s.vms[templateVMID]
	if !ok {
		return nil, nil, fmt.Errorf("template vmid %d does not exist", templateVMID)
	}
	if _, exists := s.vms[newVMID]; exists {
		return nil, nil, fmt.Errorf("vmid %d already exists", newVMID)
	}
	if name == "" {
		name = fmt.Sprintf("clone-of-%d", templateVMID)
	}
	// Mirror real PVE: qm clone copies the template's hardware config
	// (net<N>, disks, ...) into the new VM's .conf. Per-VM firewall
	// state (FirewallRules / FirewallOptions) is intentionally NOT
	// copied — it lives in /etc/pve/firewall/<vmid>.fw, which real
	// Proxmox does not clone either.
	cfgCopy := make(map[string]any, len(tpl.Config))
	for k, val := range tpl.Config {
		cfgCopy[k] = val
	}
	v := &vmRecord{
		VMID:    newVMID,
		Node:    targetNode,
		Name:    name,
		Tags:    "", // tags are applied by a subsequent PUT /config
		Running: false,
		Config:  cfgCopy,
	}
	s.vms[newVMID] = v
	return v, s.newTaskLocked(targetNode, "qmclone", fmt.Sprintf("%d", newVMID)), nil
}

// newTaskLocked allocates a task record with a monotonic UPID and stamps
// the configured duration. Caller must hold s.mu.
func (s *store) newTaskLocked(node, kind, id string) *taskRecord {
	s.nextTask++
	upid := fmt.Sprintf("UPID:%s:%08X:00000001:5F3D0000:%s:%s:scaleset@pve!automation:",
		node, s.nextTask, kind, id)
	t := &taskRecord{
		UPID:      upid,
		Node:      node,
		Type:      kind,
		ID:        id,
		StartedAt: time.Now(),
		Duration:  s.taskDur,
	}
	s.tasks[upid] = t
	return t
}

// taskCompleted reports whether the task's duration has elapsed. Caller
// must hold s.mu.
func (s *store) taskCompletedLocked(t *taskRecord) bool {
	return time.Since(t.StartedAt) >= t.Duration
}

// joinTags normalises a tag slice into semicolon-joined wire form: sorted
// alphabetically, deduped, whitespace-trimmed.
func joinTags(tags []string) string {
	if len(tags) == 0 {
		return ""
	}
	seen := make(map[string]struct{}, len(tags))
	out := make([]string, 0, len(tags))
	for _, t := range tags {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	sort.Strings(out)
	return strings.Join(out, ";")
}
