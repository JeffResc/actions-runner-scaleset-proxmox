// Package fakeproxmox is an httptest-backed fake of the Proxmox VE
// API used by github.com/jeffresc/actions-runner-scaleset-proxmox for
// in-process end-to-end testing. It implements the subset of the API
// the orchestrator actually depends on:
//
//   - cluster discovery: /version, /nodes, /nodes/{node}/status
//   - VM lifecycle: /nodes/{node}/qemu, .../clone, .../config,
//     .../status/{start,stop,shutdown,current}, DELETE
//   - task model: /nodes/{node}/tasks/{upid}/status
//   - qemu-guest-agent: get-osinfo, file-write, file-read, exec,
//     exec-status
//
// The fake is intentionally minimal but faithful: real-Proxmox response
// shapes, the {"data": ...} envelope, semicolon-joined tags, UPID task
// strings, and the time-based "running" -> "stopped" task transition
// that go-proxmox's WaitFor polls against. Failure-injection knobs
// live in inject.go; agent endpoints in agent.go.
package fakeproxmox

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

// Options configures a new fake Proxmox server. Zero-value defaults
// keep tests terse: a single node "pve1", template VMID 9000 on that
// node, no guest-agent delay, 5ms task duration.
type Options struct {
	// Nodes is the list of cluster nodes the fake claims to host. Empty
	// defaults to []string{"pve1"}.
	Nodes []string

	// TemplateVMID is the VMID seeded as a template VM on TemplateNode.
	// Zero defaults to 9000. Set to a negative value to skip seeding
	// (test will need to call SeedVM itself).
	TemplateVMID int

	// TemplateNode is the node hosting the template VM. Empty defaults
	// to Nodes[0].
	TemplateNode string

	// GuestAgentDelay is how long get-osinfo returns "agent not ready"
	// after a VM is started. Zero means immediately ready.
	GuestAgentDelay time.Duration

	// TaskDuration controls how long a Proxmox task (clone, start,
	// stop, destroy) stays in "running" state before transitioning to
	// "stopped". Zero defaults to 5ms — tight enough that tests run
	// quickly, loose enough that a polling client sees the transition
	// rather than missing it.
	TaskDuration time.Duration

	// NodeCapacity sets the physical resources each node reports, keyed
	// by node name. Nodes not listed fall back to DefaultNodeCapacity —
	// deliberately generous, so tests that predate capacity-aware
	// admission never trip over it. Drive it (or Server.SetNodeCapacity)
	// to model a node too small for the pool that wants to live on it.
	NodeCapacity map[string]NodeCapacity
}

// NodeCapacity is the physical resource envelope of one fake node.
type NodeCapacity struct {
	MemoryBytes uint64
	CPUs        int
}

// DefaultNodeCapacity is what a node reports when the test didn't say.
// Large enough that no pre-existing test can exhaust it.
var DefaultNodeCapacity = NodeCapacity{MemoryBytes: 64 * 1024 * 1024 * 1024, CPUs: 32}

// DefaultGuestMemoryMB is the memory a VM reports when its config
// carries no explicit `memory` key — mirroring PVE, where a VM created
// without one gets the qemu-server default.
const DefaultGuestMemoryMB = 512

// Server is the fake Proxmox API. Construct with New; the embedded
// httptest.Server gives Close() and a usable URL.
type Server struct {
	*httptest.Server
	store *store
	opts  Options
}

// New starts a fake Proxmox API server. The caller is responsible for
// closing it via Server.Close(); when t is a *testing.T the cleanup is
// registered via t.Cleanup so tests don't have to.
func New(t testing.TB, opts Options) *Server {
	t.Helper()
	if len(opts.Nodes) == 0 {
		opts.Nodes = []string{"pve1"}
	}
	if opts.TemplateVMID == 0 {
		opts.TemplateVMID = 9000
	}
	if opts.TemplateNode == "" {
		opts.TemplateNode = opts.Nodes[0]
	}
	if opts.TaskDuration == 0 {
		opts.TaskDuration = 5 * time.Millisecond
	}

	st := &store{
		vms:       map[int]*vmRecord{},
		tasks:     map[string]*taskRecord{},
		taskDur:   opts.TaskDuration,
		agentWait: opts.GuestAgentDelay,
	}

	s := &Server{store: st, opts: opts}
	// TLS-backed: the production config now refuses non-https Proxmox
	// endpoints (the API token traverses every request as a header).
	// Callers point their orchestrator config at this URL with
	// insecure_skip_verify: true so the self-signed test cert is
	// accepted.
	s.Server = httptest.NewTLSServer(s.routes())

	// Seed the template VM unless the caller asked for no template
	// (TemplateVMID < 0). The template is marked with template:1 so
	// the orchestrator's discoverTemplateNode finds it on the
	// configured node.
	if opts.TemplateVMID > 0 {
		st.seedVM(opts.TemplateNode, opts.TemplateVMID, "ubuntu-runner-template", true, false, nil)
	}

	t.Cleanup(s.Close)
	return s
}

// SeedVM inserts a VM directly into the fake's state, bypassing the API.
// Tests use this to set up pre-existing VMs that the orchestrator should
// discover at startup (template, leaked clones from a previous run,
// etc.).
func (s *Server) SeedVM(node string, vmid int, name string, running bool, tags []string) {
	s.store.seedVM(node, vmid, name, false, running, tags)
}

// SetNodeCapacity overrides a node's physical resource envelope,
// bypassing Options. Tests use it to shrink a node mid-scenario so the
// capacity accountant starts refusing clones.
func (s *Server) SetNodeCapacity(node string, memoryBytes uint64, cpus int) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if s.opts.NodeCapacity == nil {
		s.opts.NodeCapacity = map[string]NodeCapacity{}
	}
	s.opts.NodeCapacity[node] = NodeCapacity{MemoryBytes: memoryBytes, CPUs: cpus}
}

// SeedSecurityGroup registers a datacenter firewall security group so
// GET /cluster/firewall/groups reports it. Tests seed the group the
// orchestrator's proxmox.firewall.security_group names so the startup
// existence check (provisioner.validateFirewallSecurityGroup) passes;
// omitting it models the misconfigured/missing-group case.
func (s *Server) SeedSecurityGroup(name string) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	s.store.securityGroups = append(s.store.securityGroups, name)
}

// Snapshot returns a stable, sorted view of all VMs currently in the
// fake's state. Tests use it to assert on the orchestrator's effects.
func (s *Server) Snapshot() []VMSnapshot { return s.store.snapshot() }

// SetVMConfig sets one qemu config key on an existing VM, bypassing
// the API. Tests use it to give the seeded template a hardware config
// (e.g. a net0 string) that clones then inherit — mirroring real PVE,
// where qm clone copies the template's config keys into the new .conf.
//
// Returns an error when the VMID is unknown so a typo in a test
// surfaces immediately rather than silently no-op'ing.
func (s *Server) SetVMConfig(vmid int, key string, value any) error {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	v, ok := s.store.findVMLocked(vmid)
	if !ok {
		return fmt.Errorf("fakeproxmox: SetVMConfig: vmid %d not found", vmid)
	}
	v.Config[key] = value
	return nil
}

// PowerOff flips a VM's Running flag to false, bypassing the qm stop
// HTTP path. Used by e2e scenarios that want to model "the in-VM
// runner finished and powered itself off" without faking a complete
// task lifecycle on the API side. The orchestrator's power-state
// poller then sees the stopped VM on its next tick and calls
// MarkCompleted.
//
// Returns an error when the VMID is unknown so a typo in a test
// surfaces immediately rather than silently no-op'ing.
func (s *Server) PowerOff(vmid int) error {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	v, ok := s.store.findVMLocked(vmid)
	if !ok {
		return fmt.Errorf("fakeproxmox: PowerOff: vmid %d not found", vmid)
	}
	v.Running = false
	return nil
}

// routes builds the chi mux. We strip an optional /api2/json prefix in
// middleware so both Endpoint = srv.URL and Endpoint = srv.URL +
// "/api2/json" work — the former is what unit-style tests do, the
// latter matches production config.
func (s *Server) routes() http.Handler {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			req.URL.Path = strings.TrimPrefix(req.URL.Path, "/api2/json")
			if req.URL.Path == "" {
				req.URL.Path = "/"
			}
			next.ServeHTTP(w, req)
		})
	})

	r.Get("/version", s.handleVersion)
	r.Get("/cluster/firewall/groups", s.handleClusterFWGroups)
	r.Get("/cluster/resources", s.handleClusterResources)
	r.Get("/nodes", s.handleListNodes)
	r.Get("/nodes/{node}/status", s.handleNodeStatus)
	r.Get("/nodes/{node}/qemu", s.handleListVMs)
	r.Get("/nodes/{node}/qemu/{vmid}/status/current", s.handleVMStatus)
	r.Get("/nodes/{node}/qemu/{vmid}/config", s.handleVMConfig)
	r.Post("/nodes/{node}/qemu/{vmid}/clone", s.handleClone)
	// VM config supports both POST (async, returns UPID) and PUT (sync,
	// returns null). go-proxmox's VM.Config uses POST; older clients
	// and our own existing unit tests use PUT. Same handler, response
	// shape varies by method.
	r.Post("/nodes/{node}/qemu/{vmid}/config", s.handleSetConfig)
	r.Put("/nodes/{node}/qemu/{vmid}/config", s.handleSetConfig)
	r.Post("/nodes/{node}/qemu/{vmid}/status/start", s.handleStart)
	r.Post("/nodes/{node}/qemu/{vmid}/status/stop", s.handleStop)
	r.Post("/nodes/{node}/qemu/{vmid}/status/shutdown", s.handleShutdown)
	r.Post("/nodes/{node}/qemu/{vmid}/snapshot", s.handleSnapshotCreate)
	r.Get("/nodes/{node}/qemu/{vmid}/snapshot", s.handleSnapshotList)
	r.Post("/nodes/{node}/qemu/{vmid}/snapshot/{snapname}/rollback", s.handleSnapshotRollback)
	// Per-VM firewall endpoints. The mutating pair is synchronous on
	// real PVE (no UPID task) — the response is a bare {"data": null}.
	// The GET pair backs the provisioner's ensureFirewall pre-start
	// verification.
	r.Post("/nodes/{node}/qemu/{vmid}/firewall/rules", s.handleFirewallRuleCreate)
	r.Get("/nodes/{node}/qemu/{vmid}/firewall/rules", s.handleFirewallRulesList)
	r.Put("/nodes/{node}/qemu/{vmid}/firewall/options", s.handleFirewallOptionsSet)
	r.Get("/nodes/{node}/qemu/{vmid}/firewall/options", s.handleFirewallOptionsGet)
	r.Delete("/nodes/{node}/qemu/{vmid}", s.handleDestroy)

	r.Get("/nodes/{node}/tasks/{upid}/status", s.handleTaskStatus)

	r.Get("/nodes/{node}/qemu/{vmid}/agent/get-osinfo", s.handleAgentGetOSInfo)
	r.Post("/nodes/{node}/qemu/{vmid}/agent/file-write", s.handleAgentFileWrite)
	r.Get("/nodes/{node}/qemu/{vmid}/agent/file-read", s.handleAgentFileRead)
	r.Post("/nodes/{node}/qemu/{vmid}/agent/exec", s.handleAgentExec)
	r.Get("/nodes/{node}/qemu/{vmid}/agent/exec-status", s.handleAgentExecStatus)

	return r
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

// writeData wraps payload in Proxmox's standard `{"data": ...}`
// envelope and writes it as JSON.
func writeData(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"data": payload})
}

// writeError replies with the given HTTP status and body. Proxmox 5xx
// responses use a plain-text body; the orchestrator's error
// classification matches on substrings (see provisioner.go). Body
// strings are always constructed by handlers in this package — never
// reflected from request input — so the gosec G705 XSS rule does not
// apply here.
func writeError(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	// #nosec G705 -- body is hard-coded or constructed from typed
	// path parameters (node names, vmids); never echoed from
	// untrusted request input. The Content-Type is text/plain so
	// browsers won't execute it even if it were tainted.
	_, _ = io.WriteString(w, body)
}

// vmidParam parses {vmid} from the URL.
func vmidParam(r *http.Request) (int, error) {
	raw := chi.URLParam(r, "vmid")
	return strconv.Atoi(raw)
}

// ---------------------------------------------------------------------------
// Cluster + node handlers
// ---------------------------------------------------------------------------

func (s *Server) handleVersion(w http.ResponseWriter, _ *http.Request) {
	writeData(w, map[string]any{
		"version": "9.0.0",
		"release": "9.0",
		"repoid":  "fake",
	})
}

// handleClusterFWGroups models GET /cluster/firewall/groups. go-proxmox
// decodes the data array into []*FirewallSecurityGroup, keyed on the
// "group" field. Backs the provisioner's startup security-group
// existence check.
func (s *Server) handleClusterFWGroups(w http.ResponseWriter, _ *http.Request) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	out := make([]map[string]any, 0, len(s.store.securityGroups))
	for _, name := range s.store.securityGroups {
		out = append(out, map[string]any{"group": name})
	}
	writeData(w, out)
}

func (s *Server) handleListNodes(w http.ResponseWriter, _ *http.Request) {
	out := make([]map[string]any, 0, len(s.opts.Nodes))
	for _, n := range s.opts.Nodes {
		cap := s.nodeCapacity(n)
		out = append(out, map[string]any{
			"node":   n,
			"type":   "node",
			"status": "online",
			"maxmem": cap.MemoryBytes,
			"maxcpu": cap.CPUs,
		})
	}
	writeData(w, out)
}

// handleClusterResources models GET /cluster/resources — the single
// call the capacity accountant uses. Real PVE returns node rows and
// guest rows in one undifferentiated list, with each guest's maxmem and
// maxcpu taken from its CONFIG (so they are populated for stopped
// guests too) and template guests flagged template=1.
func (s *Server) handleClusterResources(w http.ResponseWriter, _ *http.Request) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()

	out := make([]map[string]any, 0, len(s.opts.Nodes)+len(s.store.vms))
	for _, n := range s.opts.Nodes {
		cap := s.nodeCapacityLocked(n)
		out = append(out, map[string]any{
			"id":     "node/" + n,
			"type":   "node",
			"node":   n,
			"status": "online",
			"maxmem": cap.MemoryBytes,
			"maxcpu": cap.CPUs,
		})
	}
	for _, v := range s.store.vms {
		row := map[string]any{
			"id":     fmt.Sprintf("qemu/%d", v.VMID),
			"type":   "qemu",
			"node":   v.Node,
			"vmid":   v.VMID,
			"name":   v.Name,
			"status": "stopped",
			"maxmem": guestMemoryBytes(v),
			"maxcpu": guestVCPUs(v),
			// Real PVE reports tags here, and the capacity accountant
			// uses them to tell its own guests from foreign ones.
			"tags": v.Tags,
		}
		if v.Running {
			row["status"] = "running"
		}
		if v.Template {
			row["template"] = 1
		}
		out = append(out, row)
	}
	writeData(w, out)
}

// nodeCapacity resolves a node's physical envelope, taking the lock.
func (s *Server) nodeCapacity(node string) NodeCapacity {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	return s.nodeCapacityLocked(node)
}

// nodeCapacityLocked resolves a node's physical envelope. Caller must
// hold s.store.mu (SetNodeCapacity writes s.opts.NodeCapacity under it).
func (s *Server) nodeCapacityLocked(node string) NodeCapacity {
	if c, ok := s.opts.NodeCapacity[node]; ok {
		return c
	}
	return DefaultNodeCapacity
}

func (s *Server) handleNodeStatus(w http.ResponseWriter, r *http.Request) {
	node := chi.URLParam(r, "node")
	if !s.knownNode(node) {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("node '%s' does not exist", node))
		return
	}
	writeData(w, map[string]any{
		"uptime": 12345,
	})
}

func (s *Server) knownNode(name string) bool {
	for _, n := range s.opts.Nodes {
		if n == name {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// VM handlers
// ---------------------------------------------------------------------------

func (s *Server) handleListVMs(w http.ResponseWriter, r *http.Request) {
	node := chi.URLParam(r, "node")
	s.store.mu.Lock()
	defer s.store.mu.Unlock()

	out := make([]map[string]any, 0)
	for _, v := range s.store.vms {
		if v.Node != node {
			continue
		}
		out = append(out, vmJSON(v))
	}
	writeData(w, out)
}

// handleVMConfig returns the VM's qemu config. go-proxmox's
// node.VirtualMachine() makes two GETs — status/current + config — and
// fails the whole call if either errors. We return a minimal payload
// echoing back tags and any config knobs set via PUT.
func (s *Server) handleVMConfig(w http.ResponseWriter, r *http.Request) {
	vmid, err := vmidParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad vmid")
		return
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	v, ok := s.store.findVMLocked(vmid)
	if !ok {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("Configuration file 'nodes/%s/qemu-server/%d.conf' does not exist",
				chi.URLParam(r, "node"), vmid))
		return
	}
	out := map[string]any{
		"name": v.Name,
		"tags": v.Tags,
	}
	for k, val := range v.Config {
		out[k] = val
	}
	if v.Template {
		out["template"] = 1
	}
	writeData(w, out)
}

func (s *Server) handleVMStatus(w http.ResponseWriter, r *http.Request) {
	vmid, err := vmidParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad vmid")
		return
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	v, ok := s.store.findVMLocked(vmid)
	if !ok {
		// Match the "Configuration file ... does not exist" shape so
		// the orchestrator's ErrVMNotFound classifier kicks in.
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("Configuration file 'nodes/%s/qemu-server/%d.conf' does not exist",
				chi.URLParam(r, "node"), vmid))
		return
	}
	writeData(w, vmJSON(v))
}

func (s *Server) handleClone(w http.ResponseWriter, r *http.Request) {
	templateVMID, err := vmidParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad vmid")
		return
	}
	var body struct {
		NewID  int    `json:"newid"`
		Name   string `json:"name"`
		Target string `json:"target"`
		Full   any    `json:"full"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "bad clone body: "+err.Error())
		return
	}
	if body.NewID == 0 {
		writeError(w, http.StatusBadRequest, "newid is required")
		return
	}
	target := body.Target
	if target == "" {
		// Linked clone — stays on the template's node.
		target = chi.URLParam(r, "node")
	}
	s.store.mu.Lock()
	_, task, err := s.store.cloneVMLocked(templateVMID, body.NewID, target, body.Name)
	s.store.mu.Unlock()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeData(w, task.UPID)
}

func (s *Server) handleSetConfig(w http.ResponseWriter, r *http.Request) {
	vmid, err := vmidParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad vmid")
		return
	}
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "bad config body: "+err.Error())
		return
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	v, ok := s.store.findVMLocked(vmid)
	if !ok {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("Configuration file 'nodes/%s/qemu-server/%d.conf' does not exist",
				chi.URLParam(r, "node"), vmid))
		return
	}
	if tagsAny, ok := body["tags"]; ok {
		if tagsStr, ok := tagsAny.(string); ok {
			v.Tags = tagsStr
		}
	}
	for k, val := range body {
		if k == "tags" {
			continue
		}
		v.Config[k] = val
	}
	// Real Proxmox returns a UPID for POST (async) and null for PUT
	// (sync). go-proxmox's VM.Config uses POST and unmarshals data
	// into a UPID string — an empty "data": null breaks it with
	// "unexpected end of JSON input".
	if r.Method == http.MethodPost {
		task := s.store.newTaskLocked(v.Node, "qmconfig", fmt.Sprintf("%d", vmid))
		writeData(w, task.UPID)
		return
	}
	writeData(w, nil)
}

func (s *Server) handleStart(w http.ResponseWriter, r *http.Request) {
	vmid, err := vmidParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad vmid")
		return
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	// FaultStatus500Spam: when a matching fault is registered for
	// this VMID (or VMID=0 wildcard), reply 500 and consume one
	// count. Used by partial-failure e2e scenarios that need the
	// "clone succeeded, start failed" cascade (issue #287).
	if f, ok := s.store.matchFaultLocked(FaultStatus500Spam, vmid); ok && f.Count > 0 {
		s.store.consumeFaultLocked(FaultStatus500Spam, vmid)
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("VM %d start failed (injected: FaultStatus500Spam)", vmid))
		return
	}
	v, ok := s.store.findVMLocked(vmid)
	if !ok {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("Configuration file 'nodes/%s/qemu-server/%d.conf' does not exist",
				chi.URLParam(r, "node"), vmid))
		return
	}
	if v.Running {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("VM %d is already running", vmid))
		return
	}
	v.Running = true
	v.StartedAt = time.Now()
	if !v.EverStarted {
		// Freeze the firewall state observed at the VM's first-ever
		// start: the orchestrator's contract is "sandbox active before
		// first boot", and tests assert on this bit to catch an
		// apply-after-start ordering regression.
		v.EverStarted = true
		enabled := false
		if v.FirewallOptions != nil {
			enabled = jsonNumEquals(v.FirewallOptions["enable"], 1)
		}
		v.FirewallActiveAtFirstStart = enabled && len(v.FirewallRules) > 0
	}
	task := s.store.newTaskLocked(v.Node, "qmstart", fmt.Sprintf("%d", vmid))
	writeData(w, task.UPID)
}

// jsonNumEquals compares a decoded-JSON value (float64 / int / bool
// per encoding/json) against an int, tolerating the 1-vs-true shapes
// clients use for PVE boolean flags.
func jsonNumEquals(v any, want int) bool {
	switch n := v.(type) {
	case float64:
		return int(n) == want
	case int:
		return n == want
	case bool:
		return n == (want != 0)
	default:
		return false
	}
}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	s.stopLike(w, r, "qmstop")
}

func (s *Server) handleShutdown(w http.ResponseWriter, r *http.Request) {
	s.stopLike(w, r, "qmshutdown")
}

func (s *Server) stopLike(w http.ResponseWriter, r *http.Request, kind string) {
	vmid, err := vmidParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad vmid")
		return
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	v, ok := s.store.findVMLocked(vmid)
	if !ok {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("Configuration file 'nodes/%s/qemu-server/%d.conf' does not exist",
				chi.URLParam(r, "node"), vmid))
		return
	}
	v.Running = false
	task := s.store.newTaskLocked(v.Node, kind, fmt.Sprintf("%d", vmid))
	writeData(w, task.UPID)
}

// handleSnapshotCreate models POST /nodes/{node}/qemu/{vmid}/snapshot.
// Records the snapshot name on the VM and returns a UPID task like the
// other mutating routes. Duplicate names are rejected the way real PVE
// does ("snapshot name 'x' already used").
func (s *Server) handleSnapshotCreate(w http.ResponseWriter, r *http.Request) {
	vmid, err := vmidParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad vmid")
		return
	}
	var body struct {
		SnapName string `json:"snapname"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "bad snapshot body: "+err.Error())
		return
	}
	if body.SnapName == "" {
		writeError(w, http.StatusBadRequest, "snapname is required")
		return
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	v, ok := s.store.findVMLocked(vmid)
	if !ok {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("Configuration file 'nodes/%s/qemu-server/%d.conf' does not exist",
				chi.URLParam(r, "node"), vmid))
		return
	}
	for _, name := range v.Snapshots {
		if name == body.SnapName {
			writeError(w, http.StatusInternalServerError,
				fmt.Sprintf("snapshot name '%s' already used", body.SnapName))
			return
		}
	}
	v.Snapshots = append(v.Snapshots, body.SnapName)
	v.SnapshotCreates++
	task := s.store.newTaskLocked(v.Node, "qmsnapshot", fmt.Sprintf("%d", vmid))
	writeData(w, task.UPID)
}

// handleSnapshotList models GET /nodes/{node}/qemu/{vmid}/snapshot.
// Real PVE appends a synthetic "current" entry ("You are here!");
// go-proxmox just unmarshals whatever names come back, so we mirror
// that shape for fidelity.
func (s *Server) handleSnapshotList(w http.ResponseWriter, r *http.Request) {
	vmid, err := vmidParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad vmid")
		return
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	v, ok := s.store.findVMLocked(vmid)
	if !ok {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("Configuration file 'nodes/%s/qemu-server/%d.conf' does not exist",
				chi.URLParam(r, "node"), vmid))
		return
	}
	out := make([]map[string]any, 0, len(v.Snapshots)+1)
	for _, name := range v.Snapshots {
		out = append(out, map[string]any{"name": name})
	}
	out = append(out, map[string]any{"name": "current", "description": "You are here!"})
	writeData(w, out)
}

// handleSnapshotRollback models
// POST /nodes/{node}/qemu/{vmid}/snapshot/{snapname}/rollback. An
// unknown snapshot fails with the "does not exist" body shape the
// orchestrator's error classifier keys on. On success the VM is marked
// stopped — the clean snapshot was taken pre-boot, so rolling back
// lands on the stopped snapshot state.
func (s *Server) handleSnapshotRollback(w http.ResponseWriter, r *http.Request) {
	vmid, err := vmidParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad vmid")
		return
	}
	snapname := chi.URLParam(r, "snapname")
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	v, ok := s.store.findVMLocked(vmid)
	if !ok {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("Configuration file 'nodes/%s/qemu-server/%d.conf' does not exist",
				chi.URLParam(r, "node"), vmid))
		return
	}
	found := false
	for _, name := range v.Snapshots {
		if name == snapname {
			found = true
			break
		}
	}
	if !found {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("snapshot '%s' does not exist", snapname))
		return
	}
	v.Rollbacks++
	v.Running = false
	task := s.store.newTaskLocked(v.Node, "qmrollback", fmt.Sprintf("%d", vmid))
	writeData(w, task.UPID)
}

// handleFirewallRuleCreate models
// POST /nodes/{node}/qemu/{vmid}/firewall/rules. Synchronous on real
// PVE — no task is returned; the rule takes effect immediately. The
// recorded rule fields are exposed via Snapshot for assertions.
func (s *Server) handleFirewallRuleCreate(w http.ResponseWriter, r *http.Request) {
	vmid, err := vmidParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad vmid")
		return
	}
	var body struct {
		Type   string `json:"type"`
		Action string `json:"action"`
		Enable int    `json:"enable"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "bad firewall rule body: "+err.Error())
		return
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if _, ok := s.store.matchFaultLocked(FaultFirewallFail, vmid); ok {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("firewall rule create failed for VM %d (injected: FaultFirewallFail)", vmid))
		return
	}
	v, ok := s.store.findVMLocked(vmid)
	if !ok {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("Configuration file 'nodes/%s/qemu-server/%d.conf' does not exist",
				chi.URLParam(r, "node"), vmid))
		return
	}
	if body.Type == "" || body.Action == "" {
		writeError(w, http.StatusBadRequest, "type and action are required")
		return
	}
	v.FirewallRules = append(v.FirewallRules, FirewallRuleRecord{
		Type:   body.Type,
		Action: body.Action,
		Enable: body.Enable,
	})
	writeData(w, nil)
}

// handleFirewallRulesList models
// GET /nodes/{node}/qemu/{vmid}/firewall/rules. Real PVE returns the
// rule list with positional indexes; go-proxmox decodes it into
// []*FirewallRule. Honors FaultFirewallFail so tests can model a
// firewall API that is broken for reads as well as writes.
func (s *Server) handleFirewallRulesList(w http.ResponseWriter, r *http.Request) {
	vmid, err := vmidParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad vmid")
		return
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if _, ok := s.store.matchFaultLocked(FaultFirewallFail, vmid); ok {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("firewall rules list failed for VM %d (injected: FaultFirewallFail)", vmid))
		return
	}
	v, ok := s.store.findVMLocked(vmid)
	if !ok {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("Configuration file 'nodes/%s/qemu-server/%d.conf' does not exist",
				chi.URLParam(r, "node"), vmid))
		return
	}
	out := make([]map[string]any, 0, len(v.FirewallRules))
	for i, rule := range v.FirewallRules {
		out = append(out, map[string]any{
			"pos":    i,
			"type":   rule.Type,
			"action": rule.Action,
			"enable": rule.Enable,
		})
	}
	writeData(w, out)
}

// handleFirewallOptionsGet models
// GET /nodes/{node}/qemu/{vmid}/firewall/options. A VM that never had
// options set returns an empty object (real PVE returns the defaults;
// enable absent decodes as false, which is the same signal). Honors
// FaultFirewallFail like the other firewall routes.
func (s *Server) handleFirewallOptionsGet(w http.ResponseWriter, r *http.Request) {
	vmid, err := vmidParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad vmid")
		return
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if _, ok := s.store.matchFaultLocked(FaultFirewallFail, vmid); ok {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("firewall options get failed for VM %d (injected: FaultFirewallFail)", vmid))
		return
	}
	v, ok := s.store.findVMLocked(vmid)
	if !ok {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("Configuration file 'nodes/%s/qemu-server/%d.conf' does not exist",
				chi.URLParam(r, "node"), vmid))
		return
	}
	opts := v.FirewallOptions
	if opts == nil {
		opts = map[string]any{}
	}
	writeData(w, opts)
}

// handleFirewallOptionsSet models
// PUT /nodes/{node}/qemu/{vmid}/firewall/options. Synchronous like the
// rules endpoint. The full decoded body is stored so tests can assert
// on enable / dhcp exactly as sent on the wire.
func (s *Server) handleFirewallOptionsSet(w http.ResponseWriter, r *http.Request) {
	vmid, err := vmidParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad vmid")
		return
	}
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "bad firewall options body: "+err.Error())
		return
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if _, ok := s.store.matchFaultLocked(FaultFirewallFail, vmid); ok {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("firewall options set failed for VM %d (injected: FaultFirewallFail)", vmid))
		return
	}
	v, ok := s.store.findVMLocked(vmid)
	if !ok {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("Configuration file 'nodes/%s/qemu-server/%d.conf' does not exist",
				chi.URLParam(r, "node"), vmid))
		return
	}
	if v.FirewallOptions == nil {
		v.FirewallOptions = map[string]any{}
	}
	for k, val := range body {
		v.FirewallOptions[k] = val
	}
	writeData(w, nil)
}

func (s *Server) handleDestroy(w http.ResponseWriter, r *http.Request) {
	vmid, err := vmidParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad vmid")
		return
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if _, ok := s.store.matchFaultLocked(FaultVMNotFoundOnDestroy, vmid); ok {
		// Simulate "operator already deleted the VM out-of-band": the
		// orchestrator's destroy path must classify this as
		// ErrVMNotFound and treat it as idempotent success.
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("Configuration file 'nodes/%s/qemu-server/%d.conf' does not exist",
				chi.URLParam(r, "node"), vmid))
		return
	}
	v, ok := s.store.findVMLocked(vmid)
	if !ok {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("Configuration file 'nodes/%s/qemu-server/%d.conf' does not exist",
				chi.URLParam(r, "node"), vmid))
		return
	}
	delete(s.store.vms, vmid)
	task := s.store.newTaskLocked(v.Node, "qmdestroy", fmt.Sprintf("%d", vmid))
	writeData(w, task.UPID)
}

// ---------------------------------------------------------------------------
// Task handler
// ---------------------------------------------------------------------------

func (s *Server) handleTaskStatus(w http.ResponseWriter, r *http.Request) {
	upid := chi.URLParam(r, "upid")
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	t, ok := s.store.tasks[upid]
	if !ok {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("no such task %q", upid))
		return
	}
	status := "running"
	exit := ""
	if s.store.taskCompletedLocked(t) {
		status = "stopped"
		exit = "OK"
		// FaultTaskFails: the task completes (per its normal duration)
		// but with a failure exit status, modelling real Proxmox tasks
		// that finish with "TASK ERROR: ...". Matched by the affected
		// VMID (taskRecord.ID) and, optionally, the task type. This
		// drives the orchestrator's failure-classification path for
		// failed (not hung) tasks.
		taskVMID, _ := strconv.Atoi(t.ID)
		if f, ok := s.store.matchFaultLocked(FaultTaskFails, taskVMID); ok && (f.TaskType == "" || f.TaskType == t.Type) {
			exit = "TASK ERROR: injected " + t.Type + " failure"
		}
	}
	writeData(w, map[string]any{
		"upid":       t.UPID,
		"type":       t.Type,
		"id":         t.ID,
		"status":     status,
		"exitstatus": exit,
		"starttime":  float64(t.StartedAt.Unix()),
		"node":       t.Node,
		"user":       "scaleset@pve!automation",
	})
}

// ---------------------------------------------------------------------------
// Marshalling helpers
// ---------------------------------------------------------------------------

func vmJSON(v *vmRecord) map[string]any {
	out := map[string]any{
		"vmid":   v.VMID,
		"name":   v.Name,
		"status": "stopped",
		"tags":   v.Tags,
		"maxmem": guestMemoryBytes(v),
		"cpus":   guestVCPUs(v),
	}
	if v.Running {
		out["status"] = "running"
	}
	if v.Template {
		out["template"] = 1
	}
	return out
}

// guestMemoryBytes derives a VM's ALLOCATED memory from its qemu config,
// exactly as PVE does (`get_current_memory($conf->{memory}) * 1MiB`).
// Reading it back from Config rather than storing it separately keeps
// the fake automatically faithful to the orchestrator's own post-clone
// memory override: whatever the provisioner PUTs to /config is what the
// capacity accountant then observes.
func guestMemoryBytes(v *vmRecord) uint64 {
	mb := configInt(v.Config["memory"], DefaultGuestMemoryMB)
	if mb < 0 {
		mb = 0
	}
	return uint64(mb) * 1024 * 1024
}

// guestVCPUs mirrors PVE's `sockets * cores`, both defaulting to 1.
func guestVCPUs(v *vmRecord) int {
	return configInt(v.Config["sockets"], 1) * configInt(v.Config["cores"], 1)
}

// configInt coerces a qemu config value to an int. Values arrive either
// as JSON numbers (float64) from the API path or as native ints from
// Server.SetVMConfig, so both are accepted.
func configInt(raw any, fallback int) int {
	switch n := raw.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case string:
		if parsed, err := strconv.Atoi(n); err == nil {
			return parsed
		}
	}
	return fallback
}
