package fakeproxmox

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

// handleAgentGetOSInfo replies with a stub OS info payload, or with the
// 500 "guest agent is not responding" body during a VM's
// GuestAgentDelay window after Start.
func (s *Server) handleAgentGetOSInfo(w http.ResponseWriter, r *http.Request) {
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
	if !v.Running {
		writeError(w, http.StatusInternalServerError, "VM is not running")
		return
	}
	if s.store.agentWait > 0 && time.Since(v.StartedAt) < s.store.agentWait {
		writeError(w, http.StatusInternalServerError, "guest agent is not responding")
		return
	}
	writeData(w, map[string]any{
		"result": map[string]any{
			"id":             "ubuntu",
			"kernel-release": "6.8.0-fake",
			"name":           "Ubuntu",
			"pretty-name":    "Ubuntu 26.04 LTS",
			"version":        "26.04",
			"version-id":     "26.04",
		},
	})
}

// handleAgentFileWrite records the write but doesn't persist anything
// (the orchestrator's only consumers are file-read against the same
// path, and we ack file-write as success in either case). Returns
// {"data": null} like real Proxmox.
func (s *Server) handleAgentFileWrite(w http.ResponseWriter, r *http.Request) {
	if !s.assertVMRunning(w, r) {
		return
	}
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	writeData(w, nil)
}

// handleAgentFileRead returns an empty file contents by default. Tests
// can override per-file content with SeedAgentFile (TODO when needed
// for richer scenarios — current callers only care about the empty-case
// shape).
func (s *Server) handleAgentFileRead(w http.ResponseWriter, r *http.Request) {
	if !s.assertVMRunning(w, r) {
		return
	}
	writeData(w, map[string]any{
		"content":   "",
		"truncated": 0,
	})
}

// handleAgentExec acknowledges any exec request with a synthetic PID.
// The orchestrator's caller polls exec-status next.
func (s *Server) handleAgentExec(w http.ResponseWriter, r *http.Request) {
	if !s.assertVMRunning(w, r) {
		return
	}
	writeData(w, map[string]any{"pid": 4242})
}

// handleAgentExecStatus reports "exited successfully" — no real exec
// happens. exited=1 + exitcode=0 is what real Proxmox returns when a
// guest exec completed cleanly.
func (s *Server) handleAgentExecStatus(w http.ResponseWriter, r *http.Request) {
	if !s.assertVMRunning(w, r) {
		return
	}
	writeData(w, map[string]any{
		"exited":   1,
		"exitcode": 0,
		"out-data": "",
		"err-data": "",
	})
}

// assertVMRunning returns true after writing nothing if the VM exists
// and is running. Otherwise it writes a 500 reply and returns false.
// Centralises the precondition shared by every guest-agent handler.
func (s *Server) assertVMRunning(w http.ResponseWriter, r *http.Request) bool {
	vmid, err := vmidParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad vmid")
		return false
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	v, ok := s.store.findVMLocked(vmid)
	if !ok {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("Configuration file 'nodes/%s/qemu-server/%d.conf' does not exist",
				chi.URLParam(r, "node"), vmid))
		return false
	}
	if !v.Running {
		writeError(w, http.StatusInternalServerError, "VM is not running")
		return false
	}
	return true
}
