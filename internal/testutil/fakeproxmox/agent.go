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
	if f, ok := s.store.matchFaultLocked(FaultGuestAgentNotReady, vmid); ok {
		// Stay "not ready" until Duration elapses since the VM started.
		// Mirrors the real-world startup window where qemu-guest-agent
		// is installed but the systemd unit hasn't come up yet.
		if time.Since(v.StartedAt) < f.Duration {
			writeError(w, http.StatusInternalServerError, "QEMU guest agent is not running")
			return
		}
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
//
// When FaultJITInjectFail is registered for the target VMID (or
// VMID==0 to match all), every write returns 500 with a permission-
// denied body. This is the persistent-failure shape #247 exercises.
func (s *Server) handleAgentFileWrite(w http.ResponseWriter, r *http.Request) {
	if !s.assertVMRunning(w, r) {
		return
	}
	vmid, _ := vmidParam(r)
	s.store.mu.Lock()
	_, faulted := s.store.matchFaultLocked(FaultJITInjectFail, vmid)
	s.store.mu.Unlock()
	if faulted {
		writeError(w, http.StatusInternalServerError,
			"permission denied: cannot write /opt/actions-runner/jitconfig.env.tmp")
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
