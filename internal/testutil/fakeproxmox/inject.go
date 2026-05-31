package fakeproxmox

import "time"

// FaultKind enumerates the failure modes the fake can simulate. Tests
// add a Fault to the server via InjectFault and the matching handler
// replies with the corresponding error shape instead of normal success.
//
// P1 wires the type and registration but does NOT yet implement the
// per-handler matchers — those land with P4 (failure-injection
// scenarios). The type lives here so the public surface is stable
// across pillars.
type FaultKind int

const (
	// FaultNone is the zero value; an unset fault matches nothing.
	FaultNone FaultKind = iota

	// FaultVMNotFoundOnDestroy makes DELETE /qemu/{vmid} reply with a
	// 500 "Configuration file ... does not exist" body — the shape
	// production-Proxmox returns when an operator already removed the
	// VM out-of-band. The orchestrator must treat this as idempotent
	// success.
	FaultVMNotFoundOnDestroy

	// FaultVMNotFoundOnStop is the equivalent for status/stop and
	// status/shutdown.
	FaultVMNotFoundOnStop

	// FaultGuestAgentNotReady makes get-osinfo return 500 with the
	// "guest agent is not running" body for the configured Duration
	// after the matched VMID starts.
	FaultGuestAgentNotReady

	// FaultStatus500Spam makes the matched Path return HTTP 500 for
	// the next Count requests, then resume normal behavior.
	FaultStatus500Spam

	// FaultTaskNeverCompletes pins the matched task in "running"
	// state regardless of its configured duration. Used to exercise
	// orchestrator-side task timeouts.
	FaultTaskNeverCompletes

	// FaultTagApplyDelay delays the tag-stamping PUT /config response
	// by Duration, exposing the window between Clone returning and
	// tags landing.
	FaultTagApplyDelay

	// FaultJITInjectFail makes every agent/file-write call return 500
	// with a "permission denied" body — modelling the case where the
	// in-VM qemu-guest-agent is broken or the runner template is
	// missing the writable jitconfig directory. Used by the
	// inject-retry-exhaustion e2e test (#247) to drive a clone through
	// the orchestrator's full inject → retry → mark-completed →
	// destroy chain.
	FaultJITInjectFail
)

// Fault describes a single injected failure. Match semantics:
//   - VMID == 0 matches any VMID; otherwise the specific one.
//   - Path == "" matches the kind's default endpoint; otherwise the
//     specific path (used by FaultStatus500Spam).
//   - Count is consumed by FaultStatus500Spam (each spam decrements).
//   - Duration is consumed by FaultGuestAgentNotReady and
//     FaultTagApplyDelay.
type Fault struct {
	Kind     FaultKind
	VMID     int
	Path     string
	Count    int
	Duration time.Duration
}

// InjectFault registers a fault. Multiple faults can be active
// simultaneously; the handlers consult the slice in order.
func (s *Server) InjectFault(f Fault) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	s.store.faults = append(s.store.faults, f)
}

// ClearFaults removes all registered faults.
func (s *Server) ClearFaults() {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	s.store.faults = nil
}

// matchFaultLocked returns the first registered fault that matches kind
// and vmid. VMID 0 on the fault matches any vmid. Caller must hold
// s.mu. The fault is NOT removed — each handler decides whether the
// behaviour is one-shot or sticky.
func (s *store) matchFaultLocked(kind FaultKind, vmid int) (Fault, bool) {
	for _, f := range s.faults {
		if f.Kind != kind {
			continue
		}
		if f.VMID != 0 && f.VMID != vmid {
			continue
		}
		return f, true
	}
	return Fault{}, false
}

// consumeFaultLocked decrements the Count of the first matching
// fault (kind, vmid). When Count reaches 0 the fault is removed
// so subsequent requests resume normal handler behaviour. Used by
// count-bounded faults like FaultStatus500Spam to model "the
// next N requests fail then recover." Caller must hold s.mu.
func (s *store) consumeFaultLocked(kind FaultKind, vmid int) {
	for i, f := range s.faults {
		if f.Kind != kind {
			continue
		}
		if f.VMID != 0 && f.VMID != vmid {
			continue
		}
		s.faults[i].Count--
		if s.faults[i].Count <= 0 {
			s.faults = append(s.faults[:i], s.faults[i+1:]...)
		}
		return
	}
}
