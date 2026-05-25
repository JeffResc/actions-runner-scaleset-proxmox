// Package canary stages a template rollout: a percentage of new
// clones use a candidate template_vmid while the rest use the
// current stable template. Boot failures on canary clones feed a
// running failure rate; when the rate exceeds a configurable
// threshold the controller auto-reverts the percentage to 0 so the
// candidate stops getting traffic while the operator investigates.
//
// The controller is intentionally per-process — config promotion
// updates this in-memory state but does NOT rewrite the YAML
// config. Operators that want the promotion to survive a restart
// should also edit `template_vmid` in their config.
//
// Routing rules (issue #5):
//   - A profile with no candidate configured (or canary_percent=0)
//     always returns the stable template.
//   - With candidate + canary_percent=N, deterministically pick
//     candidate for ~N% of VMIDs using a hash-mod check. The
//     determinism matters because Pick is called from racy
//     reconcile goroutines — using the VMID rather than a global
//     counter keeps repeated Pick(profile, vmid) calls stable
//     across retries.
//   - RecordFailure feeds a cumulative canary-failure counter.
//     Once the counter has at least minSamples canary clones,
//     the controller compares failures/clones against the
//     operator's max_failure_rate and flips canary_percent to 0
//     when exceeded (revertedPercent stores the original so
//     Promote / restart can recover the value).
package canary

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
)

// ErrUnknownProfile is returned by every method when the profile
// name doesn't match any registered profile.
var ErrUnknownProfile = errors.New("canary: unknown profile")

// ErrNoCandidate is returned by Promote when the profile doesn't
// have a candidate template configured — there's nothing to
// promote.
var ErrNoCandidate = errors.New("canary: profile has no candidate template")

// DefaultMinFailureSamples is the minimum number of canary clones
// the controller must observe before the failure-rate check
// fires. Without this gate a single failed clone would auto-
// revert any non-zero rate threshold (1/1 = 100% > any threshold
// below 1). Per-Controller via SetMinFailureSamples so tests can
// drive the threshold deterministically without racing on
// package state.
const DefaultMinFailureSamples = 5

// ProfileConfig is the per-profile canary state at construction
// time. Operators feed this via internal/config; tests build it
// directly.
type ProfileConfig struct {
	// Name identifies the profile (matches the pool's profile
	// name).
	Name string

	// StableTemplateVMID is the always-on template. Required.
	StableTemplateVMID int

	// CandidateTemplateVMID is the staging template that
	// Percent% of new clones use. Zero = no candidate (Pick
	// always returns stable).
	CandidateTemplateVMID int

	// Percent is the operator's requested canary share, 0-100.
	// Zero disables canary traffic regardless of
	// CandidateTemplateVMID.
	Percent int

	// MaxFailureRate is the canary-only failure ratio at which
	// the controller auto-reverts Percent to 0. 0.0 disables
	// the auto-revert (operator wants manual control). Range
	// (0.0, 1.0].
	MaxFailureRate float64
}

// Template is the tag-friendly classification of a clone.
type Template string

const (
	// Stable means the clone used the current production
	// template.
	Stable Template = "stable"
	// Candidate means the clone used the staging template
	// (Percent% of traffic).
	Candidate Template = "candidate"
)

// PickResult is what Pick returns: the template VMID to clone
// from plus the classification (for store / tag / metric
// labelling).
type PickResult struct {
	TemplateVMID int
	Template     Template
}

// profileState is the controller's per-profile mutable state.
type profileState struct {
	mu sync.Mutex

	stable    int
	candidate int

	// percent is the live canary share. RecordFailure may
	// reduce it to 0; Promote may swap candidate into stable.
	percent int

	// originalPercent preserves the operator's configured
	// Percent so Status / log lines can show "auto-reverted
	// from N% to 0%".
	originalPercent int

	maxFailureRate float64

	// counters
	canaryClones   int
	canaryFailures int

	// reverted is true after an auto-revert fires. Stays true
	// until Promote (which clears it as part of the swap).
	reverted bool
}

// Controller picks templates for new clones and tracks
// canary-only failure rates. Construct once at app startup with
// the per-profile ProfileConfig list; all methods are safe for
// concurrent use.
type Controller struct {
	mu       sync.RWMutex
	profiles map[string]*profileState
	// minFailureSamples gates the auto-revert check. Tests set
	// this via SetMinFailureSamples to drive RecordFailure
	// deterministically; production uses DefaultMinFailureSamples.
	minFailureSamples int
}

// New builds a Controller from a slice of per-profile configs.
// Rejects duplicate names, negative percents > 100, and out-of-
// range MaxFailureRate values. A nil/empty list is allowed — Pick
// then returns ErrUnknownProfile for every call (callers should
// fall back to the orchestrator-wide template, which is the no-
// canary path).
func New(cfgs []ProfileConfig) (*Controller, error) {
	c := &Controller{
		profiles:          make(map[string]*profileState, len(cfgs)),
		minFailureSamples: DefaultMinFailureSamples,
	}
	for i, p := range cfgs {
		if p.Name == "" {
			return nil, fmt.Errorf("canary: profiles[%d].name is required", i)
		}
		if _, dup := c.profiles[p.Name]; dup {
			return nil, fmt.Errorf("canary: duplicate profile name %q", p.Name)
		}
		if p.Percent < 0 || p.Percent > 100 {
			return nil, fmt.Errorf("canary: profiles[%d] %q: percent must be in [0, 100]", i, p.Name)
		}
		if p.MaxFailureRate < 0 || p.MaxFailureRate > 1 {
			return nil, fmt.Errorf("canary: profiles[%d] %q: max_failure_rate must be in [0, 1]", i, p.Name)
		}
		c.profiles[p.Name] = &profileState{
			stable:          p.StableTemplateVMID,
			candidate:       p.CandidateTemplateVMID,
			percent:         p.Percent,
			originalPercent: p.Percent,
			maxFailureRate:  p.MaxFailureRate,
		}
	}
	return c, nil
}

// SetMinFailureSamples overrides the minimum-canary-clones gate
// for the auto-revert check. Used by tests to drive RecordFailure
// deterministically; production wiring leaves the default in
// place. Values < 1 are clamped to 1 (a 0 threshold would
// auto-revert on the very first failure).
func (c *Controller) SetMinFailureSamples(n int) {
	if n < 1 {
		n = 1
	}
	c.mu.Lock()
	c.minFailureSamples = n
	c.mu.Unlock()
}

// Pick returns the template VMID to clone from for a given (profile,
// vmid). The choice is deterministic: hash(vmid) modulo 100
// against the current Percent. Returns ErrUnknownProfile when the
// profile isn't registered; callers should treat that as "use the
// orchestrator-wide template" (the no-canary fallback path).
func (c *Controller) Pick(profile string, vmid int) (PickResult, error) {
	c.mu.RLock()
	ps, ok := c.profiles[profile]
	c.mu.RUnlock()
	if !ok {
		return PickResult{}, ErrUnknownProfile
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.candidate == 0 || ps.percent == 0 {
		return PickResult{TemplateVMID: ps.stable, Template: Stable}, nil
	}
	// hash(vmid) mod 100 < percent → candidate.
	if hashMod100(vmid) < ps.percent {
		return PickResult{TemplateVMID: ps.candidate, Template: Candidate}, nil
	}
	return PickResult{TemplateVMID: ps.stable, Template: Stable}, nil
}

// RecordClone increments the canary-clones counter for the
// profile (only for clones that actually used the candidate).
// Stable clones are not tracked — the failure rate is canary-
// scoped. Unknown profile is silently ignored so the pool path
// doesn't need to fan out lookups.
func (c *Controller) RecordClone(profile string, t Template) {
	if t != Candidate {
		return
	}
	c.mu.RLock()
	ps, ok := c.profiles[profile]
	c.mu.RUnlock()
	if !ok {
		return
	}
	ps.mu.Lock()
	ps.canaryClones++
	ps.mu.Unlock()
}

// RecordFailure increments the canary-failures counter and, when
// the cumulative rate now exceeds MaxFailureRate (after observing
// at least MinFailureSamples), flips Percent to 0 and returns
// true. The caller logs / emits a metric on the true return. As
// with RecordClone, only Candidate clones contribute.
func (c *Controller) RecordFailure(profile string, t Template) bool {
	if t != Candidate {
		return false
	}
	c.mu.RLock()
	ps, ok := c.profiles[profile]
	minSamples := c.minFailureSamples
	c.mu.RUnlock()
	if !ok {
		return false
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.canaryFailures++
	if ps.maxFailureRate <= 0 || ps.canaryClones < minSamples {
		return false
	}
	rate := float64(ps.canaryFailures) / float64(ps.canaryClones)
	if rate > ps.maxFailureRate && !ps.reverted {
		ps.reverted = true
		ps.percent = 0
		return true
	}
	return false
}

// Promote atomically swaps the candidate template into the stable
// slot, resets the canary counters, and zeroes the candidate.
// After Promote, the candidate is the new production template;
// subsequent Pick calls always return the new stable VMID
// (Percent stays 0 until the operator declares a new candidate
// via config or a re-init).
//
// Returns ErrNoCandidate when there's nothing to promote.
func (c *Controller) Promote(profile string) error {
	c.mu.RLock()
	ps, ok := c.profiles[profile]
	c.mu.RUnlock()
	if !ok {
		return ErrUnknownProfile
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.candidate == 0 {
		return ErrNoCandidate
	}
	ps.stable = ps.candidate
	ps.candidate = 0
	ps.percent = 0
	ps.originalPercent = 0
	ps.canaryClones = 0
	ps.canaryFailures = 0
	ps.reverted = false
	return nil
}

// Status is a snapshot of one profile's canary state for admin
// inspection / metrics.
type Status struct {
	StableTemplateVMID    int
	CandidateTemplateVMID int
	Percent               int
	OriginalPercent       int
	MaxFailureRate        float64
	CanaryClones          int
	CanaryFailures        int
	Reverted              bool
}

// Status returns the current snapshot for a profile.
func (c *Controller) Status(profile string) (Status, error) {
	c.mu.RLock()
	ps, ok := c.profiles[profile]
	c.mu.RUnlock()
	if !ok {
		return Status{}, ErrUnknownProfile
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return Status{
		StableTemplateVMID:    ps.stable,
		CandidateTemplateVMID: ps.candidate,
		Percent:               ps.percent,
		OriginalPercent:       ps.originalPercent,
		MaxFailureRate:        ps.maxFailureRate,
		CanaryClones:          ps.canaryClones,
		CanaryFailures:        ps.canaryFailures,
		Reverted:              ps.reverted,
	}, nil
}

// hashMod100 maps an integer to [0, 100) deterministically.
// FNV-1a is the lightweight stdlib choice — well-distributed for
// the small VMID input domain (typically 10000-19999) and ~10x
// faster than SHA-256 on this call path. Sampling is not
// adversarial so cryptographic uniformity isn't needed.
func hashMod100(vmid int) int {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(vmid)) // #nosec G115 -- vmid is a positive int from the allocator
	h := fnv.New64a()
	_, _ = h.Write(buf[:])
	return int(h.Sum64() % 100)
}
