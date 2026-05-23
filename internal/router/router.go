// Package router maps a job's requested labels to one of the
// orchestrator's configured runner profiles.
//
// Routing rules (issue #7):
//   - A profile satisfies a job iff the profile's Labels are a
//     SUPERSET of the job's requested labels.
//   - When multiple profiles satisfy, pick the one with the smallest
//     extra-label count (most specific). The intuition: a job asking
//     for [self-hosted, linux, x64] prefers a profile labelled
//     [self-hosted, linux, x64] over one labelled
//     [self-hosted, linux, x64, gpu, fast-disk].
//   - Ties (same extra-label count) resolve by declaration order so
//     operators can express priority by listing profiles in their
//     preferred order.
//   - When no profile satisfies, Route returns ErrNoMatchingProfile.
//     Callers (scaler) emit an unrouted-jobs metric and log the
//     mismatch so operators can fix their profile coverage.
//
// The router is intentionally pure: no side effects, no I/O, no
// metrics. Wiring decisions live in the caller.
package router

import (
	"errors"
	"fmt"
)

// ErrNoMatchingProfile means no configured profile's Labels are a
// superset of the job's requested labels.
var ErrNoMatchingProfile = errors.New("router: no matching profile")

// Profile is the minimal projection the router needs from
// config.ProfileConfig. Re-stated here so this package doesn't take a
// dependency on internal/config.
type Profile struct {
	// Name identifies the profile and is what Route returns on a hit.
	Name string
	// Labels are the GitHub Actions labels this profile satisfies.
	// Order doesn't matter; Route compares as a set.
	Labels []string
}

// Router holds the operator-declared profile list. Construct once at
// app startup and share across goroutines — Route is read-only after
// construction.
type Router struct {
	profiles []normalisedProfile
}

// normalisedProfile is the Router's internal view of a Profile with
// its labels pre-built as a set so Route doesn't pay for the
// conversion on every call.
type normalisedProfile struct {
	name      string
	labels    map[string]struct{}
	labelsRaw []string // preserved for diagnostics
}

// New builds a Router from the operator's profile list. Order is
// preserved so tie-breaking is deterministic. Returns an error when
// a profile has an empty name or duplicate profile names exist —
// the config layer catches these too but the router enforces them
// defensively in case it is constructed from a different source.
func New(profiles []Profile) (*Router, error) {
	r := &Router{profiles: make([]normalisedProfile, 0, len(profiles))}
	seen := make(map[string]struct{}, len(profiles))
	for i, p := range profiles {
		if p.Name == "" {
			return nil, fmt.Errorf("router: profiles[%d].name is required", i)
		}
		if _, dup := seen[p.Name]; dup {
			return nil, fmt.Errorf("router: duplicate profile name %q", p.Name)
		}
		seen[p.Name] = struct{}{}
		set := make(map[string]struct{}, len(p.Labels))
		for _, l := range p.Labels {
			set[l] = struct{}{}
		}
		r.profiles = append(r.profiles, normalisedProfile{
			name:      p.Name,
			labels:    set,
			labelsRaw: append([]string(nil), p.Labels...),
		})
	}
	return r, nil
}

// Route returns the name of the profile that best matches the
// requested labels (smallest extra-label count among supersets;
// ties broken by declaration order). Returns ErrNoMatchingProfile
// when no profile's labels are a superset of jobLabels.
//
// An empty jobLabels slice matches any profile that has at least one
// label and picks the smallest by extra-label count (which is the
// profile's whole label set). This is a defensive default: GitHub
// always sends some labels (at minimum "self-hosted"), so an empty
// slice here is a synthetic case mostly seen in tests.
func (r *Router) Route(jobLabels []string) (string, error) {
	if r == nil || len(r.profiles) == 0 {
		return "", ErrNoMatchingProfile
	}
	bestIdx := -1
	bestExtra := -1
	for i, p := range r.profiles {
		if !containsAll(p.labels, jobLabels) {
			continue
		}
		// Defensive: jobLabels with duplicates can push the
		// difference negative; clamp to 0 so the comparison stays
		// meaningful.
		extra := max(len(p.labels)-len(jobLabels), 0)
		if bestIdx == -1 || extra < bestExtra {
			bestIdx = i
			bestExtra = extra
		}
	}
	if bestIdx == -1 {
		return "", ErrNoMatchingProfile
	}
	return r.profiles[bestIdx].name, nil
}

// containsAll reports whether every element of want appears in haveSet.
func containsAll(haveSet map[string]struct{}, want []string) bool {
	for _, w := range want {
		if _, ok := haveSet[w]; !ok {
			return false
		}
	}
	return true
}

// CoverageGaps returns the labels in scaleSetLabels that no profile's
// label set contains. Empty result means the operator's profiles
// collectively cover every label the scaleset advertises — which is
// the invariant config validation enforces. Returned slice is sorted
// for stable error messages.
func (r *Router) CoverageGaps(scaleSetLabels []string) []string {
	if r == nil {
		return append([]string(nil), scaleSetLabels...)
	}
	union := make(map[string]struct{})
	for _, p := range r.profiles {
		for l := range p.labels {
			union[l] = struct{}{}
		}
	}
	var gaps []string
	for _, l := range scaleSetLabels {
		if _, ok := union[l]; !ok {
			gaps = append(gaps, l)
		}
	}
	return gaps
}
