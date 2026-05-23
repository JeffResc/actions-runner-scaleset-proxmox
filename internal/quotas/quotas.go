// Package quotas resolves the per-(org|repo) concurrency cap that
// applies to a given job.
//
// Operators declare default caps + zero-or-more overrides keyed by
// either repo or org. Resolve picks the most specific cap with a
// deterministic precedence:
//
//  1. Repo override (matches the exact "owner/repo")
//  2. Org override (matches the exact org)
//  3. default_per_repo (when a repo is set on the job)
//  4. default_per_org  (when only an org is set)
//
// A returned cap of 0 means "no cap" — the caller skips the quota
// check entirely. This matches the natural YAML default for unset
// integer fields and lets operators opt in per-block.
//
// The package is intentionally pure: no I/O, no metrics, no
// side-effects. Wiring decisions live in the caller.
package quotas

import (
	"errors"
	"fmt"
)

// ErrAmbiguousOverride means an Override had both Org and Repo set
// (or neither). New refuses these so Resolve can dispatch with a
// single equality check per override.
var ErrAmbiguousOverride = errors.New("quotas: override must set exactly one of org or repo")

// Override scopes a cap to a single org or a single owner/repo.
// Exactly one of Org or Repo must be set.
type Override struct {
	// Org matches a job whose owner equals this value.
	Org string
	// Repo matches a job whose full name equals this value, in
	// "owner/repo" form.
	Repo string
	// MaxConcurrent is the cap. 0 means "no cap" — a deliberate
	// "opt this scope out of the default" knob.
	MaxConcurrent int
}

// Config is the operator's full quota config.
type Config struct {
	// DefaultPerRepo applies to jobs whose repo has no override.
	// 0 disables the default. The cap is per-repo: a fleet with
	// 4 repos can have 4 × DefaultPerRepo VMs in flight
	// (subject to MaxConcurrentRunners).
	DefaultPerRepo int

	// DefaultPerOrg applies to jobs whose org has no override.
	// Same semantics as DefaultPerRepo.
	DefaultPerOrg int

	// Overrides take precedence over the defaults. A repo
	// override beats an org override for the same job.
	Overrides []Override
}

// Validate enforces the exactly-one-of-org-or-repo rule on every
// override. Called by config.Validate; callers that build Config
// programmatically should also call it.
func (c Config) Validate() error {
	for i, o := range c.Overrides {
		hasOrg, hasRepo := o.Org != "", o.Repo != ""
		if hasOrg == hasRepo {
			return fmt.Errorf("%w (index %d)", ErrAmbiguousOverride, i)
		}
		if o.MaxConcurrent < 0 {
			return fmt.Errorf("quotas: overrides[%d].max_concurrent must be >= 0", i)
		}
	}
	if c.DefaultPerRepo < 0 {
		return errors.New("quotas: default_per_repo must be >= 0")
	}
	if c.DefaultPerOrg < 0 {
		return errors.New("quotas: default_per_org must be >= 0")
	}
	return nil
}

// Resolver pre-indexes overrides so per-job lookups are O(1).
// Construct once at startup; Resolve is read-only after.
type Resolver struct {
	cfg     Config
	byRepo  map[string]int
	byOrg   map[string]int
	enabled bool // true when ANY cap is non-zero
}

// New validates cfg and returns a Resolver.
func New(cfg Config) (*Resolver, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	r := &Resolver{
		cfg:    cfg,
		byRepo: make(map[string]int, len(cfg.Overrides)),
		byOrg:  make(map[string]int, len(cfg.Overrides)),
	}
	for _, o := range cfg.Overrides {
		if o.Repo != "" {
			r.byRepo[o.Repo] = o.MaxConcurrent
		} else if o.Org != "" {
			r.byOrg[o.Org] = o.MaxConcurrent
		}
	}
	r.enabled = cfg.DefaultPerRepo > 0 || cfg.DefaultPerOrg > 0 ||
		len(cfg.Overrides) > 0
	return r, nil
}

// Enabled reports whether the resolver has any non-zero cap. The
// scaler checks this before paying the cost of stamping Org / Repo
// metadata onto the store row, so a deployment without quotas
// pays nothing.
func (r *Resolver) Enabled() bool {
	if r == nil {
		return false
	}
	return r.enabled
}

// Scope is what kind of quota Resolve matched on. Used as a
// Prometheus label so dashboards can attribute throttling to the
// right knob.
type Scope string

const (
	// ScopeRepo means the cap came from a repo override or
	// default_per_repo.
	ScopeRepo Scope = "repo"
	// ScopeOrg means the cap came from an org override or
	// default_per_org.
	ScopeOrg Scope = "org"
	// ScopeNone means no cap applies — the cap field of the
	// returned Result is 0 and the caller skips the check.
	ScopeNone Scope = ""
)

// Result is the effective cap for a job.
type Result struct {
	// Scope is the dimension the cap is enforced on.
	Scope Scope
	// Name is the bucket key for metrics (the org or owner/repo).
	// Empty when Scope == ScopeNone.
	Name string
	// Cap is the limit. 0 means "no cap" — the caller skips the
	// check. Always 0 when Scope == ScopeNone.
	Cap int
}

// Resolve returns the effective cap for a job. Precedence:
//
//  1. Repo override (matches the exact "owner/repo")
//  2. Org override (matches the exact org)
//  3. default_per_repo when repo is non-empty
//  4. default_per_org when only org is set
//
// repo is expected in "owner/repo" form; the scaler joins
// OwnerName + RepositoryName before calling. Empty repo + empty
// org returns ScopeNone.
func (r *Resolver) Resolve(org, repo string) Result {
	if r == nil || !r.enabled {
		return Result{Scope: ScopeNone}
	}
	if repo != "" {
		if cap, ok := r.byRepo[repo]; ok {
			return Result{Scope: ScopeRepo, Name: repo, Cap: cap}
		}
	}
	if org != "" {
		if cap, ok := r.byOrg[org]; ok {
			return Result{Scope: ScopeOrg, Name: org, Cap: cap}
		}
	}
	if repo != "" && r.cfg.DefaultPerRepo > 0 {
		return Result{Scope: ScopeRepo, Name: repo, Cap: r.cfg.DefaultPerRepo}
	}
	if org != "" && r.cfg.DefaultPerOrg > 0 {
		return Result{Scope: ScopeOrg, Name: org, Cap: r.cfg.DefaultPerOrg}
	}
	return Result{Scope: ScopeNone}
}
