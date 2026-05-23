// Package priority maps a job's per-message metadata to one of the
// operator-declared priority classes.
//
// Routing rules (issue #10):
//   - Each class declares a Match selector (any combination of
//     workflow_label / repo / org).
//   - A class matches a job iff EVERY non-empty selector field
//     equals the job's corresponding field. Empty selector fields
//     are wildcards.
//   - When multiple classes match, the most specific (highest
//     selector field count) wins. Ties resolve by declaration
//     order so operators can express priority by listing classes
//     in their preferred order.
//   - When no class matches, [Classify] returns the default class
//     (the first class with an empty Match, or the synthetic
//     ZeroClass when none is declared).
//
// The package is intentionally pure: no I/O, no metrics, no
// side-effects. Wiring decisions live in the caller.
package priority

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
)

// ErrDuplicateClass means two classes share a name. New refuses to
// build a Matcher containing duplicates so the priority comparator
// can rely on names being unique.
var ErrDuplicateClass = errors.New("priority: duplicate class name")

// ErrEmptyClassName means a class was declared with an empty name.
var ErrEmptyClassName = errors.New("priority: class name is required")

// JobInfo is the projection of upstream scaleset.JobMessageBase the
// matcher consults. Reproduced here so this package doesn't take a
// dependency on the listener types.
type JobInfo struct {
	// Org is GitHub's owner login (e.g. "acme").
	Org string
	// Repo is the bare repository name (e.g. "platform"), NOT in
	// owner/repo form. Listener payloads carry the bare name on
	// JobMessageBase.RepositoryName.
	Repo string
	// WorkflowLabels is the merged set of `runs-on` labels the
	// workflow requested plus any workflow-level labels the user
	// has added via gh-actions-runner labels.
	WorkflowLabels []string
}

// Match is the selector for a single class. An empty field is a
// wildcard — set just the dimension(s) you want to scope by.
type Match struct {
	WorkflowLabel string // matches when present in JobInfo.WorkflowLabels
	Repo          string // matches when equal to JobInfo.Repo
	Org           string // matches when equal to JobInfo.Org
}

// Class is one operator-declared priority class.
type Class struct {
	// Name identifies the class in metrics, admin output, and
	// log lines. Required, must be unique.
	Name string

	// Match is the selector. An entirely empty Match means
	// "match everything" — used to declare a default class.
	Match Match

	// Weight orders classes. Higher = more important. The actual
	// numeric values are operator-defined; the matcher only uses
	// them for comparisons.
	Weight int

	// Preempt, when true, makes this class eligible to evict a
	// lower-weight Assigned (not-yet-Running) VM when the pool is
	// at capacity. The matcher does not act on this; consumers
	// (scaler / admin) check the flag.
	Preempt bool
}

// ZeroClass is the synthetic "no priority configured" class returned
// by Classify when the operator hasn't declared any classes. It
// matches nothing in particular but lets the scaler always have a
// non-nil class for metric labels.
var ZeroClass = Class{Name: "default", Weight: 0, Preempt: false}

// Matcher is the operator's class list, pre-validated and ordered
// for deterministic selection. Construct once at app startup and
// share across goroutines — Classify is read-only after
// construction.
type Matcher struct {
	classes []Class
}

// New validates the class list and constructs a Matcher. Returns
// ErrEmptyClassName / ErrDuplicateClass when input is malformed.
// An empty class list is allowed — Classify then always returns
// ZeroClass.
func New(classes []Class) (*Matcher, error) {
	seen := make(map[string]struct{}, len(classes))
	out := make([]Class, 0, len(classes))
	for i, c := range classes {
		if c.Name == "" {
			return nil, fmt.Errorf("%w (index %d)", ErrEmptyClassName, i)
		}
		if _, dup := seen[c.Name]; dup {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateClass, c.Name)
		}
		seen[c.Name] = struct{}{}
		out = append(out, c)
	}
	return &Matcher{classes: out}, nil
}

// Classify returns the class that best matches the given job. Best
// match is most-specific (highest count of populated selector
// fields); ties resolve by declaration order. Returns ZeroClass
// when no class matches (including when the Matcher is nil or
// empty).
func (m *Matcher) Classify(info JobInfo) Class {
	if m == nil || len(m.classes) == 0 {
		return ZeroClass
	}
	bestIdx := -1
	bestSpec := -1
	for i, c := range m.classes {
		if !c.Match.satisfies(info) {
			continue
		}
		spec := c.Match.specificity()
		if bestIdx == -1 || spec > bestSpec {
			bestIdx = i
			bestSpec = spec
		}
	}
	if bestIdx == -1 {
		return ZeroClass
	}
	return m.classes[bestIdx]
}

// Classes returns the configured classes in declaration order so
// the caller can iterate (e.g. to pre-create metric series). The
// returned slice is a copy.
func (m *Matcher) Classes() []Class {
	if m == nil {
		return nil
	}
	out := make([]Class, len(m.classes))
	copy(out, m.classes)
	return out
}

// satisfies reports whether the selector matches the given job.
// All non-empty fields must equal their corresponding job field.
func (s Match) satisfies(info JobInfo) bool {
	if s.Org != "" && s.Org != info.Org {
		return false
	}
	if s.Repo != "" && s.Repo != info.Repo {
		return false
	}
	if s.WorkflowLabel != "" && !slices.Contains(info.WorkflowLabels, s.WorkflowLabel) {
		return false
	}
	return true
}

// specificity counts how many selector fields are populated.
func (s Match) specificity() int {
	n := 0
	if s.Org != "" {
		n++
	}
	if s.Repo != "" {
		n++
	}
	if s.WorkflowLabel != "" {
		n++
	}
	return n
}

// String renders the class for log fields and admin output.
func (c Class) String() string {
	var parts []string
	parts = append(parts, c.Name, fmt.Sprintf("weight=%d", c.Weight))
	if c.Preempt {
		parts = append(parts, "preempt=true")
	}
	if spec := c.Match.specificity(); spec > 0 {
		var sels []string
		if c.Match.Org != "" {
			sels = append(sels, "org="+c.Match.Org)
		}
		if c.Match.Repo != "" {
			sels = append(sels, "repo="+c.Match.Repo)
		}
		if c.Match.WorkflowLabel != "" {
			sels = append(sels, "label="+c.Match.WorkflowLabel)
		}
		sort.Strings(sels)
		parts = append(parts, "{"+strings.Join(sels, ",")+"}")
	}
	return strings.Join(parts, " ")
}
