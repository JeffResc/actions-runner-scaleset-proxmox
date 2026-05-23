package priority_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/priority"
)

func TestClassify_NoMatchReturnsZero(t *testing.T) {
	t.Parallel()
	m, err := priority.New([]priority.Class{
		{Name: "high", Weight: 100, Match: priority.Match{Org: "acme"}},
	})
	require.NoError(t, err)

	got := m.Classify(priority.JobInfo{Org: "other"})
	require.Equal(t, priority.ZeroClass, got)
}

func TestClassify_OrgMatch(t *testing.T) {
	t.Parallel()
	m, err := priority.New([]priority.Class{
		{Name: "acme-high", Weight: 100, Match: priority.Match{Org: "acme"}},
	})
	require.NoError(t, err)

	got := m.Classify(priority.JobInfo{Org: "acme", Repo: "platform"})
	require.Equal(t, "acme-high", got.Name)
}

func TestClassify_MostSpecificWins(t *testing.T) {
	t.Parallel()
	// Both classes match acme/platform; the repo-scoped class is
	// more specific (2 selector fields vs 1) and must win.
	m, err := priority.New([]priority.Class{
		{Name: "org-wide", Weight: 50, Match: priority.Match{Org: "acme"}},
		{Name: "repo-pinned", Weight: 100, Match: priority.Match{Org: "acme", Repo: "platform"}},
	})
	require.NoError(t, err)

	got := m.Classify(priority.JobInfo{Org: "acme", Repo: "platform"})
	require.Equal(t, "repo-pinned", got.Name)
}

func TestClassify_TieResolvedByDeclarationOrder(t *testing.T) {
	t.Parallel()
	// Two classes with the same specificity (both Org-only) match
	// — declaration order wins.
	m, err := priority.New([]priority.Class{
		{Name: "first", Weight: 100, Match: priority.Match{Org: "acme"}},
		{Name: "second", Weight: 200, Match: priority.Match{Org: "acme"}},
	})
	require.NoError(t, err)

	got := m.Classify(priority.JobInfo{Org: "acme"})
	require.Equal(t, "first", got.Name)
}

func TestClassify_WorkflowLabelSelector(t *testing.T) {
	t.Parallel()
	m, err := priority.New([]priority.Class{
		{Name: "critical", Weight: 200, Match: priority.Match{WorkflowLabel: "priority:critical"}},
	})
	require.NoError(t, err)

	hit := m.Classify(priority.JobInfo{
		WorkflowLabels: []string{"self-hosted", "linux", "priority:critical"},
	})
	require.Equal(t, "critical", hit.Name)

	miss := m.Classify(priority.JobInfo{
		WorkflowLabels: []string{"self-hosted", "linux"},
	})
	require.Equal(t, priority.ZeroClass, miss)
}

func TestClassify_EmptyMatchIsWildcard(t *testing.T) {
	t.Parallel()
	// An empty Match means "match everything" — useful as a
	// default class. Without any other classes, every job falls
	// into this one.
	m, err := priority.New([]priority.Class{
		{Name: "default", Weight: 10}, // empty Match
	})
	require.NoError(t, err)

	got := m.Classify(priority.JobInfo{Org: "acme", Repo: "platform"})
	require.Equal(t, "default", got.Name)
}

func TestClassify_DefaultLosesToSpecific(t *testing.T) {
	t.Parallel()
	m, err := priority.New([]priority.Class{
		{Name: "default", Weight: 10},
		{Name: "critical", Weight: 200, Match: priority.Match{Org: "acme"}},
	})
	require.NoError(t, err)

	// acme job: critical (specificity=1) beats default (specificity=0).
	got := m.Classify(priority.JobInfo{Org: "acme"})
	require.Equal(t, "critical", got.Name)

	// non-acme job: only the wildcard default matches.
	got = m.Classify(priority.JobInfo{Org: "other"})
	require.Equal(t, "default", got.Name)
}

func TestClassify_NilOrEmptyMatcher(t *testing.T) {
	t.Parallel()
	var m *priority.Matcher
	require.Equal(t, priority.ZeroClass, m.Classify(priority.JobInfo{Org: "acme"}))

	empty, err := priority.New(nil)
	require.NoError(t, err)
	require.Equal(t, priority.ZeroClass, empty.Classify(priority.JobInfo{Org: "acme"}))
}

func TestNew_RejectsEmptyName(t *testing.T) {
	t.Parallel()
	_, err := priority.New([]priority.Class{{Name: "", Weight: 1}})
	require.ErrorIs(t, err, priority.ErrEmptyClassName)
}

func TestNew_RejectsDuplicateName(t *testing.T) {
	t.Parallel()
	_, err := priority.New([]priority.Class{
		{Name: "x", Weight: 1},
		{Name: "x", Weight: 2},
	})
	require.ErrorIs(t, err, priority.ErrDuplicateClass)
}

func TestClasses_ReturnsCopy(t *testing.T) {
	t.Parallel()
	in := []priority.Class{{Name: "a", Weight: 1}, {Name: "b", Weight: 2}}
	m, err := priority.New(in)
	require.NoError(t, err)
	got := m.Classes()
	require.Len(t, got, 2)

	// Mutating the returned slice must not affect the Matcher.
	got[0].Name = "MUTATED"
	require.Equal(t, "a", m.Classify(priority.JobInfo{}).Name)
}

func TestClass_String(t *testing.T) {
	t.Parallel()
	c := priority.Class{
		Name: "critical", Weight: 200, Preempt: true,
		Match: priority.Match{Org: "acme", WorkflowLabel: "priority:critical"},
	}
	s := c.String()
	require.Contains(t, s, "critical")
	require.Contains(t, s, "weight=200")
	require.Contains(t, s, "preempt=true")
	require.Contains(t, s, "label=priority:critical")
	require.Contains(t, s, "org=acme")
}
