package router_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/router"
)

func TestRoute_ExactMatch(t *testing.T) {
	t.Parallel()
	r, err := router.New([]router.Profile{
		{Name: "linux-x64", Labels: []string{"self-hosted", "linux", "x64"}},
	})
	require.NoError(t, err)
	got, err := r.Route([]string{"self-hosted", "linux", "x64"})
	require.NoError(t, err)
	require.Equal(t, "linux-x64", got)
}

func TestRoute_ProfileExtrasMatch(t *testing.T) {
	t.Parallel()
	// Profile has labels [self-hosted, linux, x64, fast-disk]; a job
	// requesting [self-hosted, linux, x64] matches because the
	// profile is a SUPERSET.
	r, err := router.New([]router.Profile{
		{Name: "fast-x64", Labels: []string{"self-hosted", "linux", "x64", "fast-disk"}},
	})
	require.NoError(t, err)
	got, err := r.Route([]string{"self-hosted", "linux", "x64"})
	require.NoError(t, err)
	require.Equal(t, "fast-x64", got)
}

func TestRoute_JobExtrasDoNotMatch(t *testing.T) {
	t.Parallel()
	// Job requests [self-hosted, linux, gpu] but the only profile
	// lacks `gpu` — must not match.
	r, err := router.New([]router.Profile{
		{Name: "linux-x64", Labels: []string{"self-hosted", "linux", "x64"}},
	})
	require.NoError(t, err)
	_, err = r.Route([]string{"self-hosted", "linux", "gpu"})
	require.ErrorIs(t, err, router.ErrNoMatchingProfile)
}

func TestRoute_SmallestExtraWins(t *testing.T) {
	t.Parallel()
	// Both profiles match — but "linux-x64" has 0 extras and
	// "fast-x64" has 1, so the smaller wins.
	r, err := router.New([]router.Profile{
		{Name: "fast-x64", Labels: []string{"self-hosted", "linux", "x64", "fast-disk"}},
		{Name: "linux-x64", Labels: []string{"self-hosted", "linux", "x64"}},
	})
	require.NoError(t, err)
	got, err := r.Route([]string{"self-hosted", "linux", "x64"})
	require.NoError(t, err)
	require.Equal(t, "linux-x64", got)
}

func TestRoute_TieResolvedByDeclarationOrder(t *testing.T) {
	t.Parallel()
	// Two profiles, both with the same labels (same extra count).
	// First declared wins — operators express priority by ordering.
	r, err := router.New([]router.Profile{
		{Name: "primary", Labels: []string{"self-hosted", "linux", "x64"}},
		{Name: "backup", Labels: []string{"self-hosted", "linux", "x64"}},
	})
	require.NoError(t, err)
	got, err := r.Route([]string{"self-hosted", "linux", "x64"})
	require.NoError(t, err)
	require.Equal(t, "primary", got)
}

func TestRoute_NoMatch(t *testing.T) {
	t.Parallel()
	r, err := router.New([]router.Profile{
		{Name: "linux-x64", Labels: []string{"self-hosted", "linux", "x64"}},
		{Name: "gpu", Labels: []string{"self-hosted", "linux", "gpu"}},
	})
	require.NoError(t, err)
	_, err = r.Route([]string{"self-hosted", "windows"})
	require.ErrorIs(t, err, router.ErrNoMatchingProfile)
}

func TestRoute_PrefersGPUProfileForGPULabel(t *testing.T) {
	t.Parallel()
	// Real-world arrangement: declare a generic profile first, then
	// a gpu profile. A gpu-requesting job must route to gpu even
	// though the generic profile is declared first.
	r, err := router.New([]router.Profile{
		{Name: "linux-x64", Labels: []string{"self-hosted", "linux", "x64"}},
		{Name: "gpu", Labels: []string{"self-hosted", "linux", "x64", "gpu"}},
	})
	require.NoError(t, err)

	got, err := r.Route([]string{"self-hosted", "linux", "x64", "gpu"})
	require.NoError(t, err)
	require.Equal(t, "gpu", got, "gpu job must reach the gpu profile")

	got, err = r.Route([]string{"self-hosted", "linux", "x64"})
	require.NoError(t, err)
	require.Equal(t, "linux-x64", got, "non-gpu job prefers the smaller-extra match")
}

func TestRoute_EmptyJobLabelsMatchesFirstProfile(t *testing.T) {
	t.Parallel()
	// Edge case: synthetic empty label set. Every profile is a
	// superset of the empty set; tie resolution picks the first.
	r, err := router.New([]router.Profile{
		{Name: "primary", Labels: []string{"self-hosted", "linux"}},
		{Name: "secondary", Labels: []string{"self-hosted", "linux", "extra"}},
	})
	require.NoError(t, err)
	got, err := r.Route(nil)
	require.NoError(t, err)
	require.Equal(t, "primary", got, "empty job labels resolves to smallest-superset (tie: first declared)")
}

func TestRoute_NilRouterReturnsNoMatch(t *testing.T) {
	t.Parallel()
	var r *router.Router
	_, err := r.Route([]string{"any"})
	require.ErrorIs(t, err, router.ErrNoMatchingProfile)
}

func TestNew_RejectsEmptyName(t *testing.T) {
	t.Parallel()
	_, err := router.New([]router.Profile{{Name: "", Labels: []string{"x"}}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "name is required")
}

func TestNew_RejectsDuplicateName(t *testing.T) {
	t.Parallel()
	_, err := router.New([]router.Profile{
		{Name: "x", Labels: []string{"a"}},
		{Name: "x", Labels: []string{"b"}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")
}

func TestCoverageGaps(t *testing.T) {
	t.Parallel()
	r, err := router.New([]router.Profile{
		{Name: "p1", Labels: []string{"self-hosted", "linux", "x64"}},
		{Name: "p2", Labels: []string{"self-hosted", "linux", "gpu"}},
	})
	require.NoError(t, err)

	// Every scaleset label is covered by at least one profile.
	require.Empty(t, r.CoverageGaps([]string{"self-hosted", "linux", "x64", "gpu"}))

	// A scaleset advertising "windows" is uncovered.
	gaps := r.CoverageGaps([]string{"self-hosted", "windows"})
	require.Equal(t, []string{"windows"}, gaps)
}
