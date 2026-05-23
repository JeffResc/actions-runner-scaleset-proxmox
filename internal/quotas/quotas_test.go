package quotas_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/quotas"
)

func TestResolve_NoConfigIsScopeNone(t *testing.T) {
	t.Parallel()
	r, err := quotas.New(quotas.Config{})
	require.NoError(t, err)
	require.False(t, r.Enabled())
	got := r.Resolve("acme", "acme/platform")
	require.Equal(t, quotas.ScopeNone, got.Scope)
	require.Equal(t, 0, got.Cap)
}

func TestResolve_DefaultPerRepoApplies(t *testing.T) {
	t.Parallel()
	r, err := quotas.New(quotas.Config{DefaultPerRepo: 5})
	require.NoError(t, err)
	got := r.Resolve("acme", "acme/platform")
	require.Equal(t, quotas.ScopeRepo, got.Scope)
	require.Equal(t, "acme/platform", got.Name)
	require.Equal(t, 5, got.Cap)
}

func TestResolve_DefaultPerOrgWhenNoRepo(t *testing.T) {
	t.Parallel()
	r, err := quotas.New(quotas.Config{DefaultPerOrg: 20})
	require.NoError(t, err)
	got := r.Resolve("acme", "")
	require.Equal(t, quotas.ScopeOrg, got.Scope)
	require.Equal(t, "acme", got.Name)
	require.Equal(t, 20, got.Cap)
}

func TestResolve_RepoOverrideBeatsDefault(t *testing.T) {
	t.Parallel()
	r, err := quotas.New(quotas.Config{
		DefaultPerRepo: 5,
		Overrides: []quotas.Override{
			{Repo: "acme/heavy-ci", MaxConcurrent: 15},
		},
	})
	require.NoError(t, err)

	heavy := r.Resolve("acme", "acme/heavy-ci")
	require.Equal(t, 15, heavy.Cap, "matched repo override")
	light := r.Resolve("acme", "acme/platform")
	require.Equal(t, 5, light.Cap, "unmatched repo falls back to default_per_repo")
}

func TestResolve_RepoOverrideBeatsOrgOverride(t *testing.T) {
	t.Parallel()
	r, err := quotas.New(quotas.Config{
		Overrides: []quotas.Override{
			{Org: "acme", MaxConcurrent: 30},
			{Repo: "acme/heavy-ci", MaxConcurrent: 15},
		},
	})
	require.NoError(t, err)

	heavy := r.Resolve("acme", "acme/heavy-ci")
	require.Equal(t, quotas.ScopeRepo, heavy.Scope)
	require.Equal(t, 15, heavy.Cap, "repo override wins over org override")
}

func TestResolve_OrgOverrideAppliesWhenNoRepoMatch(t *testing.T) {
	t.Parallel()
	r, err := quotas.New(quotas.Config{
		Overrides: []quotas.Override{{Org: "acme-platform", MaxConcurrent: 30}},
	})
	require.NoError(t, err)

	got := r.Resolve("acme-platform", "acme-platform/anything")
	require.Equal(t, quotas.ScopeOrg, got.Scope)
	require.Equal(t, "acme-platform", got.Name)
	require.Equal(t, 30, got.Cap)
}

func TestResolve_ZeroOverrideOptsOutEvenWithDefault(t *testing.T) {
	t.Parallel()
	// An override of 0 means "no cap for this scope, even though
	// there's a default" — useful for an internal-tooling repo
	// the operator wants uncapped.
	r, err := quotas.New(quotas.Config{
		DefaultPerRepo: 5,
		Overrides:      []quotas.Override{{Repo: "acme/internal", MaxConcurrent: 0}},
	})
	require.NoError(t, err)

	free := r.Resolve("acme", "acme/internal")
	require.Equal(t, quotas.ScopeRepo, free.Scope, "scope matches even when cap is 0")
	require.Equal(t, 0, free.Cap)
}

func TestResolve_NilResolverIsScopeNone(t *testing.T) {
	t.Parallel()
	var r *quotas.Resolver
	got := r.Resolve("acme", "acme/platform")
	require.Equal(t, quotas.ScopeNone, got.Scope)
}

func TestValidate_RejectsAmbiguousOverride(t *testing.T) {
	t.Parallel()
	_, err := quotas.New(quotas.Config{
		Overrides: []quotas.Override{{Org: "acme", Repo: "acme/platform", MaxConcurrent: 5}},
	})
	require.ErrorIs(t, err, quotas.ErrAmbiguousOverride)

	_, err = quotas.New(quotas.Config{
		Overrides: []quotas.Override{{MaxConcurrent: 5}},
	})
	require.ErrorIs(t, err, quotas.ErrAmbiguousOverride)
}

func TestValidate_RejectsNegative(t *testing.T) {
	t.Parallel()
	_, err := quotas.New(quotas.Config{DefaultPerRepo: -1})
	require.Error(t, err)

	_, err = quotas.New(quotas.Config{DefaultPerOrg: -1})
	require.Error(t, err)

	_, err = quotas.New(quotas.Config{
		Overrides: []quotas.Override{{Repo: "acme/x", MaxConcurrent: -1}},
	})
	require.Error(t, err)
}

func TestResolve_EmptyOrgAndRepoIsScopeNone(t *testing.T) {
	t.Parallel()
	r, err := quotas.New(quotas.Config{DefaultPerRepo: 5, DefaultPerOrg: 10})
	require.NoError(t, err)
	got := r.Resolve("", "")
	require.Equal(t, quotas.ScopeNone, got.Scope)
}
