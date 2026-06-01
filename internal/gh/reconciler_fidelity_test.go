package gh

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/githubauth"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakegithub"
)

// These tests exercise the reconciler's list pagination and rate-limit
// retry paths against the higher-fidelity fakegithub (#328): before the
// fake gained pagination + injectable 429s, those code paths were never
// driven by any test.

func TestListRunnersByPrefix_FollowsPagination(t *testing.T) {
	t.Parallel()
	fg := fakegithub.New(t, fakegithub.Options{
		PageSize: 2, // force multi-page responses
		InitialRunners: []fakegithub.Runner{
			{ID: 1, Name: "gh-runner-test-1", Status: "online"},
			{ID: 2, Name: "gh-runner-test-2", Status: "online"},
			{ID: 3, Name: "gh-runner-test-3", Status: "online"},
			{ID: 4, Name: "gh-runner-test-4", Status: "online"},
			{ID: 5, Name: "gh-runner-test-5", Status: "online"},
			{ID: 9, Name: "unrelated-runner", Status: "online"}, // filtered by prefix
		},
	})

	cli := newTestClient(t, fg.Server)
	out, err := ListRunnersByPrefix(context.Background(), cli,
		githubauth.Scope{Org: "octocat"}, "gh-runner-test-", silentLogger())
	require.NoError(t, err)
	require.Len(t, out, 5, "all 5 prefixed runners must be collected across pages; the unrelated one is filtered")
	for i := 1; i <= 5; i++ {
		require.Contains(t, out, "gh-runner-test-"+itoa64(int64(i)))
	}
}

func TestListRunnersByPrefix_RetriesPastRateLimit(t *testing.T) {
	t.Parallel()
	fg := fakegithub.New(t, fakegithub.Options{
		InitialRunners: []fakegithub.Runner{
			{ID: 1, Name: "gh-runner-test-1", Status: "online"},
		},
	})
	// First two list calls get a 429 + Retry-After; the per-page retry
	// budget (3 tries) absorbs them and the third succeeds.
	fg.InjectListFailure(429, 1, 2)

	cli := newTestClient(t, fg.Server)
	out, err := ListRunnersByPrefix(context.Background(), cli,
		githubauth.Scope{Org: "octocat"}, "gh-runner-test-", silentLogger())
	require.NoError(t, err, "the reconciler must retry past a transient 429 burst")
	require.Len(t, out, 1)
}

func TestRemoveRunner_SurfacesInjectedDeregisterFailure(t *testing.T) {
	t.Parallel()
	fg := fakegithub.New(t, fakegithub.Options{
		InitialRunners: []fakegithub.Runner{{ID: 7, Name: "gh-runner-test-7", Status: "online"}},
	})
	// The next deregister fails — this is the fault that makes the
	// destroy path's OnRunnerOrphaned leaked-registration branch
	// reachable (#327).
	fg.InjectDeleteFailure(500, 1)

	cli := newTestClient(t, fg.Server)
	_, err := cli.Actions.RemoveOrganizationRunner(context.Background(), "octocat", 7)
	require.Error(t, err, "an injected deregister failure must surface as an error to the caller")

	// And it recovers on the next attempt (fault is count-bounded).
	_, err = cli.Actions.RemoveOrganizationRunner(context.Background(), "octocat", 7)
	require.NoError(t, err, "the count-bounded delete fault must clear after its budget is spent")
}

// itoa64 is a tiny strconv-free helper for the table assertions above.
func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
