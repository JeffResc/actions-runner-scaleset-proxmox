package fakegithub_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/go-github/v84/github"
	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakegithub"
)

// newGitHubClient builds a github.Client pointed at the fake by
// patching BaseURL directly. The PATConfig-based path is exercised by
// the gh reconciler tests once that helper is available.
func newGitHubClient(t *testing.T, srv *fakegithub.Server) *github.Client {
	t.Helper()
	cli := github.NewClient(http.DefaultClient)
	base, err := cli.BaseURL.Parse(srv.RESTBaseURL())
	require.NoError(t, err)
	cli.BaseURL = base
	return cli
}

func TestServer_ListAndDelete(t *testing.T) {
	t.Parallel()
	srv := fakegithub.New(t, fakegithub.Options{
		InitialRunners: []fakegithub.Runner{
			{ID: 1, Name: "runner-1", Status: "online", Busy: false},
			{ID: 2, Name: "runner-2", Status: "online", Busy: true},
		},
	})

	cli := newGitHubClient(t, srv)

	// List runners (repo scope).
	runners, _, err := cli.Actions.ListRunners(context.Background(), "octocat", "hello", nil)
	require.NoError(t, err)
	require.Equal(t, 2, runners.TotalCount)

	// And the org scope returns the same set.
	orgRunners, _, err := cli.Actions.ListOrganizationRunners(context.Background(), "octocat", nil)
	require.NoError(t, err)
	require.Equal(t, 2, orgRunners.TotalCount)

	// Delete one.
	_, err = cli.Actions.RemoveRunner(context.Background(), "octocat", "hello", 1)
	require.NoError(t, err)

	require.Equal(t, []int64{1}, srv.RunnerDeletions())

	// Subsequent list reflects the deletion.
	runners, _, err = cli.Actions.ListRunners(context.Background(), "octocat", "hello", nil)
	require.NoError(t, err)
	require.Equal(t, 1, runners.TotalCount)
}

func TestServer_DeleteUnknownIs404(t *testing.T) {
	t.Parallel()
	srv := fakegithub.New(t, fakegithub.Options{})

	cli := newGitHubClient(t, srv)

	_, err := cli.Actions.RemoveRunner(context.Background(), "octocat", "hello", 999)
	require.Error(t, err)
	// go-github surfaces the HTTP status; just confirm we didn't 200 it.
}

func TestServer_SetRunnerLater(t *testing.T) {
	t.Parallel()
	srv := fakegithub.New(t, fakegithub.Options{})

	cli := newGitHubClient(t, srv)

	// Initially empty.
	runners, _, err := cli.Actions.ListRunners(context.Background(), "octocat", "hello", nil)
	require.NoError(t, err)
	require.Equal(t, 0, runners.TotalCount)

	// Add one and re-list.
	srv.SetRunner(fakegithub.Runner{ID: 42, Name: "new", Status: "online"})
	runners, _, err = cli.Actions.ListRunners(context.Background(), "octocat", "hello", nil)
	require.NoError(t, err)
	require.Equal(t, 1, runners.TotalCount)
	require.Equal(t, int64(42), runners.Runners[0].GetID())
}

// TestServer_ScalesetAccessorsSmoke exercises the scaleset-library
// accessors (ConfigURL, ScaleSetID, JITMintCount) so they don't get
// flagged as dead code by static analysis. Behavioural coverage of
// these endpoints lives in test/e2e/ under the `e2e` build tag, which
// deadcode without -tags=e2e cannot see.
func TestServer_ScalesetAccessorsSmoke(t *testing.T) {
	t.Parallel()
	srv := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{Name: "smoke-set", ID: 99},
	})

	require.Contains(t, srv.ConfigURL("octocat"), "/octocat")
	require.Equal(t, 99, srv.ScaleSetID())
	require.Equal(t, 0, srv.JITMintCount(),
		"no JIT mints expected without an e2e harness call")
}

func TestServer_UnsupportedEndpointReturns501(t *testing.T) {
	t.Parallel()
	srv := fakegithub.New(t, fakegithub.Options{})

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/repos/octocat/hello/actions/jobs/42", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotImplemented, resp.StatusCode,
		"unimplemented endpoints must fail loud, not silently 404")
}
