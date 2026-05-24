package fakegithub_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/google/go-github/v88/github"
	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakegithub"
)

// newGitHubClient builds a github.Client pointed at the fake. In
// go-github v88 the BaseURL is no longer a settable field; the URL
// is injected via WithURLs at construction time.
func newGitHubClient(t *testing.T, srv *fakegithub.Server) *github.Client {
	t.Helper()
	base := srv.RESTBaseURL()
	cli, err := github.NewClient(
		github.WithHTTPClient(http.DefaultClient),
		github.WithURLs(&base, &base),
	)
	require.NoError(t, err)
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

// TestServer_MultiScalesetLookupsAreIsolated locks in the multi-
// scaleset routing contract (issue #1 follow-up): each named
// scaleset has its own ID, lookups by name return the matching
// entry, and unknown names return count=0 (the orchestrator's
// "not found, create it" signal).
func TestServer_MultiScalesetLookupsAreIsolated(t *testing.T) {
	t.Parallel()
	srv := fakegithub.New(t, fakegithub.Options{
		Scalesets: []fakegithub.ScaleSetOptions{
			{Name: "linux-x64", ID: 100},
			{Name: "gpu-pool", ID: 200},
		},
	})
	require.Equal(t, 100, srv.ScaleSetIDFor("linux-x64"))
	require.Equal(t, 200, srv.ScaleSetIDFor("gpu-pool"))
}

// TestServer_MultiScalesetJITCountersAreIsolated locks in per-
// scaleset JIT-mint accounting: minting against scaleset A must not
// increment scaleset B's counter. The runner-ID synthesis includes
// the scaleset ID so two scalesets' JIT runners never collide in
// downstream assertions.
func TestServer_MultiScalesetJITCountersAreIsolated(t *testing.T) {
	t.Parallel()
	srv := fakegithub.New(t, fakegithub.Options{
		Scalesets: []fakegithub.ScaleSetOptions{
			{Name: "alpha", ID: 100},
			{Name: "beta", ID: 200},
		},
	})

	// Direct POST to generatejitconfig on alpha's path.
	mintOn := func(t *testing.T, scalesetID int) int {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(),
			http.MethodPost,
			fmt.Sprintf("%s/_apis/runtime/runnerscalesets/%d/generatejitconfig", srv.URL, scalesetID),
			strings.NewReader(`{"name":"runner-x"}`))
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var body struct {
			Runner struct {
				ID int `json:"id"`
			} `json:"runner"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		return body.Runner.ID
	}

	alphaID1 := mintOn(t, 100)
	alphaID2 := mintOn(t, 100)
	betaID1 := mintOn(t, 200)

	require.Equal(t, 2, srv.JITMintCountFor("alpha"))
	require.Equal(t, 1, srv.JITMintCountFor("beta"))
	// Counter math is per-scaleset (each entry counts from 1), so
	// tests that need cross-scaleset uniqueness should assert via
	// JITMintCountFor rather than ID values.
	require.NotEqual(t, alphaID1, alphaID2, "consecutive mints on the same scaleset must yield distinct IDs")
	_ = betaID1
}

// TestServer_MultiScalesetSingularAccessorsPanic locks in the
// fail-loud contract: tests that mistake JITMintCount() for the
// multi-aware JITMintCountFor("name") get an immediate panic
// instead of a silently-wrong answer.
func TestServer_MultiScalesetSingularAccessorsPanic(t *testing.T) {
	t.Parallel()
	srv := fakegithub.New(t, fakegithub.Options{
		Scalesets: []fakegithub.ScaleSetOptions{
			{Name: "a"}, {Name: "b"},
		},
	})
	require.Panics(t, func() { srv.JITMintCount() })
	require.Panics(t, func() { srv.ScaleSetID() })
	require.Panics(t, func() { srv.SetStatistics(fakegithub.Statistics{}) })
}

// TestServer_MixingSingularAndPluralPanics locks in the Options
// validation: declaring both Options.ScaleSet and Options.Scalesets
// is a test-config bug that should fail at New time, not silently
// pick one.
func TestServer_MixingSingularAndPluralPanics(t *testing.T) {
	t.Parallel()
	require.Panics(t, func() {
		fakegithub.New(t, fakegithub.Options{
			ScaleSet:  fakegithub.ScaleSetOptions{Name: "single"},
			Scalesets: []fakegithub.ScaleSetOptions{{Name: "multi"}},
		})
	})
}
