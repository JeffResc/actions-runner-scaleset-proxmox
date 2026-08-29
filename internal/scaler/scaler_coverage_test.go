package scaler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/pool"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakegithub"
)

// newScalesetClient builds a *scaleset.Client wired to a fakegithub
// server so the real provisionOne / cleanupStaleRunnerByName paths can be
// exercised (the scaler holds a concrete *scaleset.Client, so there's no
// interface seam to stub). Retries are disabled to keep the tests fast
// and deterministic.
func newScalesetClient(t *testing.T, fg *fakegithub.Server, org string) *scaleset.Client {
	t.Helper()
	cli, err := scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{
		GitHubConfigURL:     fg.ConfigURL(org),
		PersonalAccessToken: "ghp_test",
		SystemInfo:          scaleset.SystemInfo{System: "t", Version: "0", CommitSHA: "t"},
	}, scaleset.WithRetryMax(0))
	require.NoError(t, err)
	return cli
}

// TestProvisionOne_SetRunnerIDFailureIsNonFatal pins the swallowed-error
// contract in provisionOne (#358): when pool.SetRunnerID fails, the
// provision must still complete successfully — the JIT config was already
// minted and injected, so the VM is delivered to GitHub and must NOT be
// released. The error is logged, not returned.
//
// NOTE(#358): documents current behavior; possible bug — swallowing the
// SetRunnerID error leaves the store row without a runner_id. If that row
// is destroyed before the gh.Reconciler observes the runner (a sub-poll-
// interval job), OnRunnerOrphaned has nothing to deregister and the
// GitHub-side registration leaks until the orphan-runner sweep reaps it.
// The production comment accepts this trade-off; this test locks in the
// non-fatal behaviour so a regression to "return the error" (which would
// wrongly release a delivered VM) is caught.
func TestProvisionOne_SetRunnerIDFailureIsNonFatal(t *testing.T) {
	t.Parallel()
	fg := fakegithub.New(t, fakegithub.Options{})
	cli := newScalesetClient(t, fg, "octocat")

	fp := &fakePool{setRunnerIDErr: errors.New("store: row not found")}
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	s := New(Config{
		ScaleSetID:   fg.ScaleSetID(),
		ScaleSetName: "test",
		NamePrefix:   "gh-runner-test-",
		WorkFolder:   "_work",
	}, cli, fp, stubProvForScaler{}, log, nil)

	ok := s.provisionOne(context.Background(), &pool.VM{
		VMID: 10042, Node: "pve1", Name: "gh-runner-test-10042",
	})

	require.True(t, ok,
		"a swallowed SetRunnerID error must not fail the provision — the VM is already minted+injected")
	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.Equal(t, []markedRunningCall{{VMID: 10042, RunnerID: 100001}}, fp.setRunnerIDCalls,
		"SetRunnerID must have been attempted with the minted runner id before injection")
	require.Empty(t, fp.markedCompleted,
		"a delivered VM must NOT be released via MarkCompleted just because SetRunnerID failed")
}

// TestCleanupStaleRunnerByName_UsesOwnBoundedTimeout pins the detached-
// context contract (#358): cleanupStaleRunnerByName takes no caller ctx —
// it builds its own context.WithTimeout(context.Background(), ...) so a
// cancelled listener ctx can never abort the idempotent GitHub deregister
// and leave a stale registration behind. This test proves the "own
// timeout" half: pointed at a GitHub API whose handshake hangs forever,
// the call must still return promptly (bounded by staleRunnerCleanupTimeout)
// rather than blocking on the OS-level connect/read timeout.
func TestCleanupStaleRunnerByName_UsesOwnBoundedTimeout(t *testing.T) {
	// Serial: mutates the package-level staleRunnerCleanupTimeout var.
	orig := staleRunnerCleanupTimeout
	staleRunnerCleanupTimeout = 200 * time.Millisecond
	t.Cleanup(func() { staleRunnerCleanupTimeout = orig })

	// A server that hangs every request until its context fires. The
	// scaleset client's first handshake call (registration-token) never
	// gets a response, so only the client's own request-context timeout
	// (derived from staleRunnerCleanupTimeout) can unblock it.
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	cli, err := scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{
		GitHubConfigURL:     srv.URL + "/octocat",
		PersonalAccessToken: "ghp_test",
		SystemInfo:          scaleset.SystemInfo{System: "t", Version: "0", CommitSHA: "t"},
	}, scaleset.WithRetryMax(0))
	require.NoError(t, err)

	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	s := New(Config{ScaleSetName: "test", NamePrefix: "gh-runner-test-"}, cli, &fakePool{}, stubProvForScaler{}, log, nil)

	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		s.cleanupStaleRunnerByName("gh-runner-test-10042")
	}()

	select {
	case <-done:
		require.Less(t, time.Since(start), 3*time.Second,
			"cleanupStaleRunnerByName must return within its own timeout, not the OS connect/read timeout")
	case <-time.After(5 * time.Second):
		t.Fatal("cleanupStaleRunnerByName blocked past its own bounded timeout — detached-context timeout regressed")
	}
}
