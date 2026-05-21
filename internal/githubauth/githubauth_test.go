package githubauth_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/actions/scaleset"
	"github.com/stretchr/testify/require"

	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/githubauth"
)

// fakePEM is a syntactically valid PEM that scaleset only inspects when it
// goes to sign a JWT — never reached by the construction-only paths
// exercised here.
const fakePEM = `-----BEGIN RSA PRIVATE KEY-----
MIIBOQIBAAJBAKj34GkxFhD90vcNLYLInFEX6Ppy1tPf9Cnzj4p4WGeKLs1Pt8Qu
KUpRKfFLfRYC9AIKjbJTWit+CqvjWYzvQwECAwEAAQJAIJLixBy2qpFoS4DSmoEm
o3qGy0t6z09AIJtH+5OeRV1be+N4cDYJKffGzDa88vQENZiRm0GRq6a+HPGQMd2k
TQIhAKMSvzIBnni7ot/OSie2TmJLY4SwTQAevXysE2RbFDYdAiEBCUEaRQnMnbp7
9mxDXDf6AU0cN/RPBjb9qSHDcWZHGzUCIG2Es59z8ugGrDY+pxLQnwfotadxd+Uy
v/Ow5T0q5gIJAiEAyS4RaI9YG8EWx/2w0T67ZUVAw8eOMB6BIUg0Xcu+3okCIBOs
/5OiPgoTdSy7bcF9IGpSE8ZgGKzgYQVZeN97YE00
-----END RSA PRIVATE KEY-----`

var validSystemInfo = scaleset.SystemInfo{
	System:    "scaleset-test",
	Version:   "0.0.0",
	CommitSHA: "test",
}

func TestScope_Validate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		scope   githubauth.Scope
		wantErr bool
	}{
		{"org-only", githubauth.Scope{Org: "octocat"}, false},
		{"repo-only", githubauth.Scope{Repo: "octocat/hello-world"}, false},
		{"both", githubauth.Scope{Org: "a", Repo: "a/b"}, true},
		{"neither", githubauth.Scope{}, true},
		{"bad-repo-no-slash", githubauth.Scope{Repo: "octocat"}, true},
		{"bad-repo-empty-half", githubauth.Scope{Repo: "octocat/"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.scope.Validate()
			if c.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestScope_URL(t *testing.T) {
	t.Parallel()
	require.Equal(t, "https://github.com/octocat", githubauth.Scope{Org: "octocat"}.URL())
	require.Equal(t, "https://github.com/octocat/hello-world", githubauth.Scope{Repo: "octocat/hello-world"}.URL())
}

func TestNewPAT_Validates(t *testing.T) {
	t.Parallel()
	_, err := githubauth.NewPAT("")
	require.Error(t, err)

	auth, err := githubauth.NewPAT("ghp_test")
	require.NoError(t, err)
	require.NotNil(t, auth)
}

func TestNewApp_Validates(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name           string
		clientID       string
		installationID int64
		pem            []byte
		wantErr        bool
	}{
		{"happy", "Iv23likB94", 1234, []byte(fakePEM), false},
		{"empty-client-id", "", 1, []byte(fakePEM), true},
		{"zero-installation", "Iv23", 0, []byte(fakePEM), true},
		{"empty-pem", "Iv23", 1, nil, true},
		{"garbage-pem", "Iv23", 1, []byte("not a pem"), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := githubauth.NewApp(c.clientID, c.installationID, c.pem)
			if c.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewAppFromFile_ReadsPEM(t *testing.T) {
	t.Parallel()
	p := filepath.Join(t.TempDir(), "app.pem")
	require.NoError(t, os.WriteFile(p, []byte(fakePEM), 0o600))

	a, err := githubauth.NewAppFromFile("Iv23", 1, p)
	require.NoError(t, err)
	require.NotNil(t, a)

	// Missing file → error.
	_, err = githubauth.NewAppFromFile("Iv23", 1, filepath.Join(t.TempDir(), "missing.pem"))
	require.Error(t, err)
}

// TestPAT_NewScaleSetClient_BuildsWithoutNetwork verifies the PAT path
// produces a *scaleset.Client without making any HTTP requests (the
// constructor only configures internal state; the first network hit happens
// when the listener runs).
func TestPAT_NewScaleSetClient_BuildsWithoutNetwork(t *testing.T) {
	t.Parallel()
	a, err := githubauth.NewPAT("ghp_test")
	require.NoError(t, err)

	c, err := a.NewScaleSetClient(context.Background(),
		githubauth.Scope{Org: "octocat"}, validSystemInfo)
	require.NoError(t, err)
	require.NotNil(t, c)
}

func TestPAT_NewScaleSetClient_RejectsInvalidScope(t *testing.T) {
	t.Parallel()
	a, err := githubauth.NewPAT("ghp_test")
	require.NoError(t, err)

	_, err = a.NewScaleSetClient(context.Background(), githubauth.Scope{}, validSystemInfo)
	require.Error(t, err)
}

// fakeObserver captures the calls the rate-limit transport makes so the
// test can assert label values without standing up a Prometheus registry.
type fakeObserver struct {
	mu        sync.Mutex
	remaining []int
	calls     []struct{ endpoint, status string }
}

func (f *fakeObserver) ObserveRateLimit(remaining int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.remaining = append(f.remaining, remaining)
}

func (f *fakeObserver) ObserveCall(endpoint, status string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, struct{ endpoint, status string }{endpoint, status})
}

// TestPAT_NewRESTClient_HitsBaseURL drives a github.Client built by the
// PAT auth against an httptest server. Verifies (a) the Authorization
// header is set on the request, (b) the rate-limit middleware records the
// X-RateLimit-Remaining header into the observer, and (c) endpoint
// bucketing maps the path correctly.
func TestPAT_NewRESTClient_HitsBaseURL(t *testing.T) {
	t.Parallel()
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("X-RateLimit-Remaining", "4321")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"total_count": 0, "runners": []}`))
	}))
	t.Cleanup(srv.Close)

	a, err := githubauth.NewPAT("ghp_secret123")
	require.NoError(t, err)

	obs := &fakeObserver{}
	cli, err := a.NewRESTClient(context.Background(), githubauth.WithRateLimitMetrics(obs))
	require.NoError(t, err)

	base, err := cli.BaseURL.Parse(srv.URL + "/")
	require.NoError(t, err)
	cli.BaseURL = base

	_, _, err = cli.Actions.ListRunners(context.Background(), "octocat", "repo", nil)
	require.NoError(t, err)

	require.True(t, strings.HasPrefix(gotAuth, "Bearer "), "PAT auth must use Bearer scheme: %q", gotAuth)
	require.Contains(t, gotAuth, "ghp_secret123")

	obs.mu.Lock()
	defer obs.mu.Unlock()
	require.Equal(t, []int{4321}, obs.remaining)
	require.Len(t, obs.calls, 1)
	require.Equal(t, "runners", obs.calls[0].endpoint)
	require.Equal(t, "2xx", obs.calls[0].status)
}

// TestApp_NewRESTClient_RejectsNonNumericClientID guards the path where
// ghinstallation is given a new-style Client ID it can't parse. We surface
// this as a clear error instead of letting it explode at first call.
func TestApp_NewRESTClient_RejectsNonNumericClientID(t *testing.T) {
	t.Parallel()
	a, err := githubauth.NewApp("Iv23abc", 1234, []byte(fakePEM))
	require.NoError(t, err)

	_, err = a.NewRESTClient(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "numeric app_id")
}
