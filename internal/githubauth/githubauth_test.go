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
	"time"

	"github.com/actions/scaleset"
	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/githubauth"
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

// TestNewAppFromFile_RejectsLoosePerms locks in the mode-bit half of
// the PEM hardening (#148): any bit in 0o077 (group / other) must be
// rejected by NewAppFromFile. The underlying check moved into
// fileperm.CheckMode (where it has its own unit tests), but the
// integration through NewAppFromFile was previously untested — every
// existing test wrote 0o600.
//
// kubectl-mounted secrets and ConfigMap-projected files commonly land
// at 0o644 by default; this is the most likely operator misconfig and
// the most important failure mode to lock in.
func TestNewAppFromFile_RejectsLoosePerms(t *testing.T) {
	t.Parallel()
	cases := []os.FileMode{
		0o644, // group + world readable (most common kubectl default)
		0o640, // group readable
		0o604, // world readable
		0o660, // group writable
	}
	for _, mode := range cases {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			p := filepath.Join(t.TempDir(), "app.pem")
			require.NoError(t, os.WriteFile(p, []byte(fakePEM), 0o600))
			// Force mode via Chmod — os.WriteFile honors umask which
			// would otherwise tighten 0o644 down to 0o600 under a 022
			// umask.
			require.NoError(t, os.Chmod(p, mode))

			_, err := githubauth.NewAppFromFile("Iv23", 1, p)
			require.Error(t, err, "PEM at mode %#o must be rejected", mode)
			require.Contains(t, err.Error(), "insecure mode")
		})
	}
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

	a, err := githubauth.NewPATWithConfig(githubauth.PATConfig{
		Token:       "ghp_secret123",
		RESTBaseURL: srv.URL + "/",
	})
	require.NoError(t, err)

	obs := &fakeObserver{}
	cli, err := a.NewRESTClient(context.Background(), githubauth.WithRateLimitMetrics(obs))
	require.NoError(t, err)

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

// TestNewPATWithConfig_Validates exercises the new constructor's input
// validation: empty token, malformed REST base URL, and the trailing-slash
// requirement go-github imposes on BaseURL.
func TestNewPATWithConfig_Validates(t *testing.T) {
	t.Parallel()

	_, err := githubauth.NewPATWithConfig(githubauth.PATConfig{})
	require.Error(t, err, "empty token must error")

	_, err = githubauth.NewPATWithConfig(githubauth.PATConfig{
		Token:       "ghp_test",
		RESTBaseURL: "http://example.test", // missing trailing slash
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "must end with /")

	_, err = githubauth.NewPATWithConfig(githubauth.PATConfig{
		Token:       "ghp_test",
		RESTBaseURL: "http://example.test/",
	})
	require.NoError(t, err)
}

// TestPAT_RESTBaseURLOverride proves the constructor's RESTBaseURL field
// causes outbound REST calls to land on the override host without the
// caller having to patch cli.BaseURL manually.
func TestPAT_RESTBaseURLOverride(t *testing.T) {
	t.Parallel()
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"total_count": 0, "runners": []}`))
	}))
	t.Cleanup(srv.Close)

	a, err := githubauth.NewPATWithConfig(githubauth.PATConfig{
		Token:       "ghp_test",
		RESTBaseURL: srv.URL + "/",
	})
	require.NoError(t, err)

	cli, err := a.NewRESTClient(context.Background())
	require.NoError(t, err)
	require.Equal(t, srv.URL+"/", cli.BaseURL())

	_, _, err = cli.Actions.ListRunners(context.Background(), "octocat", "repo", nil)
	require.NoError(t, err)
	require.Equal(t, 1, hits, "REST call must land on the override host")
}

// TestPAT_ConfigURLOverride confirms a custom ConfigURL replaces
// scope.URL() inside NewScaleSetClient by observing that the first
// scaleset API call lands on the override host instead of github.com.
// The scaleset client's request flow does an unauthenticated discovery
// hit first — that's the one we watch for. We don't care if the call
// ultimately errors (the fake doesn't respond like the real Actions
// service); we only care that the host was the one we configured.
func TestPAT_ConfigURLOverride(t *testing.T) {
	t.Parallel()
	var hit bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hit = true
		http.Error(w, "stub", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	a, err := githubauth.NewPATWithConfig(githubauth.PATConfig{
		Token:     "ghp_test",
		ConfigURL: srv.URL + "/octocat",
	})
	require.NoError(t, err)

	c, err := a.NewScaleSetClient(context.Background(),
		githubauth.Scope{Org: "octocat"}, validSystemInfo)
	require.NoError(t, err)
	require.NotNil(t, c)

	// Trigger a request with a tight deadline (the scaleset library
	// retries internally; we don't need to wait for it to give up).
	// We don't care about the outcome, only that the outbound HTTP
	// landed on our override host.
	callCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = c.GetRunnerScaleSet(callCtx, 1, "any")
	require.True(t, hit, "ConfigURL override must route scaleset client traffic to the configured host")
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

// TestScope_PathSegment locks in the helper exposed for GHES
// multi-scaleset URL composition (issue #214). PathSegment returns
// "<org>" for an org scope and "<owner>/<repo>" for a repo scope —
// the suffix the scaleset library appends to its config base URL.
func TestScope_PathSegment(t *testing.T) {
	t.Parallel()
	require.Equal(t, "myorg", githubauth.Scope{Org: "myorg"}.PathSegment())
	require.Equal(t, "owner/repo", githubauth.Scope{Repo: "owner/repo"}.PathSegment())
}

// TestNewPATWithConfig_RejectsConfigURLPlusConfigBaseURL locks in
// the mutual-exclusion rule. The two override modes have different
// per-scope behaviour and silently picking one would be a footgun.
func TestNewPATWithConfig_RejectsConfigURLPlusConfigBaseURL(t *testing.T) {
	t.Parallel()
	_, err := githubauth.NewPATWithConfig(githubauth.PATConfig{
		Token:         "ghp_test",
		ConfigURL:     "https://ghes.example.com/myorg",
		ConfigBaseURL: "https://ghes.example.com",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "mutually exclusive")
}

// TestNewPATWithConfig_RejectsConfigBaseURLWithoutScheme locks in
// the schema requirement: ConfigBaseURL must be parseable as a full
// "scheme://host" URL so the join-with-scope.PathSegment() produces
// a sensible result. "ghes.example.com" alone would silently
// produce "/<org>" which the scaleset library would treat as a
// relative path and explode on.
func TestNewPATWithConfig_RejectsConfigBaseURLWithoutScheme(t *testing.T) {
	t.Parallel()
	_, err := githubauth.NewPATWithConfig(githubauth.PATConfig{
		Token:         "ghp_test",
		ConfigBaseURL: "ghes.example.com",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "scheme and host")
}

// TestPAT_ConfigBaseURL_PerScopeRouting is the core regression
// guard for issue #214. With ConfigBaseURL set, two distinct scopes
// must produce two distinct outbound config URLs (org-a and org-b
// must each hit their own /<org> path on the same host). The
// previous ConfigURL-only behaviour would have routed both to the
// same configured URL.
func TestPAT_ConfigBaseURL_PerScopeRouting(t *testing.T) {
	t.Parallel()
	var (
		mu    sync.Mutex
		paths []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		http.Error(w, "stub", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	a, err := githubauth.NewPATWithConfig(githubauth.PATConfig{
		Token:         "ghp_test",
		ConfigBaseURL: srv.URL,
	})
	require.NoError(t, err)

	for _, org := range []string{"org-a", "org-b"} {
		c, err := a.NewScaleSetClient(context.Background(),
			githubauth.Scope{Org: org}, validSystemInfo)
		require.NoError(t, err)
		require.NotNil(t, c)
		callCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = c.GetRunnerScaleSet(callCtx, 1, "any")
		cancel()
	}

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, paths, "expected scaleset client to hit the configured base URL at least once per scope")
	gotOrgA, gotOrgB := false, false
	for _, p := range paths {
		if strings.Contains(p, "org-a") {
			gotOrgA = true
		}
		if strings.Contains(p, "org-b") {
			gotOrgB = true
		}
	}
	require.True(t, gotOrgA, "no request observed for org-a; got paths %v", paths)
	require.True(t, gotOrgB, "no request observed for org-b; got paths %v", paths)
}
