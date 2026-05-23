// Package githubauth wraps the two authentication modes supported by
// github.com/actions/scaleset (GitHub App and personal access token) behind
// a single interface so the rest of the orchestrator can be agnostic about
// which auth flow is in use.
package githubauth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/actions/scaleset"
	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/google/go-github/v84/github"
)

// Scope identifies the GitHub registration target. Exactly one of Org or
// Repo must be set; this is checked by Validate.
type Scope struct {
	// Org is a GitHub organization slug, e.g. "my-org".
	Org string
	// Repo is "owner/repo".
	Repo string
}

// Validate returns nil iff exactly one of Org or Repo is set and Repo (if
// set) is in owner/repo form.
func (s Scope) Validate() error {
	hasOrg, hasRepo := s.Org != "", s.Repo != ""
	switch {
	case hasOrg && hasRepo:
		return errors.New("scope: exactly one of org or repo must be set (both present)")
	case !hasOrg && !hasRepo:
		return errors.New("scope: exactly one of org or repo must be set (both empty)")
	}
	if hasRepo {
		parts := strings.Split(s.Repo, "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return fmt.Errorf("scope: repo %q must be in owner/repo format", s.Repo)
		}
	}
	return nil
}

// URL returns the GitHub config URL the scaleset library expects.
func (s Scope) URL() string {
	if s.Org != "" {
		return "https://github.com/" + s.Org
	}
	return "https://github.com/" + s.Repo
}

// Auth is implemented by both auth modes. Callers invoke NewScaleSetClient
// once per scale set; the returned client owns its credentials and refreshes
// installation tokens (for App auth) automatically. NewRESTClient yields a
// generic GitHub REST client (go-github) for orchestrator components that
// need APIs the scaleset library doesn't expose — runner listing, queued
// workflow run inspection, audit log, etc.
type Auth interface {
	NewScaleSetClient(
		ctx context.Context,
		scope Scope,
		sys scaleset.SystemInfo,
		opts ...scaleset.HTTPOption,
	) (*scaleset.Client, error)

	NewRESTClient(ctx context.Context, transportWraps ...TransportWrap) (*github.Client, error)
}

// TransportWrap is a middleware that decorates an http.RoundTripper. Used
// to layer rate-limit instrumentation, retry, logging, etc. onto the
// authenticated GitHub HTTP transport without each Auth impl needing to
// know about those concerns.
type TransportWrap func(http.RoundTripper) http.RoundTripper

// ---------------------------------------------------------------------------
// PAT
// ---------------------------------------------------------------------------

type patAuth struct {
	token       string
	configURL   string // empty = use scope.URL() — production default
	restBaseURL string // empty = use go-github default (https://api.github.com/)
}

// PATConfig configures a PAT-based [Auth] with optional base-URL overrides
// for both the scaleset client and the go-github REST client. The
// override fields are e2e-test hooks — production callers should use
// [NewPAT] which leaves both empty.
type PATConfig struct {
	// Token is the personal access token. Required.
	Token string

	// ConfigURL, when non-empty, replaces scope.URL() as the
	// GitHubConfigURL passed to scaleset.NewClientWithPersonalAccessToken.
	// Use this to point the scaleset listener at a fake GitHub server.
	ConfigURL string

	// RESTBaseURL, when non-empty, overrides the go-github client's
	// BaseURL. Must end with a trailing slash (go-github requirement);
	// the constructor enforces this. Use this to point the gh
	// reconciler at a fake GitHub REST server.
	RESTBaseURL string
}

// NewPATWithConfig constructs a PAT-based Auth with optional base-URL
// overrides. See [PATConfig] for field semantics. Production callers
// should prefer [NewPAT].
func NewPATWithConfig(c PATConfig) (Auth, error) {
	if c.Token == "" {
		return nil, errors.New("githubauth: PAT token is required")
	}
	if c.RESTBaseURL != "" {
		if _, err := url.Parse(c.RESTBaseURL); err != nil {
			return nil, fmt.Errorf("githubauth: rest base url %q: %w", c.RESTBaseURL, err)
		}
		if !strings.HasSuffix(c.RESTBaseURL, "/") {
			return nil, fmt.Errorf("githubauth: rest base url %q must end with /", c.RESTBaseURL)
		}
	}
	return &patAuth{
		token:       c.Token,
		configURL:   c.ConfigURL,
		restBaseURL: c.RESTBaseURL,
	}, nil
}

// NewPAT constructs a PAT-based Auth. The token must already be resolved
// (e.g. from an env var by the config package). For test scenarios that
// need to redirect to a fake GitHub server, use [NewPATWithConfig].
func NewPAT(token string) (Auth, error) {
	return NewPATWithConfig(PATConfig{Token: token})
}

func (p *patAuth) NewScaleSetClient(
	_ context.Context,
	scope Scope,
	sys scaleset.SystemInfo,
	opts ...scaleset.HTTPOption,
) (*scaleset.Client, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	configURL := p.configURL
	if configURL == "" {
		configURL = scope.URL()
	}
	return scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{
		GitHubConfigURL:     configURL,
		PersonalAccessToken: p.token,
		SystemInfo:          sys,
	}, opts...)
}

func (p *patAuth) NewRESTClient(_ context.Context, transportWraps ...TransportWrap) (*github.Client, error) {
	httpClient := &http.Client{Transport: applyWraps(http.DefaultTransport, transportWraps)}
	cli := github.NewClient(httpClient).WithAuthToken(p.token)
	if p.restBaseURL != "" {
		base, err := url.Parse(p.restBaseURL)
		if err != nil {
			return nil, fmt.Errorf("githubauth: rest base url %q: %w", p.restBaseURL, err)
		}
		cli.BaseURL = base
	}
	return cli, nil
}

// ---------------------------------------------------------------------------
// GitHub App
// ---------------------------------------------------------------------------

type appAuth struct {
	clientID       string
	installationID int64
	privateKeyPEM  string
}

// NewApp constructs an App-based Auth from a Client ID string. The scaleset
// library accepts either a Client ID (e.g. "Iv23..." style) or the numeric
// App ID as a string — callers with only the numeric ID should
// strconv.FormatInt it before calling.
func NewApp(clientID string, installationID int64, privateKeyPEM []byte) (Auth, error) {
	if clientID == "" {
		return nil, errors.New("githubauth: app client_id is required")
	}
	if installationID == 0 {
		return nil, errors.New("githubauth: app installation_id is required")
	}
	if len(privateKeyPEM) == 0 {
		return nil, errors.New("githubauth: app private key is required")
	}
	if !strings.Contains(string(privateKeyPEM), "BEGIN") {
		return nil, errors.New("githubauth: app private key does not look like PEM")
	}
	return &appAuth{
		clientID:       clientID,
		installationID: installationID,
		privateKeyPEM:  string(privateKeyPEM),
	}, nil
}

// NewAppFromFile reads the PEM file from disk and constructs an App auth.
// Refuses to read a key file with world- or group-readable permissions
// (any bit in 0o077) — the operator's deployment is misconfigured if
// a private key is accessible by other users on the box.
//
// Also refuses (on unix) when the PEM is owned by a UID other than the
// process's effective UID. mode 0600 alone is insufficient when the
// orchestrator runs as root (or with CAP_DAC_READ_SEARCH): a key
// dropped in by another user with mode 0600 would otherwise be
// silently trusted.
func NewAppFromFile(clientID string, installationID int64, pemPath string) (Auth, error) {
	if pemPath == "" {
		return nil, errors.New("githubauth: pem path is required")
	}
	info, err := os.Stat(pemPath)
	if err != nil {
		return nil, fmt.Errorf("githubauth: stat private key %s: %w", pemPath, err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		return nil, fmt.Errorf("githubauth: private key %s has insecure mode %#o; expected 0600 (chmod 600 the file)",
			pemPath, mode)
	}
	if err := checkPEMOwnership(info, pemPath); err != nil {
		return nil, err
	}
	pem, err := os.ReadFile(pemPath) // #nosec G304 -- pemPath is operator-supplied and perm-checked above.
	if err != nil {
		return nil, fmt.Errorf("githubauth: read private key %s: %w", pemPath, err)
	}
	return NewApp(clientID, installationID, pem)
}

func (a *appAuth) NewScaleSetClient(
	_ context.Context,
	scope Scope,
	sys scaleset.SystemInfo,
	opts ...scaleset.HTTPOption,
) (*scaleset.Client, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	return scaleset.NewClientWithGitHubApp(scaleset.ClientWithGitHubAppConfig{
		GitHubConfigURL: scope.URL(),
		GitHubAppAuth: scaleset.GitHubAppAuth{
			ClientID:       a.clientID,
			InstallationID: a.installationID,
			PrivateKey:     a.privateKeyPEM,
		},
		SystemInfo: sys,
	}, opts...)
}

func (a *appAuth) NewRESTClient(_ context.Context, transportWraps ...TransportWrap) (*github.Client, error) {
	// ghinstallation needs a numeric App ID (not the new-style Client ID
	// string). Most installations have both; the scaleset library accepts
	// either, but go-github's app-auth transport is strict about the int.
	appID, err := strconv.ParseInt(a.clientID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("githubauth: REST client needs numeric app_id (got client_id %q): %w", a.clientID, err)
	}
	itr, err := ghinstallation.New(http.DefaultTransport, appID, a.installationID, []byte(a.privateKeyPEM))
	if err != nil {
		return nil, fmt.Errorf("githubauth: app installation transport: %w", err)
	}
	httpClient := &http.Client{Transport: applyWraps(itr, transportWraps)}
	return github.NewClient(httpClient), nil
}

// ---------------------------------------------------------------------------
// Transport plumbing
// ---------------------------------------------------------------------------

// applyWraps composes a chain of TransportWraps onto a base RoundTripper.
// Wraps are applied in order so the FIRST wrap is the innermost (closest to
// the wire) — the same convention as net/http middleware chains.
func applyWraps(base http.RoundTripper, wraps []TransportWrap) http.RoundTripper {
	rt := base
	for _, w := range wraps {
		if w == nil {
			continue
		}
		rt = w(rt)
	}
	return rt
}

// RateLimitObserver is satisfied by anything that can record GitHub
// rate-limit telemetry. Implemented by *observability.Metrics via
// observability.NewRateLimitObserver — kept abstract here so this package
// doesn't pull in the Prometheus client.
type RateLimitObserver interface {
	ObserveRateLimit(remaining int)
	ObserveCall(endpoint, statusClass string)
}

// WithRateLimitMetrics returns a TransportWrap that records, for every
// outbound GitHub REST call, (a) the response status class and (b) the
// most recent X-RateLimit-Remaining header value. Construct via this
// helper rather than implementing your own observer to keep the
// instrumentation consistent across services.
func WithRateLimitMetrics(obs RateLimitObserver) TransportWrap {
	if obs == nil {
		return func(rt http.RoundTripper) http.RoundTripper { return rt }
	}
	return func(rt http.RoundTripper) http.RoundTripper {
		return &rateLimitTransport{base: rt, obs: obs}
	}
}

type rateLimitTransport struct {
	base http.RoundTripper
	obs  RateLimitObserver
}

func (t *rateLimitTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	endpoint := endpointGroup(req.URL.Path)
	if err != nil {
		t.obs.ObserveCall(endpoint, "transport_error")
		return resp, err
	}
	t.obs.ObserveCall(endpoint, statusClass(resp.StatusCode))
	if v := resp.Header.Get("X-RateLimit-Remaining"); v != "" {
		if n, perr := strconv.Atoi(v); perr == nil {
			t.obs.ObserveRateLimit(n)
		}
	}
	return resp, nil
}

// endpointGroup buckets request paths into low-cardinality labels for
// metrics. Avoid putting the raw path in a Prometheus label — IDs would
// explode cardinality and tank the TSDB.
func endpointGroup(path string) string {
	switch {
	case strings.Contains(path, "/actions/runners"):
		return "runners"
	case strings.Contains(path, "/actions/jobs"):
		return "jobs"
	case strings.Contains(path, "/actions/runs"):
		return "runs"
	case strings.Contains(path, "/rate_limit"):
		return "rate_limit"
	default:
		return "other"
	}
}

func statusClass(code int) string {
	switch {
	case code >= 200 && code < 300:
		return "2xx"
	case code >= 300 && code < 400:
		return "3xx"
	case code >= 400 && code < 500:
		return "4xx"
	case code >= 500:
		return "5xx"
	default:
		return "unknown"
	}
}
