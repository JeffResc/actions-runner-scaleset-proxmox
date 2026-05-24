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
	"github.com/google/go-github/v88/github"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/fileperm"
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
// Hardcodes github.com as the host; for GHES, callers must use
// [PATConfig.ConfigURL] (single-scaleset) or
// [PATConfig.ConfigBaseURL] (multi-scaleset, issue #214).
func (s Scope) URL() string {
	return "https://github.com/" + s.PathSegment()
}

// PathSegment returns the scope-identifying suffix the scaleset
// library appends to a base URL — "<org>" for org scope or
// "<owner>/<repo>" for repo scope. Callers building a GHES config
// URL join this onto their base host (e.g.
// "https://ghes.example.com/" + scope.PathSegment()).
func (s Scope) PathSegment() string {
	if s.Org != "" {
		return s.Org
	}
	return s.Repo
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

// endpointConfig holds the GHES / test-fake override knobs both
// auth modes share. PAT and App auth get the same per-scope URL
// resolution and REST base URL override — operators on GHES need
// the same three knobs regardless of credential mode (issue
// #214 + its App-side follow-up).
type endpointConfig struct {
	configURL     string // full URL: applies as-is regardless of scope (single-scope override)
	configBaseURL string // base URL (no path): joined per-call with scope.PathSegment() — multi-scaleset GHES (issue #214)
	restBaseURL   string // empty = use go-github default (https://api.github.com/)
}

// resolveScaleSetConfigURL derives the per-scope GitHubConfigURL
// from the endpoint config, in this precedence:
//
//  1. configURL — operator-supplied full URL (single-scope
//     override or test-only fake-server redirect). Returned
//     verbatim for every scope; multi-scaleset configs against
//     GHES MUST use configBaseURL instead so each scope produces
//     a distinct URL.
//  2. configBaseURL — operator-supplied scheme+host (GHES).
//     Joined with scope.PathSegment() per-call so each scope's
//     client handshakes against the right org/repo on the same
//     host (issue #214).
//  3. scope.URL() — the github.com default. Already per-scope by
//     virtue of scope.PathSegment(), so multi-scaleset works out
//     of the box.
func (e endpointConfig) resolveScaleSetConfigURL(scope Scope) string {
	if e.configURL != "" {
		return e.configURL
	}
	if e.configBaseURL != "" {
		// Trim trailing slash before joining so the result is
		// always "<scheme>://<host>/<path>" rather than
		// "...//<path>".
		return strings.TrimRight(e.configBaseURL, "/") + "/" + scope.PathSegment()
	}
	return scope.URL()
}

// validateEndpointConfig enforces the shared input rules: the two
// config-URL forms are mutually exclusive, ConfigBaseURL must be
// a parseable URL with scheme+host, and RESTBaseURL (if set) must
// end with the trailing slash go-github requires.
func validateEndpointConfig(configURL, configBaseURL, restBaseURL string) error {
	if configURL != "" && configBaseURL != "" {
		return errors.New("githubauth: config_url and config_base_url are mutually exclusive — set one or neither")
	}
	if configBaseURL != "" {
		u, err := url.Parse(configBaseURL)
		if err != nil {
			return fmt.Errorf("githubauth: config_base_url %q: %w", configBaseURL, err)
		}
		if u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("githubauth: config_base_url %q must include scheme and host (e.g. https://ghes.example.com)", configBaseURL)
		}
	}
	if restBaseURL != "" {
		if _, err := url.Parse(restBaseURL); err != nil {
			return fmt.Errorf("githubauth: rest base url %q: %w", restBaseURL, err)
		}
		if !strings.HasSuffix(restBaseURL, "/") {
			return fmt.Errorf("githubauth: rest base url %q must end with /", restBaseURL)
		}
	}
	return nil
}

type patAuth struct {
	token string
	endpointConfig
}

// PATConfig configures a PAT-based [Auth] with optional base-URL overrides
// for both the scaleset client and the go-github REST client. The
// override fields are e2e-test hooks and GHES-deployment hooks —
// production callers against github.com should use [NewPAT] which
// leaves all three empty.
type PATConfig struct {
	// Token is the personal access token. Required.
	Token string

	// ConfigURL, when non-empty, replaces scope.URL() verbatim as
	// the GitHubConfigURL passed to scaleset.NewClientWithPersonal-
	// AccessToken. Every per-scope NewScaleSetClient call gets the
	// same URL — appropriate for single-scaleset deployments and
	// for redirecting test clients at a fake GitHub server.
	//
	// Mutually exclusive with ConfigBaseURL — set one or neither.
	// Multi-scaleset GHES deployments must use ConfigBaseURL so
	// each per-scope client gets the right org/repo path (issue
	// #214).
	ConfigURL string

	// ConfigBaseURL, when non-empty, is treated as a scheme + host
	// only (the path component is ignored). NewScaleSetClient
	// constructs the per-scope GitHubConfigURL by joining
	// ConfigBaseURL with scope.PathSegment(). Required for multi-
	// scaleset deployments against GHES where each scaleset's
	// scope points at a different org/repo on the same host.
	//
	// Mutually exclusive with ConfigURL. For github.com leave both
	// empty — the default scope.URL() resolution already produces
	// the right per-scope URL.
	ConfigBaseURL string

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
	if err := validateEndpointConfig(c.ConfigURL, c.ConfigBaseURL, c.RESTBaseURL); err != nil {
		return nil, err
	}
	return &patAuth{
		token: c.Token,
		endpointConfig: endpointConfig{
			configURL:     c.ConfigURL,
			configBaseURL: c.ConfigBaseURL,
			restBaseURL:   c.RESTBaseURL,
		},
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
	return scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{
		GitHubConfigURL:     p.resolveScaleSetConfigURL(scope),
		PersonalAccessToken: p.token,
		SystemInfo:          sys,
	}, opts...)
}

func (p *patAuth) NewRESTClient(_ context.Context, transportWraps ...TransportWrap) (*github.Client, error) {
	httpClient := &http.Client{Transport: applyWraps(http.DefaultTransport, transportWraps)}
	opts := []github.ClientOptionsFunc{
		github.WithHTTPClient(httpClient),
		github.WithAuthToken(p.token),
	}
	if p.restBaseURL != "" {
		// go-github v88 dropped direct BaseURL assignment. Use WithURLs
		// (raw) rather than WithEnterpriseURLs — the latter silently
		// appends /api/v3/ and /api/uploads/ to non-".api."/non-"api."
		// hosts, which corrupts loopback and proxy URLs used in tests.
		base := p.restBaseURL
		opts = append(opts, github.WithURLs(&base, &base))
	}
	cli, err := github.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("githubauth: build rest client: %w", err)
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
	endpointConfig
}

// AppConfig configures an App-based [Auth] with optional base-URL
// overrides. Same shape as [PATConfig]'s override fields — operators
// on GHES need the same per-scope routing semantics regardless of
// whether they authenticate via PAT or GitHub App.
type AppConfig struct {
	// ClientID is the App Client ID string (newer Apps;
	// "Iv23..."-style) or a numeric App ID rendered as a string
	// (older Apps). Required.
	ClientID string

	// InstallationID is the App installation ID for the target
	// org/repo. Required.
	InstallationID int64

	// PrivateKeyPEM is the App's RSA private key in PEM form.
	// Required (one of PrivateKeyPEM / PrivateKeyPath must be
	// non-empty when constructing via NewAppWithConfig directly;
	// NewAppFromFileWithConfig reads the file and supplies this
	// field).
	PrivateKeyPEM []byte

	// ConfigURL, ConfigBaseURL, RESTBaseURL mirror [PATConfig]'s
	// override fields. See those for full semantics; the
	// behaviour is identical across auth modes (issue #214's
	// follow-up).
	ConfigURL     string
	ConfigBaseURL string
	RESTBaseURL   string
}

// NewAppWithConfig is the override-aware constructor for App-based
// auth. GHES deployments must set ConfigURL (single-scope) or
// ConfigBaseURL (multi-scaleset) so the scaleset library hits the
// right host. github.com callers should prefer [NewApp].
func NewAppWithConfig(c AppConfig) (Auth, error) {
	if c.ClientID == "" {
		return nil, errors.New("githubauth: app client_id is required")
	}
	if c.InstallationID == 0 {
		return nil, errors.New("githubauth: app installation_id is required")
	}
	if len(c.PrivateKeyPEM) == 0 {
		return nil, errors.New("githubauth: app private key is required")
	}
	if !strings.Contains(string(c.PrivateKeyPEM), "BEGIN") {
		return nil, errors.New("githubauth: app private key does not look like PEM")
	}
	if err := validateEndpointConfig(c.ConfigURL, c.ConfigBaseURL, c.RESTBaseURL); err != nil {
		return nil, err
	}
	return &appAuth{
		clientID:       c.ClientID,
		installationID: c.InstallationID,
		privateKeyPEM:  string(c.PrivateKeyPEM),
		endpointConfig: endpointConfig{
			configURL:     c.ConfigURL,
			configBaseURL: c.ConfigBaseURL,
			restBaseURL:   c.RESTBaseURL,
		},
	}, nil
}

// NewApp constructs an App-based Auth from a Client ID string. The scaleset
// library accepts either a Client ID (e.g. "Iv23..." style) or the numeric
// App ID as a string — callers with only the numeric ID should
// strconv.FormatInt it before calling. github.com only; for GHES use
// [NewAppWithConfig].
func NewApp(clientID string, installationID int64, privateKeyPEM []byte) (Auth, error) {
	return NewAppWithConfig(AppConfig{
		ClientID:       clientID,
		InstallationID: installationID,
		PrivateKeyPEM:  privateKeyPEM,
	})
}

// NewAppFromFileWithConfig reads the PEM file from disk and
// constructs an App auth honouring the GHES override knobs (issue
// #214 follow-up). Refuses to read a key file with world- or
// group-readable permissions (any bit in 0o077) — the operator's
// deployment is misconfigured if a private key is accessible by
// other users on the box. Also refuses (on unix) when the PEM is
// owned by a UID other than the process's effective UID. mode 0600
// alone is insufficient when the orchestrator runs as root (or
// with CAP_DAC_READ_SEARCH): a key dropped in by another user
// with mode 0600 would otherwise be silently trusted.
func NewAppFromFileWithConfig(c AppConfig, pemPath string) (Auth, error) {
	if pemPath == "" {
		return nil, errors.New("githubauth: pem path is required")
	}
	info, err := os.Stat(pemPath)
	if err != nil {
		return nil, fmt.Errorf("githubauth: stat private key %s: %w", pemPath, err)
	}
	if err := fileperm.CheckMode(info, pemPath, 0o600); err != nil {
		return nil, fmt.Errorf("githubauth: private key: %w", err)
	}
	if err := fileperm.CheckOwnership(info, pemPath); err != nil {
		return nil, fmt.Errorf("githubauth: private key: %w", err)
	}
	pem, err := os.ReadFile(pemPath) // #nosec G304 -- pemPath is operator-supplied and perm-checked above.
	if err != nil {
		return nil, fmt.Errorf("githubauth: read private key %s: %w", pemPath, err)
	}
	c.PrivateKeyPEM = pem
	return NewAppWithConfig(c)
}

// NewAppFromFile reads the PEM file from disk and constructs an
// App auth pointed at github.com. For GHES (and any caller that
// needs override knobs) use [NewAppFromFileWithConfig].
func NewAppFromFile(clientID string, installationID int64, pemPath string) (Auth, error) {
	return NewAppFromFileWithConfig(AppConfig{
		ClientID:       clientID,
		InstallationID: installationID,
	}, pemPath)
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
		GitHubConfigURL: a.resolveScaleSetConfigURL(scope),
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
	// GHES needs a non-default API host. Use ghinstallation's
	// SetBaseURL when configured so the App installation-token
	// fetch hits the right server; pair it with go-github
	// WithURLs so subsequent REST calls follow.
	httpClient := &http.Client{Transport: applyWraps(itr, transportWraps)}
	opts := []github.ClientOptionsFunc{github.WithHTTPClient(httpClient)}
	if a.restBaseURL != "" {
		itr.BaseURL = strings.TrimRight(a.restBaseURL, "/")
		base := a.restBaseURL
		opts = append(opts, github.WithURLs(&base, &base))
	}
	cli, err := github.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("githubauth: build app rest client: %w", err)
	}
	return cli, nil
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
