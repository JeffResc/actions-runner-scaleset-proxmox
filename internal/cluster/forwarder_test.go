package cluster

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// stubCoord lets a test drive Coordinator.LeaderEndpoint directly.
// The Forwarder is the only consumer of that method; the rest of
// the interface is unused here.
type stubCoord struct {
	endpoint string
	err      error
}

func (s stubCoord) IsLeader() bool                                 { return false }
func (s stubCoord) LeaderEndpoint(context.Context) (string, error) { return s.endpoint, s.err }
func (s stubCoord) Run(context.Context) error                      { return nil }
func (s stubCoord) Stop() error                                    { return nil }
func (s stubCoord) AddObserver(func(IsLeader bool))                {}

// TestForwarder_NoLeader_Returns503WithRetryAfter pins the
// no-leader response shape: 503 + Retry-After: 2. This is the
// signal a hook-script retry loop converges on once an election
// completes.
func TestForwarder_NoLeader_Returns503WithRetryAfter(t *testing.T) {
	t.Parallel()
	f := NewForwarder(stubCoord{endpoint: ""}, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
	f.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code,
		"empty endpoint must surface as 503, not 502")
	require.Equal(t, "2", rec.Header().Get("Retry-After"))
	require.Contains(t, rec.Body.String(), "no leader")
}

// TestForwarder_CoordError_Returns502WithErrorText covers the
// LeaderEndpoint-errors path (#338): a genuine lookup error (e.g. a
// peer-map misconfiguration) is NOT a transient election, so it must
// surface as a distinct 502 with the error text — no Retry-After — so an
// operator can tell it apart from the "no leader yet" 503 instead of
// chasing a misleading retry signal.
func TestForwarder_CoordError_Returns502WithErrorText(t *testing.T) {
	t.Parallel()
	f := NewForwarder(stubCoord{err: errors.New("raft store unavailable")}, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
	f.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadGateway, rec.Code,
		"a real leader-lookup error must surface as 502, not a transient 503")
	require.Empty(t, rec.Header().Get("Retry-After"),
		"a config-bug error must NOT advertise Retry-After like a transient election")
	require.Contains(t, rec.Body.String(), "raft store unavailable",
		"the error text must be surfaced so the misconfiguration is debuggable")
}

// TestForwarder_LeaderUnreachable_Returns502 pins the distinction
// from the no-leader case: a valid endpoint that simply doesn't
// answer must surface as 502 (Bad Gateway), not 503. Operators
// reading admin-API logs need to tell these two failure modes
// apart.
func TestForwarder_LeaderUnreachable_Returns502(t *testing.T) {
	t.Parallel()
	// Point at a port nothing is listening on. Grab one and
	// immediately close so the dial below is guaranteed to fail.
	lc := &net.ListenConfig{}
	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	f := NewForwarder(stubCoord{endpoint: addr}, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
	f.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadGateway, rec.Code,
		"unreachable leader must surface as 502 — not 503 — so operators can tell a no-leader window from a network problem")
	require.Empty(t, rec.Header().Get("Retry-After"),
		"Retry-After is the no-leader signal; unreachable-leader must NOT carry it")
}

// TestForwarder_RewritesRequestToLeader proves the director sets
// the request's URL host to the leader's endpoint so the
// ReverseProxy actually dials the leader (and not, say, the
// original request host). Uses a real upstream so the rewrite is
// observable end-to-end.
func TestForwarder_RewritesRequestToLeader(t *testing.T) {
	t.Parallel()
	var (
		gotPath string
		gotHost string
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotHost = r.Host
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()

	u, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	f := NewForwarder(stubCoord{endpoint: u.Host}, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
	f.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "/admin/state", gotPath, "path must round-trip unchanged")
	require.Equal(t, u.Host, gotHost, "Host header must be rewritten to the leader's endpoint")
}

// TestForwarder_StripsInboundSpoofableHeaders is the security-
// critical assertion: X-Forwarded-For / X-Real-IP / True-Client-IP
// MUST be removed before the request reaches the leader, otherwise
// a hostile client hitting a standby could forge any source IP for
// the leader's per-IP rate limiter to key on.
//
// We capture the headers the leader actually sees and assert:
//   - the forged values are gone,
//   - X-Forwarded-For is set by ReverseProxy (httputil) to the
//     standby's connection peer — which is the trusted in-cluster
//     hop from the leader's perspective.
func TestForwarder_StripsInboundSpoofableHeaders(t *testing.T) {
	t.Parallel()
	var seen http.Header
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	u, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	f := NewForwarder(stubCoord{endpoint: u.Host}, nil)

	// Wrap the forwarder in a real http.Server so r.RemoteAddr is
	// populated with a realistic peer (httptest's
	// NewRequestWithContext sets a hard-coded value that
	// ReverseProxy treats specially).
	srv := httptest.NewServer(f)
	defer srv.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/admin/state", nil)
	require.NoError(t, err)
	req.Header.Set("X-Forwarded-For", "203.0.113.99") // attacker-supplied
	req.Header.Set("X-Real-IP", "203.0.113.100")
	req.Header.Set("True-Client-IP", "203.0.113.101")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// The forged X-Forwarded-For value must not survive.
	xff := seen.Get("X-Forwarded-For")
	require.NotContains(t, xff, "203.0.113.99",
		"forwarder must strip the inbound X-Forwarded-For; spoofed value leaked through to leader")
	// ReverseProxy appends the standby's connection peer as the
	// only XFF value; assert the local 127.0.0.1 hop is present
	// (sanity check the strip + rewrite actually happened, not
	// just a deletion).
	require.True(t, xff == "" || strings.HasPrefix(xff, "127.0.0.1") || strings.HasPrefix(xff, "::1"),
		"leader-side XFF should reflect the standby's connection peer; got %q", xff)
	// X-Real-IP and True-Client-IP have no ReverseProxy
	// auto-rewrite, so they must be absent.
	require.Empty(t, seen.Get("X-Real-IP"),
		"X-Real-IP must not survive the strip; spoofed value leaked through")
	require.Empty(t, seen.Get("True-Client-IP"),
		"True-Client-IP must not survive the strip; spoofed value leaked through")
}
