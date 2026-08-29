package cluster

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// scriptedCoord returns a scripted sequence of (endpoint, err) responses
// from LeaderEndpoint, one per call, so a test can model leadership
// transitions and peer-map endpoint drift ACROSS requests. The last
// scripted response repeats if more calls arrive. Only LeaderEndpoint is
// exercised by the Forwarder; the rest of Coordinator is inert.
type scriptedCoord struct {
	mu        sync.Mutex
	calls     int
	responses []leaderResp
}

type leaderResp struct {
	endpoint string
	err      error
}

func (c *scriptedCoord) LeaderEndpoint(context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := c.calls
	if i >= len(c.responses) {
		i = len(c.responses) - 1
	}
	c.calls++
	return c.responses[i].endpoint, c.responses[i].err
}

func (c *scriptedCoord) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (*scriptedCoord) IsLeader() bool                  { return false }
func (*scriptedCoord) Run(context.Context) error       { return nil }
func (*scriptedCoord) Stop() error                     { return nil }
func (*scriptedCoord) AddObserver(func(IsLeader bool)) {}

// deadAddr grabs a loopback port and immediately frees it, returning an
// address guaranteed to refuse connections — a stand-in for a leader
// endpoint that is valid in the peer map but unreachable (deposed /
// crashed target).
func deadAddr(t *testing.T) string {
	t.Helper()
	lc := &net.ListenConfig{}
	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// TestForwarder_LeadershipLostBetweenRequests_NeverWrongSuccess pins the
// deposed-leader-mid-flight framing (#357): a forwarder must re-consult
// the coordinator on EVERY request and fail closed on both failure modes,
// never returning a wrong-target 2xx.
//
//   - Request 1: LeaderEndpoint yields a valid endpoint whose target is
//     unreachable (a deposed/crashed leader) → 502 Bad Gateway.
//   - Request 2: leadership was lost between calls → LeaderEndpoint yields
//     "" → 503 + Retry-After.
//
// The same Forwarder instance handles both, proving it holds no cached
// leader and cannot silently forward to a stale target.
func TestForwarder_LeadershipLostBetweenRequests_NeverWrongSuccess(t *testing.T) {
	t.Parallel()
	coord := &scriptedCoord{responses: []leaderResp{
		{endpoint: deadAddr(t)}, // valid-but-unreachable (deposed target)
		{endpoint: ""},          // leadership lost → no leader
	}}
	f := NewForwarder(coord, nil)

	rec1 := httptest.NewRecorder()
	f.ServeHTTP(rec1, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil))
	require.Equal(t, http.StatusBadGateway, rec1.Code,
		"a valid-but-unreachable (deposed) leader must fail closed with 502, never a wrong-target success")
	require.Empty(t, rec1.Header().Get("Retry-After"),
		"unreachable-leader must not carry the no-leader Retry-After signal")

	rec2 := httptest.NewRecorder()
	f.ServeHTTP(rec2, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil))
	require.Equal(t, http.StatusServiceUnavailable, rec2.Code,
		"leadership lost between requests must surface as a transient 503, never a 2xx against a stale target")
	require.Equal(t, "2", rec2.Header().Get("Retry-After"))

	require.Equal(t, 2, coord.callCount(),
		"the forwarder must re-resolve the leader on every request (no cached leader)")
}

// TestForwarder_PeerMapEndpointDrift_DialsPerRequestTarget pins that the
// forwarder dials the endpoint it looked up for THAT request (#357): when
// LeaderEndpoint returns endpoint A then B on successive calls (a leader
// re-election / peer-map drift), request 1 must land on A and request 2
// on B — no stale-dial success against the wrong target.
func TestForwarder_PeerMapEndpointDrift_DialsPerRequestTarget(t *testing.T) {
	t.Parallel()
	var hitsA, hitsB atomic.Int64
	upstreamA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hitsA.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "A")
	}))
	defer upstreamA.Close()
	upstreamB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hitsB.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "B")
	}))
	defer upstreamB.Close()

	uA, err := url.Parse(upstreamA.URL)
	require.NoError(t, err)
	uB, err := url.Parse(upstreamB.URL)
	require.NoError(t, err)

	coord := &scriptedCoord{responses: []leaderResp{
		{endpoint: uA.Host},
		{endpoint: uB.Host},
	}}
	f := NewForwarder(coord, nil)

	rec1 := httptest.NewRecorder()
	f.ServeHTTP(rec1, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil))
	require.Equal(t, http.StatusOK, rec1.Code)
	require.Equal(t, "A", rec1.Body.String(), "request 1 must be served by the endpoint resolved for it (A)")
	require.Equal(t, int64(1), hitsA.Load())
	require.Equal(t, int64(0), hitsB.Load(),
		"request 1 must NOT dial the endpoint resolved for a later request")

	rec2 := httptest.NewRecorder()
	f.ServeHTTP(rec2, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil))
	require.Equal(t, http.StatusOK, rec2.Code)
	require.Equal(t, "B", rec2.Body.String(), "request 2 must follow the drifted endpoint (B), not a stale A")
	require.Equal(t, int64(1), hitsA.Load(), "endpoint drift must not re-dial the stale target A")
	require.Equal(t, int64(1), hitsB.Load())
}
