package adminapi

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/observability"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/pool"
)

// fakePool is a Manager whose calls record state for assertions.
type fakePool struct {
	mu              sync.Mutex
	stats           pool.Stats
	statsErr        error
	markedCompleted []int
	forceDestroyed  []int
	preempted       []int
	preemptErr      error
}

func (f *fakePool) Acquire(_ context.Context, _ int64, _ int) (*pool.VM, error) {
	return nil, pool.ErrNoneAvailable
}
func (f *fakePool) AcquireForProfile(_ context.Context, _ int64, _ string, _ int) (*pool.VM, error) {
	return nil, pool.ErrNoneAvailable
}
func (f *fakePool) MarkRunning(_ context.Context, _ int, _ int64) error { return nil }
func (f *fakePool) SetRunnerID(_ context.Context, _ int, _ int64) error { return nil }
func (f *fakePool) MarkCompleted(_ context.Context, vmid int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.markedCompleted = append(f.markedCompleted, vmid)
	return nil
}
func (f *fakePool) Stats(_ context.Context) (pool.Stats, error) {
	return f.stats, f.statsErr
}
func (f *fakePool) Adopt(_ context.Context) error { return nil }
func (f *fakePool) Run(_ context.Context) error   { return nil }
func (f *fakePool) SignalRefill()                 {}
func (f *fakePool) SetDesiredCount(_ int)         {}
func (f *fakePool) SetTargetSizes(string, int, int) error {
	return nil
}

func (f *fakePool) PromoteToRunning(_ context.Context, _ int, _, _ int64) error {
	return nil
}
func (f *fakePool) Preempt(_ context.Context, vmid int, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.preempted = append(f.preempted, vmid)
	return f.preemptErr
}
func (f *fakePool) StampJobMetadata(_ context.Context, _ int, _ pool.JobMetadata) error {
	return nil
}
func (f *fakePool) ForceDestroy(_ context.Context, vmid int, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forceDestroyed = append(f.forceDestroyed, vmid)
	return nil
}
func (f *fakePool) ListRows(_ context.Context) ([]pool.RowSnapshot, error) {
	return nil, nil
}

func newTestServer(t *testing.T, secret string) (*Server, *fakePool) {
	t.Helper()
	fp := &fakePool{stats: pool.Stats{Hot: 3, Warm: 2}}
	s, err := New(
		Config{HTTPAddr: "ignored", SharedSecret: secret},
		func() pool.Manager { return fp },
		nil, // provisioner unused in these tests
		AlwaysLeader{},
		nil, // drain callback unused
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	require.NoError(t, err)
	return s, fp
}

func mountHandler(s *Server) http.Handler {
	// Replicate the route setup from Serve so tests don't have to bind a port.
	r := http.NewServeMux()
	auth := s.requireBearerToken(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/admin/state":
			s.handleState(w, r)
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/admin/destroy/"):
			// Inject the chi path param so handleDestroyVM picks it up.
			// We bypass chi here for test simplicity by parsing manually.
			vmid := strings.TrimPrefix(r.URL.Path, "/admin/destroy/")
			r.URL.RawQuery = "vmid=" + vmid
			s.handleDestroyVMTest(w, r, vmid)
		default:
			http.NotFound(w, r)
		}
	}))
	r.Handle("/", auth)
	return r
}

// handleDestroyVMTest is a test-only entry point that takes the vmid out
// of band (the production path uses chi.URLParam).
func (s *Server) handleDestroyVMTest(w http.ResponseWriter, r *http.Request, vmidStr string) {
	if vmidStr == "" {
		http.Error(w, "missing vmid", http.StatusBadRequest)
		return
	}
	// chi looks up params from the request context; without a chi router
	// it returns "". Test instead exercises the destruction shortcut.
	s.testDestroy(r.Context(), w, vmidStr)
}

func (s *Server) testDestroy(ctx context.Context, w http.ResponseWriter, vmidStr string) {
	var vmid int
	if _, err := fmtSscan(vmidStr, &vmid); err != nil || vmid <= 0 {
		http.Error(w, "invalid vmid", http.StatusBadRequest)
		return
	}
	if err := s.pool().MarkCompleted(ctx, vmid); err != nil {
		http.Error(w, "internal", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte("queued"))
}

// fmtSscan wraps fmt.Sscanf into something terser for the test helper.
func fmtSscan(s string, dst *int) (int, error) {
	if len(s) == 0 {
		return 0, io.EOF
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, io.EOF
		}
	}
	v := 0
	for _, c := range s {
		v = v*10 + int(c-'0')
	}
	*dst = v
	return len(s), nil
}

func TestRequireBearerToken_RejectsMissingAndWrong(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, "topsecret")
	h := mountHandler(s)

	// No header.
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusUnauthorized, w.Code)

	// Wrong token.
	w = httptest.NewRecorder()
	r = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
	r.Header.Set("Authorization", "Bearer wrong")
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusUnauthorized, w.Code)
}

// TestRequireBearerToken_FailurePathsIndistinguishable locks in the
// timing-uniformity fix (#155): missing header, wrong scheme, and
// wrong token must all route through the same sha256 + ConstantTime
// compare so a probing client can't time-distinguish "no Authorization
// at all" from "Bearer wrong-token" via response latency. We can't
// directly test wall-clock timing in a stable way, so we assert the
// behavioral consequence: identical status / body / headers for all
// three failure shapes.
func TestRequireBearerToken_FailurePathsIndistinguishable(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, "topsecret")
	h := mountHandler(s)

	send := func(authHeader string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
		if authHeader != "" {
			r.Header.Set("Authorization", authHeader)
		}
		h.ServeHTTP(w, r)
		return w
	}

	missing := send("")
	wrongScheme := send("Basic dXNlcjpwYXNz")
	wrongToken := send("Bearer wrong")

	require.Equal(t, http.StatusUnauthorized, missing.Code)
	require.Equal(t, missing.Code, wrongScheme.Code, "wrong scheme must not be distinguishable from missing header")
	require.Equal(t, missing.Code, wrongToken.Code, "wrong token must not be distinguishable from missing header")
	require.Equal(t, missing.Body.String(), wrongScheme.Body.String(), "response body must be identical")
	require.Equal(t, missing.Body.String(), wrongToken.Body.String(), "response body must be identical")
}

// TestRequireBearerToken_RejectsRawSecretWithoutScheme locks down the
// contract that the `Bearer ` scheme prefix is required. The previous
// implementation used strings.TrimPrefix which silently accepted a bare
// secret with no scheme.
func TestRequireBearerToken_RejectsRawSecretWithoutScheme(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, "topsecret")
	h := mountHandler(s)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
	r.Header.Set("Authorization", "topsecret") // no "Bearer " prefix
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusUnauthorized, w.Code)
}

// TestRequireBearerToken_RefusesEmptyConfiguredSecret is defense in
// depth: if a future caller bypasses Serve's empty-secret guard, the
// middleware must still refuse to authenticate. We respond 503 rather
// than 401 because the failure is a server-side misconfiguration, not
// a missing-credential problem the client can fix — and the louder
// response surfaces the misconfig in monitoring. The previous 401
// behavior also worked but bound a precomputed sha256("") into the
// closure, which would silently authenticate empty bearer tokens if a
// future refactor removed the empty-secret short-circuit.
func TestRequireBearerToken_RefusesEmptyConfiguredSecret(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, "")
	h := mountHandler(s)

	// Empty Authorization.
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)

	// "Bearer " with empty token — the dangerous case if sha256("") were
	// bound to the closure.
	w = httptest.NewRecorder()
	r = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
	r.Header.Set("Authorization", "Bearer ")
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)

	// And the matching "Bearer <anything>" must also be refused — the
	// empty-secret handler must not delegate to ConstantTimeCompare.
	w = httptest.NewRecorder()
	r = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
	r.Header.Set("Authorization", "Bearer guess")
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestState_ReturnsPoolStats(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, "topsecret")
	h := mountHandler(s)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
	r.Header.Set("Authorization", "Bearer topsecret")
	h.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	var got stateResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&got))
	require.Equal(t, 3, got.Pool.Hot)
	require.Equal(t, 2, got.Pool.Warm)
}

// chiHandler wires the real production routes onto a chi router so tests
// exercise the same handler chain (including chi.URLParam) that Serve
// uses in production.
func chiHandler(s *Server) http.Handler {
	r := chi.NewRouter()
	r.Use(s.realIP)
	r.Use(s.leaderOrForward)
	r.Use(s.requireBearerToken)
	r.Get("/admin/state", s.handleState)
	r.Post("/admin/drain", s.handleDrain)
	r.Post("/admin/destroy/{vmid}", s.handleDestroyVM)
	return r
}

// fakeGate is a LeaderGate whose responses tests control directly.
// When isLeader is false, every request is captured and the recorded
// response is whatever forward writes — tests can inspect both.
type fakeGate struct {
	isLeader bool
	forward  http.HandlerFunc
}

func (g *fakeGate) IsLeader() bool { return g.isLeader }
func (g *fakeGate) Forward(w http.ResponseWriter, r *http.Request) {
	if g.forward != nil {
		g.forward(w, r)
		return
	}
	http.Error(w, "no forward configured", http.StatusInternalServerError)
}

// TestLeaderGate_NonLeaderForwardsBeforeAuth verifies the two critical
// properties of the leader-or-forward middleware:
//
//  1. A standby forwards the request before requireBearerToken runs,
//     so the standby never needs the shared secret.
//  2. The forward handler observes the original request — including
//     headers — so a reverse-proxy implementation can preserve
//     X-Forwarded-For and other proxy chain headers.
func TestLeaderGate_NonLeaderForwardsBeforeAuth(t *testing.T) {
	t.Parallel()
	fp := &fakePool{stats: pool.Stats{Hot: 1}}
	var forwardedAuth, forwardedXFF string
	gate := &fakeGate{
		isLeader: false,
		forward: func(w http.ResponseWriter, r *http.Request) {
			forwardedAuth = r.Header.Get("Authorization")
			forwardedXFF = r.Header.Get("X-Forwarded-For")
			w.WriteHeader(http.StatusTeapot) // distinctive marker
		},
	}
	s, err := New(
		Config{HTTPAddr: "ignored", SharedSecret: "topsecret"},
		func() pool.Manager { return fp },
		nil,
		gate,
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	require.NoError(t, err)
	h := chiHandler(s)

	// No Authorization header — a leader would 401, but the standby
	// forwards before the token check runs.
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
	r.Header.Set("X-Forwarded-For", "203.0.113.42")
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusTeapot, w.Code,
		"non-leader must forward instead of authenticating locally")
	require.Empty(t, forwardedAuth, "no auth header was sent")
	require.Equal(t, "203.0.113.42", forwardedXFF,
		"forward must observe the original X-Forwarded-For so the leader can rate-limit on the real client IP")
}

// TestLeaderGate_LeaderServesLocally locks down the inverse: when this
// replica is leader, requests are served locally and the gate's Forward
// is never invoked.
func TestLeaderGate_LeaderServesLocally(t *testing.T) {
	t.Parallel()
	fp := &fakePool{stats: pool.Stats{Hot: 5}}
	gate := &fakeGate{
		isLeader: true,
		forward: func(http.ResponseWriter, *http.Request) {
			t.Fatal("Forward must not be called when IsLeader returns true")
		},
	}
	s, err := New(
		Config{HTTPAddr: "ignored", SharedSecret: "topsecret"},
		func() pool.Manager { return fp },
		nil,
		gate,
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	require.NoError(t, err)
	h := chiHandler(s)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
	r.Header.Set("Authorization", "Bearer topsecret")
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)
}

// TestLeaderGate_LeaderWithoutPoolReturns503 covers the race where this
// replica passed the leader gate but OnElected hasn't yet wired the
// pool manager into the accessor. /admin/state must return 503 with
// Retry-After rather than crash with a nil-deref.
func TestLeaderGate_LeaderWithoutPoolReturns503(t *testing.T) {
	t.Parallel()
	s, err := New(
		Config{HTTPAddr: "ignored", SharedSecret: "topsecret"},
		func() pool.Manager { return nil },
		nil,
		AlwaysLeader{},
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	require.NoError(t, err)
	h := chiHandler(s)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
	r.Header.Set("Authorization", "Bearer topsecret")
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.NotEmpty(t, w.Header().Get("Retry-After"))
}

// TestAuth_RateLimitsBadTokens: rapid bad-bearer attacks against the
// admin API must trigger a 429 after the burst budget exhausts. Without
// the per-IP limiter, an attacker could brute-force the bearer secret
// at line rate against any operator-chosen string.
func TestAuth_RateLimitsBadTokens(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, "topsecret")
	h := chiHandler(s)

	var seen401, seen429 int
	for range 50 {
		w := httptest.NewRecorder()
		r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
		r.Header.Set("Authorization", "Bearer wrong")
		r.RemoteAddr = "1.2.3.4:5678"
		h.ServeHTTP(w, r)
		switch w.Code {
		case http.StatusUnauthorized:
			seen401++
		case http.StatusTooManyRequests:
			seen429++
		}
	}
	require.Positive(t, seen429, "expected at least one 429 from rapid bad-token requests; got %d 401s and %d 429s", seen401, seen429)
}

// TestAuth_GoodTokenNotMetered: a valid bearer must never be rate
// limited, regardless of recent failures from the same IP — operators
// running tooling shouldn't get throttled because they fat-fingered
// once.
func TestAuth_GoodTokenNotMetered(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, "topsecret")
	h := chiHandler(s)

	// Exhaust the bucket with bad tokens from a known IP.
	for range 30 {
		w := httptest.NewRecorder()
		r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
		r.Header.Set("Authorization", "Bearer wrong")
		r.RemoteAddr = "5.6.7.8:1234"
		h.ServeHTTP(w, r)
	}
	// Now hit with the correct bearer from the same IP: must succeed.
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
	r.Header.Set("Authorization", "Bearer topsecret")
	r.RemoteAddr = "5.6.7.8:1234"
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code, "valid bearer must bypass the auth-failure limiter")
}

// TestHandleDestroyVM_UsesForceDestroyForHotRow guards the property that
// the admin destroy endpoint must drop a VM regardless of its state. The
// previous handler used pool.MarkCompleted, which silently no-ops on
// Hot/Warm/Booting rows — so operators got a 202 with no actual effect.
// ForceDestroy is the right primitive for an unconditional operator
// intervention.
func TestHandleDestroyVM_UsesForceDestroyForHotRow(t *testing.T) {
	t.Parallel()
	s, fp := newTestServer(t, "topsecret")
	h := chiHandler(s)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/destroy/10042", nil)
	r.Header.Set("Authorization", "Bearer topsecret")
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusAccepted, w.Code)

	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.Contains(t, fp.forceDestroyed, 10042,
		"admin destroy must route through ForceDestroy so it works on Hot/Warm rows")
	require.NotContains(t, fp.markedCompleted, 10042,
		"admin destroy must NOT use MarkCompleted — it no-ops on non-busy rows")
}

func TestDestroyVM_QueuesAndReturns202(t *testing.T) {
	s, fp := newTestServer(t, "topsecret")
	h := chiHandler(s)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/destroy/10042", nil)
	r.Header.Set("Authorization", "Bearer topsecret")
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusAccepted, w.Code)

	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.Contains(t, fp.forceDestroyed, 10042)
}

// TestHandleDestroyVM_RejectsBadVMID locks in the parse/bounds branch
// of handleDestroyVM: non-numeric, zero, negative, and overflow VMID
// path segments must all return 400 without ever reaching ForceDestroy.
// Previously the rejection branch was untested (#137); this is the
// admin escape-hatch endpoint and a silent regression would forward a
// nonsense vmid into the pool layer.
func TestHandleDestroyVM_RejectsBadVMID(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		path string
	}{
		{"non-numeric", "/admin/destroy/abc"},
		{"zero", "/admin/destroy/0"},
		{"negative", "/admin/destroy/-1"},
		{"overflow", "/admin/destroy/9999999999999999999"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, fp := newTestServer(t, "topsecret")
			h := chiHandler(s)

			w := httptest.NewRecorder()
			r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, tc.path, nil)
			r.Header.Set("Authorization", "Bearer topsecret")
			h.ServeHTTP(w, r)

			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Contains(t, w.Body.String(), "invalid vmid")

			fp.mu.Lock()
			defer fp.mu.Unlock()
			require.Empty(t, fp.forceDestroyed,
				"bad-vmid request must NOT reach ForceDestroy")
		})
	}
}

func TestDrain_TriggersCallback(t *testing.T) {
	t.Parallel()
	fp := &fakePool{}
	drained := make(chan struct{}, 1)
	s, err := New(
		Config{HTTPAddr: "ignored", SharedSecret: "topsecret"},
		func() pool.Manager { return fp },
		nil,
		AlwaysLeader{},
		func() { drained <- struct{}{} },
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	require.NoError(t, err)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/drain", nil)
	r.Header.Set("Authorization", "Bearer topsecret")
	s.handleDrain(w, r)
	require.Equal(t, http.StatusAccepted, w.Code)

	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("drain callback was never invoked")
	}
}

// errOnWriteResponseWriter is a minimal http.ResponseWriter that
// captures the status and returns an error on every Write. Used by
// the post-header-write-failure tests below to simulate a client
// disconnect or broken-pipe mid-body.
type errOnWriteResponseWriter struct {
	header http.Header
	code   int
	werr   error
}

func newErrOnWriteResponseWriter(werr error) *errOnWriteResponseWriter {
	return &errOnWriteResponseWriter{header: http.Header{}, werr: werr}
}
func (e *errOnWriteResponseWriter) Header() http.Header { return e.header }
func (e *errOnWriteResponseWriter) Write([]byte) (int, error) {
	return 0, e.werr
}
func (e *errOnWriteResponseWriter) WriteHeader(code int) { e.code = code }

// TestHandleDrain_PostHeaderWriteFailureLogsWarn pins that a body-
// write failure on the state-changing /admin/drain endpoint is
// logged at Warn (not Debug) so the line survives production log
// levels. The operator's tooling shows "no response" while a drain
// IS in flight — Debug would have made the only signal of that
// invisible.
func TestHandleDrain_PostHeaderWriteFailureLogsWarn(t *testing.T) {
	t.Parallel()
	var logBuf bytes.Buffer
	fp := &fakePool{}
	s, err := New(
		Config{HTTPAddr: "ignored", SharedSecret: "topsecret"},
		func() pool.Manager { return fp },
		nil,
		AlwaysLeader{},
		func() {},
		slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})),
	)
	require.NoError(t, err)

	w := newErrOnWriteResponseWriter(errors.New("broken pipe"))
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/drain", nil)
	r.RemoteAddr = "203.0.113.42:54321"
	s.handleDrain(w, r)
	require.Equal(t, http.StatusAccepted, w.code)

	out := logBuf.String()
	require.Contains(t, out, "level=WARN")
	require.Contains(t, out, "admin drain: response write failed")
	require.Contains(t, out, "remote_addr=203.0.113.42:54321")
}

// TestHandleDestroyVM_PostHeaderWriteFailureLogsWarn is the same
// shape as the drain test for the other state-changing endpoint.
func TestHandleDestroyVM_PostHeaderWriteFailureLogsWarn(t *testing.T) {
	t.Parallel()
	var logBuf bytes.Buffer
	fp := &fakePool{}
	s, err := New(
		Config{HTTPAddr: "ignored", SharedSecret: "topsecret"},
		func() pool.Manager { return fp },
		nil,
		AlwaysLeader{},
		nil,
		slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})),
	)
	require.NoError(t, err)

	w := newErrOnWriteResponseWriter(errors.New("broken pipe"))
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/destroy/10042", nil)
	r.RemoteAddr = "203.0.113.42:54321"
	// Route via chi so URLParam("vmid") resolves.
	router := chi.NewRouter()
	router.Post("/admin/destroy/{vmid}", s.handleDestroyVM)
	router.ServeHTTP(w, r)
	require.Equal(t, http.StatusAccepted, w.code)

	out := logBuf.String()
	require.Contains(t, out, "level=WARN")
	require.Contains(t, out, "admin destroy: response write failed")
	require.Contains(t, out, "vmid=10042")
	require.Contains(t, out, "remote_addr=203.0.113.42:54321")
}

func TestDrain_NoCallbackReturns501(t *testing.T) {
	s, _ := newTestServer(t, "topsecret")
	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/drain", nil)
	r.Header.Set("Authorization", "Bearer topsecret")
	s.handleDrain(w, r)
	require.Equal(t, http.StatusNotImplemented, w.Code)
}

func TestServe_NoAddrIsNoOp(t *testing.T) {
	t.Parallel()
	fp := &fakePool{}
	s, err := New(Config{HTTPAddr: ""}, func() pool.Manager { return fp }, nil, AlwaysLeader{}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.NoError(t, s.Serve(ctx))
}

// TestNew_RejectsMalformedTrustedProxyCIDR pins the fail-loud behaviour
// of New: a TrustedProxies entry that fails netip.ParsePrefix must
// surface an error at startup instead of being silently dropped, so any
// future drift between the config validator and the consumer doesn't
// degrade the admin API's IP-trust boundary in production.
func TestNew_RejectsMalformedTrustedProxyCIDR(t *testing.T) {
	t.Parallel()
	fp := &fakePool{}
	s, err := New(
		Config{TrustedProxies: []string{"10.0.0.0/8", "192.168.0.0./24", "127.0.0.0/8"}},
		func() pool.Manager { return fp },
		nil,
		AlwaysLeader{},
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	require.Error(t, err)
	require.Nil(t, s)
	require.Contains(t, err.Error(), "trusted_proxies[1]")
	require.Contains(t, err.Error(), "192.168.0.0./24")
}

// newTestServerTrustedProxies is like newTestServer but populates the
// TrustedProxies CIDR list, so tests can exercise realIP's
// honors-when-trusted / ignores-when-untrusted behavior.
func newTestServerTrustedProxies(t *testing.T, secret string, trusted []string) (*Server, *fakePool) {
	t.Helper()
	fp := &fakePool{stats: pool.Stats{Hot: 3, Warm: 2}}
	s, err := New(
		Config{HTTPAddr: "ignored", SharedSecret: secret, TrustedProxies: trusted},
		func() pool.Manager { return fp },
		nil,
		AlwaysLeader{},
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	require.NoError(t, err)
	return s, fp
}

// TestRealIP_IgnoresHeadersFromUntrustedPeer: the whole point of the
// realIP variant — a hostile client connecting from outside the trusted
// CIDRs cannot inject X-Forwarded-For to swap r.RemoteAddr.
func TestRealIP_IgnoresHeadersFromUntrustedPeer(t *testing.T) {
	t.Parallel()
	s, _ := newTestServerTrustedProxies(t, "topsecret", []string{"10.0.0.0/8"})

	var seenRemote string
	h := s.realIP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenRemote = r.RemoteAddr
	}))

	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
	r.RemoteAddr = "203.0.113.50:5555" // untrusted peer
	r.Header.Set("X-Forwarded-For", "1.2.3.4")
	r.Header.Set("X-Real-IP", "5.6.7.8")
	r.Header.Set("True-Client-IP", "9.10.11.12")
	h.ServeHTTP(httptest.NewRecorder(), r)

	require.Equal(t, "203.0.113.50:5555", seenRemote,
		"untrusted peer headers must be ignored; r.RemoteAddr must stay as the connection peer")
}

// TestRealIP_HonorsHeadersFromTrustedPeer: when an in-cluster
// reverse-proxy (loopback in tests) hits the leader, X-Forwarded-For
// SHOULD be honored so per-IP rate limiting reflects the real client.
func TestRealIP_HonorsHeadersFromTrustedPeer(t *testing.T) {
	t.Parallel()
	s, _ := newTestServerTrustedProxies(t, "topsecret", []string{"127.0.0.0/8"})

	var seenRemote string
	h := s.realIP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenRemote = r.RemoteAddr
	}))

	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
	r.RemoteAddr = "127.0.0.1:5555" // trusted loopback
	r.Header.Set("X-Forwarded-For", "203.0.113.10")
	h.ServeHTTP(httptest.NewRecorder(), r)

	require.Equal(t, "203.0.113.10", seenRemote,
		"trusted peer's X-Forwarded-For must be honored")
}

// TestRealIP_XFFPrecedence: when several forwarded-for headers are
// present from a trusted peer, X-Forwarded-For wins over X-Real-IP and
// True-Client-IP. The rightmost untrusted hop in the XFF list is
// returned — that's the closest-to-the-server IP outside the operator's
// declared trusted_proxies, which is what defeats multi-hop XFF
// spoofing.
func TestRealIP_XFFPrecedence(t *testing.T) {
	t.Parallel()
	s, _ := newTestServerTrustedProxies(t, "topsecret", []string{"127.0.0.0/8"})

	var seenRemote string
	h := s.realIP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenRemote = r.RemoteAddr
	}))

	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
	r.RemoteAddr = "127.0.0.1:5555"
	r.Header.Set("X-Forwarded-For", "203.0.113.10, 198.51.100.1")
	r.Header.Set("X-Real-IP", "203.0.113.11")
	r.Header.Set("True-Client-IP", "203.0.113.12")
	h.ServeHTTP(httptest.NewRecorder(), r)

	require.Equal(t, "198.51.100.1", seenRemote,
		"rightmost untrusted XFF entry should win over X-Real-IP / True-Client-IP")
}

// TestServe_TLS_RoundTripsBearer: when TLSConfig is set, the admin
// server listens over https and a correctly-configured client gets a
// successful 200 with the shared bearer. Guards against the bug where
// the bearer secret would travel in cleartext between standby
// Forwarder and leader.
func TestServe_TLS_RoundTripsBearer(t *testing.T) {
	t.Parallel()
	certPath, keyPath := generateAdminAPITestKeypair(t)
	serverCert, err := tls.LoadX509KeyPair(certPath, keyPath)
	require.NoError(t, err)
	pemBytes, err := os.ReadFile(certPath)
	require.NoError(t, err)
	rootPool := x509.NewCertPool()
	require.True(t, rootPool.AppendCertsFromPEM(pemBytes))

	// Reserve an ephemeral port: Serve binds to cfg.HTTPAddr, so we
	// resolve a free port first.
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	s, err := New(
		Config{
			HTTPAddr:     addr,
			SharedSecret: "topsecret",
			TLSConfig:    &tls.Config{Certificates: []tls.Certificate{serverCert}, MinVersion: tls.VersionTLS12},
			TLSCertFile:  certPath,
			TLSKeyFile:   keyPath,
		},
		func() pool.Manager { return &fakePool{stats: pool.Stats{Hot: 1}} },
		nil,
		AlwaysLeader{},
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- s.Serve(ctx) }()

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: rootPool, MinVersion: tls.VersionTLS12}}}
	url := "https://" + addr + "/admin/state"
	var resp *http.Response
	require.Eventually(t, func() bool {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		req.Header.Set("Authorization", "Bearer topsecret")
		r, err := client.Do(req)
		if err != nil {
			return false
		}
		resp = r
		return true
	}, 2*time.Second, 25*time.Millisecond, "TLS admin server never accepted")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	cancel()
	require.NoError(t, <-errCh)
}

// generateAdminAPITestKeypair writes a self-signed loopback cert and
// returns the file paths. Standalone helper so this test file doesn't
// import test fixtures from internal/config.
func generateAdminAPITestKeypair(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)

	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")

	cfh, err := os.Create(certPath)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(cfh, &pem.Block{Type: "CERTIFICATE", Bytes: der}))
	require.NoError(t, cfh.Close())

	keyDER, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)
	kfh, err := os.Create(keyPath)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(kfh, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	require.NoError(t, kfh.Close())
	return certPath, keyPath
}

// TestAuth_RateLimit_CannotBeSpoofedViaXFF: end-to-end guard against
// the bug — a hostile client sending bad bearer attempts must not
// reset the limiter by rotating X-Forwarded-For per request.
func TestAuth_RateLimit_CannotBeSpoofedViaXFF(t *testing.T) {
	t.Parallel()
	// trustedProxies empty: NO peer is trusted, so XFF is always ignored.
	s, _ := newTestServerTrustedProxies(t, "topsecret", nil)
	h := chiHandler(s)

	var seen401, seen429 int
	for i := range 50 {
		w := httptest.NewRecorder()
		r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/state", nil)
		r.Header.Set("Authorization", "Bearer wrong")
		r.RemoteAddr = "203.0.113.99:5555" // single attacker IP
		// Try to spoof a different "origin" per request.
		r.Header.Set("X-Forwarded-For", fmt.Sprintf("10.0.0.%d", i))
		h.ServeHTTP(w, r)
		switch w.Code {
		case http.StatusUnauthorized:
			seen401++
		case http.StatusTooManyRequests:
			seen429++
		}
	}
	require.Positive(t, seen429,
		"spoofed XFF must NOT defeat the per-IP rate limiter; got %d 401 / %d 429", seen401, seen429)
}

// ---------------------------------------------------------------------------
// Preempt endpoint (PR 5 — issue #10)
// ---------------------------------------------------------------------------

func TestPreempt_QueuesPreemptionAndIncrementsMetric(t *testing.T) {
	t.Parallel()
	fp := &fakePool{}
	registry := prometheus.NewRegistry()
	metrics := observability.NewMetrics(registry)
	s, err := New(Config{SharedSecret: "tk"}, func() pool.Manager { return fp },
		nil, AlwaysLeader{}, nil, slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))
	require.NoError(t, err)
	s.SetMetrics(metrics)
	h := preemptHandler(s)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/preempt/12345", nil)
	req.Header.Set("Authorization", "Bearer tk")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusAccepted, w.Code, "successful preempt: 202")

	require.Equal(t, []int{12345}, fp.preempted)
	require.Equal(t, 1.0,
		testutil.ToFloat64(metrics.Preemptions.WithLabelValues("", "manual")),
		"preempt endpoint must bump scaleset_preemptions_total{to_class=manual}")
}

func TestPreempt_RefusedReturnsConflict(t *testing.T) {
	t.Parallel()
	fp := &fakePool{preemptErr: pool.ErrPreemptRefused}
	s, err := New(Config{SharedSecret: "tk"}, func() pool.Manager { return fp },
		nil, AlwaysLeader{}, nil, slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))
	require.NoError(t, err)
	h := preemptHandler(s)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/preempt/12345", nil)
	req.Header.Set("Authorization", "Bearer tk")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusConflict, w.Code,
		"refused preempt (e.g. row is Running) must surface as 409")
}

func TestPreempt_InvalidVMID(t *testing.T) {
	t.Parallel()
	fp := &fakePool{}
	s, err := New(Config{SharedSecret: "tk"}, func() pool.Manager { return fp },
		nil, AlwaysLeader{}, nil, slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))
	require.NoError(t, err)
	h := preemptHandler(s)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/preempt/not-a-number", nil)
	req.Header.Set("Authorization", "Bearer tk")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Empty(t, fp.preempted, "no preempt must fire for an invalid vmid")
}

func TestPreempt_NoLeaderReturns503(t *testing.T) {
	t.Parallel()
	s, err := New(Config{SharedSecret: "tk"}, func() pool.Manager { return nil },
		nil, AlwaysLeader{}, nil, slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))
	require.NoError(t, err)
	h := preemptHandler(s)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/preempt/12345", nil)
	req.Header.Set("Authorization", "Bearer tk")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusServiceUnavailable, w.Code,
		"pool nil (leader transition): 503 with Retry-After")
	require.NotEmpty(t, w.Header().Get("Retry-After"))
}

// preemptHandler mounts JUST the preempt route through chi so tests
// can exercise the full middleware chain (auth + path-param decode)
// without binding a port.
func preemptHandler(s *Server) http.Handler {
	r := chi.NewRouter()
	r.Use(s.requireBearerToken)
	r.Post("/admin/preempt/{vmid}", s.handlePreemptVM)
	return r
}
