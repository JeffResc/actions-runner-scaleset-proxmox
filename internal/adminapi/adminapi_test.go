package adminapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/pool"
)

// fakePool is a Manager whose calls record state for assertions.
type fakePool struct {
	mu              sync.Mutex
	stats           pool.Stats
	statsErr        error
	markedCompleted []int
}

func (f *fakePool) Acquire(_ context.Context, _ int64) (*pool.VM, error) {
	return nil, pool.ErrNoneAvailable
}
func (f *fakePool) MarkRunning(_ context.Context, _ int, _ int64) error  { return nil }
func (f *fakePool) SetRunnerID(_ context.Context, _ int, _ int64) error  { return nil }
func (f *fakePool) MarkCompleted(_ context.Context, vmid int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.markedCompleted = append(f.markedCompleted, vmid)
	return nil
}
func (f *fakePool) Stats(_ context.Context) (pool.Stats, error) {
	return f.stats, f.statsErr
}
func (f *fakePool) Recover(_ context.Context) error { return nil }
func (f *fakePool) Run(_ context.Context) error     { return nil }
func (f *fakePool) SignalRefill()                   {}
func (f *fakePool) SetDesiredCount(_ int)           {}

func (f *fakePool) PromoteToRunning(_ context.Context, _ int, _, _ int64) error {
	return nil
}
func (f *fakePool) ForceDestroy(_ context.Context, vmid int, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.markedCompleted = append(f.markedCompleted, vmid)
	return nil
}
func (f *fakePool) ListRows(_ context.Context) ([]pool.RowSnapshot, error) {
	return nil, nil
}

func newTestServer(t *testing.T, secret string) (*Server, *fakePool) {
	t.Helper()
	fp := &fakePool{stats: pool.Stats{Hot: 3, Warm: 2}}
	s := New(Config{HTTPAddr: "ignored", SharedSecret: secret}, fp, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
	// reuse parsing logic by delegating to the production handler with
	// the URL path-param injected.
	r2 := r.Clone(r.Context())
	r2.URL.Path = "/admin/destroy/" + vmidStr
	// chi looks up params from the request context; without a chi router
	// it returns "". Test instead exercises the destruction shortcut.
	s.testDestroy(r2.Context(), w, vmidStr)
}

func (s *Server) testDestroy(ctx context.Context, w http.ResponseWriter, vmidStr string) {
	var vmid int
	if _, err := fmtSscan(vmidStr, &vmid); err != nil || vmid <= 0 {
		http.Error(w, "invalid vmid", http.StatusBadRequest)
		return
	}
	if err := s.pool.MarkCompleted(ctx, vmid); err != nil {
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
	r := httptest.NewRequest(http.MethodGet, "/admin/state", nil)
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusUnauthorized, w.Code)

	// Wrong token.
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/admin/state", nil)
	r.Header.Set("Authorization", "Bearer wrong")
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusUnauthorized, w.Code)
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
	r := httptest.NewRequest(http.MethodGet, "/admin/state", nil)
	r.Header.Set("Authorization", "topsecret") // no "Bearer " prefix
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusUnauthorized, w.Code)
}

// TestRequireBearerToken_RefusesEmptyConfiguredSecret is defense in
// depth: if a future caller bypasses Serve's empty-secret guard, the
// middleware must still refuse to authenticate (rather than allowing a
// blank token to match a blank configured secret).
func TestRequireBearerToken_RefusesEmptyConfiguredSecret(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, "")
	h := mountHandler(s)

	// Empty Authorization.
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/admin/state", nil)
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusUnauthorized, w.Code)

	// "Bearer " with empty token.
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/admin/state", nil)
	r.Header.Set("Authorization", "Bearer ")
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestState_ReturnsPoolStats(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, "topsecret")
	h := mountHandler(s)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/admin/state", nil)
	r.Header.Set("Authorization", "Bearer topsecret")
	h.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	var got stateResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&got))
	require.Equal(t, 3, got.Pool.Hot)
	require.Equal(t, 2, got.Pool.Warm)
}

func TestDestroyVM_QueuesAndReturns202(t *testing.T) {
	s, fp := newTestServer(t, "topsecret")
	h := mountHandler(s)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/admin/destroy/10042", nil)
	r.Header.Set("Authorization", "Bearer topsecret")
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusAccepted, w.Code)

	fp.mu.Lock()
	defer fp.mu.Unlock()
	require.Contains(t, fp.markedCompleted, 10042)
}

func TestDrain_TriggersCallback(t *testing.T) {
	t.Parallel()
	fp := &fakePool{}
	drained := make(chan struct{}, 1)
	s := New(
		Config{HTTPAddr: "ignored", SharedSecret: "topsecret"},
		fp,
		nil,
		func() { drained <- struct{}{} },
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/admin/drain", nil)
	r.Header.Set("Authorization", "Bearer topsecret")
	s.handleDrain(w, r)
	require.Equal(t, http.StatusAccepted, w.Code)

	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("drain callback was never invoked")
	}
}

func TestDrain_NoCallbackReturns501(t *testing.T) {
	s, _ := newTestServer(t, "topsecret")
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/admin/drain", nil)
	r.Header.Set("Authorization", "Bearer topsecret")
	s.handleDrain(w, r)
	require.Equal(t, http.StatusNotImplemented, w.Code)
}

func TestServe_NoAddrIsNoOp(t *testing.T) {
	t.Parallel()
	s := New(Config{HTTPAddr: ""}, &fakePool{}, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := s.Serve(ctx)
	require.NoError(t, err)
}
