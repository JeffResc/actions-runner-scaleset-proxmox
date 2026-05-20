package runnerhook

import (
	"bytes"
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
	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/runnertoken"
)

const testHMACSecret = "0123456789abcdef0123456789abcdef"

// fakePool records what the hook handler asked the manager to do so the
// tests can assert intent without standing up an actual DB.
type fakePool struct {
	mu       sync.Mutex
	promotes []struct {
		VMID     int
		RunnerID int64
		JobID    int64
	}
	completes []int
}

func (f *fakePool) PromoteToRunning(_ context.Context, vmid int, runnerID, jobID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.promotes = append(f.promotes, struct {
		VMID     int
		RunnerID int64
		JobID    int64
	}{vmid, runnerID, jobID})
	return nil
}

func (f *fakePool) MarkCompleted(_ context.Context, vmid int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completes = append(f.completes, vmid)
	return nil
}

// Unused on this server but required by the interface.
func (f *fakePool) Acquire(context.Context, int64) (*pool.VM, error)     { return nil, nil }
func (f *fakePool) MarkRunning(context.Context, int, int64) error        { return nil }
func (f *fakePool) ForceDestroy(context.Context, int, string) error      { return nil }
func (f *fakePool) ListRows(context.Context) ([]pool.RowSnapshot, error) { return nil, nil }
func (f *fakePool) Stats(context.Context) (pool.Stats, error)            { return pool.Stats{}, nil }
func (f *fakePool) Recover(context.Context) error                        { return nil }
func (f *fakePool) Run(context.Context) error                            { return nil }
func (f *fakePool) SignalRefill()                                        {}
func (f *fakePool) SetDesiredCount(int)                                  {}

// newTestServer wires up a server + minter + verifier so tests can mint
// JWTs bound to whatever (vmid, runner_id) they need.
type testServer struct {
	handler http.Handler
	minter  *runnertoken.Minter
	pool    *fakePool
}

func newTestServer(t *testing.T) *testServer {
	t.Helper()
	verifier, err := runnertoken.NewVerifier([]byte(testHMACSecret), "test-scaleset")
	require.NoError(t, err)
	minter, err := runnertoken.NewMinter([]byte(testHMACSecret), time.Hour, "test-scaleset")
	require.NoError(t, err)
	fp := &fakePool{}
	srv := New(Config{
		HTTPAddr:         "ignored",
		Verifier:         verifier,
		RunnerNamePrefix: "gh-runner-test-",
		// Generous limits so existing tests aren't accidentally
		// throttled — the dedicated rate-limit test overrides them.
		RateLimitRPS:   1000,
		RateLimitBurst: 1000,
	}, fp, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	mux := http.NewServeMux()
	mux.Handle("/runner-event", srv.rateLimit(srv.requireToken(http.HandlerFunc(srv.handleEvent))))
	return &testServer{handler: mux, minter: minter, pool: fp}
}

func (ts *testServer) post(t *testing.T, token string, payload EventPayload) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/runner-event", bytes.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	ts.handler.ServeHTTP(w, req)
	return w
}

func TestHandleEvent_Started_Promotes(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t)
	tok, err := ts.minter.Mint(10042, 88)
	require.NoError(t, err)

	w := ts.post(t, tok, EventPayload{
		Phase:      "started",
		RunnerName: "gh-runner-test-10042",
		JobID:      777,
		RunnerID:   88,
	})

	require.Equal(t, http.StatusNoContent, w.Code)
	require.Len(t, ts.pool.promotes, 1)
	require.Equal(t, 10042, ts.pool.promotes[0].VMID)
	require.Equal(t, int64(88), ts.pool.promotes[0].RunnerID)
	require.Equal(t, int64(777), ts.pool.promotes[0].JobID)
}

func TestHandleEvent_Completed_MarksCompleted(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t)
	tok, err := ts.minter.Mint(10042, 88)
	require.NoError(t, err)

	w := ts.post(t, tok, EventPayload{
		Phase:      "completed",
		RunnerName: "gh-runner-test-10042",
		Result:     "success",
	})

	require.Equal(t, http.StatusNoContent, w.Code)
	require.Equal(t, []int{10042}, ts.pool.completes)
}

func TestHandleEvent_TokenRequired(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t)

	body, _ := json.Marshal(EventPayload{Phase: "started", RunnerName: "gh-runner-test-1"})
	req := httptest.NewRequest(http.MethodPost, "/runner-event", bytes.NewReader(body))
	w := httptest.NewRecorder()
	ts.handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusUnauthorized, w.Code)
	require.Contains(t, w.Header().Get("WWW-Authenticate"), "Bearer")
	require.Empty(t, ts.pool.promotes)
}

func TestHandleEvent_RejectsInvalidToken(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t)

	w := ts.post(t, "not.a.real.jwt", EventPayload{Phase: "started", RunnerName: "gh-runner-test-1"})
	require.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestHandleEvent_RejectsTokenFromOtherIssuer(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t)
	// Mint a token with a different issuer — must be rejected.
	wrongMinter, err := runnertoken.NewMinter([]byte(testHMACSecret), time.Hour, "different-issuer")
	require.NoError(t, err)
	tok, err := wrongMinter.Mint(10042, 1)
	require.NoError(t, err)

	w := ts.post(t, tok, EventPayload{Phase: "started", RunnerName: "gh-runner-test-10042"})
	require.Equal(t, http.StatusUnauthorized, w.Code)
}

// TestHandleEvent_VMIDClaimMismatch verifies that even a valid token
// minted for VM A cannot be used to target VM B — the blast radius of
// any leaked token is limited to its single vmid.
func TestHandleEvent_VMIDClaimMismatch(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t)
	tok, err := ts.minter.Mint(10042, 1)
	require.NoError(t, err)

	// Token is for 10042; payload targets 99999.
	w := ts.post(t, tok, EventPayload{
		Phase:      "completed",
		RunnerName: "gh-runner-test-99999",
	})
	require.Equal(t, http.StatusForbidden, w.Code)
	require.Empty(t, ts.pool.completes)
}

// TestHandleEvent_RunnerIDClaimMismatch verifies on `started` payloads
// the token's runner_id must match the payload's runner_id.
func TestHandleEvent_RunnerIDClaimMismatch(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t)
	tok, err := ts.minter.Mint(10042, 88)
	require.NoError(t, err)

	w := ts.post(t, tok, EventPayload{
		Phase:      "started",
		RunnerName: "gh-runner-test-10042",
		RunnerID:   99, // doesn't match the token's 88
	})
	require.Equal(t, http.StatusForbidden, w.Code)
	require.Empty(t, ts.pool.promotes)
}

func TestHandleEvent_BadRunnerName(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t)
	tok, err := ts.minter.Mint(1, 1)
	require.NoError(t, err)

	// Name doesn't match the prefix.
	w := ts.post(t, tok, EventPayload{Phase: "started", RunnerName: "other-runner-99"})
	require.Equal(t, http.StatusBadRequest, w.Code)

	// Name matches but trailing isn't an int.
	w = ts.post(t, tok, EventPayload{Phase: "started", RunnerName: "gh-runner-test-notanumber"})
	require.Equal(t, http.StatusBadRequest, w.Code)

	require.Empty(t, ts.pool.promotes)
}

func TestHandleEvent_UnknownPhase(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t)
	tok, err := ts.minter.Mint(1, 1)
	require.NoError(t, err)

	w := ts.post(t, tok, EventPayload{Phase: "weird", RunnerName: "gh-runner-test-1"})
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// TestHandleEvent_IdempotentDuplicates verifies repeating an event (which
// happens if the hook script retries on a transient network error) is
// safe.
func TestHandleEvent_IdempotentDuplicates(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t)
	tok, err := ts.minter.Mint(7, 0)
	require.NoError(t, err)

	for range 3 {
		w := ts.post(t, tok, EventPayload{Phase: "started", RunnerName: "gh-runner-test-7"})
		require.Equal(t, http.StatusNoContent, w.Code)
	}
	require.Len(t, ts.pool.promotes, 3)
}

func TestHandleEvent_RejectsGET(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t)
	tok, err := ts.minter.Mint(1, 1)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/runner-event", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	ts.handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

// TestHandleEvent_BodyTooLarge protects against a misbehaving runner
// pinning the orchestrator with a giant payload.
func TestHandleEvent_BodyTooLarge(t *testing.T) {
	t.Parallel()
	verifier, err := runnertoken.NewVerifier([]byte(testHMACSecret), "test-scaleset")
	require.NoError(t, err)
	minter, err := runnertoken.NewMinter([]byte(testHMACSecret), time.Hour, "test-scaleset")
	require.NoError(t, err)
	srv := New(Config{
		HTTPAddr:         "ignored",
		Verifier:         verifier,
		RunnerNamePrefix: "gh-runner-test-",
		MaxBodyBytes:     32,
	}, &fakePool{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	mux := http.NewServeMux()
	mux.Handle("/runner-event", srv.requireToken(http.HandlerFunc(srv.handleEvent)))

	tok, err := minter.Mint(1, 1)
	require.NoError(t, err)

	huge := strings.Repeat("a", 1024)
	req := httptest.NewRequest(http.MethodPost, "/runner-event", strings.NewReader(huge))
	req.Header.Set("Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
}

// TestServe_DisabledWithoutAddr returns cleanly on cancel when no addr
// is configured (the typical "don't run me" path).
func TestServe_DisabledWithoutAddr(t *testing.T) {
	t.Parallel()
	srv := New(Config{}, &fakePool{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		cancel()
	}()
	require.NoError(t, srv.Serve(ctx))
}

// TestServe_RejectsMissingVerifier refuses to start without a verifier —
// defense in depth so a misconfigured deploy doesn't leave the endpoint
// open.
func TestServe_RejectsMissingVerifier(t *testing.T) {
	t.Parallel()
	srv := New(Config{
		HTTPAddr:         "127.0.0.1:0",
		RunnerNamePrefix: "x-",
	}, &fakePool{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	err := srv.Serve(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "verifier")
}

// TestRateLimit_BlocksAfterBurst verifies that a single source IP that
// exceeds its burst budget gets 429 — closes the brute-force window
// against the per-job HMAC.
func TestRateLimit_BlocksAfterBurst(t *testing.T) {
	t.Parallel()
	verifier, err := runnertoken.NewVerifier([]byte(testHMACSecret), "test-scaleset")
	require.NoError(t, err)
	minter, err := runnertoken.NewMinter([]byte(testHMACSecret), time.Hour, "test-scaleset")
	require.NoError(t, err)
	srv := New(Config{
		HTTPAddr:         "ignored",
		Verifier:         verifier,
		RunnerNamePrefix: "gh-runner-test-",
		RateLimitRPS:     0.001, // effectively no refill during the test
		RateLimitBurst:   2,
	}, &fakePool{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	mux := http.NewServeMux()
	mux.Handle("/runner-event", srv.rateLimit(srv.requireToken(http.HandlerFunc(srv.handleEvent))))

	tok, err := minter.Mint(10042, 88)
	require.NoError(t, err)
	body, _ := json.Marshal(EventPayload{Phase: "completed", RunnerName: "gh-runner-test-10042"})

	doPost := func() int {
		req := httptest.NewRequest(http.MethodPost, "/runner-event", bytes.NewReader(body))
		req.RemoteAddr = "10.0.0.1:54321"
		req.Header.Set("Authorization", "Bearer "+tok)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		return w.Code
	}

	require.Equal(t, http.StatusNoContent, doPost())
	require.Equal(t, http.StatusNoContent, doPost())
	require.Equal(t, http.StatusTooManyRequests, doPost())
}

// TestRateLimit_PerIPIsolated verifies one noisy IP can't starve a
// well-behaved one.
func TestRateLimit_PerIPIsolated(t *testing.T) {
	t.Parallel()
	verifier, err := runnertoken.NewVerifier([]byte(testHMACSecret), "test-scaleset")
	require.NoError(t, err)
	minter, err := runnertoken.NewMinter([]byte(testHMACSecret), time.Hour, "test-scaleset")
	require.NoError(t, err)
	srv := New(Config{
		HTTPAddr:         "ignored",
		Verifier:         verifier,
		RunnerNamePrefix: "gh-runner-test-",
		RateLimitRPS:     0.001,
		RateLimitBurst:   1,
	}, &fakePool{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	mux := http.NewServeMux()
	mux.Handle("/runner-event", srv.rateLimit(srv.requireToken(http.HandlerFunc(srv.handleEvent))))

	tok, err := minter.Mint(10042, 88)
	require.NoError(t, err)
	body, _ := json.Marshal(EventPayload{Phase: "completed", RunnerName: "gh-runner-test-10042"})

	post := func(remote string) int {
		req := httptest.NewRequest(http.MethodPost, "/runner-event", bytes.NewReader(body))
		req.RemoteAddr = remote
		req.Header.Set("Authorization", "Bearer "+tok)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		return w.Code
	}

	require.Equal(t, http.StatusNoContent, post("10.0.0.1:1111"))
	require.Equal(t, http.StatusTooManyRequests, post("10.0.0.1:2222")) // same IP, exhausted
	require.Equal(t, http.StatusNoContent, post("10.0.0.2:3333"))       // different IP, fresh bucket
}

// TestRateLimit_DisabledBypassesLimiter verifies the Disabled flag
// turns the middleware into a no-op pass-through.
func TestRateLimit_DisabledBypassesLimiter(t *testing.T) {
	t.Parallel()
	verifier, err := runnertoken.NewVerifier([]byte(testHMACSecret), "test-scaleset")
	require.NoError(t, err)
	minter, err := runnertoken.NewMinter([]byte(testHMACSecret), time.Hour, "test-scaleset")
	require.NoError(t, err)
	srv := New(Config{
		HTTPAddr:          "ignored",
		Verifier:          verifier,
		RunnerNamePrefix:  "gh-runner-test-",
		RateLimitDisabled: true,
		RateLimitRPS:      0.001,
		RateLimitBurst:    1,
	}, &fakePool{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	mux := http.NewServeMux()
	mux.Handle("/runner-event", srv.rateLimit(srv.requireToken(http.HandlerFunc(srv.handleEvent))))

	tok, err := minter.Mint(10042, 88)
	require.NoError(t, err)
	body, _ := json.Marshal(EventPayload{Phase: "completed", RunnerName: "gh-runner-test-10042"})

	for i := range 5 {
		req := httptest.NewRequest(http.MethodPost, "/runner-event", bytes.NewReader(body))
		req.RemoteAddr = "10.0.0.1:54321"
		req.Header.Set("Authorization", "Bearer "+tok)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		require.Equal(t, http.StatusNoContent, w.Code, "request %d should not be rate-limited when disabled", i)
	}
}

// TestRateLimit_TrustedProxy_XForwardedFor verifies that XFF is honored
// when the direct peer is in a trusted CIDR, with right-to-left walking
// past additional trusted hops to find the real client.
func TestRateLimit_TrustedProxy_XForwardedFor(t *testing.T) {
	t.Parallel()
	verifier, err := runnertoken.NewVerifier([]byte(testHMACSecret), "test-scaleset")
	require.NoError(t, err)
	minter, err := runnertoken.NewMinter([]byte(testHMACSecret), time.Hour, "test-scaleset")
	require.NoError(t, err)
	srv := New(Config{
		HTTPAddr:         "ignored",
		Verifier:         verifier,
		RunnerNamePrefix: "gh-runner-test-",
		RateLimitRPS:     0.001,
		RateLimitBurst:   1,
		TrustedProxies:   []string{"127.0.0.0/8", "10.0.0.0/8"},
	}, &fakePool{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	mux := http.NewServeMux()
	mux.Handle("/runner-event", srv.rateLimit(srv.requireToken(http.HandlerFunc(srv.handleEvent))))

	tok, err := minter.Mint(10042, 88)
	require.NoError(t, err)
	body, _ := json.Marshal(EventPayload{Phase: "completed", RunnerName: "gh-runner-test-10042"})

	post := func(remote, xff string) int {
		req := httptest.NewRequest(http.MethodPost, "/runner-event", bytes.NewReader(body))
		req.RemoteAddr = remote
		req.Header.Set("X-Forwarded-For", xff)
		req.Header.Set("Authorization", "Bearer "+tok)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		return w.Code
	}

	// Same trusted edge, two different "real" clients via XFF — both
	// should pass (separate buckets), proving XFF is the keying signal.
	require.Equal(t, http.StatusNoContent, post("127.0.0.1:1", "203.0.113.5"))
	require.Equal(t, http.StatusNoContent, post("127.0.0.1:2", "203.0.113.6"))
	// Repeat client 203.0.113.5 — should be throttled.
	require.Equal(t, http.StatusTooManyRequests, post("127.0.0.1:3", "203.0.113.5"))
	// XFF chain with trusted hop on the right — walk past it to find
	// the real client (198.51.100.10), which is fresh.
	require.Equal(t, http.StatusNoContent, post("127.0.0.1:4", "198.51.100.10, 10.0.0.5"))
}

// TestRateLimit_UntrustedPeer_IgnoresXFF verifies that XFF set by a
// direct (untrusted) connection is NOT honored — otherwise any attacker
// could rotate XFF to dodge per-IP throttling.
func TestRateLimit_UntrustedPeer_IgnoresXFF(t *testing.T) {
	t.Parallel()
	verifier, err := runnertoken.NewVerifier([]byte(testHMACSecret), "test-scaleset")
	require.NoError(t, err)
	minter, err := runnertoken.NewMinter([]byte(testHMACSecret), time.Hour, "test-scaleset")
	require.NoError(t, err)
	srv := New(Config{
		HTTPAddr:         "ignored",
		Verifier:         verifier,
		RunnerNamePrefix: "gh-runner-test-",
		RateLimitRPS:     0.001,
		RateLimitBurst:   1,
		// 10.0.0.0/8 is trusted, but the attacker connects from
		// 198.51.100.x which is NOT in the trusted set.
		TrustedProxies: []string{"10.0.0.0/8"},
	}, &fakePool{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	mux := http.NewServeMux()
	mux.Handle("/runner-event", srv.rateLimit(srv.requireToken(http.HandlerFunc(srv.handleEvent))))

	tok, err := minter.Mint(10042, 88)
	require.NoError(t, err)
	body, _ := json.Marshal(EventPayload{Phase: "completed", RunnerName: "gh-runner-test-10042"})

	post := func(remote, xff string) int {
		req := httptest.NewRequest(http.MethodPost, "/runner-event", bytes.NewReader(body))
		req.RemoteAddr = remote
		req.Header.Set("X-Forwarded-For", xff)
		req.Header.Set("Authorization", "Bearer "+tok)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		return w.Code
	}

	require.Equal(t, http.StatusNoContent, post("198.51.100.1:1", "1.2.3.4"))
	// Attacker rotates XFF — should still be throttled by RemoteAddr.
	require.Equal(t, http.StatusTooManyRequests, post("198.51.100.1:2", "5.6.7.8"))
}

// TestRateLimit_TrustedProxy_CFConnectingIP verifies the
// Cf-Connecting-Ip header takes precedence over XFF when set by a
// trusted edge.
func TestRateLimit_TrustedProxy_CFConnectingIP(t *testing.T) {
	t.Parallel()
	verifier, err := runnertoken.NewVerifier([]byte(testHMACSecret), "test-scaleset")
	require.NoError(t, err)
	minter, err := runnertoken.NewMinter([]byte(testHMACSecret), time.Hour, "test-scaleset")
	require.NoError(t, err)
	srv := New(Config{
		HTTPAddr:         "ignored",
		Verifier:         verifier,
		RunnerNamePrefix: "gh-runner-test-",
		RateLimitRPS:     0.001,
		RateLimitBurst:   1,
		TrustedProxies:   []string{"10.0.0.0/8"},
	}, &fakePool{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	mux := http.NewServeMux()
	mux.Handle("/runner-event", srv.rateLimit(srv.requireToken(http.HandlerFunc(srv.handleEvent))))

	tok, err := minter.Mint(10042, 88)
	require.NoError(t, err)
	body, _ := json.Marshal(EventPayload{Phase: "completed", RunnerName: "gh-runner-test-10042"})

	post := func(cfIP, xff string) int {
		req := httptest.NewRequest(http.MethodPost, "/runner-event", bytes.NewReader(body))
		req.RemoteAddr = "10.0.0.1:443"
		req.Header.Set("Cf-Connecting-Ip", cfIP)
		req.Header.Set("X-Forwarded-For", xff)
		req.Header.Set("Authorization", "Bearer "+tok)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		return w.Code
	}

	// First client via CF-Connecting-Ip — passes.
	require.Equal(t, http.StatusNoContent, post("203.0.113.7", "1.1.1.1"))
	// Same CF-Connecting-Ip with different XFF — still throttled (CF
	// takes precedence over XFF, so the bucket is shared).
	require.Equal(t, http.StatusTooManyRequests, post("203.0.113.7", "2.2.2.2"))
}

// TestServe_RejectsBadTrustedProxyCIDR verifies a misconfigured CIDR
// fails Serve loud at startup rather than silently disabling proxy
// trust.
func TestServe_RejectsBadTrustedProxyCIDR(t *testing.T) {
	t.Parallel()
	verifier, err := runnertoken.NewVerifier([]byte(testHMACSecret), "test-scaleset")
	require.NoError(t, err)
	srv := New(Config{
		HTTPAddr:         "127.0.0.1:0",
		Verifier:         verifier,
		RunnerNamePrefix: "x-",
		TrustedProxies:   []string{"not-a-cidr"},
	}, &fakePool{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	err = srv.Serve(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "trusted_proxies")
}

// TestServe_RejectsEmptyPrefix prevents a misconfigured deploy where
// every event would 400 because VMID derivation can't work.
func TestServe_RejectsEmptyPrefix(t *testing.T) {
	t.Parallel()
	verifier, err := runnertoken.NewVerifier([]byte(testHMACSecret), "test-scaleset")
	require.NoError(t, err)
	srv := New(Config{
		HTTPAddr: "127.0.0.1:0",
		Verifier: verifier,
	}, &fakePool{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	err = srv.Serve(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "runner_name_prefix")
}
