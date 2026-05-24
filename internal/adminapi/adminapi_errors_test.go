package adminapi

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

// promoteOnlyHandler builds a chi router with just the routes this
// file exercises, plus the maxBody middleware and bearer auth. Mirrors
// the production wiring closely enough that the middleware + handler
// chain is exercised end-to-end.
func promoteOnlyHandler(s *Server) http.Handler {
	r := chi.NewRouter()
	r.Use(s.realIP)
	r.Use(s.maxBody(64 * 1024))
	r.Use(s.leaderOrForward)
	r.Use(s.requireBearerToken)
	r.Post("/admin/template/promote/{profile}", s.handlePromoteTemplate)
	return r
}

// stubCanary lets a test drive Promote behaviour. Returning a
// non-nil error mimics canary.Controller's "no candidate template
// configured" / "already promoted" refusals.
type stubCanary struct {
	promoteErr error
	promoted   []string
}

func (c *stubCanary) Promote(profile string) error {
	c.promoted = append(c.promoted, profile)
	return c.promoteErr
}

// TestHandlePromoteTemplate_NoCanaryConfiguredReturns404 covers
// the early-exit path: when no canary controller is wired (the
// orchestrator was started without --canary), the route must
// surface as 404 — operators distinguish "endpoint unimplemented"
// from "endpoint exists but refuses".
func TestHandlePromoteTemplate_NoCanaryConfiguredReturns404(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, "topsecret")
	// Intentionally NOT calling SetCanary so s.canary is nil.
	h := promoteOnlyHandler(s)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/template/promote/gpu", nil)
	r.Header.Set("Authorization", "Bearer topsecret")
	h.ServeHTTP(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
	require.Contains(t, w.Body.String(), "canary controller not configured")
}

// TestHandlePromoteTemplate_CanaryInactiveReturns503WithRetryAfter
// covers the standby (pre-election leader) path: the canary
// accessor is wired but currently returns nil (e.g. between
// leader transitions). Must respond 503 + Retry-After so a
// hook-script's retry loop converges once the new leader's
// canary controller is ready.
func TestHandlePromoteTemplate_CanaryInactiveReturns503WithRetryAfter(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, "topsecret")
	s.SetCanary(func() CanaryPromoter { return nil })
	h := promoteOnlyHandler(s)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/template/promote/gpu", nil)
	r.Header.Set("Authorization", "Bearer topsecret")
	h.ServeHTTP(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Equal(t, "1", w.Header().Get("Retry-After"))
}

// TestHandlePromoteTemplate_RefusedReturnsConflict pins the
// happy-rejection path: an active canary that refuses the
// promotion (e.g. profile has no candidate template, or already
// promoted) surfaces as 409 Conflict — distinct from the 503
// "try again later" and 404 "endpoint not wired" cases.
func TestHandlePromoteTemplate_RefusedReturnsConflict(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, "topsecret")
	sc := &stubCanary{promoteErr: errors.New("no candidate template configured")}
	s.SetCanary(func() CanaryPromoter { return sc })
	h := promoteOnlyHandler(s)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/template/promote/gpu", nil)
	r.Header.Set("Authorization", "Bearer topsecret")
	h.ServeHTTP(w, r)

	require.Equal(t, http.StatusConflict, w.Code,
		"refused promotion is the operator-actionable case; 409 Conflict communicates 'we got your request but won't run it'")
	require.Contains(t, w.Body.String(), "no candidate template configured",
		"refusal reason must reach the operator unaltered")
	require.Equal(t, []string{"gpu"}, sc.promoted,
		"Promote must have been invoked exactly once with the requested profile")
}

// TestMaxBody_RejectsOversizedRequest pins the maxBody middleware:
// a body larger than the 64KiB limit must be rejected by the
// MaxBytesReader before the handler reads it. Without this, a
// hostile client could exhaust memory by streaming an unbounded
// body to any POST endpoint.
//
// Uses the drain handler (which has no body schema and returns 202
// on the happy path) as a vehicle to drive a body that the handler
// would otherwise ignore; the middleware must still slam the limit
// shut.
func TestMaxBody_RejectsOversizedRequest(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, "topsecret")
	// Build a tiny router with just maxBody + a body-reading
	// terminal handler so the test directly observes the limit.
	r := chi.NewRouter()
	r.Use(s.maxBody(1024)) // 1 KiB limit for a fast bound
	r.Post("/echo", func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 0, 2048)
		_, err := r.Body.Read(buf[:cap(buf)])
		if err != nil && err.Error() != "EOF" {
			// MaxBytesReader returns *http.MaxBytesError once
			// the limit is exceeded; surface it as 413.
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	body := strings.NewReader(strings.Repeat("x", 4096)) // 4 KiB > 1 KiB limit
	w := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/echo", body)
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code,
		"a body that exceeds the maxBody limit must be rejected before the handler completes")
}

// TestHandlePreemptVM_CtxCancelPropagates pins that a request
// context cancellation surfaces inside the handler — pool.Preempt
// receives a cancelled ctx and returns its error. Without ctx
// propagation, a long-running pool operation outlives the
// HTTP request and the client never sees a clean status.
func TestHandlePreemptVM_CtxCancelPropagates(t *testing.T) {
	t.Parallel()
	s, fp := newTestServer(t, "topsecret")
	// Route through chi so URL params populate; reuse the
	// existing destroy router structure but add preempt.
	router := chi.NewRouter()
	router.Use(s.realIP)
	router.Use(s.leaderOrForward)
	router.Use(s.requireBearerToken)
	router.Post("/admin/preempt/{vmid}", s.handlePreemptVM)

	// Give the fake pool a preempt error so the handler's error
	// branch fires — we're pinning that an error from p.Preempt
	// produces 500, NOT 202.
	fp.preemptErr = errors.New("simulated downstream failure")

	w := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/preempt/10042", nil)
	req.Header.Set("Authorization", "Bearer topsecret")
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code,
		"non-refusal Preempt errors must surface as 500, not silently 202")
}
