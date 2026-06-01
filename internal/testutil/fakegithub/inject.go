package fakegithub

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// httpFault is a count-bounded canned failure for a single endpoint
// family. While count > 0 the matched handler replies with status
// (plus, for the list endpoint, a Retry-After header) instead of its
// normal behaviour, decrementing count each time. A zero-value
// httpFault (count == 0) matches nothing.
type httpFault struct {
	status     int
	retryAfter int // seconds; emitted as Retry-After when > 0
	count      int
}

// InjectListFailure makes the next `count` runner-list calls fail with
// the given HTTP status and a Retry-After header (seconds). Use a 429
// to exercise the reconciler's rate-limit backoff, or a 5xx for the
// transient-server-error retry path.
func (s *Server) InjectListFailure(status, retryAfterSeconds, count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listFault = httpFault{status: status, retryAfter: retryAfterSeconds, count: count}
}

// InjectDeleteFailure makes the next `count` runner-DELETE
// (deregister) calls fail with the given HTTP status and a GitHub-
// shaped JSON error body. This drives the destroy path's
// OnRunnerOrphaned failure branch — without it the fake always
// succeeds and the leaked-registration branch is unreachable.
func (s *Server) InjectDeleteFailure(status, count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteFault = httpFault{status: status, count: count}
}

// takeListFaultLocked returns the active list fault (if any) and
// decrements its budget. Caller must hold s.mu.
func (s *Server) takeListFaultLocked() (httpFault, bool) {
	if s.listFault.count <= 0 {
		return httpFault{}, false
	}
	f := s.listFault
	s.listFault.count--
	return f, true
}

// takeDeleteFaultLocked mirrors takeListFaultLocked for the delete
// endpoint. Caller must hold s.mu.
func (s *Server) takeDeleteFaultLocked() (httpFault, bool) {
	if s.deleteFault.count <= 0 {
		return httpFault{}, false
	}
	f := s.deleteFault
	s.deleteFault.count--
	return f, true
}

// writeGitHubError writes GitHub's canonical REST error envelope
// ({"message": ..., "documentation_url": ...}) with the given status —
// the shape go-github parses into an *ErrorResponse, unlike a plain-
// text http.Error body.
func writeGitHubError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"message":           message,
		"documentation_url": "https://docs.github.com/rest",
	})
}

// writeRateLimitHeaders stamps the standard GitHub rate-limit headers
// on a response. remaining is the advertised budget left; reset is a
// unix timestamp. Always emitted on list responses so the reconciler
// sees production-shaped headers even on the success path.
func writeRateLimitHeaders(w http.ResponseWriter, limit, remaining int, resetUnix int64) {
	h := w.Header()
	h.Set("X-RateLimit-Limit", strconv.Itoa(limit))
	h.Set("X-RateLimit-Remaining", strconv.Itoa(remaining))
	h.Set("X-RateLimit-Reset", strconv.FormatInt(resetUnix, 10))
}

// linkNextHeader returns a GitHub-style Link header value pointing at
// the next page of the given path, e.g.
// `<https://host/orgs/o/actions/runners?page=2&per_page=100>; rel="next"`.
func linkNextHeader(base, path string, nextPage, perPage int) string {
	return fmt.Sprintf(`<%s%s?page=%d&per_page=%d>; rel="next"`, base, path, nextPage, perPage)
}
