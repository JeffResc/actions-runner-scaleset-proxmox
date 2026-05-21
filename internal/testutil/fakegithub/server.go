// Package fakegithub is an httptest-backed fake of the subset of the
// GitHub REST API the orchestrator's `gh` reconciler talks to:
//
//   - GET  /orgs/{org}/actions/runners
//   - GET  /repos/{owner}/{repo}/actions/runners
//   - DELETE /orgs/{org}/actions/runners/{id}
//   - DELETE /repos/{owner}/{repo}/actions/runners/{id}
//
// The scaleset library's listener handshake (the
// /actions/runner-registration token exchange, message-session
// long-polling, and JIT-config minting) is intentionally NOT
// implemented here yet — the full-binary e2e harness will add those
// endpoints when it needs them. This package's current scope is what
// the gh reconciler needs in isolation.
//
// New tests should prefer this package over hand-rolled httptest
// fixtures. The existing fakeRunner / runnersServer / newTestClient
// helpers in internal/gh/reconciler_test.go cover the same surface
// and will be migrated incrementally — keeping that as a follow-up
// avoids a 40+ line mechanical churn here that doesn't enable
// anything new.
package fakegithub

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/go-github/v84/github"
)

// Runner is the test fixture's view of a GitHub Actions runner. It maps
// 1:1 to the fields the reconciler inspects.
type Runner struct {
	ID     int64
	Name   string
	Status string // "online" | "offline"
	Busy   bool
}

// Options configures the fake.
type Options struct {
	// InitialRunners are seeded into the server's runner table at
	// startup. Tests can mutate the table later via SetRunner.
	InitialRunners []Runner
}

// Server is the fake GitHub REST API. Construct with New; the embedded
// httptest.Server provides Close() and a usable URL.
type Server struct {
	*httptest.Server

	mu        sync.Mutex
	runners   map[int64]Runner
	deletions []int64
}

// New starts the fake and registers cleanup on t.Cleanup.
func New(t testing.TB, opts Options) *Server {
	t.Helper()
	s := &Server{runners: map[int64]Runner{}}
	for _, r := range opts.InitialRunners {
		s.runners[r.ID] = r
	}
	s.Server = httptest.NewServer(s.routes())
	t.Cleanup(s.Close)
	return s
}

// RESTBaseURL returns the URL suitable for passing as
// githubauth.PATConfig.RESTBaseURL (trailing slash included — go-github
// requires it).
func (s *Server) RESTBaseURL() string { return s.URL + "/" }

// SetRunner upserts a runner into the table. Use this to flip a runner
// from idle to busy mid-test, or to add new runners after a
// JobAvailable message would normally do so.
func (s *Server) SetRunner(r Runner) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runners[r.ID] = r
}

// RunnerDeletions returns the IDs of runners DELETE'd through the API,
// in the order they were observed. The slice is a copy — safe to read
// without holding the server's lock.
func (s *Server) RunnerDeletions() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int64, len(s.deletions))
	copy(out, s.deletions)
	return out
}

func (s *Server) routes() http.Handler {
	r := chi.NewRouter()

	// List runners — both org and repo scopes return the same shape.
	listHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		body := struct {
			TotalCount int              `json:"total_count"`
			Runners    []*github.Runner `json:"runners"`
		}{TotalCount: len(s.runners)}
		// Sorted iteration would be ideal for deterministic test output,
		// but the reconciler doesn't rely on ordering — keep it simple.
		for _, r := range s.runners {
			id, name, status, busy := r.ID, r.Name, r.Status, r.Busy
			body.Runners = append(body.Runners, &github.Runner{
				ID:     &id,
				Name:   &name,
				Status: &status,
				Busy:   &busy,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	})
	r.Get("/orgs/{org}/actions/runners", listHandler)
	r.Get("/repos/{owner}/{repo}/actions/runners", listHandler)

	deleteHandler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		raw := chi.URLParam(req, "id")
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			http.Error(w, "bad id", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, ok := s.runners[id]; !ok {
			http.Error(w, fmt.Sprintf("runner %d not found", id), http.StatusNotFound)
			return
		}
		delete(s.runners, id)
		s.deletions = append(s.deletions, id)
		w.WriteHeader(http.StatusNoContent)
	})
	r.Delete("/orgs/{org}/actions/runners/{id}", deleteHandler)
	r.Delete("/repos/{owner}/{repo}/actions/runners/{id}", deleteHandler)

	// Anything else: 501 with a clear message so tests fail loud
	// instead of silently routing to a generic 404 the orchestrator
	// might mis-classify.
	r.NotFound(func(w http.ResponseWriter, req *http.Request) {
		http.Error(w,
			"fakegithub: endpoint not implemented: "+req.Method+" "+req.URL.Path+
				" (see internal/testutil/fakegithub/server.go for the supported subset)",
			http.StatusNotImplemented)
	})

	return r
}
