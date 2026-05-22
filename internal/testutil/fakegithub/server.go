// Package fakegithub is an httptest-backed fake of the GitHub
// endpoints the orchestrator talks to:
//
//   - GitHub REST runners API (used by internal/gh's reconciler):
//     GET / DELETE /orgs/{org}/actions/runners and the /repos/...
//     equivalents.
//
//   - The scaleset library's listener handshake (used by
//     internal/app's leader plane): registration-token exchange,
//     /actions/runner-registration, runner-scale-set
//     lookup/create, message-session lifecycle with long-polled
//     GetMessage, acquirejobs, and generatejitconfig. The full
//     wire surface lives in scaleset.go.
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

	// ScaleSet configures the scale-set the fake serves on the
	// scaleset-library endpoints. Zero-value defaults to ID=42,
	// Name="test-scaleset", RunnerGroupID=1.
	ScaleSet ScaleSetOptions
}

// Server is the fake GitHub REST API. Construct with New; the embedded
// httptest.Server provides Close() and a usable URL.
type Server struct {
	*httptest.Server

	mu        sync.Mutex
	runners   map[int64]Runner
	deletions []int64

	// scaleset-library state
	scaleSet     fakeRunnerScaleSet
	adminToken   string
	session      *sessionState
	jitMintCount int

	// statistics is what the fake reports in every session-create,
	// session-refresh, and GetMessage envelope. Zero-valued by default;
	// tests use SetStatistics to drive the orchestrator's listener
	// loop — TotalAssignedJobs is what HandleDesiredRunnerCount keys
	// on, which is the only signal that triggers a Hot → Assigned
	// transition end-to-end.
	statistics fakeRunnerScaleSetStatistic

	// nextMessageID is the per-server message-ID counter used by the
	// PostJob* helpers. Each posted envelope claims a unique ID so the
	// listener's lastMessageID tracking advances naturally.
	nextMessageID int
}

// New starts the fake and registers cleanup on t.Cleanup.
func New(t testing.TB, opts Options) *Server {
	t.Helper()
	if opts.ScaleSet.ID == 0 {
		opts.ScaleSet.ID = 42
	}
	if opts.ScaleSet.Name == "" {
		opts.ScaleSet.Name = "test-scaleset"
	}
	if opts.ScaleSet.RunnerGroupID == 0 {
		opts.ScaleSet.RunnerGroupID = 1
	}
	s := &Server{
		runners: map[int64]Runner{},
		scaleSet: fakeRunnerScaleSet{
			ID:            opts.ScaleSet.ID,
			Name:          opts.ScaleSet.Name,
			RunnerGroupID: opts.ScaleSet.RunnerGroupID,
		},
		adminToken: mintAdminJWT(),
	}
	for _, r := range opts.InitialRunners {
		s.runners[r.ID] = r
	}
	s.Server = httptest.NewServer(s.routes())
	t.Cleanup(s.Close)
	return s
}

// ConfigURL returns a URL suitable for githubauth.PATConfig.ConfigURL.
// The scaleset library parses the path as <org> (1 segment) or
// <owner>/<repo> (2 segments); we always serve an org-style URL so
// e2e tests can configure cfg.github.scope.org and have the
// orchestrator's listener handshake land on this server.
func (s *Server) ConfigURL(org string) string { return s.URL + "/" + org }

// ScaleSetID returns the runner-scale-set ID the fake claims when the
// orchestrator looks it up by name. Test assertions on JIT mint
// records / runner IDs can reference this without hard-coding.
func (s *Server) ScaleSetID() int { return s.scaleSet.ID }

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

	// ---------------------------------------------------------------
	// Scaleset library wire endpoints
	//
	// The scaleset client first hits the GitHub-style auth handshake
	// on the configured GitHubConfigURL (which the library prepends
	// with /api/v3 for non-github.com hosts — that's our case in
	// tests). After /actions/runner-registration returns an
	// ActionsServiceURL, all subsequent calls go to
	// url.JoinPath(actionsServiceURL, "/_apis/runtime/..."). We
	// return our own URL as ActionsServiceURL so everything stays on
	// the same httptest server.
	// ---------------------------------------------------------------

	// /api/v3 prefix — GHES-style routing.
	r.Post("/api/v3/orgs/{org}/actions/runners/registration-token", s.handleRegistrationToken)
	r.Post("/api/v3/repos/{owner}/{repo}/actions/runners/registration-token", s.handleRegistrationToken)
	r.Post("/api/v3/enterprises/{ent}/actions/runners/registration-token", s.handleRegistrationToken)
	r.Post("/api/v3/actions/runner-registration", s.handleRunnerRegistration)

	// _apis/runtime — actions service surface (mounted under /).
	r.Get("/_apis/runtime/runnerscalesets", s.handleScaleSetLookup)
	r.Post("/_apis/runtime/runnerscalesets", s.handleScaleSetCreate)
	r.Get("/_apis/runtime/runnergroups/", s.handleRunnerGroupLookup)
	r.Get("/_apis/runtime/runnergroups", s.handleRunnerGroupLookup)
	r.Post("/_apis/runtime/runnerscalesets/{id}/sessions", s.handleSessionCreate)
	r.Patch("/_apis/runtime/runnerscalesets/{id}/sessions/{sid}", s.handleSessionRefresh)
	r.Delete("/_apis/runtime/runnerscalesets/{id}/sessions/{sid}", s.handleSessionDelete)
	r.Post("/_apis/runtime/runnerscalesets/{id}/acquirejobs", s.handleAcquireJobs)
	r.Post("/_apis/runtime/runnerscalesets/{id}/generatejitconfig", s.handleGenerateJIT)
	r.Delete("/_apis/runtime/runners/{id}", s.handleRunnerDelete)
	// scaleset.Client.RemoveRunner uses the distributed-task surface
	// (a separate path from /_apis/runtime/runners). Real GitHub
	// serves both; we mount the same handler so OnRunnerOrphaned ends
	// up in s.deletions regardless of which client surface the
	// orchestrator chose.
	r.Delete("/_apis/distributedtask/pools/{pool}/agents/{id}", s.handleRunnerDelete)

	// Custom message-queue path — the URL we return from session
	// create. The path is opaque to the library; we choose it.
	r.Get("/_messages/{sid}", s.handleGetMessage)
	r.Delete("/_messages/{sid}/{mid}", s.handleDeleteMessage)

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
