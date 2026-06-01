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
//
// Multi-scaleset support (issue #1 / PR #207 follow-up): pass
// Options.Scalesets to host N distinct scale sets behind one
// httptest server. Each entry gets its own ID, session, statistics,
// JIT mint counter, and runner-table partition (no cross-contamination).
// The singular Options.ScaleSet form keeps working for the existing
// single-scaleset tests and is normalised into a 1-entry Scalesets
// list at construction.
package fakegithub

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/go-github/v88/github"
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

	// ScaleSet configures the SINGLE scale set the fake serves on the
	// scaleset-library endpoints. Mutually exclusive with Scalesets.
	// Zero-value defaults to ID=42, Name="test-scaleset",
	// RunnerGroupID=1.
	ScaleSet ScaleSetOptions

	// Scalesets configures N scale sets behind one httptest server
	// (issue #1 multi-scaleset runtime tests). Mutually exclusive with
	// ScaleSet. Each entry has its own ID, session, statistics, and
	// JIT mint counter; the orchestrator's per-scaleset workers route
	// to the right entry via the URL {id} param.
	Scalesets []ScaleSetOptions

	// PageSize, when > 0, makes the runner-list endpoint paginate: each
	// response returns at most PageSize runners and, when more remain,
	// sets a GitHub-style `Link: ...; rel="next"` header so the
	// reconciler's pagination loop follows it. Zero (the default)
	// returns every runner in one page (back-compat).
	PageSize int
}

// Server is the fake GitHub REST API. Construct with New; the embedded
// httptest.Server provides Close() and a usable URL.
type Server struct {
	*httptest.Server

	mu        sync.Mutex
	runners   map[int64]Runner
	deletions []int64

	// pageSize mirrors Options.PageSize: 0 = single-page (back-compat).
	pageSize int

	// listFault, when count > 0, makes the next `count` runner-list
	// calls return `status` (typically 429) with a Retry-After header,
	// exercising the reconciler's rate-limit/5xx retry+backoff path.
	// Each matched call decrements count.
	listFault httpFault

	// deleteFault, when count > 0, makes the next `count` runner-DELETE
	// calls return `status` with a GitHub-shaped JSON error body —
	// used to exercise the destroy path's OnRunnerOrphaned failure
	// branch (a leaked GitHub registration). Each matched call
	// decrements count.
	deleteFault httpFault

	// scalesets is the per-scaleset state, indexed by scaleset name
	// (the operator-facing identifier). scalesetsByID is a parallel
	// view for routing handlers that key off the URL {id} param.
	// Both maps are populated in New and never resized afterwards,
	// so reads happen without a write lock once construction is done
	// (the surrounding mu guards per-entry mutable state instead).
	scalesets     map[string]*scalesetEntry
	scalesetsByID map[int]*scalesetEntry

	adminToken    string
	nextMessageID int
}

// scalesetEntry is the per-scaleset state. Each entry has its own
// session lifecycle, statistics, and JIT mint counter so multi-
// scaleset tests can assert on each scale set independently.
type scalesetEntry struct {
	spec         fakeRunnerScaleSet
	session      *sessionState
	statistics   fakeRunnerScaleSetStatistic
	jitMintCount int
	// jitMintsByName maps the runner name passed to
	// GenerateJitRunnerConfig to the synthesised runner ID handed
	// back. Concurrent-job tests use this to learn the JIT-mint ID
	// for each VM after awaitNAssignedVMs returns, so they can
	// drive JobStarted/JobCompleted with the same RunnerID the
	// orchestrator already stamped on the row (avoiding a race
	// with the gh.Reconciler's promote / force-destroy path).
	jitMintsByName map[string]int
}

// New starts the fake and registers cleanup on t.Cleanup.
func New(t testing.TB, opts Options) *Server {
	t.Helper()
	specs := normaliseScalesetOptions(opts)
	s := &Server{
		runners:       map[int64]Runner{},
		scalesets:     make(map[string]*scalesetEntry, len(specs)),
		scalesetsByID: make(map[int]*scalesetEntry, len(specs)),
		adminToken:    mintAdminJWT(),
		pageSize:      opts.PageSize,
	}
	for _, spec := range specs {
		entry := &scalesetEntry{spec: spec, jitMintsByName: map[string]int{}}
		s.scalesets[spec.Name] = entry
		s.scalesetsByID[spec.ID] = entry
	}
	for _, r := range opts.InitialRunners {
		s.runners[r.ID] = r
	}
	s.Server = httptest.NewServer(s.routes())
	t.Cleanup(s.Close)
	return s
}

// normaliseScalesetOptions resolves the singular-vs-plural Options
// shape into a deterministic ScaleSetOptions list with stable
// defaults. Auto-assigns IDs / RunnerGroupIDs / Names when the
// caller omitted them so two unnamed scalesets don't collide.
// allocateID picks the next unused ID from a monotonic counter,
// skipping any value already in seen. The caller is responsible
// for inserting the returned ID into seen and advancing its own
// nextID cursor. Extracted from normaliseScalesetOptions so the
// allocator can be unit-tested independently and the multi-
// scaleset branch's nesting drops one level.
func allocateID(seen map[int]struct{}, nextID *int) int {
	for {
		if _, used := seen[*nextID]; !used {
			id := *nextID
			*nextID++
			return id
		}
		*nextID++
	}
}

// optsToList folds the singular ScaleSet form into the slice
// form so the downstream loop can handle both shapes uniformly.
// Singular-and-plural-set is rejected at the call site (it's
// the only configuration ambiguity the helper cannot resolve).
func optsToList(opts Options) []ScaleSetOptions {
	if len(opts.Scalesets) > 0 {
		return opts.Scalesets
	}
	return []ScaleSetOptions{opts.ScaleSet}
}

func normaliseScalesetOptions(opts Options) []fakeRunnerScaleSet {
	if len(opts.Scalesets) > 0 && (opts.ScaleSet != ScaleSetOptions{}) {
		panic("fakegithub: Options.ScaleSet and Options.Scalesets are mutually exclusive")
	}
	list := optsToList(opts)
	out := make([]fakeRunnerScaleSet, 0, len(list))
	seenName := make(map[string]struct{}, len(list))
	seenID := make(map[int]struct{}, len(list))
	nextID := 42
	singular := len(opts.Scalesets) == 0
	for i, ss := range list {
		name := ss.Name
		if name == "" {
			if singular {
				name = "test-scaleset"
			} else {
				name = fmt.Sprintf("test-scaleset-%d", i)
			}
		}
		if _, dup := seenName[name]; dup {
			panic(fmt.Sprintf("fakegithub: duplicate scaleset name %q in Options.Scalesets", name))
		}
		seenName[name] = struct{}{}
		id := ss.ID
		if id == 0 {
			id = allocateID(seenID, &nextID)
		}
		if _, dup := seenID[id]; dup {
			panic(fmt.Sprintf("fakegithub: duplicate scaleset id %d in Options.Scalesets", id))
		}
		seenID[id] = struct{}{}
		rg := ss.RunnerGroupID
		if rg == 0 {
			rg = 1
		}
		out = append(out, fakeRunnerScaleSet{
			ID:            id,
			Name:          name,
			RunnerGroupID: rg,
		})
	}
	return out
}

// ConfigURL returns a URL suitable for githubauth.PATConfig.ConfigURL.
// The scaleset library parses the path as <org> (1 segment) or
// <owner>/<repo> (2 segments); we always serve an org-style URL so
// e2e tests can configure cfg.github.scope.org and have the
// orchestrator's listener handshake land on this server.
func (s *Server) ConfigURL(org string) string { return s.URL + "/" + org }

// ScaleSetID returns the runner-scale-set ID the fake claims for the
// single registered scaleset. Panics in multi-scaleset configs — use
// ScaleSetIDFor(name) instead.
func (s *Server) ScaleSetID() int {
	return s.onlyEntry("ScaleSetID").spec.ID
}

// ScaleSetIDFor returns the ID for the named scaleset (multi-scaleset
// tests). Panics on unknown names so test sequencing bugs surface
// loudly.
func (s *Server) ScaleSetIDFor(name string) int {
	return s.entryFor(name, "ScaleSetIDFor").spec.ID
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

// onlyEntry returns the single configured scaleset entry or panics
// if there are multiple. Used by the legacy singular accessors so
// they fail loud when a test mistakes them for the multi-aware
// variants.
func (s *Server) onlyEntry(caller string) *scalesetEntry {
	if len(s.scalesets) != 1 {
		panic(fmt.Sprintf("fakegithub: %s called on multi-scaleset server (%d entries); use %sFor(name) instead",
			caller, len(s.scalesets), caller))
	}
	for _, e := range s.scalesets {
		return e
	}
	return nil // unreachable
}

// entryFor returns the named entry or panics. Tests should never
// reference a scaleset they didn't configure; the panic surfaces the
// typo immediately.
func (s *Server) entryFor(name, caller string) *scalesetEntry {
	e, ok := s.scalesets[name]
	if !ok {
		panic(fmt.Sprintf("fakegithub: %s(%q): no such scaleset configured", caller, name))
	}
	return e
}

func (s *Server) routes() http.Handler {
	r := chi.NewRouter()

	// List runners — both org and repo scopes return the same shape.
	listHandler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Injectable rate-limit / transient failure: exercise the
		// reconciler's 429/5xx retry+backoff path.
		if f, ok := s.takeListFaultLocked(); ok {
			if f.retryAfter > 0 {
				w.Header().Set("Retry-After", strconv.Itoa(f.retryAfter))
			}
			writeRateLimitHeaders(w, 5000, 0, 0)
			writeGitHubError(w, f.status, "API rate limit exceeded")
			return
		}

		// Always advertise production-shaped rate-limit headers on the
		// success path so the reconciler sees realistic responses.
		writeRateLimitHeaders(w, 5000, 4999, 0)

		// Collect a sorted snapshot so pagination slices deterministically.
		all := make([]Runner, 0, len(s.runners))
		for _, r := range s.runners {
			all = append(all, r)
		}
		sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

		// Pagination: default (pageSize == 0) returns everything in one
		// page. Otherwise slice by the ?page= query param (1-based) and
		// set a Link rel="next" header when more pages remain.
		perPage := s.pageSize
		pageItems := all
		if perPage > 0 {
			page := 1
			if p, err := strconv.Atoi(req.URL.Query().Get("page")); err == nil && p > 0 {
				page = p
			}
			start := (page - 1) * perPage
			if start > len(all) {
				start = len(all)
			}
			end := start + perPage
			if end > len(all) {
				end = len(all)
			}
			pageItems = all[start:end]
			if end < len(all) {
				w.Header().Set("Link", linkNextHeader(s.URL, req.URL.Path, page+1, perPage))
			}
		}

		body := struct {
			TotalCount int              `json:"total_count"`
			Runners    []*github.Runner `json:"runners"`
		}{TotalCount: len(s.runners)}
		for _, r := range pageItems {
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
			writeGitHubError(w, http.StatusBadRequest, "invalid runner id")
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		// Injectable deregister failure (#327): drive the destroy
		// path's OnRunnerOrphaned failure branch.
		if f, ok := s.takeDeleteFaultLocked(); ok {
			writeGitHubError(w, f.status, "runner deregistration failed")
			return
		}
		if _, ok := s.runners[id]; !ok {
			writeGitHubError(w, http.StatusNotFound, fmt.Sprintf("runner %d not found", id))
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
	// create. The session ID disambiguates which scaleset's message
	// stream the long-poll should pull from. Format: /_messages/{ssID}/{sid}.
	r.Get("/_messages/{ssID}/{sid}", s.handleGetMessage)
	r.Delete("/_messages/{ssID}/{sid}/{mid}", s.handleDeleteMessage)

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
