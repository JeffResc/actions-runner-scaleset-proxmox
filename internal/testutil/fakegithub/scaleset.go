package fakegithub

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// Scaleset-library wire types (subset)
//
// These mirror github.com/actions/scaleset's exported types just enough
// to satisfy the orchestrator's listener handshake. We re-declare them
// here instead of importing scaleset directly so the fake stays free of
// circular-import worry — only the JSON shape matters.
// ---------------------------------------------------------------------------

type registrationTokenResp struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

type adminConnectionResp struct {
	URL   string `json:"url"`
	Token string `json:"token"`
}

type runnerScaleSetLabel struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
}

type fakeRunnerScaleSet struct {
	ID            int                   `json:"id"`
	Name          string                `json:"name"`
	RunnerGroupID int                   `json:"runnerGroupId"`
	Labels        []runnerScaleSetLabel `json:"labels,omitempty"`
}

type runnerScaleSetList struct {
	Count int                  `json:"count"`
	Value []fakeRunnerScaleSet `json:"value"`
}

type runnerGroupResp struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type fakeRunnerScaleSetStatistic struct {
	TotalAvailableJobs     int `json:"totalAvailableJobs"`
	TotalAcquiredJobs      int `json:"totalAcquiredJobs"`
	TotalAssignedJobs      int `json:"totalAssignedJobs"`
	TotalRunningJobs       int `json:"totalRunningJobs"`
	TotalRegisteredRunners int `json:"totalRegisteredRunners"`
	TotalBusyRunners       int `json:"totalBusyRunners"`
	TotalIdleRunners       int `json:"totalIdleRunners"`
}

type fakeSession struct {
	SessionID               uuid.UUID                    `json:"sessionId"`
	OwnerName               string                       `json:"ownerName"`
	RunnerScaleSet          fakeRunnerScaleSet           `json:"runnerScaleSet"`
	MessageQueueURL         string                       `json:"messageQueueUrl"`
	MessageQueueAccessToken string                       `json:"messageQueueAccessToken"`
	Statistics              *fakeRunnerScaleSetStatistic `json:"statistics"`
}

type fakeJITRunnerConfig struct {
	Runner           fakeRunnerReference `json:"runner"`
	EncodedJITConfig string              `json:"encodedJITConfig"`
}

type fakeRunnerReference struct {
	ID               int    `json:"id"`
	Name             string `json:"name"`
	RunnerScaleSetID int    `json:"runnerScaleSetId"`
}

// ---------------------------------------------------------------------------
// Configuration knobs
// ---------------------------------------------------------------------------

// ScaleSetOptions configures the fake's scale-set behavior. All fields
// have sensible zero-value defaults; tests usually only set ID to a
// stable value if they need to assert on it.
type ScaleSetOptions struct {
	// ID is the runner-scale-set ID the fake claims when GetRunnerScaleSet
	// is hit. Zero defaults to 42 (or auto-assigned in multi-scaleset
	// configs to avoid collisions).
	ID int

	// Name must match cfg.scaleset.name in the orchestrator's config —
	// otherwise the lookup returns "not found" and the orchestrator
	// attempts to create it (which the fake also supports). Empty
	// defaults to "test-scaleset" (or "test-scaleset-N" in multi-
	// scaleset configs).
	Name string

	// RunnerGroupID defaults to 1 (the "Default" group).
	RunnerGroupID int
}

// ---------------------------------------------------------------------------
// Session machinery (long-poll capable)
// ---------------------------------------------------------------------------

// sessionState holds one open message session. Each per-scaleset
// entry has its own *sessionState; the orchestrator's per-scaleset
// listener opens one session against one scale set's ID, so the
// 1:1 mapping holds across multi-scaleset configs.
type sessionState struct {
	id     uuid.UUID
	owner  string
	closed atomic.Bool

	// pending feeds the GetMessage handler. Each send unblocks one
	// long-poll. Tests push synthesized messages via Server.PostJob*
	// helpers; the long-poll idle path returns 202 when the channel
	// stays empty.
	pending chan json.RawMessage
}

// ---------------------------------------------------------------------------
// JWT helpers
// ---------------------------------------------------------------------------

// mintAdminJWT returns an unverified-parseable JWT with a 24h exp
// claim. The scaleset library calls jwt.ParseUnverified to extract the
// expiry; signing method and key don't matter, only structure.
func mintAdminJWT() string {
	claims := jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		IssuedAt:  jwt.NewNumericDate(time.Now()),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	// Static key — the fake's "admin" credential is for test routing,
	// not real auth.
	s, err := tok.SignedString([]byte("fakegithub-admin-secret"))
	if err != nil {
		// SignedString with HS256 + non-empty key cannot fail in
		// practice; the panic is a defensive guard against future
		// jwt library churn.
		panic(fmt.Sprintf("fakegithub: mintAdminJWT: %v", err))
	}
	return s
}

// ---------------------------------------------------------------------------
// Per-request entry lookup
// ---------------------------------------------------------------------------

// entryByURLID resolves the {id} URL param to a scaleset entry and
// writes a 404 if no entry matches. Returns nil when the lookup
// fails so callers should `if entry == nil { return }`.
func (s *Server) entryByURLID(w http.ResponseWriter, r *http.Request) *scalesetEntry {
	raw := chi.URLParam(r, "id")
	id, err := strconv.Atoi(raw)
	if err != nil {
		http.Error(w, "bad scaleset id "+raw, http.StatusBadRequest)
		return nil
	}
	entry, ok := s.scalesetsByID[id]
	if !ok {
		http.Error(w, fmt.Sprintf("scaleset %d not found", id), http.StatusNotFound)
		return nil
	}
	return entry
}

// entryByURLSSID resolves the {ssID} URL param to a scaleset entry.
// Used by the message-queue routes which include the scaleset ID
// alongside the session ID to disambiguate which entry's session
// stream a long-poll targets.
func (s *Server) entryByURLSSID(w http.ResponseWriter, r *http.Request) *scalesetEntry {
	raw := chi.URLParam(r, "ssID")
	id, err := strconv.Atoi(raw)
	if err != nil {
		http.Error(w, "bad scaleset id "+raw, http.StatusBadRequest)
		return nil
	}
	entry, ok := s.scalesetsByID[id]
	if !ok {
		http.Error(w, fmt.Sprintf("scaleset %d not found", id), http.StatusNotFound)
		return nil
	}
	return entry
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

func (s *Server) handleRegistrationToken(w http.ResponseWriter, _ *http.Request) {
	// #nosec G101 -- this is a fake test server: the literal is the
	// fake's response payload, not a real credential. gosec flags
	// any "Token: <string>" field assignment as a possible hardcoded
	// credential; in test infrastructure that's the whole point.
	const fakeToken = "fakegithub-registration-token"
	writeJSON(w, http.StatusCreated, registrationTokenResp{
		Token:     fakeToken,
		ExpiresAt: time.Now().Add(1 * time.Hour),
	})
}

func (s *Server) handleRunnerRegistration(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, adminConnectionResp{
		// The orchestrator's subsequent _apis/runtime/... calls will
		// be url.JoinPath'd against this URL — point it back at us.
		URL:   s.URL,
		Token: s.adminToken,
	})
}

func (s *Server) handleRunnerGroupLookup(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("groupName")
	if name == "" {
		http.Error(w, "groupName required", http.StatusBadRequest)
		return
	}
	// Pick any entry's RunnerGroupID — operators that name distinct
	// runner groups per scaleset would also need per-group routing;
	// none of our e2e scenarios do, so returning the first match is
	// fine. Real GitHub returns the group by name from a flat per-org
	// list, which is what we model here.
	groupID := 1
	for _, entry := range s.scalesets {
		groupID = entry.spec.RunnerGroupID
		break
	}
	writeJSON(w, http.StatusOK, struct {
		Count int               `json:"count"`
		Value []runnerGroupResp `json:"value"`
	}{
		Count: 1,
		Value: []runnerGroupResp{{ID: groupID, Name: name}},
	})
}

func (s *Server) handleScaleSetLookup(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	entry, ok := s.scalesets[name]
	if !ok {
		// Even when no name matches, the scaleset library treats
		// `count:0` as "not found" (returns nil rather than error)
		// and the orchestrator follows up with a CreateRunnerScaleSet.
		writeJSON(w, http.StatusOK, runnerScaleSetList{Count: 0})
		return
	}
	writeJSON(w, http.StatusOK, runnerScaleSetList{
		Count: 1,
		Value: []fakeRunnerScaleSet{entry.spec},
	})
}

func (s *Server) handleScaleSetCreate(w http.ResponseWriter, r *http.Request) {
	// Echo back the entry whose Name matches the create request.
	// Tests that haven't pre-declared the scaleset receive the
	// canonical default (the single configured entry).
	var body fakeRunnerScaleSet
	_ = json.NewDecoder(r.Body).Decode(&body)
	if entry, ok := s.scalesets[body.Name]; ok {
		writeJSON(w, http.StatusCreated, entry.spec)
		return
	}
	// Unknown name and we don't synthesise new scalesets at runtime
	// — return 422 so the orchestrator surfaces the misconfiguration
	// cleanly.
	http.Error(w, fmt.Sprintf("scaleset %q not configured on fake", body.Name), http.StatusUnprocessableEntity)
}

func (s *Server) handleSessionCreate(w http.ResponseWriter, r *http.Request) {
	entry := s.entryByURLID(w, r)
	if entry == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry.session != nil && !entry.session.closed.Load() {
		// Cluster-mode tests can leave a stale session behind when a
		// leader exits without explicitly closing — real GitHub
		// expires the old session after a few minutes and the new
		// leader simply opens a fresh one. Mirror that by retiring
		// the prior session here instead of returning 409, which
		// would otherwise wedge the new leader's handshake.
		entry.session.closed.Store(true)
		close(entry.session.pending)
		entry.session = nil
	}
	sid := uuid.New()
	entry.session = &sessionState{
		id:      sid,
		owner:   "fakegithub",
		pending: make(chan json.RawMessage, 16),
	}
	stats := entry.statistics
	writeJSON(w, http.StatusOK, fakeSession{
		SessionID:               sid,
		OwnerName:               "fakegithub",
		RunnerScaleSet:          entry.spec,
		MessageQueueURL:         fmt.Sprintf("%s/_messages/%d/%s", s.URL, entry.spec.ID, sid.String()),
		MessageQueueAccessToken: s.adminToken,
		Statistics:              &stats,
	})
}

func (s *Server) handleSessionDelete(w http.ResponseWriter, r *http.Request) {
	entry := s.entryByURLID(w, r)
	if entry == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry.session != nil {
		entry.session.closed.Store(true)
		// Drain to unblock any GetMessage goroutine waiting on the
		// channel; the orchestrator hits this on graceful shutdown.
		close(entry.session.pending)
		entry.session = nil
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleSessionRefresh(w http.ResponseWriter, r *http.Request) {
	entry := s.entryByURLID(w, r)
	if entry == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry.session == nil {
		http.Error(w, "no open session", http.StatusGone)
		return
	}
	stats := entry.statistics
	writeJSON(w, http.StatusOK, fakeSession{
		SessionID:               entry.session.id,
		OwnerName:               entry.session.owner,
		RunnerScaleSet:          entry.spec,
		MessageQueueURL:         fmt.Sprintf("%s/_messages/%d/%s", s.URL, entry.spec.ID, entry.session.id.String()),
		MessageQueueAccessToken: s.adminToken,
		Statistics:              &stats,
	})
}

func (s *Server) handleGetMessage(w http.ResponseWriter, r *http.Request) {
	entry := s.entryByURLSSID(w, r)
	if entry == nil {
		return
	}
	s.mu.Lock()
	sess := entry.session
	s.mu.Unlock()
	if sess == nil || sess.closed.Load() {
		http.Error(w, "no session", http.StatusGone)
		return
	}
	// Long-poll up to 5s. When no message arrives, return 202 — the
	// listener treats that as "no work, loop and try again" and calls
	// scaler.HandleDesiredRunnerCount(0). Real GitHub long-polls for
	// 50s; 5s keeps tests snappy.
	select {
	case msg, ok := <-sess.pending:
		if !ok {
			// Session closed concurrently with our read.
			http.Error(w, "session closed", http.StatusGone)
			return
		}
		writeRaw(w, http.StatusOK, msg)
	case <-time.After(5 * time.Second):
		w.WriteHeader(http.StatusAccepted)
	case <-r.Context().Done():
		// Client cancelled (graceful shutdown). The library treats
		// the context error as a normal exit path.
		w.WriteHeader(http.StatusAccepted)
	}
}

func (s *Server) handleDeleteMessage(w http.ResponseWriter, _ *http.Request) {
	// No-op: tests don't rely on message-acknowledgement state today.
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleAcquireJobs(w http.ResponseWriter, r *http.Request) {
	if entry := s.entryByURLID(w, r); entry == nil {
		return
	}
	var ids []int64
	_ = json.NewDecoder(r.Body).Decode(&ids)
	// Pretend we acquired every requested job.
	writeJSON(w, http.StatusOK, struct {
		Count int     `json:"count"`
		Value []int64 `json:"value"`
	}{Count: len(ids), Value: ids})
}

func (s *Server) handleGenerateJIT(w http.ResponseWriter, r *http.Request) {
	entry := s.entryByURLID(w, r)
	if entry == nil {
		return
	}
	var body struct {
		Name       string `json:"name"`
		WorkFolder string `json:"workFolder"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	s.mu.Lock()
	entry.jitMintCount++
	// Synthesize a runner ID. Per-scaleset counters keep
	// existing single-scaleset tests stable (first mint = 100001);
	// multi-scaleset tests that need to distinguish IDs across
	// scalesets should assert on JITMintCountFor(name) rather
	// than hardcoding 100000+N.
	runnerID := 100000 + entry.jitMintCount
	if body.Name != "" {
		entry.jitMintsByName[body.Name] = runnerID
	}
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, fakeJITRunnerConfig{
		Runner: fakeRunnerReference{
			ID:               runnerID,
			Name:             body.Name,
			RunnerScaleSetID: entry.spec.ID,
		},
		// Base64-encoded test config (literal bytes, no actual runner config).
		EncodedJITConfig: "ZmFrZWdpdGh1Yi1qaXQtY29uZmln",
	})
}

func (s *Server) handleRunnerDelete(w http.ResponseWriter, r *http.Request) {
	// Mirrors the REST path's RunnerDeletions accounting so e2e tests
	// can assert "scaler removed runner N" without distinguishing
	// which API surface drove it.
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "bad runner id "+idStr, http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.deletions = append(s.deletions, id)
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeRaw(w http.ResponseWriter, status int, raw json.RawMessage) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

// ---------------------------------------------------------------------------
// Public helpers for tests
// ---------------------------------------------------------------------------

// JITMintCount returns the JIT mint counter for the single registered
// scaleset. Panics in multi-scaleset configs — use JITMintCountFor.
func (s *Server) JITMintCount() int {
	entry := s.onlyEntry("JITMintCount")
	s.mu.Lock()
	defer s.mu.Unlock()
	return entry.jitMintCount
}

// JITMintCountFor returns the per-scaleset JIT mint counter (multi-
// scaleset tests). Panics on unknown names.
func (s *Server) JITMintCountFor(name string) int {
	entry := s.entryFor(name, "JITMintCountFor")
	s.mu.Lock()
	defer s.mu.Unlock()
	return entry.jitMintCount
}

// JITMintIDForRunner returns the synthesised runner ID that
// handleGenerateJIT handed back for the given runner name on the
// single registered scaleset, and ok=false if no JIT mint has
// happened yet for that name. Concurrent-job e2e tests call this
// after awaitNAssignedVMs to learn the ID the orchestrator already
// stamped on the VM row via scaler.SetRunnerID — so JobStarted /
// JobCompleted can reference the same ID and OnRunnerOrphaned
// deregisters the expected runner on destroy.
//
// Panics in multi-scaleset configs — use JITMintIDForRunnerOn.
func (s *Server) JITMintIDForRunner(runnerName string) (int, bool) {
	entry := s.onlyEntry("JITMintIDForRunner")
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := entry.jitMintsByName[runnerName]
	return id, ok
}

// JITMintIDForRunnerOn is the multi-scaleset variant of
// JITMintIDForRunner. Panics on unknown scaleset names.
func (s *Server) JITMintIDForRunnerOn(scaleset, runnerName string) (int, bool) {
	entry := s.entryFor(scaleset, "JITMintIDForRunnerOn")
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := entry.jitMintsByName[runnerName]
	return id, ok
}

// SetStatistics overrides the per-session statistics returned for the
// single registered scaleset. Tests set TotalAssignedJobs > 0 before
// starting the harness so the orchestrator's listener fires
// HandleDesiredRunnerCount on its initial handshake — the only path
// that drives a clone+JIT-injection pass.
//
// Panics in multi-scaleset configs; use SetStatisticsFor.
func (s *Server) SetStatistics(stats Statistics) {
	entry := s.onlyEntry("SetStatistics")
	s.mu.Lock()
	defer s.mu.Unlock()
	entry.statistics = fakeRunnerScaleSetStatistic(stats)
}

// SetStatisticsFor overrides statistics for the named scaleset.
// Panics on unknown names.
func (s *Server) SetStatisticsFor(name string, stats Statistics) {
	entry := s.entryFor(name, "SetStatisticsFor")
	s.mu.Lock()
	defer s.mu.Unlock()
	entry.statistics = fakeRunnerScaleSetStatistic(stats)
}

// Statistics is the test-facing shape SetStatistics accepts. Mirrors
// scaleset.RunnerScaleSetStatistic field-for-field, kept separate so
// the fake doesn't need to import the scaleset library just for the
// type alias.
type Statistics struct {
	TotalAvailableJobs     int
	TotalAcquiredJobs      int
	TotalAssignedJobs      int
	TotalRunningJobs       int
	TotalRegisteredRunners int
	TotalBusyRunners       int
	TotalIdleRunners       int
}

// PostJobStarted enqueues a JobStarted message on the single
// configured scaleset's session. Panics in multi-scaleset configs;
// use PostJobStartedFor.
func (s *Server) PostJobStarted(runnerName string, runnerID int) error {
	entry := s.onlyEntry("PostJobStarted")
	return s.postMessage(entry, "JobStarted", map[string]any{
		"messageType":     "JobStarted",
		"runnerName":      runnerName,
		"runnerId":        runnerID,
		"runnerRequestId": int64(runnerID),
	})
}

// PostJobStartedFor enqueues a JobStarted message on the named
// scaleset's session. Panics on unknown names; returns an error when
// no session is open for that scaleset.
func (s *Server) PostJobStartedFor(scaleset, runnerName string, runnerID int) error {
	entry := s.entryFor(scaleset, "PostJobStartedFor")
	return s.postMessage(entry, "JobStarted", map[string]any{
		"messageType":     "JobStarted",
		"runnerName":      runnerName,
		"runnerId":        runnerID,
		"runnerRequestId": int64(runnerID),
	})
}

// PostJobCompleted enqueues a JobCompleted message. See
// PostJobStarted for semantics.
func (s *Server) PostJobCompleted(runnerName string, runnerID int) error {
	entry := s.onlyEntry("PostJobCompleted")
	return s.postMessage(entry, "JobCompleted", map[string]any{
		"messageType":     "JobCompleted",
		"runnerName":      runnerName,
		"runnerId":        runnerID,
		"runnerRequestId": int64(runnerID),
		"result":          "succeeded",
	})
}

// PostJobCompletedFor enqueues a JobCompleted message on the named
// scaleset's session.
func (s *Server) PostJobCompletedFor(scaleset, runnerName string, runnerID int) error {
	entry := s.entryFor(scaleset, "PostJobCompletedFor")
	return s.postMessage(entry, "JobCompleted", map[string]any{
		"messageType":     "JobCompleted",
		"runnerName":      runnerName,
		"runnerId":        runnerID,
		"runnerRequestId": int64(runnerID),
		"result":          "succeeded",
	})
}

// postMessage marshals one job message into the outer
// runnerScaleSetMessageResponse envelope the listener expects and
// pushes it onto the named scaleset entry's pending channel.
func (s *Server) postMessage(entry *scalesetEntry, label string, inner map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry.session == nil || entry.session.closed.Load() {
		return fmt.Errorf("fakegithub: post %s on %q: no session open", label, entry.spec.Name)
	}
	bodyJSON, err := json.Marshal([]map[string]any{inner})
	if err != nil {
		return fmt.Errorf("fakegithub: marshal %s body: %w", label, err)
	}
	s.nextMessageID++
	stats := entry.statistics
	envelope, err := json.Marshal(struct {
		MessageID   int                          `json:"messageId"`
		MessageType string                       `json:"messageType"`
		Body        string                       `json:"body"`
		Statistics  *fakeRunnerScaleSetStatistic `json:"statistics"`
	}{
		MessageID:   s.nextMessageID,
		MessageType: "RunnerScaleSetJobMessages",
		Body:        string(bodyJSON),
		Statistics:  &stats,
	})
	if err != nil {
		return fmt.Errorf("fakegithub: marshal %s envelope: %w", label, err)
	}
	select {
	case entry.session.pending <- envelope:
		return nil
	default:
		return fmt.Errorf("fakegithub: session pending channel full (cap=16) — drop %s", label)
	}
}
