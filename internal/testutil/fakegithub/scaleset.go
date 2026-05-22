package fakegithub

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/golang-jwt/jwt/v4"
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
	// is hit. Zero defaults to 42.
	ID int

	// Name must match cfg.scaleset.name in the orchestrator's config —
	// otherwise the lookup returns "not found" and the orchestrator
	// attempts to create it (which the fake also supports). Empty
	// defaults to "test-scaleset".
	Name string

	// RunnerGroupID defaults to 1 (the "Default" group).
	RunnerGroupID int
}

// ---------------------------------------------------------------------------
// Session machinery (long-poll capable)
// ---------------------------------------------------------------------------

// sessionState holds one open message session. The orchestrator opens
// exactly one session at startup, so we don't need fancy multi-session
// indexing — a single atomic pointer is enough.
type sessionState struct {
	id     uuid.UUID
	owner  string
	closed atomic.Bool

	// pending feeds the GetMessage handler. Each send unblocks one
	// long-poll. Tests push synthesized messages via the public
	// PostJob* helpers (added in a follow-up); P3 only needs the
	// idle path.
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
	writeJSON(w, http.StatusOK, struct {
		Count int               `json:"count"`
		Value []runnerGroupResp `json:"value"`
	}{
		Count: 1,
		Value: []runnerGroupResp{{ID: s.scaleSet.RunnerGroupID, Name: name}},
	})
}

func (s *Server) handleScaleSetLookup(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	// Even when no name matches, the scaleset library treats `count:0`
	// as "not found" (returns nil rather than error) and the
	// orchestrator follows up with a CreateRunnerScaleSet. Match the
	// name and return one row.
	if name != s.scaleSet.Name {
		writeJSON(w, http.StatusOK, runnerScaleSetList{Count: 0})
		return
	}
	writeJSON(w, http.StatusOK, runnerScaleSetList{
		Count: 1,
		Value: []fakeRunnerScaleSet{s.scaleSet},
	})
}

func (s *Server) handleScaleSetCreate(w http.ResponseWriter, r *http.Request) {
	// Accept anything the orchestrator sends; just echo back our
	// canonical scale set. The orchestrator only looks at the
	// returned ID + Name.
	var body fakeRunnerScaleSet
	_ = json.NewDecoder(r.Body).Decode(&body)
	writeJSON(w, http.StatusCreated, s.scaleSet)
}

func (s *Server) handleSessionCreate(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.session != nil && !s.session.closed.Load() {
		// Tests should not normally hit this; surface clearly so the
		// failure mode is obvious rather than silently swapping
		// sessions out from under the listener.
		http.Error(w, "session already open; close it first", http.StatusConflict)
		return
	}
	sid := uuid.New()
	s.session = &sessionState{
		id:      sid,
		owner:   "fakegithub",
		pending: make(chan json.RawMessage, 16),
	}
	writeJSON(w, http.StatusOK, fakeSession{
		SessionID:               sid,
		OwnerName:               "fakegithub",
		RunnerScaleSet:          s.scaleSet,
		MessageQueueURL:         fmt.Sprintf("%s/_messages/%s", s.URL, sid.String()),
		MessageQueueAccessToken: s.adminToken,
		Statistics:              &fakeRunnerScaleSetStatistic{},
	})
}

func (s *Server) handleSessionDelete(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.session != nil {
		s.session.closed.Store(true)
		// Drain to unblock any GetMessage goroutine waiting on the
		// channel; the orchestrator hits this on graceful shutdown.
		close(s.session.pending)
		s.session = nil
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleSessionRefresh(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.session == nil {
		http.Error(w, "no open session", http.StatusGone)
		return
	}
	writeJSON(w, http.StatusOK, fakeSession{
		SessionID:               s.session.id,
		OwnerName:               s.session.owner,
		RunnerScaleSet:          s.scaleSet,
		MessageQueueURL:         fmt.Sprintf("%s/_messages/%s", s.URL, s.session.id.String()),
		MessageQueueAccessToken: s.adminToken,
		Statistics:              &fakeRunnerScaleSetStatistic{},
	})
}

func (s *Server) handleGetMessage(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	sess := s.session
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
	var ids []int64
	_ = json.NewDecoder(r.Body).Decode(&ids)
	// Pretend we acquired every requested job.
	writeJSON(w, http.StatusOK, struct {
		Count int     `json:"count"`
		Value []int64 `json:"value"`
	}{Count: len(ids), Value: ids})
}

func (s *Server) handleGenerateJIT(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name       string `json:"name"`
		WorkFolder string `json:"workFolder"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	s.mu.Lock()
	s.jitMintCount++
	runnerID := 100000 + s.jitMintCount
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, fakeJITRunnerConfig{
		Runner: fakeRunnerReference{
			ID:               runnerID,
			Name:             body.Name,
			RunnerScaleSetID: s.scaleSet.ID,
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

// JITMintCount returns how many JIT runner configs the fake has minted
// since startup. E2e tests assert on this to confirm the scaler called
// out to GitHub the expected number of times.
func (s *Server) JITMintCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.jitMintCount
}
