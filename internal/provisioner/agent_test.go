package provisioner

import (
	"context"
	encjson "encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestInjectJITConfig_TwoPhaseRenameSequence pins the exact
// sequence of guest-agent API calls InjectJITConfig issues. The
// two-phase write (.tmp → mv → final) is the load-bearing
// invariant: an in-VM systemd path-unit watches the final path,
// so a single-step write to the final path would let the unit
// fire mid-write and the runner would parse a truncated JIT
// config and exit.
//
// A regression that drops the .tmp + mv pattern in favour of a
// direct write would silently re-introduce that race; this test
// asserts the strict call order so a refactor can't sneak it
// past review.
func TestInjectJITConfig_TwoPhaseRenameSequence(t *testing.T) {
	t.Parallel()

	type call struct {
		method string
		path   string
		body   map[string]any
	}
	var (
		mu    sync.Mutex
		calls []call
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		var body map[string]any
		raw, _ := io.ReadAll(r.Body)
		if len(raw) > 0 {
			_ = encjson.Unmarshal(raw, &body)
		}
		calls = append(calls, call{method: r.Method, path: r.URL.Path, body: body})
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/agent/file-write"):
			_, _ = io.WriteString(w, `{"data": null}`)
		case strings.HasSuffix(r.URL.Path, "/agent/exec"):
			_, _ = io.WriteString(w, `{"data": {"pid": 7}}`)
		case strings.HasSuffix(r.URL.Path, "/agent/exec-status"):
			_, _ = io.WriteString(w, `{"data": {"exited": true, "exitcode": 0}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")
	// base64-encoded {"runner_id":42}; matches the JSON-object shape
	// validateDecodedJITConfig requires after the regex fast-path.
	require.NoError(t, p.InjectJITConfig(t.Context(), &VM{VMID: 7, Node: "pve1"}, "eyJydW5uZXJfaWQiOjQyfQ=="))

	// Expected sequence:
	//   1. POST file-write (writes .tmp)
	//   2. POST exec       (mv .tmp final)
	//   3. GET exec-status (poll until exited)
	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(calls), 3, "expected at least 3 calls; got %d", len(calls))
	require.True(t, strings.HasSuffix(calls[0].path, "/agent/file-write"),
		"first call must be file-write to the .tmp path; got %s", calls[0].path)
	require.True(t, strings.HasSuffix(calls[1].path, "/agent/exec"),
		"second call must be exec (the rename); got %s", calls[1].path)
	require.True(t, strings.HasSuffix(calls[2].path, "/agent/exec-status"),
		"third call must be exec-status (poll for exit); got %s", calls[2].path)

	// And the file-write must target the .tmp path, not the final.
	tmpFile, _ := calls[0].body["file"].(string)
	require.Equal(t, "/opt/actions-runner/jitconfig.env.tmp", tmpFile,
		"first write must target .tmp — direct write to the final path would race the in-VM path-unit")

	// And the exec must be the move command from .tmp to final.
	rawCmd, _ := calls[1].body["command"].([]any)
	require.Equal(t, []any{"mv", "/opt/actions-runner/jitconfig.env.tmp", "/opt/actions-runner/jitconfig.env"}, rawCmd,
		"second call must be the atomic rename so the final path appears in one syscall")
}

// TestAgentExecWait_NullExitedKeepsPolling pins the variant-typed
// `exited` JSON field behaviour for the `null` case: the type
// switch matches neither bool nor float64, so `exited` stays
// false and the loop keeps polling. Returning early on null
// would race a not-actually-exited command and miss its real
// exitcode.
func TestAgentExecWait_NullExitedKeepsPolling(t *testing.T) {
	t.Parallel()
	pollCount := atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/agent/exec"):
			_, _ = io.WriteString(w, `{"data": {"pid": 7}}`)
		case strings.HasSuffix(r.URL.Path, "/agent/exec-status"):
			n := pollCount.Add(1)
			if n < 3 {
				_, _ = io.WriteString(w, `{"data": {"exited": null, "exitcode": 0}}`)
				return
			}
			_, _ = io.WriteString(w, `{"data": {"exited": true, "exitcode": 0}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")
	require.NoError(t, p.agentExecWait(t.Context(), "pve1", 1, []string{"ls"}))
	require.GreaterOrEqual(t, int(pollCount.Load()), 3,
		"null exited must NOT terminate the poll loop; expected ≥3 poll iterations, got %d", pollCount.Load())
}

// TestAgentExecWait_StringExitedSilentlyIgnored pins the current
// behaviour for a string-typed `exited` field (e.g. some future
// Proxmox release that emits `"exited": "1"`): the type switch
// silently ignores it, so the poll loop continues until either
// the next status report comes with a recognised type or the
// 30s deadline fires.
//
// This is documented foot-gun territory — if Proxmox ever ships
// a release that emits a string, the orchestrator would silently
// wait the full 30s on every InjectJITConfig. The test ensures a
// future change here is deliberate.
func TestAgentExecWait_StringExitedSilentlyIgnored(t *testing.T) {
	t.Parallel()
	pollCount := atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/agent/exec"):
			_, _ = io.WriteString(w, `{"data": {"pid": 7}}`)
		case strings.HasSuffix(r.URL.Path, "/agent/exec-status"):
			n := pollCount.Add(1)
			if n < 3 {
				// String "true" must NOT be treated as exited.
				_, _ = io.WriteString(w, `{"data": {"exited": "true", "exitcode": 0}}`)
				return
			}
			// After enough polls, flip to a recognised bool so
			// the test terminates.
			_, _ = io.WriteString(w, `{"data": {"exited": true, "exitcode": 0}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")
	require.NoError(t, p.agentExecWait(t.Context(), "pve1", 1, []string{"ls"}))
	require.GreaterOrEqual(t, int(pollCount.Load()), 3,
		"string `exited` must be silently ignored so the loop polls past it; if this assertion ever fails, a future Proxmox release likely changed the field type — pick a deliberate handling")
}

// TestAgentExecWait_NonZeroExitcode pins that a command that
// exits with a non-zero code surfaces as an error containing
// both the exit code AND the stderr blob — the operator needs
// both to diagnose what the in-VM command failed on.
func TestAgentExecWait_NonZeroExitcode(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/agent/exec"):
			_, _ = io.WriteString(w, `{"data": {"pid": 7}}`)
		case strings.HasSuffix(r.URL.Path, "/agent/exec-status"):
			_, _ = io.WriteString(w, `{"data": {"exited": true, "exitcode": 42, "err-data": "permission denied"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")
	err := p.agentExecWait(t.Context(), "pve1", 1, []string{"mv", "a", "b"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "exit=42",
		"error must surface the exit code; got %v", err)
	require.Contains(t, err.Error(), "permission denied",
		"error must surface the stderr blob; got %v", err)
}

// TestAgentExecWait_ConcurrentCallsDoNotInterfere drives N
// agentExecWait calls in parallel against the same VM. The
// server assigns each exec a unique PID; each goroutine must
// poll only its own PID's status and complete independently. A
// regression that accidentally shared PID state between calls
// would either deadlock or cross-wire responses.
func TestAgentExecWait_ConcurrentCallsDoNotInterfere(t *testing.T) {
	t.Parallel()
	var pidCounter atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/agent/exec"):
			pid := pidCounter.Add(1)
			_, _ = fmt.Fprintf(w, `{"data": {"pid": %d}}`, pid)
		case strings.HasSuffix(r.URL.Path, "/agent/exec-status"):
			_, _ = io.WriteString(w, `{"data": {"exited": true, "exitcode": 0}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")
	var wg sync.WaitGroup
	const n = 8
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = p.agentExecWait(t.Context(), "pve1", 1, []string{"ls"})
		}(i)
	}
	wg.Wait()
	for i, e := range errs {
		require.NoError(t, e, "goroutine %d failed", i)
	}
	require.Equal(t, int32(n), pidCounter.Load(),
		"expected %d unique PIDs allocated (one per concurrent exec); got %d", n, pidCounter.Load())
}

// Silence the unused-import warning when refactors prune call
// sites; required because t.Context() doesn't reference
// context.Context directly in this file.
var _ = context.Background
