package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"

	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/config"
)

// quietLogger discards all log output.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// newTestProvisioner returns a *pmox that talks to the given httptest server.
// It skips template discovery (which requires a richer mock) by setting the
// template node directly.
func newTestProvisioner(t *testing.T, srv *httptest.Server, templateNode string) *pmox {
	t.Helper()
	cfg := config.ProxmoxConfig{
		Endpoint:           srv.URL,
		InsecureSkipVerify: true,
		Auth: config.ProxmoxAuth{
			TokenID:     "scaleset@pve!automation",
			TokenSecret: "fake-secret",
		},
		TemplateVMID: 9000,
	}
	cli := newProxmoxClient(cfg)

	return &pmox{
		cfg:          cfg,
		cli:          cli,
		scaleSetName: "test-scaleset",
		templateNode: templateNode,
		log:          quietLogger(),
	}
}

// captured holds what the test server saw on a single request.
type captured struct {
	Method      string
	Path        string
	BodyDecoded map[string]any
	Query       map[string][]string
}

// mockServer returns an httptest server whose handler records the request
// in *got and replies with respBody (which must already be JSON).
func mockServer(t *testing.T, got *captured, respStatus int, respBody string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Method = r.Method
		got.Path = r.URL.Path
		got.Query = r.URL.Query()
		if r.Body != nil {
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &got.BodyDecoded)
		}
		w.WriteHeader(respStatus)
		_, _ = io.WriteString(w, respBody)
	}))
}

func TestAgentFileWrite_RequestShape(t *testing.T) {
	t.Parallel()
	got := &captured{}
	srv := mockServer(t, got, http.StatusOK, `{"data": null}`)
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")
	err := p.agentFileWrite(context.Background(), "pve2", 10042, "/opt/actions-runner/jitconfig", []byte("hello world"))
	require.NoError(t, err)

	require.Equal(t, http.MethodPost, got.Method)
	require.Equal(t, "/nodes/pve2/qemu/10042/agent/file-write", got.Path)
	require.Equal(t, "/opt/actions-runner/jitconfig", got.BodyDecoded["file"])
	// Proxmox 9.x stores `content` verbatim regardless of `encode`, so
	// we send the raw bytes (the JIT config is itself ASCII base64).
	require.NotContains(t, got.BodyDecoded, "encode")
	require.Equal(t, "hello world", got.BodyDecoded["content"])
}

func TestAgentFileWrite_RejectsOversizedPayload(t *testing.T) {
	t.Parallel()
	srv := mockServer(t, &captured{}, http.StatusOK, `{"data": null}`)
	defer srv.Close()
	p := newTestProvisioner(t, srv, "pve1")

	huge := make([]byte, agentFileWriteMaxBytes+1)
	err := p.agentFileWrite(context.Background(), "pve1", 1, "/x", huge)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds")
}

func TestInjectJITConfig_RoutesToCorrectPath(t *testing.T) {
	t.Parallel()
	// InjectJITConfig does a 3-step dance: file-write to .tmp, then exec
	// `mv .tmp <final>`, then poll exec-status until exit. The mock has
	// to satisfy all three or we'll hit a 30s timeout.
	var captured struct {
		writeBody map[string]any
		writePath string
		execBody  map[string]any
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/agent/file-write"):
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &captured.writeBody)
			captured.writePath = r.URL.Path
			_, _ = io.WriteString(w, `{"data": null}`)
		case strings.HasSuffix(r.URL.Path, "/agent/exec"):
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &captured.execBody)
			_, _ = io.WriteString(w, `{"data": {"pid": 4242}}`)
		case strings.HasSuffix(r.URL.Path, "/agent/exec-status"):
			_, _ = io.WriteString(w, `{"data": {"exited": 1, "exitcode": 0}}`)
		default:
			http.Error(w, "unexpected path "+r.URL.Path, http.StatusNotFound)
		}
	}))
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")
	err := p.InjectJITConfig(context.Background(), &VM{VMID: 12345, Node: "pve3"}, "encoded-jit-config-blob")
	require.NoError(t, err)

	require.Equal(t, "/nodes/pve3/qemu/12345/agent/file-write", captured.writePath)
	require.Equal(t, "/opt/actions-runner/jitconfig.env.tmp", captured.writeBody["file"])
	require.Contains(t, captured.writeBody["content"], "JIT_CONFIG=")
	require.Contains(t, captured.writeBody["content"], "encoded-jit-config-blob")
	// And the exec call should be the atomic rename.
	cmd, _ := captured.execBody["command"].([]any)
	require.Equal(t, []any{"mv", "/opt/actions-runner/jitconfig.env.tmp", "/opt/actions-runner/jitconfig.env"}, cmd)
}

func TestInjectJITConfig_RejectsNilOrEmpty(t *testing.T) {
	t.Parallel()
	p := newTestProvisioner(t, mockServer(t, &captured{}, http.StatusOK, `{}`), "pve1")

	require.Error(t, p.InjectJITConfig(context.Background(), nil, "x"))
	require.Error(t, p.InjectJITConfig(context.Background(), &VM{VMID: 1, Node: "pve1"}, ""))
}

func TestReadAgentFile_DecodesPayload(t *testing.T) {
	t.Parallel()
	got := &captured{}
	srv := mockServer(t, got, http.StatusOK, `{"data": {"content": "some file contents"}}`)
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")
	out, err := p.ReadAgentFile(context.Background(), &VM{VMID: 555, Node: "pve9"}, "/opt/actions-runner/jitconfig")
	require.NoError(t, err)
	require.Equal(t, []byte("some file contents"), out)

	require.Equal(t, "/nodes/pve9/qemu/555/agent/file-read", got.Path)
	require.Equal(t, []string{"/opt/actions-runner/jitconfig"}, got.Query["file"])
}

func TestClone_LinkedRejectsCrossNode(t *testing.T) {
	t.Parallel()
	srv := mockServer(t, &captured{}, http.StatusOK, `{}`)
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")
	_, err := p.Clone(context.Background(), CloneOptions{
		NewVMID: 10042,
		Node:    "pve2", // different from templateNode=pve1
		Name:    "gh-runner-test-10042",
		Linked:  true,
	})
	require.ErrorIs(t, err, ErrLinkedCloneCrossNode)
}

func TestIsTemplate(t *testing.T) {
	t.Parallel()
	// Sanity check on the helper used by template discovery.
	vm := &proxmox.VirtualMachine{Template: proxmox.IsTemplate(true)}
	require.True(t, isTemplate(vm))
}

// TestClassifyProxmoxError covers the three detection layers:
// library-typed sentinels, HTTP-status prefix, and body-text fallback.
// Each detection case is verified via errors.Is against the typed
// sentinel callers actually use.
func TestClassifyProxmoxError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want error // sentinel to errors.Is against; nil = unchanged
	}{
		{"nil", nil, nil},
		{"library ErrNotFound", proxmox.ErrNotFound, ErrVMNotFound},
		{"404 status prefix", &stringError{"404 Not Found"}, ErrVMNotFound},
		{"body says does not exist", &stringError{"Configuration file 'nodes/pve1/qemu-server/10042.conf' does not exist"}, ErrVMNotFound},
		{"already running", &stringError{"VM 10042 already running"}, ErrVMAlreadyRunning},
		{"unrelated 500", &stringError{"500 Internal Server Error"}, nil},
		{"empty", &stringError{""}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got := classifyProxmoxError(c.err)
			if c.want == nil {
				if got != nil {
					// Unchanged or nil — both are fine; just check we
					// didn't accidentally tag with one of our sentinels.
					require.NotErrorIs(t, got, ErrVMNotFound)
					require.NotErrorIs(t, got, ErrVMAlreadyRunning)
				}
				return
			}
			require.ErrorIs(t, got, c.want)
			// And the original cause is still reachable via the chain.
			require.Contains(t, got.Error(), c.err.Error())
		})
	}
}

// TestHttpStatusFromError exercises the leading-NNN parser used as a
// fallback detection layer.
func TestHttpStatusFromError(t *testing.T) {
	t.Parallel()
	require.Equal(t, 404, httpStatusFromError(&stringError{"404 Not Found"}))
	require.Equal(t, 500, httpStatusFromError(&stringError{"500 Internal Server Error: details"}))
	require.Equal(t, 0, httpStatusFromError(&stringError{"4xx error"}))
	require.Equal(t, 0, httpStatusFromError(&stringError{"no status here"}))
	require.Equal(t, 0, httpStatusFromError(&stringError{"99 too small"}))
	require.Equal(t, 0, httpStatusFromError(&stringError{"700 out of range"}))
	require.Equal(t, 0, httpStatusFromError(nil))
}

// TestIsGuestAgentNotReady_TypedSentinel: callers use
// errors.Is(err, ErrGuestAgentNotReady) — the wrapper function in
// agent.go translates raw Proxmox response strings into the sentinel.
func TestIsGuestAgentNotReady_TypedSentinel(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"is not running", &stringError{"VM 10042 is not running"}, true},
		{"qemu agent not running", &stringError{"QEMU guest agent is not running"}, true},
		{"no agent configured", &stringError{"no QEMU guest agent configured"}, true},
		{"unrelated", &stringError{"some 500 error"}, false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			wrapped := wrapGuestAgent(c.err)
			require.Equal(t, c.want, errors.Is(wrapped, ErrGuestAgentNotReady))
			if c.want {
				require.Contains(t, wrapped.Error(), c.err.Error(),
					"wrapping must preserve the original error chain")
			}
		})
	}
}

// Kept for backwards-compat with internal callers (Stop/Destroy/getVM
// in provisioner.go); just sanity-checks they still detect the cases
// they always did.
func TestIsNotFound_InternalAdapter(t *testing.T) {
	t.Parallel()
	require.True(t, isNotFound(&stringError{"404 Not Found"}))
	require.True(t, isNotFound(&stringError{"vm 10042 does not exist"}))
	require.False(t, isNotFound(&stringError{"connection refused"}))
	require.False(t, isNotFound(nil))
}

func TestIsAlreadyRunning_InternalAdapter(t *testing.T) {
	t.Parallel()
	require.True(t, isAlreadyRunning(&stringError{"VM is already running"}))
	require.False(t, isAlreadyRunning(&stringError{"something else"}))
	require.False(t, isAlreadyRunning(nil))
}

func TestTemplateNode_Accessor(t *testing.T) {
	t.Parallel()
	p := newTestProvisioner(t, mockServer(t, &captured{}, http.StatusOK, `{}`), "pve7")
	require.Equal(t, "pve7", p.TemplateNode())
}

// TestAgentFileWrite_BubblesServerErrors verifies that a non-2xx response
// from Proxmox propagates as an error to the caller, exercising the full
// retry+backoff path. Skipped under -short because the configured retry
// budget makes this take ~15s end-to-end.
func TestAgentFileWrite_BubblesServerErrors(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping retry-backoff test under -short")
	}
	srv := mockServer(t, &captured{}, http.StatusInternalServerError, `{"errors":{"file":"permission denied"}}`)
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")
	err := p.agentFileWrite(context.Background(), "pve1", 1, "/x", []byte("y"))
	require.Error(t, err)
}

// Proxmox returns 500 when the in-VM agent reports a file-not-found from
// QGA. The go-proxmox library special-cases 500/501 to errors, which we
// rely on here.
func TestReadAgentFile_AgentErrorSurfaces(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping retry-backoff test under -short")
	}
	srv := mockServer(t, &captured{}, http.StatusInternalServerError, `{"errors":{"file":"no such file"}}`)
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")
	_, err := p.ReadAgentFile(context.Background(), &VM{VMID: 1, Node: "pve1"}, "/missing")
	require.Error(t, err)
}

// stringError is a trivial error type whose message we control. Used to feed
// isNotFound / isAlreadyRunning without depending on the proxmox library's
// internal error shape.
type stringError struct{ s string }

func (e *stringError) Error() string { return e.s }

// TestAgentExecWait_HonoursCtxCancel: when the in-VM command never
// finishes, ctx cancellation must unwind agentExecWait promptly rather
// than waiting for the 30s internal deadline. Regression guard for the
// previous time.Sleep-based polling loop that ignored ctx.
func TestAgentExecWait_HonoursCtxCancel(t *testing.T) {
	t.Parallel()
	// Mock server: POST /exec returns pid; subsequent GET /exec-status
	// always reports "not exited" so the poll loop never naturally
	// terminates.
	mu := sync.Mutex{}
	statusCalls := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/agent/exec"):
			_, _ = io.WriteString(w, `{"data": {"pid": 42}}`)
		case strings.HasSuffix(r.URL.Path, "/agent/exec-status"):
			statusCalls++
			_, _ = io.WriteString(w, `{"data": {"exited": false}}`)
		default:
			http.NotFound(w, r)
		}
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after 200ms — well below the internal 30s deadline.
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := p.agentExecWait(ctx, "pve1", 1, []string{"sleep", "forever"})
	elapsed := time.Since(start)

	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	// Should unwind within a few hundred ms — definitely not 30s. Be
	// generous to avoid CI flakes.
	require.Less(t, elapsed, 2*time.Second,
		"agentExecWait returned in %s — ctx cancel must propagate promptly", elapsed)
}

// Sanity: the proxmox.Client we build in tests reaches the test server.
func TestNewProxmoxClient_ReachesTestServer(t *testing.T) {
	t.Parallel()
	got := &captured{}
	srv := mockServer(t, got, http.StatusOK, `{"data":[]}`)
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")
	_, err := p.cli.Nodes(context.Background())
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(got.Path, "/nodes"))
}
