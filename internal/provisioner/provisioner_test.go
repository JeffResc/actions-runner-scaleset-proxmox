package provisioner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/config"
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
		cfg:               cfg,
		cli:               cli,
		scaleSetName:      "test-scaleset",
		templateNode:      templateNode,
		log:               quietLogger(),
		inFlightClones:    newTracker(5 * time.Minute),
		recentlyDestroyed: newTracker(10 * time.Minute),
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
	const jit = "ZW5jb2RlZGppdGNvbmZpZ2Jsb2I=" // base64("encodedjitconfigblob"); shape matches GitHub's JIT output
	err := p.InjectJITConfig(context.Background(), &VM{VMID: 12345, Node: "pve3"}, jit)
	require.NoError(t, err)

	require.Equal(t, "/nodes/pve3/qemu/12345/agent/file-write", captured.writePath)
	require.Equal(t, "/opt/actions-runner/jitconfig.env.tmp", captured.writeBody["file"])
	require.Contains(t, captured.writeBody["content"], "JIT_CONFIG=")
	require.Contains(t, captured.writeBody["content"], jit)
	// And the exec call should be the atomic rename.
	cmd, _ := captured.execBody["command"].([]any)
	require.Equal(t, []any{"mv", "/opt/actions-runner/jitconfig.env.tmp", "/opt/actions-runner/jitconfig.env"}, cmd)
}

func TestInjectJITConfig_RejectsNilOrEmpty(t *testing.T) {
	t.Parallel()
	p := newTestProvisioner(t, mockServer(t, &captured{}, http.StatusOK, `{}`), "pve1")

	require.Error(t, p.InjectJITConfig(context.Background(), nil, "validbase64=="))
	require.Error(t, p.InjectJITConfig(context.Background(), &VM{VMID: 1, Node: "pve1"}, ""))
}

// TestJITConfigPattern_AcceptsBothBase64Alphabets locks in #156: the
// pattern must accept both RFC 4648 standard (+/) and URL-safe (-_)
// base64 so a future GitHub API change to URL-safe JIT configs doesn't
// silently break runner registration. Either alphabet is safe because
// the injection guard is about quote / newline / shell metachars, none
// of which appear in either alphabet.
//
// Direct regex test rather than driving InjectJITConfig end-to-end:
// the full path requires a multi-endpoint mock (file-write + exec +
// exec-status) and the regex is the single load-bearing assertion
// here. Reject-path coverage already lives in
// TestInjectJITConfig_RejectsNonBase64.
func TestJITConfigPattern_AcceptsBothBase64Alphabets(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		input string
	}{
		{"standard alphabet", "ZW5jb2RlZGppdGNvbmZpZ2Jsb2I="},
		{"url-safe alphabet", "abc-def_ghi=="},
		{"mixed alphabets", "abc+def-ghi/jkl_mno=="},
		{"trailing padding only", "AAAA="},
		{"no padding", "ZWFzeQ"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.True(t, jitConfigPattern.MatchString(tc.input),
				"expected pattern to accept %q", tc.input)
		})
	}
}

// TestInjectJITConfig_RejectsNonBase64 guards the syntax check that
// blocks a non-base64 payload (anything that could carry an embedded
// single quote, newline, or shell metachar) from being written into
// the systemd env-file. The orchestrator's data source for this config
// is the GitHub API; a non-base64 value implies upstream returned an
// error string in the wrong field.
func TestInjectJITConfig_RejectsNonBase64(t *testing.T) {
	t.Parallel()
	p := newTestProvisioner(t, mockServer(t, &captured{}, http.StatusOK, `{}`), "pve1")
	vm := &VM{VMID: 9, Node: "pve1"}

	// Embedded single quote — would otherwise break the env-file syntax.
	err := p.InjectJITConfig(context.Background(), vm, "abc'def==")
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected base64")

	// Newline — would split the env-file into a second line under
	// systemd's parser.
	err = p.InjectJITConfig(context.Background(), vm, "abc\ndef==")
	require.Error(t, err)

	// Shell metachar — irrelevant under env-file syntax but a useful
	// fuzz boundary.
	err = p.InjectJITConfig(context.Background(), vm, "abc;rm -rf /;def")
	require.Error(t, err)
}

func TestReadJITConfig_DecodesPayload(t *testing.T) {
	t.Parallel()
	got := &captured{}
	srv := mockServer(t, got, http.StatusOK, `{"data": {"content": "some file contents"}}`)
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")
	out, err := p.ReadJITConfig(context.Background(), &VM{VMID: 555, Node: "pve9"})
	require.NoError(t, err)
	require.Equal(t, []byte("some file contents"), out)

	require.Equal(t, "/nodes/pve9/qemu/555/agent/file-read", got.Path)
	require.Equal(t, []string{"/opt/actions-runner/jitconfig.env"}, got.Query["file"],
		"ReadJITConfig must always request the canonical jitconfig path; no caller controls it")
}

// TestDiscoverTemplateNode_OneHungNodeDoesNotBlock: a single unreachable
// node in the cluster (its /nodes/<name>/status hangs) must not pin
// orchestrator startup. Before the per-node timeout fix, the unbounded
// cli.Node call would block discoverTemplateNode forever.
func TestDiscoverTemplateNode_OneHungNodeDoesNotBlock(t *testing.T) {
	t.Parallel()

	// Shorter per-node budget for the test so we don't sit on 30s.
	prev := templateDiscoveryTimeoutPerNode
	templateDiscoveryTimeoutPerNode = 200 * time.Millisecond
	t.Cleanup(func() { templateDiscoveryTimeoutPerNode = prev })

	mux := http.NewServeMux()
	mux.HandleFunc("/nodes", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":[{"node":"hung"},{"node":"fast"}]}`)
	})
	mux.HandleFunc("/nodes/hung/status", func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // hang until the per-node timeout fires
	})
	mux.HandleFunc("/nodes/fast/status", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{}}`)
	})
	mux.HandleFunc("/nodes/fast/qemu/9000/status/current", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{"vmid":9000,"template":1,"name":"runner-template","status":"stopped"}}`)
	})
	mux.HandleFunc("/nodes/fast/qemu/9000/config", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{"template":1,"name":"runner-template"}}`)
	})
	mux.HandleFunc("/nodes/hung/qemu/9000/status/current", func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
	mux.HandleFunc("/nodes/hung/qemu/9000/config", func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cfg := config.ProxmoxConfig{
		Endpoint:           srv.URL,
		InsecureSkipVerify: true,
		Auth:               config.ProxmoxAuth{TokenID: "a!b", TokenSecret: "x"},
		TemplateVMID:       9000,
	}
	p := &pmox{cfg: cfg, cli: newProxmoxClient(cfg), scaleSetName: "t", log: quietLogger()}

	start := time.Now()
	err := p.discoverTemplateNode(context.Background())
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Equal(t, "fast", p.templateNode)
	// Must complete within a small multiple of the per-node timeout.
	require.Less(t, elapsed, 2*time.Second,
		"discoverTemplateNode took %s; expected one hung node to be bounded by templateDiscoveryTimeoutPerNode", elapsed)
}

// TestListOwnedVMs_OneHungNodeDoesNotBlock: one unreachable node must
// not pin sweepProxmoxOrphans for the underlying HTTP client's full
// timeout per tick. Mirrors the per-node-timeout guarantee already in
// place for discoverTemplateNode.
func TestListOwnedVMs_OneHungNodeDoesNotBlock(t *testing.T) {
	// Mutates the package-level listOwnedVMsTimeoutPerNode, which
	// other ListOwnedVMs tests read — keep this test serial so
	// -race doesn't flag the unsynchronised var.
	prev := listOwnedVMsTimeoutPerNode
	listOwnedVMsTimeoutPerNode = 200 * time.Millisecond
	t.Cleanup(func() { listOwnedVMsTimeoutPerNode = prev })

	mux := http.NewServeMux()
	mux.HandleFunc("/nodes", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":[{"node":"hung"},{"node":"fast"}]}`)
	})
	mux.HandleFunc("/nodes/hung/status", func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
	mux.HandleFunc("/nodes/hung/qemu", func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
	mux.HandleFunc("/nodes/fast/status", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{}}`)
	})
	mux.HandleFunc("/nodes/fast/qemu", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":[]}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cfg := config.ProxmoxConfig{
		Endpoint:           srv.URL,
		InsecureSkipVerify: true,
		Auth:               config.ProxmoxAuth{TokenID: "a!b", TokenSecret: "x"},
		TemplateVMID:       9000,
		VMIDRange:          config.VMIDRange{Min: 10000, Max: 19999},
	}
	p := &pmox{cfg: cfg, cli: newProxmoxClient(cfg), scaleSetName: "t", log: quietLogger()}

	start := time.Now()
	vms, err := p.ListOwnedVMs(context.Background())
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Empty(t, vms, "fast node has no owned VMs")
	require.Less(t, elapsed, 2*time.Second,
		"ListOwnedVMs took %s; one hung node should be bounded by listOwnedVMsTimeoutPerNode", elapsed)
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

// TestClassifiers_Isolated exercises each detection strategy on its
// own so a regression in one layer surfaces independently from the
// others. The full-pipeline coverage stays in TestClassifyProxmoxError.
func TestClassifiers_Isolated(t *testing.T) {
	t.Parallel()
	t.Run("typed sentinel hit", func(t *testing.T) {
		t.Parallel()
		ok, got := classifyTypedError(proxmox.ErrNotFound)
		require.True(t, ok)
		require.ErrorIs(t, got, ErrVMNotFound)
	})
	t.Run("typed sentinel miss leaves error untouched", func(t *testing.T) {
		t.Parallel()
		ok, _ := classifyTypedError(&stringError{"404 Not Found"})
		require.False(t, ok, "typed classifier must not recognise stringly-typed errors")
	})
	t.Run("http status hit", func(t *testing.T) {
		t.Parallel()
		ok, got := classifyByHTTPStatus(&stringError{"404 Not Found"})
		require.True(t, ok)
		require.ErrorIs(t, got, ErrVMNotFound)
	})
	t.Run("http status non-404 miss", func(t *testing.T) {
		t.Parallel()
		ok, _ := classifyByHTTPStatus(&stringError{"500 Internal Server Error"})
		require.False(t, ok)
	})
	t.Run("body text hit (not-found)", func(t *testing.T) {
		t.Parallel()
		ok, got := classifyByMessage(&stringError{"vm config does not exist"})
		require.True(t, ok)
		require.ErrorIs(t, got, ErrVMNotFound)
	})
	t.Run("body text hit (already-running)", func(t *testing.T) {
		t.Parallel()
		ok, got := classifyByMessage(&stringError{"VM 10042 already running"})
		require.True(t, ok)
		require.ErrorIs(t, got, ErrVMAlreadyRunning)
	})
	t.Run("body text miss", func(t *testing.T) {
		t.Parallel()
		ok, _ := classifyByMessage(&stringError{"something else entirely"})
		require.False(t, ok)
	})
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
func TestReadJITConfig_AgentErrorSurfaces(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping retry-backoff test under -short")
	}
	srv := mockServer(t, &captured{}, http.StatusInternalServerError, `{"errors":{"file":"no such file"}}`)
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")
	_, err := p.ReadJITConfig(context.Background(), &VM{VMID: 1, Node: "pve1"})
	require.Error(t, err)
}

// stringError is a trivial error type whose message we control. Used to feed
// isNotFound / isAlreadyRunning without depending on the proxmox library's
// internal error shape.
type stringError struct{ s string }

func (e *stringError) Error() string { return e.s }

// TestAgentExecWait_HandlesBoolAndFloatExited verifies the polymorphic
// `exited` JSON field is correctly interpreted across Proxmox versions
// (some emit bool, some emit a JSON number). The previous `case int:`
// arm in the type switch was unreachable — encoding/json decodes all
// JSON numbers into float64 when the target is `any` — so the arm has
// been removed and only the bool/float64 cases remain.
func TestAgentExecWait_HandlesBoolAndFloatExited(t *testing.T) {
	t.Parallel()
	for _, exited := range []string{`true`, `1`, `1.0`} {
		t.Run("exited="+exited, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/agent/exec"):
					_, _ = io.WriteString(w, `{"data": {"pid": 42}}`)
				case strings.HasSuffix(r.URL.Path, "/agent/exec-status"):
					_, _ = io.WriteString(w, `{"data": {"exited": `+exited+`, "exitcode": 0}}`)
				default:
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()

			p := newTestProvisioner(t, srv, "pve1")
			err := p.agentExecWait(context.Background(), "pve1", 1, []string{"ls"})
			require.NoError(t, err)
		})
	}
}

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

// TestWaitReady_CtxCancelUnwindsPromptly covers the case the pool's
// promote path most cares about: the guest agent is unresponsive and
// the caller's deadline has to unwind WaitReady before the lib's
// internal budget could expire. Without this test the only WaitReady
// coverage was error-classification — the timeout-firing path itself
// was untested (#145).
//
// The fake responds OK to the status/current lookup (so getVM
// succeeds), then hangs on /agent/get-osinfo until r.Context fires.
// We pass a 200ms ctx deadline + a generous 60s lib-side timeout so
// the assertion is unambiguously about ctx-deadline propagation
// rather than the lib's own polling budget. The whole call must
// unwind well inside the caller's deadline so the pool's bootSem
// stays moving across the fleet.
func TestWaitReady_CtxCancelUnwindsPromptly(t *testing.T) {
	t.Parallel()

	var agentCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		// p.cli.Node(ctx, "pve1") in getVM hits /nodes/{node}/status.
		case r.URL.Path == "/nodes/pve1/status":
			_, _ = io.WriteString(w, `{"data":{}}`)
		// templateNode.VirtualMachine(...) hits status/current to populate
		// the VirtualMachine struct, then config to enrich it.
		case strings.HasSuffix(r.URL.Path, "/status/current"):
			_, _ = io.WriteString(w, `{"data":{"vmid":9999,"name":"runner","status":"running"}}`)
		case strings.HasSuffix(r.URL.Path, "/config"):
			_, _ = io.WriteString(w, `{"data":{}}`)
		case strings.Contains(r.URL.Path, "/agent/get-osinfo"):
			agentCalls.Add(1)
			// Hang until the request's ctx is cancelled by the caller.
			// This simulates a QGA that has accepted the call but is
			// itself stuck — the failure mode the issue calls out as
			// most operationally relevant.
			<-r.Context().Done()
		default:
			t.Logf("unexpected path: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")

	// Tight ctx deadline + generous library timeout so the assertion
	// is unambiguously about ctx-deadline propagation, not the lib's
	// internal polling budget.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := p.WaitReady(ctx, &VM{VMID: 9999, Node: "pve1"}, 60*time.Second)
	elapsed := time.Since(start)

	require.Error(t, err)
	// Bounded so a regression that ignored ctx would fail loudly
	// rather than slow CI down silently. Generous headroom for
	// retryablehttp's first-attempt backoff before the lib gives up.
	require.Less(t, elapsed, 10*time.Second,
		"WaitReady ctx-cancel must unwind in ~200ms; took %s — bootSem would back up across the pool", elapsed)
	require.Positive(t, agentCalls.Load(),
		"the test must actually exercise the agent poll path; got 0 calls")
}

// TestClone_CtxCancelledClearsInFlight is the timeout-firing twin of
// TestClone_ClearsInFlightOnError. The latter covers "PVE returns
// 500"; this covers "caller's ctx expires mid-Clone". Both shapes
// must clear the in-flight tracker via the defer at Clone's entry
// — otherwise a recurring orchestrator cancellation (e.g. drain
// during a clone burst) permanently mutes untagged-orphan warnings
// for that VMID, and operators never learn the VM is actually stuck.
func TestClone_CtxCancelledClearsInFlight(t *testing.T) {
	t.Parallel()
	// Server hangs on every request until r.Context is cancelled. With
	// an already-cancelled ctx, every HTTP call short-circuits — but
	// the in-flight tracker should still get cleared by Clone's defer.
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled when Clone is called

	_, err := p.Clone(ctx, CloneOptions{NewVMID: 10043, Node: "pve1", Name: "x"})
	require.Error(t, err, "Clone against a cancelled ctx must surface the cancellation")

	require.False(t, p.inFlightClones.Has(10043),
		"ctx-cancellation during Clone must still clear the in-flight entry — repeated cancellations would permanently mute orphan warnings for the VMID")
}

// TestWaitReady_ClassifiesVMNotFound: when go-proxmox surfaces an error
// whose body says "does not exist", WaitReady must wrap so callers can
// errors.Is(err, ErrVMNotFound) without depending on which library
// internal raised it. Uses a 400 response (which go-proxmox preserves
// the body of) since 500 responses are flattened to just "500 Internal
// Server Error" inside the library.
func TestWaitReady_ClassifiesVMNotFound(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping retry-backoff path under -short")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"errors":{"vm":"VM does not exist"}}`))
	}))
	defer srv.Close()
	p := newTestProvisioner(t, srv, "pve1")

	err := p.WaitReady(context.Background(), &VM{VMID: 9999, Node: "pve1"}, time.Second)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrVMNotFound,
		"WaitReady must wrap library errors through classifyProxmoxError")
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

// listOwnedVMsServer returns an httptest server that answers the three
// GET endpoints ListOwnedVMs needs: cluster node list, per-node status
// (go-proxmox's Client.Node helper hits /nodes/{node}/status to enrich
// the Node object), and per-node VM list. vmsJSON is the raw JSON for
// the qemu list.
func listOwnedVMsServer(t *testing.T, nodeName, vmsJSON string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/nodes", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"data":[{"node":%q,"status":"online"}]}`, nodeName)
	})
	mux.HandleFunc("/nodes/"+nodeName+"/status", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{}}`)
	})
	mux.HandleFunc("/nodes/"+nodeName+"/qemu", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, vmsJSON)
	})
	return httptest.NewServer(mux)
}

// TestListOwnedVMs_SuppressesUntaggedWarningForInFlightClones locks
// in the behaviour around the qmclone→qmconfig tag-apply window.
// During that window the VM exists in PVE with our name prefix but
// without the owner tag; ListOwnedVMs must NOT log "untagged orphan
// detected" for VMIDs we are actively cloning — the orchestrator
// already owns the VM, the tag just hasn't landed yet. The VM is
// still included in the returned slice so callers see a complete
// owned set.
func TestListOwnedVMs_SuppressesUntaggedWarningForInFlightClones(t *testing.T) {
	t.Parallel()

	// VM with our name prefix, in our VMID range, but NO tags — the
	// exact window between qmclone returning and qmconfig applying
	// tags.
	srv := listOwnedVMsServer(t, "pve1",
		`{"data":[{"vmid":10004,"name":"gh-runner-test-scaleset-10004","status":"running","tags":""}]}`)
	defer srv.Close()

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	p := newTestProvisioner(t, srv, "pve1")
	p.log = logger
	p.vmNamePrefix = "gh-runner-test-scaleset-"
	p.cfg.VMIDRange = config.VMIDRange{Min: 10000, Max: 19999}

	// Mark VMID 10004 as currently being cloned — Clone has returned
	// from PVE's clone task but hasn't yet applied the ownership tag.
	p.inFlightClones.Set(10004, time.Now(), ttlcache.DefaultTTL)

	vms, err := p.ListOwnedVMs(context.Background())
	require.NoError(t, err)
	require.Len(t, vms, 1,
		"in-flight VM must still be reported as owned (the row in the store points at this VMID)")
	require.Equal(t, 10004, vms[0].VMID)

	require.NotContains(t, logBuf.String(), "untagged orphan detected",
		"the WARN must be suppressed while the clone is in-flight; got log: %s", logBuf.String())
}

// TestTrackers_LibraryEvictsEntriesPastTTL exercises the ttlcache
// background-eviction path our trackers rely on. Without it a hung
// Clone() would leak an inflight entry forever (suppressing future
// warnings for that VMID) and the recentlyDestroyed map would grow
// unbounded under destroy churn.
func TestTrackers_LibraryEvictsEntriesPastTTL(t *testing.T) {
	t.Parallel()
	p := &pmox{
		log:               quietLogger(),
		inFlightClones:    newTracker(10 * time.Millisecond),
		recentlyDestroyed: newTracker(10 * time.Millisecond),
	}
	p.inFlightClones.Set(101, time.Now(), ttlcache.DefaultTTL)
	p.recentlyDestroyed.Set(201, time.Now(), ttlcache.DefaultTTL)

	require.Eventually(t, func() bool {
		// DeleteExpired is the deterministic eviction trigger; the
		// library's Start() loop wakes on the same path on a timer.
		p.inFlightClones.DeleteExpired()
		p.recentlyDestroyed.DeleteExpired()
		return p.inFlightClones.Get(101) == nil && p.recentlyDestroyed.Get(201) == nil
	}, time.Second, 5*time.Millisecond, "TTL eviction must drop entries past the cache TTL")
}

// TestIsRecentlyDestroyed_EvictsOnceCooldownElapses pins the
// caller-supplied cooldown semantics: an entry within the cache's
// (longer) TTL must still report "not recent" once the caller's
// cooldown has elapsed, and the entry should be dropped eagerly so
// the cache reflects ground truth.
func TestIsRecentlyDestroyed_EvictsOnceCooldownElapses(t *testing.T) {
	t.Parallel()
	p := &pmox{
		log:               quietLogger(),
		recentlyDestroyed: newTracker(time.Hour),
	}
	// Insert with a timestamp that is already past the caller's cooldown.
	p.recentlyDestroyed.Set(10042, time.Now().Add(-2*time.Minute), ttlcache.DefaultTTL)
	require.False(t, p.IsRecentlyDestroyed(10042, time.Minute),
		"cooldown elapsed → must return false even while inside the cache TTL")
	require.Nil(t, p.recentlyDestroyed.Get(10042),
		"the stale entry must be evicted on read")
}

// TestDestroy_DoesNotMarkRecentlyDestroyedOnError: if Destroy fails
// after stopping the VM (or fails early), the VMID must NOT enter the
// recently-destroyed cooldown set. Otherwise the pool's allocator
// would refuse to reissue a VMID that PVE never actually released —
// blocking the orchestrator until the cooldown expires, even though
// the VM is still up and using the ID.
func TestDestroy_DoesNotMarkRecentlyDestroyedOnError(t *testing.T) {
	t.Parallel()
	// Mock that 500s on every Proxmox call so the getVM step inside
	// Destroy returns a non-404 error and Destroy bails before any
	// destroy actually happens.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"data":null,"errors":{"vm":"transient PVE failure"}}`))
	}))
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")
	err := p.Destroy(context.Background(), &VM{VMID: 10042, Node: "pve1"})
	require.Error(t, err, "Destroy must surface the PVE failure, not swallow it")

	require.False(t, p.IsRecentlyDestroyed(10042, time.Hour),
		"Destroy failure must NOT mark the VMID as recently-destroyed — otherwise the allocator will refuse to reuse it even though PVE never released it")
}

// TestDestroy_TreatsMissingVMAsIdempotent: a Destroy targeting a VM
// that has already been deleted (concurrent admin action, prior
// crash, etc.) must return nil. The recentlyDestroyed map is NOT
// updated because there was nothing for us to destroy; the cooldown
// only protects against PVE still settling our own teardown.
func TestDestroy_TreatsMissingVMAsIdempotent(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"errors":{"vm":"VM does not exist"}}`))
	}))
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")
	err := p.Destroy(context.Background(), &VM{VMID: 10042, Node: "pve1"})
	require.NoError(t, err, "a missing VM is idempotent success")
	require.False(t, p.IsRecentlyDestroyed(10042, time.Hour),
		"a no-op Destroy must NOT enter the cooldown set")
}

// TestListOwnedVMs_PartialNodeFailureReturnsRest: if one node in the
// cluster is unreachable (returns 500), ListOwnedVMs must log a
// warning, skip that node, and return VMs from the reachable nodes.
// A whole-cluster failure was the original symptom captured in
// production ("provisioner: list nodes: not authorized" when a node
// was down) — degrading gracefully here is critical.
func TestListOwnedVMs_PartialNodeFailureReturnsRest(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.HandleFunc("/nodes", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":[
			{"node":"pve-good","status":"online"},
			{"node":"pve-bad","status":"unknown"}
		]}`)
	})
	// Healthy node: VM that's clearly ours (correct owner tag).
	mux.HandleFunc("/nodes/pve-good/status", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{}}`)
	})
	mux.HandleFunc("/nodes/pve-good/qemu", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w,
			`{"data":[{"vmid":10005,"name":"gh-runner-test-scaleset-10005","status":"running","tags":"gh-scaleset;gh-scaleset-owner-test-scaleset"}]}`)
	})
	// Failed node: 500 to anything.
	mux.HandleFunc("/nodes/pve-bad/status", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	mux.HandleFunc("/nodes/pve-bad/qemu", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve-good")
	p.vmNamePrefix = "gh-runner-test-scaleset-"
	p.cfg.VMIDRange = config.VMIDRange{Min: 10000, Max: 19999}

	vms, err := p.ListOwnedVMs(context.Background())
	require.NoError(t, err, "a partial failure must NOT cause the whole call to error")
	require.Len(t, vms, 1, "the reachable node's VMs must still be returned")
	require.Equal(t, 10005, vms[0].VMID)
}

// TestClone_ClearsInFlightOnError: if Clone() returns an error after
// reaching the Proxmox call (so the in-flight entry was already
// recorded), the defer must still remove it. Without this guarantee,
// a recurring clone failure suppresses untagged-orphan warnings
// indefinitely for that VMID — the operator never learns the VM is
// actually stuck.
func TestClone_ClearsInFlightOnError(t *testing.T) {
	t.Parallel()
	// All PVE calls 500 — Clone's get-template-node call fails fast.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"data":null}`))
	}))
	defer srv.Close()

	p := newTestProvisioner(t, srv, "pve1")
	_, err := p.Clone(context.Background(), CloneOptions{NewVMID: 10042, Node: "pve1", Name: "x"})
	require.Error(t, err, "the fake PVE returns 500 so Clone must fail")

	require.False(t, p.inFlightClones.Has(10042),
		"Clone error must still clear the in-flight entry — otherwise repeated failures permanently mute the warning for that VMID")
}

// TestListOwnedVMs_StillWarnsOnRealUntaggedOrphan is the corollary:
// a VM matching the name prefix + VMID range but NOT in the in-flight
// set is a genuine "crashed mid-clone" orphan from a previous
// orchestrator process. Those still need the WARN so operators
// notice them.
func TestListOwnedVMs_StillWarnsOnRealUntaggedOrphan(t *testing.T) {
	t.Parallel()
	srv := listOwnedVMsServer(t, "pve1",
		`{"data":[{"vmid":10004,"name":"gh-runner-test-scaleset-10004","status":"running","tags":""}]}`)
	defer srv.Close()

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	p := newTestProvisioner(t, srv, "pve1")
	p.log = logger
	p.vmNamePrefix = "gh-runner-test-scaleset-"
	p.cfg.VMIDRange = config.VMIDRange{Min: 10000, Max: 19999}
	// NOT marking 10004 as in-flight.

	_, err := p.ListOwnedVMs(context.Background())
	require.NoError(t, err)
	require.Contains(t, logBuf.String(), "untagged orphan detected",
		"genuine crash-mid-clone orphans must still warn")
}

// ---------------------------------------------------------------------------
// encodeNIC (PR 3 — issue #6)
// ---------------------------------------------------------------------------

func TestEncodeNIC_DefaultsModelToVirtio(t *testing.T) {
	t.Parallel()
	got := encodeNIC(CloneNIC{Bridge: "vmbr0"})
	require.Equal(t, "virtio,bridge=vmbr0", got)
}

func TestEncodeNIC_TaggedVLANAddsTagAttr(t *testing.T) {
	t.Parallel()
	got := encodeNIC(CloneNIC{Bridge: "vmbr0", VLANTag: 42})
	require.Equal(t, "virtio,bridge=vmbr0,tag=42", got)
}

func TestEncodeNIC_UntaggedSkipsTagEvenIfTagNumberSet(t *testing.T) {
	t.Parallel()
	// VLANUntagged=true is the operator's explicit "no tag" — the
	// VLANTag field is ignored when this is set.
	got := encodeNIC(CloneNIC{Bridge: "vmbr0", VLANTag: 42, VLANUntagged: true})
	require.Equal(t, "virtio,bridge=vmbr0", got)
}

func TestEncodeNIC_ZeroVLANTagSkipsAttribute(t *testing.T) {
	t.Parallel()
	// Tag=0 (without VLANUntagged) skips the tag= attribute so the
	// bridge's VLAN-aware default applies.
	got := encodeNIC(CloneNIC{Bridge: "vmbr0", VLANTag: 0})
	require.Equal(t, "virtio,bridge=vmbr0", got)
}

func TestEncodeNIC_MTUJumboFrames(t *testing.T) {
	t.Parallel()
	got := encodeNIC(CloneNIC{Bridge: "vmbr1", VLANTag: 100, MTU: 9000})
	require.Equal(t, "virtio,bridge=vmbr1,tag=100,mtu=9000", got)
}

func TestEncodeNIC_CustomModel(t *testing.T) {
	t.Parallel()
	got := encodeNIC(CloneNIC{Bridge: "vmbr0", Model: "e1000"})
	require.Equal(t, "e1000,bridge=vmbr0", got)
}
