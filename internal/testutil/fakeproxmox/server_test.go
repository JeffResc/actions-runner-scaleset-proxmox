package fakeproxmox_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakeproxmox"
)

// do is a tiny helper that wraps a request against the fake and returns
// (status, decoded body envelope, body text for error inspection).
func do(t *testing.T, srv *fakeproxmox.Server, method, path string, body any) (int, map[string]any, string) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, srv.URL+path, rdr)
	require.NoError(t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var env map[string]any
	_ = json.Unmarshal(raw, &env)
	return resp.StatusCode, env, string(raw)
}

func TestServer_Version(t *testing.T) {
	t.Parallel()
	srv := fakeproxmox.New(t, fakeproxmox.Options{})

	status, env, _ := do(t, srv, http.MethodGet, "/version", nil)
	require.Equal(t, http.StatusOK, status)
	data, ok := env["data"].(map[string]any)
	require.True(t, ok, "expected {data: {...}} envelope")
	require.NotEmpty(t, data["version"])
}

func TestServer_TemplateSeededByDefault(t *testing.T) {
	t.Parallel()
	srv := fakeproxmox.New(t, fakeproxmox.Options{})

	// The default template lives on the default node "pve1" with VMID 9000.
	status, env, _ := do(t, srv, http.MethodGet, "/nodes/pve1/qemu/9000/status/current", nil)
	require.Equal(t, http.StatusOK, status)
	data, _ := env["data"].(map[string]any)
	require.Equal(t, float64(9000), data["vmid"])
	// template:1 is the marker go-proxmox unmarshalls into VirtualMachine.Template.
	require.Equal(t, float64(1), data["template"])
}

func TestServer_API2JSONPrefixIsStripped(t *testing.T) {
	t.Parallel()
	srv := fakeproxmox.New(t, fakeproxmox.Options{})

	// Production config sets Endpoint = ".../api2/json". The fake's
	// stripping middleware must accept that prefix as a no-op.
	status, env, _ := do(t, srv, http.MethodGet, "/api2/json/version", nil)
	require.Equal(t, http.StatusOK, status)
	require.NotNil(t, env["data"])
}

func TestServer_CloneLifecycle(t *testing.T) {
	t.Parallel()
	srv := fakeproxmox.New(t, fakeproxmox.Options{
		TaskDuration: 5 * time.Millisecond,
	})

	// Issue the clone.
	status, env, body := do(t, srv, http.MethodPost, "/nodes/pve1/qemu/9000/clone", map[string]any{
		"newid":  10042,
		"name":   "gh-runner-test-1",
		"target": "pve1",
	})
	require.Equal(t, http.StatusOK, status, body)
	upid, ok := env["data"].(string)
	require.True(t, ok, "clone POST must return UPID as data")
	require.NotEmpty(t, upid)

	// Task should be "running" initially, "stopped"+"OK" once the
	// configured duration elapses.
	taskPath := fmt.Sprintf("/nodes/pve1/tasks/%s/status", upid)
	_, env, _ = do(t, srv, http.MethodGet, taskPath, nil)
	taskData, _ := env["data"].(map[string]any)
	require.Equal(t, "running", taskData["status"])

	require.Eventually(t, func() bool {
		_, env, _ := do(t, srv, http.MethodGet, taskPath, nil)
		td, _ := env["data"].(map[string]any)
		return td["status"] == "stopped" && td["exitstatus"] == "OK"
	}, 1*time.Second, 5*time.Millisecond)

	// New VM exists; tags are empty until the orchestrator stamps them.
	_, env, _ = do(t, srv, http.MethodGet, "/nodes/pve1/qemu/10042/status/current", nil)
	vmData, _ := env["data"].(map[string]any)
	require.Equal(t, float64(10042), vmData["vmid"])
	require.Equal(t, "stopped", vmData["status"])
	require.Equal(t, "", vmData["tags"])

	// Tag the VM via PUT /config (same call the orchestrator makes).
	status, _, body = do(t, srv, http.MethodPut, "/nodes/pve1/qemu/10042/config", map[string]any{
		"tags": "gh-scaleset;gh-scaleset-owner-test",
	})
	require.Equal(t, http.StatusOK, status, body)

	_, env, _ = do(t, srv, http.MethodGet, "/nodes/pve1/qemu/10042/status/current", nil)
	vmData, _ = env["data"].(map[string]any)
	require.Equal(t, "gh-scaleset;gh-scaleset-owner-test", vmData["tags"])

	// Snapshot mirrors the wire view.
	snap := srv.Snapshot()
	require.Len(t, snap, 2) // template + clone
	var clone fakeproxmox.VMSnapshot
	for _, v := range snap {
		if v.VMID == 10042 {
			clone = v
			break
		}
	}
	require.Equal(t, "gh-scaleset;gh-scaleset-owner-test", clone.Tags)
}

func TestServer_StartStopDestroy(t *testing.T) {
	t.Parallel()
	srv := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 1 * time.Millisecond})
	srv.SeedVM("pve1", 12345, "gh-runner-test-1", false, []string{"gh-scaleset"})

	// Start.
	status, env, _ := do(t, srv, http.MethodPost, "/nodes/pve1/qemu/12345/status/start", nil)
	require.Equal(t, http.StatusOK, status)
	require.NotEmpty(t, env["data"])

	// Status reflects running.
	_, env, _ = do(t, srv, http.MethodGet, "/nodes/pve1/qemu/12345/status/current", nil)
	vmData, _ := env["data"].(map[string]any)
	require.Equal(t, "running", vmData["status"])

	// Starting again returns "already running" (matches orchestrator's
	// ErrVMAlreadyRunning classification).
	status, _, body := do(t, srv, http.MethodPost, "/nodes/pve1/qemu/12345/status/start", nil)
	require.Equal(t, http.StatusInternalServerError, status)
	require.Contains(t, body, "already running")

	// Stop returns to stopped.
	status, _, _ = do(t, srv, http.MethodPost, "/nodes/pve1/qemu/12345/status/stop", nil)
	require.Equal(t, http.StatusOK, status)
	_, env, _ = do(t, srv, http.MethodGet, "/nodes/pve1/qemu/12345/status/current", nil)
	vmData, _ = env["data"].(map[string]any)
	require.Equal(t, "stopped", vmData["status"])

	// Destroy removes the VM. A subsequent GET reproduces the
	// "Configuration file ... does not exist" body the orchestrator
	// matches on for ErrVMNotFound.
	status, _, _ = do(t, srv, http.MethodDelete, "/nodes/pve1/qemu/12345", nil)
	require.Equal(t, http.StatusOK, status)

	status, _, body = do(t, srv, http.MethodGet, "/nodes/pve1/qemu/12345/status/current", nil)
	require.Equal(t, http.StatusInternalServerError, status)
	require.Contains(t, body, "does not exist")
}

func TestServer_AgentEndpoints(t *testing.T) {
	t.Parallel()
	srv := fakeproxmox.New(t, fakeproxmox.Options{TaskDuration: 1 * time.Millisecond})
	srv.SeedVM("pve1", 22222, "gh-runner-test", false, nil)

	// Agent calls against a stopped VM must fail (real Proxmox returns
	// "VM is not running" — orchestrator treats this class as
	// transient via ErrGuestAgentNotReady).
	status, _, body := do(t, srv, http.MethodGet, "/nodes/pve1/qemu/22222/agent/get-osinfo", nil)
	require.Equal(t, http.StatusInternalServerError, status)
	require.True(t, strings.Contains(body, "not running") || strings.Contains(body, "agent"), body)

	// Start it.
	_, _, _ = do(t, srv, http.MethodPost, "/nodes/pve1/qemu/22222/status/start", nil)

	// file-write acks with {"data": null}.
	status, env, _ := do(t, srv, http.MethodPost, "/nodes/pve1/qemu/22222/agent/file-write",
		map[string]any{"file": "/tmp/x", "content": "hello"})
	require.Equal(t, http.StatusOK, status)
	require.Contains(t, env, "data")

	// exec returns a pid; exec-status reports exited+exitcode=0.
	status, env, _ = do(t, srv, http.MethodPost, "/nodes/pve1/qemu/22222/agent/exec",
		map[string]any{"command": []string{"echo", "hi"}})
	require.Equal(t, http.StatusOK, status)
	pid, _ := env["data"].(map[string]any)
	require.NotZero(t, pid["pid"])

	status, env, _ = do(t, srv, http.MethodGet, "/nodes/pve1/qemu/22222/agent/exec-status?pid=4242", nil)
	require.Equal(t, http.StatusOK, status)
	ed, _ := env["data"].(map[string]any)
	require.Equal(t, float64(1), ed["exited"])
	require.Equal(t, float64(0), ed["exitcode"])

	// get-osinfo succeeds after the agent delay window (which is 0 here).
	status, env, _ = do(t, srv, http.MethodGet, "/nodes/pve1/qemu/22222/agent/get-osinfo", nil)
	require.Equal(t, http.StatusOK, status)
	osi, _ := env["data"].(map[string]any)
	require.NotNil(t, osi["result"])
}

func TestServer_GuestAgentDelay(t *testing.T) {
	t.Parallel()
	srv := fakeproxmox.New(t, fakeproxmox.Options{
		TaskDuration:    1 * time.Millisecond,
		GuestAgentDelay: 75 * time.Millisecond,
	})
	srv.SeedVM("pve1", 33333, "gh-runner", false, nil)
	_, _, _ = do(t, srv, http.MethodPost, "/nodes/pve1/qemu/33333/status/start", nil)

	// Immediately after start: agent not responding.
	status, _, body := do(t, srv, http.MethodGet, "/nodes/pve1/qemu/33333/agent/get-osinfo", nil)
	require.Equal(t, http.StatusInternalServerError, status)
	require.Contains(t, body, "guest agent")

	// After the window elapses, it must succeed.
	require.Eventually(t, func() bool {
		st, _, _ := do(t, srv, http.MethodGet, "/nodes/pve1/qemu/33333/agent/get-osinfo", nil)
		return st == http.StatusOK
	}, 1*time.Second, 10*time.Millisecond)
}

func TestServer_ListVMsByNode(t *testing.T) {
	t.Parallel()
	srv := fakeproxmox.New(t, fakeproxmox.Options{
		Nodes: []string{"pve1", "pve2"},
	})
	srv.SeedVM("pve1", 1001, "vm1", false, nil)
	srv.SeedVM("pve2", 2002, "vm2", true, []string{"gh-scaleset"})

	_, env, _ := do(t, srv, http.MethodGet, "/nodes/pve2/qemu", nil)
	list, _ := env["data"].([]any)
	require.Len(t, list, 1, "must filter by node")
	vm, _ := list[0].(map[string]any)
	require.Equal(t, float64(2002), vm["vmid"])
	require.Equal(t, "running", vm["status"])
	require.Equal(t, "gh-scaleset", vm["tags"])
}
