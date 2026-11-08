package provisioner

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf16"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/config"
)

func TestWindowsJITHandoffAndReadback(t *testing.T) {
	var calls []map[string]any
	var readPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/file-write"), strings.HasSuffix(r.URL.Path, "/exec"):
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			calls = append(calls, body)
			if strings.HasSuffix(r.URL.Path, "/exec") {
				_, _ = io.WriteString(w, `{"data":{"pid":7}}`)
			} else {
				_, _ = io.WriteString(w, `{"data":null}`)
			}
		case strings.HasSuffix(r.URL.Path, "/exec-status"):
			_, _ = io.WriteString(w, `{"data":{"exited":true,"exitcode":0}}`)
		case strings.HasSuffix(r.URL.Path, "/file-read"):
			readPath = r.URL.Query().Get("file")
			_, _ = io.WriteString(w, `{"data":{"content":"raw"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	p := newTestProvisioner(t, srv, "pve1")
	p.profiles = map[string]config.ProfileConfig{"win": {Name: "win", GuestOS: "windows", WindowsJITPath: `C:\handoff\jit`}}
	vm := &VM{VMID: 7, Node: "pve1", Profile: "win"}
	jit := "eyJydW5uZXJfaWQiOjQyfQ=="
	require.NoError(t, p.InjectJITConfig(t.Context(), vm, jit))
	require.Equal(t, `C:\handoff\jit.tmp`, calls[0]["file"])
	require.Equal(t, jit, calls[0]["content"])
	cmd := calls[1]["command"].([]any)
	require.Equal(t, "powershell.exe", cmd[0])
	require.Equal(t, "-EncodedCommand", cmd[3])
	require.NotContains(t, cmd[4], jit)
	encoded, err := base64.StdEncoding.DecodeString(cmd[4].(string))
	require.NoError(t, err)
	units := make([]uint16, len(encoded)/2)
	for i := range units {
		units[i] = uint16(encoded[2*i]) | uint16(encoded[2*i+1])<<8
	}
	script := string(utf16.Decode(units))
	require.Contains(t, script, `C:\handoff\jit.tmp`)
	require.Contains(t, script, "ReadAllText")
	require.Contains(t, script, "-cne")
	require.Contains(t, script, "[System.IO.File]::Move")
	data, err := p.ReadJITConfig(t.Context(), vm)
	require.NoError(t, err)
	require.Equal(t, "raw", string(data))
	require.Equal(t, `C:\handoff\jit`, readPath)
	_, _, err = p.guestJITSettings(&VM{Profile: "missing"})
	require.Error(t, err)
}

func TestWindowsJITWriteErrorDoesNotExposePayload(t *testing.T) {
	const jit = "eyJydW5uZXJfaWQiOjQyfQ=="
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, jit, http.StatusBadRequest)
	}))
	defer srv.Close()
	p := newTestProvisioner(t, srv, "pve1")
	p.profiles = map[string]config.ProfileConfig{"win": {Name: "win", GuestOS: "windows"}}
	err := p.InjectJITConfig(t.Context(), &VM{VMID: 7, Node: "pve1", Profile: "win"}, jit)
	require.Error(t, err)
	require.NotContains(t, err.Error(), jit)
}
