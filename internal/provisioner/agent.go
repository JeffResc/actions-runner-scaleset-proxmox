package provisioner

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/observability"
)

// jitConfigPattern is the character set GitHub's JIT configs are known
// to use: RFC 4648 base64, accepting both the standard (`+/`) and
// URL-safe (`-_`) alphabets plus padding. Validating against this
// before formatting into the systemd env-file blocks two failure modes:
// an embedded single quote breaking the env-file syntax, and an error
// string being misread as a config payload after a parse path elsewhere
// changes upstream. The orchestrator's only data source today is the
// GitHub API itself, so a mismatch implies an upstream contract break
// — bail out rather than write a malformed file.
//
// URL-safe characters are accepted alongside standard because newer
// GitHub APIs sometimes return URL-safe base64; either alphabet is safe
// here because the injection concern is about quote / newline / shell
// metacharacters, none of which appear in either.
var jitConfigPattern = regexp.MustCompile(`^[A-Za-z0-9+/=_-]+$`)

// validateDecodedJITConfig is the defense-in-depth step that runs
// after jitConfigPattern: base64-decode the payload (accepting both
// standard and URL-safe alphabets, with or without padding) and
// confirm the result is a non-empty JSON object. We don't pin a
// schema because the runner inside the VM is the consumer — but a
// successful JSON parse rules out two upstream-contract-break shapes
// the regex alone can't catch:
//
//  1. A base64-shaped string that isn't actually base64 (unlikely
//     today but cheap to defend against).
//  2. A base64-but-not-JSON blob (e.g. an error message that was
//     base64-encoded somewhere upstream and routed into the JIT-config
//     field by a future code path that loses GitHub-API context).
//
// Today the only producer is the GitHub API authenticated via the
// orchestrator's own credentials, so this is hardening — not closing
// a known live attack. The cost is one decode + one Unmarshal per
// inject, which is dominated by the qemu-guest-agent round-trip.
func validateDecodedJITConfig(jitConfig string) error {
	// Try standard first, then URL-safe — both are accepted by
	// jitConfigPattern, so we don't know which alphabet upstream
	// used. RawStd / RawURL also handle the unpadded variants the
	// regex accepts (e.g. "ZWFzeQ").
	decoders := []*base64.Encoding{
		base64.StdEncoding, base64.URLEncoding,
		base64.RawStdEncoding, base64.RawURLEncoding,
	}
	var (
		decoded []byte
		err     error
	)
	for _, dec := range decoders {
		decoded, err = dec.DecodeString(jitConfig)
		if err == nil {
			break
		}
	}
	if err != nil {
		return fmt.Errorf("base64 decode: %w", err)
	}
	if len(decoded) == 0 {
		return fmt.Errorf("decoded payload is empty")
	}
	// The runner expects a JSON object — anything else is an upstream
	// contract break. Unmarshal into a map (we don't enforce a schema)
	// so any well-formed JSON object passes.
	var obj map[string]any
	if err := json.Unmarshal(decoded, &obj); err != nil {
		return fmt.Errorf("decoded payload is not a JSON object: %w", err)
	}
	if len(obj) == 0 {
		return fmt.Errorf("decoded JSON object has no fields")
	}
	return nil
}

var tracer = otel.Tracer(observability.TracerName)

// Proxmox guest-agent file-write enforces a per-call body cap around 60 KiB
// (the QGA command channel limit). JIT runner configs are a few KB so this
// is plenty of headroom; if a future caller needs to write larger files we
// will need to chunk.
const agentFileWriteMaxBytes = 60 * 1024

// InjectJITConfig writes the JIT runner configuration into the canonical
// path watched by the in-VM systemd path-unit, formatted as a systemd
// environment-file. The runner unit loads the env file and passes
// JIT_CONFIG to `run.sh --jitconfig`.
//
// We use the env-file form (rather than passing via shell substitution
// in ExecStart) because shell substitution under systemd was unreliable:
// the runner intermittently received an empty value and exited with
// "Not configured", even though the underlying file was correct and the
// equivalent command worked from an interactive shell.
//
// The Proxmox guest-agent endpoint is reached directly because
// luthermonson/go-proxmox v0.7.0 does not expose a typed wrapper for it.
// We use the library's authenticated Client.Post so token rotation and
// transport configuration still go through the library.
func (p *pmox) InjectJITConfig(ctx context.Context, vm *VM, jitConfig string) error {
	if vm == nil {
		return fmt.Errorf("inject jit: nil vm")
	}
	if jitConfig == "" {
		return fmt.Errorf("inject jit: empty config for vm %d", vm.VMID)
	}
	if !jitConfigPattern.MatchString(jitConfig) {
		return fmt.Errorf("inject jit: config for vm %d does not match expected base64 form (chars outside [A-Za-z0-9+/=_-])", vm.VMID)
	}
	if err := validateDecodedJITConfig(jitConfig); err != nil {
		return fmt.Errorf("inject jit: config for vm %d failed decoded validation: %w", vm.VMID, err)
	}
	ctx, span := tracer.Start(ctx, "provisioner.InjectJITConfig", trace.WithAttributes(
		attribute.Int("vm.id", vm.VMID),
		attribute.String("vm.node", vm.Node),
		attribute.Int("jit.bytes", len(jitConfig)),
	))
	defer span.End()
	err := wrapGuestAgent(p.injectJITConfigInner(ctx, vm, jitConfig))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "inject failed")
	}
	return err
}

func (p *pmox) injectJITConfigInner(ctx context.Context, vm *VM, jitConfig string) error {
	// Quoting: JIT configs are pure ASCII base64 (chars in [A-Za-z0-9+/=]),
	// none of which need escaping in a systemd env-file value. We still
	// wrap in single quotes as a defensive measure against future format
	// changes.
	var b strings.Builder
	fmt.Fprintf(&b, "JIT_CONFIG='%s'\n", jitConfig)
	envContent := b.String()

	// Two-phase write to avoid a race with the in-VM systemd path-unit:
	// Proxmox's guest-agent file-write is NOT atomic (it does open +
	// write + close), and inotify IN_CREATE fires when open() happens.
	// If we wrote straight to /opt/actions-runner/jitconfig.env, the
	// path-unit could fire the .service while Proxmox was still streaming
	// bytes, and the runner would parse a truncated JIT and exit.
	//
	// Writing to .tmp then renaming makes the appearance of the final
	// path atomic from the kernel's perspective (single rename syscall),
	// so the path-unit sees a fully-formed file.
	const finalPath = "/opt/actions-runner/jitconfig.env"
	const tmpPath = "/opt/actions-runner/jitconfig.env.tmp"
	if err := p.agentFileWrite(ctx, vm.Node, vm.VMID, tmpPath, []byte(envContent)); err != nil {
		return fmt.Errorf("inject jit (tmp write): %w", err)
	}
	if err := p.agentExecWait(ctx, vm.Node, vm.VMID, []string{"mv", tmpPath, finalPath}); err != nil {
		return fmt.Errorf("inject jit (atomic rename): %w", err)
	}
	return nil
}

// agentExecWait runs a command inside the VM and waits for it to exit
// successfully. Returns an error if the command exits non-zero, if
// stderr is non-empty, or if ctx is cancelled mid-poll.
//
// The 30s deadline caps a single command; on top of that the loop
// honours ctx so SIGTERM (and the manager's drain cancel) propagates
// straight in instead of waiting up to 30s for the next poll tick.
func (p *pmox) agentExecWait(ctx context.Context, node string, vmid int, command []string) error {
	startResp := struct {
		Data struct {
			PID int `json:"pid"`
		} `json:"data"`
	}{}
	body := map[string]any{"command": command}
	apiPath := fmt.Sprintf("/nodes/%s/qemu/%d/agent/exec", node, vmid)
	if err := p.cli.Post(ctx, apiPath, body, &startResp.Data); err != nil {
		return fmt.Errorf("agent exec start: %w", err)
	}
	statusPath := fmt.Sprintf("/nodes/%s/qemu/%d/agent/exec-status", node, vmid)
	// Bound the wait by both a wall-clock deadline (30s) and ctx
	// cancellation. Most commands (like `mv`) finish in <100ms.
	pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		var statusResp struct {
			Exited   any    `json:"exited"`
			Exitcode int    `json:"exitcode"`
			ErrData  string `json:"err-data"`
		}
		if err := p.cli.GetWithParams(pollCtx, statusPath, map[string]int{"pid": startResp.Data.PID}, &statusResp); err != nil {
			if ctxErr := pollCtx.Err(); ctxErr != nil {
				return fmt.Errorf("agent exec %v: %w", command, ctxErr)
			}
			return fmt.Errorf("agent exec-status: %w", err)
		}
		// `exited` comes back as either bool true or a JSON number
		// (1) depending on PVE version. encoding/json decodes all
		// numbers into float64 when unmarshalled into an `any`
		// target, so a `case int:` arm is unreachable here.
		exited := false
		switch v := statusResp.Exited.(type) {
		case bool:
			exited = v
		case float64:
			exited = v == 1
		}
		if exited {
			if statusResp.Exitcode != 0 {
				return fmt.Errorf("agent exec %v failed: exit=%d stderr=%q", command, statusResp.Exitcode, statusResp.ErrData)
			}
			return nil
		}
		select {
		case <-pollCtx.Done():
			return fmt.Errorf("agent exec %v: %w", command, pollCtx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// ReadJITConfig reads the canonical JIT-config env file from inside the
// VM via the guest agent. Used by crash recovery to inspect
// /opt/actions-runner/jitconfig.env and decide whether a Hot-state VM
// has been consumed. The path is hardcoded so the only thing a caller
// controls is which VM to ask, not what file to read.
func (p *pmox) ReadJITConfig(ctx context.Context, vm *VM) ([]byte, error) {
	const jitConfigPath = "/opt/actions-runner/jitconfig.env"
	if vm == nil {
		return nil, fmt.Errorf("read jit config: nil vm")
	}
	return p.readAgentFile(ctx, vm, jitConfigPath)
}

// readAgentFile is the unexported guest-agent file-read primitive. Kept
// private so attacker-influenced paths can't reach it; every public
// caller goes through a hardcoded path wrapper above.
func (p *pmox) readAgentFile(ctx context.Context, vm *VM, path string) ([]byte, error) {
	apiPath := fmt.Sprintf("/nodes/%s/qemu/%d/agent/file-read", vm.Node, vm.VMID)
	// The library auto-unwraps the {"data": ...} envelope, so the target
	// struct describes the *inner* payload.
	var resp struct {
		Content   string `json:"content"`
		Truncated any    `json:"truncated,omitempty"` // sometimes int, sometimes bool
	}
	if err := p.cli.GetWithParams(ctx, apiPath, map[string]string{"file": path}, &resp); err != nil {
		return nil, fmt.Errorf("agent file-read %s: %w", path, err)
	}
	// file-read does NOT base64-encode by default; content is the raw file
	// bytes wrapped in JSON. We return the literal bytes.
	return []byte(resp.Content), nil
}

// isGuestAgentNotReady recognises the transient "qemu-guest-agent
// socket not yet responsive" class of errors Proxmox returns during
// firstboot churn. Wrapped via ErrGuestAgentNotReady so callers
// (e.g. scaler.injectWithRetry) can errors.Is rather than string-match.
func isGuestAgentNotReady(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "is not running") ||
		strings.Contains(s, "QEMU guest agent is not running") ||
		strings.Contains(s, "no QEMU guest agent configured")
}

// wrapGuestAgent wraps a raw Proxmox error with ErrGuestAgentNotReady
// when it matches the transient pattern, otherwise returns the error
// unchanged. Centralises the detection so future Proxmox-version
// variations only need to be added in one place.
func wrapGuestAgent(err error) error {
	if isGuestAgentNotReady(err) {
		return fmt.Errorf("%w: %w", ErrGuestAgentNotReady, err)
	}
	return err
}

// agentFileWrite POSTs to /nodes/{node}/qemu/{vmid}/agent/file-write with
// the content base64-encoded and encode=1.
func (p *pmox) agentFileWrite(ctx context.Context, node string, vmid int, path string, content []byte) error {
	if len(content) > agentFileWriteMaxBytes {
		return fmt.Errorf("agent file-write: content %d bytes exceeds %d-byte limit", len(content), agentFileWriteMaxBytes)
	}
	// Proxmox 9.x's /agent/file-write stores `content` verbatim
	// regardless of whether `encode=1` is set (confirmed via direct API
	// testing — sending `aGVsbG8K` with encode={true,1,"1"} or none all
	// stored the literal base64 string, not the decoded "hello").
	// So we MUST send the raw content, not base64-wrapped, and ASCII
	// payloads like the JIT config (which is itself base64 of JSON) pass
	// cleanly through JSON's string escaping.
	body := map[string]any{
		"file":    path,
		"content": string(content),
	}
	apiPath := fmt.Sprintf("/nodes/%s/qemu/%d/agent/file-write", node, vmid)

	// Proxmox returns {"data": null} on success; an opaque any is fine.
	var resp json.RawMessage
	if err := p.cli.Post(ctx, apiPath, body, &resp); err != nil {
		return fmt.Errorf("agent file-write %s on vmid=%d: %w", path, vmid, err)
	}
	p.log.Debug("agent file-write ok", "vmid", vmid, "node", node, "path", path, "bytes", len(content))
	return nil
}
