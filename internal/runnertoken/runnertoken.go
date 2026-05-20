// Package runnertoken mints and verifies per-job HMAC-signed JWTs that
// in-VM runner hook scripts present when calling back into the
// orchestrator's runner_hook endpoint.
//
// Why per-job, not a shared bearer:
//
// A shared bearer token has compromised-once-compromised-forever blast
// radius. Any VM that leaks the token (filesystem snapshot, core dump,
// network mirror) hands an attacker on the LAN the ability to mark every
// VM completed or impersonate every runner's lifecycle events.
//
// Per-job HMAC-signed JWTs collapse the blast radius to "one job":
//
//   - Token is bound to a single VMID + RunnerID via signed claims.
//   - Token is short-lived (configurable TTL; default 6h covers practical
//     job durations with headroom).
//   - Token is delivered via the same qemu-guest-agent channel as the JIT
//     config, so it lives inside the VM but never on disk in the host.
//
// Stateless verification: only the HMAC secret is needed on the server
// side, so no DB lookup per request. The signed claims themselves carry
// the authorisation context.
package runnertoken

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Errors returned by Verify.
var (
	// ErrInvalidToken means the JWT signature is bad, the format is
	// malformed, or the token has expired.
	ErrInvalidToken = errors.New("runnertoken: invalid token")

	// ErrClaimMismatch means the token verified but its claims don't
	// match the request — typically the vmid or runner_id in the claims
	// disagrees with the request payload.
	ErrClaimMismatch = errors.New("runnertoken: claim mismatch")
)

// Claims is the JWT body carried by every per-job runner-hook token.
type Claims struct {
	// VMID is the Proxmox VMID the token was minted for. The runner-hook
	// handler MUST verify this matches the VMID parsed from the request's
	// runner_name before acting on the request.
	VMID int `json:"vmid"`

	// RunnerID is the GitHub-assigned runner ID the token was minted
	// alongside. 0 is reserved for tokens minted before the runner
	// registers (currently unused — minting happens after
	// GenerateJitRunnerConfig returns).
	RunnerID int64 `json:"runner_id"`

	jwt.RegisteredClaims
}

// Minter signs JWTs with a shared HMAC secret and a configured TTL.
type Minter struct {
	secret []byte
	ttl    time.Duration
	issuer string
	// now is injected for tests; defaults to time.Now.
	now func() time.Time
}

// MinHMACSecretBytes is the minimum HMAC secret size we accept. RFC 7518
// §3.2 mandates a key at least as large as the digest output for HMAC
// SHA-256 (256 bits = 32 bytes). Shorter keys reduce the brute-force
// difficulty below the algorithm's design strength, so we refuse them.
const MinHMACSecretBytes = 32

// NewMinter constructs a Minter. issuer is recorded as the JWT `iss`
// claim and aids forensics — typically the scaleset name. issuer must be
// non-empty.
func NewMinter(secret []byte, ttl time.Duration, issuer string) (*Minter, error) {
	if len(secret) < MinHMACSecretBytes {
		return nil, fmt.Errorf("runnertoken: hmac secret must be at least %d bytes (got %d) — RFC 7518 §3.2", MinHMACSecretBytes, len(secret))
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("runnertoken: ttl must be positive (got %s)", ttl)
	}
	if issuer == "" {
		return nil, fmt.Errorf("runnertoken: issuer must be non-empty")
	}
	return &Minter{
		secret: secret,
		ttl:    ttl,
		issuer: issuer,
		now:    time.Now,
	}, nil
}

// Mint signs a fresh token for the given vmid + runnerID. The returned
// string is safe to embed in a systemd env-file (URL-safe base64, no
// quoting issues).
func (m *Minter) Mint(vmid int, runnerID int64) (string, error) {
	now := m.now()
	claims := Claims{
		VMID:     vmid,
		RunnerID: runnerID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    m.issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(m.ttl)),
			ID:        randomJTI(),
		},
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := t.SignedString(m.secret)
	if err != nil {
		return "", fmt.Errorf("runnertoken: sign: %w", err)
	}
	return signed, nil
}

// Verifier validates JWTs minted by a Minter sharing the same HMAC secret.
type Verifier struct {
	secret []byte
	issuer string
	parser *jwt.Parser
	// now is injected for tests; defaults to time.Now (used implicitly by
	// the parser via WithTimeFunc).
	now func() time.Time
}

// NewVerifier constructs a Verifier. issuer must match the value passed
// to the corresponding Minter and must be non-empty — verifying without
// an issuer pin lets any party that knows the HMAC secret impersonate
// us.
func NewVerifier(secret []byte, issuer string) (*Verifier, error) {
	if len(secret) < MinHMACSecretBytes {
		return nil, fmt.Errorf("runnertoken: hmac secret must be at least %d bytes (got %d) — RFC 7518 §3.2", MinHMACSecretBytes, len(secret))
	}
	if issuer == "" {
		return nil, fmt.Errorf("runnertoken: issuer must be non-empty (required for token-issuer binding)")
	}
	v := &Verifier{
		secret: secret,
		issuer: issuer,
		now:    time.Now,
	}
	v.parser = jwt.NewParser(
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithExpirationRequired(),
		jwt.WithIssuer(issuer),
	)
	return v, nil
}

// Verify parses and validates a token, returning the claims on success.
// Callers MUST then cross-check Claims.VMID against the VMID derived
// from the request (typically the runner_name suffix) — Verify confirms
// the token was issued by us, not that it's the right token for this
// request.
func (v *Verifier) Verify(tokenStr string) (*Claims, error) {
	if tokenStr == "" {
		return nil, ErrInvalidToken
	}
	claims := &Claims{}
	_, err := v.parser.ParseWithClaims(tokenStr, claims, func(_ *jwt.Token) (any, error) {
		return v.secret, nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if claims.VMID <= 0 {
		return nil, fmt.Errorf("%w: vmid claim missing or non-positive", ErrInvalidToken)
	}
	return claims, nil
}

// randomJTI returns a random JWT ID for forensics. Collisions are not a
// security property here (we don't track jti server-side), just a way to
// uniquely tag a token in logs.
func randomJTI() string {
	// 8 hex bytes is enough — uniqueness is per-orchestrator-lifetime, not
	// cryptographic. Standard library's nanotime+counter would be fine
	// too, but jwt.NewNumericDate plus a small hex suffix reads cleaner
	// in logs.
	return fmt.Sprintf("%x", time.Now().UnixNano())
}
