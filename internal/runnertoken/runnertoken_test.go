package runnertoken

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

func mustMinter(t *testing.T, ttl time.Duration) *Minter {
	t.Helper()
	m, err := NewMinter([]byte("0123456789abcdef0123456789abcdef"), ttl, "test-scaleset")
	require.NoError(t, err)
	return m
}

func mustVerifier(t *testing.T) *Verifier {
	t.Helper()
	v, err := NewVerifier([]byte("0123456789abcdef0123456789abcdef"), "test-scaleset")
	require.NoError(t, err)
	return v
}

func TestMintVerify_RoundTrip(t *testing.T) {
	t.Parallel()
	m := mustMinter(t, time.Hour)
	v := mustVerifier(t)

	tok, err := m.Mint(10042, 999)
	require.NoError(t, err)
	require.NotEmpty(t, tok)

	claims, err := v.Verify(tok)
	require.NoError(t, err)
	require.Equal(t, 10042, claims.VMID)
	require.Equal(t, int64(999), claims.RunnerID)
	require.Equal(t, "test-scaleset", claims.Issuer)
}

func TestVerify_RejectsExpired(t *testing.T) {
	t.Parallel()
	m := mustMinter(t, time.Hour)
	// Force "now" to one hour ago so the minted token's exp is already past.
	m.now = func() time.Time { return time.Now().Add(-2 * time.Hour) }
	v := mustVerifier(t)

	tok, err := m.Mint(10042, 1)
	require.NoError(t, err)

	_, err = v.Verify(tok)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestVerify_RejectsWrongSecret(t *testing.T) {
	t.Parallel()
	m := mustMinter(t, time.Hour)
	tok, err := m.Mint(10042, 1)
	require.NoError(t, err)

	v, err := NewVerifier([]byte("WRONGsecret_with_enough_length_4_HS256"), "test-scaleset")
	require.NoError(t, err)

	_, err = v.Verify(tok)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestVerify_RejectsWrongIssuer(t *testing.T) {
	t.Parallel()
	m := mustMinter(t, time.Hour)
	tok, err := m.Mint(10042, 1)
	require.NoError(t, err)

	v, err := NewVerifier([]byte("0123456789abcdef0123456789abcdef"), "different-issuer")
	require.NoError(t, err)

	_, err = v.Verify(tok)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestVerify_RejectsAlgNone(t *testing.T) {
	t.Parallel()
	// Forge an unsigned token (alg=none) — the parser must reject it
	// even though the body claims look fine.
	claims := jwt.MapClaims{
		"vmid":      10042,
		"runner_id": 1,
		"iss":       "test-scaleset",
		"exp":       time.Now().Add(time.Hour).Unix(),
		"iat":       time.Now().Unix(),
	}
	t1 := jwt.NewWithClaims(jwt.SigningMethodNone, claims)
	bad, err := t1.SignedString(jwt.UnsafeAllowNoneSignatureType)
	require.NoError(t, err)

	v := mustVerifier(t)
	_, err = v.Verify(bad)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestVerify_RejectsMalformed(t *testing.T) {
	t.Parallel()
	v := mustVerifier(t)

	for _, in := range []string{"", "not.a.jwt", "header.body", strings.Repeat("a", 100)} {
		_, err := v.Verify(in)
		require.Error(t, err, "input %q should fail", in)
		require.ErrorIs(t, err, ErrInvalidToken)
	}
}

func TestVerify_RejectsZeroVMID(t *testing.T) {
	t.Parallel()
	m := mustMinter(t, time.Hour)
	v := mustVerifier(t)

	tok, err := m.Mint(0, 1)
	require.NoError(t, err)

	_, err = v.Verify(tok)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestNewMinter_RejectsShortSecret(t *testing.T) {
	t.Parallel()
	// 31 bytes — one below the RFC 7518 §3.2 floor.
	_, err := NewMinter([]byte("0123456789abcdef0123456789abcde"), time.Hour, "x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "32 bytes")
}

func TestNewMinter_RejectsNonPositiveTTL(t *testing.T) {
	t.Parallel()
	_, err := NewMinter([]byte("0123456789abcdef0123456789abcdef"), 0, "x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ttl")
}

func TestNewMinter_RejectsEmptyIssuer(t *testing.T) {
	t.Parallel()
	_, err := NewMinter([]byte("0123456789abcdef0123456789abcdef"), time.Hour, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "issuer")
}

func TestNewVerifier_RejectsShortSecret(t *testing.T) {
	t.Parallel()
	_, err := NewVerifier([]byte("0123456789abcdef0123456789abcde"), "x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "32 bytes")
}

func TestNewVerifier_RejectsEmptyIssuer(t *testing.T) {
	t.Parallel()
	_, err := NewVerifier([]byte("0123456789abcdef0123456789abcdef"), "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "issuer")
}

func TestSentinel_ErrInvalidToken(t *testing.T) {
	t.Parallel()
	v := mustVerifier(t)
	_, err := v.Verify("garbage")
	require.True(t, errors.Is(err, ErrInvalidToken))
}
