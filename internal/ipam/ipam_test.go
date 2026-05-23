package ipam_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/ipam"
)

func TestNoop_AllocateReturnsEmpty(t *testing.T) {
	t.Parallel()
	ip, err := ipam.Noop{}.Allocate(t.Context(), 12345)
	require.NoError(t, err)
	require.Empty(t, ip, "noop allocator must return empty IP (provisioner falls back to DHCP)")
}

func TestNoop_ReleaseIsNoop(t *testing.T) {
	t.Parallel()
	require.NoError(t, ipam.Noop{}.Release(t.Context(), 12345))
	require.NoError(t, ipam.Noop{}.Release(t.Context(), 12345), "double-release must be a no-op")
}

func TestStatic_AllocateHandsOutFromPool(t *testing.T) {
	t.Parallel()
	s, err := ipam.NewStatic([]string{"10.0.0.10/24", "10.0.0.11/24"})
	require.NoError(t, err)

	got1, err := s.Allocate(t.Context(), 10001)
	require.NoError(t, err)
	require.Equal(t, "10.0.0.10/24", got1)

	got2, err := s.Allocate(t.Context(), 10002)
	require.NoError(t, err)
	require.Equal(t, "10.0.0.11/24", got2)
}

func TestStatic_AllocateIdempotentForSameVMID(t *testing.T) {
	t.Parallel()
	s, err := ipam.NewStatic([]string{"10.0.0.10/24", "10.0.0.11/24"})
	require.NoError(t, err)

	first, err := s.Allocate(t.Context(), 10001)
	require.NoError(t, err)
	second, err := s.Allocate(t.Context(), 10001)
	require.NoError(t, err, "repeat allocate for same vmid must succeed (idempotent retry)")
	require.Equal(t, first, second, "same vmid must always get the same IP")
}

func TestStatic_AllocateExhaustionErrors(t *testing.T) {
	t.Parallel()
	s, err := ipam.NewStatic([]string{"10.0.0.10/24"})
	require.NoError(t, err)

	_, err = s.Allocate(t.Context(), 10001)
	require.NoError(t, err)

	_, err = s.Allocate(t.Context(), 10002)
	require.Error(t, err, "second allocate must fail when pool is exhausted")
	require.Contains(t, err.Error(), "exhausted")
}

func TestStatic_ReleaseFreesForReuse(t *testing.T) {
	t.Parallel()
	s, err := ipam.NewStatic([]string{"10.0.0.10/24"})
	require.NoError(t, err)

	first, err := s.Allocate(t.Context(), 10001)
	require.NoError(t, err)

	require.NoError(t, s.Release(t.Context(), 10001))

	reused, err := s.Allocate(t.Context(), 10002)
	require.NoError(t, err, "after release, the freed IP must be allocatable to a new vmid")
	require.Equal(t, first, reused)
}

func TestStatic_ReleaseUnknownVMIDIsNoop(t *testing.T) {
	t.Parallel()
	s, err := ipam.NewStatic([]string{"10.0.0.10/24"})
	require.NoError(t, err)
	require.NoError(t, s.Release(t.Context(), 99999),
		"releasing a vmid that never allocated must succeed silently")
}

func TestNewStatic_RejectsEmptyPool(t *testing.T) {
	t.Parallel()
	_, err := ipam.NewStatic(nil)
	require.Error(t, err)

	_, err = ipam.NewStatic([]string{})
	require.Error(t, err)
}

// Compile-time assertion that the supplied implementations satisfy
// the interface — catches signature drift on the Allocator contract
// at build time rather than at the call site.
var (
	_ ipam.Allocator = ipam.Noop{}
	_ ipam.Allocator = (*ipam.Static)(nil)
)

// Suppress unused-context import when the package's exported
// helpers don't take a ctx in the tests above (Noop's interface
// methods do, so context IS used).
var _ = context.Background
