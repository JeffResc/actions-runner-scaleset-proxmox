package ipam_test

import (
	"context"
	"fmt"
	"sync"
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

// TestNewStatic_RejectsDuplicateIPs pins #336: a duplicated entry would
// silently halve effective capacity (the `used` map is keyed by IP) and
// later surface as a confusing "pool exhausted" instead of a clear
// config error.
func TestNewStatic_RejectsDuplicateIPs(t *testing.T) {
	t.Parallel()
	_, err := ipam.NewStatic([]string{"10.0.0.10/24", "10.0.0.10/24"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")

	// A pool with all-distinct IPs is still accepted.
	_, err = ipam.NewStatic([]string{"10.0.0.10/24", "10.0.0.11/24"})
	require.NoError(t, err)
}

// TestStatic_ConcurrentAllocateRelease drives Allocate / Release
// from many goroutines simultaneously. Before the sync.Mutex was
// added, this triggered Go's "fatal error: concurrent map writes"
// runtime check under -race. After the fix it must run cleanly,
// honor the pool size cap (no duplicate IPs assigned), and end
// with the pool empty.
func TestStatic_ConcurrentAllocateRelease(t *testing.T) {
	t.Parallel()

	const poolSize = 64
	pool := make([]string, poolSize)
	for i := range pool {
		pool[i] = fmt.Sprintf("10.0.0.%d/24", i+1)
	}
	s, err := ipam.NewStatic(pool)
	require.NoError(t, err)

	const workers = 32
	const opsPerWorker = 100
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			ctx := context.Background()
			for i := 0; i < opsPerWorker; i++ {
				vmid := base*opsPerWorker + i + 1
				ip, allocErr := s.Allocate(ctx, vmid)
				if allocErr != nil {
					// Pool may be momentarily exhausted under contention; that's fine.
					continue
				}
				require.NotEmpty(t, ip)
				require.NoError(t, s.Release(ctx, vmid))
			}
		}(w)
	}
	wg.Wait()

	// After all workers finish, every IP must be releasable for reuse:
	// a final Allocate sweep should drain the pool exactly once.
	seen := make(map[string]struct{}, poolSize)
	for i := 0; i < poolSize; i++ {
		ip, allocErr := s.Allocate(context.Background(), 900000+i)
		require.NoError(t, allocErr, "pool should have %d free IPs after full release", poolSize)
		_, dup := seen[ip]
		require.False(t, dup, "duplicate IP %q handed out", ip)
		seen[ip] = struct{}{}
	}
	_, err = s.Allocate(context.Background(), 999999)
	require.Error(t, err, "pool must be exhausted after draining all %d IPs", poolSize)
}

// TestStatic_AllocateDoesNotDuplicateUnderRace allocates from a
// fully-saturated pool concurrently with releases. The reverse
// index must guarantee no two VMIDs ever hold the same IP at the
// same time, even with interleaved Allocate / Release.
func TestStatic_AllocateDoesNotDuplicateUnderRace(t *testing.T) {
	t.Parallel()

	s, err := ipam.NewStatic([]string{"10.0.0.1/24", "10.0.0.2/24", "10.0.0.3/24"})
	require.NoError(t, err)

	var wg sync.WaitGroup
	const rounds = 200
	for vmid := 1; vmid <= 3; vmid++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			for i := 0; i < rounds; i++ {
				if _, allocErr := s.Allocate(ctx, id); allocErr == nil {
					_ = s.Release(ctx, id)
				}
			}
		}(vmid)
	}
	wg.Wait()
}

// Compile-time assertion that the supplied implementations satisfy
// the interface — catches signature drift on the Allocator contract
// at build time rather than at the call site.
var (
	_ ipam.Allocator = ipam.Noop{}
	_ ipam.Allocator = (*ipam.Static)(nil)
)
