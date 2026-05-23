package fileperm_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/fileperm"
)

func TestCheckMode_AcceptsWithinLimit(t *testing.T) {
	t.Parallel()
	p := filepath.Join(t.TempDir(), "ok")
	require.NoError(t, os.WriteFile(p, []byte("x"), 0o600))
	info, err := os.Stat(p)
	require.NoError(t, err)
	require.NoError(t, fileperm.CheckMode(info, p, 0o600))
}

func TestCheckMode_RejectsBeyondLimit(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("Windows mode bits don't map onto POSIX r/w/x")
	}
	p := filepath.Join(t.TempDir(), "bad")
	require.NoError(t, os.WriteFile(p, []byte("x"), 0o600))
	require.NoError(t, os.Chmod(p, 0o644))
	info, err := os.Stat(p)
	require.NoError(t, err)
	err = fileperm.CheckMode(info, p, 0o600)
	require.Error(t, err)
	require.Contains(t, err.Error(), "insecure mode")
	require.Contains(t, err.Error(), "0644")
	require.Contains(t, err.Error(), "0600")
}

func TestCheckOwnership_OwnUIDAccepted(t *testing.T) {
	t.Parallel()
	p := filepath.Join(t.TempDir(), "ok")
	require.NoError(t, os.WriteFile(p, []byte("x"), 0o600))
	info, err := os.Stat(p)
	require.NoError(t, err)
	require.NoError(t, fileperm.CheckOwnership(info, p))
}
