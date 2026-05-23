package cluster

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestValidateDataDir_CreatesDirOnFirstRun covers the operator-friendly
// path: first-time orchestrator boot against a fresh dataDir. The
// directory doesn't exist yet; validateDataDir creates it with 0700
// explicitly so a pre-existing world-writable parent can't host a
// loosely-perm'd raft store via MkdirAll's silent-no-op semantics.
func TestValidateDataDir_CreatesDirOnFirstRun(t *testing.T) {
	t.Parallel()
	base := tightTempDir(t)
	dataDir := filepath.Join(base, "raft")

	require.NoError(t, validateDataDir(dataDir))

	info, err := os.Stat(dataDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
}

// TestValidateDataDir_AcceptsExisting0700 covers the steady-state path.
func TestValidateDataDir_AcceptsExisting0700(t *testing.T) {
	t.Parallel()
	require.NoError(t, validateDataDir(tightTempDir(t)))
}

// TestValidateDataDir_RejectsLoosePerms is the security case from #140.
// Detail-level mask coverage lives in fileperm; here we only confirm
// the dataDir code path routes through that check.
func TestValidateDataDir_RejectsLoosePerms(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir() // intentionally NOT tightened
	require.NoError(t, os.Chmod(dataDir, 0o755))

	err := validateDataDir(dataDir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "insecure mode")
}

// TestValidateDataDir_RejectsNonDirectory locks in the "path is a
// regular file" failure mode so we refuse loudly instead of crashing
// inside the bolt-store open.
func TestValidateDataDir_RejectsNonDirectory(t *testing.T) {
	t.Parallel()
	base := tightTempDir(t)
	filePath := filepath.Join(base, "raft")
	require.NoError(t, os.WriteFile(filePath, []byte("not a dir"), 0o600))

	err := validateDataDir(filePath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a directory")
}

// TestValidateDataDir_PropagatesStatErrorsOtherThanNotExist guards
// against silently swallowing fs errors that aren't ErrNotExist (e.g.
// a regular-file parent producing ENOTDIR). Without explicit handling,
// these would collapse into the create-on-first-run branch.
func TestValidateDataDir_PropagatesStatErrorsOtherThanNotExist(t *testing.T) {
	t.Parallel()
	base := tightTempDir(t)
	notADir := filepath.Join(base, "regular-file")
	require.NoError(t, os.WriteFile(notADir, []byte("blocker"), 0o600))

	err := validateDataDir(filepath.Join(notADir, "raft"))
	require.Error(t, err)
	require.False(t, errors.Is(err, os.ErrNotExist),
		"non-ENOENT stat errors must propagate, not collapse into the create-on-first-run branch")
}
