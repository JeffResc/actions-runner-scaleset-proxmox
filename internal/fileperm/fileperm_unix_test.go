//go:build unix

package fileperm_test

import (
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/fileperm"
)

// fakeStatFileInfo lets the foreign-UID negative test fabricate a
// FileInfo whose Sys() reports a UID we choose, without needing root
// privileges to actually chown a tempfile.
type fakeStatFileInfo struct {
	name string
	uid  uint32
}

func (f fakeStatFileInfo) Name() string       { return f.name }
func (f fakeStatFileInfo) Size() int64        { return 0 }
func (f fakeStatFileInfo) Mode() os.FileMode  { return 0o600 }
func (f fakeStatFileInfo) ModTime() time.Time { return time.Time{} }
func (f fakeStatFileInfo) IsDir() bool        { return false }
func (f fakeStatFileInfo) Sys() any           { return &syscall.Stat_t{Uid: f.uid} }

func TestCheckOwnership_ForeignUIDRejected(t *testing.T) {
	t.Parallel()
	// Pick a UID guaranteed not to be ours. uint32(os.Geteuid()) + 1
	// is fine — even on a root run (euid=0) this becomes 1, which we
	// won't match.
	foreign := uint32(os.Geteuid()) + 1 //nolint:gosec // Geteuid is non-negative on unix
	err := fileperm.CheckOwnership(fakeStatFileInfo{name: "x", uid: foreign}, "/tmp/x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "is owned by uid")
}

type nonStatFileInfo struct{}

func (nonStatFileInfo) Name() string       { return "x" }
func (nonStatFileInfo) Size() int64        { return 0 }
func (nonStatFileInfo) Mode() os.FileMode  { return 0o600 }
func (nonStatFileInfo) ModTime() time.Time { return time.Time{} }
func (nonStatFileInfo) IsDir() bool        { return false }
func (nonStatFileInfo) Sys() any           { return struct{}{} }

func TestCheckOwnership_NonPOSIXSysSkips(t *testing.T) {
	t.Parallel()
	// A real os.Stat on a tempfile is always *syscall.Stat_t under
	// unix; this case covers FUSE / non-POSIX backends where Sys()
	// returns something else.
	require.NoError(t, fileperm.CheckOwnership(nonStatFileInfo{}, "/tmp/x"))
}
