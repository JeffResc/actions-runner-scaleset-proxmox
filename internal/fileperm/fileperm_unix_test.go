//go:build unix

package fileperm_test

import (
	"os"
	"path/filepath"
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

// fakeAccessFileInfo fabricates the uid/gid/mode triples CheckAccess
// reasons about. Chowning a real tempfile to another uid needs root,
// so the interesting cases have to be synthesised.
type fakeAccessFileInfo struct {
	mode os.FileMode
	uid  uint32
	gid  uint32
}

func (f fakeAccessFileInfo) Name() string       { return "x" }
func (f fakeAccessFileInfo) Size() int64        { return 0 }
func (f fakeAccessFileInfo) Mode() os.FileMode  { return f.mode }
func (f fakeAccessFileInfo) ModTime() time.Time { return time.Time{} }
func (f fakeAccessFileInfo) IsDir() bool        { return false }
func (f fakeAccessFileInfo) Sys() any {
	return &syscall.Stat_t{Uid: f.uid, Gid: f.gid}
}

func TestCheckAccess_OwnerReadableAccepted(t *testing.T) {
	t.Parallel()
	p := filepath.Join(t.TempDir(), "ok")
	require.NoError(t, os.WriteFile(p, []byte("x"), 0o600))
	info, err := os.Stat(p)
	require.NoError(t, err)
	require.NoError(t, fileperm.CheckAccess(info, p))
}

func TestCheckAccess_WorldAccessibleRejected(t *testing.T) {
	t.Parallel()
	p := filepath.Join(t.TempDir(), "bad")
	require.NoError(t, os.WriteFile(p, []byte("x"), 0o600))
	require.NoError(t, os.Chmod(p, 0o644))
	info, err := os.Stat(p)
	require.NoError(t, err)
	err = fileperm.CheckAccess(info, p)
	require.Error(t, err)
	require.Contains(t, err.Error(), "accessible by other")
	require.Contains(t, err.Error(), "0644")
}

func TestCheckAccess_OwnedButGroupAccessibleRejected(t *testing.T) {
	t.Parallel()
	// Group access is only ever a fallback for a file we don't own.
	// On one we do own it adds readers for no benefit, so it stays
	// rejected exactly as the old CheckMode(0o600) rule had it.
	p := filepath.Join(t.TempDir(), "group")
	require.NoError(t, os.WriteFile(p, []byte("x"), 0o600))
	require.NoError(t, os.Chmod(p, 0o640))
	info, err := os.Stat(p)
	require.NoError(t, err)
	err = fileperm.CheckAccess(info, p)
	require.Error(t, err)
	require.Contains(t, err.Error(), "insecure mode")
	require.Contains(t, err.Error(), "0640")
}

func TestCheckAccess_ForeignOwnerReachableViaGroupAccepted(t *testing.T) {
	t.Parallel()
	// The Kubernetes shape: a ConfigMap volume is owned by root and
	// carries the pod's fsGroup, so the process reaches it through
	// group membership rather than ownership.
	foreign := uint32(os.Geteuid()) + 1 //nolint:gosec // Geteuid is non-negative on unix
	ours := uint32(os.Getegid())        //nolint:gosec // Getegid is non-negative on unix
	info := fakeAccessFileInfo{mode: 0o440, uid: foreign, gid: ours}
	require.NoError(t, fileperm.CheckAccess(info, "/etc/scaleset/config.yaml"))
}

func TestCheckAccess_ForeignOwnerAndGroupRejected(t *testing.T) {
	t.Parallel()
	foreignUID := uint32(os.Geteuid()) + 1 //nolint:gosec // Geteuid is non-negative on unix
	// 1<<30 is well clear of any group this process could belong to.
	info := fakeAccessFileInfo{mode: 0o640, uid: foreignUID, gid: 1 << 30}
	err := fileperm.CheckAccess(info, "/etc/scaleset/config.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot reach as owner or via group")
}

func TestCheckAccess_ForeignOwnerWithoutGroupBitRejected(t *testing.T) {
	t.Parallel()
	// Owning group matches, but the group read bit is clear, so the
	// process still can't open it.
	foreign := uint32(os.Geteuid()) + 1 //nolint:gosec // Geteuid is non-negative on unix
	ours := uint32(os.Getegid())        //nolint:gosec // Getegid is non-negative on unix
	info := fakeAccessFileInfo{mode: 0o600, uid: foreign, gid: ours}
	err := fileperm.CheckAccess(info, "/etc/scaleset/config.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot reach as owner or via group")
}

func TestCheckAccess_NonPOSIXSysSkips(t *testing.T) {
	t.Parallel()
	require.NoError(t, fileperm.CheckAccess(nonStatFileInfo{}, "/tmp/x"))
}
