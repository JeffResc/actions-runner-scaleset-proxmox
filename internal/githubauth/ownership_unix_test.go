//go:build unix

package githubauth

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeStatFileInfo lets the negative-ownership test fabricate a
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

func TestCheckPEMOwnership_ForeignUIDRejected(t *testing.T) {
	t.Parallel()
	// Pick a UID guaranteed not to be ours. uint32(os.Geteuid()) + 1
	// is fine — even on a root run (euid=0) this becomes 1, which we
	// won't match.
	foreign := uint32(os.Geteuid()) + 1 //nolint:gosec // Geteuid is non-negative on unix
	err := checkPEMOwnership(fakeStatFileInfo{name: "app.pem", uid: foreign}, "/tmp/app.pem")
	require.Error(t, err)
	require.Contains(t, err.Error(), "is owned by uid")
}

func TestCheckPEMOwnership_OwnUIDAccepted(t *testing.T) {
	t.Parallel()
	mine := uint32(os.Geteuid()) //nolint:gosec // Geteuid is non-negative on unix
	require.NoError(t, checkPEMOwnership(fakeStatFileInfo{name: "app.pem", uid: mine}, "/tmp/app.pem"))
}

func TestCheckPEMOwnership_NonPOSIXSysSkips(t *testing.T) {
	t.Parallel()
	// A real os.Stat on a tempfile is always *syscall.Stat_t under
	// unix; this case covers FUSE / non-POSIX backends where Sys()
	// returns something else. Use a stub FileInfo whose Sys() is a
	// non-*Stat_t value.
	require.NoError(t, checkPEMOwnership(nonStatFileInfo{}, "/tmp/app.pem"))
}

type nonStatFileInfo struct{}

func (nonStatFileInfo) Name() string       { return "x" }
func (nonStatFileInfo) Size() int64        { return 0 }
func (nonStatFileInfo) Mode() os.FileMode  { return 0o600 }
func (nonStatFileInfo) ModTime() time.Time { return time.Time{} }
func (nonStatFileInfo) IsDir() bool        { return false }
func (nonStatFileInfo) Sys() any           { return struct{}{} }

// TestNewAppFromFile_RealStatHappyPath confirms the unit-level fakes
// match the real stat path: a PEM that we create (owned by us, mode
// 0600) should be accepted.
func TestNewAppFromFile_RealStatHappyPath(t *testing.T) {
	t.Parallel()
	p := filepath.Join(t.TempDir(), "app.pem")
	require.NoError(t, os.WriteFile(p, []byte(testPEM), 0o600))

	info, err := os.Stat(p)
	require.NoError(t, err)
	require.NoError(t, checkPEMOwnership(info, p))
}

// testPEM duplicates fakePEM from the black-box test file because the
// build-tagged white-box file is in a different package scope.
const testPEM = `-----BEGIN RSA PRIVATE KEY-----
MIIBOQIBAAJBAKj34GkxFhD90vcNLYLInFEX6Ppy1tPf9Cnzj4p4WGeKLs1Pt8Qu
KUpRKfFLfRYC9AIKjbJTWit+CqvjWYzvQwECAwEAAQJAIJLixBy2qpFoS4DSmoEm
o3qGy0t6z09AIJtH+5OeRV1be+N4cDYJKffGzDa88vQENZiRm0GRq6a+HPGQMd2k
TQIhAKMSvzIBnni7ot/OSie2TmJLY4SwTQAevXysE2RbFDYdAiEBCUEaRQnMnbp7
9mxDXDf6AU0cN/RPBjb9qSHDcWZHGzUCIG2Es59z8ugGrDY+pxLQnwfotadxd+Uy
v/Ow5T0q5gIJAiEAyS4RaI9YG8EWx/2w0T67ZUVAw8eOMB6BIUg0Xcu+3okCIBOs
/5OiPgoTdSy7bcF9IGpSE8ZgGKzgYQVZeN97YE00
-----END RSA PRIVATE KEY-----`
