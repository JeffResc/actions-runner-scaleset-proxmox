//go:build unix

package fileperm

import (
	"fmt"
	"os"
	"syscall"
)

// CheckOwnership refuses a file whose owning UID differs from the
// process's effective UID. mode-bit hardening alone (0o600) is not
// sufficient when the orchestrator runs as root or has
// CAP_DAC_READ_SEARCH — a file dropped in by another user can still be
// read.
//
// POSIX ACLs and extended attributes can grant access beyond what
// info.Mode().Perm() shows; this check doesn't see those either, but
// owner-match is the cheap, universal first line.
//
// Returns nil when info.Sys() isn't a *syscall.Stat_t (some FUSE
// mounts and non-POSIX filesystems). The matching mode check still
// bounds the blast radius, and operators on exotic backends shouldn't
// be locked out by a check that can't run.
func CheckOwnership(info os.FileInfo, path string) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	if uid := uint32(os.Geteuid()); stat.Uid != uid { //nolint:gosec // Geteuid is non-negative on unix
		return fmt.Errorf("fileperm: %s is owned by uid %d but the process runs as uid %d; chown the file to the orchestrator's user",
			path, stat.Uid, uid)
	}
	return nil
}
