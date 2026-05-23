//go:build unix

package githubauth

import (
	"fmt"
	"os"
	"syscall"
)

// checkPEMOwnership refuses the PEM when its owning UID differs from
// the process's effective UID. mode-bit hardening alone (0600) is not
// sufficient when the orchestrator runs as root or has
// CAP_DAC_READ_SEARCH — a key dropped in by another user can still be
// read.
//
// POSIX ACLs and extended attributes can grant access beyond what
// info.Mode().Perm() shows; this check doesn't see those either, but
// owner-match is the cheap, universal first line.
func checkPEMOwnership(info os.FileInfo, path string) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		// Non-POSIX filesystem (e.g. some FUSE mounts) — skip rather
		// than refuse: the perm check above still bounds the blast
		// radius, and operators on exotic backends shouldn't be locked
		// out by a check that can't run.
		return nil
	}
	if uid := uint32(os.Geteuid()); stat.Uid != uid { //nolint:gosec // Geteuid is non-negative on unix
		return fmt.Errorf("githubauth: private key %s is owned by uid %d but the process runs as uid %d; chown the file to the orchestrator's user",
			path, stat.Uid, uid)
	}
	return nil
}
