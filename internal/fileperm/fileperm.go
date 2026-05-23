// Package fileperm enforces ownership and mode-bit invariants on
// secret-bearing files before the orchestrator reads them.
//
// The orchestrator's config file and the GitHub App private key both
// hold credentials that grant control over Proxmox and GitHub from a
// single shell; a file that is world-readable, group-readable, or
// owned by a different UID is a misconfiguration we want to surface
// loudly at startup rather than after the secret has already been
// exfiltrated.
//
// The package exposes two checks:
//
//   - CheckMode refuses a file whose mode bits exceed a caller-supplied
//     maximum (typically 0o600). It is the same check on every OS.
//   - CheckOwnership refuses a file whose owner UID differs from the
//     process's effective UID. The implementation is build-tagged so
//     Windows compiles to a no-op (POSIX UIDs don't apply).
//
// Both functions take an [os.FileInfo] obtained from a prior
// [os.Stat] / [os.Lstat] so a caller can stat once and re-use the
// result for both checks plus the subsequent read.
package fileperm

import (
	"fmt"
	"os"
)

// CheckMode returns an error when info's permission bits exceed
// maxMode. The mask only looks at the low 9 bits (Perm()), so setuid
// and friends are ignored — the secret-file use case only cares about
// who can read.
//
// Example: CheckMode(info, path, 0o600) refuses anything except
// owner-only read/write.
func CheckMode(info os.FileInfo, path string, maxMode os.FileMode) error {
	mode := info.Mode().Perm()
	if mode & ^maxMode != 0 {
		return fmt.Errorf("fileperm: %s has insecure mode %#o; expected at most %#o (chmod %o the file)",
			path, mode, maxMode, maxMode)
	}
	return nil
}
