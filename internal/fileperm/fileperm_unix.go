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

// CheckAccess accepts a secret-bearing file that grants nothing to
// "other" and that this process can reach either as the owner or
// through a group it belongs to. It replaces the CheckMode(0o600) +
// CheckOwnership pair at the config-file and private-key call sites.
//
// The group case is what makes Kubernetes ConfigMap and Secret mounts
// usable. The kubelet writes projected files owned by root and applies
// the pod's fsGroup as the group owner; no Kubernetes API can set the
// owner UID, so a pod running as a non-root user could never satisfy
// an owner-match rule and the orchestrator crash-looped on its own
// Helm chart.
//
// Group access is admitted ONLY for a file this process does not own,
// where it is the sole way in. A file we do own still has to be
// 0o600 — widening it would buy nothing and only add readers. So the
// relaxation is exactly the Kubernetes shape (root:fsGroup 0o440) and
// nothing else.
//
// Directories the orchestrator creates itself (the raft data_dir) keep
// using CheckMode + CheckOwnership: nothing external writes those, so
// the stricter rule costs nothing there.
//
// Returns nil when info.Sys() isn't a *syscall.Stat_t (some FUSE
// mounts and non-POSIX filesystems), matching CheckOwnership. The
// world-access check still applies in that case.
func CheckAccess(info os.FileInfo, path string) error {
	mode := info.Mode().Perm()
	if mode&0o007 != 0 {
		return fmt.Errorf("fileperm: %s has insecure mode %#o; it is accessible by other (chmod 0600)",
			path, mode)
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		// Ownership is unknowable here, so hold the strict bar: the
		// file must be owner-only, exactly as CheckMode(0o600) required.
		if mode&0o070 != 0 {
			return fmt.Errorf("fileperm: %s has insecure mode %#o; expected at most 0600 (chmod 600 the file)",
				path, mode)
		}
		return nil
	}

	euid := uint32(os.Geteuid()) //nolint:gosec // Geteuid is non-negative on unix
	if stat.Uid == euid {
		// We own it, so we can always read it as the owner. Group
		// access buys nothing and only widens exposure — keep it out.
		if mode&0o070 != 0 {
			return fmt.Errorf("fileperm: %s has insecure mode %#o; it is owned by this process, so it must be owner-only (chmod 600 the file)",
				path, mode)
		}
		return nil
	}

	// Owned by somebody else, so the group bits are the only way in —
	// and only when this process belongs to the owning group. This is
	// the branch Kubernetes projected volumes land in.
	if mode&0o040 != 0 && inGroup(stat.Gid) {
		return nil
	}

	return fmt.Errorf("fileperm: %s is owned by uid %d gid %d with mode %#o, which the process (uid %d) cannot reach as owner or via group; chown it to the orchestrator's user, or make it group-readable by a group the process belongs to",
		path, stat.Uid, stat.Gid, mode, euid)
}

// inGroup reports whether gid is the process's effective group or one
// of its supplementary groups. A Getgroups failure is treated as "not
// a member": CheckAccess then falls through to its error, which is the
// safe direction.
func inGroup(gid uint32) bool {
	if egid := uint32(os.Getegid()); egid == gid { //nolint:gosec // Getegid is non-negative on unix
		return true
	}
	groups, err := os.Getgroups()
	if err != nil {
		return false
	}
	for _, g := range groups {
		if g >= 0 && uint32(g) == gid { //nolint:gosec // guarded non-negative above
			return true
		}
	}
	return false
}
