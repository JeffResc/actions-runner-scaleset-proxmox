//go:build windows

package fileperm

import "os"

// CheckOwnership is a no-op on Windows: the orchestrator targets
// linux/{amd64,arm64} in production, and Windows ACLs don't map onto
// the POSIX UID model used by the unix implementation. CheckMode is
// still applied by callers and bounds the blast radius.
func CheckOwnership(_ os.FileInfo, _ string) error { return nil }
