//go:build windows

package githubauth

import "os"

// checkPEMOwnership is a no-op on Windows: the orchestrator targets
// linux/{amd64,arm64} in production, and Windows ACLs don't map onto
// the POSIX UID model used by the unix implementation. The perm check
// in NewAppFromFile still runs.
func checkPEMOwnership(_ os.FileInfo, _ string) error { return nil }
