//go:build e2e

package e2e

import (
	"os"
	"testing"
)

// TestMain sets the orchestrator's secret env vars once for the whole
// e2e package before any test runs, then never mutates process env
// again. The values are constant across every scenario, so there is no
// need to set them per-test via t.Setenv -- and not using t.Setenv is
// what allows each scenario to call t.Parallel() (Go's testing package
// panics if a test that called Setenv also goes parallel). The
// constants live in harness.go.
func TestMain(m *testing.M) {
	_ = os.Setenv("SCALESET_GITHUB_PAT_TOKEN", ghToken)
	_ = os.Setenv("SCALESET_PROXMOX_AUTH_TOKEN_SECRET", proxmoxTokenSecret)
	_ = os.Setenv("SCALESET_ADMIN_API_SHARED_SECRET", adminSecret)
	os.Exit(m.Run())
}
