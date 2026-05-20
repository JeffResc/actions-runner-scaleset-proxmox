package scaler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/runnertoken"
)

const testHMACSecret = "0123456789abcdef0123456789abcdef"

// TestHookEnv verifies the env-file lines we inject into each VM's JIT
// payload so the in-VM lifecycle hook scripts know where to call back.
// The contract is "callback URL + minter both present → token minted;
// either missing → no entries". We don't try to inject a half-config.
func TestHookEnv(t *testing.T) {
	t.Parallel()
	minter, err := runnertoken.NewMinter([]byte(testHMACSecret), time.Hour, "test-scaleset")
	require.NoError(t, err)

	t.Run("both set mints token", func(t *testing.T) {
		t.Parallel()
		s := &Scaler{cfg: Config{
			HookCallbackURL: "http://192.168.0.20:9103",
			HookTokenMinter: minter,
		}}
		got, err := s.hookEnv(10042, 88)
		require.NoError(t, err)
		require.Equal(t, "http://192.168.0.20:9103", got["SCALESET_HOOK_URL"])
		require.NotEmpty(t, got["SCALESET_HOOK_TOKEN"])

		// Verifier should accept the minted token and the claims should
		// match what we asked for.
		v, err := runnertoken.NewVerifier([]byte(testHMACSecret), "test-scaleset")
		require.NoError(t, err)
		claims, err := v.Verify(got["SCALESET_HOOK_TOKEN"])
		require.NoError(t, err)
		require.Equal(t, 10042, claims.VMID)
		require.Equal(t, int64(88), claims.RunnerID)
	})

	t.Run("missing url disables", func(t *testing.T) {
		t.Parallel()
		s := &Scaler{cfg: Config{HookTokenMinter: minter}}
		got, err := s.hookEnv(1, 1)
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("missing minter disables", func(t *testing.T) {
		t.Parallel()
		s := &Scaler{cfg: Config{HookCallbackURL: "http://x:9103"}}
		got, err := s.hookEnv(1, 1)
		require.NoError(t, err)
		require.Nil(t, got)
	})
}

func TestVMIDFromRunnerName(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		prefix string
		want   int
		ok     bool
	}{
		{"gh-runner-proxmox-10042", "gh-runner-proxmox-", 10042, true},
		{"gh-runner-foo-42", "gh-runner-foo-", 42, true},
		{"gh-runner-proxmox-", "gh-runner-proxmox-", 0, false},
		{"other-name", "gh-runner-proxmox-", 0, false},
		{"gh-runner-proxmox-not-a-number", "gh-runner-proxmox-", 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got, ok := vmidFromRunnerName(c.name, c.prefix)
			require.Equal(t, c.ok, ok)
			if c.ok {
				require.Equal(t, c.want, got)
			}
		})
	}
}
