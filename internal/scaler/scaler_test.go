package scaler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
		// Trailing garbage after the numeric suffix must be rejected.
		// fmt.Sscanf would accept "10042garbage" → 10042; strconv.Atoi
		// rejects it, which is the correct behavior — a malformed
		// runner name should never map to a real VMID.
		{"gh-runner-proxmox-10042garbage", "gh-runner-proxmox-", 0, false},
		{"gh-runner-proxmox-10042 ", "gh-runner-proxmox-", 0, false},
		{"gh-runner-proxmox--1", "gh-runner-proxmox-", 0, false},
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
