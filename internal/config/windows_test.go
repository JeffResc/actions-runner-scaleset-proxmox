package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfileGuestOSValidation(t *testing.T) {
	cases := []struct {
		name     string
		profiles []ProfileConfig
		wantErr  bool
	}{
		{"linux default", []ProfileConfig{{Name: "default"}}, false},
		{"windows", []ProfileConfig{{Name: "default", GuestOS: "windows", WindowsJITPath: `C:\ProgramData\ActionsRunnerScaleSet\jitconfig`}}, false},
		{"mixed", []ProfileConfig{{Name: "a"}, {Name: "b", GuestOS: "windows"}}, true},
		{"unknown", []ProfileConfig{{Name: "default", GuestOS: "darwin"}}, true},
		{"relative path", []ProfileConfig{{Name: "default", GuestOS: "windows", WindowsJITPath: `foo\jit`}}, true},
		{"linux path", []ProfileConfig{{Name: "default", WindowsJITPath: `C:\foo\jit`}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Scalesets: []ScaleSetEntry{{Name: "test", MaxConcurrentRunners: 1, Profiles: tc.profiles}}}
			err := cfg.validateProfiles()
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Exercise the shipped example through the public loader, including defaults,
// shared-scope scale sets, resource admission and disjoint VMID validation.
func TestWindowsLinuxExample(t *testing.T) {
	t.Setenv("SCALESET_GITHUB_PAT_TOKEN", "example-test-token")
	t.Setenv("SCALESET_PROXMOX_AUTH_TOKEN_SECRET", "example-test-secret")
	data, err := os.ReadFile("../../examples/windows-linux.yaml")
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	cfg, err := Load(path)
	require.NoError(t, err)
	require.Len(t, cfg.Scalesets, 2)
	require.Equal(t, "windows", cfg.Scalesets[1].Profiles[0].GuestOS)
	require.True(t, cfg.Pool.Capacity.Enabled)
}
