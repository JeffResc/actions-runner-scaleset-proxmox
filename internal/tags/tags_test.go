package tags_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/tags"
)

func TestOwnerTag_Sanitizes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want string
	}{
		{"proxmox-ubuntu-x64", "gh-scaleset-owner-proxmox-ubuntu-x64"},
		{"Ubuntu.X64", "gh-scaleset-owner-ubuntu-x64"},
		{"runner_1", "gh-scaleset-owner-runner-1"},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			t.Parallel()
			got, err := tags.OwnerTag(c.in)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}
}

func TestOwnerTag_RejectsInvalid(t *testing.T) {
	t.Parallel()
	for _, bad := range []string{"", "-startswithhyphen", "name with spaces", "a/b"} {
		_, err := tags.OwnerTag(bad)
		require.Errorf(t, err, "expected error for %q", bad)
	}
}

func TestEncode_DeterministicAndDeduped(t *testing.T) {
	t.Parallel()
	in := []string{"b", "a", "b", "  c  ", "", "a"}
	require.Equal(t, "a;b;c", tags.Encode(in))
}

func TestDecode_HandlesEmptyAndWhitespace(t *testing.T) {
	t.Parallel()
	require.Nil(t, tags.Decode(""))
	require.Equal(t, []string{"a", "b"}, tags.Decode("  a ;; b "))
}

func TestIsOwnedBy(t *testing.T) {
	t.Parallel()
	owner, err := tags.OwnerTag("scaleset-1")
	require.NoError(t, err)
	wire := tags.Encode([]string{tags.Marker, owner, "user-added"})

	require.True(t, tags.IsOwnedBy(wire, "scaleset-1"))
	require.False(t, tags.IsOwnedBy(wire, "scaleset-2"))
	require.False(t, tags.IsOwnedBy("", "scaleset-1"))
	require.False(t, tags.IsOwnedBy("gh-scaleset", "scaleset-1"), "marker alone is not enough")
}

func TestInitial(t *testing.T) {
	t.Parallel()
	got, err := tags.Initial("proxmox-ubuntu")
	require.NoError(t, err)
	require.Equal(t, []string{tags.Marker, "gh-scaleset-owner-proxmox-ubuntu"}, got)
}
