package scaler

import (
	"strings"
	"testing"
)

// FuzzVmidFromRunnerName drives vmidFromRunnerName with arbitrary
// (name, prefix) byte strings. The function consumes values that
// originate from GitHub's listener payload — i.e. untrusted operator-
// adjacent input — so a panic here would crash the leader on an
// unexpected runner name.
//
// Properties:
//
//  1. Must never panic.
//  2. On ok == true, vmid must be positive AND name must begin with
//     prefix. (A strict prefix+strconv.Itoa(vmid)==name round-trip is
//     intentionally NOT asserted — strconv.Atoi accepts leading zeros
//     and that's fine: the scaler only ever emits names via
//     strconv.Itoa, so "01" cannot appear in production, and the
//     pre-existing leniency is not in scope for this PR.)
func FuzzVmidFromRunnerName(f *testing.F) {
	// Seeds mirror TestVMIDFromRunnerName plus adversarial shapes
	// called out in issue #142: NULs, unicode, long input, empty prefix.
	f.Add("gh-runner-proxmox-10042", "gh-runner-proxmox-")
	f.Add("gh-runner-foo-42", "gh-runner-foo-")
	f.Add("gh-runner-proxmox-", "gh-runner-proxmox-")
	f.Add("other-name", "gh-runner-proxmox-")
	f.Add("gh-runner-proxmox-10042garbage", "gh-runner-proxmox-")
	f.Add("gh-runner-proxmox--1", "gh-runner-proxmox-")
	f.Add("gh-runner-proxmox-10042\x00", "gh-runner-proxmox-")
	f.Add("gh-runner-proxmox-١٠٠٤٢", "gh-runner-proxmox-")
	f.Add("10042", "")
	f.Add("", "")

	f.Fuzz(func(t *testing.T, name, prefix string) {
		vmid, ok := vmidFromRunnerName(name, prefix)
		if !ok {
			return
		}
		if vmid <= 0 {
			t.Fatalf("vmidFromRunnerName(%q,%q) returned ok=true with non-positive vmid %d", name, prefix, vmid)
		}
		if !strings.HasPrefix(name, prefix) {
			t.Fatalf("vmidFromRunnerName(%q,%q) returned ok=true but name does not start with prefix", name, prefix)
		}
	})
}
