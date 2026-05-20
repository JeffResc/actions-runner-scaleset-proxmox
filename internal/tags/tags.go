// Package tags defines the Proxmox tag schema used to identify VMs that
// belong to this scaleset and the helpers to encode/decode them.
//
// Proxmox 8.x tags must match [a-z0-9_+.-]+ and are joined with ';' on the
// wire. To keep the schema robust against future tag-character changes we
// limit ourselves to lowercase letters, digits, and hyphens.
package tags

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
)

const (
	// Marker is a static discriminator that identifies any VM owned by an
	// instance of this orchestrator (regardless of scale set name).
	Marker = "gh-scaleset"

	// ownerPrefix is followed by the (sanitized) scale set name.
	ownerPrefix = "gh-scaleset-owner-"
)

// scaleSetNameRE is what we accept for an unsanitized scale set name.
var scaleSetNameRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$`)

// ErrNotOwned indicates the VM's tags do not match this orchestrator's
// owner tag.
var ErrNotOwned = errors.New("tags: VM is not owned by this scale set")

// OwnerTag returns the owner tag for the given scale set name.
func OwnerTag(scaleSetName string) (string, error) {
	if !scaleSetNameRE.MatchString(scaleSetName) {
		return "", fmt.Errorf("tags: scale set name %q must match %s", scaleSetName, scaleSetNameRE.String())
	}
	return ownerPrefix + sanitize(scaleSetName), nil
}

// MustOwnerTag is OwnerTag for static initialization paths where the name
// is known-good. Panics on invalid input.
func MustOwnerTag(scaleSetName string) string {
	t, err := OwnerTag(scaleSetName)
	if err != nil {
		panic(err)
	}
	return t
}

// Initial returns the canonical initial tag set for a newly-created VM.
// The slice is sorted so it produces a stable on-the-wire representation.
func Initial(scaleSetName string) ([]string, error) {
	owner, err := OwnerTag(scaleSetName)
	if err != nil {
		return nil, err
	}
	return []string{Marker, owner}, nil
}

// Encode joins a slice of tags into the semicolon-separated wire format
// Proxmox expects. The slice is sorted and deduplicated for stability.
func Encode(tags []string) string {
	uniq := make(map[string]struct{}, len(tags))
	for _, t := range tags {
		if t = strings.TrimSpace(t); t != "" {
			uniq[t] = struct{}{}
		}
	}
	out := make([]string, 0, len(uniq))
	for t := range uniq {
		out = append(out, t)
	}
	sort.Strings(out)
	return strings.Join(out, ";")
}

// Decode splits Proxmox's semicolon-separated tag wire format into a slice.
func Decode(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ";")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// IsOwnedBy returns true iff the VM's tag string contains both the static
// marker and the scale-set-specific owner tag.
func IsOwnedBy(wireTags, scaleSetName string) bool {
	owner, err := OwnerTag(scaleSetName)
	if err != nil {
		return false
	}
	decoded := Decode(wireTags)
	return slices.Contains(decoded, Marker) && slices.Contains(decoded, owner)
}

// sanitize lowercases and replaces disallowed characters with '-'.
func sanitize(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return strings.Trim(b.String(), "-")
}
