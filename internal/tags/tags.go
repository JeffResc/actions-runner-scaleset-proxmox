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

	// profilePrefix is followed by the (sanitized) runner-profile name.
	// Operators can run multiple profiles (per-label hardware shapes)
	// behind a single scale-set; the tag lets crash-recovery route each
	// VM back to the right per-profile pool.
	profilePrefix = "gh-scaleset-profile-"

	// DefaultProfile is the synthetic profile name used when an operator
	// has not declared an explicit `profiles:` block. Keeps the
	// single-profile config shape working unchanged.
	DefaultProfile = "default"

	// templatePrefix is followed by "stable" or "candidate" so
	// crash recovery can attribute boot failures to the right
	// template-version bucket (issue #5). VMs cloned before the
	// canary controller landed have no template tag; consumers
	// treat that as TemplateStable.
	templatePrefix = "gh-scaleset-template-"

	// TemplateStable is the template-class value for clones that
	// used the current production template.
	TemplateStable = "stable"

	// TemplateCandidate is the template-class value for clones
	// that used the staging template during a canary rollout.
	TemplateCandidate = "candidate"
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

// ProfileTag returns the profile tag for the given runner-profile name.
// An empty name maps to DefaultProfile so callers don't have to special-case
// the single-profile config shape.
func ProfileTag(profileName string) string {
	if profileName == "" {
		profileName = DefaultProfile
	}
	return profilePrefix + sanitize(profileName)
}

// Initial returns the canonical initial tag set for a newly-created VM.
// The slice is sorted so it produces a stable on-the-wire representation.
// profileName is the runner profile the VM was cloned for; an empty name
// is treated as DefaultProfile. templateClass is "stable" or "candidate"
// for canary attribution — empty is also treated as "stable" so callers
// that don't run a canary controller skip the cost.
func Initial(scaleSetName, profileName, templateClass string) ([]string, error) {
	owner, err := OwnerTag(scaleSetName)
	if err != nil {
		return nil, err
	}
	if templateClass == "" {
		templateClass = TemplateStable
	}
	out := make([]string, 0, 4)
	out = append(out, Marker, owner, ProfileTag(profileName), TemplateTag(templateClass))
	return out, nil
}

// TemplateTag returns the template-class tag string. Empty class
// defaults to TemplateStable so the no-canary code path doesn't
// special-case the empty value.
func TemplateTag(class string) string {
	if class == "" {
		class = TemplateStable
	}
	return templatePrefix + sanitize(class)
}

// TemplateOf returns the template class encoded in a VM's wire tag
// string, or TemplateStable when no template tag is present
// (covers VMs cloned before canary tagging was introduced — they
// were all on the only template that existed).
func TemplateOf(wireTags string) string {
	for _, t := range Decode(wireTags) {
		class, ok := strings.CutPrefix(t, templatePrefix)
		if !ok {
			continue
		}
		if class == "" {
			return TemplateStable
		}
		return class
	}
	return TemplateStable
}

// ProfileOf returns the profile name encoded in a VM's wire tag string,
// or DefaultProfile when no profile tag is present (covers VMs cloned
// before profile tagging was introduced).
func ProfileOf(wireTags string) string {
	for _, t := range Decode(wireTags) {
		name, ok := strings.CutPrefix(t, profilePrefix)
		if !ok {
			continue
		}
		if name == "" {
			return DefaultProfile
		}
		return name
	}
	return DefaultProfile
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
