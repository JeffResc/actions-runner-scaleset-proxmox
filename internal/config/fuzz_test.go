package config_test

import (
	"testing"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/config"
)

// FuzzParse drives config.Parse with adversarial YAML bytes to prove it
// never panics on operator-supplied input. Parse is the trust boundary
// for everything below it — a panic here is a crash-on-startup.
//
// The property is intentionally weak: never panic, and never return the
// nonsense pair (nil, nil). Both branches of the legitimate contract
// ((cfg, nil) and (nil, err)) are acceptable for any input; we are not
// asserting that adversarial YAML must error, only that it must never
// crash the process or violate the return-value invariant. Stronger
// shape assertions belong in the table-driven tests in config_test.go.
//
// Env-var substitution is not deterministically pinned here — koanf's
// env provider reads os.Environ() and any SCALESET_* var leaking in
// from the runner becomes part of the input. Reproductions of a
// failing seed should be run with `env -i go test -run=...`. Fuzzing
// the env layer itself is a deliberate follow-up.
func FuzzParse(f *testing.F) {
	f.Add([]byte(validPATYAML))
	f.Add([]byte(""))
	f.Add([]byte("github: {auth_mode: pat}"))
	// Adversarial shapes from the issue: deep nesting, unicode keys
	// (U+202E right-to-left override), numeric-field type confusion,
	// malformed ${VAR} refs, embedded NULs.
	f.Add([]byte("a: &a [*a]\n"))
	f.Add([]byte("github:\n  auth_mode: \"\u202epat\"\n"))
	f.Add([]byte("proxmox:\n  template_vmid: \"not-a-number\"\n"))
	f.Add([]byte("github:\n  pat:\n    token: ${UNCLOSED\n"))
	f.Add([]byte("github:\n  pat:\n    token: \"a\x00b\"\n"))

	f.Fuzz(func(t *testing.T, data []byte) {
		cfg, err := config.Parse(data)
		if cfg == nil && err == nil {
			t.Fatalf("Parse returned (nil, nil) for %q — must return either a config or an error", data)
		}
	})
}
