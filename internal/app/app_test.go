package app

import (
	"errors"
	"sync/atomic"
	"testing"
)

func TestPortFromAddr(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		addr    string
		want    int
		wantErr bool
	}{
		{name: "empty", addr: "", want: 0},
		{name: "ipv4_loopback", addr: "127.0.0.1:9101", want: 9101},
		{name: "wildcard_v4", addr: "0.0.0.0:9101", want: 9101},
		{name: "bare_port", addr: ":9101", want: 9101},
		{name: "ipv6_loopback", addr: "[::1]:9101", want: 9101},
		{name: "ipv6_wildcard", addr: "[::]:9101", want: 9101},
		{name: "ipv6_full", addr: "[fe80::1]:9101", want: 9101},
		{name: "no_port_separator", addr: "127.0.0.1", wantErr: true},
		{name: "non_numeric_port", addr: "127.0.0.1:abc", wantErr: true},
		{name: "port_zero", addr: "127.0.0.1:0", wantErr: true},
		{name: "port_too_large", addr: "127.0.0.1:70000", wantErr: true},
		{name: "ipv6_no_brackets", addr: "::1:9101", wantErr: true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := portFromAddr(tc.addr)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("portFromAddr(%q) = %d, want error", tc.addr, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("portFromAddr(%q) unexpected error: %v", tc.addr, err)
			}
			if got != tc.want {
				t.Fatalf("portFromAddr(%q) = %d, want %d", tc.addr, got, tc.want)
			}
		})
	}
}

// TestMergeLeaderPlaneErr covers the exit-code promotion path: when
// runLeaderPlane fails and cancels the root ctx, coord.Run returns
// nil (clean ctx-cancel), so g1.Wait()'s result is nil even though
// the process should exit non-zero. The helper must surface the
// stashed leader-plane error in that case so supervisors restart.
func TestMergeLeaderPlaneErr(t *testing.T) {
	t.Parallel()

	stash := func(err error) *atomic.Pointer[error] {
		var p atomic.Pointer[error]
		if err != nil {
			p.Store(&err)
		}
		return &p
	}

	leaderErr := errors.New("ensure runner scale set: bad creds")

	t.Run("phase1_nil_and_leader_nil_returns_nil", func(t *testing.T) {
		t.Parallel()
		got := mergeLeaderPlaneErr(nil, stash(nil))
		if got != nil {
			t.Fatalf("want nil, got %v", got)
		}
	})

	t.Run("phase1_nil_and_leader_set_surfaces_leader", func(t *testing.T) {
		t.Parallel()
		got := mergeLeaderPlaneErr(nil, stash(leaderErr))
		if got == nil {
			t.Fatalf("want non-nil to drive non-zero exit, got nil")
		}
		if !errors.Is(got, leaderErr) {
			t.Fatalf("want wrapped leader err, got %v", got)
		}
	})

	t.Run("phase1_set_takes_priority", func(t *testing.T) {
		t.Parallel()
		phase1 := errors.New("coord: transport: dial tcp")
		got := mergeLeaderPlaneErr(phase1, stash(leaderErr))
		if !errors.Is(got, phase1) {
			t.Fatalf("phase1 err must win, got %v", got)
		}
		if errors.Is(got, leaderErr) {
			t.Fatalf("leader err must not be wrapped when phase1 set; got %v", got)
		}
	})

	t.Run("empty_pointer_is_safe", func(t *testing.T) {
		t.Parallel()
		var p atomic.Pointer[error]
		if got := mergeLeaderPlaneErr(nil, &p); got != nil {
			t.Fatalf("unset pointer must yield nil, got %v", got)
		}
	})
}
