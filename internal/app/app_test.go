package app

import "testing"

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
