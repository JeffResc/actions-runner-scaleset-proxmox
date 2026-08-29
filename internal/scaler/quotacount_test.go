package scaler

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/quotas"
)

// recordingQuotaCounter records which bucket method was called so the
// scope-dispatch test can assert quotaCount routes each scope to the
// right lookup.
type recordingQuotaCounter struct {
	repoCalls []string
	orgCalls  []string
	repoRet   int
	orgRet    int
	err       error
}

func (r *recordingQuotaCounter) CountByRepo(repo string) (int, error) {
	r.repoCalls = append(r.repoCalls, repo)
	return r.repoRet, r.err
}

func (r *recordingQuotaCounter) CountByOrg(org string) (int, error) {
	r.orgCalls = append(r.orgCalls, org)
	return r.orgRet, r.err
}

func newQuotaScaler(t *testing.T) *Scaler {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(Config{ScaleSetID: 1, ScaleSetName: "test", NamePrefix: "gh-runner-test-"},
		nil, &fakePool{}, stubProvForScaler{}, log, nil)
}

// TestQuotaCount_ScopeDispatch (#359) is the exhaustiveness table for
// quotaCount's scope switch. Every scope quotas.Resolve can emit must be
// routed to a defined outcome:
//
//   - ScopeRepo  → CountByRepo(name)
//   - ScopeOrg   → CountByOrg(name)
//   - ScopeNone  → (0, nil), no lookup (recordQuota already short-circuits)
//   - unknown    → (0, nil), no lookup  [see NOTE below]
//
// NOTE(#359): documents current behavior; a scope value the switch does
// not recognise (i.e. anything other than repo/org/none, which is not
// reachable from quotas.Resolve today but is not prevented by the type
// system — Scope is a string) silently returns (0, nil) rather than
// failing loudly. Because 0 always passes recordQuota's `count > cap`
// check, an unknown scope would silently disable throttling for that
// job rather than surfacing the misconfiguration. A human should decide
// whether an unknown scope should error instead.
func TestQuotaCount_ScopeDispatch(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		scope       quotas.Scope
		bucket      string
		repoRet     int
		orgRet      int
		wantCount   int
		wantErr     bool
		wantRepoHit bool
		wantOrgHit  bool
	}{
		{
			name:        "repo scope routes to CountByRepo",
			scope:       quotas.ScopeRepo,
			bucket:      "acme/platform",
			repoRet:     7,
			wantCount:   7,
			wantRepoHit: true,
		},
		{
			name:       "org scope routes to CountByOrg",
			scope:      quotas.ScopeOrg,
			bucket:     "acme",
			orgRet:     4,
			wantCount:  4,
			wantOrgHit: true,
		},
		{
			name:      "none scope is a defined no-op (no lookup)",
			scope:     quotas.ScopeNone,
			bucket:    "",
			wantCount: 0,
		},
		{
			// The type system does not prevent an out-of-band scope. It
			// must fail loud, not silently return (0, nil) — a silent zero
			// reads as "under cap" and disables throttling for the scope
			// (#359).
			name:    "unknown scope fails loud with no lookup",
			scope:   quotas.Scope("datacenter"),
			bucket:  "dc1",
			wantErr: true,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			s := newQuotaScaler(t)
			counter := &recordingQuotaCounter{repoRet: c.repoRet, orgRet: c.orgRet}
			s.SetQuotaCounter(counter)

			got, err := s.quotaCount(c.scope, c.bucket)
			if c.wantErr {
				require.Error(t, err, "an unhandled scope must fail loud, not return (0, nil)")
			} else {
				require.NoError(t, err)
				require.Equal(t, c.wantCount, got)
			}

			if c.wantRepoHit {
				require.Equal(t, []string{c.bucket}, counter.repoCalls)
			} else {
				require.Empty(t, counter.repoCalls, "no repo lookup expected")
			}
			if c.wantOrgHit {
				require.Equal(t, []string{c.bucket}, counter.orgCalls)
			} else {
				require.Empty(t, counter.orgCalls, "no org lookup expected")
			}
		})
	}
}

// TestQuotaCount_NilCounter (#359) pins the unwired path: with no
// QuotaCounter attached, every scope returns (0, nil) so a missing
// wiring never manufactures false throttled events.
func TestQuotaCount_NilCounter(t *testing.T) {
	t.Parallel()
	s := newQuotaScaler(t)
	for _, scope := range []quotas.Scope{quotas.ScopeRepo, quotas.ScopeOrg, quotas.ScopeNone} {
		got, err := s.quotaCount(scope, "anything")
		require.NoError(t, err)
		require.Zero(t, got)
	}
}

// TestQuotaCount_PropagatesLookupError (#359) confirms a counter error
// surfaces to the caller (recordQuota logs and skips rather than
// emitting a bogus throttle).
func TestQuotaCount_PropagatesLookupError(t *testing.T) {
	t.Parallel()
	s := newQuotaScaler(t)
	wantErr := errors.New("store unavailable")
	s.SetQuotaCounter(&recordingQuotaCounter{err: wantErr})

	_, err := s.quotaCount(quotas.ScopeRepo, "acme/platform")
	require.ErrorIs(t, err, wantErr)
}
