package canary_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/canary"
)

func TestPick_NoCandidateAlwaysReturnsStable(t *testing.T) {
	t.Parallel()
	c, err := canary.New([]canary.ProfileConfig{
		{Name: "p", StableTemplateVMID: 9000, CandidateTemplateVMID: 0, Percent: 50},
	})
	require.NoError(t, err)

	for vmid := 10000; vmid < 10050; vmid++ {
		got, err := c.Pick("p", vmid)
		require.NoError(t, err)
		require.Equal(t, 9000, got.TemplateVMID)
		require.Equal(t, canary.Stable, got.Template)
	}
}

func TestPick_ZeroPercentNeverSelectsCandidate(t *testing.T) {
	t.Parallel()
	c, err := canary.New([]canary.ProfileConfig{
		{Name: "p", StableTemplateVMID: 9000, CandidateTemplateVMID: 9001, Percent: 0},
	})
	require.NoError(t, err)

	for vmid := 10000; vmid < 10050; vmid++ {
		got, _ := c.Pick("p", vmid)
		require.Equal(t, canary.Stable, got.Template,
			"percent=0 must never route to candidate (vmid=%d)", vmid)
	}
}

func TestPick_DeterministicForSameVMID(t *testing.T) {
	t.Parallel()
	c, err := canary.New([]canary.ProfileConfig{
		{Name: "p", StableTemplateVMID: 9000, CandidateTemplateVMID: 9001, Percent: 50},
	})
	require.NoError(t, err)

	first, _ := c.Pick("p", 12345)
	for range 20 {
		repeat, _ := c.Pick("p", 12345)
		require.Equal(t, first, repeat,
			"repeated Pick for same vmid must return same result (deterministic)")
	}
}

func TestPick_TenPercentApproximates10Percent(t *testing.T) {
	t.Parallel()
	c, err := canary.New([]canary.ProfileConfig{
		{Name: "p", StableTemplateVMID: 9000, CandidateTemplateVMID: 9001, Percent: 10},
	})
	require.NoError(t, err)

	const n = 1000
	canaryCount := 0
	for vmid := 10000; vmid < 10000+n; vmid++ {
		got, _ := c.Pick("p", vmid)
		if got.Template == canary.Candidate {
			canaryCount++
		}
	}
	// Acceptance criterion from issue #5: ~10% of clones with
	// canary_percent=10. SHA-256 mod 100 is well-distributed —
	// expect canaryCount in [70, 130] for n=1000.
	require.InDelta(t, 100, canaryCount, 30,
		"canary_percent=10 must yield ~10%% canary clones; saw %d/1000", canaryCount)
}

func TestRecordFailure_BelowMinSamplesNeverReverts(t *testing.T) {
	t.Parallel()
	c, err := canary.New([]canary.ProfileConfig{
		{Name: "p", StableTemplateVMID: 9000, CandidateTemplateVMID: 9001, Percent: 50, MaxFailureRate: 0.2},
	})
	require.NoError(t, err)
	c.SetMinFailureSamples(5)

	// Fewer canary clones than MinFailureSamples — even a 100%
	// failure rate doesn't trip the revert (statistical
	// significance gate).
	for range 4 {
		c.RecordClone("p", canary.Candidate)
		reverted := c.RecordFailure("p", canary.Candidate)
		require.False(t, reverted, "must not revert with fewer than MinFailureSamples canary clones")
	}

	s, err := c.Status("p")
	require.NoError(t, err)
	require.Equal(t, 50, s.Percent, "percent stays at 50% until min-samples gate clears")
	require.False(t, s.Reverted)
}

func TestRecordFailure_ExceedsThresholdReverts(t *testing.T) {
	t.Parallel()
	c, err := canary.New([]canary.ProfileConfig{
		{Name: "p", StableTemplateVMID: 9000, CandidateTemplateVMID: 9001, Percent: 25, MaxFailureRate: 0.2},
	})
	require.NoError(t, err)
	c.SetMinFailureSamples(5)

	// 10 canary clones with 3 failures = 30% > 20% threshold.
	for range 10 {
		c.RecordClone("p", canary.Candidate)
	}
	var reverted bool
	for range 3 {
		reverted = c.RecordFailure("p", canary.Candidate) || reverted
	}
	require.True(t, reverted, "failure rate above threshold MUST trigger auto-revert")

	s, err := c.Status("p")
	require.NoError(t, err)
	require.Equal(t, 0, s.Percent, "auto-revert flips percent to 0")
	require.Equal(t, 25, s.OriginalPercent, "OriginalPercent preserves the operator's setting")
	require.True(t, s.Reverted)
}

func TestRecordFailure_StableClonesAreIgnored(t *testing.T) {
	t.Parallel()
	c, err := canary.New([]canary.ProfileConfig{
		{Name: "p", StableTemplateVMID: 9000, CandidateTemplateVMID: 9001, Percent: 25, MaxFailureRate: 0.01},
	})
	require.NoError(t, err)

	// Hammer the controller with stable failures — must never
	// affect canary percent (the failure rate is canary-scoped).
	for range 1000 {
		c.RecordClone("p", canary.Stable)
		reverted := c.RecordFailure("p", canary.Stable)
		require.False(t, reverted)
	}
	s, err := c.Status("p")
	require.NoError(t, err)
	require.Equal(t, 25, s.Percent)
	require.Equal(t, 0, s.CanaryClones)
	require.Equal(t, 0, s.CanaryFailures)
}

func TestPromote_SwapsCandidateIntoStable(t *testing.T) {
	t.Parallel()
	c, err := canary.New([]canary.ProfileConfig{
		{Name: "p", StableTemplateVMID: 9000, CandidateTemplateVMID: 9001, Percent: 25},
	})
	require.NoError(t, err)

	require.NoError(t, c.Promote("p"))
	s, err := c.Status("p")
	require.NoError(t, err)
	require.Equal(t, 9001, s.StableTemplateVMID, "candidate is now stable")
	require.Equal(t, 0, s.CandidateTemplateVMID, "candidate slot is cleared")
	require.Equal(t, 0, s.Percent, "percent resets to 0 (operator can declare a new candidate later)")

	// Subsequent Pick always returns the new stable.
	for vmid := 10000; vmid < 10010; vmid++ {
		got, _ := c.Pick("p", vmid)
		require.Equal(t, 9001, got.TemplateVMID)
		require.Equal(t, canary.Stable, got.Template)
	}
}

func TestPromote_NoCandidateErrors(t *testing.T) {
	t.Parallel()
	c, err := canary.New([]canary.ProfileConfig{
		{Name: "p", StableTemplateVMID: 9000},
	})
	require.NoError(t, err)
	require.ErrorIs(t, c.Promote("p"), canary.ErrNoCandidate)
}

func TestPromote_UnknownProfileErrors(t *testing.T) {
	t.Parallel()
	c, err := canary.New(nil)
	require.NoError(t, err)
	require.ErrorIs(t, c.Promote("missing"), canary.ErrUnknownProfile)
}

func TestPromote_ResetsRevertedState(t *testing.T) {
	t.Parallel()
	c, err := canary.New([]canary.ProfileConfig{
		{Name: "p", StableTemplateVMID: 9000, CandidateTemplateVMID: 9001, Percent: 25, MaxFailureRate: 0.01},
	})
	require.NoError(t, err)
	c.SetMinFailureSamples(5)

	// Force an auto-revert.
	for range 10 {
		c.RecordClone("p", canary.Candidate)
	}
	c.RecordFailure("p", canary.Candidate)

	// Now promote — the post-promote state must NOT carry the
	// reverted flag (the candidate is now the production).
	require.NoError(t, c.Promote("p"))
	s, err := c.Status("p")
	require.NoError(t, err)
	require.False(t, s.Reverted)
	require.Equal(t, 0, s.CanaryFailures)
}

func TestNew_RejectsInvalidConfig(t *testing.T) {
	t.Parallel()
	_, err := canary.New([]canary.ProfileConfig{{Name: ""}})
	require.Error(t, err, "empty name")

	_, err = canary.New([]canary.ProfileConfig{
		{Name: "x"}, {Name: "x"},
	})
	require.Error(t, err, "duplicate name")

	_, err = canary.New([]canary.ProfileConfig{{Name: "x", Percent: -1}})
	require.Error(t, err)

	_, err = canary.New([]canary.ProfileConfig{{Name: "x", Percent: 101}})
	require.Error(t, err)

	_, err = canary.New([]canary.ProfileConfig{{Name: "x", MaxFailureRate: 1.5}})
	require.Error(t, err)
}

// TestRecordFailure_OnlyRevertsOnceAcrossManyExcessFailures
// pins #292: once auto-revert has fired, subsequent RecordFailure
// calls in the same profile MUST NOT return true again. Without
// this, the orchestrator's CanaryReverts metric would tick once
// per post-revert candidate failure — operators would think the
// auto-revert is firing repeatedly when it's just a sticky state.
// The current implementation uses a single `reverted` boolean per
// profile; this test locks in the one-shot semantics.
func TestRecordFailure_OnlyRevertsOnceAcrossManyExcessFailures(t *testing.T) {
	t.Parallel()
	c, err := canary.New([]canary.ProfileConfig{
		{Name: "p", StableTemplateVMID: 9000, CandidateTemplateVMID: 9001, Percent: 25, MaxFailureRate: 0.1},
	})
	require.NoError(t, err)
	c.SetMinFailureSamples(5)

	for range 20 {
		c.RecordClone("p", canary.Candidate)
	}
	// First crossing of the threshold returns true exactly once.
	var firstReverts int
	for range 5 {
		if c.RecordFailure("p", canary.Candidate) {
			firstReverts++
		}
	}
	require.Equal(t, 1, firstReverts,
		"first crossing of the failure-rate threshold must return true exactly once (issue #292)")

	// Subsequent failures must NOT re-fire the revert signal.
	for range 50 {
		require.False(t, c.RecordFailure("p", canary.Candidate),
			"post-revert RecordFailure must never return true again (issue #292)")
	}

	s, err := c.Status("p")
	require.NoError(t, err)
	require.True(t, s.Reverted)
	require.Equal(t, 0, s.Percent, "percent stays at 0 after revert")
}

// TestRecordFailure_RevertIsPerProfile pins #292: an auto-revert
// on profile A must NOT affect profile B's percent or reverted
// flag. A regression that shared canary state across profiles
// would tank all candidate rollouts in lockstep on the first
// failing profile.
func TestRecordFailure_RevertIsPerProfile(t *testing.T) {
	t.Parallel()
	c, err := canary.New([]canary.ProfileConfig{
		{Name: "a", StableTemplateVMID: 9000, CandidateTemplateVMID: 9001, Percent: 25, MaxFailureRate: 0.01},
		{Name: "b", StableTemplateVMID: 8000, CandidateTemplateVMID: 8001, Percent: 50, MaxFailureRate: 0.5},
	})
	require.NoError(t, err)
	c.SetMinFailureSamples(2)

	// Drive profile A past its threshold.
	for range 5 {
		c.RecordClone("a", canary.Candidate)
		c.RecordFailure("a", canary.Candidate)
	}
	sa, err := c.Status("a")
	require.NoError(t, err)
	require.True(t, sa.Reverted)
	require.Equal(t, 0, sa.Percent)

	// Profile B untouched.
	sb, err := c.Status("b")
	require.NoError(t, err)
	require.False(t, sb.Reverted, "profile B's reverted flag must stay false when only A trips (issue #292)")
	require.Equal(t, 50, sb.Percent, "profile B's percent must remain at its operator-set value (issue #292)")
}

// TestPromote_AfterAutoRevertSucceeds pins #292: the operator
// retains the ability to manually Promote a candidate template
// even after auto-revert has fired. Without this, an
// auto-revert would force the operator to re-declare the
// candidate via config and restart — a far slower escape hatch
// than just promoting the candidate they already trust.
func TestPromote_AfterAutoRevertSucceeds(t *testing.T) {
	t.Parallel()
	c, err := canary.New([]canary.ProfileConfig{
		{Name: "p", StableTemplateVMID: 9000, CandidateTemplateVMID: 9001, Percent: 25, MaxFailureRate: 0.01},
	})
	require.NoError(t, err)
	c.SetMinFailureSamples(2)
	for range 5 {
		c.RecordClone("p", canary.Candidate)
		c.RecordFailure("p", canary.Candidate)
	}
	s, err := c.Status("p")
	require.NoError(t, err)
	require.True(t, s.Reverted, "precondition: auto-revert must have fired")

	// Manual override after auto-revert.
	require.NoError(t, c.Promote("p"),
		"operator must be able to Promote even after auto-revert (issue #292)")
	s, err = c.Status("p")
	require.NoError(t, err)
	require.Equal(t, 9001, s.StableTemplateVMID, "promote installs the candidate as the new stable")
	require.False(t, s.Reverted, "reverted flag must clear on Promote")
}

// TestRecordFailure_ConcurrentCallsDoNotDoubleRevert exercises
// the controller under burst contention from many in-flight
// candidate-failure recordings — the canonical race shape from
// #292 when multiple boot-failure goroutines race to record
// failures past the threshold. Locks in two invariants under
// concurrency:
//
//  1. Auto-revert fires at most once across all racing callers.
//  2. The race detector reports no data race on the controller's
//     internal counters / state flags.
//
// Run with `go test -race ./internal/canary/...` to exercise (2).
func TestRecordFailure_ConcurrentCallsDoNotDoubleRevert(t *testing.T) {
	t.Parallel()
	c, err := canary.New([]canary.ProfileConfig{
		{Name: "p", StableTemplateVMID: 9000, CandidateTemplateVMID: 9001, Percent: 50, MaxFailureRate: 0.1},
	})
	require.NoError(t, err)
	c.SetMinFailureSamples(4)

	const N = 200
	// Pre-record clones so the per-call rate calc has denominator.
	for range N {
		c.RecordClone("p", canary.Candidate)
	}

	var (
		wg   sync.WaitGroup
		gate sync.WaitGroup
		hits atomic.Int32
	)
	gate.Add(1)
	for range N {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gate.Wait()
			if c.RecordFailure("p", canary.Candidate) {
				hits.Add(1)
			}
		}()
	}
	gate.Done()
	wg.Wait()

	require.Equal(t, int32(1), hits.Load(),
		"auto-revert must fire exactly once across racing RecordFailure calls (issue #292)")
}
