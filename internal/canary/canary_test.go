package canary_test

import (
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
