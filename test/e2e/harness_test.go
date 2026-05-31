//go:build e2e

package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPartitionVMIDRange covers the auto-partitioning math
// extracted from Start. Drives the rounding-remainder edge
// case (last span absorbs the leftovers) and the small-N
// cases the existing e2e scenarios exercise only transitively.
// (issue #280)
func TestPartitionVMIDRange(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		n, min, max int
		want        []vmidSpan
	}{
		"one_scaleset_takes_whole_range": {
			n: 1, min: 10000, max: 10999,
			want: []vmidSpan{{10000, 10999}},
		},
		"two_scalesets_split_evenly": {
			n: 2, min: 10000, max: 10999,
			want: []vmidSpan{{10000, 10499}, {10500, 10999}},
		},
		"three_scalesets_remainder_into_last": {
			n: 3, min: 10000, max: 10999,
			// span = 1000/3 = 333; last span absorbs the leftover.
			want: []vmidSpan{{10000, 10332}, {10333, 10665}, {10666, 10999}},
		},
		"zero_scalesets_nil": {
			n: 0, min: 10000, max: 10999,
			want: nil,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := partitionVMIDRange(tc.n, tc.min, tc.max)
			require.Equal(t, tc.want, got)
			// Union invariant: when n>0, the spans cover exactly [min, max].
			if tc.n > 0 {
				require.Equal(t, tc.min, got[0].min, "first span must start at min")
				require.Equal(t, tc.max, got[len(got)-1].max, "last span must end at max")
				// Spans must be contiguous and disjoint.
				for i := 1; i < len(got); i++ {
					require.Equal(t, got[i-1].max+1, got[i].min,
						"spans must be contiguous (no gap, no overlap) between %d and %d", i-1, i)
				}
			}
		})
	}
}

// TestApplyOptionDefaults_SingularFillsDefaults pins the
// singular-shape defaulting block extracted from Start.
// (issue #280)
func TestApplyOptionDefaults_SingularFillsDefaults(t *testing.T) {
	t.Parallel()
	got := applyOptionDefaults(Options{})
	require.Equal(t, 2, got.HotSize)
	require.Equal(t, 8, got.MaxConcurrentRunners)
	require.Equal(t, "octocat", got.Org)
	require.Equal(t, "test-scaleset", got.ScaleSetName)
}

// TestApplyOptionDefaults_MultiAutoFillsPerScalesetIdentity
// pins the per-scaleset name / org / max-concurrent defaulting
// for multi-scaleset configs. (issue #280)
func TestApplyOptionDefaults_MultiAutoFillsPerScalesetIdentity(t *testing.T) {
	t.Parallel()
	got := applyOptionDefaults(Options{
		Scalesets: []ScalesetSpec{
			{},                               // entirely default
			{Name: "explicit"},               // name only
			{Name: "z", Org: "explicit-org"}, // both set
		},
	})
	require.Len(t, got.Scalesets, 3)
	require.Equal(t, "scaleset-0", got.Scalesets[0].Name)
	require.Equal(t, "org-0", got.Scalesets[0].Org)
	require.Equal(t, 8, got.Scalesets[0].MaxConcurrentRunners)
	require.Equal(t, "explicit", got.Scalesets[1].Name)
	require.Equal(t, "org-1", got.Scalesets[1].Org)
	require.Equal(t, "z", got.Scalesets[2].Name)
	require.Equal(t, "explicit-org", got.Scalesets[2].Org)
}

// TestApplyOptionDefaults_MixingSingularAndPluralPanics
// pins the loud rejection of an ambiguous Options shape.
// (issue #280)
func TestApplyOptionDefaults_MixingSingularAndPluralPanics(t *testing.T) {
	t.Parallel()
	defer func() {
		r := recover()
		require.NotNil(t, r, "mixing singular fields with Scalesets must panic")
	}()
	applyOptionDefaults(Options{
		Scalesets:    []ScalesetSpec{{Name: "x"}},
		ScaleSetName: "should-not-coexist",
	})
}
