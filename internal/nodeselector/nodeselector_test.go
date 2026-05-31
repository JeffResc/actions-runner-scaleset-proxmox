package nodeselector

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSingle_Returns(t *testing.T) {
	t.Parallel()
	s, err := NewSingle("pve1")
	require.NoError(t, err)

	got, err := s.Select(context.Background(), Hint{})
	require.NoError(t, err)
	require.Equal(t, "pve1", got)
}

func TestSingle_RejectsAvoid(t *testing.T) {
	t.Parallel()
	s, _ := NewSingle("pve1")
	_, err := s.Select(context.Background(), Hint{Avoid: []string{"pve1"}})
	require.Error(t, err)
}

func TestSingle_RejectsEmpty(t *testing.T) {
	t.Parallel()
	_, err := NewSingle("")
	require.Error(t, err)
}

func TestRoundRobin_RotatesAndRecyclesPastAvoid(t *testing.T) {
	t.Parallel()
	s, err := NewRoundRobin([]string{"a", "b", "c"})
	require.NoError(t, err)

	ctx := context.Background()
	got1, _ := s.Select(ctx, Hint{})
	got2, _ := s.Select(ctx, Hint{})
	got3, _ := s.Select(ctx, Hint{})
	got4, _ := s.Select(ctx, Hint{})
	require.Equal(t, []string{"a", "b", "c", "a"}, []string{got1, got2, got3, got4})

	// Avoid steps over excluded nodes.
	skipB, err := s.Select(ctx, Hint{Avoid: []string{"b"}})
	require.NoError(t, err)
	require.NotEqual(t, "b", skipB)
}

func TestRoundRobin_AllAvoided(t *testing.T) {
	t.Parallel()
	s, _ := NewRoundRobin([]string{"a", "b"})
	_, err := s.Select(context.Background(), Hint{Avoid: []string{"a", "b"}})
	require.Error(t, err)
}

func TestRoundRobin_RejectsEmpty(t *testing.T) {
	t.Parallel()
	_, err := NewRoundRobin(nil)
	require.Error(t, err)
}

// fakeFetcher is an in-memory resourceFetcher.
type fakeFetcher struct {
	scores map[string]float64
	calls  int
	err    error
}

func (f *fakeFetcher) Fetch(_ context.Context) (map[string]float64, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	out := make(map[string]float64, len(f.scores))
	for k, v := range f.scores {
		out[k] = v
	}
	return out, nil
}

func TestLeastLoaded_PicksMin(t *testing.T) {
	t.Parallel()
	f := &fakeFetcher{scores: map[string]float64{"a": 0.9, "b": 0.2, "c": 0.5}}
	l := newLeastLoadedFromFetcher(f, 30*time.Second)

	got, err := l.Select(context.Background(), Hint{})
	require.NoError(t, err)
	require.Equal(t, "b", got)
}

func TestLeastLoaded_HonoursAvoid(t *testing.T) {
	t.Parallel()
	f := &fakeFetcher{scores: map[string]float64{"a": 0.1, "b": 0.5, "c": 0.3}}
	l := newLeastLoadedFromFetcher(f, 30*time.Second)

	got, err := l.Select(context.Background(), Hint{Avoid: []string{"a"}})
	require.NoError(t, err)
	require.Equal(t, "c", got)
}

// TestLeastLoaded_CachesWithinRefresh uses a real (short) refresh
// interval and a real time.Sleep. ttlcache uses time.Now() internally,
// so the previously-injected clock function no longer applies. The
// margins (200 ms refresh, 5 selects in a tight loop, then sleep
// 300 ms) are wide enough to be reliable under load.
func TestLeastLoaded_CachesWithinRefresh(t *testing.T) {
	t.Parallel()
	f := &fakeFetcher{scores: map[string]float64{"a": 0.5, "b": 0.2}}
	l := newLeastLoadedFromFetcher(f, 200*time.Millisecond)

	for range 5 {
		_, err := l.Select(context.Background(), Hint{})
		require.NoError(t, err)
	}
	require.Equal(t, 1, f.calls, "should only fetch once within the refresh window")

	// Advance past the refresh window with a real sleep.
	time.Sleep(300 * time.Millisecond)
	_, err := l.Select(context.Background(), Hint{})
	require.NoError(t, err)
	require.Equal(t, 2, f.calls)
}

func TestLeastLoaded_StaleFallbackOnFetchError(t *testing.T) {
	t.Parallel()
	f := &fakeFetcher{scores: map[string]float64{"a": 0.2}}
	l := newLeastLoadedFromFetcher(f, 50*time.Millisecond)

	// First call seeds the cache.
	got, err := l.Select(context.Background(), Hint{})
	require.NoError(t, err)
	require.Equal(t, "a", got)

	// Let the fresh-cache entry expire so the next Select takes the
	// fetch path; the fetcher will fail, and the stale fallback must
	// still answer with the previously-good map.
	f.err = errors.New("api down")
	time.Sleep(100 * time.Millisecond)

	got, err = l.Select(context.Background(), Hint{})
	require.NoError(t, err)
	require.Equal(t, "a", got)
}

func TestLeastLoaded_FetchErrorWithNoCache(t *testing.T) {
	t.Parallel()
	f := &fakeFetcher{err: errors.New("boom")}
	l := newLeastLoadedFromFetcher(f, time.Minute)
	_, err := l.Select(context.Background(), Hint{})
	require.Error(t, err)
}

func TestLeastLoaded_RejectsNilClient(t *testing.T) {
	t.Parallel()
	_, err := NewLeastLoaded(nil, nil, time.Minute)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Affinity wrapper (PR 6 — issue #8)
// ---------------------------------------------------------------------------

func TestAffinity_NoRulesReturnsUnderlyingUnchanged(t *testing.T) {
	t.Parallel()
	rr, err := NewRoundRobin([]string{"pve1", "pve2"})
	require.NoError(t, err)
	got, err := NewAffinity(rr, nil, []string{"pve1", "pve2"})
	require.NoError(t, err)
	require.Same(t, rr, got, "no rules: NewAffinity must return underlying unchanged (zero overhead)")
}

func TestAffinity_PreferNodesRequireTrueHardPins(t *testing.T) {
	t.Parallel()
	rr, err := NewRoundRobin([]string{"pve1", "pve2", "pve-gpu-1", "pve-gpu-2"})
	require.NoError(t, err)
	a, err := NewAffinity(rr, []AffinityRule{{
		Match:       AffinitySelector{Profile: "gpu"},
		PreferNodes: []string{"pve-gpu-1", "pve-gpu-2"},
		Require:     true,
	}}, []string{"pve1", "pve2", "pve-gpu-1", "pve-gpu-2"})
	require.NoError(t, err)

	// 10 calls — every result must come from prefer_nodes only.
	for range 10 {
		got, err := a.Select(t.Context(), Hint{Profile: "gpu"})
		require.NoError(t, err)
		require.Contains(t, []string{"pve-gpu-1", "pve-gpu-2"}, got,
			"hard pin must keep selection on prefer_nodes; got %q", got)
	}
}

func TestAffinity_PreferNodesRequireTrueFailsWhenNoneEligible(t *testing.T) {
	t.Parallel()
	rr, err := NewRoundRobin([]string{"pve1", "pve2", "pve-gpu-1"})
	require.NoError(t, err)
	a, err := NewAffinity(rr, []AffinityRule{{
		Match:       AffinitySelector{Profile: "gpu"},
		PreferNodes: []string{"pve-gpu-1"},
		Require:     true,
	}}, []string{"pve1", "pve2", "pve-gpu-1"})
	require.NoError(t, err)

	// The only preferred node is in the caller's avoid list.
	_, err = a.Select(t.Context(), Hint{
		Profile: "gpu",
		Avoid:   []string{"pve-gpu-1"},
	})
	require.ErrorIs(t, err, ErrAffinityRequireUnsatisfiable,
		"hard pin with all preferred nodes excluded must fail clone with a clear sentinel")
}

func TestAffinity_PreferNodesRequireFalseFallsBackToFullSet(t *testing.T) {
	t.Parallel()
	rr, err := NewRoundRobin([]string{"pve1", "pve2", "pve-gpu-1"})
	require.NoError(t, err)
	a, err := NewAffinity(rr, []AffinityRule{{
		Match:       AffinitySelector{Profile: "gpu"},
		PreferNodes: []string{"pve-gpu-1"},
		Require:     false, // soft pin
	}}, []string{"pve1", "pve2", "pve-gpu-1"})
	require.NoError(t, err)

	got, err := a.Select(t.Context(), Hint{
		Profile: "gpu",
		Avoid:   []string{"pve-gpu-1"}, // preferred is unavailable
	})
	require.NoError(t, err, "soft pin must fall back when no preferred node is eligible")
	require.Contains(t, []string{"pve1", "pve2"}, got,
		"fallback picks from non-preferred nodes; got %q", got)
}

func TestAffinity_AntiAffinityExcludesNodesHostingMatchingVMs(t *testing.T) {
	t.Parallel()
	rr, err := NewRoundRobin([]string{"pve1", "pve2", "pve3"})
	require.NoError(t, err)
	a, err := NewAffinity(rr, []AffinityRule{{
		Match:            AffinitySelector{Profile: "untrusted-pr"},
		AntiAffinityWith: AffinitySelector{Profile: "prod"},
	}}, []string{"pve1", "pve2", "pve3"})
	require.NoError(t, err)

	// pve1 hosts a `prod` VM; pve2 hosts a sibling `untrusted-pr`;
	// pve3 is clean. Anti-affinity must exclude pve1 only.
	hint := Hint{
		Profile: "untrusted-pr",
		ExistingVMs: []ExistingVM{
			{Node: "pve1", Profile: "prod"},
			{Node: "pve2", Profile: "untrusted-pr"},
		},
	}
	for range 10 {
		got, err := a.Select(t.Context(), hint)
		require.NoError(t, err)
		require.NotEqual(t, "pve1", got,
			"anti-affinity with prod must keep untrusted-pr off pve1; got %q", got)
		require.Contains(t, []string{"pve2", "pve3"}, got)
	}
}

func TestAffinity_AntiAffinityCombinedWithPreferNodesRequire(t *testing.T) {
	t.Parallel()
	// gpu profile pins to pve-gpu-1 / pve-gpu-2 AND must not
	// co-schedule with `prod`. With pve-gpu-1 hosting a prod VM,
	// only pve-gpu-2 is eligible.
	rr, err := NewRoundRobin([]string{"pve1", "pve-gpu-1", "pve-gpu-2"})
	require.NoError(t, err)
	a, err := NewAffinity(rr, []AffinityRule{{
		Match:            AffinitySelector{Profile: "gpu"},
		PreferNodes:      []string{"pve-gpu-1", "pve-gpu-2"},
		Require:          true,
		AntiAffinityWith: AffinitySelector{Profile: "prod"},
	}}, []string{"pve1", "pve-gpu-1", "pve-gpu-2"})
	require.NoError(t, err)

	hint := Hint{
		Profile: "gpu",
		ExistingVMs: []ExistingVM{
			{Node: "pve-gpu-1", Profile: "prod"},
		},
	}
	for range 5 {
		got, err := a.Select(t.Context(), hint)
		require.NoError(t, err)
		require.Equal(t, "pve-gpu-2", got,
			"anti-affinity + hard pin: only pve-gpu-2 survives both filters")
	}
}

func TestAffinity_NoMatchingRulePassesThrough(t *testing.T) {
	t.Parallel()
	rr, err := NewRoundRobin([]string{"pve1", "pve2"})
	require.NoError(t, err)
	a, err := NewAffinity(rr, []AffinityRule{{
		Match:       AffinitySelector{Profile: "gpu"},
		PreferNodes: []string{"pve-gpu-1"},
		Require:     true,
	}}, []string{"pve1", "pve2", "pve-gpu-1"})
	require.NoError(t, err)

	// linux-x64 profile doesn't match any rule — selector must
	// behave exactly as the underlying RoundRobin.
	got, err := a.Select(t.Context(), Hint{Profile: "linux-x64"})
	require.NoError(t, err)
	require.Contains(t, []string{"pve1", "pve2"}, got,
		"non-matching profile must NOT be confined by the gpu rule; got %q", got)
}

func TestAffinity_WildcardRuleMatchesEverything(t *testing.T) {
	t.Parallel()
	rr, err := NewRoundRobin([]string{"pve1", "pve2", "pve3"})
	require.NoError(t, err)
	// Empty Match.Profile = wildcard. Use it as a catch-all to
	// exclude pve3 from ALL clones.
	a, err := NewAffinity(rr, []AffinityRule{{
		Match: AffinitySelector{Profile: ""},
		// We can't express "always avoid pve3" cleanly via
		// prefer_nodes (we'd have to list every OTHER node), but
		// require=false + prefer=[pve1,pve2] gets us close.
		PreferNodes: []string{"pve1", "pve2"},
		Require:     true,
	}}, []string{"pve1", "pve2", "pve3"})
	require.NoError(t, err)

	for range 10 {
		got, err := a.Select(t.Context(), Hint{Profile: "anything"})
		require.NoError(t, err)
		require.NotEqual(t, "pve3", got,
			"wildcard rule must constrain every profile; got %q", got)
	}
}

func TestNewAffinity_RejectsNilUnderlying(t *testing.T) {
	t.Parallel()
	_, err := NewAffinity(nil, []AffinityRule{{}}, []string{"pve1"})
	require.Error(t, err)
}

func TestNewAffinity_RejectsEmptyNodeUniverse(t *testing.T) {
	t.Parallel()
	rr, err := NewRoundRobin([]string{"pve1"})
	require.NoError(t, err)
	_, err = NewAffinity(rr, []AffinityRule{{}}, nil)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Stress: all-nodes-down, flapping availability, uneven distribution (#293)
// ---------------------------------------------------------------------------

// TestRoundRobin_AllNodesDownErrorsCleanly pins #293's
// all-nodes-unavailable case for the round-robin selector: when
// every member of the cluster is in the caller's Avoid list, the
// selector must return an error rather than hang or arbitrarily
// pick one. (issue #293)
func TestRoundRobin_AllNodesDownErrorsCleanly(t *testing.T) {
	t.Parallel()
	s, err := NewRoundRobin([]string{"pve1", "pve2", "pve3", "pve4"})
	require.NoError(t, err)
	_, err = s.Select(context.Background(), Hint{Avoid: []string{"pve1", "pve2", "pve3", "pve4"}})
	require.Error(t, err,
		"round_robin with every node avoided must surface a clear error, not hang or arbitrarily pick (issue #293)")
}

// TestLeastLoaded_AllNodesDownErrorsCleanly pins #293 for the
// least-loaded selector. Same contract — every reachable node
// excluded by Avoid must yield a clear error.
func TestLeastLoaded_AllNodesDownErrorsCleanly(t *testing.T) {
	t.Parallel()
	f := &fakeFetcher{scores: map[string]float64{"a": 0.1, "b": 0.2, "c": 0.3}}
	l := newLeastLoadedFromFetcher(f, 30*time.Second)
	_, err := l.Select(context.Background(), Hint{Avoid: []string{"a", "b", "c"}})
	require.Error(t, err,
		"least_loaded with every node avoided must surface a clear error (issue #293)")
}

// TestLeastLoaded_FlappingFetchFallsBackToLastFresh covers #293's
// flapping-availability scenario at the selector layer: when the
// underlying Proxmox node-listing call starts failing, the
// selector must NOT immediately fail every Select — it must serve
// from the last-known-good cache so a transient Proxmox blip does
// not stall clone scheduling. A second tick that succeeds again
// resumes normal selection.
func TestLeastLoaded_FlappingFetchFallsBackToLastFresh(t *testing.T) {
	t.Parallel()
	f := &fakeFetcher{scores: map[string]float64{"a": 0.1, "b": 0.5}}
	l := newLeastLoadedFromFetcher(f, 50*time.Millisecond)

	// Seed lastFresh with a successful fetch.
	got, err := l.Select(context.Background(), Hint{})
	require.NoError(t, err)
	require.Equal(t, "a", got)

	// Wait for cache TTL to expire, then induce a fetch failure.
	time.Sleep(100 * time.Millisecond)
	f.err = errors.New("transient proxmox blip")
	got, err = l.Select(context.Background(), Hint{})
	require.NoError(t, err, "flapping Proxmox listing must fall back to last-fresh, not fail the Select (issue #293)")
	require.Equal(t, "a", got)

	// Recovery: fetch starts working again, Select returns to live data.
	f.err = nil
	f.scores = map[string]float64{"a": 0.9, "b": 0.1}
	time.Sleep(100 * time.Millisecond)
	got, err = l.Select(context.Background(), Hint{})
	require.NoError(t, err)
	require.Equal(t, "b", got, "post-recovery Select must use the new fresh data, not the stale fallback")
}

// TestLeastLoaded_UnevenDistributionSpreadsAccordingToLoad
// pins #293's load-spreading contract: when one node is at 100%
// CPU and another at 0%, repeated Select calls (with Avoid
// reflecting the previously-selected node) must pick the cold
// node, not the hot one. The selector's job is to spread, not
// always return the first reachable node.
func TestLeastLoaded_UnevenDistributionSpreadsAccordingToLoad(t *testing.T) {
	t.Parallel()
	// Skewed: pve-hot is 99% loaded, pve-cold is 1%. The selector
	// must consistently prefer pve-cold across many Selects.
	f := &fakeFetcher{scores: map[string]float64{
		"pve-hot":  0.99,
		"pve-cold": 0.01,
	}}
	l := newLeastLoadedFromFetcher(f, 30*time.Second)

	const N = 50
	for range N {
		got, err := l.Select(context.Background(), Hint{})
		require.NoError(t, err)
		require.Equal(t, "pve-cold", got,
			"least_loaded must NOT spread to the 99%%-loaded node — load-aware selection is the whole point (issue #293)")
	}
}
