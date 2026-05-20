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

func newLeastLoaded(f *fakeFetcher, refresh time.Duration, now func() time.Time) *leastLoaded {
	return &leastLoaded{fetcher: f, refresh: refresh, timeNow: now}
}

func TestLeastLoaded_PicksMin(t *testing.T) {
	t.Parallel()
	f := &fakeFetcher{scores: map[string]float64{"a": 0.9, "b": 0.2, "c": 0.5}}
	l := newLeastLoaded(f, 30*time.Second, time.Now)

	got, err := l.Select(context.Background(), Hint{})
	require.NoError(t, err)
	require.Equal(t, "b", got)
}

func TestLeastLoaded_HonoursAvoid(t *testing.T) {
	t.Parallel()
	f := &fakeFetcher{scores: map[string]float64{"a": 0.1, "b": 0.5, "c": 0.3}}
	l := newLeastLoaded(f, 30*time.Second, time.Now)

	got, err := l.Select(context.Background(), Hint{Avoid: []string{"a"}})
	require.NoError(t, err)
	require.Equal(t, "c", got)
}

func TestLeastLoaded_CachesWithinRefresh(t *testing.T) {
	t.Parallel()
	f := &fakeFetcher{scores: map[string]float64{"a": 0.5, "b": 0.2}}
	now := time.Now()
	clk := &now
	l := newLeastLoaded(f, time.Minute, func() time.Time { return *clk })

	for range 5 {
		_, err := l.Select(context.Background(), Hint{})
		require.NoError(t, err)
	}
	require.Equal(t, 1, f.calls, "should only fetch once within the refresh window")

	// Advance past the refresh window.
	*clk = clk.Add(2 * time.Minute)
	_, err := l.Select(context.Background(), Hint{})
	require.NoError(t, err)
	require.Equal(t, 2, f.calls)
}

func TestLeastLoaded_StaleFallbackOnFetchError(t *testing.T) {
	t.Parallel()
	f := &fakeFetcher{scores: map[string]float64{"a": 0.2}}
	now := time.Now()
	clk := &now
	l := newLeastLoaded(f, 10*time.Millisecond, func() time.Time { return *clk })

	// First call seeds the cache.
	got, err := l.Select(context.Background(), Hint{})
	require.NoError(t, err)
	require.Equal(t, "a", got)

	// Subsequent fetch fails; we should still get cached data.
	f.err = errors.New("api down")
	*clk = clk.Add(time.Second)

	got, err = l.Select(context.Background(), Hint{})
	require.NoError(t, err)
	require.Equal(t, "a", got)
}

func TestLeastLoaded_FetchErrorWithNoCache(t *testing.T) {
	t.Parallel()
	f := &fakeFetcher{err: errors.New("boom")}
	l := newLeastLoaded(f, time.Minute, time.Now)
	_, err := l.Select(context.Background(), Hint{})
	require.Error(t, err)
}

func TestLeastLoaded_RejectsNilClient(t *testing.T) {
	t.Parallel()
	_, err := NewLeastLoaded(nil, nil, time.Minute)
	require.Error(t, err)
}
