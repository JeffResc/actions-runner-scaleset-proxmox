package nodeselector

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/nodecap"
)

// fakeFitter reports a fixed fit verdict per node, and optionally fails.
type fakeFitter struct {
	roomy map[string]bool
	err   error
	// lastCandidates records what the wrapper asked about, so a test can
	// assert it never asks about nodes the caller already excluded.
	lastCandidates []string
}

func (f *fakeFitter) Fits(_ context.Context, _ nodecap.Shape, candidates []string) (map[string]bool, error) {
	f.lastCandidates = append([]string(nil), candidates...)
	if f.err != nil {
		return nil, f.err
	}
	out := make(map[string]bool, len(candidates))
	for _, c := range candidates {
		out[c] = f.roomy[c]
	}
	return out, nil
}

var testShape = nodecap.Shape{MemoryBytes: 8 * 1024 * 1024 * 1024, VCPUs: 4}

// TestCapacity_NilFitterIsPassThrough keeps the wrapper zero-cost when
// resource-aware admission is switched off.
func TestCapacity_NilFitterIsPassThrough(t *testing.T) {
	t.Parallel()
	rr, err := NewRoundRobin([]string{"a", "b"})
	require.NoError(t, err)

	got, err := NewCapacity(rr, nil, []string{"a", "b"})
	require.NoError(t, err)
	require.Same(t, rr, got, "a nil fitter must return the underlying selector unwrapped")
}

// TestCapacity_ZeroShapeIsPassThrough: a profile with no declared
// footprint has nothing to size, so the wrapper must not invent one.
func TestCapacity_ZeroShapeIsPassThrough(t *testing.T) {
	t.Parallel()
	f := &fakeFitter{roomy: map[string]bool{}}
	single, _ := NewSingle("pve1")
	sel, err := NewCapacity(single, f, []string{"pve1"})
	require.NoError(t, err)

	got, err := sel.Select(context.Background(), Hint{})
	require.NoError(t, err)
	require.Equal(t, "pve1", got)
	require.Nil(t, f.lastCandidates, "the fitter must not be consulted for a zero shape")
}

// TestCapacity_SkipsFullNodes is the core behaviour: rotation continues
// among the nodes that have room rather than stalling on a full one.
func TestCapacity_SkipsFullNodes(t *testing.T) {
	t.Parallel()
	f := &fakeFitter{roomy: map[string]bool{"a": false, "b": true, "c": false}}
	rr, _ := NewRoundRobin([]string{"a", "b", "c"})
	sel, err := NewCapacity(rr, f, []string{"a", "b", "c"})
	require.NoError(t, err)

	for range 3 {
		got, err := sel.Select(context.Background(), Hint{Shape: testShape})
		require.NoError(t, err)
		require.Equal(t, "b", got, "only b has room, so every selection lands there")
	}
}

// TestCapacity_NoRoomAnywhereIsBackpressure: the caller must be able to
// tell "the fleet is full" apart from a real fault, because the former
// is a wait and the latter is a bug.
func TestCapacity_NoRoomAnywhereIsBackpressure(t *testing.T) {
	t.Parallel()
	f := &fakeFitter{roomy: map[string]bool{"a": false, "b": false}}
	rr, _ := NewRoundRobin([]string{"a", "b"})
	sel, err := NewCapacity(rr, f, []string{"a", "b"})
	require.NoError(t, err)

	_, err = sel.Select(context.Background(), Hint{Shape: testShape, Profile: "mem-16g"})
	require.ErrorIs(t, err, ErrNoCapacity)
	require.Contains(t, err.Error(), "mem-16g", "the error should name the profile that couldn't be placed")
}

// TestCapacity_FitterErrorFailsClosed: admitting while we don't know
// what is allocated is exactly the overcommit this wrapper prevents.
func TestCapacity_FitterErrorFailsClosed(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("proxmox unreachable")
	f := &fakeFitter{err: sentinel}
	single, _ := NewSingle("pve1")
	sel, err := NewCapacity(single, f, []string{"pve1"})
	require.NoError(t, err)

	_, err = sel.Select(context.Background(), Hint{Shape: testShape})
	require.ErrorIs(t, err, ErrNoCapacity)
	require.ErrorIs(t, err, sentinel, "the underlying cause must stay inspectable")
}

// TestCapacity_HonoursPreExistingAvoid: the wrapper narrows, it never
// widens — a node the caller already excluded is not offered back, and
// isn't even asked about.
func TestCapacity_HonoursPreExistingAvoid(t *testing.T) {
	t.Parallel()
	f := &fakeFitter{roomy: map[string]bool{"a": true, "b": true}}
	rr, _ := NewRoundRobin([]string{"a", "b"})
	sel, err := NewCapacity(rr, f, []string{"a", "b"})
	require.NoError(t, err)

	got, err := sel.Select(context.Background(), Hint{Shape: testShape, Avoid: []string{"a"}})
	require.NoError(t, err)
	require.Equal(t, "b", got)
	require.Equal(t, []string{"b"}, f.lastCandidates,
		"an already-avoided node must not even be asked about")
}

// TestCapacity_OutsideAffinity pins the layering. Capacity wraps
// affinity, so a full node reaches the affinity rules as an ordinary
// avoid entry — which keeps require:true honest instead of letting a
// hard-pinned profile silently spill onto an unpinned node.
func TestCapacity_OutsideAffinity(t *testing.T) {
	t.Parallel()
	all := []string{"gpu-1", "gpu-2", "cpu-1"}
	rules := []AffinityRule{{
		Match:       AffinitySelector{Profile: "gpu"},
		PreferNodes: []string{"gpu-1", "gpu-2"},
		Require:     true,
	}}

	t.Run("hard pin fails loudly when its nodes are full", func(t *testing.T) {
		t.Parallel()
		// Both GPU nodes full; a CPU node has room but is not eligible.
		f := &fakeFitter{roomy: map[string]bool{"gpu-1": false, "gpu-2": false, "cpu-1": true}}
		rr, _ := NewRoundRobin(all)
		aff, err := NewAffinity(rr, rules, all)
		require.NoError(t, err)
		sel, err := NewCapacity(aff, f, all)
		require.NoError(t, err)

		_, err = sel.Select(context.Background(), Hint{Profile: "gpu", Shape: testShape})
		require.ErrorIs(t, err, ErrAffinityRequireUnsatisfiable,
			"a full hard-pin must fail as a pin violation, never spill onto cpu-1")
	})

	t.Run("hard pin still places on the preferred node that has room", func(t *testing.T) {
		t.Parallel()
		f := &fakeFitter{roomy: map[string]bool{"gpu-1": false, "gpu-2": true, "cpu-1": true}}
		rr, _ := NewRoundRobin(all)
		aff, err := NewAffinity(rr, rules, all)
		require.NoError(t, err)
		sel, err := NewCapacity(aff, f, all)
		require.NoError(t, err)

		got, err := sel.Select(context.Background(), Hint{Profile: "gpu", Shape: testShape})
		require.NoError(t, err)
		require.Equal(t, "gpu-2", got)
	})
}

// TestCapacity_SingleNodeFullDefers covers the homelab shape: one node,
// no room, so the clone waits rather than erroring out as a fault.
func TestCapacity_SingleNodeFullDefers(t *testing.T) {
	t.Parallel()
	f := &fakeFitter{roomy: map[string]bool{"pve1": false}}
	single, _ := NewSingle("pve1")
	sel, err := NewCapacity(single, f, []string{"pve1"})
	require.NoError(t, err)

	_, err = sel.Select(context.Background(), Hint{Shape: testShape})
	require.ErrorIs(t, err, ErrNoCapacity)
}

// TestCapacity_RequiresNodeUniverse: without the operator's node list
// the wrapper has nothing to filter, and silently degrading to a
// pass-through would disable admission without saying so.
func TestCapacity_RequiresNodeUniverse(t *testing.T) {
	t.Parallel()
	single, _ := NewSingle("pve1")
	_, err := NewCapacity(single, &fakeFitter{}, nil)
	require.Error(t, err)
}
