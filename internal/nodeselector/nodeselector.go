// Package nodeselector picks a Proxmox node to host a newly cloned VM.
//
// Three strategies are provided:
//
//   - Single: always returns the same node. The trivial case for
//     single-node Proxmox deployments.
//   - RoundRobin: rotates through a configured list of nodes. No external
//     state required.
//   - LeastLoaded: periodically polls /cluster/resources and picks the
//     node with the lowest weighted (CPU + memory) load. Best effort —
//     the score is refreshed at a configurable interval rather than per
//     selection to keep API load bounded.
//
// All implementations are safe for concurrent use.
package nodeselector

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"golang.org/x/sync/singleflight"

	"github.com/luthermonson/go-proxmox"
)

// Hint lets callers influence the selection without coupling each
// implementation to specifics. Avoid lists nodes that must not be
// returned (e.g. the source node when migrating).
//
// Profile and ExistingVMs feed the affinity wrapper (issue #8):
// Profile is the runner profile the new clone belongs to, used to
// look up the matching affinity rule; ExistingVMs is the snapshot
// of currently-tracked VMs (excluding the row about to be created)
// used to compute anti-affinity exclusions. Both are optional —
// the underlying single / round_robin / least_loaded selectors
// ignore them.
type Hint struct {
	Avoid       []string
	Profile     string
	ExistingVMs []ExistingVM
}

// ExistingVM is the affinity wrapper's projection of a tracked VM
// row. Only the fields anti-affinity actually consults are
// reproduced — the wrapper stays decoupled from the store package.
type ExistingVM struct {
	Node    string
	Profile string
}

// Selector picks a node name for a new VM.
type Selector interface {
	Select(ctx context.Context, hint Hint) (node string, err error)
}

// ---------------------------------------------------------------------------
// Single
// ---------------------------------------------------------------------------

type single struct {
	node string
}

// NewSingle returns a Selector that always returns the configured node.
func NewSingle(node string) (Selector, error) {
	if node == "" {
		return nil, errors.New("nodeselector: single requires a non-empty node")
	}
	return &single{node: node}, nil
}

func (s *single) Select(_ context.Context, hint Hint) (string, error) {
	if slices.Contains(hint.Avoid, s.node) {
		return "", fmt.Errorf("nodeselector: only node %q is in avoid list", s.node)
	}
	return s.node, nil
}

// ---------------------------------------------------------------------------
// RoundRobin
// ---------------------------------------------------------------------------

type roundRobin struct {
	mu    sync.Mutex
	nodes []string
	next  int
}

// NewRoundRobin returns a Selector that rotates through nodes in order.
func NewRoundRobin(nodes []string) (Selector, error) {
	if len(nodes) == 0 {
		return nil, errors.New("nodeselector: round_robin requires at least one node")
	}
	cp := make([]string, len(nodes))
	copy(cp, nodes)
	return &roundRobin{nodes: cp}, nil
}

func (r *roundRobin) Select(_ context.Context, hint Hint) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for range r.nodes {
		node := r.nodes[r.next]
		r.next = (r.next + 1) % len(r.nodes)
		if !slices.Contains(hint.Avoid, node) {
			return node, nil
		}
	}
	return "", errors.New("nodeselector: all nodes are in the avoid list")
}

// ---------------------------------------------------------------------------
// LeastLoaded
// ---------------------------------------------------------------------------

// resourceFetcher abstracts the Proxmox call that gathers per-node load.
// Production uses [proxmoxResourceFetcher] backed by *proxmox.Client; tests
// inject a fake.
type resourceFetcher interface {
	Fetch(ctx context.Context) (map[string]float64, error)
}

type proxmoxResourceFetcher struct {
	cli   *proxmox.Client
	nodes []string
}

// Fetch returns a map of node name → load score (lower is better). The
// score combines CPU and memory utilisation; both are in [0, 1] and the
// weights below favour CPU which is usually the binding resource for
// ephemeral runner VMs.
func (f *proxmoxResourceFetcher) Fetch(ctx context.Context) (map[string]float64, error) {
	statuses, err := f.cli.Nodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	out := make(map[string]float64, len(statuses))
	want := func(name string) bool {
		return len(f.nodes) == 0 || slices.Contains(f.nodes, name)
	}
	for _, ns := range statuses {
		if !want(ns.Node) {
			continue
		}
		// ns.MaxMem is bytes; ns.Mem is used bytes; ns.CPU is a fraction.
		var memFrac float64
		if ns.MaxMem > 0 {
			memFrac = float64(ns.Mem) / float64(ns.MaxMem)
		}
		out[ns.Node] = 0.7*ns.CPU + 0.3*memFrac
	}
	return out, nil
}

type leastLoaded struct {
	fetcher resourceFetcher

	// cache holds the most recent successful Fetch result. Expired reads
	// return nil from Get; on miss we singleflight a new Fetch. ttlcache
	// owns the TTL accounting (WithDisableTouchOnHit so reads don't
	// extend the freshness window).
	cache *ttlcache.Cache[string, map[string]float64]

	// lastFresh holds the most recent successful Fetch result regardless
	// of TTL. Consulted on the fetch-error path so a transient Proxmox
	// blip falls back to stale data instead of failing a selection.
	lastFresh atomic.Pointer[map[string]float64]

	// sf collapses concurrent cache-miss fetches into a single Proxmox
	// API call. Without it, N concurrent Select callers can each see a
	// stale cache, all call Fetch, and pile load on the API.
	sf singleflight.Group
}

// scoresCacheKey is the single key used in cache. The cache only ever
// holds one entry; we use ttlcache rather than a one-shot struct to
// delegate TTL accounting to the library.
const scoresCacheKey = "scores"

// NewLeastLoaded returns a Selector that polls Proxmox at most every
// `refresh` interval for node load. If `nodes` is non-empty, only nodes in
// the list are considered.
func NewLeastLoaded(cli *proxmox.Client, nodes []string, refresh time.Duration) (Selector, error) {
	if cli == nil {
		return nil, errors.New("nodeselector: least_loaded requires a non-nil proxmox client")
	}
	if refresh <= 0 {
		refresh = 30 * time.Second
	}
	return newLeastLoadedFromFetcher(&proxmoxResourceFetcher{cli: cli, nodes: nodes}, refresh), nil
}

// newLeastLoadedFromFetcher is the constructor used by tests to inject a
// fake fetcher. Production goes through NewLeastLoaded.
func newLeastLoadedFromFetcher(fetcher resourceFetcher, refresh time.Duration) *leastLoaded {
	return &leastLoaded{
		fetcher: fetcher,
		cache: ttlcache.New[string, map[string]float64](
			ttlcache.WithTTL[string, map[string]float64](refresh),
			ttlcache.WithDisableTouchOnHit[string, map[string]float64](),
		),
	}
}

func (l *leastLoaded) Select(ctx context.Context, hint Hint) (string, error) {
	scores, err := l.scores(ctx)
	if err != nil {
		return "", err
	}
	if len(scores) == 0 {
		return "", errors.New("nodeselector: no eligible nodes")
	}
	bestNode := ""
	bestScore := 0.0
	for node, score := range scores {
		if slices.Contains(hint.Avoid, node) {
			continue
		}
		if bestNode == "" || score < bestScore {
			bestNode, bestScore = node, score
		}
	}
	if bestNode == "" {
		return "", errors.New("nodeselector: all candidate nodes are in the avoid list")
	}
	return bestNode, nil
}

func (l *leastLoaded) scores(ctx context.Context) (map[string]float64, error) {
	if item := l.cache.Get(scoresCacheKey); item != nil {
		return maps.Clone(item.Value()), nil
	}

	// Collapse concurrent fetches into one Proxmox API call. Key is
	// constant — at most one Fetch in flight at a time across all
	// callers. Late arrivals share the result.
	v, err, _ := l.sf.Do(scoresCacheKey, func() (any, error) {
		fresh, err := l.fetcher.Fetch(ctx)
		if err != nil {
			return nil, err
		}
		l.cache.Set(scoresCacheKey, fresh, ttlcache.DefaultTTL)
		l.lastFresh.Store(&fresh)
		return fresh, nil
	})
	if err != nil {
		// Fall back to last known-good if we have any — stale info is
		// better than failing a selection during a transient Proxmox
		// blip.
		if prev := l.lastFresh.Load(); prev != nil {
			return maps.Clone(*prev), nil
		}
		return nil, err
	}
	return maps.Clone(v.(map[string]float64)), nil
}

// ---------------------------------------------------------------------------
// Affinity wrapper (issue #8)
// ---------------------------------------------------------------------------

// AffinityRule pins or excludes nodes for a given runner profile.
// Rules are matched in declaration order; the first rule whose
// Match.Profile equals the hint's Profile applies.
type AffinityRule struct {
	// Match selects which jobs this rule applies to. Empty Profile
	// matches anything (a default rule).
	Match AffinitySelector

	// PreferNodes, when non-empty, restricts candidate nodes to
	// this list. With Require=true the wrapper fails the clone
	// when no preferred node is eligible; with Require=false the
	// wrapper falls back to the full eligible set.
	PreferNodes []string

	// Require turns PreferNodes into a hard pin. Operators set
	// this for use cases like "GPU profile MUST land on a
	// GPU-equipped node — failing the clone is correct if none
	// are available, the alternative is silently running GPU jobs
	// on CPU-only hardware".
	Require bool

	// AntiAffinityWith excludes nodes that already host a tracked
	// VM whose attributes match this selector. Use case from the
	// issue: untrusted-PR runners must NEVER co-schedule with
	// prod runners on the same node.
	AntiAffinityWith AffinitySelector
}

// AffinitySelector is the projection on which a rule matches jobs
// (Match) or existing VMs (AntiAffinityWith). Today only Profile is
// supported; the type is left as a struct so future selectors
// (repo / org once the listener-integration extension lands) can
// be added without breaking the rule list shape.
type AffinitySelector struct {
	Profile string
}

// affinityWrapper composes affinity-based filtering over an
// underlying selector. The wrapper computes the eligible candidate
// set, communicates the inverse via Hint.Avoid, and delegates the
// final pick to the underlying selector — so existing strategies
// (single / round_robin / least_loaded) keep all their semantics
// (rotation, load balancing) within the eligible set.
type affinityWrapper struct {
	underlying Selector
	rules      []AffinityRule
	allNodes   []string // operator-declared node universe (NodesConfig.Members or [SingleNode])
}

// NewAffinity returns a Selector that applies the operator's
// affinity rules before delegating to the underlying selector. An
// empty rules slice returns the underlying selector unchanged so
// the affinity wrapper is zero-cost when not configured.
func NewAffinity(underlying Selector, rules []AffinityRule, allNodes []string) (Selector, error) {
	if underlying == nil {
		return nil, errors.New("nodeselector: affinity requires a non-nil underlying selector")
	}
	if len(rules) == 0 {
		return underlying, nil
	}
	if len(allNodes) == 0 {
		return nil, errors.New("nodeselector: affinity requires the operator node universe to compute eligibility")
	}
	cp := make([]string, len(allNodes))
	copy(cp, allNodes)
	return &affinityWrapper{
		underlying: underlying,
		rules:      append([]AffinityRule(nil), rules...),
		allNodes:   cp,
	}, nil
}

// ErrAffinityRequireUnsatisfiable is returned by Select when an
// AffinityRule with Require=true matches the hint but no preferred
// node is eligible (every preferred node is either in the avoid
// list or hosts an anti-affinity violator). Distinct from the
// underlying selector's "no eligible nodes" so callers / metrics
// can distinguish "hard-pin couldn't be satisfied" from "load
// balancer ran out of nodes for some other reason".
var ErrAffinityRequireUnsatisfiable = errors.New("nodeselector: affinity require=true unsatisfiable")

// eligibleFrom returns the set of candidate nodes minus any node
// listed in excluded (anti-affinity) or avoid (caller-supplied
// Hint.Avoid translated to a set). Extracted from
// affinityWrapper.Select so the three call sites (prefer_nodes
// branch, no-prefer-nodes branch, soft-pin fallback) share one
// loop. No behaviour change.
func eligibleFrom(candidates []string, excluded, avoid map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(candidates))
	for _, n := range candidates {
		if _, ex := excluded[n]; ex {
			continue
		}
		if _, av := avoid[n]; av {
			continue
		}
		out[n] = struct{}{}
	}
	return out
}

func (a *affinityWrapper) Select(ctx context.Context, hint Hint) (string, error) {
	rule := a.matchingRule(hint.Profile)
	if rule == nil {
		// No matching rule — pass through unchanged.
		return a.underlying.Select(ctx, hint)
	}

	// Compute anti-affinity exclusions: nodes hosting a tracked
	// VM whose Profile matches AntiAffinityWith.Profile.
	excluded := make(map[string]struct{})
	if rule.AntiAffinityWith.Profile != "" {
		for _, vm := range hint.ExistingVMs {
			if vm.Profile == rule.AntiAffinityWith.Profile {
				excluded[vm.Node] = struct{}{}
			}
		}
	}

	// Build the eligible set. Without prefer_nodes, eligibility
	// is the full node universe minus anti-affinity exclusions
	// minus the caller's pre-existing Avoid.
	preExistingAvoid := make(map[string]struct{}, len(hint.Avoid))
	for _, n := range hint.Avoid {
		preExistingAvoid[n] = struct{}{}
	}

	var eligible map[string]struct{}
	if len(rule.PreferNodes) > 0 {
		eligible = eligibleFrom(rule.PreferNodes, excluded, preExistingAvoid)
	} else {
		eligible = eligibleFrom(a.allNodes, excluded, preExistingAvoid)
	}

	if len(eligible) == 0 {
		if rule.Require {
			return "", fmt.Errorf("%w: profile %q has no eligible preferred node (anti_affinity_with=%q removed %d, avoid=%d)",
				ErrAffinityRequireUnsatisfiable, hint.Profile, rule.AntiAffinityWith.Profile, len(excluded), len(hint.Avoid))
		}
		// Soft pin: fall back to the full universe minus exclusions
		// and Avoid.
		eligible = eligibleFrom(a.allNodes, excluded, preExistingAvoid)
		if len(eligible) == 0 {
			return "", errors.New("nodeselector: affinity rules left no eligible node")
		}
	}

	// Translate eligibility into the existing Hint.Avoid surface so
	// the underlying selector retains its rotation / load
	// semantics within the eligible set.
	newAvoid := append([]string(nil), hint.Avoid...)
	for _, n := range a.allNodes {
		if _, ok := eligible[n]; !ok {
			newAvoid = append(newAvoid, n)
		}
	}
	newHint := hint
	newHint.Avoid = newAvoid
	return a.underlying.Select(ctx, newHint)
}

// matchingRule returns the first rule whose Match.Profile equals
// profile, or nil when no rule matches. An empty Match.Profile is
// treated as a wildcard so operators can declare a catch-all rule.
func (a *affinityWrapper) matchingRule(profile string) *AffinityRule {
	for i := range a.rules {
		r := &a.rules[i]
		if r.Match.Profile == "" || r.Match.Profile == profile {
			return r
		}
	}
	return nil
}
