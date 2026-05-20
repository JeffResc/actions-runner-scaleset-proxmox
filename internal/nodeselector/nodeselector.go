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
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/luthermonson/go-proxmox"
)

// Hint lets callers influence the selection without coupling each
// implementation to specifics. Avoid lists nodes that must not be
// returned (e.g. the source node when migrating).
type Hint struct {
	Avoid []string
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
	nodes   []string
	refresh time.Duration
	timeNow func() time.Time

	mu       sync.Mutex
	cached   map[string]float64
	lastSeen time.Time
	// sf collapses concurrent cache-miss fetches into a single Proxmox
	// API call. Without it, N concurrent Select callers can each see a
	// stale cache, all call Fetch, and pile load on the API.
	sf singleflight.Group
}

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
	return &leastLoaded{
		fetcher: &proxmoxResourceFetcher{cli: cli, nodes: nodes},
		nodes:   nodes,
		refresh: refresh,
		timeNow: time.Now,
	}, nil
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
	l.mu.Lock()
	now := l.timeNow()
	if l.cached != nil && now.Sub(l.lastSeen) < l.refresh {
		out := maps.Clone(l.cached)
		l.mu.Unlock()
		return out, nil
	}
	l.mu.Unlock()

	// Collapse concurrent fetches into one Proxmox API call. Key is
	// constant — at most one Fetch in flight at a time across all
	// callers. Late arrivals share the result.
	v, err, _ := l.sf.Do("fetch", func() (any, error) {
		return l.fetcher.Fetch(ctx)
	})
	if err != nil {
		// Fall back to cached data if we have any — stale info is better
		// than failing a selection during a transient Proxmox blip.
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.cached != nil {
			return maps.Clone(l.cached), nil
		}
		return nil, err
	}
	fresh := v.(map[string]float64)
	l.mu.Lock()
	l.cached = fresh
	l.lastSeen = now
	out := maps.Clone(fresh)
	l.mu.Unlock()
	return out, nil
}
