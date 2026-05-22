package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
)

// RaftConfig configures the embedded-raft coordinator.
//
// Every replica in the cluster must declare the full peer list under
// Peers (including its own entry). Exactly one peer's Bootstrap field
// should be true on first cluster startup; subsequent restarts can
// leave it as configured — raft tolerates BootstrapCluster being
// called against an already-bootstrapped cluster (returns
// raft.ErrCantBootstrap, which we swallow).
type RaftConfig struct {
	// NodeID is this replica's unique identifier in the raft
	// configuration. Must match exactly one entry in Peers.NodeID.
	// Typically the hostname.
	NodeID string

	// BindAddr is the listen address for the raft TCP transport,
	// e.g. "0.0.0.0:7000". Ignored in test mode (where the caller
	// supplies an InmemTransport).
	BindAddr string

	// AdvertiseAddr is the address other peers use to dial this
	// replica's raft port, e.g. "10.0.0.1:7000". When empty, falls
	// back to BindAddr — only safe when BindAddr is a routable
	// (non-wildcard) address.
	AdvertiseAddr string

	// AdminPort is this replica's admin HTTP port. Combined with the
	// peer's HTTPAddr at LeaderEndpoint time; 0 disables the local
	// fast-path (LeaderEndpoint still resolves via the peer map).
	AdminPort int

	// AdminHost is this replica's admin HTTP host (typically the same
	// as AdvertiseAddr's host). Combined with AdminPort for the local
	// fast-path. Empty disables the fast-path.
	AdminHost string

	// Peers is the full static peer list, including this replica.
	// All replicas must agree on this list at startup. Dynamic
	// membership changes (AddVoter/RemoveVoter) are not exposed.
	Peers []RaftPeer

	// Bootstrap, when true on at least one node in the cluster, makes
	// that node call raft.BootstrapCluster on first start. Other
	// nodes wait to be discovered by the bootstrapped leader. Should
	// be true on exactly one replica during initial cluster setup;
	// safe to leave true on subsequent restarts (the in-memory store
	// always has zero existing state so the call is required, but
	// raft tolerates concurrent BootstrapCluster against an
	// already-formed cluster).
	Bootstrap bool

	// Timing knobs. Zero falls back to hashicorp/raft's DefaultConfig
	// values (1s heartbeat, 1s election, 50ms commit).
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
	CommitTimeout    time.Duration

	// TestTransport, when non-nil, replaces the production TCP
	// transport. Used by raft.NewInmemTransport-based tests so
	// replicas can talk to each other without binding real ports.
	TestTransport raft.Transport

	// TestLocalAddr is the address the test transport advertises.
	// Used only when TestTransport is non-nil — production reads the
	// advertise address from the TCP transport directly.
	TestLocalAddr raft.ServerAddress
}

// RaftPeer describes one replica in the cluster. NodeID + RaftAddr
// uniquely identify the peer to raft; HTTPAddr is what standbys
// reverse-proxy admin requests to once that peer becomes leader.
type RaftPeer struct {
	NodeID   string
	RaftAddr string // "host:port" for raft RPCs
	HTTPAddr string // "host:port" for the admin API
}

func (c *RaftConfig) validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("%w: NodeID is required", ErrInvalidConfig)
	}
	if len(c.Peers) == 0 {
		return fmt.Errorf("%w: at least one peer is required", ErrInvalidConfig)
	}
	seen := make(map[string]struct{}, len(c.Peers))
	selfFound := false
	for _, p := range c.Peers {
		if p.NodeID == "" {
			return fmt.Errorf("%w: peer is missing NodeID", ErrInvalidConfig)
		}
		if _, dup := seen[p.NodeID]; dup {
			return fmt.Errorf("%w: duplicate peer NodeID %q", ErrInvalidConfig, p.NodeID)
		}
		seen[p.NodeID] = struct{}{}
		if p.RaftAddr == "" {
			return fmt.Errorf("%w: peer %q is missing RaftAddr", ErrInvalidConfig, p.NodeID)
		}
		if p.NodeID == c.NodeID {
			selfFound = true
		}
	}
	if !selfFound {
		return fmt.Errorf("%w: NodeID %q is not present in Peers", ErrInvalidConfig, c.NodeID)
	}
	if c.TestTransport == nil {
		if c.BindAddr == "" {
			return fmt.Errorf("%w: BindAddr is required (or supply TestTransport)", ErrInvalidConfig)
		}
	} else {
		if c.TestLocalAddr == "" {
			return fmt.Errorf("%w: TestLocalAddr is required when TestTransport is set", ErrInvalidConfig)
		}
	}
	return nil
}

// raftCoord is a Coordinator backed by hashicorp/raft. The FSM is a
// no-op — raft is used purely as a fault-tolerant leader-election
// primitive.
type raftCoord struct {
	cfg    RaftConfig
	cb     Callbacks
	log    *slog.Logger
	r      *raft.Raft
	tr     raft.Transport
	leader atomic.Bool

	// onDeposeWait bounds how long the leader-change loop will wait
	// for a previous OnElected goroutine to finish before starting a
	// new transition. raft.LeaderCh's old consumer waited
	// indefinitely, which could freeze the state machine when an
	// OnElected callback (e.g. runLeaderPlane) was slow to wind down.
	// Derived from HeartbeatTimeout so a stuck callback can't pin
	// leadership state longer than the cluster's own heartbeat budget.
	onDeposeWait time.Duration

	// peersByRaftAddr maps a raft address back to its HTTP address.
	// Populated once at construction (peer list is static), so
	// LeaderEndpoint is a pure in-memory lookup.
	peersByRaftAddr map[raft.ServerAddress]string
}

// NewRaft constructs a hashicorp/raft-backed Coordinator. The raft
// log/stable/snapshot stores are in-memory: a full-fleet restart
// re-bootstraps the cluster (membership in cfg.Peers is enough to
// recover), and per-process restarts rejoin via the existing
// transport.
func NewRaft(cfg RaftConfig, cb Callbacks, log *slog.Logger) (Coordinator, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}

	peersByRaftAddr := make(map[raft.ServerAddress]string, len(cfg.Peers))
	for _, p := range cfg.Peers {
		peersByRaftAddr[raft.ServerAddress(p.RaftAddr)] = p.HTTPAddr
	}

	rcfg := raft.DefaultConfig()
	rcfg.LocalID = raft.ServerID(cfg.NodeID)
	rcfg.LogOutput = slogWriter{log: log, level: slog.LevelDebug}
	if cfg.HeartbeatTimeout > 0 {
		rcfg.HeartbeatTimeout = cfg.HeartbeatTimeout
	}
	if cfg.ElectionTimeout > 0 {
		rcfg.ElectionTimeout = cfg.ElectionTimeout
	}
	if cfg.CommitTimeout > 0 {
		rcfg.CommitTimeout = cfg.CommitTimeout
	}
	// LeaderLeaseTimeout must be <= HeartbeatTimeout per raft
	// invariants. When the operator tightens heartbeat we tighten
	// lease the same way so DefaultConfig's 500ms doesn't trip
	// raft's startup validation.
	if rcfg.LeaderLeaseTimeout > rcfg.HeartbeatTimeout {
		rcfg.LeaderLeaseTimeout = rcfg.HeartbeatTimeout
	}

	logs := raft.NewInmemStore()
	stable := raft.NewInmemStore()
	snaps := raft.NewInmemSnapshotStore()

	var (
		tr      raft.Transport
		raftErr error
	)
	if cfg.TestTransport != nil {
		tr = cfg.TestTransport
	} else {
		advertise := cfg.AdvertiseAddr
		if advertise == "" {
			advertise = cfg.BindAddr
		}
		addr, err := net.ResolveTCPAddr("tcp", advertise)
		if err != nil {
			return nil, fmt.Errorf("cluster: resolve advertise %q: %w", advertise, err)
		}
		tr, raftErr = raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, slogWriter{log: log, level: slog.LevelDebug})
		if raftErr != nil {
			return nil, fmt.Errorf("cluster: tcp transport on %s: %w", cfg.BindAddr, raftErr)
		}
	}

	if cfg.Bootstrap {
		hasState, err := raft.HasExistingState(logs, stable, snaps)
		if err != nil {
			return nil, fmt.Errorf("cluster: has-existing-state: %w", err)
		}
		if !hasState {
			servers := make([]raft.Server, 0, len(cfg.Peers))
			for _, p := range cfg.Peers {
				servers = append(servers, raft.Server{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(p.NodeID),
					Address:  raft.ServerAddress(p.RaftAddr),
				})
			}
			if err := raft.BootstrapCluster(rcfg, logs, stable, snaps, tr, raft.Configuration{Servers: servers}); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
				return nil, fmt.Errorf("cluster: bootstrap: %w", err)
			}
		}
	}

	r, err := raft.NewRaft(rcfg, &noopFSM{}, logs, stable, snaps, tr)
	if err != nil {
		return nil, fmt.Errorf("cluster: new-raft: %w", err)
	}

	return &raftCoord{
		cfg:             cfg,
		cb:              cb,
		log:             log,
		r:               r,
		tr:              tr,
		onDeposeWait:    rcfg.HeartbeatTimeout,
		peersByRaftAddr: peersByRaftAddr,
	}, nil
}

func (k *raftCoord) Run(ctx context.Context) error {
	defer func() {
		// Shutdown blocks until the raft loop exits. The 5s cap is a
		// safety net; in practice the loop wraps up well under a
		// second once ctx-driven callers release their resources.
		shutdownDone := make(chan struct{})
		go func() {
			defer close(shutdownDone)
			_ = k.r.Shutdown().Error()
		}()
		select {
		case <-shutdownDone:
		case <-time.After(5 * time.Second):
			k.log.Warn("cluster: raft shutdown exceeded deadline; abandoning")
		}
	}()

	// Translate leader transitions into Callbacks. We use
	// raft.RegisterObserver rather than LeaderCh: LeaderCh is a
	// hidden buffer-1 channel that drops transitions if the receiver
	// is slow (hashicorp/raft docs flag it best-effort). With a
	// properly-sized observer channel we see every relevant change.
	// We don't trust the observation payload — instead we treat each
	// event as a wake-up and read k.r.State() to derive the current
	// truth. That makes the loop idempotent: even if the channel
	// somehow drops one event, the next one converges state again.
	obsCh := make(chan raft.Observation, 16)
	filter := func(o *raft.Observation) bool {
		switch o.Data.(type) {
		case raft.LeaderObservation, raft.RaftState:
			return true
		default:
			return false
		}
	}
	observer := raft.NewObserver(obsCh, false, filter)
	k.r.RegisterObserver(observer)
	defer k.r.DeregisterObserver(observer)

	var (
		leaderCtx    context.Context
		leaderCancel context.CancelFunc
		leaderWG     sync.WaitGroup
		wasLeader    bool
	)

	// waitWithCap awaits leaderWG with a timeout so a slow OnElected
	// (e.g. runLeaderPlane stuck on a 30s GH call) can't freeze the
	// state machine. If the wait deadline is hit we log and proceed —
	// IsLeader() has already flipped, so admin-plane forwarding is
	// already consistent with the new reality.
	waitWithCap := func() {
		if k.onDeposeWait <= 0 {
			leaderWG.Wait()
			return
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			leaderWG.Wait()
		}()
		select {
		case <-done:
		case <-time.After(k.onDeposeWait):
			k.log.Warn("cluster: previous OnElected did not return within onDeposeWait; proceeding",
				"node_id", k.cfg.NodeID, "wait", k.onDeposeWait)
		}
	}

	handle := func() {
		isLeader := k.r.State() == raft.Leader
		if isLeader == wasLeader {
			return
		}
		wasLeader = isLeader
		if isLeader {
			k.leader.Store(true)
			k.log.Info("cluster: became leader",
				"node_id", k.cfg.NodeID,
				"endpoint", k.cfg.localEndpoint())
			leaderCtx, leaderCancel = context.WithCancel(ctx)
			if k.cb.OnElected != nil {
				leaderWG.Add(1)
				lctx := leaderCtx
				go func() {
					defer leaderWG.Done()
					k.cb.OnElected(lctx)
				}()
			}
		} else {
			k.leader.Store(false)
			k.log.Info("cluster: lost leadership", "node_id", k.cfg.NodeID)
			if leaderCancel != nil {
				leaderCancel()
				waitWithCap()
				leaderCtx, leaderCancel = nil, nil
			}
			if k.cb.OnDeposed != nil {
				k.cb.OnDeposed()
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			if leaderCancel != nil {
				leaderCancel()
			}
			// Final, unbounded wait: process is shutting down, so
			// we must reap the OnElected goroutine before Run
			// returns.
			leaderWG.Wait()
			return nil
		case <-obsCh:
			handle()
		}
	}
}

func (k *raftCoord) IsLeader() bool { return k.leader.Load() }

func (k *raftCoord) LeaderEndpoint(_ context.Context) (string, error) {
	// Fast path: this replica is leader. The local endpoint is the
	// most authoritative value — peers learn our address from raft,
	// but we know our own admin host:port without consulting it.
	if k.leader.Load() {
		if ep := k.cfg.localEndpoint(); ep != "" {
			return ep, nil
		}
	}
	addr, _ := k.r.LeaderWithID()
	if addr == "" {
		// Election in flight; tell callers to return 503 + Retry-After.
		return "", nil
	}
	httpAddr, ok := k.peersByRaftAddr[addr]
	if !ok || httpAddr == "" {
		return "", fmt.Errorf("cluster: leader raft addr %q has no matching HTTP peer entry", addr)
	}
	return httpAddr, nil
}

// localEndpoint resolves the HTTP host:port for this replica using
// AdminHost and AdminPort. Falls back to the configured peer entry
// matching NodeID, so operators can declare the HTTPAddr once in the
// peer list without repeating it on the local side.
func (c *RaftConfig) localEndpoint() string {
	if ep := localEndpoint(c.AdminHost, c.AdminPort); ep != "" {
		return ep
	}
	for _, p := range c.Peers {
		if p.NodeID == c.NodeID {
			return p.HTTPAddr
		}
	}
	return ""
}

// noopFSM is the trivial FSM we plug into raft.NewRaft. We use raft
// purely as a leader-election primitive — Apply / Snapshot / Restore
// have nothing to do because no state is being replicated.
type noopFSM struct{}

func (*noopFSM) Apply(*raft.Log) any                  { return nil }
func (*noopFSM) Snapshot() (raft.FSMSnapshot, error)  { return noopSnapshot{}, nil }
func (*noopFSM) Restore(snapshot io.ReadCloser) error { return snapshot.Close() }

type noopSnapshot struct{}

func (noopSnapshot) Persist(sink raft.SnapshotSink) error { return sink.Close() }
func (noopSnapshot) Release()                             {}

// slogWriter adapts an *slog.Logger to the io.Writer hashicorp/raft
// expects for its log output. raft writes single-line, level-prefixed
// records — we strip the prefix and re-emit at the configured slog
// level so production log pipelines stay structured.
type slogWriter struct {
	log   *slog.Logger
	level slog.Level
}

func (s slogWriter) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\n")
	s.log.Log(context.Background(), s.level, msg)
	return len(p), nil
}
