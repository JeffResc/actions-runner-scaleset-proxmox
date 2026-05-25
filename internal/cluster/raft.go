package cluster

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	stdlog "log"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/fileperm"
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

	// DataDir is the on-disk directory where raft persists its log,
	// stable, and snapshot stores. Raft's election-safety invariants
	// (currentTerm, votedFor) require these to survive restart so a
	// node cannot vote twice in the same term. Required in production
	// (TestTransport == nil). Tests may leave it empty to use
	// in-memory stores.
	DataDir string

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
	// safe to leave true on subsequent restarts — with persistent
	// stores raft.HasExistingState returns true after the first start
	// and BootstrapCluster is skipped.
	Bootstrap bool

	// Timing knobs. Zero falls back to hashicorp/raft's DefaultConfig
	// values (1s heartbeat, 1s election, 50ms commit).
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
	CommitTimeout    time.Duration

	// TLS, when non-nil, wraps the raft TCP transport with a TLS
	// stream layer so peer-to-peer raft RPCs are encrypted. The
	// supplied tls.Config is used for BOTH the server-side listener
	// (Certificates + optional ClientCAs / ClientAuth) and the
	// client-side dial (Certificates + RootCAs). Pass the same
	// bundle on every replica.
	TLS *tls.Config

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
		if c.DataDir == "" {
			return fmt.Errorf("%w: DataDir is required in production (raft consensus state must survive restart)", ErrInvalidConfig)
		}
	} else if c.TestLocalAddr == "" {
		return fmt.Errorf("%w: TestLocalAddr is required when TestTransport is set", ErrInvalidConfig)
	}
	return nil
}

// newRaftStores builds the log, stable, and snapshot stores raft
// needs. With persistent=false (test mode — TestTransport is set) the
// stores are in-memory and election state vanishes on restart. With
// persistent=true (production), raft's election-safety invariants
// (currentTerm, votedFor) must survive restart so a node cannot vote
// twice in the same term, even across a partition + crash window.
//
// The returned closer releases the BoltDB file handles; raft.NewRaft
// does not own them, so the caller MUST invoke closer after
// raft.Shutdown completes. The closer is a no-op for the in-memory
// path.
func newRaftStores(persistent bool, dataDir string, log *slog.Logger) (raft.LogStore, raft.StableStore, raft.SnapshotStore, func(), error) {
	if !persistent {
		return raft.NewInmemStore(), raft.NewInmemStore(), raft.NewInmemSnapshotStore(), func() {}, nil
	}
	if err := validateDataDir(dataDir); err != nil {
		return nil, nil, nil, nil, err
	}
	snapDir := filepath.Join(dataDir, "snapshots")
	if err := os.MkdirAll(snapDir, 0o700); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("cluster: create raft snapshot dir %s: %w", snapDir, err)
	}
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "logs.bolt"))
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("cluster: open raft log store: %w", err)
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "stable.bolt"))
	if err != nil {
		_ = logStore.Close()
		return nil, nil, nil, nil, fmt.Errorf("cluster: open raft stable store: %w", err)
	}
	// retain=3 keeps the last three snapshots for operator recovery
	// while keeping the disk footprint bounded.
	snapStore, err := raft.NewFileSnapshotStoreWithLogger(snapDir, 3, newSlogHclog(log, "raft.snapshot"))
	if err != nil {
		_ = logStore.Close()
		_ = stableStore.Close()
		return nil, nil, nil, nil, fmt.Errorf("cluster: open raft snapshot store: %w", err)
	}
	closer := func() {
		if err := logStore.Close(); err != nil {
			log.Warn("cluster: close raft log store", "err", err)
		}
		if err := stableStore.Close(); err != nil {
			log.Warn("cluster: close raft stable store", "err", err)
		}
	}
	return logStore, stableStore, snapStore, closer, nil
}

// validateDataDir enforces that the raft data directory exists with
// strict perms and is owned by the orchestrator's user. Raft's
// authority over leader election, FSM state, and peer membership all
// flows through logs.bolt / stable.bolt / snapshots in this directory;
// a world-writable or other-user-owned data_dir lets a local attacker
// forge cluster state.
//
// First-run behavior: if the directory doesn't exist, it's created
// with 0700 explicitly (not via MkdirAll, which would silently no-op
// against a pre-existing world-writable parent). If it exists, the
// mode + ownership checks from internal/fileperm are reused so the
// raft check follows the same contract as the config-file and PEM
// checks.
func validateDataDir(dataDir string) error {
	info, err := os.Stat(dataDir)
	if errors.Is(err, os.ErrNotExist) {
		if mkErr := os.Mkdir(dataDir, 0o700); mkErr != nil {
			return fmt.Errorf("cluster: create raft data_dir %s: %w", dataDir, mkErr)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("cluster: stat raft data_dir %s: %w", dataDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("cluster: raft data_dir %s exists but is not a directory", dataDir)
	}
	if err := fileperm.CheckMode(info, dataDir, 0o700); err != nil {
		return fmt.Errorf("cluster: raft data_dir: %w", err)
	}
	if err := fileperm.CheckOwnership(info, dataDir); err != nil {
		return fmt.Errorf("cluster: raft data_dir: %w", err)
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

	// closeStores releases the persistent log/stable BoltDB handles
	// after raft.Shutdown completes. raft.NewRaft does not take
	// ownership of the stores, so we must close them ourselves —
	// otherwise a same-process restart (e.g. tests, or a future
	// in-place reload) would block on Bolt's exclusive file lock.
	// Nil when the stores are in-memory.
	closeStores func()

	// peersByRaftAddr maps a raft address back to its HTTP address.
	// Populated once at construction (peer list is static), so
	// LeaderEndpoint is a pure in-memory lookup.
	peersByRaftAddr map[raft.ServerAddress]string
}

// NewRaft constructs a hashicorp/raft-backed Coordinator. In
// production (cfg.DataDir set) the raft log/stable/snapshot stores
// persist to disk under cfg.DataDir — required for election safety
// because currentTerm/votedFor must survive restart. In tests with
// TestTransport set and DataDir empty, the stores are in-memory.
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
	rcfg.Logger = newSlogHclog(log, "raft")
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
	// raft's startup validation. Log a warn when the clamp fires so
	// an operator who shrank heartbeat thinking the 500ms lease
	// still applies can see the actual value in production logs.
	if rcfg.LeaderLeaseTimeout > rcfg.HeartbeatTimeout {
		log.Warn("cluster: clamping LeaderLeaseTimeout to HeartbeatTimeout",
			"original_lease", rcfg.LeaderLeaseTimeout,
			"heartbeat", rcfg.HeartbeatTimeout)
		rcfg.LeaderLeaseTimeout = rcfg.HeartbeatTimeout
	}

	logs, stable, snaps, closeStores, err := newRaftStores(cfg.TestTransport == nil, cfg.DataDir, log)
	if err != nil {
		return nil, err
	}

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
		if cfg.TLS != nil {
			// TLS stream layer: encrypted peer-to-peer raft RPCs.
			// Mutual TLS is enabled when the operator's tls.Config sets
			// ClientCAs + ClientAuth=RequireAndVerifyClientCert (the
			// shape config.TLSConfig.BuildServerTLS produces when
			// CAFile is set).
			ln, lnErr := tls.Listen("tcp", cfg.BindAddr, cfg.TLS)
			if lnErr != nil {
				return nil, fmt.Errorf("cluster: tls listen on %s: %w", cfg.BindAddr, lnErr)
			}
			stream := &tlsStreamLayer{ln: ln, advertise: addr, tlsCfg: cfg.TLS}
			tr = raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
				Stream:  stream,
				MaxPool: 3,
				Timeout: 10 * time.Second,
				Logger:  newSlogHclog(log, "raft.transport"),
			})
		} else {
			tr, raftErr = raft.NewTCPTransportWithLogger(cfg.BindAddr, addr, 3, 10*time.Second, newSlogHclog(log, "raft.transport"))
			if raftErr != nil {
				return nil, fmt.Errorf("cluster: tcp transport on %s: %w", cfg.BindAddr, raftErr)
			}
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
				closeStores()
				return nil, fmt.Errorf("cluster: bootstrap: %w", err)
			}
		}
	}

	r, err := raft.NewRaft(rcfg, &noopFSM{}, logs, stable, snaps, tr)
	if err != nil {
		closeStores()
		return nil, fmt.Errorf("cluster: new-raft: %w", err)
	}

	return &raftCoord{
		cfg:             cfg,
		cb:              cb,
		closeStores:     closeStores,
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
		// time.NewTimer (not time.After) so the timer is GC'd on
		// the happy path. time.After's runtime timer dangles for
		// the full 5s on every clean shutdown — harmless once, but
		// onDeposeWait below uses the same pattern in a leadership-
		// flap path that can fire frequently.
		shutdownTimer := time.NewTimer(5 * time.Second)
		defer shutdownTimer.Stop()
		select {
		case <-shutdownDone:
		case <-shutdownTimer.C:
			k.log.Warn("cluster: raft shutdown exceeded deadline; abandoning")
		}
		// Close BoltDB handles AFTER raft.Shutdown returns — raft
		// keeps reading/writing the log/stable stores until then.
		if k.closeStores != nil {
			k.closeStores()
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
		deposeTimer := time.NewTimer(k.onDeposeWait)
		defer deposeTimer.Stop()
		select {
		case <-done:
		case <-deposeTimer.C:
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

// tlsStreamLayer satisfies raft.StreamLayer with TLS-terminated TCP.
// The wrapped tls.Listener is consumed by raft.NewNetworkTransportWithConfig
// for inbound RPCs; Dial wraps an outbound TCP connection with
// tls.Client using the same bundle. Pinning RootCAs (one-way TLS) or
// ClientCAs + RequireAndVerifyClientCert (mTLS) is the operator's
// choice and is enforced by the supplied tls.Config.
type tlsStreamLayer struct {
	ln        net.Listener
	advertise net.Addr
	tlsCfg    *tls.Config
}

// Accept blocks until a peer dials in or the listener is closed.
func (s *tlsStreamLayer) Accept() (net.Conn, error) { return s.ln.Accept() }

// Close shuts down the listener; raft calls this on Shutdown.
func (s *tlsStreamLayer) Close() error { return s.ln.Close() }

// Addr returns the advertise address so raft can publish it to peers.
// Returning the listener's local Addr would expose 0.0.0.0:port which
// other replicas can't dial when BindAddr is a wildcard.
func (s *tlsStreamLayer) Addr() net.Addr { return s.advertise }

// Dial opens an outbound TLS connection. The timeout caps the whole
// TCP + TLS handshake budget — without it a hung peer would block
// raft's call for the OS-level connect timeout (~75s).
//
// raft.StreamLayer.Dial is context-free, so we synthesize one from the
// timeout for the modern tls.Dialer.DialContext path.
func (s *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	d := tls.Dialer{NetDialer: &net.Dialer{}, Config: s.tlsCfg}
	return d.DialContext(ctx, "tcp", string(address))
}

// slogHclog implements hclog.Logger by dispatching to slog at the
// matching level. raft / raft-tcp / file-snapshot-store all accept an
// hclog.Logger; this is the bridge so production log pipelines see
// raft's own level tagging rather than every record at Debug.
type slogHclog struct {
	log  *slog.Logger
	name string
	// implied is hclog's "With" / "Named" carrier — every value added
	// via those helpers is appended to subsequent records.
	implied []any
}

func newSlogHclog(log *slog.Logger, name string) hclog.Logger {
	return &slogHclog{log: log, name: name}
}

func (s *slogHclog) Log(level hclog.Level, msg string, args ...any) {
	s.log.Log(context.Background(), hclogLevelToSlog(level), msg, s.combine(args)...)
}

func (s *slogHclog) Trace(msg string, args ...any) { s.Log(hclog.Trace, msg, args...) }
func (s *slogHclog) Debug(msg string, args ...any) { s.Log(hclog.Debug, msg, args...) }
func (s *slogHclog) Info(msg string, args ...any)  { s.Log(hclog.Info, msg, args...) }
func (s *slogHclog) Warn(msg string, args ...any)  { s.Log(hclog.Warn, msg, args...) }
func (s *slogHclog) Error(msg string, args ...any) { s.Log(hclog.Error, msg, args...) }

func (s *slogHclog) IsTrace() bool { return s.log.Enabled(context.Background(), slog.LevelDebug) }
func (s *slogHclog) IsDebug() bool { return s.log.Enabled(context.Background(), slog.LevelDebug) }
func (s *slogHclog) IsInfo() bool  { return s.log.Enabled(context.Background(), slog.LevelInfo) }
func (s *slogHclog) IsWarn() bool  { return s.log.Enabled(context.Background(), slog.LevelWarn) }
func (s *slogHclog) IsError() bool { return s.log.Enabled(context.Background(), slog.LevelError) }

func (s *slogHclog) ImpliedArgs() []any { return s.implied }

func (s *slogHclog) With(args ...any) hclog.Logger {
	cp := *s
	cp.implied = append(append([]any{}, s.implied...), args...)
	return &cp
}

func (s *slogHclog) Name() string { return s.name }

func (s *slogHclog) Named(name string) hclog.Logger {
	cp := *s
	if s.name != "" {
		cp.name = s.name + "." + name
	} else {
		cp.name = name
	}
	return &cp
}

func (s *slogHclog) ResetNamed(name string) hclog.Logger {
	cp := *s
	cp.name = name
	return &cp
}

// SetLevel is a no-op — slog handlers decide their own level filtering
// at construction time; hclog's runtime level changes don't translate.
func (s *slogHclog) SetLevel(hclog.Level) {}
func (s *slogHclog) GetLevel() hclog.Level {
	switch {
	case s.IsDebug():
		return hclog.Debug
	case s.IsInfo():
		return hclog.Info
	case s.IsWarn():
		return hclog.Warn
	default:
		return hclog.Error
	}
}

func (s *slogHclog) StandardLogger(*hclog.StandardLoggerOptions) *stdlog.Logger {
	return stdlog.New(s.StandardWriter(nil), "", 0)
}

func (s *slogHclog) StandardWriter(*hclog.StandardLoggerOptions) io.Writer {
	return io.Discard
}

func (s *slogHclog) combine(args []any) []any {
	if len(s.implied) == 0 {
		return args
	}
	out := make([]any, 0, len(s.implied)+len(args))
	out = append(out, s.implied...)
	out = append(out, args...)
	return out
}

func hclogLevelToSlog(l hclog.Level) slog.Level {
	switch l {
	case hclog.Trace, hclog.Debug, hclog.NoLevel:
		return slog.LevelDebug
	case hclog.Info:
		return slog.LevelInfo
	case hclog.Warn:
		return slog.LevelWarn
	case hclog.Error, hclog.Off:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
