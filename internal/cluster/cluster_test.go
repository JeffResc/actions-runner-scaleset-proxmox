package cluster

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// tightTempDir is t.TempDir() chmodded to 0700 so validateDataDir
// accepts it. t.TempDir() inherits the OS default (typically 0755);
// the raft data_dir security check demands strict perms.
func tightTempDir(t *testing.T) string {
	t.Helper()
	d := t.TempDir()
	require.NoError(t, os.Chmod(d, 0o700))
	return d
}

// ---------------------------------------------------------------------------
// Standalone
// ---------------------------------------------------------------------------

func TestStandalone_OnElectedAndDeposed(t *testing.T) {
	t.Parallel()

	var elected atomic.Bool
	var deposed atomic.Bool
	var sawLeaderCtxCancel atomic.Bool

	cb := Callbacks{
		OnElected: func(ctx context.Context) {
			elected.Store(true)
			<-ctx.Done()
			sawLeaderCtxCancel.Store(true)
		},
		OnDeposed: func() { deposed.Store(true) },
	}

	coord := NewStandalone("127.0.0.1:9101", cb)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- coord.Run(ctx) }()

	require.Eventually(t, elected.Load, time.Second, 5*time.Millisecond, "OnElected never fired")
	require.True(t, coord.IsLeader())

	ep, err := coord.LeaderEndpoint(context.Background())
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:9101", ep)

	cancel()
	require.NoError(t, <-done)
	require.True(t, sawLeaderCtxCancel.Load(), "leader context wasn't cancelled on Run return")
	require.True(t, deposed.Load(), "OnDeposed never fired")
	require.False(t, coord.IsLeader())
}

func TestStandalone_NilCallbacksOK(t *testing.T) {
	t.Parallel()

	coord := NewStandalone("", Callbacks{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- coord.Run(ctx) }()
	// Brief sleep to let Run reach the <-ctx.Done() blocking point.
	time.Sleep(20 * time.Millisecond)
	require.True(t, coord.IsLeader())
	cancel()
	require.NoError(t, <-done)
}

// ---------------------------------------------------------------------------
// Raft
// ---------------------------------------------------------------------------

// raftCluster spins up N replicas wired together via raft.InmemTransport
// so the test never touches a real socket.
type raftCluster struct {
	coords     []Coordinator
	cancels    []context.CancelFunc
	dones      []chan error
	transports []*raft.InmemTransport
	addrs      []raft.ServerAddress
	elected    []*atomic.Bool
	deposed    []*atomic.Bool
}

// newRaftCluster constructs an n-replica raft cluster using InmemTransport.
// Replica 0 is the bootstrapper; the others join via the transport's
// peer-discovery on top of the bootstrapped configuration.
func newRaftCluster(t *testing.T, n int) *raftCluster {
	t.Helper()
	rc := &raftCluster{
		coords:     make([]Coordinator, n),
		cancels:    make([]context.CancelFunc, n),
		dones:      make([]chan error, n),
		transports: make([]*raft.InmemTransport, n),
		addrs:      make([]raft.ServerAddress, n),
		elected:    make([]*atomic.Bool, n),
		deposed:    make([]*atomic.Bool, n),
	}
	for i := 0; i < n; i++ {
		addr, tr := raft.NewInmemTransport("")
		rc.transports[i] = tr
		rc.addrs[i] = addr
	}
	// Wire every transport to every other transport so peers can reach
	// each other.
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			rc.transports[i].Connect(rc.addrs[j], rc.transports[j])
		}
	}

	peers := make([]RaftPeer, n)
	for i := 0; i < n; i++ {
		peers[i] = RaftPeer{
			NodeID:   nodeID(i),
			RaftAddr: string(rc.addrs[i]),
			HTTPAddr: httpAddr(i),
		}
	}

	for i := 0; i < n; i++ {
		var elected, deposed atomic.Bool
		rc.elected[i] = &elected
		rc.deposed[i] = &deposed
		cb := Callbacks{
			OnElected: func(ctx context.Context) {
				elected.Store(true)
				<-ctx.Done()
			},
			OnDeposed: func() { deposed.Store(true) },
		}
		cfg := RaftConfig{
			NodeID:           nodeID(i),
			AdminHost:        "127.0.0.1",
			AdminPort:        9100 + i,
			Peers:            peers,
			Bootstrap:        i == 0, // only replica 0 bootstraps
			HeartbeatTimeout: 50 * time.Millisecond,
			ElectionTimeout:  50 * time.Millisecond,
			CommitTimeout:    10 * time.Millisecond,
			TestTransport:    rc.transports[i],
			TestLocalAddr:    rc.addrs[i],
		}
		coord, err := NewRaft(cfg, cb, discardLogger())
		require.NoError(t, err)
		rc.coords[i] = coord

		ctx, cancel := context.WithCancel(context.Background())
		rc.cancels[i] = cancel
		done := make(chan error, 1)
		go func(c Coordinator) { done <- c.Run(ctx) }(coord)
		rc.dones[i] = done
	}
	t.Cleanup(func() { rc.shutdown() })
	return rc
}

func (rc *raftCluster) shutdown() {
	for i, cancel := range rc.cancels {
		if cancel == nil {
			continue
		}
		cancel()
		select {
		case <-rc.dones[i]:
		case <-time.After(5 * time.Second):
		}
		rc.cancels[i] = nil
	}
}

func nodeID(i int) string   { return "node-" + strconv.Itoa(i) }
func httpAddr(i int) string { return "10.0.0." + strconv.Itoa(i+1) + ":9100" }

// TestRaft_TLSTransport_ElectsLeader brings up a single-node raft
// cluster with a TLS stream layer (in-process self-signed loopback
// cert + mTLS) and asserts the node still elects itself. Guards the
// #64B fix: with TLS configured, raft RPCs must continue to work and
// the listener must actually use the TLS-wrapped TCP socket.
func TestRaft_TLSTransport_ElectsLeader(t *testing.T) {
	t.Parallel()

	certPEM, keyPEM := genSelfSignedCert(t)
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(certPEM))
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ServerName:   "127.0.0.1",
		MinVersion:   tls.VersionTLS12,
	}

	// Reserve an ephemeral port for the raft bind.
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	cfg := RaftConfig{
		NodeID: "n0",
		Peers: []RaftPeer{
			{NodeID: "n0", RaftAddr: addr, HTTPAddr: "127.0.0.1:9100"},
		},
		BindAddr:         addr,
		AdvertiseAddr:    addr,
		DataDir:          tightTempDir(t),
		Bootstrap:        true,
		HeartbeatTimeout: 50 * time.Millisecond,
		ElectionTimeout:  50 * time.Millisecond,
		CommitTimeout:    10 * time.Millisecond,
		TLS:              tlsCfg,
	}
	c, err := NewRaft(cfg, Callbacks{}, discardLogger())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	require.Eventually(t, c.IsLeader, 3*time.Second, 25*time.Millisecond,
		"single-node TLS raft must still elect itself as leader")

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("raft did not shut down within 3s")
	}
}

// genSelfSignedCert returns a PEM-encoded ECDSA cert + key valid for
// 127.0.0.1 / ::1 / localhost. Standalone helper so this test doesn't
// import the config-package test fixtures.
func genSelfSignedCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

func TestRaft_SingleNodeElectsSelf(t *testing.T) {
	t.Parallel()

	rc := newRaftCluster(t, 1)
	require.Eventually(t, func() bool { return rc.coords[0].IsLeader() },
		3*time.Second, 20*time.Millisecond, "single-node cluster never elected itself")
	require.Eventually(t, rc.elected[0].Load,
		2*time.Second, 20*time.Millisecond, "OnElected never fired")
}

// TestRaft_LeaderLeaseClampLogsWarn locks in the #70 fix: silently
// clamping LeaderLeaseTimeout to HeartbeatTimeout was surprising. With
// a 50ms heartbeat (newRaftCluster's default), DefaultConfig's 500ms
// lease has to be clamped down — and the operator now sees a warn
// line with both the original and clamped values.
func TestRaft_LeaderLeaseClampLogsWarn(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	mu := sync.Mutex{}
	w := &syncBuf{w: &buf, mu: &mu}
	log := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo}))

	addr, tr := raft.NewInmemTransport("")
	cfg := RaftConfig{
		NodeID: "n0",
		Peers: []RaftPeer{
			{NodeID: "n0", RaftAddr: string(addr), HTTPAddr: "127.0.0.1:9100"},
		},
		Bootstrap:        true,
		HeartbeatTimeout: 50 * time.Millisecond, // forces the clamp
		TestTransport:    tr,
		TestLocalAddr:    addr,
	}
	c, err := NewRaft(cfg, Callbacks{}, log)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	cancel()
	<-done

	mu.Lock()
	out := buf.String()
	mu.Unlock()
	require.Contains(t, out, "clamping LeaderLeaseTimeout to HeartbeatTimeout",
		"clamp must emit a warn line; got logs:\n%s", out)
	require.Contains(t, out, "heartbeat=50ms",
		"warn must include the clamped value; got logs:\n%s", out)
	require.Contains(t, out, "original_lease=",
		"warn must include the original value for operator triage; got logs:\n%s", out)
}

// syncBuf wraps a *bytes.Buffer with a mutex so the slog handler
// goroutine and the test goroutine can read/write without -race
// flagging the underlying append.
type syncBuf struct {
	w  *bytes.Buffer
	mu *sync.Mutex
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

// TestSlogHclog_LevelMapping locks in the #71 fix: raft's own
// INFO/WARN/ERROR records must surface at the matching slog level so
// production log pipelines (info-and-up by default) actually see
// election failures and peer-connect problems. The previous io.Writer
// adapter collapsed every raft line to Debug, leaving operators with
// zero raft diagnostics under the default log level.
func TestSlogHclog_LevelMapping(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	mu := sync.Mutex{}
	log := slog.New(slog.NewTextHandler(&syncBuf{w: &buf, mu: &mu}, &slog.HandlerOptions{Level: slog.LevelDebug}))

	h := newSlogHclog(log, "raft")
	h.Trace("trace-line")
	h.Debug("debug-line")
	h.Info("info-line")
	h.Warn("warn-line")
	h.Error("error-line")

	mu.Lock()
	out := buf.String()
	mu.Unlock()

	require.Contains(t, out, "level=DEBUG msg=trace-line")
	require.Contains(t, out, "level=DEBUG msg=debug-line")
	require.Contains(t, out, "level=INFO msg=info-line")
	require.Contains(t, out, "level=WARN msg=warn-line")
	require.Contains(t, out, "level=ERROR msg=error-line")
}

// TestSlogHclog_NamedAndWith confirms hclog's Named and With helpers
// (raft calls them when annotating subsystem and key-value pairs) land
// on the slog output. Without these survival points the adapter would
// silently drop raft's structured fields.
func TestSlogHclog_NamedAndWith(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	mu := sync.Mutex{}
	log := slog.New(slog.NewTextHandler(&syncBuf{w: &buf, mu: &mu}, &slog.HandlerOptions{Level: slog.LevelInfo}))

	base := newSlogHclog(log, "raft")
	sub := base.Named("transport").With("peer", "n0")
	sub.Info("dialed")

	mu.Lock()
	out := buf.String()
	mu.Unlock()
	require.Contains(t, out, "msg=dialed")
	require.Contains(t, out, "peer=n0")
}

func TestRaft_ThreeNodesElectExactlyOneLeader(t *testing.T) {
	t.Parallel()

	rc := newRaftCluster(t, 3)
	require.Eventually(t, func() bool {
		leaders := 0
		for _, c := range rc.coords {
			if c.IsLeader() {
				leaders++
			}
		}
		return leaders == 1
	}, 5*time.Second, 50*time.Millisecond, "expected exactly 1 leader across 3 replicas")
}

func TestRaft_LeaderEndpointResolvesPeerHTTPAddr(t *testing.T) {
	t.Parallel()

	rc := newRaftCluster(t, 3)
	// Wait for a leader to emerge.
	leaderIdx := -1
	require.Eventually(t, func() bool {
		for i, c := range rc.coords {
			if c.IsLeader() {
				leaderIdx = i
				return true
			}
		}
		return false
	}, 5*time.Second, 50*time.Millisecond, "no leader emerged")

	// Every follower's LeaderEndpoint resolves to the leader's HTTP
	// peer entry (httpAddr(leaderIdx)). The leader's own
	// LeaderEndpoint returns its local fast-path address.
	for i, c := range rc.coords {
		ep, err := c.LeaderEndpoint(context.Background())
		require.NoError(t, err)
		if i == leaderIdx {
			require.Equal(t, "127.0.0.1:"+strconv.Itoa(9100+i), ep,
				"leader %d should return its own local endpoint", i)
		} else {
			require.Equal(t, httpAddr(leaderIdx), ep,
				"follower %d should resolve leader to peer http addr", i)
		}
	}
}

func TestRaft_LeaderEndpointEmptyBeforeElection(t *testing.T) {
	t.Parallel()

	// Build a 3-node cluster but DON'T bootstrap any — election will
	// never converge, so LeaderEndpoint must return "" cleanly (no
	// error) so callers respond 503 + Retry-After.
	transports := make([]*raft.InmemTransport, 3)
	addrs := make([]raft.ServerAddress, 3)
	for i := 0; i < 3; i++ {
		addr, tr := raft.NewInmemTransport("")
		transports[i] = tr
		addrs[i] = addr
	}
	peers := make([]RaftPeer, 3)
	for i := 0; i < 3; i++ {
		peers[i] = RaftPeer{
			NodeID:   nodeID(i),
			RaftAddr: string(addrs[i]),
			HTTPAddr: httpAddr(i),
		}
	}
	cfg := RaftConfig{
		NodeID:           nodeID(0),
		AdminHost:        "127.0.0.1",
		AdminPort:        9100,
		Peers:            peers,
		Bootstrap:        false, // deliberately don't bootstrap
		HeartbeatTimeout: 50 * time.Millisecond,
		ElectionTimeout:  50 * time.Millisecond,
		CommitTimeout:    10 * time.Millisecond,
		TestTransport:    transports[0],
		TestLocalAddr:    addrs[0],
	}
	coord, err := NewRaft(cfg, Callbacks{}, discardLogger())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- coord.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	ep, err := coord.LeaderEndpoint(context.Background())
	require.NoError(t, err)
	require.Empty(t, ep, "should return empty (election in flight) rather than an error")
}

// TestRaft_LeadershipTransferFiresOnElectedOnNewLeader exercises the
// switch from raft.LeaderCh (best-effort, hidden buffer-1 channel that
// can drop transitions) to raft.RegisterObserver. After a deliberate
// LeadershipTransfer, the new leader's OnElected must fire and the
// old leader's OnDeposed must fire — no transition may be silently
// dropped.
func TestRaft_LeadershipTransferFiresOnElectedOnNewLeader(t *testing.T) {
	t.Parallel()

	rc := newRaftCluster(t, 3)

	// Wait for an initial leader to emerge.
	leaderIdx := -1
	require.Eventually(t, func() bool {
		for i, c := range rc.coords {
			if c.IsLeader() {
				leaderIdx = i
				return true
			}
		}
		return false
	}, 5*time.Second, 50*time.Millisecond, "no initial leader emerged")

	// Pick a transfer target that is NOT the current leader, reset
	// its elected flag so we can detect the next OnElected, and
	// reset the current leader's deposed flag too.
	targetIdx := (leaderIdx + 1) % 3
	rc.elected[targetIdx].Store(false)
	rc.deposed[leaderIdx].Store(false)

	rcoord, ok := rc.coords[leaderIdx].(*raftCoord)
	require.True(t, ok, "type assertion to *raftCoord failed")

	fut := rcoord.r.LeadershipTransferToServer(
		raft.ServerID(nodeID(targetIdx)),
		rc.addrs[targetIdx],
	)
	require.NoError(t, fut.Error(), "LeadershipTransferToServer failed")

	require.Eventually(t, rc.coords[targetIdx].IsLeader,
		3*time.Second, 50*time.Millisecond, "transfer target never became leader")
	require.Eventually(t, rc.elected[targetIdx].Load,
		2*time.Second, 50*time.Millisecond, "OnElected never fired on transfer target")

	require.Eventually(t, func() bool { return !rc.coords[leaderIdx].IsLeader() },
		2*time.Second, 50*time.Millisecond, "old leader did not lose leadership flag")
	require.Eventually(t, rc.deposed[leaderIdx].Load,
		2*time.Second, 50*time.Millisecond, "OnDeposed never fired on the old leader")
}

// TestRaft_PersistentStateSurvivesRestart spins a single-node raft
// against a persistent DataDir, lets it elect itself, shuts it down,
// then re-constructs it pointed at the same DataDir without
// Bootstrap. Raft must detect the existing state (currentTerm /
// votedFor / configuration) and elect itself again — this is the
// election-safety invariant that in-memory stores violated and that
// #57 was filed to fix.
//
// Uses a real TCP transport on a 127.0.0.1 ephemeral port so the
// BoltDB-backed path actually runs (the in-mem TestTransport path
// uses in-memory stores by design).
func TestRaft_PersistentStateSurvivesRestart(t *testing.T) {
	t.Parallel()

	dataDir := tightTempDir(t)

	// Reserve a real ephemeral port and immediately release it; raft
	// will re-open it. There's a tiny race here (the port could be
	// stolen between Close and raft binding) but it's vanishingly
	// rare on a developer machine and zero-cost on CI.
	var lc net.ListenConfig
	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	bindAddr := l.Addr().String()
	require.NoError(t, l.Close())

	peer := RaftPeer{
		NodeID:   "solo",
		RaftAddr: bindAddr,
		HTTPAddr: "127.0.0.1:9100",
	}

	build := func(bootstrap bool) Coordinator {
		t.Helper()
		cfg := RaftConfig{
			NodeID:           "solo",
			BindAddr:         bindAddr,
			AdvertiseAddr:    bindAddr,
			AdminHost:        "127.0.0.1",
			AdminPort:        9100,
			DataDir:          dataDir,
			Peers:            []RaftPeer{peer},
			Bootstrap:        bootstrap,
			HeartbeatTimeout: 100 * time.Millisecond,
			ElectionTimeout:  100 * time.Millisecond,
			CommitTimeout:    20 * time.Millisecond,
		}
		coord, err := NewRaft(cfg, Callbacks{}, discardLogger())
		require.NoError(t, err)
		return coord
	}

	// First run: bootstrap a single-node cluster, observe leader, shut down.
	{
		coord := build(true)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- coord.Run(ctx) }()

		require.Eventually(t, coord.IsLeader, 3*time.Second, 20*time.Millisecond,
			"first run never elected itself")

		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("first run did not shut down cleanly")
		}
	}

	// Second run: same DataDir, Bootstrap=false. Raft must recover
	// state from BoltDB + the snapshot dir and elect itself without
	// being told to bootstrap.
	{
		coord := build(false)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- coord.Run(ctx) }()
		t.Cleanup(func() {
			cancel()
			<-done
		})

		require.Eventually(t, coord.IsLeader, 3*time.Second, 20*time.Millisecond,
			"second run did not re-elect from persistent state")
	}
}

func TestRaftConfig_ValidationRejectsBadInputs(t *testing.T) {
	t.Parallel()

	base := RaftConfig{
		NodeID:        "a",
		BindAddr:      "0.0.0.0:7000",
		DataDir:       "/var/lib/scaleset/raft",
		Peers:         []RaftPeer{{NodeID: "a", RaftAddr: "10.0.0.1:7000"}},
		TestTransport: nil,
	}

	// Happy path.
	require.NoError(t, base.validate())

	// Production mode (no TestTransport) without DataDir.
	bad := base
	bad.DataDir = ""
	require.Error(t, bad.validate())

	// Missing NodeID.
	bad = base
	bad.NodeID = ""
	require.Error(t, bad.validate())

	// Empty Peers.
	bad = base
	bad.Peers = nil
	require.Error(t, bad.validate())

	// Self not in Peers.
	bad = base
	bad.Peers = []RaftPeer{{NodeID: "other", RaftAddr: "10.0.0.2:7000"}}
	require.Error(t, bad.validate())

	// Duplicate NodeID.
	bad = base
	bad.Peers = []RaftPeer{
		{NodeID: "a", RaftAddr: "10.0.0.1:7000"},
		{NodeID: "a", RaftAddr: "10.0.0.2:7000"},
	}
	require.Error(t, bad.validate())

	// Missing peer RaftAddr.
	bad = base
	bad.Peers = []RaftPeer{{NodeID: "a", RaftAddr: ""}}
	require.Error(t, bad.validate())

	// Production mode (no TestTransport) without BindAddr.
	bad = base
	bad.BindAddr = ""
	require.Error(t, bad.validate())
}

// ---------------------------------------------------------------------------
// Forwarder
// ---------------------------------------------------------------------------

// fakeCoord is a Coordinator for forwarder tests — no election, just
// fixed answers.
type fakeCoord struct {
	endpoint string
	err      error
}

func (f *fakeCoord) Run(_ context.Context) error { return nil }
func (f *fakeCoord) IsLeader() bool              { return false }
func (f *fakeCoord) LeaderEndpoint(_ context.Context) (string, error) {
	return f.endpoint, f.err
}

func TestForwarder_RoutesToLeader(t *testing.T) {
	t.Parallel()

	var gotXFF, gotXRealIP, gotTrueClient string
	var gotPath string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotXFF = r.Header.Get("X-Forwarded-For")
		gotXRealIP = r.Header.Get("X-Real-IP")
		gotTrueClient = r.Header.Get("True-Client-IP")
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello from leader"))
	}))
	defer upstream.Close()

	endpoint := strings.TrimPrefix(upstream.URL, "http://")
	fwd := NewForwarder(&fakeCoord{endpoint: endpoint}, nil)

	srv := httptest.NewServer(fwd)
	defer srv.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/admin/state", strings.NewReader(`{"q":1}`))
	require.NoError(t, err)
	// Simulate a hostile client attempting to spoof the source IP via
	// each of the three commonly-trusted forwarded-for header variants.
	req.Header.Set("X-Forwarded-For", "203.0.113.10")
	req.Header.Set("X-Real-IP", "203.0.113.11")
	req.Header.Set("True-Client-IP", "203.0.113.12")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "hello from leader", string(body))
	require.Equal(t, "/admin/state", gotPath)

	// Forwarder must strip client-supplied source-IP headers before
	// delegating. httputil.ReverseProxy then sets a fresh
	// X-Forwarded-For containing only the standby's connection peer
	// (loopback in this test). A hostile client cannot inject
	// 203.0.113.* through to the leader.
	require.NotContains(t, gotXFF, "203.0.113.10",
		"X-Forwarded-For must not contain the spoofed client value, got %q", gotXFF)
	require.Empty(t, gotXRealIP, "X-Real-IP must be stripped")
	require.Empty(t, gotTrueClient, "True-Client-IP must be stripped")
	require.NotEmpty(t, gotXFF, "ReverseProxy should still set XFF to the standby's peer")
}

func TestForwarder_NoLeaderReturns503(t *testing.T) {
	t.Parallel()

	fwd := NewForwarder(&fakeCoord{endpoint: ""}, nil)
	srv := httptest.NewServer(fwd)
	defer srv.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/admin/state", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Equal(t, "2", resp.Header.Get("Retry-After"))
}

func TestForwarder_LeaderUnreachableReturns502(t *testing.T) {
	t.Parallel()

	// Point at a port nobody listens on so the upstream dial fails.
	fwd := NewForwarder(&fakeCoord{endpoint: "127.0.0.1:1"}, nil)
	srv := httptest.NewServer(fwd)
	defer srv.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/admin/state", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

func TestForwarder_LookupErrorTreatedAsNoLeader(t *testing.T) {
	t.Parallel()

	fwd := NewForwarder(&fakeCoord{err: errors.New("lookup failed")}, nil)
	srv := httptest.NewServer(fwd)
	defer srv.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// TestForwarder_TLSDialsHTTPS: with a non-nil tlsClient, the Forwarder
// must dial the leader over https — proving the bearer-token-bearing
// admin request travels encrypted between standbys and leader.
func TestForwarder_TLSDialsHTTPS(t *testing.T) {
	t.Parallel()

	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// httptest.NewTLSServer-served handlers only run if TLS
		// completed; that's the assertion.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	endpoint := strings.TrimPrefix(upstream.URL, "https://")
	// Use the test server's own TLS config; it pins the self-signed
	// cert so the Forwarder can dial successfully.
	clientTLS := upstream.Client().Transport.(*http.Transport).TLSClientConfig.Clone()

	fwd := NewForwarder(&fakeCoord{endpoint: endpoint}, clientTLS)
	srv := httptest.NewServer(fwd)
	defer srv.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/admin/state", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "Forwarder must successfully dial the https upstream")
	require.Equal(t, "ok", string(body))
}

// TestForwarder_TLSRefusesPlainUpstream: a TLS Forwarder talking to a
// plain-HTTP upstream must fail at the transport layer rather than
// silently downgrade. The plain-http upstream's response would never be
// seen by a TLS client, so the Forwarder returns 502 via errorHandler.
func TestForwarder_TLSRefusesPlainUpstream(t *testing.T) {
	t.Parallel()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	endpoint := strings.TrimPrefix(upstream.URL, "http://")

	clientTLS := &tls.Config{MinVersion: tls.VersionTLS12} //nolint:gosec // test only
	fwd := NewForwarder(&fakeCoord{endpoint: endpoint}, clientTLS)
	srv := httptest.NewServer(fwd)
	defer srv.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/admin/state", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadGateway, resp.StatusCode,
		"TLS Forwarder must NOT downgrade to plain http")
}
