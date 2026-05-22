package cluster

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
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

func TestRaft_SingleNodeElectsSelf(t *testing.T) {
	t.Parallel()

	rc := newRaftCluster(t, 1)
	require.Eventually(t, func() bool { return rc.coords[0].IsLeader() },
		3*time.Second, 20*time.Millisecond, "single-node cluster never elected itself")
	require.Eventually(t, rc.elected[0].Load,
		2*time.Second, 20*time.Millisecond, "OnElected never fired")
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

	dataDir := t.TempDir()

	// Reserve a real ephemeral port and immediately release it; raft
	// will re-open it. There's a tiny race here (the port could be
	// stolen between Close and raft binding) but it's vanishingly
	// rare on a developer machine and zero-cost on CI.
	l, err := net.Listen("tcp", "127.0.0.1:0")
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

	var gotXFF string
	var gotPath string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotXFF = r.Header.Get("X-Forwarded-For")
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello from leader"))
	}))
	defer upstream.Close()

	endpoint := strings.TrimPrefix(upstream.URL, "http://")
	fwd := NewForwarder(&fakeCoord{endpoint: endpoint})

	srv := httptest.NewServer(fwd)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/admin/state", strings.NewReader(`{"q":1}`))
	require.NoError(t, err)
	req.Header.Set("X-Forwarded-For", "203.0.113.10") // simulate Ingress-set header

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "hello from leader", string(body))
	require.Equal(t, "/admin/state", gotPath)
	// httputil.ReverseProxy appends the direct peer; we should see the
	// original X-Forwarded-For preserved at the front.
	require.True(t, strings.HasPrefix(gotXFF, "203.0.113.10"),
		"X-Forwarded-For should preserve the original client IP, got %q", gotXFF)
}

func TestForwarder_NoLeaderReturns503(t *testing.T) {
	t.Parallel()

	fwd := NewForwarder(&fakeCoord{endpoint: ""})
	srv := httptest.NewServer(fwd)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/admin/state")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Equal(t, "2", resp.Header.Get("Retry-After"))
}

func TestForwarder_LeaderUnreachableReturns502(t *testing.T) {
	t.Parallel()

	// Point at a port nobody listens on so the upstream dial fails.
	fwd := NewForwarder(&fakeCoord{endpoint: "127.0.0.1:1"})
	srv := httptest.NewServer(fwd)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/admin/state")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

func TestForwarder_LookupErrorTreatedAsNoLeader(t *testing.T) {
	t.Parallel()

	fwd := NewForwarder(&fakeCoord{err: errors.New("lookup failed")})
	srv := httptest.NewServer(fwd)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}
