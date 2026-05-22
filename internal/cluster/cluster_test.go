package cluster

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	coordv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
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
// Kubernetes (Lease-backed)
// ---------------------------------------------------------------------------

func newTestKubeCoord(t *testing.T, identity string, port int) (Coordinator, *fake.Clientset, *atomic.Bool, *atomic.Bool) {
	t.Helper()
	client := fake.NewSimpleClientset()
	var elected atomic.Bool
	var deposed atomic.Bool
	cfg := Config{
		LeaseName:      "scaleset-test",
		LeaseNamespace: "ns",
		Identity:       identity,
		PodIP:          "10.0.0.1",
		AdminPort:      port,
		LeaseDuration:  200 * time.Millisecond,
		RenewDeadline:  100 * time.Millisecond,
		RetryPeriod:    20 * time.Millisecond,
	}
	cb := Callbacks{
		OnElected: func(ctx context.Context) {
			elected.Store(true)
			<-ctx.Done()
		},
		OnDeposed: func() { deposed.Store(true) },
	}
	return NewKubernetesWithClient(cfg, cb, discardLogger(), client), client, &elected, &deposed
}

func TestKubernetes_WinsElectionAndPublishesEndpoint(t *testing.T) {
	t.Parallel()

	coord, client, elected, deposed := newTestKubeCoord(t, "pod-1", 9101)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- coord.Run(ctx) }()

	require.Eventually(t, elected.Load, 2*time.Second, 10*time.Millisecond, "OnElected never fired")
	require.True(t, coord.IsLeader())

	// Endpoint is published into the Lease annotation.
	require.Eventually(t, func() bool {
		lease, err := client.CoordinationV1().Leases("ns").Get(context.Background(), "scaleset-test", metav1.GetOptions{})
		if err != nil {
			return false
		}
		return lease.Annotations[DefaultEndpointAnnotation] == "10.0.0.1:9101"
	}, 2*time.Second, 10*time.Millisecond, "leader endpoint annotation never appeared")

	// Local fast path returns the local endpoint while leader.
	ep, err := coord.LeaderEndpoint(context.Background())
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1:9101", ep)

	cancel()
	require.NoError(t, <-done)
	require.True(t, deposed.Load())
	require.False(t, coord.IsLeader())
}

func TestKubernetes_StandbyReadsAnnotation(t *testing.T) {
	t.Parallel()

	// Seed a Lease with an endpoint annotation as if some other leader
	// had published it; a standby Coordinator on the same client should
	// observe that endpoint via LeaderEndpoint without ever winning.
	client := fake.NewSimpleClientset(&coordv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "scaleset-test",
			Namespace: "ns",
			Annotations: map[string]string{
				DefaultEndpointAnnotation: "10.0.0.99:9101",
			},
		},
	})

	cfg := Config{
		LeaseName:      "scaleset-test",
		LeaseNamespace: "ns",
		Identity:       "pod-2",
		PodIP:          "10.0.0.2",
		AdminPort:      9101,
		LeaseDuration:  200 * time.Millisecond,
		RenewDeadline:  100 * time.Millisecond,
		RetryPeriod:    20 * time.Millisecond,
	}
	coord := NewKubernetesWithClient(cfg, Callbacks{}, discardLogger(), client)

	ep, err := coord.LeaderEndpoint(context.Background())
	require.NoError(t, err)
	require.Equal(t, "10.0.0.99:9101", ep)
}

func TestKubernetes_LeaderEndpointEmptyBeforeAnyLeader(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset() // no Lease exists yet
	cfg := Config{
		LeaseName:      "scaleset-test",
		LeaseNamespace: "ns",
		Identity:       "pod-1",
		PodIP:          "10.0.0.1",
		AdminPort:      9101,
		LeaseDuration:  200 * time.Millisecond,
		RenewDeadline:  100 * time.Millisecond,
		RetryPeriod:    20 * time.Millisecond,
	}
	coord := NewKubernetesWithClient(cfg, Callbacks{}, discardLogger(), client)

	ep, err := coord.LeaderEndpoint(context.Background())
	require.NoError(t, err)
	require.Empty(t, ep, "should return empty (election in flight) rather than an error")
}

func TestConfig_ValidateTimingOrdering(t *testing.T) {
	t.Parallel()

	// LeaseDuration must be > RenewDeadline > RetryPeriod.
	bad := Config{
		LeaseName:      "x",
		LeaseNamespace: "y",
		Identity:       "z",
		LeaseDuration:  100 * time.Millisecond,
		RenewDeadline:  100 * time.Millisecond, // not <
		RetryPeriod:    10 * time.Millisecond,
	}
	require.Error(t, bad.validate())

	bad.RenewDeadline = 90 * time.Millisecond
	bad.RetryPeriod = 90 * time.Millisecond // not <
	require.Error(t, bad.validate())

	good := Config{
		LeaseName:      "x",
		LeaseNamespace: "y",
		Identity:       "z",
		LeaseDuration:  100 * time.Millisecond,
		RenewDeadline:  80 * time.Millisecond,
		RetryPeriod:    20 * time.Millisecond,
	}
	require.NoError(t, good.validate())
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
