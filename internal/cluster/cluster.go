// Package cluster owns leader election for the orchestrator. The
// elected leader runs the entire control plane (pool reconcile loop,
// GitHub reconciler, scaleset listener); standbys hold an idle
// Proxmox client and serve health probes until promoted.
//
// Two implementations are exposed via [Coordinator]:
//
//   - [NewStandalone] is always-leader. OnElected fires synchronously
//     once at Run start; OnDeposed fires once at Run return. Used for
//     single-binary / Docker / systemd deployments.
//
//   - [NewRaft] uses an embedded hashicorp/raft cluster across N
//     replicas — no external infrastructure required. The FSM is a
//     no-op; raft is only used as a fault-tolerant leader-election
//     primitive. Standbys reverse-proxy admin traffic to the leader
//     by resolving the leader's raft address against a static peer
//     map declared in config.
package cluster

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
)

// Coordinator owns leadership state for one orchestrator replica.
type Coordinator interface {
	// Run blocks until ctx is cancelled, invoking the configured
	// callbacks as leadership transitions. Returns nil on clean
	// cancellation, non-nil on unrecoverable election failure.
	Run(ctx context.Context) error

	// IsLeader reports whether this replica currently holds leadership.
	// Safe for concurrent use.
	IsLeader() bool

	// LeaderEndpoint returns the HTTP endpoint ("host:port") of the
	// current leader. The empty string is returned when no leader has
	// been observed yet — callers should treat this as "election in
	// flight" and return 503 to upstream clients. Safe for concurrent
	// use.
	LeaderEndpoint(ctx context.Context) (string, error)
}

// Callbacks are invoked as leadership transitions.
type Callbacks struct {
	// OnElected is called on a goroutine when this replica becomes
	// leader. The provided context is cancelled when leadership is
	// lost; OnElected SHOULD block until the context is done so the
	// underlying election loop knows it can renew safely.
	OnElected func(ctx context.Context)

	// OnDeposed is called once when this replica stops being leader
	// (either by losing the election or by graceful shutdown). Use it
	// to clear any per-leader state (e.g., readiness flags).
	OnDeposed func()
}

// localEndpoint returns "host:port" suitable for the LeaderEndpoint
// fast-path, or "" when no admin port is configured.
func localEndpoint(host string, port int) string {
	if port <= 0 || host == "" {
		return ""
	}
	return net.JoinHostPort(host, strconv.Itoa(port))
}

// ErrInvalidConfig is returned by coordinator constructors when the
// provided configuration cannot produce a working election.
var ErrInvalidConfig = errors.New("cluster: invalid configuration")

// ---------------------------------------------------------------------------
// Standalone (always-leader)
// ---------------------------------------------------------------------------

type standalone struct {
	endpoint string
	cb       Callbacks
	leader   atomic.Bool
}

// NewStandalone returns a Coordinator that is always leader. OnElected
// fires once synchronously inside Run before Run blocks on ctx;
// OnDeposed fires once when Run returns.
//
// endpoint is what [Coordinator.LeaderEndpoint] returns — typically
// the admin API's listen address. Pass "" if the admin API is disabled.
func NewStandalone(endpoint string, cb Callbacks) Coordinator {
	return &standalone{endpoint: endpoint, cb: cb}
}

func (s *standalone) Run(ctx context.Context) error {
	s.leader.Store(true)
	defer s.leader.Store(false)

	leaderCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if s.cb.OnElected != nil {
			s.cb.OnElected(leaderCtx)
		}
	}()

	<-ctx.Done()
	cancel()
	<-done
	if s.cb.OnDeposed != nil {
		s.cb.OnDeposed()
	}
	return nil
}

func (s *standalone) IsLeader() bool { return s.leader.Load() }

func (s *standalone) LeaderEndpoint(_ context.Context) (string, error) {
	return s.endpoint, nil
}
