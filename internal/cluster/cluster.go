// Package cluster wraps Kubernetes leader election so the orchestrator
// can run as a single standalone process or as N replicas in Kubernetes
// behind a Lease-based active/standby model. The leader runs the entire
// control plane; standbys hold an idle Proxmox client and serve health
// probes until promoted.
//
// Two implementations are exposed via [Coordinator]:
//
//   - [NewStandalone] is always-leader. OnElected fires synchronously
//     once at Run start; OnDeposed fires once at Run return. Used for
//     single-binary / Docker / systemd deployments.
//
//   - [NewKubernetes] uses k8s.io/client-go/tools/leaderelection against
//     a coordination.k8s.io/v1 Lease. The elected leader publishes its
//     HTTP endpoint into a Lease annotation so standbys can reverse-proxy
//     to it without any other Kubernetes API mutations — keeping the
//     design safe to deploy through Flux/Argo CD.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// DefaultEndpointAnnotation is the Lease metadata.annotations key used to
// publish the leader's HTTP endpoint.
const DefaultEndpointAnnotation = "scaleset.jeffresc.dev/leader-endpoint"

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
	// underlying leader-election library knows it can renew safely.
	OnElected func(ctx context.Context)

	// OnDeposed is called once when this replica stops being leader
	// (either by losing the Lease or by graceful shutdown). Use it to
	// clear any per-leader state (e.g., readiness flags).
	OnDeposed func()
}

// Config configures the Kubernetes Lease-backed coordinator. All time
// values must be non-zero; sensible defaults are 15s/10s/2s for
// LeaseDuration/RenewDeadline/RetryPeriod respectively.
type Config struct {
	// LeaseName is the metadata.name of the Lease object used for
	// election. Typically "scaleset-<scaleset.name>".
	LeaseName string

	// LeaseNamespace is the metadata.namespace of the Lease.
	LeaseNamespace string

	// Identity is this replica's identity string (typically the K8s pod
	// name). Must be unique across replicas; two replicas with the same
	// Identity will deadlock the election.
	Identity string

	// PodIP is this pod's IP address as reported by the Kubernetes
	// Downward API. Published in the Lease annotation alongside
	// AdminPort so standbys can forward admin traffic to the leader.
	PodIP string

	// AdminPort is the TCP port at which this replica serves the
	// admin API. Combined with PodIP to form the published endpoint.
	// 0 means the admin API is disabled — LeaderEndpoint will return
	// an empty string.
	AdminPort int

	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration

	// EndpointAnnotation is the Lease metadata.annotations key used to
	// publish the leader's endpoint. Defaults to
	// [DefaultEndpointAnnotation] when empty.
	EndpointAnnotation string
}

func (c *Config) applyDefaults() {
	if c.EndpointAnnotation == "" {
		c.EndpointAnnotation = DefaultEndpointAnnotation
	}
	if c.LeaseDuration == 0 {
		c.LeaseDuration = 15 * time.Second
	}
	if c.RenewDeadline == 0 {
		c.RenewDeadline = 10 * time.Second
	}
	if c.RetryPeriod == 0 {
		c.RetryPeriod = 2 * time.Second
	}
}

func (c *Config) validate() error {
	switch {
	case c.LeaseName == "":
		return errors.New("cluster: LeaseName is required")
	case c.LeaseNamespace == "":
		return errors.New("cluster: LeaseNamespace is required")
	case c.Identity == "":
		return errors.New("cluster: Identity is required")
	}
	// LeaseDuration > RenewDeadline > RetryPeriod is required by
	// k8s.io/client-go/tools/leaderelection — surface a clearer error
	// up-front than letting RunOrDie panic with its own check.
	if c.RenewDeadline >= c.LeaseDuration {
		return fmt.Errorf("cluster: RenewDeadline (%s) must be < LeaseDuration (%s)",
			c.RenewDeadline, c.LeaseDuration)
	}
	if c.RetryPeriod >= c.RenewDeadline {
		return fmt.Errorf("cluster: RetryPeriod (%s) must be < RenewDeadline (%s)",
			c.RetryPeriod, c.RenewDeadline)
	}
	return nil
}

// localEndpoint returns "host:port" suitable for the Lease annotation,
// or "" when no admin port is configured.
func (c *Config) localEndpoint() string {
	if c.AdminPort <= 0 || c.PodIP == "" {
		return ""
	}
	return net.JoinHostPort(c.PodIP, strconv.Itoa(c.AdminPort))
}

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
// localEndpoint is what [Coordinator.LeaderEndpoint] returns — typically
// the admin API's listen address. Pass "" if the admin API is disabled.
func NewStandalone(localEndpoint string, cb Callbacks) Coordinator {
	return &standalone{endpoint: localEndpoint, cb: cb}
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

// ---------------------------------------------------------------------------
// Kubernetes (Lease-backed)
// ---------------------------------------------------------------------------

type kubeCoord struct {
	cfg    Config
	cb     Callbacks
	client kubernetes.Interface
	log    *slog.Logger
	leader atomic.Bool
	cache  leaderEndpointCache
}

// NewKubernetes returns a Coordinator backed by a coordination.k8s.io/v1
// Lease. The kubernetes client is built from the in-cluster config; for
// out-of-cluster development pass KUBECONFIG via the standard env var.
func NewKubernetes(cfg Config, cb Callbacks, log *slog.Logger) (Coordinator, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("cluster: in-cluster config: %w", err)
	}
	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("cluster: kubernetes client: %w", err)
	}
	return NewKubernetesWithClient(cfg, cb, log, client), nil
}

// NewKubernetesWithClient builds a kubeCoord against a caller-supplied
// kubernetes.Interface. The intended caller is the app package wiring in
// a fake client for e2e tests; production code goes through
// [NewKubernetes].
func NewKubernetesWithClient(cfg Config, cb Callbacks, log *slog.Logger, client kubernetes.Interface) Coordinator {
	cfg.applyDefaults()
	if log == nil {
		log = slog.Default()
	}
	return &kubeCoord{
		cfg:    cfg,
		cb:     cb,
		client: client,
		log:    log,
		cache:  newLeaderEndpointCache(client, cfg.LeaseNamespace, cfg.LeaseName, cfg.EndpointAnnotation),
	}
}

func (k *kubeCoord) Run(ctx context.Context) error {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      k.cfg.LeaseName,
			Namespace: k.cfg.LeaseNamespace,
		},
		Client: k.client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: k.cfg.Identity,
		},
	}

	leaderEndpoint := k.cfg.localEndpoint()

	// Two signals so Run can wait for OnStartedLeading to fully exit
	// before returning. The leader-election library starts
	// OnStartedLeading on its own goroutine and does NOT wait for it
	// to finish before elector.Run returns; we have to do that
	// ourselves so a caller's `coord.Run` returning implies "no
	// leader-only goroutines are still touching shared state".
	startedLeading := make(chan struct{})
	finishedLeading := make(chan struct{})
	var startedOnce, finishedOnce sync.Once

	cfg := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   k.cfg.LeaseDuration,
		RenewDeadline:   k.cfg.RenewDeadline,
		RetryPeriod:     k.cfg.RetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				startedOnce.Do(func() { close(startedLeading) })
				defer finishedOnce.Do(func() { close(finishedLeading) })
				k.leader.Store(true)
				k.log.Info("cluster: became leader",
					"identity", k.cfg.Identity,
					"lease", k.cfg.LeaseName,
					"endpoint", leaderEndpoint)
				// Publish our endpoint into the Lease annotation so
				// standbys can forward to us. The leader-election
				// library's renewal path calls Lock.Update with its
				// cached lease object — if our patch lands between
				// the library's Get and Update, the Update will
				// overwrite the annotation back to empty. Re-publish
				// on every RetryPeriod tick to repair that window;
				// the patch is idempotent so a no-op patch costs one
				// apiserver round trip and nothing else. Initial
				// publish is best-effort — if it fails the lease is
				// still ours and the rest of the control plane runs.
				if leaderEndpoint != "" {
					if err := publishLeaderEndpoint(leaderCtx, k.client, k.cfg.LeaseNamespace, k.cfg.LeaseName, k.cfg.EndpointAnnotation, leaderEndpoint); err != nil {
						k.log.Warn("cluster: publish leader endpoint failed",
							"err", err,
							"endpoint", leaderEndpoint)
					}
					go k.republishLoop(leaderCtx, leaderEndpoint)
				}
				if k.cb.OnElected != nil {
					k.cb.OnElected(leaderCtx)
				}
			},
			OnStoppedLeading: func() {
				k.leader.Store(false)
				k.log.Info("cluster: lost leadership", "identity", k.cfg.Identity)
				if k.cb.OnDeposed != nil {
					k.cb.OnDeposed()
				}
			},
			OnNewLeader: func(identity string) {
				if identity != k.cfg.Identity {
					k.log.Info("cluster: observed new leader", "leader", identity)
				}
			},
		},
	}

	elector, err := leaderelection.NewLeaderElector(cfg)
	if err != nil {
		return fmt.Errorf("cluster: build elector: %w", err)
	}
	elector.Run(ctx)

	// If we ever held leadership, wait for OnStartedLeading to return
	// so the caller can rely on coord.Run returning to mean "no
	// leader-only goroutines still in flight."
	select {
	case <-startedLeading:
		<-finishedLeading
	default:
	}
	return nil
}

// republishLoop re-patches the leader-endpoint annotation onto the
// Lease on every RetryPeriod tick while leaderCtx is alive. Necessary
// because the leader-election library's renewal Updates the Lease with
// its own cached copy — any annotation patch that lands between the
// library's Get and Update is overwritten back to empty. Continuously
// re-patching guarantees standbys see the annotation within at most
// one RetryPeriod after that race.
//
// The patch is idempotent; a steady-state tick is one PATCH that
// changes nothing on the apiserver side. Errors are logged at Debug
// to avoid noise — the worst case is a transient empty annotation
// that heals on the next tick.
func (k *kubeCoord) republishLoop(leaderCtx context.Context, endpoint string) {
	tick := time.NewTicker(k.cfg.RetryPeriod)
	defer tick.Stop()
	for {
		select {
		case <-leaderCtx.Done():
			return
		case <-tick.C:
			if err := publishLeaderEndpoint(leaderCtx, k.client, k.cfg.LeaseNamespace, k.cfg.LeaseName, k.cfg.EndpointAnnotation, endpoint); err != nil {
				k.log.Debug("cluster: republish leader endpoint failed (will retry)",
					"err", err, "endpoint", endpoint)
			}
		}
	}
}

func (k *kubeCoord) IsLeader() bool { return k.leader.Load() }

func (k *kubeCoord) LeaderEndpoint(ctx context.Context) (string, error) {
	// Fast path: this replica is leader. Skip the K8s round-trip — we
	// know our own endpoint better than the cache does.
	if k.leader.Load() {
		return k.cfg.localEndpoint(), nil
	}
	return k.cache.get(ctx)
}
