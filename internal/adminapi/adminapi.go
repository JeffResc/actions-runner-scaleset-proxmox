// Package adminapi exposes a small token-protected HTTP API for operators
// to inspect and intervene in the orchestrator at runtime. The intent is
// to provide an escape hatch — drain on demand, destroy a specific VM,
// view pool state — without needing to attach a debugger or SSH into the
// Proxmox host.
//
// The API is disabled when no http_addr is configured. When enabled, all
// endpoints require a `Authorization: Bearer <shared-secret>` header that
// matches the SharedSecret in config.
package adminapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jellydator/ttlcache/v3"
	"golang.org/x/time/rate"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/observability"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/pool"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/provisioner"
)

// Config is the runtime configuration of the admin API server.
type Config struct {
	HTTPAddr     string
	SharedSecret string

	// TrustedProxies is the list of CIDR ranges from which the server
	// will honor X-Forwarded-For / X-Real-IP headers when deriving the
	// source IP for per-IP auth-failure rate limiting. Requests whose
	// immediate TCP peer is not in this list have those headers
	// ignored; the peer's IP is used instead, so a hostile client
	// cannot spoof its source IP via a header.
	//
	// Empty list = trust nothing = always use the immediate peer's IP.
	TrustedProxies []string

	// TLSConfig, when non-nil, switches the admin server from
	// ListenAndServe to ListenAndServeTLS so the shared bearer secret
	// is transported encrypted between standbys (Forwarder) and the
	// leader. A private CA pinned via ClientCAs + RequireAndVerifyClientCert
	// gives mutual authentication.
	TLSConfig *tls.Config

	// TLSCertFile / TLSKeyFile are the on-disk paths
	// ListenAndServeTLS reads from. They mirror the (cert, key) pair
	// embedded in TLSConfig — ListenAndServeTLS insists on file paths
	// rather than parsed certs. Required when TLSConfig is set.
	TLSCertFile string
	TLSKeyFile  string
}

// LeaderGate decouples the admin API from internal/cluster. The admin
// HTTP server runs on every replica in multi-replica deployments; when
// this replica is not the leader IsLeader returns false and the
// middleware dispatches the request to Forward, which is expected to
// be a reverse-proxy to the leader's pod. Both methods may be called
// concurrently. The standalone Coordinator returns IsLeader()==true
// and Forward is never invoked.
type LeaderGate interface {
	IsLeader() bool
	Forward(w http.ResponseWriter, r *http.Request)
}

// PoolAccessor returns the current pool.Manager or nil when this
// replica is not leader. The admin API uses it instead of holding a
// direct *pool.Manager because the manager is constructed lazily inside
// the cluster.Coordinator's OnElected callback.
type PoolAccessor func() pool.Manager

// Server is the admin API.
type Server struct {
	cfg     Config
	pool    PoolAccessor
	prov    provisioner.Provisioner
	gate    LeaderGate
	log     *slog.Logger
	metrics *observability.Metrics
	// canary is consulted by POST /admin/template/promote to
	// atomically swap a profile's candidate template into the
	// stable slot. Nil (or an accessor that returns nil)
	// disables the endpoint (returns 404). The deferred
	// accessor lets the admin server (built once at startup)
	// be wired to the per-leader-election canary controller.
	canary CanaryAccessor

	// drain is invoked from POST /admin/drain. Typically wired by main()
	// to cancel the orchestrator's root context, which triggers graceful
	// shutdown across every errgroup goroutine. Nil disables the endpoint.
	drain func()

	// authFailLimiter throttles unauthenticated requests per source IP.
	// A successful auth is not metered — operators with the correct
	// secret never hit it.
	authFailLimiter *perIPLimiter

	// trustedProxies are the parsed CIDRs from cfg.TrustedProxies.
	// Populated in New so the realIP middleware can do O(N) prefix
	// checks per request without re-parsing.
	trustedProxies []netip.Prefix
}

// authFail* are the per-IP throttle parameters applied to bad-bearer
// attempts. Tight enough to make line-rate brute force pointless,
// loose enough to absorb the occasional fat-fingered operator.
const (
	authFailRPS   = 1
	authFailBurst = 10
	authFailIdle  = 10 * time.Minute
)

// perIPLimiter holds one token bucket per source IP. Idle entries are
// evicted by ttlcache's background goroutine (started in Serve) so the
// underlying map can't grow under sustained attack from many unique IPs.
//
// Default touch-on-hit semantics extend the TTL on every allow() call,
// so an actively-attacking IP keeps its limiter for the full burst
// budget rather than being recreated mid-attack with a fresh bucket.
type perIPLimiter struct {
	rps   rate.Limit
	burst int

	cache *ttlcache.Cache[string, *rate.Limiter]
}

func newPerIPLimiter(rps rate.Limit, burst int, idle time.Duration) *perIPLimiter {
	return &perIPLimiter{
		rps:   rps,
		burst: burst,
		cache: ttlcache.New[string, *rate.Limiter](
			ttlcache.WithTTL[string, *rate.Limiter](idle),
		),
	}
}

func (p *perIPLimiter) allow(ip string, now time.Time) bool {
	item, _ := p.cache.GetOrSetFunc(ip, func() *rate.Limiter {
		return rate.NewLimiter(p.rps, p.burst)
	})
	return item.Value().AllowN(now, 1)
}

// remoteIP strips the port from a net.RemoteAddr string; falls back to
// the input if SplitHostPort can't parse it.
func remoteIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

// New builds a Server. Caller invokes Serve to start listening. The
// optional `drain` callback is invoked when an operator POSTs
// /admin/drain; pass a closure over your root context's cancel function.
//
// gate must report leadership and reverse-proxy non-leader requests; in
// standalone deployments use [AlwaysLeader] (or any LeaderGate whose
// IsLeader always returns true). poolFn returns the current
// pool.Manager — nil when not leader.
//
// Returns an error when any TrustedProxies entry fails netip.ParsePrefix.
// The config validator already rejects malformed CIDRs, but the
// consumer-side parse used to silently drop bad entries — a silent drop
// meant operator-listed front-end proxies were not trusted, so the
// admin API's per-IP rate limiter keyed on the proxy's IP instead of
// the real client and X-Forwarded-For was ignored. Fail loudly here
// so any future drift between config validator and consumer surfaces
// at startup, not as degraded behaviour that looks like a network bug.
func New(cfg Config, poolFn PoolAccessor, prov provisioner.Provisioner, gate LeaderGate, drain func(), log *slog.Logger) (*Server, error) {
	if log == nil {
		log = slog.Default()
	}
	if gate == nil {
		gate = AlwaysLeader{}
	}
	prefixes := make([]netip.Prefix, 0, len(cfg.TrustedProxies))
	for i, cidr := range cfg.TrustedProxies {
		p, err := netip.ParsePrefix(cidr)
		if err != nil {
			return nil, fmt.Errorf("adminapi: trusted_proxies[%d] %q: %w", i, cidr, err)
		}
		prefixes = append(prefixes, p)
	}
	return &Server{
		cfg: cfg, pool: poolFn, prov: prov, gate: gate, drain: drain, log: log,
		authFailLimiter: newPerIPLimiter(rate.Limit(authFailRPS), authFailBurst, authFailIdle),
		trustedProxies:  prefixes,
	}, nil
}

// CanaryPromoter is the subset of *canary.Controller the admin
// server consults. Reproduced here (with the same method set) so
// this package stays free of the canary import. Any pointer type
// with a `Promote(profile string) error` method satisfies it,
// including *canary.Controller from app.Run.
type CanaryPromoter interface {
	Promote(profile string) error
}

// CanaryAccessor returns the current canary promoter — typically
// backed by an atomic.Pointer that the leader-plane code stores
// to on election and clears on deposal. The admin server calls
// the function on each request so a stale standby can't promote
// against a deposed controller. Returning nil makes the
// /admin/template/promote handler return 503.
type CanaryAccessor func() CanaryPromoter

// SetCanary attaches a deferred canary lookup. Nil disables the
// /admin/template/promote endpoint. Wired by app.Run.
func (s *Server) SetCanary(c CanaryAccessor) { s.canary = c }

// SetMetrics attaches the orchestrator's Prometheus metric set so
// handlers (preempt currently; future ones can join) can emit
// counters from operator-driven actions. Nil disables the metric
// emissions — the endpoint still works.
func (s *Server) SetMetrics(m *observability.Metrics) { s.metrics = m }

// AlwaysLeader is a LeaderGate that always reports leadership. Used by
// standalone deployments. Forward is never called and panics if it is;
// any caller using AlwaysLeader has misconfigured the gate.
type AlwaysLeader struct{}

// IsLeader always returns true.
func (AlwaysLeader) IsLeader() bool { return true }

// Forward must never be called when IsLeader returns true.
func (AlwaysLeader) Forward(_ http.ResponseWriter, _ *http.Request) {
	panic("adminapi: AlwaysLeader.Forward called — middleware should never forward when always-leader")
}

// Serve runs the admin HTTP server until ctx is cancelled or it errors.
// Returns nil on graceful shutdown.
func (s *Server) Serve(ctx context.Context) error {
	if s.cfg.HTTPAddr == "" {
		s.log.Info("admin api disabled (no http_addr configured)")
		<-ctx.Done()
		return nil
	}
	if s.cfg.SharedSecret == "" {
		return errors.New("admin: shared_secret_env must be set when http_addr is set")
	}

	r := chi.NewRouter()
	// realIP() honors X-Forwarded-For / X-Real-IP / True-Client-IP only
	// when the immediate TCP peer matches one of the configured
	// TrustedProxies CIDRs. Otherwise the immediate peer's IP is used
	// — which prevents a hostile client from spoofing its source IP via
	// a header to bypass the per-IP auth-failure rate limiter
	// downstream. cluster.Forwarder also strips these headers before
	// proxying, so even a compromised standby cannot inject them.
	r.Use(s.realIP)
	r.Use(s.accessLog)
	r.Use(s.maxBody(64 * 1024)) // 64 KiB cap on any admin request body
	// Forward-to-leader runs BEFORE bearer-token auth: when a standby
	// proxies a request, the leader does its own auth. Authenticating
	// twice (once on the standby, once on the leader) would force the
	// standby to know the shared secret, which is unnecessary and
	// expands the secret blast radius across replicas.
	r.Use(s.leaderOrForward)
	r.Use(s.requireBearerToken)
	r.Get("/admin/state", s.handleState)
	r.Post("/admin/drain", s.handleDrain)
	r.Post("/admin/destroy/{vmid}", s.handleDestroyVM)
	r.Post("/admin/preempt/{vmid}", s.handlePreemptVM)
	r.Post("/admin/template/promote/{profile}", s.handlePromoteTemplate)

	srv := &http.Server{
		Addr:              s.cfg.HTTPAddr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		TLSConfig:         s.cfg.TLSConfig,
	}
	errCh := make(chan error, 1)
	go func() {
		var err error
		if s.cfg.TLSConfig != nil {
			s.log.Info("admin api listening (tls)", "addr", s.cfg.HTTPAddr)
			err = srv.ListenAndServeTLS(s.cfg.TLSCertFile, s.cfg.TLSKeyFile)
		} else {
			s.log.Info("admin api listening", "addr", s.cfg.HTTPAddr)
			err = srv.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	// ttlcache.Start runs a background eviction loop that prunes idle
	// per-IP rate-limit entries so the underlying map can't grow under
	// sustained attack from many unique IPs.
	go s.authFailLimiter.cache.Start()
	go func() {
		<-ctx.Done()
		s.authFailLimiter.cache.Stop()
	}()
	select {
	case <-ctx.Done():
		// Shutdown intentionally uses a fresh context: ctx is already
		// cancelled, so deriving from it would short-circuit Shutdown
		// before in-flight handlers finish their drain budget.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx) //nolint:contextcheck // see comment above
	case err := <-errCh:
		return err
	}
}

// realIP rewrites r.RemoteAddr from X-Forwarded-For / X-Real-IP /
// True-Client-IP only when the immediate TCP peer's IP is within one
// of the configured TrustedProxies CIDRs. From other peers the
// inbound headers are ignored — preserving r.RemoteAddr as the real
// connection peer so per-IP rate limiters key on something a hostile
// client can't fake.
//
// The two-step "check peer, then maybe rewrite" is intentional: chi's
// middleware.RealIP rewrites unconditionally, which is the bug
// motivating this variant.
func (s *Server) realIP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.peerIsTrustedProxy(r.RemoteAddr) {
			if forwarded := headerFirstIP(r.Header.Get("X-Forwarded-For")); forwarded != "" {
				r.RemoteAddr = forwarded
			} else if real := r.Header.Get("X-Real-IP"); real != "" {
				r.RemoteAddr = real
			} else if tc := r.Header.Get("True-Client-IP"); tc != "" {
				r.RemoteAddr = tc
			}
		}
		next.ServeHTTP(w, r)
	})
}

// peerIsTrustedProxy reports whether remoteAddr's host falls within
// any configured TrustedProxies CIDR. Falls back to false on parse
// failure — the safe default when in doubt.
func (s *Server) peerIsTrustedProxy(remoteAddr string) bool {
	if len(s.trustedProxies) == 0 {
		return false
	}
	host := remoteIP(remoteAddr)
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	for _, p := range s.trustedProxies {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

// headerFirstIP returns the first comma-separated IP from an XFF-style
// header, trimmed of whitespace, or "" when empty. RFC 7239 says the
// first entry is the original client.
func headerFirstIP(hv string) string {
	if hv == "" {
		return ""
	}
	if comma := strings.IndexByte(hv, ','); comma >= 0 {
		hv = hv[:comma]
	}
	return strings.TrimSpace(hv)
}

// accessLog logs every admin request with method, path, remote addr,
// status, and duration. Critical for forensics — an admin endpoint hit
// in production usually means an operator did something specific, and
// we want a record of who and when.
func (s *Server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		s.log.Info("admin api request",
			"method", r.Method,
			"path", r.URL.Path,
			"remote", r.RemoteAddr,
			"status", ww.Status(),
			"bytes", ww.BytesWritten(),
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

// maxBody caps inbound request bodies. Defense against a misbehaving or
// hostile client sending an unbounded body — the standard http.Server
// only caps headers by default.
func (s *Server) maxBody(limit int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, limit)
			next.ServeHTTP(w, r)
		})
	}
}

// leaderOrForward gates every admin request on leadership: leaders
// serve locally, non-leaders hand off to the gate's Forward method
// (typically a reverse-proxy to the leader's pod published in the
// Lease annotation). Runs before requireBearerToken so a standby never
// needs the shared secret.
func (s *Server) leaderOrForward(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.gate.IsLeader() {
			next.ServeHTTP(w, r)
			return
		}
		s.gate.Forward(w, r)
	})
}

// requireBearerToken enforces a shared-secret bearer token on every admin
// request. Both presented token and configured secret are hashed with
// SHA-256 before constant-time comparison — comparing the raw bytes
// directly would short-circuit on length mismatch (per crypto/subtle
// docs) and leak the secret's length via timing.
//
// Failed auths are metered per source IP via authFailLimiter so a
// brute-force attempt against the configured secret is throttled to
// the burst budget (10) within idle TTL. Successful auths are never
// metered — operators with the correct secret pass straight through.
func (s *Server) requireBearerToken(next http.Handler) http.Handler {
	const scheme = "Bearer "
	// Refuse to mount the middleware against an empty secret. Serve()
	// already rejects this at startup, but a future caller (someone
	// splitting requireBearerToken to share with a non-admin path,
	// for example) could bypass that gate. Returning a 503-only
	// handler here means the dangerous precomputed sha256("") is
	// never bound to the closure and a misconfigured deployment fails
	// loudly on every request instead of silently accepting an empty
	// bearer token via ConstantTimeCompare(sha256(""), sha256("")) = 1.
	if s.cfg.SharedSecret == "" {
		s.log.Error("admin: requireBearerToken constructed against empty secret; refusing all requests")
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "admin api misconfigured: empty shared secret", http.StatusServiceUnavailable)
		})
	}
	wantHash := sha256.Sum256([]byte(s.cfg.SharedSecret))
	denyUnauthorized := func(w http.ResponseWriter, r *http.Request) {
		// Apply the rate limiter only on the failure path so a valid
		// token from the same IP isn't throttled by prior typos.
		if s.authFailLimiter != nil {
			if !s.authFailLimiter.allow(remoteIP(r.RemoteAddr), time.Now()) {
				w.Header().Set("Retry-After", "1")
				http.Error(w, "too many failed attempts", http.StatusTooManyRequests)
				return
			}
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Compute gotHash on every request — including missing-header
		// and wrong-scheme cases — so a probing client can't time-
		// distinguish "no Authorization at all" from "Bearer wrong-
		// token". When the scheme is absent we feed an empty token
		// through sha256 so the work done is identical to the
		// wrong-token path; the result still mismatches wantHash and
		// the request lands in denyUnauthorized.
		auth := r.Header.Get("Authorization")
		var token string
		if strings.HasPrefix(auth, scheme) {
			token = auth[len(scheme):]
		}
		gotHash := sha256.Sum256([]byte(token))
		if subtle.ConstantTimeCompare(gotHash[:], wantHash[:]) != 1 {
			denyUnauthorized(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// stateResponse is the JSON shape of GET /admin/state.
type stateResponse struct {
	Pool pool.Stats `json:"pool"`
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	p := s.pool()
	if p == nil {
		// We passed the leader gate yet the pool is nil — race during
		// election handover. Tell the caller to retry rather than 500.
		w.Header().Set("Retry-After", "1")
		http.Error(w, "leader transition in progress", http.StatusServiceUnavailable)
		return
	}
	stats, err := p.Stats(r.Context())
	if err != nil {
		s.log.Error("admin state: pool stats failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	// Encode to a buffer FIRST so a serialisation failure surfaces as
	// a clean 500 instead of a truncated 200 (issue #147 follow-up).
	// Only set Content-Type + write headers once we know we have a
	// complete payload to ship.
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(stateResponse{Pool: stats}); err != nil {
		s.log.Error("admin state: response encode failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(buf.Bytes()); err != nil {
		// Post-header write failure on a read-only endpoint — log at
		// Info (still survives default log levels) so an operator can
		// spot truncated responses on flaky links.
		s.log.Info("admin state: response write failed",
			"endpoint", "GET /admin/state", "remote_addr", r.RemoteAddr, "err", err)
	}
}

// handleDrain triggers a one-shot graceful drain by invoking the callback
// the orchestrator wires in main(). The callback typically cancels the
// root errgroup context, which fires the pool's gracefulDrain path. The
// response returns immediately (202) — the actual shutdown unwinds
// asynchronously and is bounded by pool.drain_timeout.
func (s *Server) handleDrain(w http.ResponseWriter, r *http.Request) {
	if s.drain == nil {
		http.Error(w, "drain endpoint not wired (send SIGTERM to the process instead)", http.StatusNotImplemented)
		return
	}
	s.log.Info("admin drain requested")
	go s.drain()
	w.WriteHeader(http.StatusAccepted)
	if _, err := w.Write([]byte("draining")); err != nil {
		// State-changing endpoint: the 202 body is the operator's
		// only confirmation the request landed. A broken-pipe or
		// write-timeout HERE is a real operational signal — the
		// operator's tooling may show "no response" while a drain
		// IS in flight. Warn so the line survives production log
		// levels (issue #147).
		s.log.Warn("admin drain: response write failed",
			"endpoint", "POST /admin/drain", "remote_addr", r.RemoteAddr, "err", err)
	}
}

// handlePromoteTemplate atomically swaps a profile's canary
// candidate template into the stable slot via the canary
// controller's Promote method. The change is in-process only —
// operators that want the promotion to survive a restart should
// also update `template_vmid` in their config (the orchestrator
// log warning at startup if the YAML hasn't caught up). Returns
// 404 when no canary controller is wired, 409 when the profile
// has no candidate to promote, and 202 on success.
func (s *Server) handlePromoteTemplate(w http.ResponseWriter, r *http.Request) {
	if s.canary == nil {
		http.Error(w, "canary controller not configured", http.StatusNotFound)
		return
	}
	c := s.canary()
	if c == nil {
		// Standby replica (or pre-election leader) — surface
		// as 503 so callers retry against the current leader.
		w.Header().Set("Retry-After", "1")
		http.Error(w, "canary controller not active (leader transition in progress)", http.StatusServiceUnavailable)
		return
	}
	profile := chi.URLParam(r, "profile")
	if profile == "" {
		http.Error(w, "missing profile", http.StatusBadRequest)
		return
	}
	if err := c.Promote(profile); err != nil {
		s.log.Warn("admin promote: refused", "profile", profile, "err", err)
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	s.log.Warn("admin: promoted canary template to stable", "profile", profile)
	w.WriteHeader(http.StatusAccepted)
	if _, err := w.Write([]byte("promoted")); err != nil {
		s.log.Debug("admin promote: response write failed", "err", err)
	}
}

// handlePreemptVM destroys an Assigned-but-not-yet-Running VM via
// the pool's Preempt API (issue #10). Refuses on Running VMs and
// other non-Assigned states; surfaces those refusals as 409
// Conflict so operators can see at a glance whether the preempt
// was actionable. Increments scaleset_preemptions_total{from_class,
// to_class="manual"} when the preempt actually fires — the
// "manual" to_class records that this came from operator action
// rather than an automatic priority decision.
func (s *Server) handlePreemptVM(w http.ResponseWriter, r *http.Request) {
	vmidStr := chi.URLParam(r, "vmid")
	vmid, err := strconv.Atoi(vmidStr)
	if err != nil || vmid <= 0 {
		http.Error(w, fmt.Sprintf("invalid vmid %q", vmidStr), http.StatusBadRequest)
		return
	}
	p := s.pool()
	if p == nil {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "leader transition in progress", http.StatusServiceUnavailable)
		return
	}
	err = p.Preempt(r.Context(), vmid, "admin preempt endpoint")
	if err != nil {
		if errors.Is(err, pool.ErrPreemptRefused) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		s.log.Error("admin preempt: failed", "vmid", vmid, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	// The from_class label is intentionally empty for manual
	// preempts: the admin endpoint doesn't know the row's
	// PriorityClass (RowSnapshot doesn't expose it yet) and we
	// don't want to widen the snapshot just for an admin-action
	// label. Operators care about the increment.
	if s.metrics != nil {
		s.metrics.Preemptions.WithLabelValues("", "manual").Inc()
	}
	w.WriteHeader(http.StatusAccepted)
	if _, err := w.Write([]byte("preempt queued")); err != nil {
		// State-changing endpoint: see comment in handleDrain.
		s.log.Warn("admin preempt: response write failed",
			"endpoint", "POST /admin/preempt/{vmid}", "vmid", vmid,
			"remote_addr", r.RemoteAddr, "err", err)
	}
}

func (s *Server) handleDestroyVM(w http.ResponseWriter, r *http.Request) {
	vmidStr := chi.URLParam(r, "vmid")
	vmid, err := strconv.Atoi(vmidStr)
	if err != nil || vmid <= 0 {
		http.Error(w, fmt.Sprintf("invalid vmid %q", vmidStr), http.StatusBadRequest)
		return
	}
	p := s.pool()
	if p == nil {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "leader transition in progress", http.StatusServiceUnavailable)
		return
	}
	// ForceDestroy (not MarkCompleted): MarkCompleted only acts on
	// Assigned/Running rows and silently no-ops elsewhere, which means
	// an operator targeting a Hot/Warm/Booting VM would get 202 with no
	// effect. ForceDestroy is the unconditional drop the endpoint
	// promises.
	if err := p.ForceDestroy(r.Context(), vmid, "admin destroy endpoint"); err != nil {
		s.log.Error("admin destroy: force destroy failed", "vmid", vmid, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	if _, err := w.Write([]byte("queued for destruction")); err != nil {
		// State-changing endpoint: see comment in handleDrain.
		s.log.Warn("admin destroy: response write failed",
			"endpoint", "POST /admin/destroy/{vmid}", "vmid", vmid,
			"remote_addr", r.RemoteAddr, "err", err)
	}
}
