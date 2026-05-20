// Package runnerhook exposes a tiny HTTP endpoint on a dedicated port
// that the in-VM GitHub Actions runner pings via its
// ACTIONS_RUNNER_HOOK_JOB_STARTED / _JOB_COMPLETED hook scripts. This is
// the most reliable lifecycle signal we can get — millisecond-precision,
// originating from inside the runner process, surviving GitHub API
// outages.
//
// Authentication: each job receives its own short-lived HMAC-signed JWT
// (see internal/runnertoken). The runner-hook script POSTs the JWT in
// the Authorization header. The server verifies the signature + exp and
// cross-checks the token's vmid claim against the runner_name in the
// payload — a leaked token can only target the one VM it was minted for,
// and only until exp.
//
// Why a separate port (not /admin/runner-event):
//   - Different trust boundary. Admin API requires an operator secret;
//     the runner-hook token is per-job and bound to a specific VM.
//   - Different listen address. Admin API binds 127.0.0.1; the runner
//     hook needs to be reachable from the VM bridge network.
//   - Independent rate limits and logs make audit/forensics simpler.
package runnerhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/observability"
	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/pool"
	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/runnertoken"
)

// Config is the server's runtime configuration.
type Config struct {
	// HTTPAddr is what the server binds to. Empty disables the server.
	HTTPAddr string

	// Verifier checks per-job HMAC JWTs presented by the in-VM hook
	// script. Required when HTTPAddr is set.
	Verifier *runnertoken.Verifier

	// RunnerNamePrefix is used to derive a VMID from the runner name
	// the hook script reports (default convention:
	// "<prefix><vmid>" — e.g. "gh-runner-proxmox-ubuntu-x64-10042").
	RunnerNamePrefix string

	// MaxBodyBytes caps the request body size so a misbehaving runner
	// can't OOM the orchestrator. Defaults to 4 KiB.
	MaxBodyBytes int64

	// RateLimitDisabled turns off per-IP rate limiting entirely. The
	// default is enabled — disable only if there's a fronting reverse
	// proxy enforcing rate limits of its own (and you've configured
	// TrustedProxies so the receiver still logs the real client IP).
	RateLimitDisabled bool

	// RateLimitRPS is the sustained per-IP request rate the server will
	// accept before returning 429. Zero or negative selects the default
	// (5 RPS) — well above any legitimate runner-hook traffic (a runner
	// posts at most two events for its lifetime) but low enough to make
	// brute-forcing a 32-byte HMAC infeasible.
	RateLimitRPS float64

	// RateLimitBurst is the burst budget per IP. Zero selects the
	// default (10).
	RateLimitBurst int

	// RateLimitIdleTTL is how long an idle per-IP entry is retained
	// before the sweeper drops it. Zero selects the default (10m).
	RateLimitIdleTTL time.Duration

	// TrustedProxies is a list of CIDR ranges. When a request arrives
	// from a source IP in one of these ranges, the receiver consults
	// proxy headers (Cf-Connecting-Ip, X-Real-Ip, then X-Forwarded-For
	// from right to left, skipping trusted hops) to determine the real
	// client IP used for rate limiting and logs. Requests from outside
	// the trusted set always use the direct RemoteAddr — proxy headers
	// from untrusted sources are ignored entirely so an attacker
	// connecting directly cannot spoof their source.
	//
	// Default: empty (no proxy headers honored). Set entries like
	// "127.0.0.1/32" when behind a local reverse proxy, or the
	// published Cloudflare ranges when behind Cloudflare.
	TrustedProxies []string
}

// Server is the runner-hook receiver.
type Server struct {
	cfg         Config
	pool        pool.Manager
	log         *slog.Logger
	metrics     *observability.Metrics
	limiter     *perIPLimiter
	trustedNets []*net.IPNet
	// cfgErr is surfaced at Serve() time so New stays infallible while
	// still refusing to start with a malformed TrustedProxies entry.
	cfgErr error
}

// New constructs a Server. log may be nil. Malformed TrustedProxies
// entries don't fail here — they surface as the first Serve() call's
// error, so misconfigured deploys fail loudly at startup rather than
// silently ignoring header trust.
func New(cfg Config, p pool.Manager, log *slog.Logger, metrics *observability.Metrics) *Server {
	if log == nil {
		log = slog.Default()
	}
	if cfg.MaxBodyBytes == 0 {
		cfg.MaxBodyBytes = 4 * 1024
	}
	if cfg.RateLimitRPS <= 0 {
		cfg.RateLimitRPS = 5
	}
	if cfg.RateLimitBurst <= 0 {
		cfg.RateLimitBurst = 10
	}
	if cfg.RateLimitIdleTTL <= 0 {
		cfg.RateLimitIdleTTL = 10 * time.Minute
	}
	s := &Server{
		cfg:     cfg,
		pool:    p,
		log:     log,
		metrics: metrics,
		limiter: newPerIPLimiter(rate.Limit(cfg.RateLimitRPS), cfg.RateLimitBurst, cfg.RateLimitIdleTTL),
	}
	for _, cidr := range cfg.TrustedProxies {
		_, n, err := net.ParseCIDR(strings.TrimSpace(cidr))
		if err != nil {
			s.cfgErr = fmt.Errorf("runnerhook: trusted_proxies entry %q is not a valid CIDR: %w", cidr, err)
			return s
		}
		s.trustedNets = append(s.trustedNets, n)
	}
	return s
}

// perIPLimiter keeps a separate token bucket per remote IP. A sweeper
// goroutine drops idle entries so a long-lived process doesn't grow the
// map without bound. The map is small in normal operation (one entry
// per Proxmox VM bridge IP currently posting) but unbounded under
// attack — hence the sweeper.
type perIPLimiter struct {
	rps   rate.Limit
	burst int
	idle  time.Duration

	mu       sync.Mutex
	limiters map[string]*ipEntry
}

type ipEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

func newPerIPLimiter(rps rate.Limit, burst int, idle time.Duration) *perIPLimiter {
	return &perIPLimiter{
		rps:      rps,
		burst:    burst,
		idle:     idle,
		limiters: map[string]*ipEntry{},
	}
}

// allow returns true if the IP may proceed and false if it has exceeded
// its rate. Always records lastSeen so the sweeper only evicts IPs that
// have actually gone quiet.
func (p *perIPLimiter) allow(ip string, now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.limiters[ip]
	if !ok {
		e = &ipEntry{limiter: rate.NewLimiter(p.rps, p.burst)}
		p.limiters[ip] = e
	}
	e.lastSeen = now
	return e.limiter.AllowN(now, 1)
}

// sweep drops entries whose lastSeen is older than idle. Cheap O(N) walk
// over a map that should never exceed a few hundred entries.
func (p *perIPLimiter) sweep(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for ip, e := range p.limiters {
		if now.Sub(e.lastSeen) > p.idle {
			delete(p.limiters, ip)
		}
	}
}

// Serve runs the HTTP server until ctx is cancelled. Returns nil on
// graceful shutdown.
func (s *Server) Serve(ctx context.Context) error {
	if s.cfgErr != nil {
		return s.cfgErr
	}
	if s.cfg.HTTPAddr == "" {
		s.log.Info("runner-hook server disabled (no http_addr configured)")
		<-ctx.Done()
		return nil
	}
	if s.cfg.Verifier == nil {
		return errors.New("runnerhook: verifier must be set when http_addr is set")
	}
	if s.cfg.RunnerNamePrefix == "" {
		return errors.New("runnerhook: runner_name_prefix must be set")
	}

	mux := http.NewServeMux()
	mux.Handle("/runner-event", s.rateLimit(s.requireToken(http.HandlerFunc(s.handleEvent))))

	srv := &http.Server{
		Addr:              s.cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	// Sweeper for the per-IP limiter map. Runs at half the idle TTL so
	// stale entries are evicted within one full TTL after going quiet.
	sweepInterval := s.cfg.RateLimitIdleTTL / 2
	if sweepInterval < time.Minute {
		sweepInterval = time.Minute
	}
	sweepCtx, sweepCancel := context.WithCancel(ctx)
	defer sweepCancel()
	go func() {
		t := time.NewTicker(sweepInterval)
		defer t.Stop()
		for {
			select {
			case <-sweepCtx.Done():
				return
			case now := <-t.C:
				s.limiter.sweep(now)
			}
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		s.log.Info("runner-hook listening", "addr", s.cfg.HTTPAddr)
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("runner-hook shutdown: %w", err)
		}
		return nil
	case err := <-errCh:
		return err
	}
}

// rateLimit applies a per-source-IP token bucket to incoming requests.
// It sits before requireToken so brute-force attempts against the HMAC
// don't get a free pass — failed auths cost the attacker tokens too.
// When TrustedProxies is set and the direct peer falls inside one of
// those ranges, proxy headers determine the client IP (see clientIP).
// When RateLimitDisabled is true the middleware is a no-op pass-through.
func (s *Server) rateLimit(next http.Handler) http.Handler {
	if s.cfg.RateLimitDisabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := s.clientIP(r)
		if !s.limiter.allow(ip, time.Now()) {
			s.log.Warn("runner-hook: rate limited", "client_ip", ip, "remote", r.RemoteAddr)
			s.observe("rate_limit", "blocked")
			w.Header().Set("Retry-After", "1")
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// clientIP returns the IP the rate limiter (and logs) should treat as
// "the caller". If TrustedProxies is unset or the direct peer is not in
// any trusted range, the direct RemoteAddr's IP is returned — this is
// the safe default, because proxy headers from an untrusted source are
// attacker-controlled and must not be honored.
//
// When the peer IS trusted, headers are consulted in this order:
//
//  1. Cf-Connecting-Ip — Cloudflare's per-request client IP. Single
//     value; trusted because Cloudflare strips this header from
//     inbound requests before re-setting it.
//  2. X-Real-Ip — a single value set by nginx-style reverse proxies.
//  3. X-Forwarded-For — a comma-separated chain of "client, proxy1,
//     proxy2, ...". We walk right-to-left and return the first entry
//     whose IP is NOT itself in the trusted set — that's the closest
//     hop the trusted edge actually observed. Walking RTL (rather than
//     blindly trusting the leftmost) defeats clients who try to inject
//     a fake leftmost entry into their own XFF header.
func (s *Server) clientIP(r *http.Request) string {
	direct := remoteIP(r.RemoteAddr)
	if len(s.trustedNets) == 0 {
		return direct
	}
	parsedDirect := net.ParseIP(direct)
	if parsedDirect == nil || !ipInNets(parsedDirect, s.trustedNets) {
		return direct
	}
	if v := strings.TrimSpace(r.Header.Get("Cf-Connecting-Ip")); v != "" {
		if ip := net.ParseIP(v); ip != nil {
			return ip.String()
		}
	}
	if v := strings.TrimSpace(r.Header.Get("X-Real-Ip")); v != "" {
		if ip := net.ParseIP(v); ip != nil {
			return ip.String()
		}
	}
	if v := r.Header.Get("X-Forwarded-For"); v != "" {
		parts := strings.Split(v, ",")
		for i := len(parts) - 1; i >= 0; i-- {
			entry := strings.TrimSpace(parts[i])
			ip := net.ParseIP(entry)
			if ip == nil {
				continue
			}
			if !ipInNets(ip, s.trustedNets) {
				return ip.String()
			}
		}
	}
	return direct
}

// remoteIP strips the port from a net.RemoteAddr string. Falls back to
// the input when SplitHostPort fails (which shouldn't happen for real
// HTTP requests but is harmless if it does — every limiter entry is
// still per-source-string).
func remoteIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

func ipInNets(ip net.IP, nets []*net.IPNet) bool {
	for _, n := range nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// claimsCtxKey is the context key under which verified token claims are
// stored after requireToken accepts a request.
type claimsCtxKey struct{}

// requireToken verifies the bearer JWT and stuffs the parsed claims into
// the request context for the handler. Verification covers signature,
// expiration, issuer, and that the vmid claim is positive — but NOT
// that the vmid matches the request payload (the handler does that).
func (s *Server) requireToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authz := r.Header.Get("Authorization")
		if !strings.HasPrefix(authz, "Bearer ") {
			w.Header().Set("WWW-Authenticate", `Bearer realm="runner-hook"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		raw := strings.TrimPrefix(authz, "Bearer ")
		claims, err := s.cfg.Verifier.Verify(raw)
		if err != nil {
			s.log.Warn("runner-hook: token verify failed", "err", err, "remote", r.RemoteAddr)
			w.Header().Set("WWW-Authenticate", `Bearer realm="runner-hook", error="invalid_token"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), claimsCtxKey{}, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// claimsFromCtx extracts the verified claims placed by requireToken.
func claimsFromCtx(ctx context.Context) *runnertoken.Claims {
	c, _ := ctx.Value(claimsCtxKey{}).(*runnertoken.Claims)
	return c
}

// claimVMID safely renders the vmid claim for logs even when claims is nil.
func claimVMID(c *runnertoken.Claims) int {
	if c == nil {
		return 0
	}
	return c.VMID
}

// EventPayload is the JSON body a runner hook posts. Schema is small on
// purpose — the runner script is shell + jq and we want it to stay
// readable.
type EventPayload struct {
	// Phase is "started" or "completed".
	Phase string `json:"phase"`

	// RunnerName is the registered runner name (matches our scaleset
	// naming convention). The VMID is derived from this.
	RunnerName string `json:"runner_name"`

	// JobID is the GitHub Actions job ID. May be 0 if the runner hook
	// couldn't determine it; that's not fatal.
	JobID int64 `json:"job_id,omitempty"`

	// RunnerID is the runner's GH-assigned numeric ID. Only set on
	// "started" payloads (the hook reads it from
	// /opt/actions-runner/.runner).
	RunnerID int64 `json:"runner_id,omitempty"`

	// Result is "success" / "failure" / "cancelled" on "completed"
	// payloads. Ignored on "started".
	Result string `json:"result,omitempty"`

	// Timestamp is RFC3339 — informational only.
	Timestamp string `json:"timestamp,omitempty"`
}

func (s *Server) handleEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	if err != nil {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	var p EventPayload
	if err := json.Unmarshal(body, &p); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	vmid, ok := vmidFromName(p.RunnerName, s.cfg.RunnerNamePrefix)
	if !ok {
		s.log.Warn("runner-hook: cannot derive vmid from runner name",
			"runner_name", p.RunnerName, "phase", p.Phase)
		s.observe(p.Phase, "bad_name")
		http.Error(w, "bad runner_name", http.StatusBadRequest)
		return
	}

	// Defense in depth: even though the token verified, the claims must
	// agree with the request the caller is making. A token minted for
	// VM 1001 cannot be used to mark VM 1002 completed.
	claims := claimsFromCtx(r.Context())
	if claims == nil || claims.VMID != vmid {
		s.log.Warn("runner-hook: token vmid claim mismatch",
			"runner_name", p.RunnerName, "payload_vmid", vmid,
			"claim_vmid", claimVMID(claims))
		s.observe(p.Phase, "claim_mismatch")
		http.Error(w, "vmid mismatch", http.StatusForbidden)
		return
	}

	switch p.Phase {
	case "started":
		// On started, the payload's RunnerID must match the runner_id
		// the token was minted for. (For completed, RunnerID isn't sent
		// by the hook so we rely on the vmid binding alone.)
		if p.RunnerID != 0 && claims.RunnerID != 0 && p.RunnerID != claims.RunnerID {
			s.log.Warn("runner-hook: token runner_id claim mismatch",
				"vmid", vmid, "payload_runner_id", p.RunnerID, "claim_runner_id", claims.RunnerID)
			s.observe("started", "claim_mismatch")
			http.Error(w, "runner_id mismatch", http.StatusForbidden)
			return
		}
		if err := s.pool.PromoteToRunning(r.Context(), vmid, p.RunnerID, p.JobID); err != nil {
			s.log.Warn("runner-hook: promote failed", "vmid", vmid, "err", err)
			s.observe("started", "error")
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		s.observe("started", "ok")
		s.log.Info("runner-hook: job started", "vmid", vmid, "job_id", p.JobID, "runner_id", p.RunnerID)
	case "completed":
		if err := s.pool.MarkCompleted(r.Context(), vmid); err != nil {
			s.log.Warn("runner-hook: mark completed failed", "vmid", vmid, "err", err)
			s.observe("completed", "error")
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		s.observe("completed", "ok")
		s.log.Info("runner-hook: job completed", "vmid", vmid, "result", p.Result)
	default:
		s.observe(p.Phase, "bad_phase")
		http.Error(w, "unknown phase", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) observe(phase, result string) {
	if s.metrics != nil {
		s.metrics.RunnerHookEvents.WithLabelValues(phase, result).Inc()
	}
}

// vmidFromName extracts the trailing integer the orchestrator encoded
// into the runner name at clone time. Returns false if the prefix
// doesn't match or the suffix isn't a positive integer.
func vmidFromName(name, prefix string) (int, bool) {
	if !strings.HasPrefix(name, prefix) {
		return 0, false
	}
	rest := strings.TrimPrefix(name, prefix)
	vmid, err := strconv.Atoi(rest)
	if err != nil || vmid <= 0 {
		return 0, false
	}
	return vmid, true
}
