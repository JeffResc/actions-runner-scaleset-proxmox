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
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/pool"
	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/provisioner"
)

// Config is the runtime configuration of the admin API server.
type Config struct {
	HTTPAddr     string
	SharedSecret string
}

// Server is the admin API.
type Server struct {
	cfg  Config
	pool pool.Manager
	prov provisioner.Provisioner
	log  *slog.Logger

	// drain is invoked from POST /admin/drain. Typically wired by main()
	// to cancel the orchestrator's root context, which triggers graceful
	// shutdown across every errgroup goroutine. Nil disables the endpoint.
	drain func()
}

// New builds a Server. Caller invokes Serve to start listening. The
// optional `drain` callback is invoked when an operator POSTs
// /admin/drain; pass a closure over your root context's cancel function.
func New(cfg Config, p pool.Manager, prov provisioner.Provisioner, drain func(), log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{cfg: cfg, pool: p, prov: prov, drain: drain, log: log}
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
	r.Use(middleware.RealIP)
	r.Use(s.accessLog)
	r.Use(s.maxBody(64 * 1024)) // 64 KiB cap on any admin request body
	r.Use(s.requireBearerToken)
	r.Get("/admin/state", s.handleState)
	r.Post("/admin/drain", s.handleDrain)
	r.Post("/admin/destroy/{vmid}", s.handleDestroyVM)

	srv := &http.Server{
		Addr:              s.cfg.HTTPAddr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		s.log.Info("admin api listening", "addr", s.cfg.HTTPAddr)
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
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
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

// requireBearerToken enforces a shared-secret bearer token on every admin
// request. Both presented token and configured secret are hashed with
// SHA-256 before constant-time comparison — comparing the raw bytes
// directly would short-circuit on length mismatch (per crypto/subtle
// docs) and leak the secret's length via timing.
func (s *Server) requireBearerToken(next http.Handler) http.Handler {
	const scheme = "Bearer "
	wantHash := sha256.Sum256([]byte(s.cfg.SharedSecret))
	secretEmpty := s.cfg.SharedSecret == ""
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Defense in depth: refuse to authenticate against an empty
		// configured secret even if Serve's startup check is bypassed
		// by a future caller — ConstantTimeCompare("","") returns 1.
		if secretEmpty {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, scheme) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		gotHash := sha256.Sum256([]byte(auth[len(scheme):]))
		if subtle.ConstantTimeCompare(gotHash[:], wantHash[:]) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
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
	stats, err := s.pool.Stats(r.Context())
	if err != nil {
		s.log.Error("admin state: pool stats failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stateResponse{Pool: stats})
}

// handleDrain triggers a one-shot graceful drain by invoking the callback
// the orchestrator wires in main(). The callback typically cancels the
// root errgroup context, which fires the pool's gracefulDrain path. The
// response returns immediately (202) — the actual shutdown unwinds
// asynchronously and is bounded by pool.drain_timeout.
func (s *Server) handleDrain(w http.ResponseWriter, _ *http.Request) {
	if s.drain == nil {
		http.Error(w, "drain endpoint not wired (send SIGTERM to the process instead)", http.StatusNotImplemented)
		return
	}
	s.log.Info("admin drain requested")
	go s.drain()
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte("draining"))
}

func (s *Server) handleDestroyVM(w http.ResponseWriter, r *http.Request) {
	vmidStr := chi.URLParam(r, "vmid")
	vmid, err := strconv.Atoi(vmidStr)
	if err != nil || vmid <= 0 {
		http.Error(w, fmt.Sprintf("invalid vmid %q", vmidStr), http.StatusBadRequest)
		return
	}
	// ForceDestroy (not MarkCompleted): MarkCompleted only acts on
	// Assigned/Running rows and silently no-ops elsewhere, which means
	// an operator targeting a Hot/Warm/Booting VM would get 202 with no
	// effect. ForceDestroy is the unconditional drop the endpoint
	// promises.
	if err := s.pool.ForceDestroy(r.Context(), vmid, "admin destroy endpoint"); err != nil {
		s.log.Error("admin destroy: force destroy failed", "vmid", vmid, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte("queued for destruction"))
}
