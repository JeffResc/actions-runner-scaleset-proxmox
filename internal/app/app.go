// Package app is the orchestrator's library entrypoint. The cmd/scaleset
// binary is a thin cobra wrapper that builds Options and calls Run; tests
// drive the same Run with the same Options to exercise the full
// orchestrator in-process against fake Proxmox and GitHub HTTP servers.
package app

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"golang.org/x/sync/errgroup"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/adminapi"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/canary"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/cluster"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/config"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/gh"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/githubauth"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/ipam"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/nodeselector"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/observability"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/pool"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/priority"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/provisioner"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/quotas"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/router"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/scaler"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/schedule"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/store"
)

// Options configures a single Run invocation. The first four fields mirror
// the CLI flags exposed by cmd/scaleset; the remaining fields are test
// hooks that production callers leave nil.
type Options struct {
	// ConfigPath is the YAML config file path. Required.
	ConfigPath string

	// DryRun, when true, wraps the Proxmox provisioner with a logger that
	// short-circuits destructive operations. Mirrors `--dry-run`.
	DryRun bool

	// Version is the build version (passed through to log lines and
	// tracer service.version). Empty string is acceptable.
	Version string

	// RaftTransport lets callers (notably e2e tests) inject an
	// in-process raft.Transport — typically a raft.NewInmemTransport
	// shared between all replicas in a test — in place of the
	// production TCP transport. Nil in production.
	RaftTransport raft.Transport

	// RaftLocalAddr is the address the test transport advertises;
	// only consulted when RaftTransport is non-nil. Nil in production.
	RaftLocalAddr raft.ServerAddress

	// AuthOverride bypasses cfg.GitHub.AuthMode and the on-disk PEM /
	// PAT resolution. When non-nil it is used verbatim, letting tests
	// point the orchestrator at fake GitHub servers without minting
	// real credentials.
	AuthOverride githubauth.Auth
}

// Run executes the orchestrator until ctx is cancelled or an unrecoverable
// error occurs. The caller owns the context lifecycle — main installs a
// signal.NotifyContext for SIGINT/SIGTERM; tests cancel on their own
// schedule.
//
//nolint:contextcheck // tracer/shutdown defers deliberately use fresh contexts; see in-body comments
func Run(ctx context.Context, opts Options) error {
	cfg, err := config.Load(opts.ConfigPath)
	if err != nil {
		return err
	}

	log, err := observability.NewLogger(cfg.Observability.LogLevel, cfg.Observability.LogFormat)
	if err != nil {
		return err
	}
	slog.SetDefault(log)
	log.Info("scaleset starting", "version", opts.Version, "config", opts.ConfigPath, "dry_run", opts.DryRun,
		"scalesets", len(cfg.Scalesets))

	// Warn about SCALESET_*-prefixed env vars that look like overrides
	// but don't map to any schema key — the canonical operator-typo
	// signal (e.g. SCALESET_POOL_HOTSIZE instead of SCALESET_POOL_HOT_SIZE).
	// Without this, the override silently no-ops and the operator is
	// left wondering why their change didn't take.
	for _, name := range cfg.UnknownEnvOverrides() {
		log.Warn("unknown SCALESET_* env var; ignored (typo?)", "name", name)
	}

	// Derive a cancellable child so leader-plane failures and admin
	// drain can force the whole process down; SIGTERM cancellation
	// arrives via the parent ctx.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tracerShutdown, err := observability.InitTracer(ctx, observability.TracingOptions{
		Endpoint:       cfg.Observability.Tracing.Endpoint,
		Insecure:       cfg.Observability.Tracing.Insecure,
		SampleRatio:    cfg.Observability.Tracing.SampleRatio,
		ServiceName:    "actions-runner-scaleset-proxmox",
		ServiceVersion: opts.Version,
	}, log)
	if err != nil {
		return fmt.Errorf("init tracer: %w", err)
	}
	defer func() { //nolint:contextcheck // see comment below
		// Tracer flush uses a fresh context: this defer runs after
		// ctx has been cancelled (signal or leader-plane failure), and
		// deriving from a cancelled ctx would skip the flush.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tracerShutdown(shutdownCtx); err != nil {
			log.Warn("tracer shutdown failed", "err", err)
		}
	}()

	// Per-scaleset provisioners + per-scaleset state slots. Each
	// scale set gets its own provisioner (own scaleSetName + own
	// in-flight/destroyed trackers) and its own atomic.Pointer
	// pair for pool.Manager / canary.Controller — populated when
	// this replica is leader, cleared on deposal.
	scStates := make(map[string]*scalesetState, len(cfg.Scalesets))
	for _, s := range cfg.Scalesets {
		vmPrefix := fmt.Sprintf("gh-runner-%s-", s.Name)
		prov, err := provisioner.New(ctx, cfg.Proxmox, s.Name, vmPrefix, provisioner.Options{
			CloneInflightTTL:     cfg.Pool.CloneInflightGrace.D(),
			RecentlyDestroyedTTL: cfg.Pool.VMIDReuseCooldown.D() * 4,
		}, log)
		if err != nil {
			return fmt.Errorf("init provisioner for scaleset %q: %w", s.Name, err)
		}
		if opts.DryRun {
			prov = provisioner.NewDryRun(prov, log)
		}
		scStates[s.Name] = &scalesetState{name: s.Name, prov: prov, vmPrefix: vmPrefix}
	}
	if opts.DryRun {
		log.Info("dry-run mode active: destructive Proxmox operations will be logged, not executed")
	}

	// Pick any scaleset's provisioner for the shared
	// health-refresher ping and the admin-server construction.
	// All provisioners hit the same Proxmox endpoint, so any
	// liveness ping is representative; adminapi.New takes a
	// provisioner only as a future-proofing hook (no handler
	// reads from it today).
	var sharedProv provisioner.Provisioner
	for _, s := range cfg.Scalesets {
		sharedProv = scStates[s.Name].prov
		break
	}

	sel, err := buildNodeSelector(cfg, sharedProv)
	if err != nil {
		return fmt.Errorf("init node selector: %w", err)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
	)
	metrics := observability.NewMetrics(reg)
	health := observability.NewHealth(30 * time.Second)
	// Register every declared scale set so /readyz requires
	// each to signal listener-connected + recovery-done before
	// the leader is considered ready (issue #1).
	for _, s := range cfg.Scalesets {
		health.RegisterScaleset(s.Name)
	}

	auth, err := buildGitHubAuth(cfg, opts.AuthOverride)
	if err != nil {
		return fmt.Errorf("init github auth: %w", err)
	}
	sysInfo := scaleset.SystemInfo{
		System:    "actions-runner-scaleset-proxmox",
		Version:   opts.Version,
		CommitSHA: "",
	}

	runOneScaleset := func(leaderCtx context.Context, entry config.ScaleSetEntry, state *scalesetState) error {
		scope := githubauth.Scope{Org: entry.Scope.Org, Repo: entry.Scope.Repo}
		ssSysInfo := sysInfo // copy; ScaleSetID stamped per-scaleset below
		ghClient, err := auth.NewScaleSetClient(leaderCtx, scope, ssSysInfo)
		if err != nil {
			return fmt.Errorf("build scaleset client: %w", err)
		}
		restCli, err := auth.NewRESTClient(leaderCtx, githubauth.WithRateLimitMetrics(metrics.ForScaleset(entry.Name)))
		if err != nil {
			return fmt.Errorf("build github rest client: %w", err)
		}

		rss, err := ensureScaleSetForEntry(leaderCtx, ghClient, entry, log)
		if err != nil {
			return fmt.Errorf("ensure runner scale set: %w", err)
		}
		ssSysInfo.ScaleSetID = rss.ID
		ghClient.SetSystemInfo(ssSysInfo)
		log.Info("runner scale set ready", "scaleset", entry.Name, "name", rss.Name, "id", rss.ID)

		// Build the per-profile canary controller. Errors are
		// non-fatal — log + degrade to no-canary (every clone
		// uses the profile's stable TemplateVMID).
		canaryCtrl, cerr := canaryControllerForScaleset(entry, cfg.Proxmox.TemplateVMID)
		if cerr != nil {
			log.Warn("canary: controller build failed; canary rollout disabled", "scaleset", entry.Name, "err", cerr)
			canaryCtrl = nil
		}
		state.canaryPtr.Store(canaryCtrl)
		defer state.canaryPtr.Store(nil)

		st, err := store.New()
		if err != nil {
			return fmt.Errorf("init store: %w", err)
		}

		mgr, err := pool.NewManager(pool.Config{
			HotSize:              cfg.Pool.HotSize,
			WarmSize:             cfg.Pool.WarmSize,
			MaxConcurrentRunners: entry.MaxConcurrentRunners,
			ReconcileInterval:    cfg.Pool.ReconcileInterval.D(),
			PowerPollInterval:    cfg.Pool.PowerPollInterval.D(),
			VMMaxAge:             cfg.Pool.VMMaxAge.D(),
			DrainTimeout:         cfg.Pool.DrainTimeout.D(),
			BootMaxAttempts:      cfg.Pool.BootMaxAttempts,
			Profiles:             profileSettingsForScaleset(entry, cfg),
			Canary:               canaryCtrl,
			ScaleSetName:         entry.Name,
			VMNamePrefix:         state.vmPrefix,
			VMIDRange:            entryVMIDRange(entry, cfg.Proxmox.VMIDRange),
			LinkedClones:         cfg.Proxmox.Clone.LinkedOrDefault(),
			TemplateNode:         state.prov.TemplateNode(),
			VMIDReuseCooldown:    cfg.Pool.VMIDReuseCooldown.D(),
			OnRunnerOrphaned:     ghClient.RemoveRunner,
			RunnerLister:         gh.NewRunnerLister(restCli, scope, state.vmPrefix, log),
		}, st, state.prov, sel, log, metrics)
		if err != nil {
			return fmt.Errorf("init pool: %w", err)
		}

		if err := mgr.Adopt(leaderCtx); err != nil {
			log.Warn("adopt: list-owned-vms failed; starting with empty pool", "scaleset", entry.Name, "err", err)
		}
		health.MarkScalesetRecoveryDone(entry.Name)
		health.MarkProxmoxOK()

		state.poolPtr.Store(&mgr)
		defer state.poolPtr.Store(nil)

		sc := scaler.New(scaler.Config{
			ScaleSetID:   rss.ID,
			ScaleSetName: entry.Name,
			WorkFolder:   "_work",
			NamePrefix:   state.vmPrefix,
		}, ghClient, mgr, state.prov, log, metrics)

		if r, err := routerForScaleset(entry); err != nil {
			log.Warn("router: build failed; routing observations disabled", "scaleset", entry.Name, "err", err)
		} else {
			sc.SetRouter(r)
		}
		if q, err := quotasResolverFromConfig(cfg); err != nil {
			log.Warn("quotas: resolver build failed; quotas observation disabled", "scaleset", entry.Name, "err", err)
		} else {
			sc.SetQuotas(q)
			sc.SetQuotaCounter(st)
		}
		if pm, err := priorityMatcherFromConfig(cfg); err != nil {
			log.Warn("priority: matcher build failed; priority observation disabled", "scaleset", entry.Name, "err", err)
		} else {
			sc.SetPriority(pm)
		}

		owner := scope.Org
		if owner == "" {
			idx := strings.IndexByte(scope.Repo, '/')
			if idx <= 0 {
				return fmt.Errorf("scalesets[%q].scope.repo %q is not in owner/repo form (validation should have caught this)",
					entry.Name, scope.Repo)
			}
			owner = scope.Repo[:idx]
		}
		sessionClient, err := ghClient.MessageSessionClient(leaderCtx, rss.ID, owner)
		if err != nil {
			return fmt.Errorf("open message session: %w", err)
		}

		lst, err := listener.New(sessionClient, listener.Config{
			ScaleSetID: rss.ID,
			MaxRunners: entry.MaxConcurrentRunners,
			Logger:     log,
		})
		if err != nil {
			return fmt.Errorf("build listener: %w", err)
		}

		rec, err := gh.New(gh.Config{
			Scope:                scope,
			ScaleSetName:         entry.Name,
			PollInterval:         cfg.GitHub.PollInterval.D(),
			AssignedGrace:        cfg.GitHub.AssignedGrace.D(),
			RunningIdleGrace:     cfg.GitHub.RunningIdleGrace.D(),
			AssignedOfflineGrace: cfg.GitHub.AssignedOfflineGrace.D(),
			OrphanGrace:          cfg.Pool.OrphanGrace.D(),
			RunnerNamePrefix:     state.vmPrefix,
		}, restCli, mgr, state.prov, log, metrics)
		if err != nil {
			return fmt.Errorf("build gh reconciler: %w", err)
		}

		schedRunner, serr := scheduleRunnerForScaleset(entry, cfg.Pool, mgr, log, metrics)
		if serr != nil {
			log.Warn("schedule: runner build failed; schedules disabled", "scaleset", entry.Name, "err", serr)
		}

		g, ctxg := errgroup.WithContext(leaderCtx)
		g.Go(func() error { return mgr.Run(ctxg) })
		if schedRunner != nil {
			g.Go(func() error {
				err := schedRunner.Run(ctxg)
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			})
		}
		g.Go(func() error {
			err := rec.Run(ctxg)
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		})
		g.Go(func() error {
			health.MarkScalesetListenerConnected(entry.Name)
			err := lst.Run(ctxg, sc)
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		})
		return g.Wait()
	}

	// runLeaderPlane fans the per-scaleset workers out under one
	// supervisor errgroup. A failing scaleset must not poison its
	// siblings — each worker is wrapped in superviseScaleset
	// which logs the failure but does not propagate it, so the
	// other scale sets keep running. Sibling shutdown still
	// happens via ctx cancel on process-wide SIGTERM / drain.
	runLeaderPlane := func(leaderCtx context.Context) error {
		g, ctxg := errgroup.WithContext(leaderCtx)
		for i := range cfg.Scalesets {
			entry := cfg.Scalesets[i]
			state := scStates[entry.Name]
			g.Go(func() error {
				return superviseScaleset(ctxg, entry, state, log, runOneScaleset)
			})
		}
		return g.Wait()
	}

	// leaderPlaneErr surfaces a runLeaderPlane failure to the
	// post-g1.Wait() handling below. OnElected cancels the root ctx
	// on error, which causes coord.Run(ctx1) to return nil (clean
	// ctx-cancel is not an error from its perspective). Without this
	// hand-off, Run would return nil and the process would exit 0,
	// defeating systemd Restart=on-failure / k8s restartPolicy:
	// OnFailure on a class of bugs the code itself deemed fatal.
	var leaderPlaneErr atomic.Pointer[error]

	cbCallbacks := cluster.Callbacks{
		OnElected: func(leaderCtx context.Context) {
			health.MarkLeader(true)
			metrics.Leader.Set(1)
			if err := runLeaderPlane(leaderCtx); err != nil {
				log.Error("leader plane failed; shutting down", "err", err)
				leaderPlaneErr.Store(&err)
				cancel()
			}
		},
		OnDeposed: func() {
			health.MarkLeader(false)
			metrics.Leader.Set(0)
			for _, s := range cfg.Scalesets {
				health.ClearScalesetState(s.Name)
			}
		},
	}

	coord, err := buildCoordinator(cfg, cbCallbacks, log, opts)
	if err != nil {
		return fmt.Errorf("init cluster coordinator: %w", err)
	}

	var adminServerTLS, adminClientTLS *tls.Config
	if cfg.AdminAPI.TLS != nil {
		adminServerTLS, err = cfg.AdminAPI.TLS.BuildServerTLS()
		if err != nil {
			return fmt.Errorf("admin api: build server tls: %w", err)
		}
		adminClientTLS, err = cfg.AdminAPI.TLS.BuildClientTLS()
		if err != nil {
			return fmt.Errorf("admin api: build client tls: %w", err)
		}
	}

	adminAPIConfig := adminapi.Config{
		HTTPAddr:       cfg.AdminAPI.HTTPAddr,
		SharedSecret:   cfg.AdminAPI.SharedSecret,
		TrustedProxies: cfg.AdminAPI.TrustedProxies,
		TLSConfig:      adminServerTLS,
	}
	if cfg.AdminAPI.TLS != nil {
		adminAPIConfig.TLSCertFile = cfg.AdminAPI.TLS.CertFile
		adminAPIConfig.TLSKeyFile = cfg.AdminAPI.TLS.KeyFile
	}
	// Default poolFn / canaryFn back the legacy un-namespaced
	// admin routes (`/admin/state`, `/admin/destroy/{vmid}`, etc.).
	// With N == 1 they alias the single scaleset's accessors so
	// existing single-scaleset operators keep working. With N > 1
	// they return nil — the legacy routes 503 because they cannot
	// disambiguate which scaleset to target; operators must use
	// the namespaced `/admin/{scaleset}/...` paths.
	var defaultPoolFn adminapi.PoolAccessor
	var defaultCanaryFn adminapi.CanaryAccessor
	if len(cfg.Scalesets) == 1 {
		only := scStates[cfg.Scalesets[0].Name]
		defaultPoolFn = func() pool.Manager {
			p := only.poolPtr.Load()
			if p == nil {
				return nil
			}
			return *p
		}
		defaultCanaryFn = adminapi.CanaryAccessor(func() adminapi.CanaryPromoter {
			c := only.canaryPtr.Load()
			if c == nil {
				return nil
			}
			return c
		})
	}

	admin, err := adminapi.New(adminAPIConfig, defaultPoolFn, sharedProv, buildAdminGate(cfg, coord, adminClientTLS), func() {
		log.Warn("admin drain triggered; cancelling root context")
		cancel()
	}, log)
	if err != nil {
		return fmt.Errorf("admin api: build server: %w", err)
	}
	admin.SetMetrics(metrics)
	if defaultCanaryFn != nil {
		admin.SetCanary(defaultCanaryFn)
	}
	// Register the namespaced accessors for every scaleset so
	// `/admin/{scaleset}/...` routes resolve to the right
	// per-scaleset pool / canary controller (issue #1). Closures
	// capture each state pointer by reference, not by value, so
	// reads happen at request time and reflect the current
	// leader-election state.
	for _, s := range cfg.Scalesets {
		state := scStates[s.Name]
		admin.SetScalesetPool(s.Name, func() pool.Manager {
			p := state.poolPtr.Load()
			if p == nil {
				return nil
			}
			return *p
		})
		admin.SetScalesetCanary(s.Name, adminapi.CanaryAccessor(func() adminapi.CanaryPromoter {
			c := state.canaryPtr.Load()
			if c == nil {
				return nil
			}
			return c
		}))
	}

	// Two-phase shutdown:
	//
	//   Phase 1 — leader plane (g1): the cluster.Coordinator drives
	//     the GH listener, REST reconciler, and pool manager when (and
	//     only when) this replica holds the Lease. In standalone mode
	//     it always holds it.
	//
	//   Phase 2 — HTTP plane (g2): observability, admin API, and the
	//     health refresher. These STAY UP during drain so:
	//       - /metrics + /readyz remain observable during the drain
	//         window (operators need visibility exactly here)
	//       - /admin/state and friends remain usable during drain
	//     They shut down only after g1.Wait() returns. The
	//     runHealthRefresher runs on every replica — standbys need
	//     Proxmox liveness to be ready to take over.
	g1, ctx1 := errgroup.WithContext(ctx)
	g1.Go(func() error { return coord.Run(ctx1) })

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	g2, ctx2g := errgroup.WithContext(ctx2)
	g2.Go(func() error { return observability.Serve(ctx2g, cfg.Observability.HTTPAddr, reg, health, log) })
	g2.Go(func() error { return admin.Serve(ctx2g) })
	g2.Go(func() error { return runHealthRefresher(ctx2g, sharedProv, health, log) })

	log.Info("scaleset running", "cluster_mode", cfg.Cluster.Mode)

	phase1Err := mergeLeaderPlaneErr(g1.Wait(), &leaderPlaneErr)
	log.Info("scaleset: phase 1 complete; stopping HTTP servers")
	cancel2()
	phase2Err := g2.Wait()

	if phase1Err != nil {
		return fmt.Errorf("scaleset terminated (phase 1): %w", phase1Err)
	}
	if phase2Err != nil {
		return fmt.Errorf("scaleset terminated (phase 2): %w", phase2Err)
	}
	log.Info("scaleset stopped cleanly")
	return nil
}

// runHealthRefresher pings Proxmox every ~15s and updates the readiness
// tracker. Without this, /readyz flips to 503 once the staleness window
// (default 30s) elapses past startup.
func runHealthRefresher(ctx context.Context, prov provisioner.Provisioner, health *observability.Health, log *slog.Logger) error {
	const interval = 15 * time.Second
	tick := time.NewTicker(interval)
	defer tick.Stop()
	if err := prov.Ping(ctx); err == nil {
		health.MarkProxmoxOK()
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := prov.Ping(pctx)
			cancel()
			if err != nil {
				log.Warn("proxmox health probe failed", "err", err)
				continue
			}
			health.MarkProxmoxOK()
		}
	}
}

// buildCoordinator selects the cluster.Coordinator implementation based on
// cfg.Cluster.Mode. Standalone is the default (single-replica deployments).
// raft mode builds an embedded hashicorp/raft cluster across the configured
// static peer list — no external infrastructure required.
//
// The opts.RaftTransport / opts.RaftLocalAddr pair is the e2e-test hook:
// when supplied, the coordinator wires raft to an InmemTransport in place
// of a real TCP listener, letting tests stand up N in-process replicas
// without binding ports.
func buildCoordinator(cfg *config.Config, cb cluster.Callbacks, log *slog.Logger, opts Options) (cluster.Coordinator, error) {
	if cfg.Cluster.Mode != "raft" {
		return cluster.NewStandalone(cfg.AdminAPI.HTTPAddr, cb), nil
	}
	port, err := portFromAddr(cfg.AdminAPI.HTTPAddr)
	if err != nil {
		return nil, fmt.Errorf("cluster: extract admin port: %w", err)
	}
	host, _ := splitHostPort(cfg.AdminAPI.HTTPAddr)
	peers := make([]cluster.RaftPeer, 0, len(cfg.Cluster.Raft.Peers))
	for _, p := range cfg.Cluster.Raft.Peers {
		peers = append(peers, cluster.RaftPeer{
			NodeID:   p.NodeID,
			RaftAddr: p.RaftAddr,
			HTTPAddr: p.HTTPAddr,
		})
	}
	rcfg := cluster.RaftConfig{
		NodeID:           cfg.Cluster.Raft.NodeID,
		BindAddr:         cfg.Cluster.Raft.BindAddr,
		AdvertiseAddr:    cfg.Cluster.Raft.AdvertiseAddr,
		DataDir:          cfg.Cluster.Raft.DataDir,
		AdminPort:        port,
		AdminHost:        host,
		Peers:            peers,
		Bootstrap:        cfg.Cluster.Raft.Bootstrap,
		HeartbeatTimeout: cfg.Cluster.Raft.HeartbeatTimeout.D(),
		ElectionTimeout:  cfg.Cluster.Raft.ElectionTimeout.D(),
		CommitTimeout:    cfg.Cluster.Raft.CommitTimeout.D(),
		TestTransport:    opts.RaftTransport,
		TestLocalAddr:    opts.RaftLocalAddr,
	}
	// BuildServerTLS produces a tls.Config that doubles as the dial-
	// side bundle (Certificates + RootCAs from the same CA file), so
	// we can pass the same value to both halves of the StreamLayer.
	if cfg.Cluster.Raft.TLS != nil {
		raftTLS, err := cfg.Cluster.Raft.TLS.BuildServerTLS()
		if err != nil {
			return nil, fmt.Errorf("cluster: raft tls: %w", err)
		}
		// BuildServerTLS doesn't set RootCAs (server side has ClientCAs),
		// so reuse the client builder to populate RootCAs for the dial.
		if cfg.Cluster.Raft.TLS.CAFile != "" {
			client, err := cfg.Cluster.Raft.TLS.BuildClientTLS()
			if err != nil {
				return nil, fmt.Errorf("cluster: raft tls (client): %w", err)
			}
			raftTLS.RootCAs = client.RootCAs
		}
		rcfg.TLS = raftTLS
	}
	return cluster.NewRaft(rcfg, cb, log)
}

// splitHostPort returns just the host part of a "host:port" string;
// "" is returned for both halves when addr is empty.
func splitHostPort(addr string) (host, port string) {
	if addr == "" {
		return "", ""
	}
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		return "", ""
	}
	return h, p
}

// mergeLeaderPlaneErr promotes a stashed runLeaderPlane error into the
// errgroup result when g1.Wait() returns nil (which happens whenever
// OnElected cancelled the root ctx — coord.Run treats clean ctx-cancel
// as success). Without this promotion, Run() would return nil on a
// leader-plane crash and supervisors (systemd Restart=on-failure,
// k8s restartPolicy: OnFailure) would not restart.
func mergeLeaderPlaneErr(phase1Err error, leaderPlaneErr *atomic.Pointer[error]) error {
	if phase1Err != nil {
		return phase1Err
	}
	if errPtr := leaderPlaneErr.Load(); errPtr != nil && *errPtr != nil {
		return fmt.Errorf("leader plane failed: %w", *errPtr)
	}
	return nil
}

// portFromAddr parses ":9101" / "0.0.0.0:9101" / "127.0.0.1:9101" /
// "[::1]:9101" into the integer port via net.SplitHostPort, which
// handles bracketed IPv6 literals correctly. Returns 0 (no error)
// when addr is empty so admin API disabled doesn't surface as a
// startup failure.
func portFromAddr(addr string) (int, error) {
	if addr == "" {
		return 0, nil
	}
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, fmt.Errorf("split host:port %q: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, fmt.Errorf("parse port %q: %w", portStr, err)
	}
	if port <= 0 || port > 65535 {
		return 0, fmt.Errorf("port %d out of range", port)
	}
	return port, nil
}

// coordAdminGate adapts a cluster.Coordinator to adminapi.LeaderGate.
// Non-leader requests are reverse-proxied to the leader via the
// shared cluster.Forwarder, preserving X-Forwarded-For so any future
// per-IP rate-limiting on the admin side still sees the original
// client address.
type coordAdminGate struct {
	coord cluster.Coordinator
	fwd   *cluster.Forwarder
}

func (g *coordAdminGate) IsLeader() bool { return g.coord.IsLeader() }
func (g *coordAdminGate) Forward(w http.ResponseWriter, r *http.Request) {
	g.fwd.ServeHTTP(w, r)
}

// buildAdminGate picks the LeaderGate that matches the cluster mode.
// Standalone deployments always serve admin locally; multi-replica
// deployments either serve locally (when leader) or proxy to the
// leader. When admin TLS is configured, the Forwarder dials the leader
// over https with the same TLSConfig (so a private-CA mTLS bundle
// applies on both ends).
func buildAdminGate(cfg *config.Config, coord cluster.Coordinator, tlsClient *tls.Config) adminapi.LeaderGate {
	if cfg.Cluster.Mode == "standalone" {
		return adminapi.AlwaysLeader{}
	}
	return &coordAdminGate{coord: coord, fwd: cluster.NewForwarder(coord, tlsClient)}
}

// buildNodeSelector translates config into a Selector. Pass the
// provisioner so least_loaded can borrow its Proxmox client. When
// nodes.affinity rules are declared, the underlying strategy is
// wrapped in nodeselector.NewAffinity so prefer_nodes /
// anti_affinity_with rules apply before rotation / load balancing.
func buildNodeSelector(cfg *config.Config, prov provisioner.Provisioner) (nodeselector.Selector, error) {
	underlying, err := buildUnderlyingSelector(cfg, prov)
	if err != nil {
		return nil, err
	}
	if len(cfg.Nodes.Affinity) == 0 {
		return underlying, nil
	}
	return nodeselector.NewAffinity(underlying, affinityRulesFromConfig(cfg), affinityNodeUniverse(cfg))
}

func buildUnderlyingSelector(cfg *config.Config, prov provisioner.Provisioner) (nodeselector.Selector, error) {
	switch cfg.Nodes.Strategy {
	case "single":
		return nodeselector.NewSingle(cfg.Nodes.SingleNode)
	case "round_robin":
		return nodeselector.NewRoundRobin(cfg.Nodes.Members)
	case "least_loaded":
		return nodeselector.NewLeastLoaded(prov.Client(), cfg.Nodes.Members, 30*time.Second)
	}
	return nil, fmt.Errorf("unknown nodes.strategy %q", cfg.Nodes.Strategy)
}

// affinityRulesFromConfig projects YAML-level rules into the
// nodeselector shape.
func affinityRulesFromConfig(cfg *config.Config) []nodeselector.AffinityRule {
	out := make([]nodeselector.AffinityRule, 0, len(cfg.Nodes.Affinity))
	for _, r := range cfg.Nodes.Affinity {
		out = append(out, nodeselector.AffinityRule{
			Match:            nodeselector.AffinitySelector{Profile: r.Match.Profile},
			PreferNodes:      append([]string(nil), r.PreferNodes...),
			Require:          r.Require,
			AntiAffinityWith: nodeselector.AffinitySelector{Profile: r.AntiAffinityWith.Profile},
		})
	}
	return out
}

// affinityNodeUniverse returns the operator-declared node list the
// affinity wrapper uses to compute "eligible = universe minus
// exclusions". Single-node deployments collapse to a one-element
// slice.
func affinityNodeUniverse(cfg *config.Config) []string {
	if cfg.Nodes.Strategy == "single" {
		return []string{cfg.Nodes.SingleNode}
	}
	return cfg.Nodes.Members
}

// buildGitHubAuth translates config into a githubauth.Auth. When
// override is non-nil it is used verbatim, bypassing cfg.GitHub.AuthMode
// entirely — the test hook for pointing the orchestrator at a fake
// GitHub server without minting real PAT/App credentials.
func buildGitHubAuth(cfg *config.Config, override githubauth.Auth) (githubauth.Auth, error) {
	if override != nil {
		return override, nil
	}
	switch cfg.GitHub.AuthMode {
	case "pat":
		// Config-time validation guarantees ConfigURL and ConfigBaseURL
		// aren't both set; pass them straight through to NewPATWithConfig
		// which re-enforces the mutual-exclusion rule at the auth
		// boundary (defence in depth — a future config refactor that
		// forgets the validator still gets caught here).
		return githubauth.NewPATWithConfig(githubauth.PATConfig{
			Token:         cfg.GitHub.PAT.Token,
			ConfigURL:     cfg.GitHub.PAT.ConfigURL,
			ConfigBaseURL: cfg.GitHub.PAT.ConfigBaseURL,
		})
	case "app":
		return githubauth.NewAppFromFileWithConfig(githubauth.AppConfig{
			ClientID:       cfg.GitHub.App.Issuer(),
			InstallationID: cfg.GitHub.App.InstallationID,
			ConfigURL:      cfg.GitHub.App.ConfigURL,
			ConfigBaseURL:  cfg.GitHub.App.ConfigBaseURL,
			RESTBaseURL:    cfg.GitHub.App.RESTBaseURL,
		}, cfg.GitHub.App.PrivateKeyPath)
	}
	return nil, fmt.Errorf("unknown github.auth_mode %q", cfg.GitHub.AuthMode)
}

// ensureScaleSet locates an existing scale set by name or creates one.
//
// The scaleset library's contract for GetRunnerScaleSet:
//   - (rss, nil) — found
//   - (nil, nil) — not found (clean "doesn't exist" signal)
//   - (nil, err) — actual failure (auth, network, multiple-match, etc.)
//
// We must distinguish the last case from the second one. A previous
// implementation silently fell through to CreateRunnerScaleSet on any
// non-nil error, turning a 5xx into a misleading "create failed".
// scalesetState is the per-scaleset runtime state shared
// between the leader-plane fan-out and the admin server's
// per-scaleset accessors (issue #1). The atomic.Pointer pair
// is populated when this replica is leader and a per-scaleset
// worker has completed its build; reads via the admin
// accessors return nil during standby or in the gap between
// election and pool construction.
type scalesetState struct {
	name      string
	prov      provisioner.Provisioner
	vmPrefix  string
	poolPtr   atomic.Pointer[pool.Manager]
	canaryPtr atomic.Pointer[canary.Controller]
}

// superviseScaleset wraps one per-scaleset worker so a panic or
// returned error in scale set A does not poison scale set B.
// On panic: log + recover. On error: log + return nil so the
// outer errgroup keeps the siblings running. Sibling shutdown
// still happens via ctx cancel on SIGTERM / drain / process-
// wide failure.
func superviseScaleset(ctx context.Context, entry config.ScaleSetEntry, state *scalesetState, log *slog.Logger,
	run func(context.Context, config.ScaleSetEntry, *scalesetState) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("scaleset worker panicked", "scaleset", entry.Name, "panic", fmt.Sprintf("%v", r))
			err = nil // sibling-isolated: never propagate
		}
	}()
	if err := run(ctx, entry, state); err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Error("scaleset worker failed; siblings unaffected", "scaleset", entry.Name, "err", err)
		}
	}
	return nil
}

// ensureScaleSetForEntry is the per-entry variant of the
// previous ensureScaleSet helper. Mirrors the same lookup-then-
// create semantics, but reads identity (Name / Labels /
// RunnerGroup) from the per-scaleset entry rather than the
// legacy top-level cfg.ScaleSet block.
func ensureScaleSetForEntry(ctx context.Context, gh *scaleset.Client, entry config.ScaleSetEntry, log *slog.Logger) (*scaleset.RunnerScaleSet, error) {
	groupID := 1
	if entry.RunnerGroup != "" {
		rg, err := gh.GetRunnerGroupByName(ctx, entry.RunnerGroup)
		if err != nil {
			return nil, fmt.Errorf("get runner group %q: %w", entry.RunnerGroup, err)
		}
		groupID = rg.ID
	}
	existing, err := gh.GetRunnerScaleSet(ctx, groupID, entry.Name)
	if err != nil {
		return nil, fmt.Errorf("lookup runner scale set %q: %w", entry.Name, err)
	}
	if existing != nil {
		return existing, nil
	}
	log.Info("creating runner scale set", "name", entry.Name)
	labels := make([]scaleset.Label, 0, len(entry.Labels))
	for _, l := range entry.Labels {
		labels = append(labels, scaleset.Label{Name: l, Type: "User"})
	}
	created, err := gh.CreateRunnerScaleSet(ctx, &scaleset.RunnerScaleSet{
		Name:          entry.Name,
		RunnerGroupID: groupID,
		Labels:        labels,
	})
	if err != nil {
		return nil, fmt.Errorf("create runner scale set: %w", err)
	}
	return created, nil
}

// scheduleMetricsAdapter adapts the orchestrator's Prometheus
// Metrics into the schedule.Metrics interface (which is
// intentionally narrow so the schedule package stays free of
// the observability import).
type scheduleMetricsAdapter struct{ m *observability.Metrics }

func (a scheduleMetricsAdapter) IncFire(profile, sched string) {
	a.m.ScheduleFires.WithLabelValues(profile, sched).Inc()
}

func (a scheduleMetricsAdapter) SetActive(profile, sched string) {
	// Encode the per-profile state as one time series per
	// (profile, schedule) — clear the previous schedule's
	// active=1 by setting it to 0, then mark the new one. The
	// baseline state (sched == "") gets recorded so dashboards
	// can show "currently no override" explicitly rather than
	// reading the absence of metrics.
	a.m.ScheduleActive.DeletePartialMatch(map[string]string{"profile": profile})
	a.m.ScheduleActive.WithLabelValues(profile, sched).Set(1)
}

// scheduleRunnerFromConfig builds a schedule.Runner from every
// profile's Schedules block. Returns (nil, nil) when no profile
// declared a schedule — caller skips the goroutine spawn.
func scheduleRunnerForScaleset(ss config.ScaleSetEntry, poolDefaults config.PoolConfig, mgr pool.Manager, log *slog.Logger, metrics *observability.Metrics) (*schedule.Runner, error) {
	var entries []schedule.Entry
	for _, p := range ss.Profiles {
		baseHot := p.HotSizeOrDefault(poolDefaults.HotSize)
		baseWarm := p.WarmSizeOrDefault(poolDefaults.WarmSize)
		for _, s := range p.Schedules {
			entries = append(entries, schedule.Entry{
				Name:         s.Name,
				Profile:      p.Name,
				Spec:         s.Cron,
				Cron:         s.CronSchedule,
				Duration:     s.Duration.D(),
				Location:     s.Location,
				HotSize:      s.HotSize,
				WarmSize:     s.WarmSize,
				BaselineHot:  baseHot,
				BaselineWarm: baseWarm,
			})
		}
	}
	if len(entries) == 0 {
		return nil, nil //nolint:nilnil // "no schedules" is signalled by (nil, nil); caller skips the goroutine spawn
	}
	apply := func(profile string, hot, warm int) {
		if err := mgr.SetTargetSizes(profile, hot, warm); err != nil {
			log.Warn("schedule: SetTargetSizes failed", "profile", profile, "err", err)
		}
	}
	return schedule.NewRunner(entries, apply, schedule.RealClock{}, log, scheduleMetricsAdapter{m: metrics})
}

// canaryControllerFromConfig projects per-profile canary fields
// into the canary.Controller shape. A profile with
// canary_template_vmid == 0 contributes a stable-only entry so
// Pick still has a registered profile to query (avoiding
// ErrUnknownProfile in the hot path). Returns nil + the
// underlying error from canary.New on validation failures.
// entryVMIDRange returns the per-scaleset VMID range, falling
// back to the shared cfg.Proxmox.VMIDRange when the entry
// inherits (the single-scaleset back-compat path). With N > 1
// scalesets the config validator (validateScalesetVMIDRanges)
// requires every entry to declare its own range, so the
// fall-through here is only reached for single-scaleset configs
// and for tests that construct a Config in-process.
func entryVMIDRange(entry config.ScaleSetEntry, fallback config.VMIDRange) config.VMIDRange {
	if entry.VMIDRange != nil {
		return *entry.VMIDRange
	}
	return fallback
}

func canaryControllerForScaleset(ss config.ScaleSetEntry, defaultTemplateVMID int) (*canary.Controller, error) {
	cfgs := make([]canary.ProfileConfig, 0, len(ss.Profiles))
	for _, p := range ss.Profiles {
		stable := p.TemplateVMID
		if stable == 0 {
			stable = defaultTemplateVMID
		}
		cfgs = append(cfgs, canary.ProfileConfig{
			Name:                  p.Name,
			StableTemplateVMID:    stable,
			CandidateTemplateVMID: p.CanaryTemplateVMID,
			Percent:               p.CanaryPercent,
			MaxFailureRate:        p.CanaryMaxFailureRate,
		})
	}
	return canary.New(cfgs)
}

// profileSettingsFromConfig projects the YAML-level config.ProfileConfig
// slice into the pool.ProfileSettings shape the manager consumes. The
// per-profile resource fields (CPU, memory, etc.) are threaded through
// CloneOptions; sizing fields drive the per-profile reconcile loop.
//
// ApplyDefaults has already synthesised the single default profile and
// inherited unset fields from the global pool / scaleset blocks, so
// this projection is a straight mapping.
// quotasResolverFromConfig projects the YAML-level QuotasConfig
// into the internal/quotas shape. Returns nil + an error when the
// resolver refuses construction (e.g. ambiguous override). Caller
// is expected to log + continue without quotas, not abort startup.
func quotasResolverFromConfig(cfg *config.Config) (*quotas.Resolver, error) {
	overrides := make([]quotas.Override, 0, len(cfg.Quotas.Overrides))
	for _, o := range cfg.Quotas.Overrides {
		overrides = append(overrides, quotas.Override{
			Org:           o.Match.Org,
			Repo:          o.Match.Repo,
			MaxConcurrent: o.MaxConcurrent,
		})
	}
	return quotas.New(quotas.Config{
		DefaultPerRepo: cfg.Quotas.DefaultPerRepo,
		DefaultPerOrg:  cfg.Quotas.DefaultPerOrg,
		Overrides:      overrides,
	})
}

// priorityMatcherFromConfig projects the YAML-level PriorityConfig
// into the internal/priority shape. An empty class list returns a
// Matcher that always classifies into priority.ZeroClass — the
// caller can still attach it without changing observed behaviour.
func priorityMatcherFromConfig(cfg *config.Config) (*priority.Matcher, error) {
	classes := make([]priority.Class, 0, len(cfg.Priority.Classes))
	for _, c := range cfg.Priority.Classes {
		classes = append(classes, priority.Class{
			Name:    c.Name,
			Weight:  c.Weight,
			Preempt: c.Preempt,
			Match: priority.Match{
				WorkflowLabel: c.Match.WorkflowLabel,
				Repo:          c.Match.Repo,
				Org:           c.Match.Org,
			},
		})
	}
	return priority.New(classes)
}

// routerFromConfig projects the YAML-level profiles into the
// router.Profile shape and constructs a Router. Returns nil + the
// underlying error when construction fails (router.New only fails on
// duplicate / empty profile names, which config validation already
// rejects).
func routerForScaleset(ss config.ScaleSetEntry) (*router.Router, error) {
	profiles := make([]router.Profile, 0, len(ss.Profiles))
	for _, p := range ss.Profiles {
		profiles = append(profiles, router.Profile{
			Name:   p.Name,
			Labels: append([]string(nil), p.Labels...),
		})
	}
	return router.New(profiles)
}

func profileSettingsForScaleset(ss config.ScaleSetEntry, cfg *config.Config) []pool.ProfileSettings {
	out := make([]pool.ProfileSettings, 0, len(ss.Profiles))
	for _, p := range ss.Profiles {
		out = append(out, pool.ProfileSettings{
			Name:                 p.Name,
			HotSize:              p.HotSizeOrDefault(cfg.Pool.HotSize),
			WarmSize:             p.WarmSizeOrDefault(cfg.Pool.WarmSize),
			MaxConcurrentRunners: p.MaxConcurrentRunnersOrDefault(ss.MaxConcurrentRunners),
			BootMaxAttempts:      p.BootMaxAttemptsOrDefault(cfg.Pool.BootMaxAttempts),
			VMMaxAge:             p.VMMaxAge.D(),
			TemplateVMID:         p.TemplateVMID,
			CPUCores:             p.CPUCores,
			MemoryMB:             p.MemoryMB,
			DiskGB:               p.DiskGB,
			Storage:              p.Storage,
			NICs:                 nicsFromProfileNetwork(cfg, p),
			IPAM:                 ipamFromProfileNetwork(p, slog.Default()),
		})
	}
	return out
}

// nicsFromProfileNetwork builds the CloneNIC slice for one
// profile, layering the optional per-profile network block over
// the global proxmox.network defaults. Empty network = no NIC
// override (the template's interfaces stay).
func nicsFromProfileNetwork(cfg *config.Config, p config.ProfileConfig) []provisioner.CloneNIC {
	if p.Network == nil {
		return nil
	}
	primary := provisioner.CloneNIC{
		Bridge:       firstNonEmpty(p.Network.Bridge, cfg.Proxmox.Network.Bridge),
		VLANTag:      p.Network.VLANTag,
		VLANUntagged: p.Network.VLANUntagged,
		MTU:          p.Network.MTU,
	}
	// Fall back to the global VLAN tag when the profile didn't
	// set its own (and isn't explicitly untagged).
	if primary.VLANTag == 0 && !primary.VLANUntagged {
		primary.VLANTag = cfg.Proxmox.Network.VLANTag
	}
	out := []provisioner.CloneNIC{primary}
	for _, nic := range p.Network.ExtraNICs {
		out = append(out, provisioner.CloneNIC{
			Bridge:       nic.Bridge,
			VLANTag:      nic.VLANTag,
			VLANUntagged: nic.VLANUntagged,
			MTU:          nic.MTU,
		})
	}
	return out
}

// ipamFromProfileNetwork builds the per-profile IPAM allocator.
// Returns ipam.Noop when no IPAM block is configured so the pool
// manager can call Allocate/Release unconditionally.
func ipamFromProfileNetwork(p config.ProfileConfig, log *slog.Logger) ipam.Allocator {
	if p.Network == nil || p.Network.IPAM == nil {
		return ipam.Noop{}
	}
	switch p.Network.IPAM.Backend {
	case "static":
		s, err := ipam.NewStatic(p.Network.IPAM.Pool)
		if err != nil {
			log.Warn("ipam: static allocator build failed; falling back to noop",
				"profile", p.Name, "err", err)
			return ipam.Noop{}
		}
		return s
	case "noop", "":
		return ipam.Noop{}
	}
	log.Warn("ipam: unknown backend; falling back to noop",
		"profile", p.Name, "backend", p.Network.IPAM.Backend)
	return ipam.Noop{}
}

// firstNonEmpty returns the first non-empty argument. Used to
// layer a profile override over the global default.
func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
