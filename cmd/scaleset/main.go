// scaleset is the orchestrator binary. It runs an actions/scaleset
// listener backed by Proxmox VMs as ephemeral GitHub Actions runners.
//
// Subcommands:
//
//	scaleset run [--config=path] [--dry-run]
//	scaleset version
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/adminapi"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/cluster"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/config"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/gh"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/githubauth"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/nodeselector"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/observability"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/pool"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/provisioner"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/scaler"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/store"
)

// version is overridden via -ldflags at build time.
var version = "dev"

func main() {
	root := &cobra.Command{
		Use:           "scaleset",
		Short:         "Run GitHub Actions jobs as ephemeral Proxmox VMs",
		SilenceErrors: true,
		SilenceUsage:  true,
	}

	var (
		configPath           string
		dryRun               bool
		allowPartialRecovery bool
	)
	root.PersistentFlags().StringVarP(&configPath, "config", "c", "config.yaml", "Path to config YAML.")

	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Run the orchestrator until SIGINT/SIGTERM.",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runOrchestrator(configPath, dryRun, allowPartialRecovery)
		},
	}
	runCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Log intended Proxmox actions without executing them.")
	runCmd.Flags().BoolVar(&allowPartialRecovery, "allow-partial-recovery", false,
		"Start even when crash recovery couldn't destroy every orphaned Proxmox VM. "+
			"Dangerous — the orchestrator will clone fresh VMs on top of leaked ones. "+
			"Use only as a one-time escape hatch when a Proxmox node is permanently unreachable.")

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print the build version and exit.",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println(version)
		},
	}

	root.AddCommand(runCmd, versionCmd)

	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "scaleset: %v\n", err)
		os.Exit(1)
	}
}

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

func runOrchestrator(configPath string, dryRun, allowPartialRecovery bool) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}

	log, err := observability.NewLogger(cfg.Observability.LogLevel, cfg.Observability.LogFormat)
	if err != nil {
		return err
	}
	slog.SetDefault(log)
	log.Info("scaleset starting", "version", version, "config", configPath, "dry_run", dryRun)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Tracing. Initialised early so all subsequent instrumented code
	// paths land on the configured TracerProvider rather than the no-op
	// fallback. shutdown is called via defer so pending spans are flushed
	// even on error returns.
	tracerShutdown, err := observability.InitTracer(ctx, observability.TracingOptions{
		Endpoint:       cfg.Observability.Tracing.Endpoint,
		Insecure:       cfg.Observability.Tracing.Insecure,
		SampleRatio:    cfg.Observability.Tracing.SampleRatio,
		ServiceName:    "actions-runner-scaleset-proxmox",
		ServiceVersion: version,
	}, log)
	if err != nil {
		return fmt.Errorf("init tracer: %w", err)
	}
	defer func() {
		// Use a fresh context — root ctx is likely cancelled by this
		// point, and exporter Shutdown needs a working ctx to flush.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tracerShutdown(shutdownCtx); err != nil {
			log.Warn("tracer shutdown failed", "err", err)
		}
	}()

	// In-memory state. No persistent backing; crash recovery is handled
	// by reconciling against Proxmox in pool.Manager.Recover.
	st, err := store.New()
	if err != nil {
		return fmt.Errorf("init store: %w", err)
	}

	// Proxmox provisioner. Pass the VM-name prefix so the provisioner
	// can detect untagged orphans whose tag-apply step was lost mid-clone.
	vmNamePrefix := fmt.Sprintf("gh-runner-%s-", cfg.ScaleSet.Name)
	prov, err := provisioner.New(ctx, cfg.Proxmox, cfg.ScaleSet.Name, vmNamePrefix, provisioner.Options{
		CloneInflightTTL:     cfg.Pool.CloneInflightGraceD,
		RecentlyDestroyedTTL: cfg.Pool.VMIDReuseCooldownD * 4, // memory ceiling well above the pool's reuse cooldown
	}, log)
	if err != nil {
		return fmt.Errorf("init provisioner: %w", err)
	}
	if dryRun {
		log.Info("dry-run mode active: destructive Proxmox operations will be logged, not executed")
		prov = provisioner.NewDryRun(prov, log)
	}

	// NodeSelector (after provisioner so least_loaded can borrow its client).
	sel, err := buildNodeSelector(cfg, prov)
	if err != nil {
		return fmt.Errorf("init node selector: %w", err)
	}

	// Observability HTTP.
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
	)
	metrics := observability.NewMetrics(reg)
	health := observability.NewHealth(30 * time.Second) // staleness allowed before /readyz flips

	// GitHub auth + scaleset client.
	auth, err := buildGitHubAuth(cfg)
	if err != nil {
		return fmt.Errorf("init github auth: %w", err)
	}
	scope := githubauth.Scope{Org: cfg.GitHub.Scope.Org, Repo: cfg.GitHub.Scope.Repo}
	sysInfo := scaleset.SystemInfo{
		System:    "actions-runner-scaleset-proxmox",
		Version:   version,
		CommitSHA: "",
	}
	ghClient, err := auth.NewScaleSetClient(ctx, scope, sysInfo)
	if err != nil {
		return fmt.Errorf("build scaleset client: %w", err)
	}

	// Locate or create the runner scale set on GitHub.
	rss, err := ensureScaleSet(ctx, ghClient, cfg, log)
	if err != nil {
		return fmt.Errorf("ensure runner scale set: %w", err)
	}
	sysInfo.ScaleSetID = rss.ID
	ghClient.SetSystemInfo(sysInfo)
	log.Info("runner scale set ready", "name", rss.Name, "id", rss.ID)

	// GitHub REST client (read-side). Building it here keeps it on every
	// replica even though only the leader runs the gh reconciler — the
	// client carries no state of its own.
	restCli, err := auth.NewRESTClient(ctx, githubauth.WithRateLimitMetrics(metrics))
	if err != nil {
		return fmt.Errorf("build github rest client: %w", err)
	}

	prefix := vmNamePrefix

	// poolPtr holds the leader's pool.Manager. Standbys see nil. The
	// admin API and any other code that needs the manager goes through
	// the accessor closure so handoff between leader and standby is
	// atomic from the consumer's perspective.
	var poolPtr atomic.Pointer[pool.Manager]
	poolFn := adminapi.PoolAccessor(func() pool.Manager {
		p := poolPtr.Load()
		if p == nil {
			return nil
		}
		return *p
	})

	// Cluster coordinator. In standalone mode the OnElected callback
	// runs immediately and once; in kubernetes mode it fires whenever
	// this replica wins the coordination.k8s.io/v1 Lease.
	//
	// All leader-only setup — runner scale set lookup/create, pool
	// manager construction, crash recovery, scaleset listener,
	// gh reconciler — happens inside runLeaderPlane, which blocks
	// until the supplied leaderCtx is cancelled by either coord
	// (leadership loss) or main (SIGTERM).
	runLeaderPlane := func(leaderCtx context.Context) error {
		// Locate or create the runner scale set on GitHub.
		rss, err := ensureScaleSet(leaderCtx, ghClient, cfg, log)
		if err != nil {
			return fmt.Errorf("ensure runner scale set: %w", err)
		}
		sysInfo.ScaleSetID = rss.ID
		ghClient.SetSystemInfo(sysInfo)
		log.Info("runner scale set ready", "name", rss.Name, "id", rss.ID)

		mgr, err := pool.NewManager(pool.Config{
			HotSize:              cfg.Pool.HotSize,
			WarmSize:             cfg.Pool.WarmSize,
			MaxConcurrentRunners: cfg.ScaleSet.MaxConcurrentRunners,
			ReconcileInterval:    cfg.Pool.ReconcileIntervalD,
			PowerPollInterval:    cfg.Pool.PowerPollIntervalD,
			VMMaxAge:             cfg.Pool.VMMaxAgeD,
			DrainTimeout:         cfg.Pool.DrainTimeoutD,
			BootMaxAttempts:      cfg.Pool.BootMaxAttempts,
			ScaleSetName:         cfg.ScaleSet.Name,
			VMNamePrefix:         prefix,
			VMIDRange:            cfg.Proxmox.VMIDRange,
			LinkedClones:         cfg.Proxmox.Clone.LinkedOrDefault(),
			TemplateNode:         prov.TemplateNode(),
			VMIDReuseCooldown:    cfg.Pool.VMIDReuseCooldownD,
			OnRunnerOrphaned:     ghClient.RemoveRunner,
		}, st, prov, sel, log, metrics)
		if err != nil {
			return fmt.Errorf("init pool: %w", err)
		}

		// Crash recovery before listening. Partial recovery is unsafe:
		// any orphan VM the orchestrator can't destroy on startup will
		// live on in Proxmox while a fresh clone takes its place —
		// eventually exhausting the VMID range or hardware. Refuse to
		// start unless the operator explicitly opts into partial
		// recovery. In multi-replica deployments this is the SAME path
		// used on failover, so each new leader rebuilds its in-memory
		// view from Proxmox before serving traffic.
		if err := mgr.Recover(leaderCtx); err != nil {
			if !allowPartialRecovery {
				return fmt.Errorf("crash recovery failed (start with --allow-partial-recovery to override at your own risk): %w", err)
			}
			log.Error("crash recovery failed; --allow-partial-recovery in effect, continuing with orphaned VMs in Proxmox", "err", err)
		}
		health.MarkRecoveryDone()
		health.MarkProxmoxOK()

		// Publish the manager so the admin API can reach it.
		poolPtr.Store(&mgr)
		defer poolPtr.Store(nil)

		sc := scaler.New(scaler.Config{
			ScaleSetID: rss.ID,
			WorkFolder: "_work",
			NamePrefix: prefix,
		}, ghClient, mgr, prov, log, metrics)

		// Open a message session for the listener. The scaleset
		// session API uses just the owning org/user slug (NOT
		// "owner/repo"), so for repo-scoped scale sets we strip the
		// repo half. Config validation already guarantees exactly one
		// of Org or Repo is set, and that Repo (when present) is in
		// "owner/repo" form — but fail loudly if someone ever loosens
		// those checks.
		owner := cfg.GitHub.Scope.Org
		if owner == "" {
			idx := strings.IndexByte(cfg.GitHub.Scope.Repo, '/')
			if idx <= 0 {
				return fmt.Errorf("github.scope.repo %q is not in owner/repo form (validation should have caught this)",
					cfg.GitHub.Scope.Repo)
			}
			owner = cfg.GitHub.Scope.Repo[:idx]
		}
		sessionClient, err := ghClient.MessageSessionClient(leaderCtx, rss.ID, owner)
		if err != nil {
			return fmt.Errorf("open message session: %w", err)
		}

		lst, err := listener.New(sessionClient, listener.Config{
			ScaleSetID: rss.ID,
			MaxRunners: cfg.ScaleSet.MaxConcurrentRunners,
			Logger:     log,
		})
		if err != nil {
			return fmt.Errorf("build listener: %w", err)
		}

		rec, err := gh.New(gh.Config{
			Scope:                scope,
			PollInterval:         cfg.GitHub.PollIntervalD,
			AssignedGrace:        cfg.GitHub.AssignedGraceD,
			RunningIdleGrace:     cfg.GitHub.RunningIdleGraceD,
			AssignedOfflineGrace: cfg.GitHub.AssignedOfflineGraceD,
			OrphanGrace:          cfg.Pool.OrphanGraceD,
			RunnerNamePrefix:     prefix,
		}, restCli, mgr, prov, log, metrics)
		if err != nil {
			return fmt.Errorf("build gh reconciler: %w", err)
		}

		// Leader-side errgroup: pool manager, gh reconciler, scaleset
		// listener. mgr.Run's drain() waits for in-flight Proxmox
		// calls to finish (up to DrainTimeout) when leaderCtx is
		// cancelled.
		g, ctxg := errgroup.WithContext(leaderCtx)
		g.Go(func() error { return mgr.Run(ctxg) })
		g.Go(func() error {
			err := rec.Run(ctxg)
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		})
		g.Go(func() error {
			health.MarkListenerConnected()
			err := lst.Run(ctxg, sc)
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		})
		return g.Wait()
	}

	cbCallbacks := cluster.Callbacks{
		OnElected: func(leaderCtx context.Context) {
			health.MarkLeader(true)
			if err := runLeaderPlane(leaderCtx); err != nil {
				log.Error("leader plane failed; shutting down", "err", err)
				cancel() // tear down the whole process
			}
		},
		OnDeposed: func() {
			health.MarkLeader(false)
			health.ClearListenerConnected()
			health.ClearRecoveryDone()
		},
	}

	coord, err := buildCoordinator(cfg, cbCallbacks, log)
	if err != nil {
		return fmt.Errorf("init cluster coordinator: %w", err)
	}

	// Admin API. The drain callback cancels the root context so the
	// errgroup unwinds and the pool's drain timer applies. Gate is
	// AlwaysLeader in standalone mode, coordinator-backed in K8s mode;
	// the leader-or-forward middleware in adminapi handles the rest.
	admin := adminapi.New(adminapi.Config{
		HTTPAddr:     cfg.AdminAPI.HTTPAddr,
		SharedSecret: cfg.AdminAPI.SharedSecret,
	}, poolFn, prov, buildAdminGate(cfg, coord), func() {
		log.Warn("admin drain triggered; cancelling root context")
		cancel()
	}, log)

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

	// Phase 2 context is rooted at Background because it must outlive
	// ctx (which fires on SIGTERM). It's cancelled explicitly after
	// phase 1 finishes draining.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	g2, ctx2g := errgroup.WithContext(ctx2)
	g2.Go(func() error { return observability.Serve(ctx2g, cfg.Observability.HTTPAddr, reg, health, log) })
	g2.Go(func() error { return admin.Serve(ctx2g) })
	g2.Go(func() error { return runHealthRefresher(ctx2g, prov, health, log) })

	log.Info("scaleset running", "cluster_mode", cfg.Cluster.Mode)

	phase1Err := g1.Wait()
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
	// Probe once immediately so /readyz can flip green as soon as we start.
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

// buildCoordinator selects the cluster.Coordinator implementation
// based on cfg.Cluster.Mode. Standalone is the default (and only
// option when not running in Kubernetes).
func buildCoordinator(cfg *config.Config, cb cluster.Callbacks, log *slog.Logger) (cluster.Coordinator, error) {
	if cfg.Cluster.Mode != "kubernetes" {
		return cluster.NewStandalone(cfg.AdminAPI.HTTPAddr, cb), nil
	}
	port, err := portFromAddr(cfg.AdminAPI.HTTPAddr)
	if err != nil {
		return nil, fmt.Errorf("cluster: extract admin port for Lease annotation: %w", err)
	}
	return cluster.NewKubernetes(cluster.Config{
		LeaseName:          cfg.Cluster.Kubernetes.LeaseName,
		LeaseNamespace:     cfg.Cluster.Kubernetes.LeaseNamespace,
		Identity:           cfg.Cluster.Kubernetes.Identity,
		PodIP:              cfg.Cluster.Kubernetes.PodIP,
		AdminPort:          port,
		LeaseDuration:      cfg.Cluster.Kubernetes.LeaseDurationD,
		RenewDeadline:      cfg.Cluster.Kubernetes.RenewDeadlineD,
		RetryPeriod:        cfg.Cluster.Kubernetes.RetryPeriodD,
		EndpointAnnotation: cfg.Cluster.Kubernetes.LeaderEndpointAnnotation,
	}, cb, log)
}

// portFromAddr parses ":9101" / "0.0.0.0:9101" / "127.0.0.1:9101" into
// the integer port. Returns 0 (no error) when addr is empty so admin
// API disabled doesn't surface as a startup failure.
func portFromAddr(addr string) (int, error) {
	if addr == "" {
		return 0, nil
	}
	idx := strings.LastIndexByte(addr, ':')
	if idx < 0 {
		return 0, fmt.Errorf("address %q has no port separator", addr)
	}
	portStr := addr[idx+1:]
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
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
// Standalone deployments always serve admin locally; K8s deployments
// either serve locally (when leader) or proxy to the leader.
func buildAdminGate(cfg *config.Config, coord cluster.Coordinator) adminapi.LeaderGate {
	if cfg.Cluster.Mode != "kubernetes" {
		return adminapi.AlwaysLeader{}
	}
	return &coordAdminGate{coord: coord, fwd: cluster.NewForwarder(coord)}
}

// buildNodeSelector translates config into a Selector. Pass the
// provisioner so least_loaded can borrow its Proxmox client.
func buildNodeSelector(cfg *config.Config, prov provisioner.Provisioner) (nodeselector.Selector, error) {
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

// buildGitHubAuth translates config into a githubauth.Auth.
func buildGitHubAuth(cfg *config.Config) (githubauth.Auth, error) {
	switch cfg.GitHub.AuthMode {
	case "pat":
		return githubauth.NewPAT(cfg.GitHub.PAT.Token)
	case "app":
		return githubauth.NewAppFromFile(
			cfg.GitHub.App.Issuer(),
			cfg.GitHub.App.InstallationID,
			cfg.GitHub.App.PrivateKeyPath,
		)
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
// We must distinguish the last case from the second one. The previous
// implementation silently fell through to CreateRunnerScaleSet on any
// non-nil error, turning a 5xx into a misleading "create failed".
func ensureScaleSet(ctx context.Context, gh *scaleset.Client, cfg *config.Config, log *slog.Logger) (*scaleset.RunnerScaleSet, error) {
	// Use runner group 1 (Default) unless overridden.
	groupID := 1
	if cfg.ScaleSet.RunnerGroup != "" {
		rg, err := gh.GetRunnerGroupByName(ctx, cfg.ScaleSet.RunnerGroup)
		if err != nil {
			return nil, fmt.Errorf("get runner group %q: %w", cfg.ScaleSet.RunnerGroup, err)
		}
		groupID = rg.ID
	}
	existing, err := gh.GetRunnerScaleSet(ctx, groupID, cfg.ScaleSet.Name)
	if err != nil {
		return nil, fmt.Errorf("lookup runner scale set %q: %w", cfg.ScaleSet.Name, err)
	}
	if existing != nil {
		return existing, nil
	}
	log.Info("creating runner scale set", "name", cfg.ScaleSet.Name)
	labels := make([]scaleset.Label, 0, len(cfg.ScaleSet.Labels))
	for _, l := range cfg.ScaleSet.Labels {
		labels = append(labels, scaleset.Label{Name: l, Type: "User"})
	}
	created, err := gh.CreateRunnerScaleSet(ctx, &scaleset.RunnerScaleSet{
		Name:          cfg.ScaleSet.Name,
		RunnerGroupID: groupID,
		Labels:        labels,
	})
	if err != nil {
		return nil, fmt.Errorf("create runner scale set: %w", err)
	}
	return created, nil
}
