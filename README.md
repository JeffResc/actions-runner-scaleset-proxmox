# actions-runner-scaleset-proxmox

> Ephemeral, single-use GitHub Actions self-hosted runners backed by Proxmox VMs.

A Go service that orchestrates GitHub Actions self-hosted runners as ephemeral Proxmox VMs. Each job runs in a fresh single-use VM that is destroyed after the job completes. Hot and warm pools keep job start times low; both single-node and clustered Proxmox topologies are supported.

## Status

Pre-1.0, in active development.

## How it works

The service implements the [`actions/scaleset`](https://github.com/actions/scaleset) `Scaler` interface and long-polls GitHub for runner-demand signals. When a runner is needed it either (a) takes a fully-booted VM from the **hot pool**, (b) starts a pre-cloned VM from the **warm pool**, or (c) clones a new one from the template VMID. The JIT runner configuration is injected via the QEMU guest agent (no SSH required) and a systemd path-unit inside the VM picks it up and starts the runner. When the job finishes the runner unit's `ExecStopPost=poweroff` shuts the VM down; the orchestrator's power-state poller observes that and queues destruction.

A second control loop — the **GitHub REST reconciler** — periodically lists the runners API and joins the result against the local DB. It is the backstop for the listener occasionally dropping `JobStarted` / `JobCompleted` callbacks or delivering them with empty fields: rows stuck in `assigned` past `assigned_grace`, `running` rows whose runner went idle past `running_idle_grace`, and runners that registered then went offline are all force-destroyed. It also sweeps Proxmox for VMs that carry our owner tags but have no matching DB row (with `orphan_grace` to avoid racing the boot pipeline), and removes orphan GitHub runner registrations whose VM is gone.

State lives in-process in [hashicorp/go-memdb](https://github.com/hashicorp/go-memdb) — no on-disk DB, no migrations. On startup the orchestrator reconciles its empty view against Proxmox by listing VMs tagged as owned by this scale set; any leftovers from a previous process are destroyed.

Proxmox node placement is pluggable via `nodes.strategy`: **`single`** (always the same node, for single-node PVE), **`round_robin`** (rotate through a configured member list), or **`least_loaded`** (periodically polls `/cluster/resources` and picks the node with the lowest weighted CPU + memory load).

An optional `nodes.affinity:` block layers profile-keyed rules over the chosen strategy: pin a profile to specific nodes with `prefer_nodes: [...]` (combine with `require: true` for a hard pin that fails the clone when no preferred node is eligible), and keep two profiles off the same node with `anti_affinity_with: { profile: ... }`. Rules apply before the underlying strategy picks among the surviving candidates so rotation / load balancing keep their semantics within the eligible set. Hard-pin failures surface as `nodeselector.ErrAffinityRequireUnsatisfiable`. Config validation rejects rules that name an undeclared profile or node — typos surface at load time rather than as silent no-ops at runtime.

## Runner profiles

A scale set can declare one or more **profiles** — named bundles of `{labels, template VMID, CPU / memory / disk shape, hot/warm/max sizing}`. Each profile gets its own reconcile loop and pool state; VMs are tagged with their profile name so crash recovery routes them back into the right pool on restart. Configs without a `profiles:` block keep working unchanged — the orchestrator synthesises a single `default` profile from the global `pool:` / `scaleset:` blocks. See `profiles:` in [config.example.yaml](config.example.yaml) for the full schema. Prometheus metrics are partitioned by `profile=` so dashboards can slice by hardware shape.

Job-to-profile routing is _best-match by labels_: a profile satisfies a job when its labels are a superset of the job's `RequestLabels`, and the profile with the smallest extra-label count wins (ties resolve by declaration order). When no profile satisfies a job, `scaleset_unrouted_jobs_total{labels="..."}` increments so operators can spot the coverage gap. Config validation rejects scale sets whose declared labels aren't collectively covered by some profile — that misconfiguration is caught at load time rather than per-job at runtime.

Each profile can declare its own `network:` block to override `proxmox.network` defaults — useful for putting GPU runners on VLAN 30, untrusted-PR runners on VLAN 99, or build-cache runners on a separate storage VLAN. Multi-NIC setups are supported via `extra_nics: [...]`. An optional `ipam:` selector picks the IP allocator: `noop` (default; DHCP via Proxmox cloud-init) or `static` (in-memory pool fed by `ipam.pool: [...]`). External IPAM backends (NetBox, Infoblox, phpIPAM, etc.) plug in behind the same `ipam.Allocator` interface — none ship in-tree yet because they need a live IPAM to verify. The pool manager calls `Allocate` before each clone and `Release` on destroy so allocations don't leak across recycles.

## Scheduled pool sizes

Each profile can declare a list of cron-driven hot/warm size overrides under `schedules:` so steady-state capacity tracks demand (e.g. `hot=10 warm=20` during business hours, `hot=0 warm=2` overnight). Each entry has a `cron` expression (standard 5-field robfig/cron syntax + `@hourly`/`@daily`/etc.), a `duration` for how long the override stays active after each fire, an optional `timezone` (IANA name; defaults to UTC), and explicit `hot_size`/`warm_size`. Overlapping schedules resolve as **last-fired wins** — when two windows are simultaneously open, the one whose cron tick was more recent applies. On startup the runner replays each schedule's most recent past fire so restarting at 02:00 inside a "midnight + 8h" window re-applies the night override instead of briefly snapping back to the profile baseline. When a window closes with no other override active, the reconcile loop reverts to the profile's configured `hot_size`/`warm_size`. Sizes are clamped to `max_concurrent_runners` (hot trimmed last). Fires increment `scaleset_schedule_fires_total{profile, schedule}`; the currently-active override is exposed as `scaleset_schedule_active{profile, schedule}` (`schedule=""` represents baseline).

## Template canary rollouts

Each profile can stage a new template image via `canary_template_vmid` + `canary_percent`: ~N% of new clones use the candidate template, the rest stay on the stable `template_vmid`. The dice is deterministic (hash of the allocated VMID), so retries of the same VMID always pick the same template — the orchestrator never accidentally rolls back to stable mid-clone. Boot failures on canary VMs feed a cumulative failure-rate counter; once the rate exceeds `canary_max_failure_rate` (with at least a small statistical sample) the orchestrator auto-reverts `canary_percent` to 0 in-process and increments `scaleset_canary_reverted_total{profile}`. Operators investigate before re-enabling. When confident in the candidate, `POST /admin/template/promote/{profile}` atomically swaps it into the stable slot. Both the auto-revert and the promotion are **in-process only** — to persist across a restart, also update `template_vmid` in the YAML.

## Multi-tenancy: quotas + priority

Optional `quotas:` and `priority:` blocks cap concurrent VMs per org or per repo, and classify jobs into priority lanes for visibility. Both are **observational today** — the `actions/scaleset` listener interface surfaces per-job metadata (`OwnerName`, `RepositoryName`, `RequestLabels`, `JobWorkflowRef`) only AFTER GitHub has paired a job with a VM, so at-acquire-time admission control needs a deeper listener-integration extension (deferred to a future PR). What ships today:

- **Stamping**: when `JobStarted` fires the scaler records the job's `Org`/`Repo`/`PriorityClass` on the VM row.
- **Counters**: `scaleset_quota_throttled_total{scope, name}` fires when a stamped row pushes its (org or repo) bucket past the configured cap; `scaleset_priority_acquires_total{class}` partitions every JobStarted by its class.
- **Manual preempt**: `POST /admin/preempt/{vmid}` destroys an Assigned-but-not-yet-Running VM via the pool's safety-gated `Preempt` API. Running VMs are refused (HTTP 409) — preempting an actively-executing job is the destructive behaviour the orchestrator explicitly promises never to do. `scaleset_preemptions_total{from_class, to_class}` records each successful preempt.

See `quotas:` and `priority:` in [config.example.yaml](config.example.yaml) for the YAML surface.

## Components

| Package | Purpose |
| --- | --- |
| `cmd/scaleset` | Entrypoint and CLI subcommands |
| `internal/config` | YAML configuration with env-var expansion |
| `internal/githubauth` | GitHub App and PAT authentication |
| `internal/scaler` | `scaleset.Scaler` implementation |
| `internal/pool` | Pool manager + VM state machine + reconcile loop |
| `internal/provisioner` | Proxmox client wrapper + JIT injection |
| `internal/nodeselector` | Cluster node placement strategies (`single` / `round_robin` / `least_loaded`) |
| `internal/store` | In-memory state via `hashicorp/go-memdb` |
| `internal/tags` | Proxmox tag schema identifying VMs owned by this scale set |
| `internal/gh` | GitHub REST reconciler — backstop for missed listener callbacks; orphan sweeps |
| `internal/adminapi` | Token-protected admin HTTP API (state, drain, force-destroy) |
| `internal/observability` | Structured logging, Prometheus metrics, OTLP/HTTP tracing, health probes |
| `internal/cluster` | Leader election (standalone or Kubernetes Lease) + admin-API reverse-proxy to leader |

The source under `internal/...` is the canonical reference for behaviour; package-level doc comments cover each subsystem's contract and design rationale.

## Observability

`observability.http_addr` exposes `/metrics` (Prometheus), `/healthz`, and `/readyz` on every replica. Readiness is leader-aware: standbys are ready as long as Proxmox is reachable within the staleness window (so they can take over); leaders additionally require the scaleset listener to have connected and crash-recovery to have completed.

OTLP/HTTP tracing is opt-in via `observability.tracing.endpoint`. When empty, instrumented code paths use the no-op tracer and pay zero overhead. `sample_ratio` accepts `[0.0, 1.0]`.

## Admin API

Optional escape-hatch HTTP API enabled by setting `admin_api.http_addr` and supplying the bearer secret via the `SCALESET_ADMIN_API_SHARED_SECRET` env var. Every endpoint requires `Authorization: Bearer <shared-secret>`; failed auth attempts are rate-limited per source IP.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/admin/state` | Current pool stats (counts per lifecycle state) |
| `POST` | `/admin/drain` | Trigger a graceful drain (bounded by `pool.drain_timeout`) |
| `POST` | `/admin/preempt/{vmid}` | Preempt an Assigned VM (refuses Running; 409 when not preempt-eligible) |
| `POST` | `/admin/template/promote/{profile}` | Atomically swap a profile's canary candidate template into stable (409 when no candidate; 503 during leader transition) |
| `POST` | `/admin/destroy/{vmid}` | Force-destroy a specific VM regardless of its state |

In Kubernetes multi-replica deployments the admin API is bound on every replica. Non-leader replicas reverse-proxy requests to the leader (whose endpoint is published in a Lease annotation), so callers don't need to know which pod holds the lease.

## Deployment modes

The same binary supports both single-process and Kubernetes multi-replica deployments. The mode is selected by `cluster.mode` in `config.yaml`:

- **`standalone`** (default) — the process is always leader. Used for the Docker image at [deploy/docker/Dockerfile](deploy/docker/Dockerfile) and the systemd unit at [deploy/systemd/scaleset.service](deploy/systemd/scaleset.service).
- **`kubernetes`** — N replicas elect a leader via a `coordination.k8s.io/v1` Lease (`k8s.io/client-go/tools/leaderelection`). Only the leader runs the control plane (GitHub scaleset listener, REST reconciler, pool manager + Proxmox power-state poller); standbys serve only `/healthz`, `/readyz`, and `/metrics` until promoted. The admin API is bound on every replica and reverse-proxies to the leader (whose endpoint is published in a Lease annotation), so the design is safe to deploy through Flux/Argo CD — the controller only writes to the Lease it created.

A Helm chart for Kubernetes deployment lives in [deploy/chart/](deploy/chart/).

## Development

Common workflows ship as [Taskfile](https://taskfile.dev) targets — `task --list` to discover them:

| Target | Purpose |
| --- | --- |
| `task test` | Unit tests, race-enabled, no build tags |
| `task e2e` | In-process e2e suite against fake Proxmox + fake GitHub (no external deps; ~30s) |
| `task e2e-packer` | `packer validate -syntax-only` on the HCL (skips if Packer not installed) |
| `task build` | Compile `bin/scaleset` |
| `task lint` | `golangci-lint` over the module |

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full dev setup.

## License

GNU General Public License 3.0 — see [LICENSE](LICENSE).
