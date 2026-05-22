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

Optional escape-hatch HTTP API enabled by setting `admin_api.http_addr` and `admin_api.shared_secret_env`. Every endpoint requires `Authorization: Bearer <shared-secret>`; failed auth attempts are rate-limited per source IP.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/admin/state` | Current pool stats (counts per lifecycle state) |
| `POST` | `/admin/drain` | Trigger a graceful drain (bounded by `pool.drain_timeout`) |
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

Apache 2.0 — see [LICENSE](LICENSE).
