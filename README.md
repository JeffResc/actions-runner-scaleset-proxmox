# github-actions-proxmox-scaleset

A Go service that orchestrates GitHub Actions self-hosted runners as ephemeral Proxmox VMs — the Proxmox equivalent of GitLab's fleeting VM-based runners. Each job runs in a fresh single-use VM that is destroyed after the job completes. Hot and warm pools keep job start times low; both single-node and clustered Proxmox topologies are supported.

## Status

Pre-1.0, in active development.

## How it works

The service implements the [`actions/scaleset`](https://github.com/actions/scaleset) `Scaler` interface and long-polls GitHub for runner-demand signals. When a runner is needed it either (a) takes a fully-booted VM from the **hot pool**, (b) starts a pre-cloned VM from the **warm pool**, or (c) clones a new one from the template VMID. The JIT runner configuration is injected via the QEMU guest agent (no SSH required) and a systemd path-unit inside the VM picks it up and starts the runner. When the job finishes the runner unit's `ExecStopPost=poweroff` shuts the VM down; the orchestrator's power-state poller observes that and queues destruction.

State lives in-process in [hashicorp/go-memdb](https://github.com/hashicorp/go-memdb) — no on-disk DB, no migrations. On startup the orchestrator reconciles its empty view against Proxmox by listing VMs tagged as owned by this scale set; any leftovers from a previous process are destroyed.

## Components

| Package | Purpose |
| --- | --- |
| `cmd/scaleset` | Entrypoint and CLI subcommands |
| `internal/config` | YAML configuration with env-var expansion |
| `internal/githubauth` | GitHub App and PAT authentication |
| `internal/scaler` | `scaleset.Scaler` implementation |
| `internal/pool` | Pool manager + VM state machine + reconcile loop |
| `internal/provisioner` | Proxmox client wrapper + JIT injection |
| `internal/nodeselector` | Cluster node placement strategies |
| `internal/store` | In-memory state via `hashicorp/go-memdb` |
| `internal/observability` | Structured logging + Prometheus metrics |

The source under `internal/...` is the canonical reference for behaviour; package-level doc comments cover each subsystem's contract and design rationale.

## License

Apache 2.0 — see [LICENSE](LICENSE).
