# Configuration

[config.example.yaml](../config.example.yaml) is the canonical reference —
every key is present there with inline documentation. This page explains the
shape of the file and the rules that aren't obvious from reading it.

The orchestrator loads `config.yaml` from the path given by `--config`
(default `config.yaml`).

## Top-level blocks

| Block | Purpose | Detail |
| --- | --- | --- |
| `github:` | Auth mode (PAT or App), scope, REST-reconciler timings | [getting-started.md](getting-started.md#3-set-up-github-credentials) |
| `scaleset:` | Name, labels, runner group, concurrency ceiling | below |
| `scalesets:` | Multi-scale-set alternative to `scaleset:` | [multi-scaleset.md](multi-scaleset.md) |
| `proxmox:` | Endpoint, API token, template VMID, VMID range, storage, network | [getting-started.md](getting-started.md#4-write-a-minimal-configyaml) |
| `nodes:` | Node-placement strategy and affinity rules | [node-placement.md](node-placement.md) |
| `pool:` | Default hot/warm sizing and lifecycle timings | [profiles-and-scaling.md](profiles-and-scaling.md) |
| `profiles:` | Named hardware/label/template bundles | [profiles-and-scaling.md](profiles-and-scaling.md) |
| `quotas:` / `priority:` | Per-org/repo caps and priority classes | [profiles-and-scaling.md](profiles-and-scaling.md#multi-tenancy-quotas-and-priority) |
| `observability:` | Metrics/health address, log level, OTLP tracing | [operations.md](operations.md) |
| `admin_api:` | Optional admin HTTP API | [operations.md](operations.md#admin-api) |
| `cluster:` | `standalone` or `raft` leader election | [deployment.md](deployment.md#high-availability-with-raft) |

## Environment variable overrides

Every key is overridable by an environment variable named with the YAML key
path uppercased, dots replaced by underscores, and prefixed with `SCALESET_`.
`snake_case` YAML keys are preserved as single tokens:

| YAML key | Environment variable |
| --- | --- |
| `github.pat.token` | `SCALESET_GITHUB_PAT_TOKEN` |
| `proxmox.auth.token_secret` | `SCALESET_PROXMOX_AUTH_TOKEN_SECRET` |
| `admin_api.shared_secret` | `SCALESET_ADMIN_API_SHARED_SECRET` |
| `pool.hot_size` | `SCALESET_POOL_HOT_SIZE` |

Note that `pool.hot_size` is one key — it is **not** nested as `hot.size`.

Unknown `SCALESET_*`-prefixed variables are logged at startup, so a typo like
`SCALESET_POOL_HOTSIZE` is loud rather than a silent no-op.

## Secrets

Secrets must come from the environment (or another secure config-loader
source). Do not write them into YAML. A typical PAT deployment needs:

- `SCALESET_GITHUB_PAT_TOKEN`
- `SCALESET_PROXMOX_AUTH_TOKEN_SECRET`
- `SCALESET_ADMIN_API_SHARED_SECRET` — required when `admin_api.http_addr` is
  set

## Validation

Config is validated fully before anything connects, so misconfiguration is a
startup error rather than a runtime surprise. Notable rules:

- `github.auth_mode` must be `app` or `pat`, and the matching block must be
  present.
- `github.scope` must set exactly one of `org` or `repo`.
- A scale set's declared `labels` must be collectively covered by some
  profile — otherwise jobs matching those labels could never be routed.
- `nodes.affinity` rules naming an undeclared profile or node are rejected.
- With more than one scale set declared, every entry must carry its own
  `vmid_range` and the ranges must be pairwise disjoint.
- Mixing the singular `scaleset:` form with the plural `scalesets:` form is
  rejected, with an error naming the offending legacy keys.

## GitHub Enterprise Server

Two mutually exclusive overrides point the orchestrator at a GHES instance.
Both live under `github.pat:` or `github.app:` depending on your auth mode:

- **`config_url`** — a full URL with the org/repo baked in. Use this for a
  single scale set. It is rejected in multi-scale-set deployments, since every
  scale set would be forced to handshake against the same scope.
- **`config_base_url`** — scheme and host only. The orchestrator joins it with
  each scale set's scope at runtime, so per-scale-set clients hit the right
  `/<org>` path on the shared host. This is the required form for
  multi-scale-set GHES.

For App auth, `rest_base_url` additionally overrides the base URL used for the
installation-token fetch and subsequent REST calls (typically
`https://ghes.example.com/api/v3/`).

Leave all of these empty when running against github.com — the default
per-scope URL derivation already targets the right endpoint.
