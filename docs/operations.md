# Operations

## Health probes

`observability.http_addr` (default `:9100`) exposes three endpoints on every
replica:

| Endpoint | Meaning |
| --- | --- |
| `/healthz` | The process is alive |
| `/readyz` | Leader-aware readiness — see below |
| `/metrics` | Prometheus exposition |

Readiness is leader-aware. Standbys are ready as long as Proxmox is reachable
within the staleness window, so they can take over. Leaders additionally
require the scaleset listener to have connected and crash recovery to have
completed. With multiple scale sets, *every* registered scale set must signal
both conditions — one stalled scale set holds the gate red.

## Metrics

All metrics are namespaced `scaleset_`. Every per-scale-set metric carries
`scaleset` as its first label; profile-keyed metrics carry `profile` next.

### Pool and capacity

| Metric | Labels | Notes |
| --- | --- | --- |
| `scaleset_pool_size` | `scaleset, profile, state` | VMs per lifecycle state — the primary dashboard gauge |
| `scaleset_vms_total` | `scaleset, profile, outcome` | Cumulative VMs created, by outcome |
| `scaleset_at_capacity_total` | `scaleset` | Acquires rejected at `max_concurrent_runners` |
| `scaleset_pool_destroy_backlog_depth` | `scaleset` | Depth of the destroy dispatcher queue |
| `scaleset_pool_destroy_backlog_full_total` | `scaleset, profile` | Destroys dropped because the queue was full |

### Latency

| Metric | Labels | Notes |
| --- | --- | --- |
| `scaleset_acquire_duration_seconds` | `scaleset, profile, tier` | Acquire to ready VM. `tier` tells you whether it came from hot, warm, or a fresh clone |
| `scaleset_clone_duration_seconds` | `scaleset, profile, linked, node` | Template clone time |
| `scaleset_boot_duration_seconds` | `scaleset, profile, node` | VM start to guest-agent ready |
| `scaleset_reconcile_duration_seconds` | `scaleset` | One pool reconciliation pass |

### Routing, quotas, and priority

| Metric | Labels | Notes |
| --- | --- | --- |
| `scaleset_unrouted_jobs_total` | `scaleset, labels` | No profile matched the job's labels. `labels` is hashed into 64 buckets to bound cardinality |
| `scaleset_labels_drift` | `scaleset` | 1 when the labels on GitHub disagree with `scaleset.labels` and the orchestrator could not repair them — it was busy, already recreated once this process, or the delete/create failed |
| `scaleset_quota_throttled_total` | `scaleset, scope, name` | Observed over-quota jobs (observational — see [profiles-and-scaling.md](profiles-and-scaling.md#multi-tenancy-quotas-and-priority)) |
| `scaleset_priority_acquires_total` | `scaleset, class` | Jobs paired with a VM, by priority class |
| `scaleset_preemptions_total` | `scaleset, from_class, to_class` | Successful preempts |

### Templates and schedules

| Metric | Labels | Notes |
| --- | --- | --- |
| `scaleset_canary_reverted_total` | `scaleset, profile` | Canary auto-reverted to 0% |
| `scaleset_schedule_fires_total` | `scaleset, profile, schedule` | Cron fires that applied an override |
| `scaleset_schedule_active` | `scaleset, profile, schedule` | 1 for the currently-applying override; `schedule=""` is the baseline |

### Upstream APIs and cluster

| Metric | Labels | Notes |
| --- | --- | --- |
| `scaleset_proxmox_api_errors_total` | `scaleset, operation, node` | |
| `scaleset_github_api_errors_total` | `scaleset, endpoint` | |
| `scaleset_gh_api_calls_total` | `scaleset, endpoint, status` | |
| `scaleset_gh_rate_limit_remaining` | `scaleset` | Latest `X-RateLimit-Remaining` seen |
| `scaleset_gh_runner_state_mismatch_total` | `scaleset, db_state, gh_state, action` | Reconciler divergences and what it did about them |
| `scaleset_reconcile_errors_total` | `scaleset, op` | Errors inside the REST reconciler |
| `scaleset_listener_messages_total` | `scaleset, kind` | Inbound listener messages |
| `scaleset_runner_hook_events_total` | `scaleset, phase, result` | Events from in-VM runner lifecycle hooks |
| `scaleset_leader` | — | 1 when this replica holds leadership. Always 1 in standalone mode |
| `scaleset_panics_recovered_total` | `scaleset, op` | Panics caught by the pool's worker guards |

### Alerts worth having

- `rate(scaleset_panics_recovered_total[5m]) > 0` — always a real bug.
- `rate(scaleset_unrouted_jobs_total[15m]) > 0` — a label coverage gap; jobs
  are queueing with nothing to run them.
- `scaleset_labels_drift > 0` — GitHub does not hold the configured labels.
  GitHub routes no job that asks for a label it does not know, so those jobs
  queue with no other symptom: the listener stays connected, the pool stays
  idle, and `/readyz` stays green.

- `rate(scaleset_pool_destroy_backlog_full_total[5m]) > 0` — destroys are being
  dropped and VMs will leak until the orphan sweep catches them.
- `scaleset_canary_reverted_total` increasing — a bad template candidate.
- `scaleset_gh_rate_limit_remaining` trending toward zero.

### Changing a scale set's labels

GitHub cannot change the labels of a registered runner scale set. The update
endpoint accepts a new label set, answers `200`, and applies nothing — probed
against the live API with three body shapes, all of which read back unchanged.
The orchestrator therefore reconciles by **deleting the scale set and creating
it again** with the configured labels, at startup, before the listener opens a
session.

What that means for an operator:

- The scale set gets a **new ID**. Runner registrations and JIT configs issued
  against the old one are void, so the orchestrator does this only when GitHub
  reports the scale set idle — no queued, assigned, or running jobs and no
  registered runners. A busy scale set keeps its labels and sets
  `scaleset_labels_drift`; the next start retries.
- It happens **at most once per scale set per process**. If the labels still
  disagree afterwards, the orchestrator reports drift rather than recreating in
  a loop.
- If the delete succeeds and the create fails, the scale set is gone from
  GitHub. The worker fails, its supervisor retries, and the retry re-registers
  it through the normal create path. Jobs queue until it comes back.
- An entry with no `labels:` is left alone entirely — that is how you adopt a
  scale set whose labels you manage elsewhere.

## Tracing

OTLP/HTTP tracing is opt-in:

```yaml
observability:
  tracing:
    endpoint: "otel-collector:4318"   # host:port only
    insecure: false                   # true = plain HTTP
    sample_ratio: 1.0                 # [0.0, 1.0]
```

When `endpoint` is empty, instrumented code paths use the no-op tracer and pay
zero overhead.

## Logging

```yaml
observability:
  log_level: info                     # debug | info | warn | error
  log_format: json                    # json | text
```

## Admin API

An optional escape hatch, enabled by setting `admin_api.http_addr` and
supplying the bearer secret via `SCALESET_ADMIN_API_SHARED_SECRET`. Every
endpoint requires `Authorization: Bearer <shared-secret>`; failed auth attempts
are rate-limited per source IP.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/admin/state` | Current pool stats (counts per lifecycle state) |
| `POST` | `/admin/drain` | Trigger a graceful drain, bounded by `pool.drain_timeout` |
| `POST` | `/admin/preempt/{vmid}` | Preempt an assigned VM. Refuses running VMs with 409 |
| `POST` | `/admin/template/promote/{profile}` | Swap a profile's canary candidate into the stable slot. 409 when there is no candidate, 503 during a leader transition |
| `POST` | `/admin/destroy/{vmid}` | Force-destroy a specific VM regardless of state |

```sh
curl -s -H "Authorization: Bearer $SECRET" localhost:9101/admin/state | jq
```

Every endpoint except `/admin/drain` also accepts a `/admin/{scaleset}/...`
prefix, so operators running multiple scale sets in one process can
disambiguate. Unknown scale set names return 404; the un-prefixed paths route
to the single configured scale set. See
[multi-scaleset.md](multi-scaleset.md#metrics-and-admin-endpoints).

In multi-replica deployments the admin API is bound on every replica, and
non-leaders reverse-proxy to the leader — callers don't need to know which
replica is in charge. See
[deployment.md](deployment.md#high-availability-with-raft).

## Runbook notes

**Draining for maintenance.** `POST /admin/drain` stops accepting new jobs,
lets running jobs finish, and destroys idle pool VMs. It is bounded by
`pool.drain_timeout`. SIGTERM does the same thing.

**Promoting a canary.** Watch `scaleset_canary_reverted_total` and boot
failures for the profile, then
`POST /admin/template/promote/{profile}`. **Also update `template_vmid` in the
YAML** — the promotion is in-process only and will not survive a restart.

**Preempting.** `POST /admin/preempt/{vmid}` only works on
assigned-but-not-yet-running VMs. Running VMs return 409 by design; the
orchestrator never kills an executing job.

**Poisoned VMs.** After `pool.boot_max_attempts` guest-agent failures a VM
enters the `poison` state and is left in Proxmox for inspection. Check the
template's `qemu-guest-agent`, then clear it with
`POST /admin/destroy/{vmid}`.

**Leaked VMs.** VMs carrying the owner tag with no matching local row are
destroyed by the orphan sweep after `pool.orphan_grace`. If VMs are
accumulating faster than that, check
`scaleset_pool_destroy_backlog_full_total`.
