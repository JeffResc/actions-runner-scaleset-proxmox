# Profiles and scaling

## Hot and warm pools

Two pools trade cost against job-start latency:

- **Hot** VMs are cloned, booted, and idle. A job starts on one in seconds.
  They cost CPU and RAM continuously.
- **Warm** VMs are cloned and configured but powered off. Promoting one to hot
  costs a boot, but they cost almost nothing while parked.

When neither pool has capacity, the orchestrator clones on demand. Set both
sizes under `pool:` for the whole scale set, or per profile to size each
hardware shape independently.

Other `pool:` knobs worth knowing:

| Key | Purpose |
| --- | --- |
| `vm_max_age` | Idle hot/warm VMs older than this are recycled |
| `boot_max_attempts` | After this many guest-agent timeouts a VM is marked `poison` |
| `power_poll_interval` | How often Proxmox is polled for the power state of assigned/running VMs — this is the end-of-job tail latency |
| `vmid_reuse_cooldown` | Minimum wait before the allocator reissues a destroyed VMID, so a fresh clone doesn't race a still-settling `qmdestroy` |
| `orphan_grace` | How long a Proxmox VM may exist without a local row before the orphan sweep destroys it |
| `drain_timeout` | Maximum wait for running jobs on SIGTERM |
| `global_max` | Optional fleet-wide ceiling on the sum of per-profile `max_concurrent_runners` |

## Runner profiles

A scale set can declare one or more **profiles** — named bundles of
`{labels, template VMID, CPU / memory / disk shape, hot/warm/max sizing}`.
Each profile gets its own reconcile loop and pool state. VMs are tagged with
their profile name, so crash recovery routes them back into the right pool on
restart.

```yaml
profiles:
  - name: linux-x64
    labels: [self-hosted, linux, proxmox, x64]
    template_vmid: 9000
    cpu: 4
    memory_mb: 8192
    hot_size: 5
    warm_size: 10
    max_concurrent_runners: 20
  - name: gpu
    labels: [self-hosted, linux, proxmox, gpu]
    template_vmid: 9100
    cpu: 8
    memory_mb: 32768
    hot_size: 0            # explicit 0 — no idle pool, clone on demand
    warm_size: 1
    max_concurrent_runners: 4
```

Configs without a `profiles:` block keep working unchanged — the orchestrator
synthesises a single `default` profile from the global `pool:` and `scaleset:`
blocks.

Sizing knobs (`hot_size`, `warm_size`, `max_concurrent_runners`,
`boot_max_attempts`) accept an explicit `0`. Leave them off entirely to inherit
the global default; set them to `0` to mean zero.

Prometheus metrics are partitioned by `profile=` so dashboards can slice by
hardware shape. See `profiles:` in
[config.example.yaml](../config.example.yaml) for the full schema.

### Label routing

Job-to-profile routing is *best-match by labels*. A profile satisfies a job
when its labels are a **superset** of the job's `RequestLabels`, and the
profile with the smallest extra-label count wins. Ties resolve by declaration
order.

When no profile satisfies a job,
`scaleset_unrouted_jobs_total{labels="..."}` increments so the coverage gap is
visible. Config validation rejects scale sets whose declared labels aren't
collectively covered by some profile, so that misconfiguration is caught at
load time rather than per-job at runtime.

### Per-profile networking

Each profile can declare its own `network:` block overriding the
`proxmox.network` defaults — useful for putting GPU runners on VLAN 30,
untrusted-PR runners on VLAN 99, or build-cache runners on a separate storage
VLAN. Multi-NIC setups are supported via `extra_nics:`.

```yaml
network:
  bridge: vmbr1
  vlan_tag: 30
  mtu: 9000
  extra_nics:
    - bridge: vmbr-storage
      vlan_tag: 100
      mtu: 9000
  ipam:
    backend: static
    pool:
      - 10.0.30.10/24
      - 10.0.30.11/24
```

An optional `ipam:` selector picks the IP allocator:

- **`noop`** (default) — DHCP via Proxmox cloud-init.
- **`static`** — an in-memory pool fed by `ipam.pool: [...]`.

The pool manager calls `Allocate` before each clone and `Release` on destroy,
so allocations don't leak across recycles. External IPAM backends (NetBox,
Infoblox, phpIPAM, and so on) plug in behind the same `ipam.Allocator`
interface; none ship in-tree yet because they need a live IPAM to verify.

## Scheduled pool sizes

Each profile can declare cron-driven hot/warm size overrides under
`schedules:`, so steady-state capacity tracks demand:

```yaml
schedules:
  - name: business-hours
    cron: "0 8 * * 1-5"          # 8am Mon-Fri
    duration: 10h
    timezone: America/New_York
    hot_size: 10
    warm_size: 20
  - name: night-mode
    cron: "0 18 * * 1-5"         # 6pm Mon-Fri
    duration: 14h                # extends to 8am next day
    timezone: America/New_York
    hot_size: 2
    warm_size: 5
```

Each entry takes a `cron` expression (standard 5-field robfig/cron syntax plus
`@hourly` / `@daily` / etc.), a `duration` for how long the override stays
active after each fire, an optional `timezone` (IANA name, defaults to UTC),
and explicit `hot_size` / `warm_size`.

Behaviour worth knowing:

- **Overlaps resolve as last-fired wins.** When two windows are open
  simultaneously, the one whose cron tick was more recent applies.
- **Startup replays the most recent past fire.** Restarting at 02:00 inside a
  "midnight + 8h" window re-applies the night override instead of briefly
  snapping back to the profile baseline.
- **Window close reverts to baseline.** When a window closes with no other
  override active, the reconcile loop returns to the profile's configured
  `hot_size` / `warm_size`.
- **Sizes are clamped** to `max_concurrent_runners`, with hot trimmed last.

Fires increment `scaleset_schedule_fires_total{profile, schedule}`. The
currently-active override is exposed as
`scaleset_schedule_active{profile, schedule}`, where `schedule=""` represents
the baseline.

## Template canary rollouts

Each profile can stage a new template image via `canary_template_vmid` and
`canary_percent`:

```yaml
canary_template_vmid: 9101      # staging template
canary_percent: 10              # ~10% of new clones use the candidate
canary_max_failure_rate: 0.2    # auto-revert at 20% canary boot failures
```

Roughly `canary_percent`% of new clones use the candidate template; the rest
stay on the stable `template_vmid`. The dice is deterministic — a hash of the
allocated VMID — so retries of the same VMID always pick the same template and
the orchestrator never accidentally rolls back to stable mid-clone.

Boot failures on canary VMs feed a cumulative failure-rate counter. Once the
rate exceeds `canary_max_failure_rate` (with at least a small statistical
sample) the orchestrator auto-reverts `canary_percent` to 0 in-process and
increments `scaleset_canary_reverted_total{profile}`. Investigate before
re-enabling.

When you're confident in the candidate,
`POST /admin/template/promote/{profile}` atomically swaps it into the stable
slot.

> Both the auto-revert and the promotion are **in-process only**. To persist
> either across a restart, also update `template_vmid` in the YAML.

## Multi-tenancy: quotas and priority

Optional `quotas:` and `priority:` blocks cap concurrent VMs per org or repo,
and classify jobs into priority lanes.

> **Both blocks are observational today.** The `actions/scaleset` listener
> interface surfaces per-job metadata (`OwnerName`, `RepositoryName`,
> `RequestLabels`, `JobWorkflowRef`) only *after* GitHub has paired a job with
> a VM. At-acquire-time admission control needs a deeper listener-integration
> extension, which is deferred. Configure these for visibility; future
> enforcement will reuse the same config shape.

What ships today:

- **Stamping** — when `JobStarted` fires, the scaler records the job's `Org`,
  `Repo`, and `PriorityClass` on the VM row.
- **Counters** — `scaleset_quota_throttled_total{scope, name}` fires when a
  stamped row pushes its (org or repo) bucket past the configured cap.
  `scaleset_priority_acquires_total{class}` partitions every `JobStarted` by
  its class.
- **Manual preempt** — `POST /admin/preempt/{vmid}` destroys an
  assigned-but-not-yet-running VM via the pool's safety-gated `Preempt` API.
  Running VMs are refused with HTTP 409; preempting an actively-executing job
  is the destructive behaviour the orchestrator explicitly promises never to
  do. `scaleset_preemptions_total{from_class, to_class}` records each
  successful preempt.

```yaml
quotas:
  default_per_repo: 5
  default_per_org: 20
  overrides:
    - match: { repo: "acme/heavy-ci" }
      max_concurrent: 15

priority:
  classes:
    - name: critical
      match: { workflow_label: "priority:critical" }
      weight: 100
      preempt: true
    - name: standard
      weight: 10
```

Each quota override must set exactly one of `org` or `repo`; the
`default_per_*` knobs apply when nothing more specific matches. `0` disables a
knob — an override of `0` explicitly opts a scope out of the default cap.

For priority classes, every non-empty selector field must equal the job's
corresponding field; empty selectors are wildcards. Same-specificity ties
resolve by declaration order, so list classes from most to least important.

See `quotas:` and `priority:` in
[config.example.yaml](../config.example.yaml) for the full YAML surface.
