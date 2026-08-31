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
| `recycle_mode` | `destroy` (default) or `snapshot_rollback` — reuse VMs by rolling back to a post-clone snapshot instead of destroying them. See [Recycling VMs with snapshot rollback](#recycling-vms-with-snapshot-rollback) |
| `boot_max_attempts` | After this many guest-agent timeouts a VM is marked `poison` |
| `power_poll_interval` | How often Proxmox is polled for the power state of assigned/running VMs — this is the end-of-job tail latency |
| `vmid_reuse_cooldown` | Minimum wait before the allocator reissues a destroyed VMID, so a fresh clone doesn't race a still-settling `qmdestroy` |
| `orphan_grace` | How long a Proxmox VM may exist without a local row before the orphan sweep destroys it |
| `drain_timeout` | Maximum wait for running jobs on SIGTERM |
| `global_max` | Optional fleet-wide ceiling on the sum of per-profile `max_concurrent_runners` |
| `capacity` | Opt-in resource-aware admission — gate clones on what actually fits on each node. See [Resource-aware admission control](#resource-aware-admission-control) |

### Recycling VMs with snapshot rollback

By default (`pool.recycle_mode: destroy`) every VM is torn down after its job
and a replacement is cloned from the template. On storage without linked-clone
support that costs a full disk copy per job — minutes of I/O.

Set `pool.recycle_mode: snapshot_rollback` to reuse VMs instead. Each clone is
snapshotted once (`scaleset-clean`) right after clone, while it is still stopped
and before the runner's first boot. When a job finishes, the orchestrator rolls
the VM back to that snapshot and returns it to the warm pool instead of
destroying it — typically ~15s versus minutes for a full re-clone. The rollback
resets the disk exactly like a fresh clone, so isolation between jobs is
unchanged: the GitHub runner is deregistered and a new single-use JIT
registration is minted for each job.

- **Storage:** requires snapshot-capable storage (ZFS, LVM-thin, qcow2, Ceph
  RBD, LINSTOR, ...). On storage that cannot snapshot (raw disks, raw-on-NFS),
  the clone-time snapshot fails and the VM silently falls back to the destroy
  path, so no recycling happens.
- **Template updates still land:** `vm_max_age` still forces a periodic full
  destroy + re-clone, so a recycled VM is eventually replaced and picks up new
  template images. The VM's original clone time is preserved across recycles, so
  age is measured from the first clone, not the last rollback.
- **Fallback:** any rollback failure falls back to the destroy path, so the pool
  self-heals with a fresh clone.
- **Pool-level:** `recycle_mode` applies to every profile of the scale set;
  there is no per-profile override.

Metrics:

| Metric | Meaning |
| --- | --- |
| `scaleset_recycles_total` | VMs returned to the warm pool via snapshot rollback instead of destroyed |
| `scaleset_recycle_failures_total` | Rollbacks that failed and fell back to the destroy path |
| `scaleset_clone_suppressed_total` | Replacement clones skipped because a busy VM will return to the pool via rollback |

## Resource-aware admission control

By default the only bound on how many VMs exist is the static
`max_concurrent_runners` count per profile. With heterogeneous profiles that
forces a bad trade: you must size each profile for its worst case, so the node
sits underused whenever the jobs that turn up are small — and it can still be
oversubscribed if you tuned the numbers optimistically.

Consider a 32 GiB node serving three shapes:

| profile | cpu | memory_mb | `max_concurrent_runners` |
| --- | --- | --- | --- |
| `mem-4g` | 2 | 4096 | 2 |
| `mem-8g` | 4 | 8192 | 1 |
| `mem-16g` | 8 | 16384 | 1 |

Those maxima reserve 32 GiB of worst case regardless of what is running. Raise
any of them and the node can be overcommitted instead.

`pool.capacity` replaces that guesswork with the real number:

```yaml
pool:
  capacity:
    enabled: true
    reserve_memory_mb: 4096
```

A clone is now admitted only if it also fits in the target node's remaining
**allocated** memory:

```
available = node RAM − host reserve − Σ configured memory of every guest on the node
```

Three things about that sum are load-bearing:

- **Allocated, not used.** A booted-but-idle 16 GiB runner reports almost no
  resident memory while owning its full 16 GiB. Planning against usage would
  admit VMs the node cannot actually back.
- **Every *running* guest counts**, including VMs and LXC containers this
  orchestrator does not own. A node's capacity is a property of the node, not of
  the pool.

  Powered-off foreign guests do **not** count: Proxmox reserves nothing for
  them, so withholding their configured memory would refuse clones over
  capacity that physically exists. A host with two dormant VMs is otherwise
  enough to stall the pool indefinitely.

  The orchestrator's own VMs are the exception — they count whatever their
  power state, because the warm tier is stopped by design and its memory is
  genuinely spoken for. Ownership is decided by `vmid_range`.

  The residual risk is someone booting a large dormant VM while runners hold
  the memory it wants. If you have one you expect to wake, cover it with
  `reserve_memory_mb`.
- **The host reserve is not optional.** PVE itself, ZFS ARC and qemu's per-VM
  overhead all live outside the guests' configured memory. The effective reserve
  is the larger of `reserve_memory_mb` and `reserve_memory_fraction × node RAM`,
  so you can set a floor and a proportional reserve at once. It defaults to
  2048 MiB.

The data comes from a single cached `GET /cluster/resources` call, refreshed
every `refresh_interval` (default `15s`).

### Deferral is backpressure, not failure

A clone that does not fit is **deferred**: no VM is created, no row is
persisted, nothing is reported to GitHub, and the next reconcile tick retries.
The job simply waits in GitHub's queue until capacity frees up. Watch it with:

| Metric | Meaning |
| --- | --- |
| `scaleset_clone_deferred_capacity_total` | Clones turned away for lack of room, by profile, node and hot/warm. Sustained non-zero means the fleet wants more RAM than the nodes have |
| `scaleset_node_memory_total_bytes` | Physical memory of each node |
| `scaleset_node_memory_committed_bytes` | Memory allocated on each node (foreign guests included) plus outstanding clone reservations |
| `scaleset_node_memory_available_bytes` | What each node can still admit, net of the host reserve |

The three node gauges carry no `scaleset` label — the ledger behind them is
fleet-wide, and a per-scale-set copy would report the same number several times.

### CPU

Memory is gated hard because it cannot be overcommitted safely. CPU is
time-shared, so it is only gated if you ask:

```yaml
pool:
  capacity:
    cpu_overcommit_ratio: 4.0   # allow 4 vCPU per physical core
```

`0` (the default) means vCPUs are never a reason to refuse a clone.

When the ratio is set, both dimensions are treated alike everywhere — including
by idle eviction, which will reclaim vCPU as readily as memory. That matters
because idle pool VMs are cheap on RAM and not on cores: a node can be saturated
on vCPU with memory to spare, and an eviction that only ever looked at memory
would find nothing to reclaim there.

### Making memory the only cap

With capacity admission on, `max_concurrent_runners` becomes optional. Omit it
and there is no static cap at all — the node's memory decides how many runners
exist, which is usually what you wanted from the static counts in the first
place:

```yaml
scalesets:
  - name: homelab
    # no max_concurrent_runners: memory is the cap
    profiles:
      - name: mem-4g
        cpu: 2
        memory_mb: 4096
```

Keep both and the stricter one binds — they are independent checks, so a clone
must satisfy each. Without `pool.capacity.enabled`, `max_concurrent_runners`
stays mandatory: it is then the only thing bounding the fleet.

Every profile must declare `memory_mb` (and `cpu`, if you set
`cpu_overcommit_ratio`) when capacity admission is on. A profile that inherits
the template's memory has no footprint the orchestrator can reserve against, and
guessing one would silently overcommit the node — so config load fails instead.

### Letting a queued job reclaim idle capacity

A node can fill up with *idle* pool VMs. If a job then arrives for a profile
that no longer fits, it waits behind warmth that nothing is using:

```yaml
pool:
  capacity:
    evict_idle_for_demand: true
```

With this on, a queued job that cannot be placed anywhere will reclaim capacity
from an idle VM: the orchestrator destroys the oldest idle one (warm before hot
— no boot is invested in a warm VM, and hot VMs serve job-start latency) and
admits the job on the following reconcile tick. The one-tick gap is deliberate:
cloning the replacement before the victim's `qmdestroy` lands would genuinely
oversubscribe the node for those seconds. The freed memory is reserved for the
requesting profile in the meantime, so the victim's own profile cannot
immediately re-clone into it.

Nothing is evicted when there is room for both. Only genuine queued demand
triggers it — never routine hot/warm pool top-up, which would have two profiles
destroying each other's VMs forever. Count it with
`scaleset_capacity_evictions_total{profile,victim_profile}`; a high rate between
two profiles means the node is too small for both pools to sit warm at once.

Eviction respects [node placement](node-placement.md). It considers only nodes
the requesting profile could actually be placed on, and among those takes the
first with a viable victim — so a profile pinned by an affinity rule (or by
linked clones) never destroys idle VMs on a node it could not land on, and a
profile with no placeable node evicts nothing at all, since no amount of freed
memory would help it. For the same reason, free memory on an ineligible node
does not count as "there is room for both": a profile pinned to a full node
still evicts, rather than waiting forever on capacity it can never use.

The freed capacity is re-checked against those same rules before the replacement
clone consumes it, so an `anti_affinity_with` guarantee holds even if the node's
occupants changed while the claim was parked. A claim blocked that way is held
and retried — co-tenancy resolves itself when the other job finishes — while one
blocked by a hard pin, which will not resolve on its own, is released back to
the fleet.

Two limits worth knowing:

- Eviction is scoped to one scale set's own VMs. A scale set cannot reclaim
  capacity from a sibling scale set's pool, or from foreign VMs.
- It is off by default, because it trades a sibling profile's warm-start latency
  for the queued job's ability to run at all — a call that belongs to you.

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
