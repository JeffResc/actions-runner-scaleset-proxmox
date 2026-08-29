# Multiple scale sets in one process

One orchestrator process can drive several independent scale sets — for
example, separate pools for two GitHub organisations, or a general-purpose
pool alongside a GPU pool with its own concurrency ceiling.

## Config shape

Replace the singular `scaleset:` block (plus the top-level `github.scope` and
`profiles:`) with a `scalesets:` list. Each entry carries its own `name`,
`labels`, `runner_group`, `max_concurrent_runners`, GitHub `scope`,
`vmid_range`, and `profiles:`.

```yaml
scalesets:
  - name: linux-x64
    labels: [self-hosted, linux, x64, proxmox]
    runner_group: default
    max_concurrent_runners: 50
    scope: { org: my-org }
    vmid_range: { min: 10000, max: 14999 }
    profiles:
      - name: linux-x64
        labels: [self-hosted, linux, x64, proxmox]
        hot_size: 5
        warm_size: 10
        max_concurrent_runners: 50
  - name: gpu-pool
    labels: [self-hosted, linux, gpu]
    max_concurrent_runners: 4
    scope: { org: my-other-org }
    vmid_range: { min: 15000, max: 19999 }
    profiles:
      - name: gpu
        labels: [self-hosted, linux, gpu]
        template_vmid: 9100
        cpu: 8
        memory_mb: 32768
        hot_size: 0
        warm_size: 1
        max_concurrent_runners: 4
```

The legacy singular form is automatically normalised into a one-element
`scalesets:` list at load time, so existing configs keep working unchanged.
Mixing the two shapes is rejected at load with an error naming the offending
legacy keys.

## VMID ranges must be disjoint

With more than one scale set declared, **every entry must carry its own
`vmid_range` and the ranges must be pairwise disjoint.** Each scale set's pool
manager runs an independent VMID allocator, so sharing one range would race on
Proxmox clone. Partition the global `proxmox.vmid_range` into per-scale-set
slices that together cover it, or into any disjoint subset.

## Runtime fan-out

Under leader election, each declared scale set runs its own per-scale-set pool
manager, scaler, listener, GitHub REST reconciler, canary controller, schedule
runner, store, and provisioner — the last with its own owner-tagged crash
recovery.

The per-scale-set workers are spawned under a single supervisor errgroup: a
panic or returned error in one worker is recovered and logged but does **not**
propagate to its siblings, so one failing scale set never poisons the others.
Sibling shutdown happens only via process-wide context cancellation — SIGTERM,
admin drain, or leader deposal.

Readiness is leader-aware *and* per-scale-set: `/readyz` only flips green when
every registered scale set has signalled both listener-connected and
recovery-done. One stalled scale set is enough to hold the gate red.

## Metrics and admin endpoints

Every per-scale-set Prometheus metric carries `scaleset=<name>` as its first
label, so dashboards can slice cleanly.

Admin endpoints are reachable under a `/admin/{scaleset}/...` prefix — for
example `/admin/{scaleset}/state`, `/admin/{scaleset}/preempt/{vmid}`,
`/admin/{scaleset}/template/promote/{profile}`. Unknown scale set names return
404. The un-namespaced `/admin/...` paths keep working when there is exactly
one scale set. See [operations.md](operations.md#admin-api).

## GitHub Enterprise Server

Multi-scale-set GHES deployments must use `config_base_url` rather than
`config_url` — see
[configuration.md](configuration.md#github-enterprise-server).
