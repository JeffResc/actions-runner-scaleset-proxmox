# Node placement

On a Proxmox cluster, the orchestrator has to decide which node each new VM
lands on. That decision is pluggable via `nodes.strategy`, with optional
profile-keyed affinity rules layered on top.

## Strategies

```yaml
nodes:
  strategy: least_loaded          # single | round_robin | least_loaded
  members: [pve1, pve2, pve3]     # ignored for "single"
  single_node: pve1               # only used when strategy = single
```

| Strategy | Behaviour |
| --- | --- |
| `single` | Always the same node, named by `single_node`. The right choice for single-node PVE. |
| `round_robin` | Rotate through the configured `members` list. |
| `least_loaded` | Periodically list the cluster's nodes and pick the one with the lowest weighted load. |

`least_loaded` scores each node as `0.7 × CPU utilisation + 0.3 × memory
utilisation` — lower is better. CPU is weighted higher because it is usually
the binding resource for ephemeral runner VMs. Results are cached with a TTL;
on a transient Proxmox error the selector falls back to the last known-good
scores rather than failing the selection.

## Affinity rules

An optional `nodes.affinity:` block layers profile-keyed rules over the chosen
strategy:

```yaml
nodes:
  strategy: least_loaded
  members: [pve1, pve2, pve-gpu-1, pve-gpu-2]
  affinity:
    - match: { profile: gpu }
      prefer_nodes: [pve-gpu-1, pve-gpu-2]
      require: true                          # hard pin
    - match: { profile: untrusted-pr }
      anti_affinity_with: { profile: prod }
```

- **`prefer_nodes`** restricts the candidate set to the listed nodes.
- **`require: true`** turns that into a hard pin — the clone fails with
  `nodeselector.ErrAffinityRequireUnsatisfiable` when no preferred node is
  eligible. Without it, the preference degrades gracefully to the full
  candidate set.
- **`anti_affinity_with: { profile: ... }`** excludes nodes already hosting a
  tracked VM of the named profile, keeping two profiles off the same node.

Rules are evaluated against the cloning row's profile in declaration order and
the first match wins. An empty `match.profile` is a wildcard.

Both filters apply **before** the underlying strategy picks among the surviving
candidates, so rotation and load balancing keep their semantics within the
eligible set.

Config validation rejects rules that name an undeclared profile or node —
typos surface at load time rather than as silent no-ops at runtime.
