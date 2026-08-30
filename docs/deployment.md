# Deployment

The same binary covers every deployment shape. Only the `cluster:` block
changes between a single process and a highly-available cluster.

## Artifacts

| Artifact | Where |
| --- | --- |
| Container image | `ghcr.io/jeffresc/actions-runner-scaleset-proxmox` |
| Helm chart | `oci://ghcr.io/jeffresc/charts/scaleset` |
| Prebuilt binaries | Attached to each [GitHub release](https://github.com/jeffresc/actions-runner-scaleset-proxmox/releases) |

## Binary

```sh
scaleset run --config=/etc/scaleset/config.yaml
scaleset version
```

`--config` defaults to `config.yaml` in the working directory. SIGINT and
SIGTERM trigger a graceful drain bounded by `pool.drain_timeout`.

## Container

```sh
docker run --rm \
  --user "$(id -u):$(id -g)" \
  -e SCALESET_GITHUB_PAT_TOKEN \
  -e SCALESET_PROXMOX_AUTH_TOKEN_SECRET \
  -v "$PWD/config.yaml:/etc/scaleset/config.yaml:ro" \
  -p 9100:9100 \
  ghcr.io/jeffresc/actions-runner-scaleset-proxmox:latest
```

The image's entrypoint is the binary and its default arguments are
`run --config=/etc/scaleset/config.yaml`, so mounting your config at that path
is all that's needed. See
[deploy/docker/Dockerfile](../deploy/docker/Dockerfile).

The image is distroless and runs as `nonroot` (UID 65532). The config loader
refuses a file that grants any access to "other", and the running UID must
reach it as the owner or through one of its groups — so either `chown` the
file to 65532 or override the container's user as above.

## systemd

A hardened unit ships at
[deploy/systemd/scaleset.service](../deploy/systemd/scaleset.service):

```sh
sudo useradd --system --no-create-home scaleset
sudo install -m 0755 bin/scaleset /usr/local/bin/scaleset
sudo install -d -m 0750 -o scaleset -g scaleset /etc/scaleset
sudo install -m 0600 -o scaleset -g scaleset config.yaml /etc/scaleset/config.yaml
sudo install -m 0600 -o scaleset -g scaleset /dev/null /etc/scaleset/env
sudo cp deploy/systemd/scaleset.service /etc/systemd/system/
sudo systemctl enable --now scaleset
```

The config file must be mode `0600` and owned by the user in the unit's
`User=` — it holds credentials, and the loader refuses to read a file that is
world-readable or that the running user can reach neither as owner nor
through one of its groups.

Secrets go in `/etc/scaleset/env` (also 0600, never committed):

```
SCALESET_GITHUB_PAT_TOKEN=ghp_...
SCALESET_PROXMOX_AUTH_TOKEN_SECRET=...
```

The unit's `TimeoutStopSec=35m` deliberately exceeds the default
`pool.drain_timeout` of 30 minutes. If you change one, change the other.

## Kubernetes (Helm)

```sh
helm install scaleset oci://ghcr.io/jeffresc/charts/scaleset \
  --namespace gh-runners --create-namespace \
  -f values.yaml
```

For anything beyond a dev cluster use a values file rather than `--set`
flags. See [deploy/chart/README.md](../deploy/chart/README.md) and
[deploy/chart/values.yaml](../deploy/chart/values.yaml) for the full schema,
production secret handling (`secrets.existingSecret`), the ServiceMonitor, and
the bundled `helm test` smoke tests.

> **Keep `replicaCount: 1`.** The chart deploys with the default
> `cluster.mode: standalone`, in which every process believes it is leader.
> Raising the replica count would have each replica drive the pool
> independently. See below for what real HA requires.

## High availability with Raft

`cluster.mode` selects the leader-election backend:

- **`standalone`** (default) — the process is always leader. This is the right
  mode for Docker, systemd, and single-replica Kubernetes.
- **`raft`** — replicas elect a leader through a
  [hashicorp/raft](https://github.com/hashicorp/raft) quorum. Only the leader
  runs the control plane (the GitHub scaleset listener, REST reconciler, pool
  manager, and Proxmox power-state poller). Standbys serve only `/healthz`,
  `/readyz`, and `/metrics` until promoted, then rebuild their in-memory state
  from Proxmox via the same recovery path used after a crash.

```yaml
cluster:
  mode: raft
  raft:
    node_id: ""                          # defaults to $HOSTNAME, then os.Hostname
    bind_addr: "0.0.0.0:7000"
    advertise_addr: "10.0.0.1:7000"
    data_dir: "/var/lib/scaleset/raft"
    bootstrap: true                      # exactly one replica, at initial setup only
    heartbeat_timeout: 1s
    election_timeout: 1s
    commit_timeout: 50ms
    peers:
      - { node_id: node-a, raft_addr: 10.0.0.1:7000, http_addr: 10.0.0.1:9101 }
      - { node_id: node-b, raft_addr: 10.0.0.2:7000, http_addr: 10.0.0.2:9101 }
      - { node_id: node-c, raft_addr: 10.0.0.3:7000, http_addr: 10.0.0.3:9101 }
```

Three requirements are easy to get wrong:

1. **`data_dir` must be persistent.** Raft's election-safety invariants
   (`currentTerm`, `votedFor`) have to survive a restart, or a node could vote
   twice in the same term. Put it on a volume that outlives the
   container or process.
2. **`bootstrap: true` on exactly one replica, at initial setup only.**
3. **Each peer's `http_addr` must be its admin-API address.** The admin
   forwarder resolves the leader's raft address to an HTTP endpoint through
   this map. A missing or wrong entry produces a `502` with
   "leader raft addr has no matching HTTP peer entry" rather than a silent
   misroute.

Run an odd number of replicas — three tolerates one failure, five tolerates
two.

The admin API is bound on every replica. Non-leaders reverse-proxy to the
leader, so callers don't need to know which replica holds leadership. Inbound
`X-Forwarded-For` / `X-Real-IP` / `True-Client-IP` headers are stripped before
proxying, so hitting a standby cannot spoof the source IP the leader records.
When no leader has been observed yet, the forwarder returns `503` with
`Retry-After: 2`.

> The Helm chart is Deployment-based and does not yet provide the stable
> network identities and per-replica persistent volumes that `raft` mode needs.
> Running HA in Kubernetes today means authoring your own StatefulSet with a
> headless Service, or running the replicas outside the cluster.

## Graceful shutdown

SIGTERM starts a drain: no new jobs are accepted, running jobs are allowed to
finish, and idle pool VMs are destroyed. The drain is bounded by
`pool.drain_timeout`. Whatever supervises the process must allow at least that
long before sending SIGKILL — `TimeoutStopSec` for systemd,
`terminationGracePeriodSeconds` for Kubernetes.

A drain can also be triggered on demand through the admin API; see
[operations.md](operations.md#admin-api).
