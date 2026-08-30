# Getting started

This guide takes you from nothing to a GitHub Actions job running inside an
ephemeral Proxmox VM.

There are four things to put in place:

1. A **Proxmox VM template** with the Actions runner baked in.
2. A **Proxmox API token** for the orchestrator.
3. **GitHub credentials** — a PAT or a GitHub App.
4. A **`config.yaml`** that ties them together.

## 1. Build the runner template

The orchestrator does not build VMs from scratch — it clones a template you
build once and rebuild periodically. A Packer template ships in
[`packer/`](../packer/):

```sh
cd packer
packer init .
cp variables.auto.pkrvars.hcl.example variables.auto.pkrvars.hcl
$EDITOR variables.auto.pkrvars.hcl        # Proxmox endpoint, node, ISO checksum
export PKR_VAR_proxmox_token='your-token-secret'
packer build .
```

The build takes 10–20 minutes and leaves a template at the VMID you configured
(`template_vm_id`, default `9000`). See [packer/README.md](../packer/README.md)
for what the image contains, its hardening posture, the build-time token
permissions, and how to build an `arm64` variant.

The template must have `qemu-guest-agent` enabled — that is the only channel
the orchestrator uses to talk to a VM. There is no SSH.

## 2. Create the orchestrator's Proxmox API token

In the Proxmox UI: **Datacenter → Permissions → API Tokens → Add**. Create a
token such as `scaleset@pve!automation` and record the secret — Proxmox shows
it exactly once.

Then grant it a role on `/` covering the API calls the orchestrator makes:

| Privilege | Why it is needed |
| --- | --- |
| `VM.Audit` | List VMs and read config/status during crash recovery, orphan sweeps, and power polling |
| `VM.Allocate` | Create the clone, and destroy it when the job finishes |
| `VM.Clone` | Clone the template |
| `VM.Config.Options` | Write the owner/profile tags that mark a VM as ours |
| `VM.Config.CPU` | Apply a profile's `cpu:` override |
| `VM.Config.Memory` | Apply a profile's `memory_mb:` override |
| `VM.Config.Network` | Apply a profile's `network:` block and `extra_nics:` |
| `VM.Config.Disk` | Apply a profile's `disk_gb:` resize |
| `VM.Config.Cloudinit` | Write `ipconfig0` when `ipam.backend: static` is in use |
| `VM.PowerMgmt` | Start and stop VMs |
| `VM.Monitor` | Guest-agent `exec`, `exec-status`, `file-read`, `file-write` — the JIT-config injection path |
| `Datastore.AllocateSpace` | Full clones onto a storage pool |
| `Datastore.Audit` | Storage lookups |
| `Sys.Audit` | List nodes — required for cluster topologies and for `nodes.strategy: least_loaded` |

> **Guest-agent privileges vary by PVE version.** Proxmox VE 8 split the
> guest-agent surface into `VM.GuestAgent.Audit`,
> `VM.GuestAgent.FileSystemMgmt`, `VM.GuestAgent.FileWrite`, and
> `VM.GuestAgent.Unrestricted`, where older releases gated all of `/agent/*`
> behind `VM.Monitor`. If JIT injection fails with a 403, check which of these
> your PVE version expects and grant the equivalent. The built-in
> `PVEVMAdmin` role covers the VM privileges above on every current version and
> is a reasonable starting point.

The built-in `Administrator` role works but is far more than the orchestrator
needs.

## 3. Set up GitHub credentials

Pick one of two auth modes.

**PAT** — simplest. Create a classic PAT with the `admin:org` scope (or `repo`
for a repo-scoped scale set) and export it:

```sh
export SCALESET_GITHUB_PAT_TOKEN='ghp_...'
```

```yaml
github:
  auth_mode: pat
  pat: {}
  scope:
    org: my-org        # or: repo: my-org/my-repo
```

**GitHub App** — better for organisations. Install the App on the org, note its
installation ID, and download the private key:

```yaml
github:
  auth_mode: app
  app:
    client_id: "Iv23likB94..."     # or app_id: 123456 for legacy numeric IDs
    installation_id: 789012
    private_key_path: /etc/scaleset/github-app.pem
  scope:
    org: my-org
```

Running against GitHub Enterprise Server? See
[configuration.md](configuration.md#github-enterprise-server).

## 4. Write a minimal `config.yaml`

Everything below is required. [config.example.yaml](../config.example.yaml) is
the annotated reference for every other knob.

```yaml
github:
  auth_mode: pat
  pat: {}                                    # token comes from the env var
  scope:
    org: my-org

scaleset:
  name: proxmox-ubuntu-x64
  labels: [self-hosted, linux, x64, proxmox]
  max_concurrent_runners: 50

proxmox:
  endpoint: https://pve.example.com:8006/api2/json
  auth:
    token_id: scaleset@pve!automation        # secret comes from the env var
  template_vmid: 9000                        # the template Packer built
  vmid_range: { min: 10000, max: 19999 }     # VMIDs the orchestrator may claim
  storage:
    disk: local-lvm
    snippets: local
  network:
    bridge: vmbr0
    vlan_tag: 0                              # 0 = untagged

nodes:
  strategy: single                           # single node PVE
  single_node: pve1

pool:
  hot_size: 2                                # booted, idle, ready to take a job
  warm_size: 3                               # cloned but powered off

observability:
  http_addr: ":9100"
```

**The config file must be mode `0600` and owned by the user the orchestrator
runs as.** It holds credentials, so the loader refuses to read a file that is
world-readable, or that the process can reach neither as owner nor through
one of its groups:

```sh
chmod 600 config.yaml
```

A few of the keys deserve a note:

- **`vmid_range`** must not overlap anything else on the cluster. The
  orchestrator claims IDs in this window and destroys VMs it finds there that
  match its naming prefix.
- **`hot_size` vs `warm_size`** is the latency/cost trade-off. Hot VMs are
  fully booted and take a job in seconds; warm VMs are cloned but powered off,
  so they cost RAM only while booting. See
  [profiles-and-scaling.md](profiles-and-scaling.md).
- **`labels`** must match the `runs-on:` in your workflows.

## 5. Run it

Export the secrets, then start the orchestrator:

```sh
export SCALESET_GITHUB_PAT_TOKEN='ghp_...'
export SCALESET_PROXMOX_AUTH_TOKEN_SECRET='your-pve-token-secret'

docker run --rm \
  --user "$(id -u):$(id -g)" \
  -e SCALESET_GITHUB_PAT_TOKEN \
  -e SCALESET_PROXMOX_AUTH_TOKEN_SECRET \
  -v "$PWD/config.yaml:/etc/scaleset/config.yaml:ro" \
  -p 9100:9100 \
  ghcr.io/jeffresc/actions-runner-scaleset-proxmox:latest
```

The image runs as `nonroot` (UID 65532) by default. `--user` overrides that to
your own UID so the bind-mounted config passes the ownership check described
above.

Every config key can also be supplied as an environment variable — see
[configuration.md](configuration.md#environment-variable-overrides). For
systemd, Kubernetes, and HA deployments, see [deployment.md](deployment.md).

## 6. Verify

```sh
curl -s localhost:9100/healthz          # process is alive
curl -s localhost:9100/readyz           # listener connected + recovery done
curl -s localhost:9100/metrics | grep scaleset_pool_size
```

`scaleset_pool_size` should climb to your configured `hot_size` and
`warm_size` within a minute or two of the first clone. In the Proxmox UI you
should see VMs appearing in your `vmid_range`, tagged with the scale set name.

Now run a job:

```yaml
name: smoke
on: workflow_dispatch
jobs:
  hello:
    runs-on: [self-hosted, linux, x64, proxmox]   # must match scaleset.labels
    steps:
      - run: uname -a && echo "hello from a disposable VM"
```

Dispatch it. A hot VM picks up the job, runs it, powers itself off, and the
orchestrator destroys it — the pool then reclones to get back to size.

## Troubleshooting the first run

**`/readyz` never turns green.** Readiness needs both the GitHub listener
connected and crash recovery finished. Check the logs for GitHub auth failures
(bad PAT scope, wrong `installation_id`) and for Proxmox connectivity. If your
PVE uses a self-signed certificate, either trust the CA or set
`proxmox.insecure_skip_verify: true`.

**`config: fileperm: ... is accessible by other`.** The config file grants
access to "other". `chmod 600 config.yaml`. The same check rejects a file the
process can reach neither as owner nor through one of its groups — under
Docker the image runs as `nonroot` (UID 65532), so a bind-mounted config must
be owned by that UID or you must run the container as your own UID with
`--user "$(id -u):$(id -g)"`.

**Config rejected at startup.** Validation is strict and runs before anything
connects, so the error names the offending key. Also watch for the warning
about unknown `SCALESET_*` env vars — a typo like `SCALESET_POOL_HOTSIZE`
(missing the second underscore) is logged rather than silently ignored.

**VMs clone but never become hot.** The orchestrator waits on
`qemu-guest-agent`. Boot the template manually and confirm the agent is running
and enabled. After `pool.boot_max_attempts` failures a VM is marked `poison`
and left in place for inspection.

**Jobs queue but no VM is assigned.** The job's `runs-on:` labels must be a
subset of a profile's labels. Check
`scaleset_unrouted_jobs_total` in `/metrics` — a non-zero value means no
profile matched. See [profiles-and-scaling.md](profiles-and-scaling.md#label-routing).

**Clone fails with "VM N is running - destroy failed" or lock timeouts.**
Proxmox is still tearing down a VMID the allocator reissued. Raise
`pool.vmid_reuse_cooldown`.

## Next steps

- [Runner profiles and scaling](profiles-and-scaling.md) — different hardware
  shapes, cron-driven capacity, canary template rollouts
- [Node placement](node-placement.md) — spreading VMs across a PVE cluster
- [Deployment](deployment.md) — systemd, Helm, and Raft-based HA
- [Operations](operations.md) — metrics, tracing, the admin API
- [Architecture](architecture.md) — how the control loops actually work
