# Windows runners

A Windows profile uses a prepared Windows VM template and the QEMU guest agent
for the same single-use JIT registration as Linux. Put Windows and Linux in
separate scale sets with different labels: GitHub routes to a scale set, not an
individual profile. All profiles in a scale set must use the same guest OS and
advertise its labels. See [the complete mixed example](../examples/windows-linux.yaml).

## Prepare an existing Windows VM once

1. Install Windows and updates, the build tools your jobs need, VirtIO drivers
   as appropriate, and the GitHub Actions runner distribution under
   `C:\actions-runner`. Do not register the runner in the template. Verify the
   VM can reach GitHub and any required package and artifact endpoints. Account
   for Windows licensing and activation in your environment.
2. Install the QEMU guest agent in Windows, enable its service, and enable the
   QEMU guest agent in the Proxmox VM options. Verify the agent responds from
   Proxmox after a reboot. The controller uses it to publish the JIT file; SSH
   is not required.
3. If this VM already has a conventional GitHub runner, deregister it in
   GitHub with its removal token first. Follow the safeguards and installation
   commands in [scripts/windows/README.md](../scripts/windows/README.md): run
   `Install-JitRunnerWatcher.ps1` elevated, with its cleanup switch only when
   appropriate. The installer refuses existing registration state by default.
   Ensure no runner credentials, pending JIT file, started marker, job data,
   or other secrets are in the golden image.
4. The installer creates a boot-triggered scheduled task running as Local
   System. This is Session 0: jobs must work unattended and cannot rely on an
   interactive desktop, an RDP user's profile, mapped drives, or GUI prompts.
   Test permissions and tools under that account; use a separately designed
   interactive setup for workloads that require a visible desktop.
5. Shut down the clean VM and convert it to a Proxmox template. Keep the
   template unchanged while linked clones depend on it; build a new template
   for updates and retire the old one only after its clones are gone.

The shipped watcher reads
`C:\ProgramData\ActionsRunnerScaleSet\jitconfig`. `guest_os: windows` selects
Windows injection; `windows_jit_path` can override the destination, but it must
match the guest launcher. The installer accepts `-RunnerDirectory` and
`-JitPath` (a filename within its secured handoff directory); other directories
require a custom launcher and appropriate ACLs. The
file is a sensitive, single-use handoff; do not print or copy it into a golden
image. The watcher starts the runner once and shuts down the VM after it exits.
See the script README for behavior and troubleshooting. Sysprep is not run on
each clone. Decide whether to generalize the golden image once based on your
Windows deployment, identity, domain and licensing requirements; validate the
result before conversion.

## Configure and route jobs

Copy [examples/windows-linux.yaml](../examples/windows-linux.yaml), replace the
placeholder org, node, endpoint, token ID, storage, bridge and template VMIDs,
and provide `SCALESET_GITHUB_PAT_TOKEN` and
`SCALESET_PROXMOX_AUTH_TOKEN_SECRET` securely. Store the deployed config with
restrictive permissions (for example, `chmod 600 config.yaml`). The profile defaults to Linux
when `guest_os` is omitted; Windows explicitly sets `guest_os: windows`.
Ranges must be disjoint and free of existing VMIDs. Both templates must be
available to the selected node and their actual storage must support the
requested clone and snapshot operations. The example starts with zero hot and
warm capacity for both profiles, so initial demand takes the cold clone and
boot path. Adjust concurrency and capacity only after measuring your hardware.

Select the intended scale set in a workflow, for example:

```yaml
jobs:
  windows:
    runs-on: [self-hosted, windows, x64, proxmox-windows]
    steps:
      - run: Write-Output 'Windows runner'
  linux:
    runs-on: [self-hosted, linux, x64, proxmox-linux]
    steps:
      - run: echo 'Linux runner'
```

Use OS-specific labels in every job; a job requesting only shared labels may
match either scale set. Restrict runner groups and repository access as needed.

## Storage, recycling and performance

The example requests linked clones and `snapshot_rollback` globally. Proxmox's
[LVM-thin documentation](https://pve.proxmox.com/pve-docs/pve-admin-guide.html#storage_lvmthin)
describes local LVM-thin storage with linked clones and snapshots. Linked
clones stay on the template's node and storage; a storage name shared across
nodes does not make local LVM-thin data shared. All VM disks and devices must
support the requested snapshot operation. The recycler snapshots each stopped
clone before first boot, then rolls back after a job; if snapshot creation or
rollback fails, it falls back to destroy and replacement. `vm_max_age` still
replaces old clones to pick up updated templates. See
[recycling](profiles-and-scaling.md#recycling-vms-with-snapshot-rollback).

The controller clones the template's current state; selecting a named source
snapshot is not a configuration option. A snapshot is created on the clone for
recycling, not selected as a clone source. Verify that linked clone plus
snapshot rollback works on your exact Proxmox version, storage and VM devices
before relying on it. Monitor thin-pool data **and metadata** free capacity,
copy-on-write growth, RAM, CPU and I/O; thin provisioning does not create
physical capacity. Hot VMs consume resources continuously; warm VMs avoid
clone latency but still need a boot. A zero/zero pool trades idle cost for cold
start latency, and recycling effectiveness at that size should be measured.

## Manual canary validation

Use an isolated test repository and small concurrency limit. Record the
Proxmox version, storage, template IDs, guest versions, VMIDs and timestamps.

1. Verify both scale sets register with the intended labels and only intended
   repositories can use them. Submit one harmless job for each OS and verify
   OS identity, guest-agent readiness, successful completion and VM shutdown.
2. For Windows, verify the scheduled task runs as SYSTEM and the runner accepts
   exactly one job. Confirm the baseline has no registration or JIT state;
   avoid exposing the JIT file or credentials in logs and artifacts.
3. Inspect clone, snapshot and rollback tasks and controller logs/metrics. Run
   a second Windows job after creating a harmless sentinel in the first and
   verify it is absent; confirm a fresh JIT registration is used. Observe
   whether the VM was actually recycled or destroyed, including fallback.
4. Record cold, warm and hot queue-to-start times separately if those tiers are
   configured, along with clone, boot and recycle timings and host/thin-pool
   peaks. The startup stage metric in [operations](operations.md) helps break
   down power-on, guest-agent, injection and online waits. Test a representative Windows build, not only an empty job. Size
   pools and reserve headroom from those measurements; repeat after template,
   storage or workload changes.

These are validation steps, not a claim that your environment has been tested.

## Recovery and operational limits

After a restart, Windows guests with a published JIT handoff or an unreadable
handoff are held as assigned, rather than offered a second job. A missing file
may cause this conservative classification too; the normal registration grace
and cleanup recover it. Stopped Windows guests are rolled back in snapshot
mode or destroyed in destroy mode before reuse. Unknown profiles are destroyed.

For linked-clone canary rollouts, keep the stable and canary templates on the
same node. Candidate selection currently happens after node admission; a
candidate on another node fails closed and is cleaned up. Named source snapshot
selection is deferred: prepared stopped templates already supply the supported
linked-clone baseline without introducing a second source-state lifecycle.
