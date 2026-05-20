# Packer template: Ubuntu 26.04 LTS GitHub Actions runner

Builds a Proxmox VM template that the [orchestrator](../README.md) clones into
ephemeral GitHub Actions runners.

## What the template contains

- Ubuntu 26.04 LTS server, installed via Subiquity autoinstall.
- `qemu-guest-agent` enabled — the orchestrator drives the VM through this.
- GitHub Actions runner unpacked at `/opt/actions-runner/`.
- A systemd path-unit (`gh-runner.path`) that watches for
  `/opt/actions-runner/jitconfig`. When the orchestrator writes that file via
  guest-agent file-write, the corresponding `gh-runner.service` starts the
  runner with `--jitconfig "$(cat …)"`. The runner takes one job and exits;
  the service's `ExecStopPost=systemctl poweroff` then shuts the VM down so
  the orchestrator's `Destroy` path doesn't have to wait.
- Kernel + service hardening (see [files/99-scaleset-hardening.conf](files/99-scaleset-hardening.conf)
  and [scripts/provision.sh](scripts/provision.sh)).

## Hardening posture

| Surface | Decision |
| --- | --- |
| SSH | **Removed** — `openssh-server` is purged. Operator access via Proxmox console or `qm guest exec`. |
| Build user | Locked + removed from `sudo` group as the final provisioning step. |
| Runner user | Non-root (`runner`), no password, NOPASSWD sudo restricted to that account. |
| Network firewall | Off (ephemeral VMs; firewall is enforced at the Proxmox/network level). |
| Kernel sysctls | rp_filter, no redirects, syncookies, kptr_restrict=2, dmesg_restrict, BPF lockdown, protected hardlinks/symlinks/fifos/regular. |
| Snap / unattended-upgrades / iscsi / multipath / ModemManager | Removed or masked. |
| Swap | Disabled and removed from fstab. |
| Core dumps | Disabled (limits.conf + suid_dumpable=0). |
| machine-id | Cleared so clones generate a unique ID on first boot. |
| Secure Boot | UEFI enabled, pre-enrolled keys **off** by default (toggle per-deployment after testing your kernel). |

## One-time setup

1. **Install Packer >= 1.10** and the Proxmox plugin:

    ```sh
    packer init .
    ```

2. **Create a Proxmox API token** with `VM.Allocate`, `VM.Audit`, `VM.Clone`,
   `VM.Config.*`, `VM.PowerMgmt`, `Datastore.AllocateSpace`,
   `Datastore.AllocateTemplate`, and `Datastore.Audit` on `/`.

3. **Download the Ubuntu 26.04 LTS server ISO checksum** from
   <https://releases.ubuntu.com/26.04/> and copy your local values into:

    ```sh
    cp variables.auto.pkrvars.hcl.example variables.auto.pkrvars.hcl
    $EDITOR variables.auto.pkrvars.hcl
    ```

4. **Export the API token secret** so it doesn't end up in the var-file:

    ```sh
    export PKR_VAR_proxmox_token='your-token-secret'
    ```

## Build

```sh
packer validate .
packer build .
```

Build time is typically 10–20 minutes (Ubuntu autoinstall + apt-get + runner
tarball download).

When the build finishes you'll have a Proxmox template at the VMID you
configured (`template_vm_id`, default 9000). Point the orchestrator's
`proxmox.template_vmid` at it.

## Re-building

The template is meant to be rebuilt regularly (monthly is reasonable) so that
security updates land — ephemeral VMs deliberately have no
`unattended-upgrades` and pull no updates at boot. Bump `runner_version` in
`variables.auto.pkrvars.hcl` whenever a new Actions runner is published.

## Notes

- The Subiquity password hash in [http/user-data](http/user-data) corresponds to
  the literal string `ubuntu`. It's only valid during the Packer build window;
  the build user is locked before the template is finalized.
- Tighten `gh-runner.service` (in [files/](files/)) further if your workflows
  don't need namespace/SUID flexibility (Docker-in-Docker, podman, mount, etc.
  routinely need them).
- If you need a `arm64` template, set `runner_arch = "arm64"` and switch the
  ISO URL/checksum to the arm64 release; ensure your Proxmox node supports
  arm64 emulation (or runs on arm64 hardware).
