#!/usr/bin/env bash
# Main provisioning + hardening pass for the runner template.
#
# Run as root by Packer's shell provisioner after Subiquity autoinstall
# completes. Sourced env vars (set by Packer):
#   RUNNER_VERSION   - GitHub Actions runner release (e.g. 2.323.0)
#   RUNNER_ARCH      - runner architecture (x64 / arm64)
#   RUNNER_USER      - local user the runner runs as
#   BUILD_USERNAME   - the Subiquity build user (locked at the end)
#
# This script is intentionally linear and idempotent-on-replay so a
# Packer build failure mid-way can be re-run safely. Every section logs
# what it's doing so build output is auditable.

set -euo pipefail

RUNNER_VERSION="${RUNNER_VERSION:?missing}"
RUNNER_ARCH="${RUNNER_ARCH:?missing}"
RUNNER_USER="${RUNNER_USER:?missing}"
BUILD_USERNAME="${BUILD_USERNAME:?missing}"

STAGE_DIR="/tmp/scaleset-files"

log() { printf '[provision] %s\n' "$*"; }

# -----------------------------------------------------------------------------
# Wait for cloud-init / apt to settle.
# -----------------------------------------------------------------------------
log "waiting for cloud-init to finish"
cloud-init status --wait || true

log "waiting for apt locks"
while fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1 \
   || fuser /var/lib/apt/lists/lock >/dev/null 2>&1; do
    sleep 2
done

export DEBIAN_FRONTEND=noninteractive

# -----------------------------------------------------------------------------
# Base packages.
# -----------------------------------------------------------------------------
log "updating apt cache + installing baseline packages"
apt-get update
apt-get install -y --no-install-recommends \
    qemu-guest-agent \
    ca-certificates \
    curl \
    git \
    jq \
    sudo \
    iproute2 \
    iputils-ping \
    netcat-openbsd \
    unzip \
    xz-utils \
    libicu-dev   # required by the Actions runner's .NET runtime

systemctl enable --now qemu-guest-agent
log "qemu-guest-agent active"

# -----------------------------------------------------------------------------
# Runner user.
# -----------------------------------------------------------------------------
log "creating runner user ${RUNNER_USER}"
if ! id "${RUNNER_USER}" >/dev/null 2>&1; then
    useradd --create-home --shell /bin/bash "${RUNNER_USER}"
fi
# Lock the password so console / SSH password login is impossible.
passwd -l "${RUNNER_USER}" || true

# The runner needs to run sudo for `actions/setup-*` recipes and similar.
# Restricted to NOPASSWD because no human ever logs in.
install -m 0440 /dev/stdin /etc/sudoers.d/10-runner <<EOF
${RUNNER_USER} ALL=(ALL) NOPASSWD: ALL
EOF

# -----------------------------------------------------------------------------
# GitHub Actions runner binary.
# -----------------------------------------------------------------------------
log "installing GitHub Actions runner v${RUNNER_VERSION} (${RUNNER_ARCH})"
install -d -o "${RUNNER_USER}" -g "${RUNNER_USER}" -m 0755 /opt/actions-runner
cd /opt/actions-runner

TARBALL="actions-runner-linux-${RUNNER_ARCH}-${RUNNER_VERSION}.tar.gz"
URL="https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/${TARBALL}"

curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
     -o "/tmp/${TARBALL}" "${URL}"
tar -xzf "/tmp/${TARBALL}" -C /opt/actions-runner
rm -f "/tmp/${TARBALL}"
chown -R "${RUNNER_USER}:${RUNNER_USER}" /opt/actions-runner

# The runner ships with a script that pulls in libs it needs (.NET deps).
log "installing runner OS dependencies"
/opt/actions-runner/bin/installdependencies.sh

# The orchestrator writes the JIT config as a systemd env file at this
# path on each clone. The path itself must NOT exist in the template.
test ! -e /opt/actions-runner/jitconfig.env

# -----------------------------------------------------------------------------
# systemd units.
# -----------------------------------------------------------------------------
log "installing gh-runner.{path,service}"
install -m 0644 "${STAGE_DIR}/gh-runner.path"    /etc/systemd/system/gh-runner.path
install -m 0644 "${STAGE_DIR}/gh-runner.service" /etc/systemd/system/gh-runner.service
systemctl daemon-reload
# Only the path unit is enabled — it'll start the service when the
# orchestrator writes the jitconfig file.
systemctl enable gh-runner.path

# -----------------------------------------------------------------------------
# Runner-side lifecycle hooks.
#
# The GitHub Actions runner invokes ACTIONS_RUNNER_HOOK_JOB_STARTED /
# ACTIONS_RUNNER_HOOK_JOB_COMPLETED scripts at the obvious moments. Our
# scripts POST a JSON payload to the orchestrator's :9103 endpoint,
# giving us millisecond-precision lifecycle signals that don't depend
# on the scaleset listener. The orchestrator's runner_hook.shared_secret
# is provisioned per-clone via the JIT env file, NOT baked into the
# image — so a leaked image alone can't impersonate runners.
# -----------------------------------------------------------------------------
log "installing runner lifecycle hook scripts"
install -m 0755 -o "${RUNNER_USER}" -g "${RUNNER_USER}" \
    "${STAGE_DIR}/hook-job-started.sh"   /opt/actions-runner/hook-job-started.sh
install -m 0755 -o "${RUNNER_USER}" -g "${RUNNER_USER}" \
    "${STAGE_DIR}/hook-job-completed.sh" /opt/actions-runner/hook-job-completed.sh

# -----------------------------------------------------------------------------
# Kernel hardening.
# -----------------------------------------------------------------------------
log "installing sysctl hardening"
install -m 0644 "${STAGE_DIR}/99-scaleset-hardening.conf" /etc/sysctl.d/99-scaleset-hardening.conf

# -----------------------------------------------------------------------------
# Cloud-init: disable apt-update on first boot.
# -----------------------------------------------------------------------------
# By default cloud-init runs `apt-get update` (and optionally upgrade) on
# every clone's first boot. The template is built with current packages;
# burning ~10MB of bandwidth per clone for incremental refreshes is wasted
# and pointless (the VM lives for one job then dies).
#
# Workflows that need newer packages can run `apt-get update` themselves.
log "disabling cloud-init apt update/upgrade"
install -m 0644 /dev/stdin /etc/cloud/cloud.cfg.d/99-no-apt-update.cfg <<'CINIT'
# Disable cloud-init's first-boot apt refresh. The template is rebuilt
# regularly (security updates land via Packer re-runs, not via per-boot
# apt). Override per-VM if a specific workflow needs it.
package_update: false
package_upgrade: false
package_reboot_if_required: false
apt:
  preserve_sources_list: true
CINIT
# Defer sysctl --system to first boot via systemd; running it now in the
# build VM is unnecessary and may interact with the live ISO's network.

# -----------------------------------------------------------------------------
# SSH disable on first boot of every clone.
#
# We CAN'T remove openssh-server here — Packer is connected over the very
# SSH session we'd be terminating, so it dies mid-script and the build is
# torn down. Instead we install a oneshot systemd unit that runs once on
# the first boot of every clone (ConditionFirstBoot=yes) and removes SSH
# at that point. The template itself keeps SSH so we can re-Packer it.
# -----------------------------------------------------------------------------
log "installing firstboot SSH-removal oneshot"
install -m 0755 /dev/stdin /usr/local/sbin/firstboot-harden <<'HARDEN'
#!/bin/bash
# Runs once on first boot of a clone. Stops + masks openssh-server so
# the runner VM has no network-reachable shell, then self-disables.
#
# We DON'T purge openssh-server here — earlier attempts using
# `apt-get purge openssh-server && apt-get autoremove --purge` left
# the VM in a state where the Proxmox guest-agent endpoint returned
# "VM is not running" for ~30s, breaking JIT injection. Masking the
# service achieves the same security posture (no sshd ever runs)
# without touching apt or pulling network bandwidth.
set -eu
systemctl stop ssh.service ssh.socket 2>/dev/null || true
systemctl mask ssh.service ssh.socket 2>/dev/null || true
systemctl disable firstboot-harden.service 2>/dev/null || true
HARDEN

install -m 0644 /dev/stdin /etc/systemd/system/firstboot-harden.service <<'UNIT'
[Unit]
Description=First-boot hardening for ephemeral runner clone
ConditionFirstBoot=yes
After=multi-user.target
# NOTE: do NOT add `Before=gh-runner.path` here. gh-runner.path has
# WantedBy=multi-user.target, so adding Before=gh-runner.path creates
# an ordering cycle that systemd resolves by deleting gh-runner.path
# from the boot sequence. firstboot-harden running in parallel with
# the runner is fine — SSH removal is a defence-in-depth measure, not
# a precondition for the runner to start.

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/firstboot-harden
RemainAfterExit=true

[Install]
WantedBy=multi-user.target
UNIT
systemctl enable firstboot-harden.service

# -----------------------------------------------------------------------------
# Service trimming.
# -----------------------------------------------------------------------------
log "disabling unnecessary services"
# snapd: not used; substantial attack surface for a single-job VM.
apt-get purge -y snapd || true
rm -rf /root/snap /home/*/snap /var/cache/snapd

# Pollyfilly services we don't need on an ephemeral runner.
for svc in \
    multipathd.service \
    iscsid.service iscsid.socket open-iscsi.service \
    ModemManager.service \
    unattended-upgrades.service apt-daily.service apt-daily-upgrade.service \
    apt-daily.timer apt-daily-upgrade.timer
do
    systemctl disable --now "${svc}" 2>/dev/null || true
    systemctl mask         "${svc}" 2>/dev/null || true
done

# -----------------------------------------------------------------------------
# Login hardening.
# -----------------------------------------------------------------------------
log "tightening login defaults"
sed -i \
    -e 's/^UMASK\s\+.*/UMASK\t\t027/' \
    -e 's/^PASS_MAX_DAYS\s\+.*/PASS_MAX_DAYS\t90/' \
    -e 's/^PASS_MIN_DAYS\s\+.*/PASS_MIN_DAYS\t1/' \
    -e 's/^PASS_WARN_AGE\s\+.*/PASS_WARN_AGE\t14/' \
    /etc/login.defs

# Disable core dumps via limits.conf (sysctl drop-in already disables suid_dumpable).
cat >/etc/security/limits.d/99-no-core.conf <<'EOF'
* hard core 0
* soft core 0
EOF

# -----------------------------------------------------------------------------
# File permissions.
# -----------------------------------------------------------------------------
log "tightening permissions on sensitive files"
chmod 0640 /etc/shadow /etc/gshadow
chmod 0644 /etc/passwd /etc/group
chmod 0700 /root
[ -d "/home/${RUNNER_USER}" ] && chmod 0750 "/home/${RUNNER_USER}"

# -----------------------------------------------------------------------------
# Disable swap.
# -----------------------------------------------------------------------------
# Ephemeral VMs are sized to fit their job; swap mostly just persists
# secrets to disk. Remove any swap entries Subiquity may have created.
log "disabling swap"
swapoff -a || true
sed -i.bak -E '/(^|\s)swap(\s|$)/d' /etc/fstab
rm -f /swap.img /swapfile

# -----------------------------------------------------------------------------
# Cleanup for template conversion.
# -----------------------------------------------------------------------------
log "cleaning apt + tmp + logs"
apt-get autoremove --purge -y
apt-get clean
rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* "${STAGE_DIR}"

log "rotating + truncating journal"
journalctl --rotate || true
journalctl --vacuum-time=1s || true
find /var/log -type f -exec truncate -s 0 {} \;

# Clear shell history.
unset HISTFILE
rm -f /root/.bash_history /home/*/.bash_history

# Clear machine-id so each clone gets a fresh one on first boot.
# Empty file is required (not absent) so /etc/machine-id is regenerated.
log "clearing machine-id"
truncate -s 0 /etc/machine-id
test ! -L /var/lib/dbus/machine-id && ln -sf /etc/machine-id /var/lib/dbus/machine-id

# Reset cloud-init so clones perform first-boot routines.
log "resetting cloud-init"
cloud-init clean --logs --machine-id 2>/dev/null || cloud-init clean --logs || true

# -----------------------------------------------------------------------------
# Revoke build credentials (LAST step).
#
# After this point Packer will lose sudo on the next provisioner; we
# keep this script as the final shell provisioner in the build.
# -----------------------------------------------------------------------------
log "locking + de-privileging the build user (${BUILD_USERNAME})"
if id "${BUILD_USERNAME}" >/dev/null 2>&1; then
    # Lock the password so the account can't be used to log in.
    passwd -l "${BUILD_USERNAME}" || true
    # Remove the user from sudo group entirely.
    deluser "${BUILD_USERNAME}" sudo 2>/dev/null || true
fi

# TRIM the disk so Proxmox can compact the template.
log "fstrim"
fstrim -av || true

log "provisioning complete"
sync
