# Ubuntu 26.04 LTS template build for the orchestrator.
#
# Produces a Proxmox VM template containing:
#   - Ubuntu 26.04 LTS (server), installed via Subiquity autoinstall.
#   - qemu-guest-agent enabled (required for JIT injection).
#   - GitHub Actions runner binary at /opt/actions-runner/.
#   - A systemd path-unit watching /opt/actions-runner/jitconfig that
#     starts the runner when the orchestrator drops the file, then powers
#     the VM off after the job finishes.
#   - Kernel + service hardening defaults (see scripts/provision.sh).
#
# Run with:
#   packer init .
#   packer validate .
#   packer build .

packer {
  required_plugins {
    proxmox = {
      version = ">= 1.2.0"
      source  = "github.com/hashicorp/proxmox"
    }
  }
}

source "proxmox-iso" "ubuntu_2604_runner" {
  # ---- Proxmox connection ----
  proxmox_url              = var.proxmox_url
  username                 = var.proxmox_username
  token                    = var.proxmox_token
  insecure_skip_tls_verify = var.proxmox_insecure_skip_tls_verify
  node                     = var.proxmox_node

  # ---- Resulting template ----
  vm_id                = var.template_vm_id
  vm_name              = "${var.template_name}-builder"
  template_name        = var.template_name
  template_description = "Ubuntu 26.04 LTS GitHub Actions runner. Built {{ timestamp }}."

  # ---- Hardware ----
  cpu_type = "host"
  cores    = var.vm_cores
  sockets  = 1
  memory   = var.vm_memory_mb
  machine  = "q35"
  bios     = "ovmf"
  efi_config {
    efi_storage_pool  = var.storage_pool
    efi_type          = "4m"
    pre_enrolled_keys = false # secure-boot disabled by default; enable per-deployment after testing
  }

  scsi_controller = "virtio-scsi-single"

  disks {
    type         = "scsi"
    storage_pool = var.storage_pool
    disk_size    = "${var.vm_disk_size_gb}G"
    format       = "raw"
    io_thread    = true
    discard      = true
    ssd          = true
  }

  network_adapters {
    bridge   = var.network_bridge
    model    = "virtio"
    vlan_tag = var.network_vlan_tag != 0 ? tostring(var.network_vlan_tag) : ""
    firewall = false # firewall is orchestrator-level; runner VM is ephemeral
  }

  # ---- qemu-guest-agent ----
  # Required by the orchestrator's WaitReady + InjectJITConfig calls.
  qemu_agent = true

  # ---- cloud-init drive ----
  # Present in the template so clones can receive (minimal) per-clone
  # configuration if needed. The orchestrator's hot/warm path injects
  # the JIT config via guest-agent file-write, not cloud-init userdata.
  cloud_init              = true
  cloud_init_storage_pool = var.snippets_pool

  # ---- ISO + autoinstall ----
  # Exactly one of iso_url / iso_file must be set; Packer rejects both.
  boot_iso {
    type             = "scsi"
    iso_url          = var.iso_url
    iso_file         = var.iso_file
    iso_checksum     = var.iso_checksum
    iso_storage_pool = var.iso_storage_pool
    iso_download_pve = var.iso_download_pve
    unmount          = true
  }

  http_directory = "http"

  boot_command = [
    "c<wait>",
    "linux /casper/vmlinuz --- autoinstall ds=\"nocloud-net;s=http://{{ .HTTPIP }}:{{ .HTTPPort }}/\"",
    "<enter><wait>",
    "initrd /casper/initrd",
    "<enter><wait>",
    "boot",
    "<enter>",
  ]
  boot_wait = "8s"

  # ---- SSH (build-time only) ----
  ssh_username = var.build_username
  ssh_password = var.build_password
  ssh_timeout  = "45m"

  # tags become Proxmox VM tags on the template. The orchestrator
  # filters on `gh-scaleset-owner-*` not on these; these are purely for
  # human-readable indexing. Proxmox 9.x rejects '.' in tag names.
  tags = "ubuntu-2604;gh-runner;packer"
}

build {
  name    = "ubuntu-2604-runner"
  sources = ["source.proxmox-iso.ubuntu_2604_runner"]

  # The destination directory must exist before the file provisioner
  # uploads into it, otherwise scp refuses with "Is a directory".
  provisioner "shell" {
    inline = [
      "mkdir -p /tmp/scaleset-files",
    ]
  }

  # Copy systemd units + sysctl drop-in into the staging dir so the
  # provision script can install them with correct ownership/perms.
  provisioner "file" {
    source      = "files/"
    destination = "/tmp/scaleset-files/"
  }

  # Main provisioning + hardening pass.
  # NOTE: `{{ .Vars }}` is positioned AFTER `sudo -S` so the env vars are
  # passed as command-line assignments that sudo natively forwards.
  # `sudo -E` is ignored on Ubuntu (default sudoers), so the older
  # "{{ .Vars }} sudo -S -E bash" form silently drops everything.
  provisioner "shell" {
    execute_command   = "echo '${var.build_password}' | sudo -S {{ .Vars }} bash -eux '{{ .Path }}'"
    expect_disconnect = true
    environment_vars = [
      "RUNNER_VERSION=${var.runner_version}",
      "RUNNER_ARCH=${var.runner_arch}",
      "RUNNER_USER=${var.runner_user}",
      "BUILD_USERNAME=${var.build_username}",
    ]
    scripts = [
      "scripts/provision.sh",
    ]
  }
}
