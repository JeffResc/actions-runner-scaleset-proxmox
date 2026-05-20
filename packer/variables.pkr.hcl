# Variables for the Ubuntu 26.04 LTS runner template build.
#
# Provide values via a `variables.auto.pkrvars.hcl` file (gitignored) or
# the `-var` / `-var-file` flags. See `variables.auto.pkrvars.hcl.example`.

# ---------- Proxmox connection ----------

variable "proxmox_url" {
  type        = string
  description = "Proxmox VE API endpoint, e.g. https://pve.example.com:8006/api2/json"
}

variable "proxmox_username" {
  type        = string
  description = "API token ID, e.g. packer@pve!automation"
}

variable "proxmox_token" {
  type        = string
  description = "API token secret. Loaded from PKR_VAR_proxmox_token in CI."
  sensitive   = true
}

variable "proxmox_insecure_skip_tls_verify" {
  type        = bool
  default     = false
  description = "Set to true only when the controller serves a self-signed cert and you accept the risk."
}

variable "proxmox_node" {
  type        = string
  description = "Proxmox node to run the build on."
}

# ---------- Template VM ----------

variable "template_vm_id" {
  type        = number
  default     = 9000
  description = "VMID the resulting template will get. Must NOT overlap the orchestrator's vmid_range."
}

variable "template_name" {
  type        = string
  default     = "ubuntu-2604-runner-template"
}

variable "vm_cores" {
  type    = number
  default = 4
}

variable "vm_memory_mb" {
  type    = number
  default = 4096
}

variable "vm_disk_size_gb" {
  type    = number
  default = 32
}

# ---------- Storage / network ----------

variable "storage_pool" {
  type        = string
  description = "Proxmox storage pool for the OS disk (e.g. local-lvm)."
}

variable "iso_storage_pool" {
  type        = string
  description = "Storage pool that holds (or will hold) the Ubuntu ISO."
}

variable "snippets_pool" {
  type        = string
  default     = "local"
  description = "Storage pool with the 'snippets' content type enabled, used for cloud-init drive."
}

variable "network_bridge" {
  type    = string
  default = "vmbr0"
}

variable "network_vlan_tag" {
  type    = number
  default = 0
}

# ---------- ISO ----------

variable "iso_url" {
  type        = string
  default     = ""
  description = "HTTPS URL to the Ubuntu 26.04 ISO. Set this OR iso_file (mutually exclusive)."
}

variable "iso_file" {
  type        = string
  default     = ""
  description = "Proxmox storage path to a pre-uploaded ISO, e.g. 'local:iso/ubuntu-26.04-live-server-amd64.iso'. Set this OR iso_url."
}

variable "iso_checksum" {
  type        = string
  description = "Checksum of the ISO, e.g. 'sha256:abc...'. Always required."
}

variable "iso_download_pve" {
  type        = bool
  default     = true
  description = "When iso_url is set: have the Proxmox host download the ISO directly. true is recommended on LAN — avoids a ~2.7GB round-trip through Packer's host."
}

# ---------- Runner ----------

variable "runner_version" {
  type        = string
  default     = "2.334.0"
  description = "GitHub Actions runner release to bake into the image. See https://github.com/actions/runner/releases."
}

variable "runner_arch" {
  type    = string
  default = "x64"
}

variable "runner_user" {
  type    = string
  default = "runner"
}

# ---------- Build-time provisioning user ----------
#
# These credentials are used ONLY during the Packer build so that Packer's
# SSH provisioner can connect. The provision script locks the account
# (and uninstalls SSH entirely) before the template is finalized — they
# do not exist in the resulting template.

variable "build_username" {
  type    = string
  default = "ubuntu"
}

variable "build_password" {
  type      = string
  default   = "ubuntu"
  sensitive = true
}
