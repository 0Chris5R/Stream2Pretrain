# Variables for the DHBWCloud VM bootstrap.
#
# The remaining defaults describe the measured DHBW cluster: VMs attach to the
# existing DHBWV6 network and k3s is installed later through Ansible.

variable "cluster_name" {
  description = "Logical cluster name; used as prefix for all VM names."
  type        = string
  default     = "stream2pretrain"
}

variable "image_id" {
  description = "OpenStack Glance image id for all VMs."
  type        = string

  validation {
    condition     = length(trimspace(var.image_id)) > 0
    error_message = "image_id must be supplied from the target DHBWCloud project."
  }
}

variable "control_flavor" {
  description = "Nova flavor for the k3s control-plane VM."
  type        = string
  default     = "k8s.node"
}

variable "worker_flavor" {
  description = "Nova flavor for k3s worker VMs."
  type        = string
  default     = "k8s.node"
}

variable "worker_count" {
  description = "Number of k3s worker nodes."
  type        = number
  default     = 2

  validation {
    condition     = var.worker_count >= 1 && floor(var.worker_count) == var.worker_count
    error_message = "worker_count must be a positive whole number."
  }
}

variable "key_pair" {
  description = "Existing OpenStack keypair name used for SSH access."
  type        = string

  validation {
    condition     = length(trimspace(var.key_pair)) > 0
    error_message = "key_pair must name an existing keypair in the target project."
  }
}

variable "network_name" {
  description = "Existing OpenStack network name. DHBWCloud demo uses DHBWV6."
  type        = string
  default     = "DHBWV6"
}

variable "ip_family" {
  description = "Address family used in the generated Ansible inventory."
  type        = string
  default     = "dual"

  validation {
    condition     = contains(["ipv4", "ipv6", "dual"], var.ip_family)
    error_message = "ip_family must be ipv4, ipv6, or dual."
  }
}

variable "ssh_user" {
  description = "Default cloud user on the image."
  type        = string
  default     = "ubuntu"
}

variable "interpreter_python" {
  description = "Python interpreter path used by Ansible on the VMs."
  type        = string
  default     = "/usr/bin/python3"
}

variable "security_groups" {
  description = "OpenStack security groups attached to all VMs. The lecture deployment uses the project default group."
  type        = list(string)
  default     = ["default"]
}

variable "availability_zone" {
  description = "Nova availability zone. Empty string lets the cloud decide."
  type        = string
  default     = ""
}

variable "extra_metadata" {
  description = "Additional OpenStack metadata written onto every VM."
  type        = map(string)
  default     = {}
}
