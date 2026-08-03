# Variables for the DHBWCloud VM bootstrap.
#
# Defaults mirror the already-working demo project in ~/DHBW/cloud: VMs are
# attached directly to the existing DHBWV6 network and k3s is installed later
# through Ansible.

variable "cluster_name" {
  description = "Logical cluster name; used as prefix for all VM names."
  type        = string
  default     = "stream2pretrain"
}

variable "image_id" {
  description = "OpenStack Glance image id for all VMs."
  type        = string
  default     = "7842eb53-0ac7-4677-9160-2466371b4302"
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
}

variable "key_pair" {
  description = "Existing OpenStack keypair name used for SSH access."
  type        = string
  default     = "Julian"
}

variable "network_name" {
  description = "Existing OpenStack network name. DHBWCloud demo uses DHBWV6."
  type        = string
  default     = "DHBWV6"
}

variable "ip_family" {
  description = "Address family used in the generated Ansible inventory."
  type        = string
  default     = "ipv6"

  validation {
    condition     = contains(["ipv4", "ipv6"], var.ip_family)
    error_message = "ip_family must be either ipv4 or ipv6."
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
  description = "OpenStack security groups attached to all VMs."
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
