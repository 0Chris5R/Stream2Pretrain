# Variables for Stream2Pretrain OpenStack bootstrap.
# Required vars must be supplied via terraform.tfvars (gitignored) or TF_VAR_* env.
# Defaults reflect the DHBWCloud assumptions in CLAUDE.md (1 control + 2 workers).

variable "cluster_name" {
  description = "Logical cluster name; used as prefix for all resources."
  type        = string
  default     = "stream2pretrain"
}

variable "image_name" {
  description = "Glance image name for control + workers. Ubuntu 24.04 LTS expected."
  type        = string
  default     = "Ubuntu 24.04 LTS"
}

variable "control_flavor" {
  description = "Nova flavor for the k3s control-plane VM."
  type        = string
  default     = "m1.large"
}

variable "worker_flavor" {
  description = "Nova flavor for k3s worker VMs."
  type        = string
  default     = "m1.xlarge"
}

variable "worker_count" {
  description = "Number of k3s worker nodes."
  type        = number
  default     = 2
}

variable "external_network" {
  description = "Name of the external/floating-IP network (e.g. ext-net)."
  type        = string
}

variable "tenant_network_cidr" {
  description = "CIDR for the internal tenant subnet."
  type        = string
  default     = "10.123.0.0/24"
}

variable "dns_nameservers" {
  description = "Resolvers used by the tenant subnet."
  type        = list(string)
  default     = ["1.1.1.1", "9.9.9.9"]
}

variable "ssh_public_key" {
  description = "SSH public key (OpenSSH format) injected into all nodes."
  type        = string
}

variable "ssh_user" {
  description = "Default cloud user on the image (Ubuntu 24.04 = ubuntu)."
  type        = string
  default     = "ubuntu"
}

variable "control_data_volume_gb" {
  description = "Size of /var/lib/rancher/k3s data volume on the control plane."
  type        = number
  default     = 50
}

variable "worker_data_volume_gb" {
  description = "Size of /var/lib/rancher/k3s data volume on each worker."
  type        = number
  default     = 100
}

variable "k3s_version" {
  description = "k3s release channel or pinned version (e.g. v1.30.3+k3s1)."
  type        = string
  default     = "v1.30.3+k3s1"
}

variable "k3s_token" {
  description = "Cluster join token. If empty, a random one is generated."
  type        = string
  default     = ""
  sensitive   = true
}

variable "allowed_admin_cidrs" {
  description = "CIDRs allowed to reach the k3s API (6443) and SSH (22)."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "ingress_cidrs" {
  description = "CIDRs allowed to reach Traefik (80/443)."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "node_metrics_cidrs" {
  description = "CIDRs allowed to scrape node-exporter / kubelet metrics. Defaults to in-cluster only."
  type        = list(string)
  default     = ["10.123.0.0/24"]
}

variable "availability_zone" {
  description = "Nova/Cinder AZ. Empty string lets the cloud decide."
  type        = string
  default     = ""
}

variable "extra_tags" {
  description = "Additional metadata tags written onto every server."
  type        = map(string)
  default     = {}
}
