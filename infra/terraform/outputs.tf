# Useful Terraform outputs for the downstream Ansible bootstrap.

output "master_ip" {
  description = "Address of the k3s control-plane VM used by Ansible."
  value       = local.master_ansible_host
}

output "worker_ips" {
  description = "Addresses of the k3s worker VMs used by Ansible."
  value       = [for worker in openstack_compute_instance_v2.worker : local.worker_ansible_hosts[worker.name]]
}

output "ansible_inventory" {
  description = "Path to the generated Ansible inventory."
  value       = local_file.ansible_inventory.filename
}

output "ansible_ping_command" {
  description = "Quick connectivity check for the generated inventory."
  value       = "ansible -i ${local_file.ansible_inventory.filename} all -m ping"
}
