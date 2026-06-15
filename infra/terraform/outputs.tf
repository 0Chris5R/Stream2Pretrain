# Useful Terraform outputs for downstream tooling (k3s install, kubeconfig fetch).

output "control_floating_ip" {
  description = "Public IP of the k3s control plane (kubectl + Traefik dashboard host)."
  value       = openstack_networking_floatingip_v2.control.address
}

output "ingress_floating_ip" {
  description = "Public IP attached to the first worker for HTTP(S) ingress."
  value       = openstack_networking_floatingip_v2.ingress.address
}

output "control_internal_ip" {
  description = "Tenant-network IP of the control-plane VM."
  value       = openstack_compute_instance_v2.control.access_ip_v4
}

output "worker_internal_ips" {
  description = "Tenant-network IPs of worker VMs."
  value       = [for w in openstack_compute_instance_v2.worker : w.access_ip_v4]
}

output "ssh_user" {
  description = "Default cloud user for SSH."
  value       = var.ssh_user
}

output "kubeconfig_fetch_command" {
  description = "How to retrieve the cluster kubeconfig from the control plane."
  value = format(
    "ssh -o StrictHostKeyChecking=accept-new %s@%s 'sudo cat /etc/rancher/k3s/k3s.yaml' | sed 's/127.0.0.1/%s/' > kubeconfig.yaml && export KUBECONFIG=$PWD/kubeconfig.yaml",
    var.ssh_user,
    openstack_networking_floatingip_v2.control.address,
    openstack_networking_floatingip_v2.control.address,
  )
}

output "k3s_token" {
  description = "Cluster join token (sensitive)."
  value       = local.k3s_token
  sensitive   = true
}
