# Stream2Pretrain DHBWCloud bootstrap.
#
# Terraform only provisions the OpenStack VMs on the existing DHBWV6 network
# and writes an Ansible inventory. k3s installation and cluster add-ons are
# handled by the pinned Ansible role afterwards.

locals {
  base_metadata = merge(
    {
      stream2pretrain = "true"
      cluster         = var.cluster_name
    },
    var.extra_metadata,
  )
}

resource "openstack_compute_instance_v2" "master" {
  name              = "${var.cluster_name}-master"
  image_id          = var.image_id
  flavor_name       = var.control_flavor
  key_pair          = var.key_pair
  security_groups   = var.security_groups
  availability_zone = var.availability_zone != "" ? var.availability_zone : null

  network {
    name = var.network_name
  }

  metadata = merge(local.base_metadata, { role = "server" })

  lifecycle {
    prevent_destroy = true
  }
}

resource "openstack_compute_instance_v2" "worker" {
  count             = var.worker_count
  name              = "${var.cluster_name}-worker-${count.index + 1}"
  image_id          = var.image_id
  flavor_name       = var.worker_flavor
  key_pair          = var.key_pair
  security_groups   = var.security_groups
  availability_zone = var.availability_zone != "" ? var.availability_zone : null

  network {
    name = var.network_name
  }

  metadata = merge(local.base_metadata, {
    role  = "agent"
    index = tostring(count.index + 1)
  })

  lifecycle {
    prevent_destroy = true
  }
}

resource "local_file" "ansible_inventory" {
  filename = "${path.module}/generated-inventory.yml"
  content = yamlencode({
    all = {
      children = {
        stream2pretrain = {
          children = {
            stream2pretrain_k3s_server = {
              hosts = {
                (local.master_ansible_host) = {
                  ansible_user       = var.ssh_user
                  interpreter_python = var.interpreter_python
                  ip_family          = var.ip_family
                  k3s_role           = "server"
                }
              }
            }
            stream2pretrain_k3s_agent = {
              vars = {
                interpreter_python = var.interpreter_python
                ip_family          = var.ip_family
                k3s_server_host    = local.master_ansible_host
                k3s_role           = "agent"
              }
              hosts = {
                for worker in openstack_compute_instance_v2.worker :
                local.worker_ansible_hosts[worker.name] => {
                  ansible_user = var.ssh_user
                }
              }
            }
          }
        }
      }
    }
  })
}

locals {
  master_ansible_host = var.ip_family == "ipv6" ? openstack_compute_instance_v2.master.network[0].fixed_ip_v6 : openstack_compute_instance_v2.master.network[0].fixed_ip_v4
  worker_ansible_hosts = {
    for worker in openstack_compute_instance_v2.worker :
    worker.name => (var.ip_family == "ipv6" ? worker.network[0].fixed_ip_v6 : worker.network[0].fixed_ip_v4)
  }
}
