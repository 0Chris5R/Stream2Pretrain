# Stream2Pretrain OpenStack bootstrap.
# Provisions:
#   - one tenant network + subnet + router to var.external_network
#   - one security group for the k3s control plane (API + SSH)
#   - one security group for ingress (Traefik 80/443)
#   - one shared in-cluster security group (full mesh between nodes)
#   - one control-plane VM running `k3s server` (single-server, embedded etcd disabled)
#   - var.worker_count worker VMs joining via the cluster token
#   - one Cinder volume per node mounted at /var/lib/rancher/k3s for durable state
#   - one floating IP on the control plane (kubectl + ingress)
#   - one floating IP shared via the workers (round-robin DNS in production)

resource "random_password" "k3s_token" {
  length  = 64
  special = false
}

locals {
  k3s_token = var.k3s_token != "" ? var.k3s_token : random_password.k3s_token.result

  base_tags = merge(
    {
      "stream2pretrain" = "true"
      "cluster"         = var.cluster_name
    },
    var.extra_tags,
  )
}

# ---- Network ----------------------------------------------------------------

resource "openstack_networking_network_v2" "tenant" {
  name           = "${var.cluster_name}-net"
  admin_state_up = true
  tags           = [for k, v in local.base_tags : "${k}=${v}"]
}

resource "openstack_networking_subnet_v2" "tenant" {
  name            = "${var.cluster_name}-subnet"
  network_id      = openstack_networking_network_v2.tenant.id
  cidr            = var.tenant_network_cidr
  ip_version      = 4
  dns_nameservers = var.dns_nameservers
  tags            = [for k, v in local.base_tags : "${k}=${v}"]
}

data "openstack_networking_network_v2" "external" {
  name = var.external_network
}

resource "openstack_networking_router_v2" "router" {
  name                = "${var.cluster_name}-router"
  external_network_id = data.openstack_networking_network_v2.external.id
  admin_state_up      = true
}

resource "openstack_networking_router_interface_v2" "router_iface" {
  router_id = openstack_networking_router_v2.router.id
  subnet_id = openstack_networking_subnet_v2.tenant.id
}

# ---- Security groups --------------------------------------------------------

resource "openstack_networking_secgroup_v2" "admin" {
  name        = "${var.cluster_name}-admin"
  description = "SSH + k3s API access from operators."
}

resource "openstack_networking_secgroup_rule_v2" "admin_ssh" {
  for_each          = toset(var.allowed_admin_cidrs)
  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "tcp"
  port_range_min    = 22
  port_range_max    = 22
  remote_ip_prefix  = each.value
  security_group_id = openstack_networking_secgroup_v2.admin.id
}

resource "openstack_networking_secgroup_rule_v2" "admin_kube_api" {
  for_each          = toset(var.allowed_admin_cidrs)
  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "tcp"
  port_range_min    = 6443
  port_range_max    = 6443
  remote_ip_prefix  = each.value
  security_group_id = openstack_networking_secgroup_v2.admin.id
}

resource "openstack_networking_secgroup_v2" "ingress" {
  name        = "${var.cluster_name}-ingress"
  description = "HTTP/HTTPS via Traefik."
}

resource "openstack_networking_secgroup_rule_v2" "ingress_http" {
  for_each          = toset(var.ingress_cidrs)
  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "tcp"
  port_range_min    = 80
  port_range_max    = 80
  remote_ip_prefix  = each.value
  security_group_id = openstack_networking_secgroup_v2.ingress.id
}

resource "openstack_networking_secgroup_rule_v2" "ingress_https" {
  for_each          = toset(var.ingress_cidrs)
  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "tcp"
  port_range_min    = 443
  port_range_max    = 443
  remote_ip_prefix  = each.value
  security_group_id = openstack_networking_secgroup_v2.ingress.id
}

resource "openstack_networking_secgroup_v2" "cluster_mesh" {
  name        = "${var.cluster_name}-mesh"
  description = "Full intra-cluster traffic between k3s nodes."
}

resource "openstack_networking_secgroup_rule_v2" "mesh_all_tcp" {
  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "tcp"
  remote_group_id   = openstack_networking_secgroup_v2.cluster_mesh.id
  security_group_id = openstack_networking_secgroup_v2.cluster_mesh.id
}

resource "openstack_networking_secgroup_rule_v2" "mesh_all_udp" {
  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "udp"
  remote_group_id   = openstack_networking_secgroup_v2.cluster_mesh.id
  security_group_id = openstack_networking_secgroup_v2.cluster_mesh.id
}

# Flannel VXLAN (UDP 8472) and Wireguard (51820) between nodes are covered by
# the all-UDP intra-mesh rule above.

resource "openstack_networking_secgroup_v2" "metrics" {
  name        = "${var.cluster_name}-metrics"
  description = "Allow Prometheus to scrape node-exporter (9100) and kubelet (10250)."
}

resource "openstack_networking_secgroup_rule_v2" "metrics_node_exporter" {
  for_each          = toset(var.node_metrics_cidrs)
  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "tcp"
  port_range_min    = 9100
  port_range_max    = 9100
  remote_ip_prefix  = each.value
  security_group_id = openstack_networking_secgroup_v2.metrics.id
}

resource "openstack_networking_secgroup_rule_v2" "metrics_kubelet" {
  for_each          = toset(var.node_metrics_cidrs)
  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "tcp"
  port_range_min    = 10250
  port_range_max    = 10250
  remote_ip_prefix  = each.value
  security_group_id = openstack_networking_secgroup_v2.metrics.id
}

# ---- SSH keypair ------------------------------------------------------------

resource "openstack_compute_keypair_v2" "ssh" {
  name       = "${var.cluster_name}-key"
  public_key = var.ssh_public_key
}

# ---- Cloud-init -------------------------------------------------------------

locals {
  cloud_init_server = file("${path.module}/../k3s-install/cloud-init-server.yaml")
  cloud_init_agent  = file("${path.module}/../k3s-install/cloud-init-agent.yaml")
}

data "cloudinit_config" "server" {
  gzip          = true
  base64_encode = true

  part {
    filename     = "00-base.yaml"
    content_type = "text/cloud-config"
    content      = local.cloud_init_server
  }

  part {
    filename     = "10-k3s-server.sh"
    content_type = "text/x-shellscript"
    content = templatefile(
      "${path.module}/../k3s-install/k3s-server.sh",
      {
        k3s_version  = var.k3s_version
        k3s_token    = local.k3s_token
        cluster_name = var.cluster_name
        cluster_cidr = var.tenant_network_cidr
      }
    )
  }
}

# ---- Persistent data volumes ------------------------------------------------

resource "openstack_blockstorage_volume_v3" "control_data" {
  name              = "${var.cluster_name}-control-data"
  size              = var.control_data_volume_gb
  availability_zone = var.availability_zone != "" ? var.availability_zone : null
}

resource "openstack_blockstorage_volume_v3" "worker_data" {
  count             = var.worker_count
  name              = "${var.cluster_name}-worker-${count.index}-data"
  size              = var.worker_data_volume_gb
  availability_zone = var.availability_zone != "" ? var.availability_zone : null
}

# ---- Control-plane VM -------------------------------------------------------

resource "openstack_compute_instance_v2" "control" {
  name              = "${var.cluster_name}-control"
  image_name        = var.image_name
  flavor_name       = var.control_flavor
  key_pair          = openstack_compute_keypair_v2.ssh.name
  availability_zone = var.availability_zone != "" ? var.availability_zone : null
  user_data         = data.cloudinit_config.server.rendered

  security_groups = [
    "default",
    openstack_networking_secgroup_v2.admin.name,
    openstack_networking_secgroup_v2.ingress.name,
    openstack_networking_secgroup_v2.cluster_mesh.name,
    openstack_networking_secgroup_v2.metrics.name,
  ]

  network {
    uuid = openstack_networking_network_v2.tenant.id
  }

  metadata = merge(local.base_tags, { "role" = "control" })

  depends_on = [openstack_networking_router_interface_v2.router_iface]
}

resource "openstack_compute_volume_attach_v2" "control_data" {
  instance_id = openstack_compute_instance_v2.control.id
  volume_id   = openstack_blockstorage_volume_v3.control_data.id
}

# In provider v3 the compute floating-IP associate resource was removed.
# We look up the instance's first port and bind the FIP at the networking layer.
data "openstack_networking_port_v2" "control" {
  device_id  = openstack_compute_instance_v2.control.id
  network_id = openstack_networking_network_v2.tenant.id
}

resource "openstack_networking_floatingip_v2" "control" {
  pool = data.openstack_networking_network_v2.external.name
}

resource "openstack_networking_floatingip_associate_v2" "control" {
  floating_ip = openstack_networking_floatingip_v2.control.address
  port_id     = data.openstack_networking_port_v2.control.id
}

# ---- Worker VMs -------------------------------------------------------------

data "cloudinit_config" "agent" {
  count         = var.worker_count
  gzip          = true
  base64_encode = true

  part {
    filename     = "00-base.yaml"
    content_type = "text/cloud-config"
    content      = local.cloud_init_agent
  }

  part {
    filename     = "10-k3s-agent.sh"
    content_type = "text/x-shellscript"
    content = templatefile(
      "${path.module}/../k3s-install/k3s-agent.sh",
      {
        k3s_version = var.k3s_version
        k3s_token   = local.k3s_token
        server_url  = "https://${openstack_compute_instance_v2.control.access_ip_v4}:6443"
        node_label  = "stream2pretrain.io/worker=true"
      }
    )
  }
}

resource "openstack_compute_instance_v2" "worker" {
  count             = var.worker_count
  name              = "${var.cluster_name}-worker-${count.index}"
  image_name        = var.image_name
  flavor_name       = var.worker_flavor
  key_pair          = openstack_compute_keypair_v2.ssh.name
  availability_zone = var.availability_zone != "" ? var.availability_zone : null
  user_data         = data.cloudinit_config.agent[count.index].rendered

  security_groups = [
    "default",
    openstack_networking_secgroup_v2.admin.name,
    openstack_networking_secgroup_v2.ingress.name,
    openstack_networking_secgroup_v2.cluster_mesh.name,
    openstack_networking_secgroup_v2.metrics.name,
  ]

  network {
    uuid = openstack_networking_network_v2.tenant.id
  }

  metadata = merge(local.base_tags, { "role" = "worker", "index" = tostring(count.index) })

  depends_on = [openstack_compute_instance_v2.control]
}

resource "openstack_compute_volume_attach_v2" "worker_data" {
  count       = var.worker_count
  instance_id = openstack_compute_instance_v2.worker[count.index].id
  volume_id   = openstack_blockstorage_volume_v3.worker_data[count.index].id
}

# Optionally one extra floating IP attached to the first worker for ingress fan-in
# (Traefik runs as a DaemonSet so any node can serve 80/443; floating IP can be
# moved between workers manually for HA).
data "openstack_networking_port_v2" "worker" {
  count      = var.worker_count
  device_id  = openstack_compute_instance_v2.worker[count.index].id
  network_id = openstack_networking_network_v2.tenant.id
}

resource "openstack_networking_floatingip_v2" "ingress" {
  pool = data.openstack_networking_network_v2.external.name
}

resource "openstack_networking_floatingip_associate_v2" "ingress" {
  count       = var.worker_count > 0 ? 1 : 0
  floating_ip = openstack_networking_floatingip_v2.ingress.address
  port_id     = data.openstack_networking_port_v2.worker[0].id
}
