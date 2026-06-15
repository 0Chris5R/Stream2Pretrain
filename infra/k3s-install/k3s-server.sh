#!/usr/bin/env bash
# k3s server bootstrap for the Stream2Pretrain control plane.
# Single-server cluster (embedded etcd disabled) - SQLite is sufficient at this
# scale; HA is out of scope for the demo (CLAUDE.md, Week 1).
#
# Templated by Terraform via templatefile() with these variables:
#   k3s_version  - pinned channel/tag, e.g. v1.30.3+k3s1
#   k3s_token    - shared join secret used by agents
#   cluster_name - logical cluster name (used as node label)
#   cluster_cidr - tenant subnet (used to bind the API server)
#
# Idempotent: re-running this on an already-installed node is a no-op.

set -euo pipefail

K3S_VERSION="${k3s_version}"
K3S_TOKEN="${k3s_token}"
CLUSTER_NAME="${cluster_name}"
CLUSTER_CIDR="${cluster_cidr}"

DATA_DEV="/dev/vdb"
DATA_DIR="/var/lib/rancher/k3s"

log() { printf '[k3s-server][%s] %s\n' "$(date -u +%FT%TZ)" "$*"; }

if [ -x /usr/local/bin/k3s ]; then
  log "k3s already installed; skipping installer."
else
  log "preparing data volume $DATA_DEV -> $DATA_DIR"
  mkdir -p "$DATA_DIR"

  # Format only if the device exists and has no filesystem (idempotent).
  if [ -b "$DATA_DEV" ]; then
    if ! blkid "$DATA_DEV" >/dev/null 2>&1; then
      mkfs.ext4 -L k3s-data "$DATA_DEV"
    fi
    grep -q "LABEL=k3s-data" /etc/fstab || \
      echo "LABEL=k3s-data $DATA_DIR ext4 defaults,nofail 0 2" >> /etc/fstab
    mount -a
  else
    log "WARN: $DATA_DEV not present; falling back to root volume for $DATA_DIR"
  fi

  log "installing k3s $K3S_VERSION (server)"
  curl -sfL https://get.k3s.io | \
    INSTALL_K3S_VERSION="$K3S_VERSION" \
    K3S_TOKEN="$K3S_TOKEN" \
    INSTALL_K3S_EXEC="server \
      --cluster-cidr=10.42.0.0/16 \
      --service-cidr=10.43.0.0/16 \
      --tls-san=$(hostname -I | awk '{print $1}') \
      --node-label=stream2pretrain.io/cluster=$CLUSTER_NAME \
      --node-label=stream2pretrain.io/role=control \
      --disable=traefik \
      --write-kubeconfig-mode=0640 \
      --kube-controller-manager-arg=bind-address=0.0.0.0 \
      --kube-scheduler-arg=bind-address=0.0.0.0 \
      --kube-proxy-arg=metrics-bind-address=0.0.0.0 \
      --kubelet-arg=container-log-max-files=5 \
      --kubelet-arg=container-log-max-size=50Mi" \
    sh -
fi

log "waiting for k3s API to become ready"
for _ in $(seq 1 60); do
  if k3s kubectl get nodes >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

# Drop the kubeconfig into the default user's home for convenience.
if id -u ubuntu >/dev/null 2>&1; then
  install -d -o ubuntu -g ubuntu -m 0700 /home/ubuntu/.kube
  install -o ubuntu -g ubuntu -m 0600 /etc/rancher/k3s/k3s.yaml /home/ubuntu/.kube/config
fi

log "k3s server ready; cluster-cidr=$CLUSTER_CIDR; traefik disabled (Helmfile installs it)."
