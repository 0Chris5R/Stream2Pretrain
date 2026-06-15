#!/usr/bin/env bash
# k3s agent bootstrap for Stream2Pretrain workers.
#
# Templated by Terraform with:
#   k3s_version - pinned tag
#   k3s_token   - shared join secret
#   server_url  - https://<control-internal-ip>:6443
#   node_label  - extra label, key=value

set -euo pipefail

K3S_VERSION="${k3s_version}"
K3S_TOKEN="${k3s_token}"
K3S_URL="${server_url}"
NODE_LABEL="${node_label}"

DATA_DEV="/dev/vdb"
DATA_DIR="/var/lib/rancher/k3s"

log() { printf '[k3s-agent][%s] %s\n' "$(date -u +%FT%TZ)" "$*"; }

if [ -x /usr/local/bin/k3s-agent ] || [ -x /usr/local/bin/k3s ]; then
  log "k3s already installed; skipping installer."
else
  log "preparing data volume $DATA_DEV -> $DATA_DIR"
  mkdir -p "$DATA_DIR"

  if [ -b "$DATA_DEV" ]; then
    if ! blkid "$DATA_DEV" >/dev/null 2>&1; then
      mkfs.ext4 -L k3s-data "$DATA_DEV"
    fi
    grep -q "LABEL=k3s-data" /etc/fstab || \
      echo "LABEL=k3s-data $DATA_DIR ext4 defaults,nofail 0 2" >> /etc/fstab
    mount -a
  else
    log "WARN: $DATA_DEV not present; falling back to root volume."
  fi

  # Wait for the control plane to be reachable before joining.
  log "waiting for control plane at $K3S_URL"
  for _ in $(seq 1 90); do
    if curl -sk --max-time 3 "$K3S_URL/healthz" >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done

  log "installing k3s $K3S_VERSION (agent)"
  curl -sfL https://get.k3s.io | \
    INSTALL_K3S_VERSION="$K3S_VERSION" \
    K3S_URL="$K3S_URL" \
    K3S_TOKEN="$K3S_TOKEN" \
    INSTALL_K3S_EXEC="agent \
      --node-label=$NODE_LABEL \
      --kubelet-arg=container-log-max-files=5 \
      --kubelet-arg=container-log-max-size=50Mi" \
    sh -
fi

log "k3s agent install complete; joined $K3S_URL"
