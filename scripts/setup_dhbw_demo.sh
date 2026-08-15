#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMMAND="${1:-verify}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
KUBECONFIG_PATH="${KUBECONFIG:-$ROOT_DIR/infra/kubeconfig-stream2pretrain.yaml}"
OPENRC_PATH="${OPENRC_PATH:-}"
if [[ -x /opt/homebrew/opt/helm@3/bin/helm ]]; then
  DEFAULT_HELM_BINARY=/opt/homebrew/opt/helm@3/bin/helm
else
  DEFAULT_HELM_BINARY=helm
fi
HELM_BINARY="${HELM_BINARY:-$DEFAULT_HELM_BINARY}"

if [[ "$ENVIRONMENT" != "dev" ]]; then
  printf 'Only the measured DHBW dev profile is deployable. Production remains needs-measurement.\n' >&2
  exit 1
fi

require_tool() {
  command -v "$1" >/dev/null 2>&1 || {
    printf 'Missing required tool: %s\n' "$1" >&2
    exit 1
  }
}

load_openstack_credentials() {
  if [[ -n "$OPENRC_PATH" && -f "$OPENRC_PATH" ]]; then
    set +u
    # shellcheck disable=SC1090
    source "$OPENRC_PATH"
    set -u
  elif [[ -n "$OPENRC_PATH" ]]; then
    printf 'OpenStack credential file does not exist: %s\n' "$OPENRC_PATH" >&2
    exit 1
  elif [[ -z "${OS_AUTH_URL:-}" && -z "${OS_CLOUD:-}" ]]; then
    printf 'OpenStack credentials are not configured. Set OPENRC_PATH or OS_CLOUD.\n' >&2
    exit 1
  fi
}

validate() {
  require_tool terraform
  require_tool kubectl
  require_tool helmfile
  if [[ ! -x "$HELM_BINARY" ]] && ! command -v "$HELM_BINARY" >/dev/null 2>&1; then
    printf 'Helm binary is not executable: %s\n' "$HELM_BINARY" >&2
    exit 1
  fi
  if [[ "$($HELM_BINARY version --template '{{.Version}}')" != v3.* ]]; then
    printf 'Helm 3 is required; set HELM_BINARY to a Helm 3 executable.\n' >&2
    exit 1
  fi
  terraform -chdir="$ROOT_DIR/infra/terraform" init -backend=false >/dev/null
  terraform -chdir="$ROOT_DIR/infra/terraform" fmt -check
  terraform -chdir="$ROOT_DIR/infra/terraform" validate
  "$HELM_BINARY" lint "$ROOT_DIR/charts/stream2pretrain" \
    -f "$ROOT_DIR/charts/stream2pretrain/values-$ENVIRONMENT.yaml" \
    -f "$ROOT_DIR/infra/helmfile-values/stream2pretrain.$ENVIRONMENT.yaml"
  local tier
  for tier in platform catalog application; do
    helmfile -b "$HELM_BINARY" -f "$ROOT_DIR/helmfile.yaml" -e "$ENVIRONMENT" \
      --selector "tier=$tier" lint
  done
}

plan_cluster() {
  load_openstack_credentials
  terraform -chdir="$ROOT_DIR/infra/terraform" init
  terraform -chdir="$ROOT_DIR/infra/terraform" plan -out=tfplan
}

apply_cluster() {
  require_tool ansible-galaxy
  require_tool ansible-playbook
  plan_cluster
  terraform -chdir="$ROOT_DIR/infra/terraform" apply tfplan
  ansible-galaxy install -r "$ROOT_DIR/infra/ansible/requirements.yml" --force
  ansible-playbook \
    -i "$ROOT_DIR/infra/terraform/generated-inventory.yml" \
    "$ROOT_DIR/infra/ansible/deploy.yaml"
}

apply_tier() {
  local tier="$1"
  require_tool helmfile
  export KUBECONFIG="$KUBECONFIG_PATH"
  helmfile -b "$HELM_BINARY" -f "$ROOT_DIR/helmfile.yaml" -e "$ENVIRONMENT" \
    --selector "tier=$tier" apply
}

required_secrets() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  local missing=0
  local item
  for item in \
    stream2pretrain/stream2pretrain-minio \
    stream2pretrain/stream2pretrain-polaris \
    stream2pretrain/stream2pretrain-github \
    stream2pretrain/stream2pretrain-hf \
    stream2pretrain/stream2pretrain-decon-signing; do
    if ! kubectl get secret -n "${item%%/*}" "${item##*/}" >/dev/null 2>&1; then
      printf 'Missing required Secret: %s\n' "$item" >&2
      missing=1
    fi
  done
  if ! kubectl get configmap -n stream2pretrain stream2pretrain-decon-benchmarks >/dev/null 2>&1; then
    printf 'Missing required ConfigMap: stream2pretrain/stream2pretrain-decon-benchmarks\n' >&2
    missing=1
  fi
  return "$missing"
}

required_platform_secret() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  if ! kubectl get secret -n monitoring grafana-admin >/dev/null 2>&1; then
    printf 'Missing required Secret: monitoring/grafana-admin\n' >&2
    return 1
  fi
}

required_catalog_secret() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  if ! kubectl get secret -n polaris polaris-bootstrap >/dev/null 2>&1; then
    printf 'Missing required Secret: polaris/polaris-bootstrap\n' >&2
    return 1
  fi
}

required_application_services() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  local missing=0
  local item
  for item in \
    minio/minio \
    polaris/polaris \
    redpanda/redpanda; do
    if ! kubectl get service -n "${item%%/*}" "${item##*/}" >/dev/null 2>&1; then
      printf 'Missing required Service: %s\n' "$item" >&2
      missing=1
    fi
  done
  return "$missing"
}

topic_exists() {
  local topic="$1"
  kubectl -n redpanda exec redpanda-0 -c redpanda -- rpk topic list \
    | awk 'NR > 1 {print $1}' \
    | grep -qx "$topic"
}

ensure_topics() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  local topic
  for topic in raw.fetched docs.normalized docs.curated decon.attest; do
    if ! topic_exists "$topic"; then
      kubectl -n redpanda exec redpanda-0 -c redpanda -- \
        rpk topic create "$topic" \
          --partitions 1 \
          --replicas 1 \
          --config retention.ms=604800000 \
          --config cleanup.policy=delete
    fi
  done
}

required_topics() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  local missing=0
  local topic
  for topic in raw.fetched docs.normalized docs.curated decon.attest; do
    if ! topic_exists "$topic"; then
      printf 'Missing required Redpanda topic: %s\n' "$topic" >&2
      missing=1
    fi
  done
  return "$missing"
}

verify() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  kubectl --request-timeout=20s get nodes
  kubectl --request-timeout=20s -n redpanda exec redpanda-0 -c redpanda -- rpk cluster health
  kubectl --request-timeout=20s -n redpanda exec redpanda-0 -c redpanda -- rpk topic list
  kubectl --request-timeout=20s -n stream2pretrain get deploy,statefulset,cronjob
}

case "$COMMAND" in
  validate)
    validate
    ;;
  plan)
    validate
    plan_cluster
    ;;
  cluster)
    validate
    apply_cluster
    ;;
  platform)
    validate
    required_platform_secret
    apply_tier "$COMMAND"
    ;;
  catalog)
    validate
    required_catalog_secret
    apply_tier catalog
    ;;
  topics)
    validate
    ensure_topics
    ;;
  application)
    validate
    required_secrets
    required_application_services
    required_topics
    apply_tier application
    ;;
  verify)
    validate
    verify
    ;;
  *)
    printf 'Usage: %s {validate|plan|cluster|platform|catalog|topics|application|verify}\n' "$0" >&2
    exit 2
    ;;
esac
