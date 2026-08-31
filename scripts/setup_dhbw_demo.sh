#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMMAND="${1:-verify}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
KUBECONFIG_PATH="${KUBECONFIG:-$ROOT_DIR/infra/kubeconfig-stream2pretrain.yaml}"
OPENRC_PATH="${OPENRC_PATH:-}"
DNS_CREDENTIALS_INVENTORY="${DNS_CREDENTIALS_INVENTORY:-$ROOT_DIR/../cloud/dns-credentials.yaml}"
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
  helmfile -b "$HELM_BINARY" -f "$ROOT_DIR/helmfile.yaml" -e "$ENVIRONMENT" lint
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
  ensure_ipv4_egress
}

ensure_ipv4_egress() {
  export KUBECONFIG="$KUBECONFIG_PATH"

  if kubectl get nodes \
    -o jsonpath='{range .items[*].spec.podCIDRs[*]}{.}{"\n"}{end}' \
    | grep -q '\.'; then
    return
  fi

  ansible-playbook \
    -i "$ROOT_DIR/infra/terraform/generated-inventory.yml" \
    "$ROOT_DIR/infra/ansible/configure-nat64.yaml"
}

apply_tier() {
  local tier="$1"
  require_tool helmfile
  export KUBECONFIG="$KUBECONFIG_PATH"
  helmfile -b "$HELM_BINARY" -f "$ROOT_DIR/helmfile.yaml" -e "$ENVIRONMENT" \
    --selector "tier=$tier" apply
}

apply_named_release() {
  local release_name="$1"
  require_tool helmfile
  export KUBECONFIG="$KUBECONFIG_PATH"
  helmfile -b "$HELM_BINARY" -f "$ROOT_DIR/helmfile.yaml" -e "$ENVIRONMENT" \
    --selector "name=$release_name" apply
}

configure_dns() {
  require_tool ansible-playbook

  if [[ ! -f "$DNS_CREDENTIALS_INVENTORY" ]]; then
    printf 'DNS credential inventory does not exist: %s\n' "$DNS_CREDENTIALS_INVENTORY" >&2
    exit 1
  fi

  ansible-playbook \
    -i "$ROOT_DIR/infra/terraform/generated-inventory.yml" \
    -i "$DNS_CREDENTIALS_INVENTORY" \
    "$ROOT_DIR/infra/ansible/configure-edge.yaml"
}

apply_ui_ingress() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  "$HELM_BINARY" template stream2pretrain "$ROOT_DIR/charts/stream2pretrain" \
    --namespace stream2pretrain \
    --values "$ROOT_DIR/charts/stream2pretrain/values-$ENVIRONMENT.yaml" \
    --values "$ROOT_DIR/infra/helmfile-values/stream2pretrain.$ENVIRONMENT.yaml" \
    --show-only templates/ui-ingress.yaml \
    | kubectl apply -f -
}

apply_edge() {
  ensure_ipv4_egress
  apply_named_release cert-manager
  apply_named_release traefik
  configure_dns
  export KUBECONFIG="$KUBECONFIG_PATH"
  kubectl wait --for=condition=Ready clusterissuer/dhbw-acme --timeout=120s
  kubectl -n traefik wait --for=condition=Ready \
    certificate/stream2pretrain-wildcard --timeout=180s
  apply_named_release external-dns
  apply_ui_ingress
  kubectl -n traefik rollout status deployment/traefik --timeout=120s
  kubectl -n external-dns rollout status deployment/external-dns --timeout=120s

  local stale_issuer
  stale_issuer="$(kubectl -n stream2pretrain get certificate stream2pretrain-ui-tls \
    -o jsonpath='{.spec.issuerRef.name}' 2>/dev/null || true)"
  if [[ "$stale_issuer" == "letsencrypt-prod" ]]; then
    kubectl -n stream2pretrain delete certificate stream2pretrain-ui-tls
  fi
}

bootstrap_polaris() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  kubectl -n stream2pretrain rollout status deployment/stream2pretrain-duckdb --timeout=120s
  kubectl -n stream2pretrain exec -i deployment/stream2pretrain-duckdb -- \
    python - < "$ROOT_DIR/scripts/bootstrap_polaris.py"
}

ensure_foundry_signing_identity() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  bash "$ROOT_DIR/scripts/ensure_foundry_signing_secret.sh" \
    stream2pretrain stream2pretrain-foundry-signing
}

required_secrets() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  local missing=0
  local item
  local namespace
  local remainder
  local secret
  local key
  for item in \
    stream2pretrain/stream2pretrain-minio/accessKey \
    stream2pretrain/stream2pretrain-minio/secretKey \
    stream2pretrain/stream2pretrain-polaris/credential \
    stream2pretrain/stream2pretrain-polaris/scope \
    stream2pretrain/stream2pretrain-hf/token; do
    namespace="${item%%/*}"
    remainder="${item#*/}"
    secret="${remainder%%/*}"
    key="${remainder#*/}"
    if ! kubectl get secret -n "$namespace" "$secret" \
      -o "go-template={{ index .data \"$key\" }}" 2>/dev/null \
      | grep -q .; then
      printf 'Missing required Secret key: %s\n' "$item" >&2
      missing=1
    fi
  done
  if "$HELM_BINARY" template stream2pretrain "$ROOT_DIR/charts/stream2pretrain" \
    --namespace stream2pretrain \
    --values "$ROOT_DIR/charts/stream2pretrain/values-$ENVIRONMENT.yaml" \
    --values "$ROOT_DIR/infra/helmfile-values/stream2pretrain.$ENVIRONMENT.yaml" \
    --show-only templates/processor-foundry.yaml \
    | grep -q '^kind: StatefulSet$'; then
    for item in \
      stream2pretrain/stream2pretrain-foundry-providers/HETZNER_INFERENCE_API_KEY \
      stream2pretrain/stream2pretrain-foundry-providers/controlToken; do
      namespace="${item%%/*}"
      remainder="${item#*/}"
      secret="${remainder%%/*}"
      key="${remainder#*/}"
      if ! kubectl get secret -n "$namespace" "$secret" \
        -o "go-template={{ index .data \"$key\" }}" 2>/dev/null \
        | grep -q .; then
        printf 'Missing required Secret key: %s\n' "$item" >&2
        missing=1
      fi
    done
  fi
  return "$missing"
}

required_catalog_secret() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  local missing=0
  local secret
  for secret in polaris-bootstrap polaris-minio; do
    if ! kubectl get secret -n polaris "$secret" >/dev/null 2>&1; then
      printf 'Missing required Secret: polaris/%s\n' "$secret" >&2
      missing=1
    fi
  done
  return "$missing"
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
  for topic in \
    raw.fetched raw.smoke \
    docs.normalized docs.normalized.smoke \
    docs.curated docs.curated.smoke \
    curation.decisions curation.decisions.smoke \
    license.admissions license.admissions.smoke \
    foundry.jobs foundry.events foundry.artifacts; do
    if ! topic_exists "$topic"; then
      local retention_ms=604800000
      [[ "$topic" == *.smoke ]] && retention_ms=86400000
      kubectl -n redpanda exec redpanda-0 -c redpanda -- \
        rpk topic create "$topic" \
          --partitions 1 \
          --replicas 1 \
          --topic-config "retention.ms=$retention_ms" \
          --topic-config cleanup.policy=delete \
          --topic-config max.message.bytes=2097152
    fi
  done
}

required_topics() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  local missing=0
  local topic
  for topic in \
    raw.fetched raw.smoke \
    docs.normalized docs.normalized.smoke \
    docs.curated docs.curated.smoke \
    curation.decisions curation.decisions.smoke \
    license.admissions license.admissions.smoke \
    foundry.jobs foundry.events foundry.artifacts; do
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
    apply_edge
    apply_tier "$COMMAND"
    ;;
  edge)
    validate
    apply_edge
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
    ensure_foundry_signing_identity
    required_secrets
    required_application_services
    required_topics
    apply_tier application
    bootstrap_polaris
    ;;
  verify)
    validate
    verify
    ;;
  *)
    printf 'Usage: %s {validate|plan|cluster|platform|edge|catalog|topics|application|verify}\n' "$0" >&2
    exit 2
    ;;
esac
