#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMMAND="${1:-verify}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
CORE_ONLY="${S2P_CORE_ONLY:-0}"
KUBECONFIG_PATH="${KUBECONFIG:-$ROOT_DIR/infra/kubeconfig-stream2pretrain.yaml}"
OPENRC_PATH="${OPENRC_PATH:-}"
DNS_CREDENTIALS_INVENTORY="${DNS_CREDENTIALS_INVENTORY:-$ROOT_DIR/../cloud/dns-credentials.yaml}"
if [[ -x /opt/homebrew/opt/helm@3/bin/helm ]]; then
  DEFAULT_HELM_BINARY=/opt/homebrew/opt/helm@3/bin/helm
else
  DEFAULT_HELM_BINARY=helm
fi
HELM_BINARY="${HELM_BINARY:-$DEFAULT_HELM_BINARY}"
BYTEWAX_QUIESCED_RESOURCES=()

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
  require_tool jq
  require_tool uv
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
  "$HELM_BINARY" lint "$ROOT_DIR/charts/minio"
  "$HELM_BINARY" lint "$ROOT_DIR/charts/stream2pretrain" \
    -f "$ROOT_DIR/charts/stream2pretrain/values-$ENVIRONMENT.yaml" \
    -f "$ROOT_DIR/infra/helmfile-values/stream2pretrain.$ENVIRONMENT.yaml"
  "$HELM_BINARY" template stream2pretrain "$ROOT_DIR/charts/stream2pretrain" \
    --namespace stream2pretrain \
    --values "$ROOT_DIR/charts/stream2pretrain/values-$ENVIRONMENT.yaml" \
    --values "$ROOT_DIR/infra/helmfile-values/stream2pretrain.$ENVIRONMENT.yaml" \
    --values "$ROOT_DIR/infra/helmfile-values/stream2pretrain.horizontal-scaling.yaml" \
    >/dev/null
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

require_minio_data_migration() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  local legacy_claim
  legacy_claim="$(
    kubectl -n minio get statefulset/minio -o json 2>/dev/null \
      | jq -r '
          first(
            .spec.template.spec.volumes[]?
            | select(.name == "data")
            | .persistentVolumeClaim.claimName
          ) // empty
        ' \
      || true
  )"
  if [[ -n "$legacy_claim" ]]; then
    printf 'The retained standalone MinIO claim has not been migrated: %s\n' \
      "$legacy_claim" >&2
    printf 'Copy its objects into the distributed claims and verify checksums before this upgrade.\n' >&2
    return 1
  fi
}

require_polaris_postgres_migration() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  if kubectl -n polaris get statefulset/polaris-postgres >/dev/null 2>&1 \
     && ! kubectl -n polaris get \
       cluster.postgresql.cnpg.io/polaris-postgres >/dev/null 2>&1; then
    printf 'The retained standalone Polaris PostgreSQL data has not been migrated.\n' >&2
    printf 'Complete and verify a pg_dump/restore into CloudNativePG before this upgrade.\n' >&2
    return 1
  fi
}

validate_bytewax_checkpoint_storage() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  local feature_values="$ROOT_DIR/infra/helmfile-values/stream2pretrain.full.yaml"
  if [[ "$CORE_ONLY" == "1" ]]; then
    feature_values="$ROOT_DIR/infra/helmfile-values/stream2pretrain.core-only.yaml"
  fi
  local rendered
  rendered="$(mktemp "${TMPDIR:-/tmp}/s2p-bytewax-render.XXXXXX")"
  if ! "$HELM_BINARY" template stream2pretrain "$ROOT_DIR/charts/stream2pretrain" \
    --namespace stream2pretrain \
    --values "$ROOT_DIR/charts/stream2pretrain/values-$ENVIRONMENT.yaml" \
    --values "$ROOT_DIR/infra/helmfile-values/stream2pretrain.$ENVIRONMENT.yaml" \
    --values "$feature_values" \
    > "$rendered"; then
    rm -f "$rendered"
    return 1
  fi

  local contracts
  if ! contracts="$(
    uv run --frozen python - \
      "$rendered" \
      "$ROOT_DIR/charts/stream2pretrain/values.yaml" \
      "$ROOT_DIR/charts/stream2pretrain/values-$ENVIRONMENT.yaml" \
      "$ROOT_DIR/infra/helmfile-values/stream2pretrain.$ENVIRONMENT.yaml" \
      "$feature_values" <<'PY'
import sys

import yaml


def merge(target, update):
    for key, value in update.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            merge(target[key], value)
        else:
            target[key] = value


with open(sys.argv[1], encoding="utf-8") as stream:
    resources = [item for item in yaml.safe_load_all(stream) if item]
values = {}
for path in sys.argv[2:]:
    with open(path, encoding="utf-8") as stream:
        merge(values, yaml.safe_load(stream) or {})

claims = {
    item["metadata"]["name"]: item
    for item in resources
    if item.get("kind") == "PersistentVolumeClaim"
}
statefulsets = {
    item.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component"): item
    for item in resources
    if item.get("kind") == "StatefulSet"
}
workloads = {
    "processor-fetcher": ("fetcher", "checkpoint"),
    "processor-curate": ("curate", "checkpoint"),
    "processor-iceberg-writer": ("iceberg", "checkpoint"),
    "foundry-worker": ("foundry", "state"),
}
for component, (values_key, checkpoint_key) in workloads.items():
    statefulset = statefulsets.get(component)
    if statefulset is None:
        continue
    replicas = int(statefulset["spec"]["replicas"])
    recovery = next(
        item
        for item in statefulset["spec"]["template"]["spec"]["volumes"]
        if item.get("name") == "recovery"
    )
    claim_name = recovery["persistentVolumeClaim"]["claimName"]
    claim = claims.get(claim_name)
    checkpoint = values["processor"][values_key][checkpoint_key]
    access_modes = (
        claim["spec"].get("accessModes", [])
        if claim is not None
        else [checkpoint.get("accessMode", "")]
    )
    storage_class = (
        claim["spec"].get("storageClassName", "")
        if claim is not None
        else checkpoint.get("storageClass", "")
    )
    if replicas > 1:
        if "ReadWriteMany" not in access_modes:
            raise SystemExit(f"{component} needs a ReadWriteMany checkpoint before scaling")
        if not storage_class or storage_class == "local-path":
            raise SystemExit(f"{component} needs an explicit non-local RWX storage class")
    managed = "true" if claim is not None else "false"
    print(
        f"{component}\t{statefulset['metadata']['name']}\t{replicas}\t"
        f"{claim_name}\t{storage_class}\t{managed}"
    )
PY
  )"; then
    rm -f "$rendered"
    return 1
  fi
  rm -f "$rendered"

  local component
  local workload
  local replicas
  local claim_name
  local desired_storage_class
  local managed
  local legacy_claim
  local actual_claim
  local actual_access_modes
  local actual_storage_class
  local migrated_from
  local migration_verified
  local coordination_secret
  local foundry_control_migrated_from
  local foundry_control_migration_verified
  local foundry_control_manifest_sha256
  while IFS=$'\t' read -r \
    component workload replicas claim_name desired_storage_class managed; do
    if [[ -z "$workload" ]]; then
      continue
    fi
    actual_claim=""
    legacy_claim=""
    case "$component" in
      processor-curate)
        legacy_claim=checkpoint-stream2pretrain-processor-curate-0
        ;;
      foundry-worker)
        legacy_claim=state-stream2pretrain-foundry-0
        ;;
    esac
    if [[ -n "$legacy_claim" ]] \
       && kubectl -n stream2pretrain get "persistentvolumeclaim/$legacy_claim" \
         >/dev/null 2>&1; then
      if [[ "$managed" == "true" ]]; then
        printf 'Legacy Bytewax recovery still exists: %s\n' "$legacy_claim" >&2
        printf 'Set the component existingClaim to an explicitly copied and verified target before this upgrade.\n' >&2
        return 1
      fi
      if ! actual_claim="$(
        kubectl -n stream2pretrain get \
          "persistentvolumeclaim/$claim_name" -o json 2>/dev/null
      )"; then
        printf 'Copied Bytewax checkpoint target is missing: %s\n' "$claim_name" >&2
        printf 'Copy and verify %s before this upgrade.\n' "$legacy_claim" >&2
        return 1
      fi
      migrated_from="$(
        jq -r '.metadata.annotations["stream2pretrain.io/migrated-from"] // empty' \
          <<< "$actual_claim"
      )"
      migration_verified="$(
        jq -r '.metadata.annotations["stream2pretrain.io/migration-verified"] // empty' \
          <<< "$actual_claim"
      )"
      if [[ "$migrated_from" != "$legacy_claim" \
         || "$migration_verified" != "true" ]]; then
        printf 'Bytewax migration target is not marked as copied and verified: %s\n' \
          "$claim_name" >&2
        printf 'Migrate recovery plus local decision and duplicate state before setting the migration annotations.\n' >&2
        printf 'Required annotations: stream2pretrain.io/migrated-from=%s and stream2pretrain.io/migration-verified=true\n' \
          "$legacy_claim" >&2
        return 1
      fi
      if [[ "$component" == "foundry-worker" ]]; then
        if ! coordination_secret="$(
          kubectl -n stream2pretrain get \
            secret/stream2pretrain-coordination -o json 2>/dev/null
        )"; then
          printf 'Foundry control-state migration marker Secret is missing.\n' >&2
          return 1
        fi
        foundry_control_migrated_from="$(
          jq -r '.metadata.annotations["stream2pretrain.io/foundry-control-migrated-from"] // empty' \
            <<< "$coordination_secret"
        )"
        foundry_control_migration_verified="$(
          jq -r '.metadata.annotations["stream2pretrain.io/foundry-control-migration-verified"] // empty' \
            <<< "$coordination_secret"
        )"
        foundry_control_manifest_sha256="$(
          jq -r '.metadata.annotations["stream2pretrain.io/foundry-control-migration-manifest-sha256"] // empty' \
            <<< "$coordination_secret"
        )"
        if [[ "$foundry_control_migrated_from" != "$legacy_claim" \
           || "$foundry_control_migration_verified" != "true" \
           || ! "$foundry_control_manifest_sha256" =~ ^[0-9A-Fa-f]{64}$ ]]; then
          printf 'Foundry SQLite control and quota migration is not independently verified.\n' >&2
          printf 'Annotate secret/stream2pretrain-coordination with the migrated source, verified flag and 64-hex manifest digest.\n' >&2
          return 1
        fi
      fi
    fi
    if [[ -z "$actual_claim" ]] \
       && ! actual_claim="$(
         kubectl -n stream2pretrain get \
           "persistentvolumeclaim/$claim_name" -o json 2>/dev/null
       )"; then
      if [[ "$managed" == "false" ]]; then
        printf 'Configured Bytewax checkpoint claim is missing: %s\n' "$claim_name" >&2
        return 1
      fi
      continue
    fi
    if [[ "$replicas" -le 1 ]]; then
      continue
    fi
    actual_access_modes="$(jq -r '.spec.accessModes // [] | join(",")' <<< "$actual_claim")"
    actual_storage_class="$(jq -r '.spec.storageClassName // empty' <<< "$actual_claim")"
    if [[ ",$actual_access_modes," != *,ReadWriteMany,* ]]; then
      printf 'Retained Bytewax checkpoint claim is not ReadWriteMany: %s\n' "$claim_name" >&2
      return 1
    fi
    if [[ "$actual_storage_class" == "local-path" ]]; then
      printf 'Retained Bytewax checkpoint claim uses local-path: %s\n' "$claim_name" >&2
      return 1
    fi
    if [[ "$actual_storage_class" != "$desired_storage_class" ]]; then
      printf 'Retained Bytewax checkpoint storage class differs from the rendered class: %s\n' \
        "$claim_name" >&2
      return 1
    fi
  done <<< "$contracts"
}

delete_legacy_bytewax_statefulsets() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  local curator
  curator="$(
    kubectl -n stream2pretrain get \
      statefulset/stream2pretrain-processor-curate -o json 2>/dev/null \
      || true
  )"
  if [[ -n "$curator" ]] \
     && [[ "$(jq '.spec.volumeClaimTemplates // [] | length' <<< "$curator")" -gt 0 ]]; then
    kubectl -n stream2pretrain delete \
      statefulset/stream2pretrain-processor-curate --wait=true
  fi
  if kubectl -n stream2pretrain get \
    statefulset/stream2pretrain-foundry >/dev/null 2>&1; then
    kubectl -n stream2pretrain delete \
      statefulset/stream2pretrain-foundry --wait=true
  fi
}

restore_quiesced_bytewax_executions() {
  local saved
  local resource
  local replicas
  for saved in "${BYTEWAX_QUIESCED_RESOURCES[@]}"; do
    resource="${saved%:*}"
    replicas="${saved##*:}"
    if kubectl -n stream2pretrain get "$resource" >/dev/null 2>&1; then
      kubectl -n stream2pretrain scale "$resource" --replicas="$replicas" || true
    fi
  done
}

quiesce_bytewax_executions() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  BYTEWAX_QUIESCED_RESOURCES=()
  local resource
  local replicas
  for resource in \
    statefulset/stream2pretrain-processor-fetcher \
    statefulset/stream2pretrain-processor-curate \
    statefulset/stream2pretrain-processor-iceberg-writer \
    statefulset/stream2pretrain-foundry-worker \
    statefulset/stream2pretrain-foundry \
    deployment/stream2pretrain-processor-fetcher \
    deployment/stream2pretrain-processor-iceberg-writer
  do
    if ! kubectl -n stream2pretrain get "$resource" >/dev/null 2>&1; then
      continue
    fi
    if ! replicas="$(
      kubectl -n stream2pretrain get "$resource" \
        -o jsonpath='{.spec.replicas}'
    )"; then
      restore_quiesced_bytewax_executions
      return 1
    fi
    BYTEWAX_QUIESCED_RESOURCES+=("$resource:$replicas")
    if ! kubectl -n stream2pretrain scale "$resource" --replicas=0; then
      restore_quiesced_bytewax_executions
      return 1
    fi
  done

  local component
  local remaining
  for component in \
    processor-fetcher \
    processor-curate \
    processor-iceberg-writer \
    foundry-worker \
    foundry
  do
    if ! remaining="$(
      kubectl -n stream2pretrain get pod \
        -l "app.kubernetes.io/component=$component" -o name
    )"; then
      restore_quiesced_bytewax_executions
      return 1
    fi
    if [[ -z "$remaining" ]]; then
      continue
    fi
    if ! kubectl -n stream2pretrain wait --for=delete pod \
      -l "app.kubernetes.io/component=$component" --timeout=180s; then
      printf 'Bytewax Pods did not stop cleanly: %s\n' "$component" >&2
      restore_quiesced_bytewax_executions
      return 1
    fi
  done
}

apply_application_tier() {
  if ! quiesce_bytewax_executions; then
    return 1
  fi
  if ! delete_legacy_bytewax_statefulsets; then
    restore_quiesced_bytewax_executions
    return 1
  fi
  if ! apply_tier application; then
    restore_quiesced_bytewax_executions
    return 1
  fi

  local workload
  for workload in \
    stream2pretrain-processor-fetcher \
    stream2pretrain-processor-curate \
    stream2pretrain-processor-iceberg-writer \
    stream2pretrain-foundry-worker
  do
    if ! kubectl -n stream2pretrain get "statefulset/$workload" >/dev/null 2>&1; then
      continue
    fi
    kubectl -n stream2pretrain rollout status \
      "statefulset/$workload" --timeout=300s
  done
  BYTEWAX_QUIESCED_RESOURCES=()
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
  if [[ "$CORE_ONLY" != "1" ]]; then
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
  for secret in polaris-minio; do
    if ! kubectl get secret -n polaris "$secret" >/dev/null 2>&1; then
      printf 'Missing required Secret: polaris/%s\n' "$secret" >&2
      missing=1
    fi
  done
  return "$missing"
}

ensure_polaris_persistence_identity() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  kubectl create namespace polaris --dry-run=client -o yaml | kubectl apply -f -
  if ! kubectl -n polaris get secret polaris-persistence >/dev/null 2>&1; then
    local password
    password="$(openssl rand -hex 32)"
    kubectl -n polaris create secret generic polaris-persistence \
      --type=kubernetes.io/basic-auth \
      --from-literal=username=polaris \
      --from-literal=password="$password" \
      --from-literal=jdbcUrl='jdbc:postgresql://polaris-postgres.polaris.svc.cluster.local:5432/polaris'
    unset password
  fi
  kubectl -n polaris patch secret polaris-persistence \
    --type=merge \
    -p '{"type":"kubernetes.io/basic-auth"}' >/dev/null
  local key
  for key in username password jdbcUrl; do
    if ! kubectl -n polaris get secret polaris-persistence \
      -o "go-template={{ index .data \"$key\" }}" 2>/dev/null \
      | grep -q .; then
      printf 'Missing required Secret key: polaris/polaris-persistence/%s\n' "$key" >&2
      return 1
    fi
  done
  local application_credential
  application_credential="$(
    kubectl -n stream2pretrain get secret stream2pretrain-polaris \
      -o jsonpath='{.data.credential}' | base64 --decode
  )"
  local bootstrap_client bootstrap_secret
  IFS=':' read -r bootstrap_client bootstrap_secret <<< "$application_credential"
  if [[ -z "$bootstrap_client" || -z "$bootstrap_secret" ]]; then
    printf 'Polaris application credential must be client:secret\n' >&2
    return 1
  fi
  kubectl -n polaris create secret generic polaris-root-identity \
    --from-literal=credentials="POLARIS,$bootstrap_client,$bootstrap_secret" \
    --dry-run=client -o yaml | kubectl apply -f -
  unset bootstrap_client bootstrap_secret
  unset application_credential
}

ensure_coordination_identity() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  local username
  local password
  local encoded_username
  local encoded_password
  local coordination_url
  username="$(
    kubectl -n polaris get secret polaris-persistence \
      -o jsonpath='{.data.username}' | base64 --decode
  )"
  password="$(
    kubectl -n polaris get secret polaris-persistence \
      -o jsonpath='{.data.password}' | base64 --decode
  )"
  if [[ -z "$username" || -z "$password" ]]; then
    printf 'Polaris persistence credentials are required for worker coordination.\n' >&2
    return 1
  fi
  encoded_username="$(jq -rn --arg value "$username" '$value | @uri')"
  encoded_password="$(jq -rn --arg value "$password" '$value | @uri')"
  coordination_url="postgresql://${encoded_username}:${encoded_password}@polaris-postgres.polaris.svc.cluster.local:5432/stream2pretrain"
  kubectl -n stream2pretrain create secret generic stream2pretrain-coordination \
    --from-literal=url="$coordination_url" \
    --dry-run=client -o yaml | kubectl apply -f -
  unset username password encoded_username encoded_password coordination_url
}

adopt_polaris_release_resources() {
  export KUBECONFIG="$KUBECONFIG_PATH"
  local resource
  for resource in \
    serviceaccount/polaris \
    configmap/polaris \
    service/polaris-mgmt \
    service/polaris \
    deployment/polaris; do
    if kubectl -n polaris get "$resource" >/dev/null 2>&1; then
      kubectl -n polaris label "$resource" \
        app.kubernetes.io/managed-by=Helm --overwrite
      kubectl -n polaris annotate "$resource" \
        meta.helm.sh/release-name=polaris \
        meta.helm.sh/release-namespace=polaris --overwrite
    fi
  done
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
  local replication_factor="${S2P_TOPIC_REPLICATION_FACTOR:-3}"
  local core_partitions="${S2P_CORE_TOPIC_PARTITIONS:-4}"
  if ! [[ "$replication_factor" =~ ^[1-9][0-9]*$ ]]; then
    printf 'S2P_TOPIC_REPLICATION_FACTOR must be a positive integer\n' >&2
    return 1
  fi
  if ! [[ "$core_partitions" =~ ^[1-9][0-9]*$ ]]; then
    printf 'S2P_CORE_TOPIC_PARTITIONS must be a positive integer\n' >&2
    return 1
  fi
  local topic
  for topic in \
    arxiv.discovery \
    raw.fetched raw.smoke \
    docs.normalized docs.normalized.smoke \
    docs.curated docs.curated.smoke \
    curation.decisions curation.decisions.smoke \
    license.admissions license.admissions.smoke \
    foundry.jobs foundry.events foundry.artifacts; do
    if ! topic_exists "$topic"; then
      local retention_ms=604800000
      local partitions=1
      [[ "$topic" == *.smoke ]] && retention_ms=86400000
      case "$topic" in
        arxiv.discovery | raw.fetched | raw.smoke | \
          docs.normalized | docs.normalized.smoke | \
          docs.curated | docs.curated.smoke | \
          curation.decisions | curation.decisions.smoke | \
          license.admissions | license.admissions.smoke)
          partitions="$core_partitions"
          ;;
      esac
      kubectl -n redpanda exec redpanda-0 -c redpanda -- \
        rpk topic create "$topic" \
          --partitions "$partitions" \
          --replicas "$replication_factor" \
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
    arxiv.discovery \
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
  storage)
    validate
    require_minio_data_migration
    apply_tier storage
    ;;
  edge)
    validate
    apply_edge
    ;;
  catalog)
    validate
    require_polaris_postgres_migration
    required_catalog_secret
    ensure_polaris_persistence_identity
    adopt_polaris_release_resources
    apply_tier catalog
    ;;
  topics)
    validate
    ensure_topics
    ;;
  application)
    validate
    validate_bytewax_checkpoint_storage
    if [[ "$CORE_ONLY" != "1" ]]; then
      ensure_foundry_signing_identity
    fi
    required_secrets
    required_application_services
    required_topics
    ensure_coordination_identity
    apply_application_tier
    bootstrap_polaris
    ;;
  verify)
    validate
    verify
    ;;
  *)
    printf 'Usage: %s {validate|plan|cluster|platform|storage|edge|catalog|topics|application|verify}\n' "$0" >&2
    exit 2
    ;;
esac
