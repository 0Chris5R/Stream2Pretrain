#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KUBECONFIG_PATH="${KUBECONFIG:-$ROOT_DIR/infra/kubeconfig-stream2pretrain.yaml}"
OPENRC_PATH="${OPENRC_PATH:-/Users/I749974/DHBW/cloud/app-cred-Julian-openrc.sh}"

RUN_TERRAFORM="${RUN_TERRAFORM:-1}"
RUN_ANSIBLE="${RUN_ANSIBLE:-1}"
RUN_PLATFORM="${RUN_PLATFORM:-1}"
RUN_STORAGE="${RUN_STORAGE:-1}"

MINIO_ROOT_USER="${MINIO_ROOT_USER:-stream2pretrain}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-stream2pretrain-dev-key}"
MINIO_IMAGE="${MINIO_IMAGE:-quay.io/minio/minio@sha256:a1a8bd4ac40ad7881a245bab97323e18f971e4d4cba2c2007ec1bedd21cbaba2}"
MINIO_MC_IMAGE="${MINIO_MC_IMAGE:-quay.io/minio/mc@sha256:eb4ea9884b77704230e2423e9004d2fa738dc272876b9cc41a297d29443b8780}"

log() {
  printf '[setup-dhbw-demo] %s\n' "$*"
}

require_tool() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Missing required tool: %s\n' "$1" >&2
    exit 1
  fi
}

require_tools() {
  require_tool terraform
  require_tool ansible-playbook
  require_tool ansible-galaxy
  require_tool helm
  require_tool kubectl
  require_tool jq
}

run_terraform() {
  if [[ "$RUN_TERRAFORM" != "1" ]]; then
    log "Skipping Terraform"
    return
  fi

  if [[ ! -f "$ROOT_DIR/infra/terraform/terraform.tfvars" ]]; then
    printf 'Missing infra/terraform/terraform.tfvars. Copy terraform.tfvars.example first.\n' >&2
    exit 1
  fi

  if [[ -f "$OPENRC_PATH" ]]; then
    log "Loading OpenStack credentials from $OPENRC_PATH"
    set +u
    # shellcheck disable=SC1090
    source "$OPENRC_PATH"
    set -u
  else
    log "OPENRC_PATH not found; assuming OpenStack env vars are already set"
  fi

  log "Provisioning DHBWCloud VMs with Terraform"
  terraform -chdir="$ROOT_DIR/infra/terraform" init
  terraform -chdir="$ROOT_DIR/infra/terraform" apply -auto-approve
}

run_ansible() {
  if [[ "$RUN_ANSIBLE" != "1" ]]; then
    log "Skipping Ansible"
    return
  fi

  log "Installing Ansible role dependencies"
  ansible-galaxy install -r "$ROOT_DIR/infra/ansible/requirements.yml" --force

  log "Installing k3s on the Terraform VMs"
  ansible-playbook \
    -i "$ROOT_DIR/infra/terraform/generated-inventory.yml" \
    "$ROOT_DIR/infra/ansible/deploy.yaml"
}

helm_repo_add() {
  helm repo add "$1" "$2" --force-update >/dev/null
}

install_platform() {
  if [[ "$RUN_PLATFORM" != "1" ]]; then
    log "Skipping platform Helm releases"
    return
  fi

  export KUBECONFIG="$KUBECONFIG_PATH"

  log "Adding Helm repositories"
  helm_repo_add jetstack https://charts.jetstack.io
  helm_repo_add traefik https://traefik.github.io/charts
  helm_repo_add prometheus-community https://prometheus-community.github.io/helm-charts
  helm_repo_add grafana https://grafana.github.io/helm-charts
  helm_repo_add kedacore https://kedacore.github.io/charts
  helm_repo_add gatekeeper https://open-policy-agent.github.io/gatekeeper/charts
  helm_repo_add redpanda https://charts.redpanda.com
  helm repo update

  log "Installing kube-prometheus-stack"
  helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
    --namespace monitoring --create-namespace --version 65.1.0 \
    -f "$ROOT_DIR/infra/helmfile-values/kube-prometheus-stack.dev.yaml" \
    --wait --timeout 15m

  log "Installing cert-manager"
  helm upgrade --install cert-manager jetstack/cert-manager \
    --namespace cert-manager --create-namespace --version v1.15.3 \
    -f "$ROOT_DIR/infra/helmfile-values/cert-manager.dev.yaml" \
    --wait --timeout 10m

  log "Installing Traefik"
  helm upgrade --install traefik traefik/traefik \
    --namespace traefik --create-namespace --version 30.1.0 \
    -f "$ROOT_DIR/infra/helmfile-values/traefik.dev.yaml" \
    --wait --timeout 10m

  log "Installing KEDA"
  helm upgrade --install keda kedacore/keda \
    --namespace keda --create-namespace --version 2.15.1 \
    -f "$ROOT_DIR/infra/helmfile-values/keda.dev.yaml" \
    --wait --timeout 10m

  kubectl create namespace stream2pretrain --dry-run=client -o yaml | kubectl apply -f -

  log "Installing Gatekeeper"
  helm upgrade --install gatekeeper gatekeeper/gatekeeper \
    --namespace gatekeeper-system --create-namespace --version 3.17.0 \
    -f "$ROOT_DIR/infra/helmfile-values/gatekeeper.dev.yaml" \
    --wait --timeout 10m

  log "Installing Redpanda without waiting for IPv4-only probes"
  helm upgrade --install redpanda redpanda/redpanda \
    --namespace redpanda --create-namespace --version 5.9.5 \
    -f "$ROOT_DIR/infra/helmfile-values/redpanda.dev.yaml" \
    --wait=hookOnly --force-conflicts --timeout 10m

  patch_redpanda_for_ipv6
  seed_redpanda_topics
}

patch_redpanda_for_ipv6() {
  export KUBECONFIG="$KUBECONFIG_PATH"

  log "Patching Redpanda listener bind addresses for IPv6 pod networking"
  for _ in {1..60}; do
    if kubectl -n redpanda get configmap redpanda >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done

  local patched patch
  patched="$(kubectl get configmap -n redpanda redpanda -o json \
    | jq -r '.data["redpanda.yaml"] | gsub("address: 0.0.0.0"; "address: \"::\"")')"
  patch="$(jq -n --arg data "$patched" '{data:{"redpanda.yaml":$data}}')"
  kubectl patch configmap -n redpanda redpanda --type=merge -p "$patch" >/dev/null

  kubectl rollout restart statefulset/redpanda -n redpanda >/dev/null
  kubectl rollout status statefulset/redpanda -n redpanda --timeout=180s
  kubectl delete deployment -n redpanda redpanda-console --ignore-not-found >/dev/null
}

topic_exists() {
  local topic="$1"
  kubectl exec -n redpanda redpanda-0 -c redpanda -- rpk topic list \
    | awk 'NR > 1 {print $1}' \
    | grep -qx "$topic"
}

create_topic_if_missing() {
  local topic="$1"
  if topic_exists "$topic"; then
    log "Topic already exists: $topic"
    return
  fi

  kubectl exec -n redpanda redpanda-0 -c redpanda -- \
    rpk topic create "$topic" -p 1 -r 1 \
      -c retention.ms=604800000 \
      -c cleanup.policy=delete
}

seed_redpanda_topics() {
  export KUBECONFIG="$KUBECONFIG_PATH"

  log "Waiting for Redpanda health"
  kubectl exec -n redpanda redpanda-0 -c redpanda -- rpk cluster health

  log "Creating Stream2Pretrain Kafka topics"
  create_topic_if_missing _schemas
  create_topic_if_missing raw.fetched
  create_topic_if_missing docs.normalized
  create_topic_if_missing docs.curated
  create_topic_if_missing decon.attest
}

install_minio_standalone() {
  if [[ "$RUN_STORAGE" != "1" ]]; then
    log "Skipping MinIO standalone storage"
    return
  fi

  export KUBECONFIG="$KUBECONFIG_PATH"

  log "Removing failed Helm-managed MinIO release if present"
  if helm status minio -n minio >/dev/null 2>&1; then
    helm uninstall minio -n minio || true
  fi

  log "Installing standalone MinIO demo storage"
  kubectl apply -f - <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: minio
---
apiVersion: v1
kind: Secret
metadata:
  name: minio-root
  namespace: minio
type: Opaque
stringData:
  MINIO_ROOT_USER: ${MINIO_ROOT_USER}
  MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: minio-data
  namespace: minio
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: local-path
  resources:
    requests:
      storage: 10Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: minio
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: minio
  template:
    metadata:
      labels:
        app.kubernetes.io/name: minio
    spec:
      containers:
        - name: minio
          image: ${MINIO_IMAGE}
          args: ["server", "/data", "--console-address", ":9001"]
          envFrom:
            - secretRef:
                name: minio-root
          ports:
            - name: api
              containerPort: 9000
            - name: console
              containerPort: 9001
          readinessProbe:
            httpGet:
              path: /minio/health/ready
              port: api
            initialDelaySeconds: 5
            periodSeconds: 5
          livenessProbe:
            httpGet:
              path: /minio/health/live
              port: api
            initialDelaySeconds: 10
            periodSeconds: 10
          resources:
            requests:
              cpu: 100m
              memory: 256Mi
            limits:
              cpu: 500m
              memory: 1Gi
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: minio-data
---
apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: minio
spec:
  selector:
    app.kubernetes.io/name: minio
  ports:
    - name: api
      port: 9000
      targetPort: api
    - name: console
      port: 9001
      targetPort: console
YAML

  kubectl rollout status deployment/minio -n minio --timeout=180s
  create_minio_buckets
  create_stream2pretrain_stub_secrets
}

create_minio_buckets() {
  export KUBECONFIG="$KUBECONFIG_PATH"

  log "Creating MinIO buckets"
  kubectl delete job -n minio minio-create-buckets --ignore-not-found >/dev/null
  kubectl apply -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: minio-create-buckets
  namespace: minio
spec:
  backoffLimit: 4
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: mc
          image: ${MINIO_MC_IMAGE}
          envFrom:
            - secretRef:
                name: minio-root
          command: ["/bin/sh", "-c"]
          args:
            - |
              set -eu
              until mc alias set local http://minio.minio.svc.cluster.local:9000 "\$MINIO_ROOT_USER" "\$MINIO_ROOT_PASSWORD"; do
                sleep 2
              done
              for bucket in bronze silver gold decon-attestations loki-chunks loki-ruler loki-admin tempo polaris-warehouse s2p-bronze s2p-silver s2p-gold s2p-decon; do
                mc mb --ignore-existing "local/\$bucket"
              done
YAML
  kubectl wait -n minio --for=condition=Complete job/minio-create-buckets --timeout=180s
}

create_stream2pretrain_stub_secrets() {
  export KUBECONFIG="$KUBECONFIG_PATH"

  log "Creating Stream2Pretrain demo secrets"
  kubectl create namespace stream2pretrain --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl -n stream2pretrain create secret generic stream2pretrain-minio \
    --from-literal=accessKey="$MINIO_ROOT_USER" \
    --from-literal=secretKey="$MINIO_ROOT_PASSWORD" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl -n stream2pretrain create secret generic stream2pretrain-minio-secret \
    --from-literal=accessKey="$MINIO_ROOT_USER" \
    --from-literal=secretKey="$MINIO_ROOT_PASSWORD" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl -n stream2pretrain create secret generic stream2pretrain-github \
    --from-literal=token="${GITHUB_TOKEN:-demo-token}" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl -n stream2pretrain create secret generic stream2pretrain-hf \
    --from-literal=token="${HF_TOKEN:-demo-token}" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl -n stream2pretrain create secret generic stream2pretrain-decon-signing \
    --from-literal=ed25519.key="demo-only-signing-key" \
    --from-literal=ed25519.crt="demo-only-signing-cert" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl -n stream2pretrain create configmap stream2pretrain-decon-benchmarks \
    --from-literal=corpus.json='{"documents":[]}' \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
}

verify_cluster() {
  export KUBECONFIG="$KUBECONFIG_PATH"

  log "Cluster summary"
  kubectl get nodes -o wide
  helm list -A
  kubectl get pods -A
  kubectl exec -n redpanda redpanda-0 -c redpanda -- rpk topic list
}

main() {
  require_tools
  run_terraform
  run_ansible
  install_platform
  install_minio_standalone
  verify_cluster
  log "Done. KUBECONFIG=$KUBECONFIG_PATH"
}

main "$@"
