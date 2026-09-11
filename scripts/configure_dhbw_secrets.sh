#!/usr/bin/env bash
set -euo pipefail

: "${MINIO_ACCESS_KEY:?Set MINIO_ACCESS_KEY}"
: "${MINIO_SECRET_KEY:?Set MINIO_SECRET_KEY}"
: "${POLARIS_CREDENTIAL:?Set POLARIS_CREDENTIAL to client-id:client-secret}"
: "${POLARIS_SCOPE:?Set POLARIS_SCOPE}"
: "${HF_TOKEN:?Set HF_TOKEN}"
CORE_ONLY="${S2P_CORE_ONLY:-0}"
if [[ "$CORE_ONLY" != "1" ]]; then
  : "${HETZNER_INFERENCE_API_KEY:?Set HETZNER_INFERENCE_API_KEY}"
  : "${FOUNDRY_CONTROL_TOKEN:?Set FOUNDRY_CONTROL_TOKEN}"
fi

for namespace in minio polaris stream2pretrain; do
  kubectl create namespace "$namespace" --dry-run=client -o yaml | kubectl apply -f -
done

kubectl -n minio create secret generic minio-root \
  --from-literal=accessKey="$MINIO_ACCESS_KEY" \
  --from-literal=secretKey="$MINIO_SECRET_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n polaris create secret generic polaris-minio \
  --from-literal=accessKey="$MINIO_ACCESS_KEY" \
  --from-literal=secretKey="$MINIO_SECRET_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n stream2pretrain create secret generic stream2pretrain-minio \
  --from-literal=accessKey="$MINIO_ACCESS_KEY" \
  --from-literal=secretKey="$MINIO_SECRET_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n stream2pretrain create secret generic stream2pretrain-polaris \
  --from-literal=credential="$POLARIS_CREDENTIAL" \
  --from-literal=scope="$POLARIS_SCOPE" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n stream2pretrain create secret generic stream2pretrain-hf \
  --from-literal=token="$HF_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -

if [[ "$CORE_ONLY" != "1" ]]; then
  kubectl -n stream2pretrain create secret generic stream2pretrain-foundry-providers \
    --from-literal=HETZNER_INFERENCE_API_KEY="$HETZNER_INFERENCE_API_KEY" \
    --from-literal=controlToken="$FOUNDRY_CONTROL_TOKEN" \
    --dry-run=client -o yaml | kubectl apply -f -
fi

printf 'Configured required Kubernetes Secrets without writing credential files.\n'
