#!/usr/bin/env bash
set -Eeuo pipefail

namespace="${1:-stream2pretrain}"
secret_name="${2:-stream2pretrain-foundry-signing}"

existing_json="$(kubectl -n "$namespace" get "secret/$secret_name" -o json 2>/dev/null || true)"
if [[ -n "$existing_json" ]]; then
  if ! jq -e '.data["ed25519.key"] | length > 0' <<<"$existing_json" >/dev/null \
    || ! jq -e '.data["ed25519.crt"] | length > 0' <<<"$existing_json" >/dev/null; then
    echo "Existing Foundry signing Secret is missing ed25519.key or ed25519.crt." >&2
    exit 1
  fi
  echo "Foundry signing identity is present."
  exit 0
fi

signing_dir="$(mktemp -d)"
cleanup() {
  rm -f -- "$signing_dir/ed25519.key" "$signing_dir/ed25519.crt"
  rmdir -- "$signing_dir"
}
trap cleanup EXIT
umask 077

openssl genpkey -algorithm Ed25519 -out "$signing_dir/ed25519.key"
openssl req -new -x509 \
  -key "$signing_dir/ed25519.key" \
  -out "$signing_dir/ed25519.crt" \
  -days 3650 \
  -subj "/CN=stream2pretrain-foundry/O=Stream2Pretrain"

kubectl -n "$namespace" create secret generic "$secret_name" \
  --from-file=ed25519.key="$signing_dir/ed25519.key" \
  --from-file=ed25519.crt="$signing_dir/ed25519.crt" \
  --dry-run=client -o yaml \
  | kubectl apply -f -
kubectl -n "$namespace" annotate "secret/$secret_name" \
  stream2pretrain.io/purpose=foundry-artifact-signing \
  --overwrite >/dev/null

echo "Created the persistent in-cluster Foundry signing identity."
