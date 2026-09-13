#!/usr/bin/env bash
set -Eeuo pipefail

namespace="stream2pretrain"
release="stream2pretrain"
state_claim="state-stream2pretrain-foundry-0"
previous_revision="$(helm -n "$namespace" list -o json \
  | jq -r --arg release "$release" 'first(.[] | select(.name == $release) | .revision)')"

kubectl -n "$namespace" get "pvc/$state_claim" -o json \
  | jq -e '.status.phase == "Bound"' >/dev/null

for workload in \
  statefulset/stream2pretrain-foundry-worker \
  statefulset/stream2pretrain-foundry; do
  if kubectl -n "$namespace" get "$workload" >/dev/null 2>&1; then
    kubectl -n "$namespace" scale "$workload" --replicas=0
  fi
done
kubectl -n "$namespace" wait --for=delete pod \
  -l app.kubernetes.io/component=foundry-worker --timeout=180s || true
kubectl -n "$namespace" wait --for=delete pod \
  -l app.kubernetes.io/component=foundry --timeout=180s || true

current_values="$(mktemp)"
restore_values="$(mktemp)"
cleanup() {
  rm -f "$current_values" "$restore_values"
}
trap cleanup EXIT
helm -n "$namespace" get values "$release" -o json > "$current_values"
jq '
  .processor.foundry |= (
    del(.workerReplicas, .workersPerProcess, .apiReplicas)
    | .enabled = true
    | .replicas = 1
    | .state = {storageClass: "", size: "5Gi"}
  )
' "$current_values" > "$restore_values"

if ! helm upgrade "$release" charts/stream2pretrain \
  --namespace "$namespace" \
  --reset-values \
  -f charts/stream2pretrain/values-dev.yaml \
  -f infra/helmfile-values/stream2pretrain.dev.yaml \
  -f "$restore_values" \
  --wait=false; then
  helm -n "$namespace" rollback "$release" "$previous_revision" --wait=false || true
  exit 1
fi

kubectl -n "$namespace" rollout status \
  statefulset/stream2pretrain-foundry --timeout=300s
kubectl -n "$namespace" wait --for=condition=Ready \
  pod/stream2pretrain-foundry-0 --timeout=300s
if kubectl -n "$namespace" get \
  statefulset/stream2pretrain-foundry-worker >/dev/null 2>&1 \
  || kubectl -n "$namespace" get \
  deployment/stream2pretrain-foundry-api >/dev/null 2>&1; then
  echo "Split Foundry resources remain after restoration" >&2
  exit 1
fi
kubectl -n "$namespace" get endpointslice \
  -l kubernetes.io/service-name=stream2pretrain-foundry -o json \
  | jq -e '[.items[].endpoints[]? | select(.conditions.ready != false)] | length > 0' \
    >/dev/null

kubectl -n "$namespace" exec -i statefulset/stream2pretrain-foundry -c api -- \
  python - <<'PY'
import json
import urllib.request

opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
payloads = {}
for name, path in (
    ("health", "/healthz"),
    ("readiness", "/readyz"),
    ("dashboard", "/api/foundry/dashboard"),
    ("jobs", "/api/foundry/jobs?limit=3"),
    ("artifacts", "/api/foundry/artifacts?limit=3"),
):
    with opener.open(f"http://[::1]:8092{path}", timeout=30) as response:
        payloads[name] = json.loads(response.read())

dashboard = payloads["dashboard"]
artifact_total = sum(int(value) for value in dashboard.get("artifacts", {}).values())
if not dashboard.get("recent_jobs") or artifact_total == 0:
    raise SystemExit("Foundry restored without its retained jobs and artifacts")
print(json.dumps(payloads, sort_keys=True))
PY
