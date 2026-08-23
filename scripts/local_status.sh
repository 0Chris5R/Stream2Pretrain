#!/usr/bin/env bash
# Read-only status summary for the Podman/Docker local end-to-end profile.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

if [[ -n "${CONTAINER_ENGINE:-}" ]]; then
  ENGINE="${CONTAINER_ENGINE}"
elif command -v podman >/dev/null 2>&1; then
  ENGINE="podman"
elif command -v docker >/dev/null 2>&1; then
  ENGINE="docker"
else
  echo "error: neither podman nor docker is available" >&2
  exit 2
fi

COMPOSE=("${ENGINE}" compose -f compose.local.yml)

echo "Container state"
"${COMPOSE[@]}" ps

echo
echo "Redpanda topics"
"${COMPOSE[@]}" exec -T redpanda \
  rpk -X brokers=redpanda:29092 topic describe \
  raw.fetched raw.smoke docs.normalized docs.curated curation.decisions license.admissions decon.attest -p

check_url() {
  local label="$1"
  local url="$2"
  if curl -fsS --max-time 5 "${url}" >/dev/null; then
    printf 'OK   %-24s %s\n' "${label}" "${url}"
  else
    printf 'FAIL %-24s %s\n' "${label}" "${url}"
    return 1
  fi
}

echo
echo "HTTP surfaces"
check_url "UI" "http://localhost:3100/api/health"
check_url "MinIO" "http://localhost:9000/minio/health/live"
check_url "Redpanda admin" "http://localhost:9644/v1/status/ready"
check_url "Prometheus" "http://localhost:9091/-/healthy"
check_url "Decon API" "http://localhost:8081/healthz"
check_url "DuckDB API" "http://localhost:8090/healthz"
check_url "Documents API" "http://localhost:8090/documents?limit=1"
check_url "Sources API" "http://localhost:3100/api/sources"
check_url "Foundry API" "http://localhost:8092/healthz"

echo
echo "Useful pages"
echo "  Cockpit:          http://localhost:3100"
echo "  Dashboard:        http://localhost:3100/dashboard"
echo "  Sources:          http://localhost:3100/sources"
echo "  Benchmark safety: http://localhost:3100/decon"
echo "  Datasets/export:  http://localhost:3100/datasets"
echo "  Documents/OCR:    http://localhost:3100/documents"
echo "  Post-training:    http://localhost:3100/post-training"
echo "  Redpanda console: http://localhost:8080"
echo "  MinIO console:    http://localhost:9001  (minioadmin / minioadmin)"
echo "  Prometheus:       http://localhost:9091"
