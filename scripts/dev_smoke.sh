#!/usr/bin/env bash
# Stream2Pretrain - one-shot dev smoke.
#
# Verifies the laptop dev stack works end-to-end:
#   1. docker compose up -d (Redpanda + MinIO)
#   2. seed_topics.sh  (create the four core topics)
#   3. probe Redpanda admin API for cluster health
#   4. probe MinIO health
#   5. (best-effort) start the FastAPI submit API in the background, POST a
#      known URL, and assert a bronze record lands on `raw.fetched`.
#
# This is the "30 seconds to confidence" check developers run after `git pull`.
#
# Usage:
#   bash scripts/dev_smoke.sh
#   SKIP_API=1 bash scripts/dev_smoke.sh    # don't spin up the API
#
# Exit codes:
#   0  all checks passed
#   1  a check failed; logs are above
#   2  prerequisites missing (docker, uv)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

ok()    { printf '  \033[32mOK\033[0m  %s\n' "$1"; }
fail()  { printf '  \033[31mFAIL\033[0m %s\n' "$1"; exit 1; }
info()  { printf '\n>>> %s\n' "$1"; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: required command not found: $1" >&2
    exit 2
  }
}

require_cmd docker
require_cmd uv

# 1. dev stack
info "starting dev stack (docker compose -f docker-compose.dev.yml up -d)"
docker compose -f docker-compose.dev.yml up -d >/dev/null
ok "compose up"

# 2. wait for Redpanda admin API
info "waiting for Redpanda admin API"
for i in $(seq 1 30); do
  if curl -fsS "http://localhost:9644/v1/status/ready" >/dev/null 2>&1; then
    ok "redpanda admin reachable"
    break
  fi
  sleep 1
  if [[ "$i" == "30" ]]; then fail "redpanda admin not reachable after 30s"; fi
done

# 3. wait for MinIO health
info "waiting for MinIO"
for i in $(seq 1 30); do
  if curl -fsS "http://localhost:9000/minio/health/live" >/dev/null 2>&1; then
    ok "minio live"
    break
  fi
  sleep 1
  if [[ "$i" == "30" ]]; then fail "minio not reachable after 30s"; fi
done

# 4. seed topics
info "seeding topics"
bash "${REPO_ROOT}/scripts/seed_topics.sh"
ok "topics created (or already existed)"

# 5. optional API check
if [[ "${SKIP_API:-0}" == "1" ]]; then
  info "skipping submit API check (SKIP_API=1)"
  echo
  echo "dev stack is healthy."
  exit 0
fi

info "starting submit API (background)"
LOG_DIR="$(mktemp -d -t s2p-smoke-XXXXXX)"
API_LOG="${LOG_DIR}/submit_api.log"

# Use the dev feed catalogue and dev MinIO/Redpanda endpoints.
S2P_FEED_CONFIG="${REPO_ROOT}/ingest/feeds.dev.yaml" \
S2P_REDPANDA_BROKERS="localhost:9092" \
S2P_MINIO_ENDPOINT="http://localhost:9000" \
S2P_MINIO_ACCESS_KEY="minioadmin" \
S2P_MINIO_SECRET_KEY="minioadmin" \
S2P_RAW_TOPIC="raw.fetched" \
S2P_LOG_LEVEL="info" \
  uv run uvicorn ingest.submit_api.app:app --host 127.0.0.1 --port 8000 \
  >"${API_LOG}" 2>&1 &
API_PID=$!

cleanup() {
  if kill -0 "${API_PID}" >/dev/null 2>&1; then
    kill "${API_PID}" >/dev/null 2>&1 || true
    wait "${API_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

for i in $(seq 1 30); do
  if curl -fsS "http://localhost:8000/healthz" >/dev/null 2>&1; then
    ok "submit API responsive"
    break
  fi
  sleep 1
  if [[ "$i" == "30" ]]; then
    cat "${API_LOG}" >&2 || true
    fail "submit API never came up; see logs at ${API_LOG}"
  fi
done

info "POST /submit"
SUBMIT_BODY='{"url":"https://export.arxiv.org/abs/2402.00159","source_feed":"manual-submit"}'
RESP="$(curl -fsS -X POST "http://localhost:8000/submit" \
  -H 'Content-Type: application/json' -d "${SUBMIT_BODY}")" || fail "POST /submit failed"
echo "    response: ${RESP}"
DOC_ID="$(printf '%s' "${RESP}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["doc_id"])')"
[[ -n "${DOC_ID}" ]] || fail "no doc_id in response"
ok "submitted ${DOC_ID}"

info "checking raw.fetched for the doc_id (timeout 15s)"
if docker exec s2p-redpanda rpk topic consume raw.fetched --num 50 --offset start \
    --format '%v\n' 2>/dev/null \
    | grep -q "${DOC_ID}"; then
  ok "doc_id appeared on raw.fetched"
else
  fail "doc_id ${DOC_ID} did not appear on raw.fetched"
fi

echo
echo "dev smoke completed. logs: ${API_LOG}"
