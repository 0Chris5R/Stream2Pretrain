#!/usr/bin/env bash
# Stream2Pretrain - one-shot dev smoke.
#
# Verifies the laptop dev stack works end-to-end:
#   1. docker compose up -d (Redpanda + MinIO)
#   2. seed_topics.sh  (create the four core topics)
#   3. probe Redpanda admin API for cluster health
#   4. probe MinIO health
#   5. (best-effort) drive a single ``arxiv_html_fetcher`` run against a
#      pinned arXiv id and assert a bronze record lands on ``raw.fetched``.
#
# This is the "30 seconds to confidence" check developers run after `git pull`.
#
# Usage:
#   bash scripts/dev_smoke.sh
#   SKIP_FETCH=1 bash scripts/dev_smoke.sh    # don't drive the fetcher
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

# 5. optional fulltext-fetcher check
if [[ "${SKIP_FETCH:-0}" == "1" ]]; then
  info "skipping arxiv_html_fetcher check (SKIP_FETCH=1)"
  echo
  echo "dev stack is healthy."
  exit 0
fi

LOG_DIR="$(mktemp -d -t s2p-smoke-XXXXXX)"
FETCH_LOG="${LOG_DIR}/arxiv_html_fetcher.log"

# A pinned, small, stable arXiv paper that has a native HTML rendering. The
# fetcher walks /html/<id> first and falls back to ar5iv.labs.arxiv.org.
PINNED_ARXIV_ID="2402.00159"
EXPECTED_DOC_ID_PREFIX="sha256:"

info "running arxiv_html_fetcher --once for arXiv:${PINNED_ARXIV_ID}"
S2P_FEED_CONFIG="${REPO_ROOT}/ingest/feeds.dev.yaml" \
REDPANDA_BROKERS="localhost:9092" \
MINIO_ENDPOINT="http://localhost:9000" \
MINIO_ACCESS_KEY="minioadmin" \
MINIO_SECRET_KEY="minioadmin" \
S2P_RAW_TOPIC="raw.fetched" \
LOG_LEVEL="INFO" \
S2P_ARXIV_IDS="${PINNED_ARXIV_ID}" \
  uv run python -m ingest.arxiv_html_fetcher.fetcher --once \
  >"${FETCH_LOG}" 2>&1 || {
    cat "${FETCH_LOG}" >&2 || true
    fail "arxiv_html_fetcher --once exited non-zero; see ${FETCH_LOG}"
  }
ok "arxiv_html_fetcher one-shot completed"

info "checking raw.fetched for the arXiv id (timeout 15s)"
if docker exec s2p-redpanda rpk topic consume raw.fetched --num 50 --offset start \
    --format '%v\n' 2>/dev/null \
    | grep -q "${PINNED_ARXIV_ID}"; then
  ok "arXiv id appeared on raw.fetched"
else
  fail "arXiv id ${PINNED_ARXIV_ID} did not appear on raw.fetched"
fi

echo
echo "dev smoke completed. logs: ${FETCH_LOG}"
