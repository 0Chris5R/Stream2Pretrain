#!/usr/bin/env bash
# Stream2Pretrain - create the managed Redpanda topics on the local dev cluster.
#
# Idempotent: rpk returns non-zero on "topic already exists"; we tolerate that.
# Partition counts match schemas/topics.py::dev_topic_configs. Replication is
# configurable so local Docker keeps one replica while the cluster profile can
# use its three-broker replication factor.
#
# Usage:
#   bash scripts/seed_topics.sh                 # talks to localhost:9092
#   RPK_BROKERS=redpanda:9092 bash scripts/seed_topics.sh

set -euo pipefail

BROKERS="${RPK_BROKERS:-localhost:9092}"
TOPIC_REPLICAS="${S2P_TOPIC_REPLICATION_FACTOR:-1}"
RETENTION_MS_DEV=$((7 * 24 * 60 * 60 * 1000))   # 7 days
RETENTION_MS_SMOKE=$((24 * 60 * 60 * 1000))     # 1 day

if ! [[ "$TOPIC_REPLICAS" =~ ^[1-9][0-9]*$ ]]; then
  echo "S2P_TOPIC_REPLICATION_FACTOR must be a positive integer" >&2
  exit 1
fi

# Prefer running rpk inside the dev container if it is up; otherwise fall back
# to a host-installed rpk binary. This avoids forcing every contributor to
# install rpk locally.
if docker ps --format '{{.Names}}' | grep -q '^s2p-redpanda$'; then
  RPK=(docker exec -i s2p-redpanda rpk --brokers "redpanda:9092")
else
  if ! command -v rpk >/dev/null 2>&1; then
    echo "error: rpk not found on PATH and the s2p-redpanda dev container is not running." >&2
    echo "       run 'make dev-up' or install rpk: https://docs.redpanda.com/docs/get-started/rpk-install/" >&2
    exit 1
  fi
  RPK=(rpk --brokers "$BROKERS")
fi

create_topic() {
  local name="$1"
  local partitions="$2"
  local replicas="$3"
  local retention_ms="${4:-$RETENTION_MS_DEV}"

  echo "creating topic ${name} (partitions=${partitions}, replicas=${replicas})"
  if ! "${RPK[@]}" topic create "$name" \
        --partitions "$partitions" \
        --replicas "$replicas" \
        --config "retention.ms=${retention_ms}" \
        --config "cleanup.policy=delete" \
        --config "max.message.bytes=2097152" 2> >(tee /tmp/rpk_err >&2) ; then
    if grep -q 'TOPIC_ALREADY_EXISTS' /tmp/rpk_err 2>/dev/null; then
      echo "  -> already exists, skipping"
    else
      echo "  -> rpk reported an error other than TOPIC_ALREADY_EXISTS" >&2
      exit 1
    fi
  fi
}

create_topic "arxiv.discovery"  4 "$TOPIC_REPLICAS"
create_topic "raw.fetched"      4 "$TOPIC_REPLICAS"
create_topic "raw.smoke"        4 "$TOPIC_REPLICAS" "$RETENTION_MS_SMOKE"
create_topic "docs.normalized"  4 "$TOPIC_REPLICAS"
create_topic "docs.normalized.smoke" 4 "$TOPIC_REPLICAS" "$RETENTION_MS_SMOKE"
create_topic "docs.curated"     4 "$TOPIC_REPLICAS"
create_topic "docs.curated.smoke" 4 "$TOPIC_REPLICAS" "$RETENTION_MS_SMOKE"
create_topic "curation.decisions" 4 "$TOPIC_REPLICAS"
create_topic "curation.decisions.smoke" 4 "$TOPIC_REPLICAS" "$RETENTION_MS_SMOKE"
create_topic "license.admissions" 4 "$TOPIC_REPLICAS"
create_topic "license.admissions.smoke" 4 "$TOPIC_REPLICAS" "$RETENTION_MS_SMOKE"
create_topic "foundry.jobs"     1 "$TOPIC_REPLICAS"
create_topic "foundry.events"   1 "$TOPIC_REPLICAS"
create_topic "foundry.artifacts" 1 "$TOPIC_REPLICAS"

echo
echo "current topics:"
"${RPK[@]}" topic list
