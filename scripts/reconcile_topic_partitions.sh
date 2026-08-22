#!/usr/bin/env bash
# Reconcile the four core stream topics to a minimum partition count.

set -euo pipefail

target="${S2P_CORE_TOPIC_PARTITIONS:-4}"
max_message_bytes="${S2P_KAFKA_MESSAGE_MAX_BYTES:-2097152}"
if ! [[ "$target" =~ ^[1-9][0-9]*$ ]]; then
  echo "S2P_CORE_TOPIC_PARTITIONS must be a positive integer" >&2
  exit 1
fi
if ! [[ "$max_message_bytes" =~ ^[1-9][0-9]*$ ]]; then
  echo "S2P_KAFKA_MESSAGE_MAX_BYTES must be a positive integer" >&2
  exit 1
fi

topics=(raw.fetched docs.normalized docs.curated curation.decisions)
for topic in "${topics[@]}"; do
  current="$(
    kubectl -n redpanda exec statefulset/redpanda -- \
      rpk topic describe "$topic" -p \
      | awk 'NR > 1 && $1 ~ /^[0-9]+$/ { count++ } END { print count + 0 }'
  )"
  if [[ "$current" -eq 0 ]]; then
    echo "Core topic is missing or has no partitions: $topic" >&2
    exit 1
  fi
  if [[ "$current" -lt "$target" ]]; then
    add=$((target - current))
    echo "Expanding $topic from $current to $target partitions"
    kubectl -n redpanda exec statefulset/redpanda -- \
      rpk topic add-partitions "$topic" --num "$add"
  else
    echo "$topic already has $current partitions"
  fi
  kubectl -n redpanda exec statefulset/redpanda -- \
    rpk topic alter-config "$topic" --set "max.message.bytes=$max_message_bytes"
done
