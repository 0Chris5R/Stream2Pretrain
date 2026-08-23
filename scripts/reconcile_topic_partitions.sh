#!/usr/bin/env bash
# Reconcile the core streams and short-lived deployment canary topic.

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

smoke_topic="${S2P_SMOKE_RAW_TOPIC:-raw.smoke}"
smoke_normalized_topic="${S2P_SMOKE_NORMALIZED_TOPIC:-docs.normalized.smoke}"
github_release_jobs_topic="${S2P_GITHUB_RELEASE_JOBS_TOPIC:-github.release.jobs}"
for managed_topic in "$smoke_topic" "$smoke_normalized_topic" "$github_release_jobs_topic"; do
  if ! kubectl -n redpanda exec statefulset/redpanda -- rpk topic list \
    | awk 'NR > 1 {print $1}' \
    | grep -qx "$managed_topic"; then
    retention_ms=604800000
    if [[ "$managed_topic" == "$smoke_topic" || "$managed_topic" == "$smoke_normalized_topic" ]]; then
      retention_ms=86400000
    fi
    echo "Creating managed topic $managed_topic"
    kubectl -n redpanda exec statefulset/redpanda -- \
      rpk topic create "$managed_topic" \
        --partitions "$target" \
        --replicas 1 \
        --topic-config "retention.ms=$retention_ms" \
        --topic-config cleanup.policy=delete \
        --topic-config "max.message.bytes=$max_message_bytes"
  fi
done

topics=(
  raw.fetched
  "$smoke_topic"
  "$github_release_jobs_topic"
  docs.normalized
  "$smoke_normalized_topic"
  docs.curated
  curation.decisions
)
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
  if [[ "$topic" == "$smoke_topic" || "$topic" == "$smoke_normalized_topic" ]]; then
    kubectl -n redpanda exec statefulset/redpanda -- \
      rpk topic alter-config "$topic" --set retention.ms=86400000
  fi
done
