#!/usr/bin/env bash
# Reconcile the core streams and short-lived deployment canary topic.

set -euo pipefail

kubectl_retry() {
  local attempt=1
  local max_attempts="${S2P_KUBECTL_MAX_ATTEMPTS:-6}"
  local delay_seconds="${S2P_KUBECTL_RETRY_DELAY_SECONDS:-2}"
  while true; do
    if kubectl --request-timeout=30s "$@"; then
      return 0
    fi
    if [[ "$attempt" -ge "$max_attempts" ]]; then
      echo "kubectl failed after $attempt attempts: $*" >&2
      return 1
    fi
    echo "kubectl attempt $attempt failed; retrying: $*" >&2
    sleep "$delay_seconds"
    attempt=$((attempt + 1))
  done
}

target="${S2P_CORE_TOPIC_PARTITIONS:-4}"
max_message_bytes="${S2P_KAFKA_MESSAGE_MAX_BYTES:-2097152}"
core_retention_ms="${S2P_CORE_TOPIC_RETENTION_MS:-604800000}"
smoke_retention_ms="${S2P_SMOKE_TOPIC_RETENTION_MS:-86400000}"
if ! [[ "$target" =~ ^[1-9][0-9]*$ ]]; then
  echo "S2P_CORE_TOPIC_PARTITIONS must be a positive integer" >&2
  exit 1
fi
if ! [[ "$max_message_bytes" =~ ^[1-9][0-9]*$ ]]; then
  echo "S2P_KAFKA_MESSAGE_MAX_BYTES must be a positive integer" >&2
  exit 1
fi
if ! [[ "$core_retention_ms" =~ ^[1-9][0-9]*$ ]]; then
  echo "S2P_CORE_TOPIC_RETENTION_MS must be a positive integer" >&2
  exit 1
fi
if ! [[ "$smoke_retention_ms" =~ ^[1-9][0-9]*$ ]]; then
  echo "S2P_SMOKE_TOPIC_RETENTION_MS must be a positive integer" >&2
  exit 1
fi

smoke_topic="${S2P_SMOKE_RAW_TOPIC:-raw.smoke}"
smoke_normalized_topic="${S2P_SMOKE_NORMALIZED_TOPIC:-docs.normalized.smoke}"
smoke_curated_topic="${S2P_SMOKE_CURATED_TOPIC:-docs.curated.smoke}"
smoke_decisions_topic="${S2P_SMOKE_DECISIONS_TOPIC:-curation.decisions.smoke}"
smoke_license_admissions_topic="${S2P_SMOKE_LICENSE_ADMISSIONS_TOPIC:-license.admissions.smoke}"
license_admissions_topic="${S2P_LICENSE_ADMISSIONS_TOPIC:-license.admissions}"
github_release_jobs_topic="${S2P_GITHUB_RELEASE_JOBS_TOPIC:-github.release.jobs}"
for managed_topic in \
  "$smoke_topic" \
  "$smoke_normalized_topic" \
  "$smoke_curated_topic" \
  "$smoke_decisions_topic" \
  "$smoke_license_admissions_topic" \
  "$github_release_jobs_topic"; do
  if ! kubectl_retry -n redpanda exec statefulset/redpanda -- rpk topic list \
    | awk 'NR > 1 {print $1}' \
    | grep -qx "$managed_topic"; then
    retention_ms="$core_retention_ms"
    if [[ "$managed_topic" == "$smoke_topic" \
       || "$managed_topic" == "$smoke_normalized_topic" \
       || "$managed_topic" == "$smoke_curated_topic" \
       || "$managed_topic" == "$smoke_decisions_topic" \
       || "$managed_topic" == "$smoke_license_admissions_topic" ]]; then
      retention_ms="$smoke_retention_ms"
    fi
    echo "Creating managed topic $managed_topic"
    kubectl_retry -n redpanda exec statefulset/redpanda -- \
      rpk topic create "$managed_topic" \
        --partitions "$target" \
        --replicas 1 \
        --topic-config "retention.ms=$retention_ms" \
        --topic-config cleanup.policy=delete \
        --topic-config "max.message.bytes=$max_message_bytes"
  fi
done

license_partitions="$(
  kubectl_retry -n redpanda exec statefulset/redpanda -- \
    rpk topic describe "$license_admissions_topic" -p \
    | awk 'NR > 1 && $1 ~ /^[0-9]+$/ { count++ } END { print count + 0 }'
)"
if [[ "$license_partitions" -eq 0 ]]; then
  echo "Licence admission topic is missing or has no partitions: $license_admissions_topic" >&2
  exit 1
fi
kubectl_retry -n redpanda exec statefulset/redpanda -- \
  rpk topic alter-config "$license_admissions_topic" \
    --set "max.message.bytes=$max_message_bytes"
kubectl_retry -n redpanda exec statefulset/redpanda -- \
  rpk topic alter-config "$license_admissions_topic" \
    --set "retention.ms=$core_retention_ms"
kubectl_retry -n redpanda exec statefulset/redpanda -- \
  rpk topic alter-config "$license_admissions_topic" --set cleanup.policy=delete

topics=(
  raw.fetched
  "$smoke_topic"
  "$github_release_jobs_topic"
  docs.normalized
  "$smoke_normalized_topic"
  docs.curated
  "$smoke_curated_topic"
  curation.decisions
  "$smoke_decisions_topic"
  "$smoke_license_admissions_topic"
)
for topic in "${topics[@]}"; do
  current="$(
    kubectl_retry -n redpanda exec statefulset/redpanda -- \
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
    kubectl_retry -n redpanda exec statefulset/redpanda -- \
      rpk topic add-partitions "$topic" --num "$add"
  else
    echo "$topic already has $current partitions"
  fi
  kubectl_retry -n redpanda exec statefulset/redpanda -- \
    rpk topic alter-config "$topic" --set "max.message.bytes=$max_message_bytes"
  if [[ "$topic" == "$smoke_topic" \
     || "$topic" == "$smoke_normalized_topic" \
     || "$topic" == "$smoke_curated_topic" \
     || "$topic" == "$smoke_decisions_topic" \
     || "$topic" == "$smoke_license_admissions_topic" ]]; then
    kubectl_retry -n redpanda exec statefulset/redpanda -- \
      rpk topic alter-config "$topic" --set "retention.ms=$smoke_retention_ms"
  else
    kubectl_retry -n redpanda exec statefulset/redpanda -- \
      rpk topic alter-config "$topic" --set "retention.ms=$core_retention_ms"
  fi
  kubectl_retry -n redpanda exec statefulset/redpanda -- \
    rpk topic alter-config "$topic" --set cleanup.policy=delete
done
