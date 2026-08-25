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
smoke_topics=(
  "$smoke_topic"
  "$smoke_normalized_topic"
  "$smoke_curated_topic"
  "$smoke_decisions_topic"
  "$smoke_license_admissions_topic"
)
core_topics=(
  raw.fetched
  "$github_release_jobs_topic"
  docs.normalized
  docs.curated
  curation.decisions
)
topics=("${core_topics[@]}" "${smoke_topics[@]}")
configured_core_topics=("$license_admissions_topic" "${core_topics[@]}")

# One inventory request replaces a separate Kubernetes exec for every topic.
topic_inventory="$(
  kubectl_retry -n redpanda exec statefulset/redpanda -- rpk topic list
)"
license_partitions="$(
  awk -v topic="$license_admissions_topic" \
    'NR > 1 && $1 == topic {print $2; exit}' <<<"$topic_inventory"
)"
if ! [[ "$license_partitions" =~ ^[1-9][0-9]*$ ]]; then
  echo "Licence admission topic is missing or has no partitions: $license_admissions_topic" >&2
  exit 1
fi
missing_core=()
missing_smoke=()
for managed_topic in "$github_release_jobs_topic" "${smoke_topics[@]}"; do
  if ! awk -v topic="$managed_topic" 'NR > 1 && $1 == topic {found=1} END {exit !found}' \
    <<<"$topic_inventory"; then
    if [[ "$managed_topic" == "$github_release_jobs_topic" ]]; then
      missing_core+=("$managed_topic")
    else
      missing_smoke+=("$managed_topic")
    fi
  fi
done
if [[ "${#missing_core[@]}" -gt 0 ]]; then
  echo "Creating managed core topics: ${missing_core[*]}"
  kubectl_retry -n redpanda exec statefulset/redpanda -- \
    rpk topic create "${missing_core[@]}" \
      --partitions "$target" \
      --replicas 1 \
      --topic-config "retention.ms=$core_retention_ms" \
      --topic-config cleanup.policy=delete \
      --topic-config "max.message.bytes=$max_message_bytes"
fi
if [[ "${#missing_smoke[@]}" -gt 0 ]]; then
  echo "Creating managed smoke topics: ${missing_smoke[*]}"
  kubectl_retry -n redpanda exec statefulset/redpanda -- \
    rpk topic create "${missing_smoke[@]}" \
      --partitions "$target" \
      --replicas 1 \
      --topic-config "retention.ms=$smoke_retention_ms" \
      --topic-config cleanup.policy=delete \
      --topic-config "max.message.bytes=$max_message_bytes"
fi

# Refresh once after any creates. rpk topic list reports the partition count,
# so no per-topic describe sessions are needed.
topic_inventory="$(
  kubectl_retry -n redpanda exec statefulset/redpanda -- rpk topic list
)"
for topic in "${topics[@]}"; do
  current="$(
    awk -v topic="$topic" 'NR > 1 && $1 == topic {print $2; exit}' \
      <<<"$topic_inventory"
  )"
  if ! [[ "$current" =~ ^[1-9][0-9]*$ ]]; then
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
done

# Redpanda supports altering multiple topics in one command. Keep the two
# retention classes separate, while setting all three properties atomically
# per class.
kubectl_retry -n redpanda exec statefulset/redpanda -- \
  rpk topic alter-config "${configured_core_topics[@]}" \
    --set "max.message.bytes=$max_message_bytes" \
    --set "retention.ms=$core_retention_ms" \
    --set cleanup.policy=delete
kubectl_retry -n redpanda exec statefulset/redpanda -- \
  rpk topic alter-config "${smoke_topics[@]}" \
    --set "max.message.bytes=$max_message_bytes" \
    --set "retention.ms=$smoke_retention_ms" \
    --set cleanup.policy=delete
