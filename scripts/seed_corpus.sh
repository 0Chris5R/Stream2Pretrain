#!/usr/bin/env bash
# Stream2Pretrain - render and apply the seed-loader Bytewax Job.
#
# Wraps the Helm template at
# charts/stream2pretrain/templates/job-seed-loader.yaml so an operator can
# kick off a one-shot seed ingest without editing values.yaml.
#
# Usage:
#   bash scripts/seed_corpus.sh                                # all 5 components
#   bash scripts/seed_corpus.sh --components=pes2o,stack-edu   # subset
#   bash scripts/seed_corpus.sh --dry-run                      # log only, no produce
#   bash scripts/seed_corpus.sh --max-docs=1000                # smoke run
#   NAMESPACE=stream2pretrain bash scripts/seed_corpus.sh --components=wayback
#
# Environment overrides:
#   NAMESPACE        kube namespace (default: stream2pretrain)
#   RELEASE_NAME     Helm release name (default: stream2pretrain)
#   CHART_DIR        path to the chart (default: charts/stream2pretrain)
#   RENDER_ONLY=1    print the Job manifest instead of applying it

set -euo pipefail

NAMESPACE="${NAMESPACE:-stream2pretrain}"
RELEASE_NAME="${RELEASE_NAME:-stream2pretrain}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART_DIR="${CHART_DIR:-${REPO_ROOT}/charts/stream2pretrain}"
RENDER_ONLY="${RENDER_ONLY:-0}"

COMPONENTS=""
DRY_RUN="0"
MAX_DOCS=""

for arg in "$@"; do
  case "${arg}" in
    --components=*)
      COMPONENTS="${arg#--components=}"
      ;;
    --dry-run)
      DRY_RUN="1"
      ;;
    --max-docs=*)
      MAX_DOCS="${arg#--max-docs=}"
      ;;
    -h|--help)
      sed -n '1,30p' "${BASH_SOURCE[0]}"
      exit 0
      ;;
    *)
      echo "error: unknown argument ${arg}" >&2
      echo "usage: bash scripts/seed_corpus.sh [--components=a,b] [--dry-run] [--max-docs=N]" >&2
      exit 2
      ;;
  esac
done

if [[ "${RENDER_ONLY}" != "1" ]]; then
  command -v kubectl >/dev/null 2>&1 || {
    echo "error: kubectl not found on PATH" >&2
    exit 1
  }
fi
command -v helm >/dev/null 2>&1 || {
  echo "error: helm not found on PATH" >&2
  exit 1
}

if [[ ! -d "${CHART_DIR}" ]]; then
  echo "error: chart directory ${CHART_DIR} does not exist" >&2
  exit 1
fi

# Build the helm --set list lazily so we keep the rendered template
# minimal when no flags are passed.
SET_FLAGS=("--set" "seedLoader.enabled=true")
if [[ -n "${COMPONENTS}" ]]; then
  # Translate comma-separated list to helm array notation.
  IFS=',' read -r -a parts <<<"${COMPONENTS}"
  for i in "${!parts[@]}"; do
    SET_FLAGS+=("--set" "seedLoader.components[${i}]=${parts[${i}]}")
  done
fi
if [[ "${DRY_RUN}" == "1" ]]; then
  SET_FLAGS+=("--set" "seedLoader.dryRun=true")
fi
if [[ -n "${MAX_DOCS}" ]]; then
  SET_FLAGS+=("--set" "seedLoader.maxDocsPerComponent=${MAX_DOCS}")
fi

# Job names are date-stamped so the same script can run repeatedly without
# colliding on a "completed" Job object that kube refuses to overwrite.
TIMESTAMP="$(date -u +%Y%m%d-%H%M%S)"
SET_FLAGS+=("--set" "seedLoader.runId=${TIMESTAMP}")

echo "rendering seed-loader Job (components=${COMPONENTS:-all}, dry_run=${DRY_RUN}, max_docs=${MAX_DOCS:-none}, run_id=${TIMESTAMP})"

MANIFEST="$(helm template "${RELEASE_NAME}" "${CHART_DIR}" \
  --namespace "${NAMESPACE}" \
  --show-only templates/job-seed-loader.yaml \
  "${SET_FLAGS[@]}")"

if [[ "${RENDER_ONLY}" == "1" ]]; then
  echo "${MANIFEST}"
  exit 0
fi

echo "${MANIFEST}" | kubectl apply -n "${NAMESPACE}" -f -

echo
echo "submitted Jobs:"
kubectl get jobs -n "${NAMESPACE}" -l app.kubernetes.io/component=seed-loader -o wide || true
