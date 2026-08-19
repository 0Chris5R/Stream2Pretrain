#!/usr/bin/env bash
# Podman-first lifecycle for the post-training foundry. No command deletes volumes.

set -euo pipefail

REPOSITORY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPOSITORY_ROOT}"

if ! command -v podman >/dev/null 2>&1; then
  echo "error: podman is required" >&2
  exit 2
fi

COMPOSE=(podman compose -f compose.local.yml --profile foundry)

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "error: ${name} must be set" >&2
    exit 2
  fi
}

case "${1:-help}" in
  base)
    podman compose -f compose.local.yml up -d --build
    ;;
  worker)
    require_env HETZNER_INFERENCE_API_KEY
    "${COMPOSE[@]}" up -d processor-foundry
    ;;
  export-replay)
    if [[ -z "${2:-}" ]]; then
      echo "error: export-replay requires a job ID" >&2
      exit 2
    fi
    "${COMPOSE[@]}" run --rm --no-deps --entrypoint s2p-foundry-export-replay \
      processor-foundry --job-id "$2" --output /var/lib/s2p/foundry/replay.json
    ;;
  build-oracle)
    shift
    uv run s2p-foundry-build-oracle "$@"
    ;;
  status)
    "${COMPOSE[@]}" ps
    curl -fsS http://localhost:8092/api/foundry/dashboard
    printf '\nPost-training UI: http://localhost:3100/post-training\n'
    ;;
  logs)
    "${COMPOSE[@]}" logs -f processor-foundry foundry-api
    ;;
  down)
    "${COMPOSE[@]}" stop processor-foundry foundry-api
    ;;
  help|*)
    cat <<'EOF'
Usage: scripts/foundry_local.sh COMMAND

  base               Build/start the existing local pipeline and UI
  worker             Discover configured models and start the foundry worker
  export-replay ID   Export a completed live job to the preserved state volume
  build-oracle ARGS  Build a local network-isolated official-artifact oracle
  status             Show services and the foundry dashboard payload
  logs               Follow foundry worker/API logs
  down               Stop only foundry worker/API; volumes are preserved

The worker needs only the Hetzner API key. Manual dataset runs and
artifact audits are triggered from the Post-training UI.
EOF
    ;;
esac
