#!/usr/bin/env bash
# Stream2Pretrain - apply the SourceFeed CRDs into the active kube context.
#
# This converts the dev YAML catalogue (ingest/feeds.dev.yaml) into proper
# SourceFeed CRD instances and applies them with `kubectl`. The shape mirrors
# charts/stream2pretrain/crds/sourcefeed.yaml.
#
# Usage:
#   bash scripts/load_seed_feeds.sh                   # default namespace
#   NAMESPACE=stream2pretrain bash scripts/load_seed_feeds.sh
#   DRY_RUN=1 bash scripts/load_seed_feeds.sh         # render only
#
# Idempotent: re-running updates existing SourceFeed objects in place.

set -euo pipefail

NAMESPACE="${NAMESPACE:-stream2pretrain}"
DRY_RUN="${DRY_RUN:-0}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CRD_FILE="${REPO_ROOT}/charts/stream2pretrain/crds/sourcefeed.yaml"

if [[ "${DRY_RUN}" != "1" ]]; then
  command -v kubectl >/dev/null 2>&1 || {
    echo "error: kubectl not found on PATH" >&2
    exit 1
  }
fi

if [[ ! -f "${CRD_FILE}" ]]; then
  echo "error: SourceFeed CRD not found at ${CRD_FILE}" >&2
  exit 1
fi

# The controller-supported set, expressed as inline manifests so the
# script has no Python dependency. This script installs only sources handled by
# the generic arXiv RSS and OAI-PMH discovery templates. Discovery CRDs are
# internal scheduling lanes and are not exposed as corpus sources.
read -r -d '' MANIFEST <<'YAML' || true
---
apiVersion: stream2pretrain.io/v1alpha1
kind: SourceFeed
metadata:
  name: rss-arxiv-cs-cl
spec:
  name: rss-arxiv-cs-cl
  protocol: rss
  endpoint: https://rss.arxiv.org/rss/cs.CL
  pollIntervalSeconds: 7200
  rateLimit:
    requestsPerSecond: 1.0
    burst: 4
  licenseDefault: per-record
  egressAllow: ["rss.arxiv.org", "arxiv.org", "export.arxiv.org"]
  enabled: true
---
apiVersion: stream2pretrain.io/v1alpha1
kind: SourceFeed
metadata:
  name: rss-arxiv-cs-lg
spec:
  name: rss-arxiv-cs-lg
  protocol: rss
  endpoint: https://rss.arxiv.org/rss/cs.LG
  pollIntervalSeconds: 7200
  rateLimit:
    requestsPerSecond: 1.0
    burst: 4
  licenseDefault: per-record
  egressAllow: ["rss.arxiv.org", "arxiv.org", "export.arxiv.org"]
  enabled: true
---
apiVersion: stream2pretrain.io/v1alpha1
kind: SourceFeed
metadata:
  name: rss-arxiv-cs-ai
spec:
  name: rss-arxiv-cs-ai
  protocol: rss
  endpoint: https://rss.arxiv.org/rss/cs.AI
  pollIntervalSeconds: 7200
  rateLimit:
    requestsPerSecond: 1.0
    burst: 4
  licenseDefault: per-record
  egressAllow: ["rss.arxiv.org", "arxiv.org", "export.arxiv.org"]
  enabled: true
---
apiVersion: stream2pretrain.io/v1alpha1
kind: SourceFeed
metadata:
  name: rss-arxiv-cs-cv
spec:
  name: rss-arxiv-cs-cv
  protocol: rss
  endpoint: https://rss.arxiv.org/rss/cs.CV
  pollIntervalSeconds: 7200
  rateLimit:
    requestsPerSecond: 1.0
    burst: 4
  licenseDefault: per-record
  egressAllow: ["rss.arxiv.org", "arxiv.org", "export.arxiv.org"]
  enabled: true
---
apiVersion: stream2pretrain.io/v1alpha1
kind: SourceFeed
metadata:
  name: oai-arxiv-cs
spec:
  name: oai-arxiv-cs
  protocol: oai-pmh
  endpoint: https://oaipmh.arxiv.org/oai
  pollIntervalSeconds: 7200
  rateLimit:
    requestsPerSecond: 4.0
    burst: 4
  licenseDefault: per-record
  egressAllow: ["oaipmh.arxiv.org", "export.arxiv.org"]
  enabled: true
YAML

if [[ "${DRY_RUN}" == "1" ]]; then
  echo "${MANIFEST}"
  exit 0
fi

echo "applying SourceFeed catalogue to namespace=${NAMESPACE}"
echo "${MANIFEST}" | kubectl apply -n "${NAMESPACE}" -f -

echo
echo "current SourceFeeds:"
kubectl get sourcefeeds.stream2pretrain.io -n "${NAMESPACE}" -o wide || true
