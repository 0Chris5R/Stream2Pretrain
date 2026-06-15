#!/usr/bin/env bash
# Stream2Pretrain - apply the Phase-1 SourceFeed CRDs into the active kube context.
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

# The Phase-1 set, expressed as inline manifests so the script has no Python
# dependency. Endpoints, intervals, and rate limits track SOURCES.md and
# ingest/feeds.dev.yaml verbatim.
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
  licenseDefault: arxiv-non-exclusive-distribution
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
  licenseDefault: arxiv-non-exclusive-distribution
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
  licenseDefault: arxiv-non-exclusive-distribution
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
  licenseDefault: arxiv-non-exclusive-distribution
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
  licenseDefault: arxiv-non-exclusive-distribution
  egressAllow: ["oaipmh.arxiv.org", "export.arxiv.org"]
  enabled: true
---
apiVersion: stream2pretrain.io/v1alpha1
kind: SourceFeed
metadata:
  name: github-events
spec:
  name: github-events
  protocol: rest-json
  endpoint: https://api.github.com/events
  pollIntervalSeconds: 60
  rateLimit:
    requestsPerSecond: 1.0
    burst: 1
    respectXPollInterval: true
  licenseDefault: unknown
  auth:
    type: bearer
    secretName: github-token
    secretKey: token
  egressAllow: ["api.github.com", "github.com"]
  enabled: true
---
apiVersion: stream2pretrain.io/v1alpha1
kind: SourceFeed
metadata:
  name: github-releases
spec:
  name: github-releases
  protocol: atom
  endpoint: https://github.com
  pollIntervalSeconds: 7200
  rateLimit:
    requestsPerSecond: 2.0
    burst: 4
  licenseDefault: unknown
  egressAllow: ["github.com", "api.github.com"]
  enabled: true
---
apiVersion: stream2pretrain.io/v1alpha1
kind: SourceFeed
metadata:
  name: hf-models
spec:
  name: hf-models
  protocol: rest-json
  endpoint: https://huggingface.co/api/models
  pollIntervalSeconds: 900
  rateLimit:
    requestsPerSecond: 1.0
    burst: 2
  licenseDefault: unknown
  auth:
    type: bearer
    secretName: hf-token
    secretKey: token
  egressAllow: ["huggingface.co", "hf.co"]
  enabled: true
---
apiVersion: stream2pretrain.io/v1alpha1
kind: SourceFeed
metadata:
  name: hf-daily-papers
spec:
  name: hf-daily-papers
  protocol: rest-json
  endpoint: https://huggingface.co/api/daily_papers
  pollIntervalSeconds: 21600
  rateLimit:
    requestsPerSecond: 0.5
    burst: 1
  licenseDefault: unknown
  auth:
    type: bearer
    secretName: hf-token
    secretKey: token
  egressAllow: ["huggingface.co", "hf.co"]
  enabled: true
---
apiVersion: stream2pretrain.io/v1alpha1
kind: SourceFeed
metadata:
  name: rss-openai-news
spec:
  name: rss-openai-news
  protocol: rss
  endpoint: https://openai.com/news/rss.xml
  pollIntervalSeconds: 21600
  rateLimit:
    requestsPerSecond: 1.0
    burst: 2
  licenseDefault: unknown
  egressAllow: ["openai.com", "cdn.openai.com"]
  enabled: true
---
apiVersion: stream2pretrain.io/v1alpha1
kind: SourceFeed
metadata:
  name: rss-deepmind-blog
spec:
  name: rss-deepmind-blog
  protocol: rss
  endpoint: https://deepmind.google/blog/rss.xml
  pollIntervalSeconds: 21600
  rateLimit:
    requestsPerSecond: 1.0
    burst: 2
  licenseDefault: unknown
  egressAllow: ["deepmind.google"]
  enabled: true
---
apiVersion: stream2pretrain.io/v1alpha1
kind: SourceFeed
metadata:
  name: rss-hf-blog
spec:
  name: rss-hf-blog
  protocol: rss
  endpoint: https://huggingface.co/blog/feed.xml
  pollIntervalSeconds: 21600
  rateLimit:
    requestsPerSecond: 1.0
    burst: 2
  licenseDefault: unknown
  egressAllow: ["huggingface.co", "hf.co"]
  enabled: true
---
apiVersion: stream2pretrain.io/v1alpha1
kind: SourceFeed
metadata:
  name: rss-bair-blog
spec:
  name: rss-bair-blog
  protocol: rss
  endpoint: https://bair.berkeley.edu/blog/feed.xml
  pollIntervalSeconds: 86400
  rateLimit:
    requestsPerSecond: 0.5
    burst: 1
  licenseDefault: unknown
  egressAllow: ["bair.berkeley.edu"]
  enabled: true
---
apiVersion: stream2pretrain.io/v1alpha1
kind: SourceFeed
metadata:
  name: rss-eleuther-blog
spec:
  name: rss-eleuther-blog
  protocol: rss
  endpoint: https://blog.eleuther.ai/index.xml
  pollIntervalSeconds: 86400
  rateLimit:
    requestsPerSecond: 0.5
    burst: 1
  licenseDefault: unknown
  egressAllow: ["blog.eleuther.ai"]
  enabled: true
YAML

if [[ "${DRY_RUN}" == "1" ]]; then
  echo "${MANIFEST}"
  exit 0
fi

echo "applying Phase-1 SourceFeed catalogue to namespace=${NAMESPACE}"
echo "${MANIFEST}" | kubectl apply -n "${NAMESPACE}" -f -

echo
echo "current SourceFeeds:"
kubectl get sourcefeeds.stream2pretrain.io -n "${NAMESPACE}" -o wide || true
