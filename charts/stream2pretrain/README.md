# Stream2Pretrain Helm chart

The chart owns the source pollers, Bytewax processors, classifier services,
Iceberg writer, DuckDB API, Foundry, UI and mixture controller. It also supplies
SourceFeed and MixtureRecipe CRDs, KEDA configuration, persistence, metrics,
alerts, network policies and Gatekeeper constraints.

Platform dependencies are installed through [the root Helmfile](../../helmfile.yaml).
The measured DHBW overlay is
[stream2pretrain.dev.yaml](../../infra/helmfile-values/stream2pretrain.dev.yaml).
The production values enable security controls but require measured capacity
overrides; they are not a second validated deployment.

## Images

CI builds immutable application images and reuses unchanged dependency/model
layers. Image tags and per-component digests are chart values.

| Image suffix | Workload |
|---|---|
| ingest-rss | arXiv RSS discovery |
| ingest-oaipmh | arXiv OAI-PMH discovery |
| ingest-hf | Model and dataset README polling |
| ingest-arxiv-html | Full-paper acquisition |
| processor | Curator, Iceberg writer, DuckDB and mixture controller |
| processor-fetcher-model | Extraction, PDF, OCR and figure processing |
| processor-quality-model | Four custom ModernBERT classifiers |
| processor-kenlm-model | Generic web-prose perplexity service |
| processor-foundry | SFT/RL worker and API |
| ui | Next.js cockpit |

The [deployment workflow](../../.github/workflows/deploy-main.yml) supplies the
registry, immutable image identities and workload-specific overrides.

## Installation

Create the platform dependencies and Secrets before installing the application.
Use the [repository deployment guide](../../README.md#9-deployment-guide) for the
complete bootstrap order.

```sh
helm upgrade --install stream2pretrain ./charts/stream2pretrain \
  --namespace stream2pretrain --create-namespace \
  -f charts/stream2pretrain/values-dev.yaml \
  -f infra/helmfile-values/stream2pretrain.dev.yaml
```

Supply built image references for a manual install. The CI workflow does this
automatically.

## Secrets

| Secret | Keys | Consumer |
|---|---|---|
| stream2pretrain-minio | accessKey, secretKey | Object-store clients |
| stream2pretrain-polaris | clientId, clientSecret | Catalog clients |
| stream2pretrain-hf | token | HF poller |
| stream2pretrain-foundry-providers | HETZNER_INFERENCE_API_KEY, controlToken | Foundry worker/API and authenticated artifact audits |
| stream2pretrain-foundry-signing | ed25519.key, ed25519.crt | Artifact signer; deployment creates it once if absent |
| stream2pretrain-keda-redpanda | sasl, tls, username, password | KEDA only when broker authentication is enabled |

Secret names and key names are configurable. Never commit credential values.

## State and scaling

The core Bytewax flows use coordinated executions with retained recovery PVCs.
Broker lag is not their checkpoint authority. Stateless inference replicas and
independent pollers have separate scaling controls. The arXiv acquisition
worker consumes and publishes on the shared raw topic, so that topic's lag is
not a valid autoscaling signal for this worker.

SourceFeed and MixtureRecipe schemas live under `crds/`. Helm installs them
on first installation. Apply changed CRD schemas explicitly before an upgrade.
Gatekeeper can enforce per-item licensing and configured polling bounds.

## Observability

ServiceMonitors and the Grafana dashboard expose stage throughput, classifier
scores and latency, workload availability, persistence latency, queue depth and
Foundry provider/validation activity. The dashboard JSON is packaged from
`dashboards/stream2pretrain.json`.

## Deterministic validation

```sh
helm lint charts/stream2pretrain
helm lint charts/stream2pretrain -f charts/stream2pretrain/values-dev.yaml
helm lint charts/stream2pretrain -f charts/stream2pretrain/values-prod.yaml
helm template stream2pretrain charts/stream2pretrain \
  -f charts/stream2pretrain/values-dev.yaml \
  -f infra/helmfile-values/stream2pretrain.dev.yaml
```

Rendering does not connect to Kubernetes. Runtime smoke validation uses the
isolated lane described in the repository deployment guide.
