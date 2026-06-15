# stream2pretrain Helm chart

The single Helm chart that deploys every Stream2Pretrain component on a
Kubernetes cluster: ingest CronJobs/Deployments, the Bytewax curate StatefulSet
with the decon-gate sidecar, the iceberg-writer Deployment, the FastAPI
submit-api, the Next.js UI, the kopf mixture-controller, the SourceFeed and
MixtureRecipe CRDs, KEDA ScaledObjects, NetworkPolicies, ServiceMonitors, the
Stream2Pretrain Grafana dashboard, and OPA Gatekeeper constraints.

The platform-layer dependencies (Redpanda, MinIO, Polaris, KEDA controllers,
OPA Gatekeeper, kube-prometheus-stack, Loki, Alloy, Tempo, Traefik,
cert-manager) are NOT included here - install them via `infra/helmfile.yaml`.

## Image building is out of scope for this chart

The chart references images by `<registry>/<repo>:<tag>` where tag falls back
to `.Chart.AppVersion`. Producing those images is a CI job. Expected refs:

- `<registry>/stream2pretrain/ingest-rss:<tag>`
- `<registry>/stream2pretrain/ingest-oaipmh:<tag>`
- `<registry>/stream2pretrain/ingest-sitemap:<tag>`
- `<registry>/stream2pretrain/ingest-github-events:<tag>`
- `<registry>/stream2pretrain/ingest-github-releases:<tag>`
- `<registry>/stream2pretrain/ingest-hf:<tag>`
- `<registry>/stream2pretrain/submit-api:<tag>`
- `<registry>/stream2pretrain/processor-fetcher:<tag>`
- `<registry>/stream2pretrain/processor-curate:<tag>`
- `<registry>/stream2pretrain/processor-iceberg-writer:<tag>`
- `<registry>/stream2pretrain/decon-gate:<tag>`
- `<registry>/stream2pretrain/mixture-controller:<tag>`
- `<registry>/stream2pretrain/ui:<tag>`

Override the registry via `--set image.registry=ghcr.io/myorg`.

## Install

```sh
helm install stream2pretrain ./charts/stream2pretrain \
    --namespace stream2pretrain --create-namespace \
    -f charts/stream2pretrain/values-dev.yaml
```

For production:

```sh
helm install stream2pretrain ./charts/stream2pretrain \
    --namespace stream2pretrain --create-namespace \
    -f charts/stream2pretrain/values-prod.yaml \
    --set ui.ingress.host=stream2pretrain.example.com \
    --set ingest.submitApi.ingress.host=submit.stream2pretrain.example.com
```

## Required secrets

The chart references the following Secrets but does not create them. Provide
via `sealed-secrets` or External Secrets Operator before `helm install`:

| Secret                                              | Keys                              | Used by                       |
|-----------------------------------------------------|-----------------------------------|-------------------------------|
| `stream2pretrain-minio` (`.Values.minio.credentialsSecret`)      | `accessKey`, `secretKey`          | every component               |
| `stream2pretrain-github` (`.Values.sources.github.events.tokenSecret`) | `token` (PAT, `read:public`)      | ingest-github-events / -releases |
| `stream2pretrain-hf` (`.Values.sources.huggingface.models.tokenSecret`) | `token` (HF user token)           | ingest-hf                     |
| `stream2pretrain-decon-signing` (`.Values.processor.deconGate.signingKeySecret`) | `ed25519.key` (raw 32-byte key) | decon-gate sidecar            |
| `stream2pretrain-keda-redpanda` (`.Values.keda.triggerAuthSecret`) | `sasl`, `tls`, `username`, `password` | KEDA Kafka trigger            |

## CRDs

The chart ships two CRDs under `crds/`:

- `SourceFeed.stream2pretrain.io/v1alpha1`
- `MixtureRecipe.stream2pretrain.io/v1alpha1`

OpenAPI v3 schemas mirror `schemas/sourcefeed.py`. Helm installs CRDs from
`crds/` exactly once on first install; upgrades require explicit `kubectl
apply -f charts/stream2pretrain/crds/`.

## OPA Gatekeeper

When `gatekeeper.enabled=true`, the chart installs a ConstraintTemplate +
Constraint that:

- Rejects SourceFeeds with `licenseDefault = unknown`.
- Rejects SourceFeeds with `pollIntervalSeconds` outside
  `[gatekeeper.minPollIntervalSeconds, gatekeeper.maxPollIntervalSeconds]`.
- Rejects SourceFeeds whose license is not on the SPDX allow-list.

## Grafana dashboard

`dashboards/stream2pretrain.json` is a ready-made dashboard with:

- Throughput per stage (`s2p_documents_emitted_total`)
- Topic lag and KEDA replica counts
- FineWeb-Edu quality-score histogram
- Decon flag rate (overall and per benchmark)
- Iceberg flush latency p95
- SourceFeed poll outcomes

The chart wraps it in a ConfigMap with the `grafana_dashboard: "1"` label so
the kube-prometheus-stack Grafana sidecar auto-loads it.

## Lint

```sh
helm lint charts/stream2pretrain
helm lint charts/stream2pretrain -f charts/stream2pretrain/values-dev.yaml
helm lint charts/stream2pretrain -f charts/stream2pretrain/values-prod.yaml
helm template charts/stream2pretrain -f charts/stream2pretrain/values-dev.yaml | \
    kubectl apply --dry-run=client -f -
```

## Layout

```
charts/stream2pretrain/
  Chart.yaml
  values.yaml                 -- canonical defaults
  values-dev.yaml             -- local k3s + single broker overrides
  values-prod.yaml            -- DHBWCloud / 3-broker overrides
  values.schema.json          -- JSON Schema validation
  crds/
    sourcefeed.yaml
    mixturerecipe.yaml
  dashboards/
    stream2pretrain.json      -- Grafana dashboard JSON (loaded via ConfigMap)
  templates/
    _helpers.tpl
    NOTES.txt
    serviceaccounts.yaml      -- SA + Role + RoleBinding
    configmap-feeds.yaml      -- per-source JSON config bundles
    secret-tokens.yaml        -- references only; populate externally
    networkpolicies.yaml      -- default-deny + per-egress-class allow
    ingest-*.yaml             -- one file per ingest component
    processor-*.yaml          -- fetcher / curate / iceberg-writer
    mixturecontroller.yaml
    ui.yaml                   -- Deployment + Service + IngressRoute + Cert
    scaledobjects.yaml        -- KEDA ScaledObject + TriggerAuthentication
    servicemonitors.yaml
    grafana-dashboards.yaml   -- ConfigMap embedding dashboards/*.json
    gatekeeper-constraints.yaml
```

The Grafana dashboard JSON lives under `dashboards/` rather than
`templates/grafana-dashboards/` to keep Helm from trying to render it as a
manifest. The wrapper template `grafana-dashboards.yaml` reads it via
`.Files.Get`.
