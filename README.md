# Cloud Computing & Big Data — Prüfungsleistung 2026

Datengetriebener Prototyp auf Kubernetes-Basis. Project name: **Stream2Pretrain**. Domain focus: streaming curation for fresh AI-research pretraining data (arXiv, GitHub, HuggingFace, AI-lab blogs).

## What this directory contains right now

This is the **research and planning phase** of the project. No application code has been written yet.

| File / dir | Purpose |
|---|---|
| `RESEARCH.md` | Full deep-research report + implementation plan. The single most important document. |
| `SOURCES.md` | Phase-1 + Phase-2 source feeds with endpoints, rate limits, and big-data V's mapping. |
| `lecture_slides/` | Archived copy of all 19 markdown lecture decks from `farberg.de/talks/cloud/` for offline reference. |
| `CLAUDE.md` | Decision log + working notes for the AI pair. |
| `README.md` | This file. |

The eventual project repo (`stream2pretrain-curator/`) will live as a sibling directory once Week 1 of the implementation plan begins. See `RESEARCH.md` section 8 for the planned repo layout.

## Project idea in one paragraph

A Kubernetes-native, streaming-first curation pipeline for LLM pretraining data. It ingests live web documents (RSS, sitemaps, manual submissions), runs FineWeb-class curation operators (HTML extraction, language ID, Gopher/C4 heuristics, MinHash near-deduplication, classifier-based quality scoring, PII filtering, benchmark decontamination) as long-running stateful stream operators on event-time semantics, and lands curated tokens into an Apache Iceberg lakehouse on MinIO. Three differentiators that survived adversarial novelty review: (a) a streaming Decon-Gate sidecar that emits a per-Iceberg-snapshot signed contamination attestation, (b) a per-document validity-interval column with an `as_of(timestamp)` query view for deterministic contamination replay, and (c) a shadow-mode A/B mixture comparison primitive where two `MixtureRecipe` CRDs read the same live SourceFeed.

## Where to read what

- **Use case + Big-Data motivation** — `RESEARCH.md` section 1 (Executive Summary)
- **State of the art / why this is not redundant** — `RESEARCH.md` section 2
- **Genuinely novel angles (with adversarial verification)** — `RESEARCH.md` section 3
- **Architecture diagram + component breakdown** — `RESEARCH.md` section 4
- **Final tech stack picks with reasoning** — `RESEARCH.md` section 5
- **Bronze/Silver/Gold data model + data passport** — `RESEARCH.md` section 6
- **Week-by-week implementation plan (4-6 weeks)** — `RESEARCH.md` section 7
- **Demo / screenshot story for the grader** — `RESEARCH.md` section 9
- **Honest risks and open questions** — `RESEARCH.md` section 10

## Lecture-stack alignment (from `lecture_slides/`)

The course is heavily Kubernetes-centric: Terraform + Ansible -> k3s on DHBWCloud, Helm + Skaffold for app delivery, KEDA for autoscaling, Prometheus + Grafana + Loki + Alloy for observability, Traefik + cert-manager for ingress + TLS, Valkey as a queue example. **No Hadoop, Spark, or Kafka decks** in the course - choosing Redpanda + Bytewax counts as a "begründete Abweichung" eligible for bonus points.

## Source data the research was built from

- Lecture slides (this folder): all 19 markdown files from `farberg.de/talks/cloud/`, fetched 2026-06-15 with Keycloak login
- Project assignment: the prompt text the user pasted on 2026-06-15
- Web research via Perplexity + targeted GitHub/arXiv searches across 6 landscape lenses, 5 novelty lenses, and 14 adversarial verifiers, run as a single Workflow on 2026-06-15 (run id `wf_14fc06f4-2b8`, 30 subagents, 244 tool uses)

## Next steps

Start Week 1 of the implementation plan in `RESEARCH.md` section 7.

## Quickstart (dev)

The local dev stack runs Redpanda single-node + MinIO + the FastAPI submit
API on a laptop, no Kubernetes required. It backs all `tests/integration/`.

```bash
# 1. boot Redpanda + MinIO
make dev-up

# 2. seed the four core topics
make seed-topics

# 3. run the submit API (uses the dev feed catalogue)
S2P_FEED_CONFIG=ingest/feeds.dev.yaml \
S2P_REDPANDA_BROKERS=localhost:9092 \
S2P_MINIO_ENDPOINT=http://localhost:9000 \
S2P_MINIO_ACCESS_KEY=minioadmin \
S2P_MINIO_SECRET_KEY=minioadmin \
  uv run uvicorn ingest.submit_api.app:app --host 127.0.0.1 --port 8000

# 4. one-shot smoke (compose up + topics + API + sample submission)
bash scripts/dev_smoke.sh

# 5. tests
uv run pytest

# 6. load test (k6 must be installed separately)
k6 run -e SUBMIT_URL=http://localhost:8000/submit tests/load/k6_submit.js
```

UIs to open while developing:

- Redpanda Console: http://localhost:8080
- MinIO Console: http://localhost:9001 (minioadmin / minioadmin)
- Submit API: http://localhost:8000/docs

Tear the stack down with `make dev-down` (preserves volumes) or
`make dev-reset` (destroys volumes).

## Quickstart (k3s)

The full deployment runs as a single Helm release pulled in via Helmfile.
The chart bundles every Stream2Pretrain component plus the lecture stack
(kube-prometheus-stack, Loki, Alloy, Tempo, Traefik, cert-manager, KEDA,
OPA Gatekeeper).

```bash
# 1. provision the cluster on DHBWCloud (one-time)
bash infra/k3s-install.sh

# 2. apply the chart and dependencies
helmfile -f helmfile.yaml apply

# 3. seed the Phase-1 SourceFeed CRDs
NAMESPACE=stream2pretrain bash scripts/load_seed_feeds.sh

# 4. open the cockpit
kubectl -n stream2pretrain port-forward svc/stream2pretrain-ui 3000:3000
open http://localhost:3000/dashboard

# 5. drive load
k6 run -e SUBMIT_URL=https://stream2pretrain.demo/submit tests/load/k6_submit.js
```

Operational runbooks (scale, debug, restart from checkpoint, rotate signing
key) live in [`docs/operations.md`](docs/operations.md).

## Repo layout

```
.
|- charts/stream2pretrain/   single Helm chart, all components + CRDs
|  |- crds/                  SourceFeed, MixtureRecipe
|  |- templates/             ingest, processor, ui, observability, policy
|  |- dashboards/            Grafana JSON
|  +- values{,-dev,-prod}.yaml
|- helmfile.yaml             top-level deploy
|- infra/                    Terraform + k3s install + helmfile values
|- ingest/                   pollers + FastAPI submit
|  |- common/                shared HTTP, S3, kafka, OTel, rate-limit, robots
|  |- submit_api/            FastAPI Deployment
|  |- rss_poller/, sitemap_poller/, oaipmh_poller/
|  +- github_events/, github_releases/, hf_poller/
|- processor/                Bytewax dataflows + curation operators
|  |- operators/             extraction, langid, gopher/c4, minhash, lshbloom,
|  |                         quality, kenlm, pii, validity, decon
|  |- mixture_controller/    shadow-A/B reconciler
|  |- fetcher.py, curate.py, iceberg_writer.py, decon_gate.py, sign.py
|- schemas/                  shared Pydantic + JSON-Schema mirror
|  +- bronze.py, silver.py, gold.py, decon.py, sourcefeed.py, topics.py
|- ui/                       Next.js 14 App Router + shadcn/ui + TanStack
|  |- app/{dashboard,sources,decon,as-of,mixtures}
|  +- components/, lib/duckdb-client.ts
|- docs/                     architecture, data-model, operations, novelty,
|                            threat-model
|- scripts/                  seed_topics.sh, load_seed_feeds.sh,
|                            dev_smoke.sh, decon_bisect.py
|- tests/                    integration + load (component unit tests live
|                            alongside their packages)
|- docker-compose.dev.yml    laptop dev stack
|- pyproject.toml + uv.lock  uv workspace
+- README.md, CLAUDE.md, RESEARCH.md, SOURCES.md, CHANGELOG.md, LICENSE
```
