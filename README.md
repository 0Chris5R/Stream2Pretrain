# Stream2Pretrain

[![Version](https://img.shields.io/badge/version-0.2.0-blue)](CHANGELOG.md) [![License](https://img.shields.io/badge/license-Apache--2.0-green)](LICENSE) [![Python](https://img.shields.io/badge/python-3.11%2B-blue)](pyproject.toml) [![Node](https://img.shields.io/badge/node-20%2B-blue)](ui/package.json) [![Kubernetes](https://img.shields.io/badge/kubernetes-1.30%2B-blue)](charts/stream2pretrain)

A Kubernetes-native, streaming-first curation pipeline for **frontier-LLM-research pretraining data**. Live arXiv HTML/PDF, OpenReview reviews, GitHub release source archives, HuggingFace metadata, and AI-lab blogs flow through a Bytewax dataflow that runs a source-aware FinePDFs/FineWeb/DCLM/Dolma-inspired recipe: structured scientific extraction, language ID, Gopher/C4 heuristics, KenLM perplexity, MinHash plus LSHBloom near-deduplication, pinned FinePDFs Edu v2 for scientific text, FineWeb-Edu for general web text, code-specific rules, PII detection, and benchmark decontamination. Every outcome lands in an auditable Iceberg V2 decision table, while only accepted records enter Gold. The chart supports KEDA lag scaling and OpenTelemetry export after their thresholds and backends are measured; the DHBW baseline keeps both disabled. Prometheus and the Next.js cockpit expose the live pipeline, scientific artifacts, quality signals, dataset exports, and signed contamination attestations. Shadow A/B mixture training remains the explicitly deferred final feature.

This repository is the Cloud Computing & Big Data **Prüfungsleistung 2026** at DHBW. It hits all five Vs of Big Data on the Kubernetes substrate the lecture defines, with one **begründete Abweichung** (Redpanda + Bytewax, justified below) eligible for bonus points.

## What it does

- Ingests live AI-research documents from 9 sources: arXiv OAI-PMH + 4 RSS feeds, arXiv native HTML at `/html/<id>` (with `ar5iv` fallback), GitHub Public Events filtered to ~30 curated AI repos, GitHub Releases Atom plus per-release source tarballs, HuggingFace Hub models + Daily Papers, OpenReview API v2 + REVIEWARENA backfill for ICLR/NeurIPS/ICML/COLM, sitemap walks, and an AI-lab blog RSS bundle.
- Pre-loads ~55-65 B tokens of historical material via a one-shot **seed loader** that streams a 5-component HuggingFace mixture (peS2o cs.\* + RedPajama-arxiv + FineWeb-Edu URL-filtered + Stack-Edu Python/ML + Wayback backfill) into the same pipeline, so the temporal-query demo has data on day 1.
- Applies source-aware quality policy: pinned FinePDFs Edu v2 for scientific
  text, FineWeb-Edu for web text, and code-specific structural rules, while
  keeping model scores separate from the explainable composite score.
- Builds a clean scientific training projection while preserving sections,
  tables, equations, citations, figure assets, CPU OCR, figure-type routing,
  and a bounded Docling CPU fallback when arXiv HTML is unavailable.
- Routes every scored outcome to broad pretraining, reasoning candidate,
  benchmark candidate, quarantine, or extraction retry. Complete decision
  records and validity intervals remain auditable; only accepted training rows
  enter Gold.
- Makes restart and replay behavior durable through a fingerprinted decision
  cache, persistent near-duplicate state, and idempotent Iceberg appends.
- Ships a compact cockpit with per-stage activity windows, filterable document
  and artifact inspection, SourceFeed controls, benchmark coverage and signed
  scans, and bounded JSONL/Parquet dataset exports.

## Three locked novelty differentiators

These survived adversarial verification across 14 candidate claims; they are the demo headliners and the rubric-relevant bonus material.

**N1 - Streaming Decon-Gate with per-snapshot signed contamination attestation.** A 13-gram Bloom filter plus an E5-small ONNX embedding sketch run inline during ingestion against MMLU / GSM8K / HumanEval / MATH / GPQA. Every curation outcome is retained in `curation.decisions`; on an Iceberg write batch the gate emits a canonical-JSON attestation signed with Ed25519, carrying per-benchmark hit counts, rejected document hashes, the benchmark-set version pin, and the authoritative decision snapshot id. Implemented in `processor/decon_gate.py`, `processor/sign.py`, `processor/iceberg_writer.py`, surfaced in `ui/app/decon`.

**N2 - Per-document validity interval with range exports.** Every record carries a typed `[valid_from, valid_to)` interval populated from the strongest available source evidence. DuckDB evaluates those intervals against the current Gold relation, while Datasets selects a date range and exports a pinned manifest plus JSONL/Parquet. This is record-validity time, not Iceberg snapshot time travel. Implemented in `processor/operators/validity.py`, `processor/iceberg_writer.py`, `processor/duckdb_api.py`, and `ui/app/datasets`.

**N3 - Shadow-mode A/B mixture comparison via two `MixtureRecipe` CRDs.** This is deliberately future work. The repository contains the CRD, controller/UI skeleton, and proxy-runner interface, but it does not yet materialise two real Iceberg branches or train/evaluate equal-budget models. The final optional milestone is to place a short-lived remote GPU runner behind that interface and keep promotion in shadow-recommendation mode until its guards are proven.

## Architecture

```
                        +------------------+
                        |  Next.js UI      |  dashboard, documents, sources,
                        +--------+---------+  benchmark safety, datasets
                                 |
                          REST/WebSocket/SSE
                                 |
+----------------+               |
| RSS / Atom /   |               |
| Sitemap /      |               |
| OAI-PMH /      |               |               +--------------------+
| arXiv HTML/PDF +-------------->|               |  Redpanda          |
| GitHub Events /|               +-------------->+  topics:           |
| GH Releases /  |   (live ingest pollers and    |  raw.fetched       |
| GH Tarballs /  |    fetchers, KEDA-scaled)     |  docs.normalized   |
| HF Hub /       |                               |  docs.curated      |
| OpenReview     |                               |  decon.attest      |
|                |                               |  curation.decisions|
+----------------+                               +---------+----------+
+--------------------------------+                         |
| Seed loader (one-shot Bytewax) +------------------------>|  Kafka API
|  peS2o + RedPajama + FineWeb-  |                         v
|  Edu + Stack-Edu + Wayback     |   +------------------------------------------------+
+--------------------------------+   |  Bytewax curation dataflow (KEDA on lag)       |
                                     |                                                |
                                     |  fetcher -> Resiliparse/Docling -> langID      |
                                     |     -> Gopher/C4 taggers -> KenLM perplexity   |
                                     |     -> Rensa MinHash -> LSHBloom near-dup      |
                                     |     -> source quality CPU -> PII/Presidio      |
                                     |     -> Decon-Gate (N1) + sign                  |
                                     |     -> validity-interval enricher (N2)         |
                                     |     -> Iceberg writer                          |
                                     +------------------------+-----------------------+
                                                              v
                                  +---------------------------+--------------+
                                  |  MinIO (S3) + Apache Iceberg V2         |
                                  |  science, decisions, gold, attestations |
                                  +-------------------+---------------------+
                                                      |
                                                      v
                                       DuckDB API (UI lakehouse queries)
                                       Polaris REST catalog

Cross-cutting target: kube-prometheus-stack + optional Loki + Alloy + Tempo,
Traefik IngressRoute + cert-manager TLS, OPA Gatekeeper for SourceFeed admission,
NetworkPolicies (default-deny + per-source egress jail), KEDA Kafka-lag scalers,
mixture-controller (kopf) reconciling MixtureRecipe CRDs (N3).
```

The measured DHBW baseline currently enables kube-prometheus-stack and keeps
Loki, Alloy, Tempo, application NetworkPolicies, Gatekeeper constraints, and
KEDA disabled pending the prerequisites documented below.

Detailed walkthrough in [`docs/architecture.md`](docs/architecture.md). Mermaid source in [`docs/architecture.mmd`](docs/architecture.mmd).

## Repo layout

```
.
|- charts/stream2pretrain/   single Helm chart - all components, CRDs,
|  |- crds/                    SourceFeed, MixtureRecipe
|  |- templates/               ingest, processor, ui, KEDA scalers,
|  |                           NetworkPolicies, Gatekeeper, ServiceMonitors
|  |- dashboards/              Grafana JSON
|  +- values{,-dev,-prod}.yaml + values.schema.json
|- helmfile.yaml             measured DHBW releases: cert-manager, Traefik,
|                              kube-prometheus-stack, KEDA, Gatekeeper,
|                              Redpanda, Polaris, Stream2Pretrain
|- infra/                    Terraform OpenStack (1 control + 2 workers),
|                              k3s install scripts, cloud-init, helmfile
|                              values overlays per environment
|- ingest/                   live pollers and fetchers (Python, uv workspace)
|  |- common/                  HTTP client, S3, Kafka, OTel, rate-limit, robots
|  |- rss_poller/, sitemap_poller/, oaipmh_poller/
|  |- github_events/, github_releases/, github_release_tarball_fetcher/
|  |- hf_poller/, openreview_poller/, arxiv_html_fetcher/
|- processor/                Bytewax dataflows + curation operators
|  |- operators/               extract (Resiliparse), langid, gopher, c4,
|  |                           kenlm_score, minhash (Rensa), lshbloom,
|  |                           quality (FinePDFs/FineWeb/code), pii, validity
|  |- seed/                    per-component HF dataset loaders
|  |- mixture_controller/      kopf-based reconciler (N3)
|  +- fetcher.py, curate.py, scientific.py, scientific_policy.py,
|     decision_cache.py, iceberg_writer.py, decon_gate.py, sign.py,
|     seed_loader.py, tokenize.py
|- ui/                       Next.js App Router + shadcn/ui + TanStack Query
|  |- app/{dashboard,documents,sources,decon,datasets,mixture}
|  |- app/api/               throughput SSE, decon verify, sources CRUD,
|  |                           dashboard, documents, exports, mixture compare
|  +- components/, lib/duckdb-client.ts
|- schemas/                  shared Pydantic v2 + JSON Schemas
|  +- bronze.py, silver.py, gold.py, decon.py, code.py, sourcefeed.py,
|     topics.py, json_schema/
|- docs/                     architecture, data-model, operations,
|                              novelty, threat-model, two research reports
|- scripts/                  seed_topics.sh, load_seed_feeds.sh,
|                              dev_smoke.sh, decon_bisect.py, seed_corpus.sh
|- tests/                    cross-component integration + k6 load
|- docker-compose.dev.yml    laptop dev stack (Redpanda + MinIO)
|- pyproject.toml + Makefile uv workspace + dev targets
+- README.md, CLAUDE.md, RESEARCH.md, SOURCES.md, CHANGELOG.md, LICENSE,
   CONTRIBUTING.md, CODE_OF_CONDUCT.md
```

## Quickstart - dev (laptop)

The dev stack runs Redpanda single-node + MinIO via Docker Compose. No Kubernetes required. Backs every test under `tests/integration/`.

```bash
# 1. boot Redpanda + MinIO
make dev-up

# 2. seed the five core topics
make seed-topics

# 3. run the dev smoke test (compose up + topics + arxiv-html one-shot)
bash scripts/dev_smoke.sh

# 4. tests
uv run pytest
```

UIs while developing:

- Redpanda Console: http://localhost:8080
- MinIO Console: http://localhost:9001 (`minioadmin` / `minioadmin`)

Tear down with `make dev-down` (preserves volumes) or `make dev-reset` (destroys volumes).

For the Podman-first full local path (processors, local Iceberg catalog,
DuckDB APIs, Prometheus, and cockpit), use [`local/README.md`](local/README.md).
The audited student-project scope and implementation plan are in
[`docs/STUDENT_PROJECT_PLAN.md`](docs/STUDENT_PROJECT_PLAN.md); measured output
from the validated nine-document CPU replay is in
[`docs/LOCAL_PILOT_REPORT.md`](docs/LOCAL_PILOT_REPORT.md).

## Quickstart - k3s on DHBWCloud

The supported path is staged so infrastructure, credentials, and stateful data
cannot be changed by one opaque command. It requires Helm 3.

```bash
cp infra/terraform/terraform.tfvars.example infra/terraform/terraform.tfvars
$EDITOR infra/terraform/terraform.tfvars

# Validate locally and review the OpenStack plan.
HELM_BINARY=/opt/homebrew/opt/helm@3/bin/helm \
  ./scripts/setup_dhbw_demo.sh validate
OPENRC_PATH=/absolute/path/to/openrc.sh ./scripts/setup_dhbw_demo.sh plan

# Provision VMs and install k3s only after reviewing the plan.
OPENRC_PATH=/absolute/path/to/openrc.sh ./scripts/setup_dhbw_demo.sh cluster

# Provision the external MinIO service, buckets, Secrets, and benchmark
# ConfigMap listed below, then apply one ownership tier at a time.
./scripts/setup_dhbw_demo.sh platform
./scripts/setup_dhbw_demo.sh catalog
./scripts/setup_dhbw_demo.sh topics
./scripts/setup_dhbw_demo.sh application
./scripts/setup_dhbw_demo.sh verify
```

Do not run the `application` stage against the current legacy release until its
immutable-selector and curator StatefulSet migration is approved. The exact
safe path and measured live state are documented in
[`docs/infrastructure-reimplementation.md`](docs/infrastructure-reimplementation.md).
The detailed operator reference is [`infra/README.md`](infra/README.md).

## What you must provide

The repo is fully implemented in code; what is **not** committed for security and environment reasons:

- **DHBWCloud OpenStack credentials**, image id, existing keypair, flavor names,
  network name, and reviewed security groups (`terraform.tfvars`).
- **DNS zone** for the wildcard cert. Replace the markers in
  `infra/dns/cert-manager-issuer.yaml` only after the team provides the zone.
- **RFC 2136 TSIG key** + authoritative nameserver for cert-manager DNS-01.
- **Externally managed Kubernetes objects** before applying their Helmfile tier:
  - `monitoring/grafana-admin` Secret (`admin-user`, `admin-password`)
  - `polaris/polaris-bootstrap` Secret (`credentials`)
  - `stream2pretrain/stream2pretrain-minio` Secret (`accessKey`, `secretKey`)
  - `stream2pretrain/stream2pretrain-github` Secret (`token`)
  - `stream2pretrain/stream2pretrain-hf` Secret (`token`)
  - `stream2pretrain/stream2pretrain-decon-signing` Secret (`ed25519.key`, `ed25519.crt`)
  - `stream2pretrain/stream2pretrain-polaris` Secret (`credential`, `scope`)
  - `stream2pretrain/stream2pretrain-decon-benchmarks` ConfigMap (`corpus.json`)
- **MinIO service and buckets**. The cluster must provide `minio/minio` and the
  `s2p-bronze`, `s2p-silver`, `s2p-gold`, and `s2p-decon` buckets. The existing
  MinIO deployment is preserved and is not owned by Helmfile.
- **Container image builds** for the 13 component images (`registry/stream2pretrain/<component>:0.2.0`). The Helm chart references images; building them is a CI job. Per-component Dockerfile build commands are in `charts/stream2pretrain/README.md`.
- **Target-cluster capacity measurements** - run `uv run python
  scripts/capacity_probe.py` on the DHBWCloud k3s context and follow
  [`docs/capacity-benchmark.md`](docs/capacity-benchmark.md) before locking
  production Redpanda partitions, worker resources, MinIO throughput, or
  seed-loader PVC size.

## Source data

- **Phase-1 metadata feeds**: arXiv OAI-PMH (`set=cs`), arXiv RSS (cs.CL/LG/AI/CV), GitHub Events (AI-filtered), GitHub Releases Atom (~30 curated AI repos), HF Hub models, HF Daily Papers, AI-lab blog RSS bundle. Target 5-20k docs/day.
- **Phase-1.5 fulltext + code (v0.2.0)**: arXiv native HTML at `/html/<id>` with `ar5iv` fallback, OpenReview API v2 plus REVIEWARENA HF dataset, GitHub release tarballs.
- **Seed corpus (v0.2.0)**: peS2o cs.\* (~50 GB, ODC-By 1.0), RedPajama-arxiv (~92 GB), FineWeb-Edu URL-filtered (~50 GB, ODC-By 1.0), Stack-Edu Python+ML (~80 GB), Wayback 24-month backfill of Phase-1 feeds. Total ~275-280 GB Bronze, ~55-65 B tokens, all permissive licenses.
- **Phase-2** (post-submission): remaining arXiv categories, HF Datasets/Spaces, Semantic Scholar, GitHub READMEs at scale, long-tail blogs, Alignment Forum, marker-pdf GPU sidecar.

Endpoints, rate limits, license posture, and the full Big-Data Vs mapping in [`SOURCES.md`](SOURCES.md). The two research reports backing the v0.2.0 expansions are [`docs/research-fulltext-and-code.md`](docs/research-fulltext-and-code.md) and [`docs/research-seed-corpus.md`](docs/research-seed-corpus.md).

## Demo story

After the measured blockers are resolved, the target grader walkthrough is:

1. **Architecture overview** - the diagram above, every component labelled.
2. `kubectl get pods -A` - all required pods Ready across the three nodes.
3. **Grafana** - throughput per stage, Redpanda lag per topic, quality-score histogram, and decon flag rate.
4. **Cockpit** - live counters (last hour: ingested, curated, rejected by reason), per-source acceptance rates, quality histogram.
5. **Decon-Gate viewer** - click a snapshot, see the signed certificate (per-benchmark hit counts, rejected hashes, signature verification status).
6. **`as_of(timestamp)` view** - date picker; the table shows records in the
   current Gold relation whose `[valid_from, valid_to)` interval contains the
   timestamp.
7. **DuckDB query** - `SELECT lang, COUNT(*), SUM(tokens) FROM gold WHERE risk_tier=1 GROUP BY lang;`.
8. **Trace correlation** - after a measured Tempo backend is installed, a fresh SourceFeed item can be followed by the `trace_id` materialised into the manifest.
9. **Measured KEDA scale-up** - after capacity benchmarking, use the recorded load profile and replica limits to demonstrate lag-based scaling.
10. **Failure recovery** - `kubectl delete pod` on a curator, then measure and
    verify Bytewax recovery plus durable decision-cache, LSH, and Iceberg
    idempotence; report Kafka replay events separately from unique durable rows.

## Why Redpanda + Bytewax (begründete Abweichung)

The lecture stack does not include a streaming bus or stream engine. This project picks **Redpanda** (single binary, Kafka API, ~3-4x lower RAM than JVM Kafka, Kafka-API compatibility preserves the Iceberg connector + KEDA scaler unchanged) and **Bytewax** (Python with a Rust core, Helm-installable, no JVM heap tuning on a 2-worker cluster, Python-native operators line up with the FineWeb / DCLM / Dolma reference implementations). Apache Flink would have demanded a JVM operator and significantly more RAM than DHBWCloud's 2-worker layout can spare. This is the single justified deviation and is documented for bonus-point eligibility per the rubric.

## Documentation index

- [`CHANGELOG.md`](CHANGELOG.md) - per-version what-changed
- [`SOURCES.md`](SOURCES.md) - source feed catalog with rate limits and Vs mapping
- [`RESEARCH.md`](RESEARCH.md) - locked research plan + v0.2 amendment
- [`CLAUDE.md`](CLAUDE.md) - decision log + working notes for the AI pair
- [`CONTRIBUTING.md`](CONTRIBUTING.md) - dev workflow, lint, tests
- [`docs/architecture.md`](docs/architecture.md) - component breakdown
- [`docs/data-model.md`](docs/data-model.md) - Bronze/Silver/Gold field-by-field
- [`docs/operations.md`](docs/operations.md) - deploy, scale, debug, recover
- [`docs/infrastructure-reimplementation.md`](docs/infrastructure-reimplementation.md) - measured DHBW rewrite, live state, and migration boundary
- [`docs/capacity-benchmark.md`](docs/capacity-benchmark.md) - target-cluster capacity measurement procedure
- [`docs/novelty.md`](docs/novelty.md) - the three differentiators with evidence
- [`docs/threat-model.md`](docs/threat-model.md) - STRIDE for the curator
- [`docs/research-fulltext-and-code.md`](docs/research-fulltext-and-code.md) - mid-2026 fulltext + code acquisition research
- [`docs/research-seed-corpus.md`](docs/research-seed-corpus.md) - mid-2026 seed mixture research
- [`docs/STUDENT_PROJECT_PLAN.md`](docs/STUDENT_PROJECT_PLAN.md) - audited state, focused scope, CPU visual path, and local test plan
- [`docs/LOCAL_PILOT_REPORT.md`](docs/LOCAL_PILOT_REPORT.md) - measured Podman replay, model backends, routes, resources, and UI verification

## License

Apache-2.0 - see [`LICENSE`](LICENSE). License detection on ingested documents
is **heuristic**; Stream2Pretrain is best-effort, not a legal-compliance
product. The provisional policy admits non-code documents without a
machine-readable license while preserving `unknown` provenance. Code remains
restricted to the configured permissive SPDX allowlist.

## Project framing

Cloud Computing & Big Data Prüfungsleistung 2026, DHBW. Submission target: a multi-component cloud system on Kubernetes that hits all five Vs of Big Data, aligns with the lecture's tech stack, and demonstrates one or more bonus-point novelties. The streaming-K8s-native + lakehouse + temporal + signed-attestation integration shape is the wedge versus DataTrove / NeMo Curator / Dolma / data-juicer / data-prep-kit / DCLM.
