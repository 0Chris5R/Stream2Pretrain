# Stream2Pretrain

[![Version](https://img.shields.io/badge/version-0.2.0-blue)](CHANGELOG.md) [![License](https://img.shields.io/badge/license-Apache--2.0-green)](LICENSE) [![Python](https://img.shields.io/badge/python-3.11%2B-blue)](pyproject.toml) [![Node](https://img.shields.io/badge/node-20%2B-blue)](ui/package.json) [![Kubernetes](https://img.shields.io/badge/kubernetes-1.30%2B-blue)](charts/stream2pretrain)

A Kubernetes-native, streaming-first curation pipeline for **frontier-LLM-research pretraining data**. Live arXiv full-text HTML, OpenReview reviews, GitHub release source archives, HuggingFace metadata, and AI-lab blogs flow through a Bytewax dataflow that runs the FineWeb / DCLM / Dolma curation recipe (HTML extraction, language ID, Gopher/C4 heuristics, KenLM perplexity, MinHash + LSHBloom near-dup, FineWeb-Edu ONNX INT8 quality, PII redaction, benchmark decontamination), then lands typed Bronze/Silver/Gold tables in an Apache Iceberg V3 lakehouse on MinIO. The chart supports KEDA lag scaling and OpenTelemetry export after their thresholds and backends are measured; the DHBW baseline keeps both disabled. Prometheus and a Next.js 14 cockpit expose throughput, signed contamination attestations, temporal `as_of(timestamp)` queries, and the shadow A/B mixture comparison.

This repository is the Cloud Computing & Big Data **Prüfungsleistung 2026** at DHBW. It hits all five Vs of Big Data on the Kubernetes substrate the lecture defines, with one **begründete Abweichung** (Redpanda + Bytewax, justified below) eligible for bonus points.

## What it does

- Ingests live AI-research documents from 9 sources: arXiv OAI-PMH + 4 RSS feeds, arXiv native HTML at `/html/<id>` (with `ar5iv` fallback), GitHub Public Events filtered to ~30 curated AI repos, GitHub Releases Atom plus per-release source tarballs, HuggingFace Hub models + Daily Papers, OpenReview API v2 + REVIEWARENA backfill for ICLR/NeurIPS/ICML/COLM, sitemap walks, and an AI-lab blog RSS bundle.
- Pre-loads ~55-65 B tokens of historical material via a one-shot **seed loader** that streams a 5-component HuggingFace mixture (peS2o cs.\* + RedPajama-arxiv + FineWeb-Edu URL-filtered + Stack-Edu Python/ML + Wayback backfill) into the same pipeline, so the temporal-query demo has data on day 1.
- Runs the full FineWeb-class curation recipe as a single Bytewax dataflow with event-time semantics and stateful operators across job boundaries.
- Lands per-document validity intervals, SPDX license tags, and signed contamination attestations alongside the curated Gold table.
- Ships a cockpit UI with live throughput, quality histograms, signed-attestation viewer, an `as_of(timestamp)` temporal browser, and a shadow A/B mixture comparison view.

## Three locked novelty differentiators

These survived adversarial verification across 14 candidate claims; they are the demo headliners and the rubric-relevant bonus material.

**N1 - Streaming Decon-Gate with per-snapshot signed contamination attestation.** A 13-gram Bloom filter plus an E5-small ONNX embedding sketch run inline during ingestion against MMLU / GSM8K / HumanEval / MATH / GPQA. On every Iceberg snapshot commit the gate emits a canonical-JSON attestation signed with cosign (or Ed25519 from a Secret-mounted key in dev) carrying per-benchmark hit counts, rejected document hashes, the benchmark-set version pin, and the snapshot id. Implemented in `processor/decon_gate.py`, `processor/sign.py`, `processor/iceberg_writer.py`, surfaced in `ui/app/decon`. No other OSS curator ships streaming + snapshot-bound + signed.

**N2 - Per-document validity interval with `as_of(timestamp)` temporal query.** Every record carries a typed `[valid_from, valid_to)` interval populated from the strongest available source (HTTP Last-Modified > schema.org datePublished > sitemap lastmod > Wayback first-seen > dataset native publication date > fetched_at). The interval propagates all the way into the token-shard manifest as a column over token-id ranges, and `as_of(timestamp)` returns the deterministic mixture as it would have been at any past instant - the foundation for contamination replay and time-conditioned training. Implemented in `processor/operators/validity.py`, `processor/iceberg_writer.py`, `ui/app/as-of`. The seed loader populates this column from each dataset's native publication metadata, so the `as_of('2024-06-01')` demo works on day 1, not after multi-day live polling.

**N3 - Shadow-mode A/B mixture comparison via two `MixtureRecipe` CRDs.** Two `MixtureRecipe` CRDs subscribe to the same live `SourceFeed`, materialise separate Iceberg branches, a small proxy LM continuously trains on each branch on a rolling window, and per-domain perplexity deltas gate auto-promotion. Argo-Rollouts-style progressive delivery transplanted onto a streaming data-curation substrate. Implemented in `processor/mixture_controller/`, `ui/app/mixture`. The proxy LM is a documented stub with a clean swap-in interface.

## Architecture

```
                        +------------------+
                        |  Next.js UI      |  dashboard, cockpit, decon viewer,
                        +--------+---------+  as_of, mixtures
                                 |
                          REST/WebSocket/SSE
                                 |
+----------------+               |
| RSS / Atom /   |               |
| Sitemap /      |               |
| OAI-PMH /      |               |               +--------------------+
| arXiv-HTML /   +-------------->|               |  Redpanda          |
| GitHub Events /|               +-------------->+  topics:           |
| GH Releases /  |   (live ingest pollers and    |  raw.fetched       |
| GH Tarballs /  |    fetchers, KEDA-scaled)     |  docs.normalized   |
| HF Hub /       |                               |  docs.curated      |
| OpenReview     |                               |  decon.attest      |
+----------------+                               +---------+----------+
+--------------------------------+                         |
| Seed loader (one-shot Bytewax) +------------------------>|  Kafka API
|  peS2o + RedPajama + FineWeb-  |                         v
|  Edu + Stack-Edu + Wayback     |   +------------------------------------------------+
+--------------------------------+   |  Bytewax curation dataflow (KEDA on lag)       |
                                     |                                                |
                                     |  fetcher -> Resiliparse -> langID              |
                                     |     -> Gopher/C4 taggers -> KenLM perplexity   |
                                     |     -> Rensa MinHash -> LSHBloom near-dup      |
                                     |     -> FineWeb-Edu ONNX INT8 -> PII regex      |
                                     |     -> Decon-Gate (N1) + sign                  |
                                     |     -> validity-interval enricher (N2)         |
                                     |     -> Iceberg writer                          |
                                     +------------------------+-----------------------+
                                                              v
                                  +---------------------------+--------------+
                                  |  MinIO (S3) + Apache Iceberg V3         |
                                  |  bronze, silver, gold, decon_attestations|
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
|  |                           quality (FineWeb-Edu ONNX), pii, validity
|  |- seed/                    per-component HF dataset loaders
|  |- mixture_controller/      kopf-based reconciler (N3)
|  +- fetcher.py, curate.py, iceberg_writer.py, decon_gate.py, sign.py,
|     seed_loader.py, tokenize.py
|- ui/                       Next.js 14 App Router + shadcn/ui + TanStack Query
|  |- app/{dashboard,sources,decon,as-of,mixture}
|  |- app/api/               throughput SSE, decon verify, sources CRUD,
|  |                           dashboard, duckdb query, mixture compare
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

# 2. seed the four core topics
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
6. **`as_of(timestamp)` view** - date picker; the table shows the deterministic token mixture as it would have been at that timestamp (powered by the seed loader on day 1).
7. **DuckDB query** - `SELECT lang, COUNT(*), SUM(tokens) FROM gold WHERE risk_tier=1 GROUP BY lang;`.
8. **Trace correlation** - after a measured Tempo backend is installed, a fresh SourceFeed item can be followed by the `trace_id` materialised into the manifest.
9. **Measured KEDA scale-up** - after capacity benchmarking, use the recorded load profile and replica limits to demonstrate lag-based scaling.
10. **Failure recovery** - `kubectl delete pod` on a curator; Bytewax restores from RocksDB checkpoint and resumes without duplicates (Redpanda transactional consumer).

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

## License

Apache-2.0 - see [`LICENSE`](LICENSE). License detection on ingested documents is **heuristic**; Stream2Pretrain is best-effort, not a legal-compliance product.

## Project framing

Cloud Computing & Big Data Prüfungsleistung 2026, DHBW. Submission target: a multi-component cloud system on Kubernetes that hits all five Vs of Big Data, aligns with the lecture's tech stack, and demonstrates one or more bonus-point novelties. The streaming-K8s-native + lakehouse + temporal + signed-attestation integration shape is the wedge versus DataTrove / NeMo Curator / Dolma / data-juicer / data-prep-kit / DCLM.
