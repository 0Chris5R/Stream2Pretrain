# Changelog

All notable changes to Stream2Pretrain are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Added the scientific-paper post-training foundry: immutable paper bundles,
  evidence graphs, grounded task/trajectory generation, deterministic verifier
  compilation, adversarial and mutation validation, signed SFT/RL packages,
  and Prime Verifiers v1 export.
- Added strict Hetzner `Qwen3.8-27B` model discovery, exact route/licence
  records, published minute-quota reservation, streaming checkpoints,
  idempotent call replay, and catalogue-change records.
- Added three foundry Redpanda topics, MinIO/Iceberg persistence, Podman and
  Kubernetes oracle sandboxes, Helm resources, Prometheus/Grafana coverage, a
  read-only API, and the Post-training UI.
- Added a once-daily ranked post-training snapshot bounded by transactional
  provider quota, plus durable leakage-safe 80/20 train/benchmark allocation
  independently for SFT and RL paper packages.
- Added immutable pre-fetch licence decisions and folded their admitted or
  quarantined outcomes into the corpus route ledger and document audit view.
- Added an authenticated manual foundry run that freezes and drains the same
  ranked queue as the daily scheduler.
- Added append-only per-artifact human approval and rejection with reviewer
  identity, optional reason, API controls, and Post-training UI inspection.

### Changed

- Consolidated every model-authored foundry role onto Hetzner
  `Qwen3.8-27B`; removed the retired second-provider route and the artificial
  quota reserve.
- Added a deterministic lossless prompt projection for equation- and
  table-heavy papers while preserving the complete durable `PaperBundle`.
- Bounded each evidence-graph compiler pass to a prioritized incremental delta
  so dense papers cannot truncate structured JSON at the output ceiling.
- Removed provider qualification benchmarks, score thresholds, availability
  heartbeats, maximum-gap rules, and provider approval state.
- Removed copy-ratio, shared-word, and minimum-answer-length gates and metrics.
- Replaced active `broad_pretraining`, `reasoning_candidate`, and
  `benchmark_candidate` routing with `pretrain` plus optional
  `posttrain_candidate`; the 80/20 benchmark split now occurs only after SFT/RL
  artifact validation.
- Disabled optional official-artifact oracles by default and retained them as
  later work; Prometheus and Grafana remain the only observability stack.


- Made licence admission fail closed for every content format. Missing,
  excluded, and dataset-wrapper-only licences are quarantined before body
  retrieval and cannot enter pretraining export or post-training generation.
- Reimplemented the DHBW deployment as measured `platform`, `catalog`,
  `topics`, and `application` stages with Helm 3 validation and pinned charts.
- Replaced the missing local Polaris chart with official Apache Polaris 1.7.0
  and documented its in-memory demo limitation.
- Removed unmeasured production, MinIO-operator, Loki, Tempo, and Alloy
  overlays. MinIO is now an explicit external stateful prerequisite.
- Made chart values strict, disabled optional exporters and autoscaling by
  default, and added a selectable Kafka starting offset for safe smoke tests.
- Removed committed demo credential defaults and protected Terraform VMs from
  accidental destruction.
- Renamed the durable reasoning route to `posttrain_candidate`; the previous
  value remains an opt-in historical read alias.

## [0.2.0] - 2026-06-15

Fulltext, code, and seed-mixture release. Turns the v0.1 metadata-and-blog
pipeline into a real fulltext + code curator, and lands a 5-component HF
seed mixture so the per-document validity-interval novelty has material to
query on day 1 of the demo. Driven by two research reports:
`docs/research-fulltext-and-code.md` and `docs/research-seed-corpus.md`.

### Added
- **Ingest**: `ingest/arxiv_html_fetcher/` (native arXiv `/html/<id>` with
  `ar5iv.labs.arxiv.org` fallback for older papers; full paper bodies, not
  abstracts), `ingest/openreview_poller/` (OpenReview API v2 for fresh
  venues + REVIEWARENA HF dataset for backfill of ICLR / NeurIPS / ICML /
  COLM full PDFs and review text),
  `ingest/github_release_tarball_fetcher/` (per-release tarball at
  `/repos/{o}/{r}/tarball/{tag}` -> per-file `CodeFileRecord`, reusing the
  shared 5000 req/h GitHub rate-limit helper).
- **Processor**: `processor/seed_loader.py` - one-shot Bytewax Job that
  streams the 5-component HF seed mixture (peS2o cs.* + RedPajama-arxiv +
  FineWeb-Edu URL-filtered + Stack-Edu Python+ML + custom Wayback backfill)
  into `docs.normalized` as Silver records. Native publication-date columns
  populate `valid_from`.
- **Schemas**: `source_format`
  (`html`|`pdf`|`latex`|`code`|`web`|`metadata`|`review`),
  `extraction_pipeline`, `spdx_license`, `spdx_license_source` on
  `BronzeRecord`, `SilverRecord`, `GoldRecord`. New `CodeFileRecord`
  (`schemas/code.py`) for per-file code records emitted by the release
  tarball fetcher (`doc_id`, `repo_full_name`, `ref`, `path`, `language`,
  `sloc`, `license`, `raw_s3_uri`, `valid_from`, `valid_to`). All JSON
  Schema sidecars regenerated.
- **Docs**: `docs/research-fulltext-and-code.md` and
  `docs/research-seed-corpus.md`. New "Phase-1.5 fulltext + code (v0.2.0)"
  and "Seed corpus (v0.2.0)" sections in `SOURCES.md`. New "v0.2.0
  amendment" section in `RESEARCH.md`. New v0.2.0 highlights paragraph in
  `README.md`.

### Removed
- **Ingest**: `ingest/submit_api/` (manual URL submit FastAPI endpoint).
  The Helm template `templates/ingest-submit-api.yaml`, every reference in
  `values.yaml` and `networkpolicies.yaml`, the Quickstart submit-API
  invocation in `README.md`, and the k6 load profile pointed at it are all
  removed. The seed loader plus live pollers cover the v0.1 demo role
  without the abuse surface.

### Changed
- Topic catalogue: explicit decision (`schemas/topics.py`,
  `CODE_SOURCE_FORMAT`) NOT to add a fifth `docs.code` topic. Code records
  ride the existing `raw.fetched` / `docs.normalized` topics with
  `source_format='code'` and downstream operators dispatch on that column.
- `valid_from_source` on `SilverRecord` gains two values:
  `dataset_metadata` (HF seed datasets) and `release_published_at` (GitHub
  release tarballs).

### Notes
- All numerical estimates for the new sources are marked `needs-measurement`
  until benchmarked on the actual k3s cluster in Week 5.
- License detection on tarball-extracted files relies on the GitHub License
  API (`/repos/{o}/{r}/license`) plus the SPDX-permissive whitelist (MIT,
  BSD-2-Clause, BSD-3-Clause, Apache-2.0, MPL-2.0). It remains heuristic;
  v0.2.0 is still not a legal-compliance product.

## [0.1.0] - 2026-06-15

Initial public preview. This is the v0.1 reference cut described in
`RESEARCH.md`.

### Added
- **Foundation**: monorepo layout, `pyproject.toml` with `uv` workspace,
  shared Pydantic schemas (`schemas/bronze.py`, `silver.py`, `gold.py`,
  `decon.py`, `sourcefeed.py`, `topics.py`).
- **Infra**: Terraform for DHBWCloud OpenStack (3 VMs), `infra/k3s-install.sh`,
  helmfile of the dependency stack (kube-prometheus-stack, Loki, Alloy,
  Tempo, Traefik, cert-manager, KEDA, OPA Gatekeeper, MinIO, Redpanda,
  Polaris-lite).
- **Helm chart**: `charts/stream2pretrain/` with templates for every
  component, `SourceFeed` and `MixtureRecipe` CRDs, KEDA `ScaledObject`s,
  `ServiceMonitor`s, `NetworkPolicies`, Gatekeeper constraints, and a
  Grafana dashboard JSON.
- **Ingest**: `submit_api` (FastAPI), `rss_poller`, `sitemap_poller`,
  `oaipmh_poller`, `github_events`, `github_releases`, `hf_poller`. All
  share `ingest/common/` for HTTP client, hashing, MinIO writer, kafka
  producer, OTel, structured logging, rate limiting, and feed loading.
- **Processor**: Bytewax dataflows `processor/fetcher.py`,
  `processor/curate.py`, `processor/iceberg_writer.py`,
  `processor/decon_gate.py`. Operators in `processor/operators/` cover
  Resiliparse extraction, fastText langid, Gopher / C4 taggers, MinHash
  (Rensa), LSHBloom near-dup, FineWeb-Edu ONNX classifier, KenLM
  perplexity, PII regex, validity-interval enricher.
- **Storage**: MinIO buckets bronze / silver / gold / decon-attestations /
  checkpoints; Iceberg V3 tables with row lineage; Polaris-lite catalog.
- **UI**: Next.js 14 App Router app with shadcn/ui + TanStack Query,
  routes `/dashboard`, `/sources`, `/decon`, `/as-of`, `/mixtures`.
- **Mixture controller**: shadow A/B comparison primitive driven by two
  `MixtureRecipe` CRDs, perplexity-delta promotion gate.
- **Decon-Gate**: streaming 13-gram Bloom + E5-small ONNX sketch with
  Ed25519-signed per-snapshot attestations on the `decon.attest` topic;
  `scripts/decon_bisect.py` reproduces any past attestation by snapshot
  id.
- **Validity intervals**: `[valid_from, valid_to)` propagated all the way
  into the gold table; DuckDB `gold_as_of(ts)` view; `/as-of` UI.
- **Tests**: `tests/integration/test_end_to_end_dev.py`,
  `test_decon_attestation_signing.py`, `test_iceberg_as_of.py`. Component
  unit tests live alongside their packages.
- **Load**: `tests/load/k6_submit.js` ramps the submit API to 100 RPS for
  60 s with thresholds.
- **Scripts**: `scripts/seed_topics.sh`, `scripts/load_seed_feeds.sh`,
  `scripts/decon_bisect.py`, `scripts/dev_smoke.sh`.
- **Docs**: `docs/architecture.md` + `architecture.mmd`, `docs/data-model.md`,
  `docs/operations.md`, `docs/novelty.md`, `docs/threat-model.md`.

### Notes
- Throughput numbers, partition counts in prod, and Polaris token TTLs are
  marked `needs-measurement` and will be benchmarked in Week 5.
- License detection is heuristic; this release is not a legal-compliance
  product.
- Single-broker Redpanda in dev; 3-broker target documented but
  out-of-scope for the 2-worker prototype cluster.

[Unreleased]: https://github.com/stream2pretrain/stream2pretrain/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/stream2pretrain/stream2pretrain/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/stream2pretrain/stream2pretrain/releases/tag/v0.1.0
