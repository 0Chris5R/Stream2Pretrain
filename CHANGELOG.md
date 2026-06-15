# Changelog

All notable changes to Stream2Pretrain are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/stream2pretrain/stream2pretrain/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/stream2pretrain/stream2pretrain/releases/tag/v0.1.0
