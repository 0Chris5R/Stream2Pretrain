# CLAUDE.md — Working notes for the AI pair

See README.md for the high-level project description and RESEARCH.md for the full plan. This file holds decisions and context not derivable from those.

## Style

- No emojis, no em dashes (use hyphens or colons).
- Use `uv` for any Python work.
- Validate at small scale before scaling up.
- Never invent numerical values - mark unmeasured numbers as `needs-measurement`.

## Decision log

### 2026-06-15 — Project framing locked
- Use case: streaming-first LLM pretraining data curator on Kubernetes.
- Architecture: Kappa (streaming-only), per the lecture default.
- Three locked novelty differentiators (survived adversarial verification):
  1. Streaming Decon-Gate with signed per-snapshot contamination attestations.
  2. Per-document validity intervals + Iceberg `as_of(timestamp)` query view.
  3. Shadow-mode A/B mixture comparison via two `MixtureRecipe` CRDs.

### 2026-06-15 — Tech stack frozen
- Streaming bus: **Redpanda** (single-binary Kafka API; begründete Abweichung).
- Stream engine: **Bytewax** (Python, Rust core; lighter than Flink for 2 worker nodes).
- Object store: **MinIO** (lecture default).
- Table format: **Apache Iceberg V3** + **Polaris** REST catalog (Iceberg picked over Delta for vendor-neutrality and row lineage).
- HTML extraction: **Resiliparse** (DCLM-Baseline default, faster than Trafilatura).
- MinHash: **Rensa** (Rust). Near-dup index: **LSHBloom** (band-partitioned Bloom).
- Quality classifier: **FineWeb-Edu** distilled to ONNX INT8 for CPU.
- UI: **Next.js 14 App Router + shadcn/ui + TanStack Query**.
- Lakehouse query: **DuckDB + iceberg extension**.
- Submit API: **FastAPI**.
- Autoscaling: **KEDA** Kafka-lag trigger.
- Observability: lecture stack (kube-prometheus-stack + Loki + Alloy + Tempo for traces).
- Ingress + TLS: lecture stack (Traefik + cert-manager).
- Policy: **OPA Gatekeeper** for SourceFeed CRD admission.

### 2026-06-15 — Naming locked
- Final name: **Stream2Pretrain**.
- Verified free on PyPI, GitHub user/repo, Hugging Face, npm, crates.io (2026-06-15).
- Backup names if conflict ever surfaces: `Corpustide`, `Verbastream`, `Distilstream`, `Kairoscorpus` (all probed free same day).
- Earlier note in this log incorrectly said Stream2Pretrain was taken on PyPI - that was a misread; corrected.

### 2026-06-15 — Scope tightened to AI research
- Domain focus: streaming curation for fresh AI-research pretraining data.
- Phase-1 sources: arXiv OAI-PMH + 4 arXiv RSS feeds + GitHub Events (AI-filtered) + GitHub Releases Atom (~30 curated AI repos) + HF Hub models + HF Daily Papers + AI-lab blog RSS bundle. Target 5-20k docs/day.
- Phase-2 expansion: remaining arXiv categories, HF Datasets/Spaces, OpenReview, Semantic Scholar, GitHub READMEs, long-tail blogs, Alignment Forum.
- Full source catalog with rate limits in `SOURCES.md`.
- Out of scope (with reasons): GitHub Trending, YouTube transcripts, Twitter/X, Reddit, full arXiv PDFs, paid proceedings.

## Open questions to resolve before Week 1

- DHBWCloud OpenStack quota: how many vCPU / RAM / disk per VM? Sets whether 1+2 worker layout actually works for Redpanda + Bytewax + MinIO + monitoring stack.
- Wildcard TLS DNS zone available to the team (rfc2136 zone + tsig credentials per Exercise Track 1).
- Group size: assignment says "nach Vorgabe der Veranstaltung" - confirm and divide week-1..6 work accordingly.
- Final project name (see above).

## Anti-patterns to avoid

- Do not claim parity with NeMo Curator on operator surface - their classifier zoo is broader.
- Do not claim Stream2Pretrain is a legal-compliance tool - license detection is heuristic.
- Do not invent throughput numbers - measure on the actual k3s cluster in Week 5.
- Do not skip the "begründete Abweichung" justification for Redpanda+Bytewax in the README; it is required for bonus points.

## Reference

- All lecture slides in `lecture_slides/`.
- Full research output in `RESEARCH.md`.
- Workflow run id (deep research): `wf_14fc06f4-2b8`, 30 subagents, 14 candidate novelty claims, 7 survived adversarial review.

### 2026-06-15 - Implementation layout landed (v0.1.0)

The repo now contains the v0.1.0 implementation footprint described in
RESEARCH.md sections 4-8. Key paths:

- `schemas/` - shared Pydantic models for bronze, silver, gold, decon
  attestations, SourceFeed and MixtureRecipe CRDs, plus Redpanda topic
  catalogue.
- `ingest/` - pollers (rss, sitemap, oai-pmh, github events / releases,
  HF Hub) + FastAPI submit API. `ingest/common/` hosts the shared HTTP,
  S3, Kafka, OTel, rate-limit, robots, and feeds-loader libs.
- `processor/` - Bytewax dataflows (fetcher, curate, iceberg_writer,
  decon_gate, sign) plus operators in `processor/operators/`. Mixture
  controller lives in `processor/mixture_controller/`.
- `ui/` - Next.js 14 App Router UI with shadcn/ui + TanStack Query.
  Routes: `/dashboard`, `/sources`, `/decon`, `/as-of`, `/mixtures`.
  DuckDB-backed via `ui/lib/duckdb-client.ts`.
- `charts/stream2pretrain/` - single Helm chart for every component plus
  CRDs (`SourceFeed`, `MixtureRecipe`), KEDA `ScaledObject`s,
  `ServiceMonitor`s, `NetworkPolicies`, Gatekeeper constraints, Grafana
  dashboards. Lints clean.
- `infra/` - Terraform for OpenStack VMs, k3s install script, helmfile
  values for the lecture-stack dependencies (kube-prometheus-stack, Loki,
  Alloy, Tempo, Traefik, cert-manager, KEDA, Gatekeeper, MinIO, Redpanda,
  Polaris-lite).
- `tests/` - cross-component integration tests (`tests/integration/`)
  and a k6 submit-API load profile (`tests/load/`). Component unit tests
  live alongside their packages.
- `scripts/` - `seed_topics.sh`, `load_seed_feeds.sh`, `decon_bisect.py`,
  `dev_smoke.sh`.
- `docs/` - `architecture.md` + `architecture.mmd`, `data-model.md`,
  `operations.md`, `novelty.md`, `threat-model.md`.
- Top-level: `CHANGELOG.md`, `CODE_OF_CONDUCT.md`, `CONTRIBUTING.md`,
  `LICENSE` (Apache-2.0). README has Quickstart (dev) + Quickstart (k3s)
  + Repo layout sections appended.

Open items still marked `needs-measurement`: throughput on the actual k3s
cluster, prod partition counts, Polaris RBAC token TTLs, integrity-scan
cadence on MinIO bronze, and DHBWCloud quotas.
