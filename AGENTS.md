# AGENTS.md - Working notes for the AI pair

See README.md for the high-level project description and RESEARCH.md for the full plan. This file holds decisions and context not derivable from those.

## Style

- No emojis, no em dashes (use hyphens or colons).
- Use `uv` for any Python work.
- Validate at small scale before scaling up.
- Never invent numerical values - mark unmeasured numbers as `needs-measurement`.

## Decision log

### 2026-08-23 - Pipeline remediation contract locked

- `docs/PIPELINE_REMEDIATION_CONTRACT.md` is the binding source of truth for
  remediation of `fix/deployment-pipeline`.
- Licence admission is purpose-aware but always per individual content item:
  permissive items may enter pretraining and post-training, while reviewed
  grey-area items and missing item rights may only ground derived post-training
  artifacts. Explicit incompatible rights quarantine before body fetch.
- Source defaults, hosting platforms, and dataset wrapper licences never replace
  item-level licence resolution.
- Bytewax remains the production stream engine. Deployment recovery changes
  must preserve coverage and at-least-once behavior without silently replacing
  the frozen architecture.
- Source-aware quality policies require current primary-source research and
  explicit model, revision, licence, projection, threshold, and CPU-runtime
  documentation.
- The Sources cockpit covers every configured source workload, not only
  SourceFeed CRDs, and reports observed item-level licence outcomes.
- Storage maintenance remains configurable for the constrained cluster.
- The Foundry is complete only after live SFT and RL acceptance, inspectability,
  human audit, and benchmark-split behavior are verified.

### 2026-08-19 - Post-training foundry contract locked

- `docs/POSTTRAIN_FOUNDRY.md` is the binding source of truth for the
  scientific-paper to SFT and RL environment foundry.
- The project keeps the Stream2Pretrain name until the live acceptance gate is
  complete. A Stream2Train rename is not implied by implementation alone.
- Hetzner Experiments Inference is the sole provider and `Qwen3.8-27B` is the
  sole model for every model-authored role. Exact catalogue discovery, licence
  records, and exact returned-model logging are mandatory.
- `posttrain_candidate` replaces `reasoning_candidate`; the old value is a
  historical read alias and is not accepted by the worker unless explicitly
  enabled.
- Every pretraining and post-training content path uses the per-item
  purpose-aware licence contract in `docs/PIPELINE_REMEDIATION_CONTRACT.md`.
  Grey-area or unlicensed content may be fetched only for derived
  post-training generation; explicit incompatible items quarantine.
- Provider qualification benchmarks, score thresholds, availability
  heartbeats, maximum-gap rules, and provider approval are deliberately absent.
- Human approval and rejection apply per generated SFT/RL artifact. The
  reviewer is entered manually at audit time and every audit record is retained.
- Copy-ratio, shared-word, and minimum-answer-length limits are deliberately
  absent from gates, diagnostics, and logs.
- Official-artifact oracles and W&B are disabled future work. No live provider
  credentials or output fixtures may be committed. Replay is regression
  evidence only.

### 2026-08-15 - Curation product execution contract locked

- `docs/CURATION_PRODUCT_EXECUTION_PLAN.md` is the binding source of truth for
  the post-pilot scoring, OCR, benchmark-safety, source-aware classifier, UI,
  export, validation, and Kubernetes-capacity work.
- Do not silently reduce, omit, proxy, or relabel its requirements because an
  implementation is expensive. Scope changes require an explicit team decision
  recorded in that file.
- FinePDFs Edu v2 is the expected scientific-quality default, subject to the
  recorded same-sample comparison with FineWeb-Edu.
- The normal UI must be concise and self-explanatory. Detailed limitations and
  implementation exposition live in documentation or collapsed audit views,
  not as repeated page prose.
- N3 shadow-mode mixture training is the only explicitly deferred feature.

### 2026-06-15 - Project framing locked
- Use case: streaming-first LLM pretraining data curator on Kubernetes.
- Architecture: Kappa (streaming-only), per the lecture default.
- Three locked novelty differentiators (survived adversarial verification):
  1. Streaming Decon-Gate with signed per-snapshot contamination attestations.
  2. Per-document validity intervals + Iceberg `as_of(timestamp)` query view.
  3. Shadow-mode A/B mixture comparison via two `MixtureRecipe` CRDs.

### 2026-06-15 - Tech stack frozen
- Streaming bus: **Redpanda** (single-binary Kafka API; begründete Abweichung).
- Stream engine: **Bytewax** (Python, Rust core; lighter than Flink for 2 worker nodes).
- Object store: **MinIO** (lecture default).
- Table format: **Apache Iceberg V3** + **Polaris** REST catalog (Iceberg picked over Delta for vendor-neutrality and row lineage).
- HTML extraction: **Resiliparse** (DCLM-Baseline default, faster than Trafilatura).
- MinHash: **Rensa** (Rust). Near-dup index: **LSHBloom** (band-partitioned Bloom).
- Quality classifier: **FineWeb-Edu** distilled to ONNX INT8 for CPU.
- UI: **Next.js 14 App Router + shadcn/ui + TanStack Query**.
- Lakehouse query: **DuckDB + iceberg extension**.
- Submit API: **FastAPI** (in v0.1.0 only; removed in v0.2.0 - see decision log).
- Autoscaling: **KEDA** Kafka-lag trigger.
- Observability: lecture stack (kube-prometheus-stack + Loki + Alloy + Tempo for traces).
- Ingress + TLS: lecture stack (Traefik + cert-manager).
- Policy: **OPA Gatekeeper** for SourceFeed CRD admission.

### 2026-06-15 - Naming locked
- Final name: **Stream2Pretrain**.
- Verified free on PyPI, GitHub user/repo, Hugging Face, npm, crates.io (2026-06-15).
- Backup names if conflict ever surfaces: `Corpustide`, `Verbastream`, `Distilstream`, `Kairoscorpus` (all probed free same day).

### 2026-08-25 - Live source catalogue simplified

- Discovery envelopes are internal scheduling events and never appear as
  corpus sources, documents, acceptances, or quarantines.
- Active content sources are arXiv papers, curated GitHub release files and
  docs, Hugging Face model cards, and Hugging Face dataset cards.
- OpenReview live is licence-suitable but remains absent from the active
  catalogue until live API access is configured and verified.
- GitHub Events, HF Daily Papers, HF Spaces, all five blog feeds, and every
  historical seed/backfill workload were removed.
- Licence admission has exactly three tiers: permissive -> both routes; grey
  area or missing rights -> posttrain-only; explicit incompatible rights ->
  quarantine.
- The project owner explicitly approved discarding the historical stream
  backlog and restarting each consumer at the current live frontier before a
  fresh throughput measurement.

### 2026-06-15 - Scope tightened to AI research (historical)
- Domain focus: streaming curation for fresh AI-research pretraining data.
- Phase-1 sources: arXiv OAI-PMH + 4 arXiv RSS feeds + GitHub Events (AI-filtered) + GitHub Releases Atom (~30 curated AI repos) + HF Hub models + HF Daily Papers + AI-lab blog RSS bundle. Target 5-20k docs/day.
- Phase-2 expansion: remaining arXiv categories, HF Datasets/Spaces, OpenReview, Semantic Scholar, GitHub READMEs, long-tail blogs, Alignment Forum.
- Full source catalog with rate limits in `SOURCES.md`.
- Out of scope (with reasons): GitHub Trending, YouTube transcripts, Twitter/X, Reddit, full arXiv PDFs, paid proceedings.

## Open questions to resolve before deploying

These are the v0.2.0 deploy-blockers documented in the README "What you must
provide" section. Tracked here for the AI pair.

- DHBWCloud OpenStack quota: how many vCPU / RAM / disk per VM? Sets whether 1+2 worker layout actually works for Redpanda + Bytewax + MinIO + monitoring stack. The Terraform defaults assume `m1.large` and `ext-net` external network - verify and override in `terraform.tfvars`.
- Wildcard TLS DNS zone available to the team (rfc2136 zone + tsig credentials per Exercise Track 1). Replace `stream2pretrain.example.org` placeholders.
- The five Kubernetes Secrets enumerated in the README before `helm install`.
- Container image builds for the 12 component images. Build commands per component are in `charts/stream2pretrain/README.md`.

## Open TODOs from v0.2.0 adversarial review

- **Resolved (2026-08-23): KEDA self-amplification on
  `ingest-github-tarball-fetcher`.** The shared document topics remain stable:
  extracted code still uses `raw.fetched`. GitHub release metadata is also
  dispatched to the bounded `github.release.jobs` control topic, and the
  tarball worker scales from that topic's consumer lag. New raw metadata is
  marked so the migration subscription does not fetch the same tarball twice.
  Four partitions bound useful replicas; per-pod request budgets keep the
  maximum aggregate GitHub REST rate below the shared account limit.

- **TODO (v0.3.0): SeedDocument.extra metadata propagation.**
  Per-component loaders populate `SeedDocument.extra` with `pes2o_version`,
  `redpajama_config`, `wayback_timestamp`, `feed_name`, `repository_name`,
  `fineweb_edu_score`, `language` but `to_silver()` drops the map and
  `SilverRecord` has no general-purpose extras column. v0.2.0 mitigation:
  the docstring on `SeedDocument.extra` and the README seed-loader notes
  document the drop honestly so operators do not rely on these tags. Real
  fix: extend `SilverRecord` with `Optional[Dict[str, str]] extra` plus the
  matching Iceberg V3 nested column on Silver and Gold, then propagate
  `doc.extra` into it inside `to_silver()`.

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

### 2026-06-15 - v0.2.0 scope change

- **Drop the manual URL submit endpoint.** `ingest/submit_api/`, the Helm
  template `templates/ingest-submit-api.yaml`, every reference in
  `values.yaml`, `networkpolicies.yaml`, README, and docs go away. Manual
  submit was a v0.1 demo affordance with no Phase-2 path; live pollers plus
  the new seed loader cover all demo needs without the abuse surface.
- **Add three fulltext / code ingest modules**:
  `ingest/arxiv_html_fetcher/` (native arXiv `/html/<id>` with
  `ar5iv.labs.arxiv.org` fallback for older papers - full paper bodies, not
  abstracts); `ingest/openreview_poller/` (OpenReview API v2 for fresh
  venues plus the REVIEWARENA HuggingFace dataset for backfill of ICLR /
  NeurIPS / ICML / COLM full PDFs + review text);
  `ingest/github_release_tarball_fetcher/` (per-release tarball at
  `/repos/{o}/{r}/tarball/{tag}` -> per-file `CodeFileRecord`, reusing the
  existing 5000 req/h rate-limit helper).
- **Add `processor/seed_loader.py`** - one-shot Bytewax Job that streams the
  5-component HF seed mixture (peS2o cs.* + RedPajama-arxiv +
  FineWeb-Edu URL-filtered + Stack-Edu Python+ML + custom Wayback backfill)
  directly into `docs.normalized` as Silver records. Native publication-date
  metadata populates `valid_from` so the N2 validity-interval novelty has
  data on day 1.
- **Schema additions**: `source_format`
  (html | pdf | latex | code | web | metadata | review),
  `extraction_pipeline`, `spdx_license`, `spdx_license_source` on
  `BronzeRecord` / `SilverRecord` / `GoldRecord`. New `CodeFileRecord` model
  for per-file code records emitted by the release tarball fetcher
  (`doc_id`, `repo_full_name`, `ref`, `path`, `language`, `sloc`, `license`,
  `raw_s3_uri`, `valid_from`, `valid_to`).
- **Topic decision**: do NOT add `docs.code`. Reuse `docs.normalized` (and
  `raw.fetched`) and dispatch on `source_format == 'code'`. The 4-topic
  Redpanda contract stays stable across v0.1 -> v0.2 and the existing KEDA
  scalers do not need a refactor.
- **Source documents**: `docs/research-fulltext-and-code.md` (mid-2026
  verified channels for arXiv HTML, OpenReview, GitHub tarballs) and
  `docs/research-seed-corpus.md` (5-component seed mixture sizing + license
  matrix).
- **Primary arXiv fulltext channel**: native arXiv HTML at `/html/<id>`
  (~97% coverage, ~75% LaTeXML-clean). `ar5iv.labs.arxiv.org` fallback for
  older papers. LaTeX bulk + PDF parsing both deferred (PDF parsing
  requires a GPU node, not feasible on the 2-worker cluster).
- **Code primary path**: GitHub release tarballs from the existing 30-repo
  allowlist (~720 req/day inside the 5000 req/h authed PAT budget). Do not
  crawl GitHub at scale. The Stack v2 backfill is a Phase-2 candidate.
- **SPDX whitelist for code Apache-2.0 release**: MIT, BSD-2-Clause,
  BSD-3-Clause, Apache-2.0, MPL-2.0. License detection remains heuristic;
  this is not a legal-compliance product.

### 2026-06-15 - Documentation refresh

- README.md rewritten end-to-end. The earlier framing (lines 5-17 / 27-37 /
  43-51 of the old README) still claimed "research and planning phase, no
  application code". The current README reflects v0.2.0 actual state: full
  ingest + processor + UI + chart + infra inventory, three locked novelty
  paragraphs, repo layout matching what is on disk, dev + k3s quickstarts,
  the demo story updated to drop the manual /submit step (replaced by a
  known-MMLU SourceFeed) and to note that the seed loader pre-populates
  day-zero data, an explicit "What you must provide" section consolidating
  the v0.1 + v0.2 deploy-blockers, the Redpanda + Bytewax begründete
  Abweichung paragraph for bonus-point eligibility, and a documentation
  index linking every doc.
- AGENTS.md (this file) lightly trimmed: stale "Week 1" framing removed
  from open-questions section, FastAPI submit-API stack note flagged as
  v0.1-only, this entry added.
- Other docs (RESEARCH.md, SOURCES.md, CHANGELOG.md, docs/* and per-
  component READMEs) deliberately not refreshed in this pass - they were
  written by the v0.1 / v0.2 implementation workflows and are still
  internally consistent. Touch them only when content actually changes.

### 2026-08-19 - Strict content-license admission policy

Superseded on 2026-08-25 by the per-item purpose-aware policy in
`docs/PIPELINE_REMEDIATION_CONTRACT.md`. The immutable pre-fetch decision and
item-level provenance requirements below remain in force.

- Every content-bearing source emits an immutable `license.admissions` record
  before it may retrieve or process the document body. Publication failure
  fails the ingest attempt closed.
- Missing or grey-area rights enter only the derived post-training route and
  are excluded from verbatim pretraining. Explicit incompatible or
  no-derivatives items quarantine before Bronze storage and processing.
- Dataset wrapper licences such as ODC-By do not establish rights in each
  contained paper, page, or code file. Only per-record content licences or an
  explicitly audited source-wide content licence qualify.
- The corpus routes ledger records permissive, transform-only, and quarantined
  decisions. Gold and dataset export retain the exact licence identifier and
  provenance.
- This is a conservative curation and provenance policy, not a legal-compliance
  determination.

### 2026-08-16 - Authoritative 2026 assignment requirements

- Source: `Cloud Computing und Big Data - Prüfungsleistung 2026 - Datengetriebener Prototyp auf Kubernetes-Basis`, provided by the project team on 2026-08-16.
- The submission is one ZIP file. No external files or running deployment are inspected during grading.
- `README.md` is the only report document and must contain all twelve required sections:
  1. Use case and motivation.
  2. Data characteristics and relevant Big Data V's.
  3. Kappa or Lambda architecture decision with justification and diagram.
  4. Components, technology choices, and end-to-end data flow.
  5. Processing logic, including transformation, windowing or state, and late data.
  6. Storage format, partitioning, schema, and lakehouse justification.
  7. User-facing UI role, real pipeline connection, and operating flow.
  8. Kubernetes workloads, configuration, persistence, and horizontal scaling.
  9. Reproducible deployment instructions and prerequisites.
  10. Links to central ingestion, processing, UI, and manifest code locations with explanations.
  11. Embedded screenshots of the running UI, pods, serving output, and example pipeline output.
  12. Honest prototype limits and outlook.
- The prototype must implement ingestion, non-trivial stream processing, lakehouse storage, serving, and a real containerized UI on Kubernetes.
- Every component must be designed for horizontal scaling and the scaling behavior must be shown.
- The README must explain the team contribution split. Git history must remain plausible.
- Bonus claims require a clear benefit. Extra work without recognizable value earns no bonus.
- The final README must be concise, use direct prose, and avoid excessive parentheses, semicolons, and long dashes.
