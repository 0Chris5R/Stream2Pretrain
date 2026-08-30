# Stream2Pretrain research basis

Stream2Pretrain is a Kubernetes-native, streaming-first curation system for
fresh AI research data. Redpanda decouples source ingestion, normalization,
curation, lakehouse persistence, and serving. Bytewax supplies durable stream
state and replay. MinIO, Apache Iceberg, Polaris, and DuckDB provide the
auditable lakehouse. The Next.js cockpit is read-only monitoring, except for
named human approval or rejection of generated post-training artifacts.

## Current source scope

The active content sources are:

- full arXiv papers discovered through arXiv feeds and resolved to native HTML,
  with bounded CPU PDF fallback for papers without usable HTML;
- exact-revision Hugging Face model-card README text;
- exact-revision Hugging Face dataset-card README text.

Discovery messages schedule content retrieval and are not corpus documents.
Every content item receives an item-level licence decision before retained body
processing. Permissive material can enter both pretraining and post-training.
Missing or reviewed grey-area rights can only ground derived post-training
artifacts. Explicitly incompatible rights quarantine before body fetch.

## Curation research choices

Scientific papers use structure-preserving extraction, section removal,
FinePDFs Edu v2, language identification, segment-level PII handling, KenLM as
an audit signal, exact and MinHash near-duplicate detection, and explicit
reasoning/readiness features. Hugging Face cards use a source-specific Markdown
projection and structural quality policy because paper and general-web
classifiers are not calibrated admission gates for model cards.

The exact running implementation, thresholds, regular expressions, and model
prompts are documented in `docs/PIPELINE_IMPLEMENTATION_REFERENCE.md`.

## Architecture decision

The project uses a Kappa architecture: all live and replayed input follows the
same stream path. Redpanda was selected instead of the lecture-default Kafka
distribution because it preserves the Kafka API while fitting the available
cluster more comfortably. Bytewax was selected for Python-native integration
with the extraction and classifier stack and durable stateful processing.

## Product differentiators

1. Per-document validity intervals propagate into Iceberg and support
   deterministic `as_of(timestamp)` dataset selection.
2. Two `MixtureRecipe` CRDs can materialize shadow branches for future small-LM
   mixture comparison and progressive promotion. The training worker remains
   deferred until a suitable GPU budget is available.
3. The scientific-paper Foundry creates inspectable SFT trajectories and
   verifier-backed RL environments from the same durable structured artifact,
   with exact provenance and per-artifact human review.

## Honest limits

- Source-specific classifier coverage is deliberately narrower than the
  classifier catalogues in NeMo Curator and DataTrove.
- Licence resolution is conservative provenance and routing logic, not legal
  advice.
- CPU PDF extraction is the expensive stage and must be measured on the actual
  cluster before claiming a sustained throughput number.
- Shadow mixture training is designed but not yet an always-on production
  workload.
