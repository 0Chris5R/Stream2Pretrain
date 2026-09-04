# Data model

Pydantic models in [schemas](../schemas) define the event contracts.
[Generated JSON schemas](../schemas/json_schema) are deterministic serialization
contracts. Iceberg columns and Arrow conversion are defined in
[the writer](../processor/iceberg_writer.py).

## Storage and event layers

| Layer | Contents | Retention |
|---|---|---|
| Bronze | Compressed source bytes in MinIO; typed pointers on `raw.fetched` | One-day source audit window |
| Silver | Normalized text, retained sections, heuristic signals and scientific evidence on `docs.normalized` | Kafka retention; transient MinIO extraction assets have one-day retention |
| Decisions | All curation outcomes in `gold.curation_decisions` | Durable |
| Gold | Trainable records in `gold.curated`; retained scientific evidence for Foundry candidates | Durable |
| Post-training | Queue evidence, generated tasks, trajectories, verifier packages and artifact audits | Durable |

The per-item pre-fetch licence decision is a separate internal event contract,
folded into the same corpus route view by serving. Discovery events are not
corpus records. Explicit incompatible rights produce a quarantine record
without fetching the source body.

[Storage ownership](storage-scaling.md) defines physical cleanup boundaries.
Neither the pretraining corpus nor its decision history has a one-day expiry.

## Identity and provenance

`doc_id` identifies a source revision with a SHA-256 digest. Canonical URL
identity is sufficient for immutable papers; HF cards bind the exact README
blob, so weight-only commits do not create new card content.

Bronze carries source, URL, HTTP status, content type, fetch time, object URI,
source format, extraction pipeline, item licence and licence provenance.
`training_usage` preserves the purpose boundary: both uses, derived
post-training only, or quarantine.

Silver adds normalized text, title, language/confidence, full retained sections,
MinHash signatures, structured scientific evidence and validity intervals.
Each section has a stable ID, title, role and text. Scientific artifacts retain
tables, equations, figures/captions, citations, OCR and extraction provenance.

## Gold fields

| Field group | Meaning |
|---|---|
| `text`, `tokens`, `tokenizer_revision` | Training projection and reproducible token count; current runtime uses cl100k_base |
| `edu_score`, `quality_diagnostics` | Source-specific quality output, four-head section diagnostics, probabilities, confidence and model provenance |
| `quality_score`, `structural_quality_score`, `extraction_completeness` | Composite score and its structural inputs |
| `segment_scores`, inclusion/exclusion counts | Section decisions and extraction coverage |
| `route`, `eligible_routes`, `reject_reasons`, `route_reasons` | Purpose-specific outcome and reasons |
| `content_tags`, `reasoning_score` | Content annotations and heuristic task suitability |
| `lang*`, `gopher_*`, `c4_*`, `perplexity*` | Language and source-applicable heuristic signals |
| `pii_*`, `metadata_pii_flags`, `removed_body_pii_flags` | Privacy findings, redaction and scanner provenance |
| `near_duplicate`, cluster and backend fields | Durable near-duplicate decision and implementation |
| `license*`, `spdx_license*`, `training_usage` | Exact licence identifier, provenance and allowed purpose |
| `scoring_version`, `classifier_revision`, `policy_revision` | Reproducible processing identity |
| `valid_from`, `valid_to` | Half-open validity interval |
| `scientific_artifact_s3_uri`, figure/table/equation counts | Retained structured evidence |
| `snapshot_id`, `_row_id`, `trace_id` | Storage and tracing identity; row lineage is reserved and null in the V2 writer |

[Scoring and routing](SCORING_AND_ROUTING.md) defines score formulas and gates.
A skipped model is not a measured zero-quality prediction: inspect backend and
diagnostic status as well as its numeric wire field.

## Validity

The generic enricher chooses the first available value from HTTP
Last-Modified, schema.org publication date, sitemap lastmod, optional archive
lookup, licence-effective date and finally fetch time. Source adapters can
supply stronger publication metadata. The selected origin is retained on
Silver. A supplied valid retraction date closes the interval; no date means an
open upper bound. This is a temporal data model, not an automatic retraction
monitor for every publisher.

The as-of predicate is
`valid_from <= timestamp AND (valid_to IS NULL OR valid_to > timestamp)`.

## Persistence and serving

Both Gold tables partition by language, risk tier and month of `valid_from`.
Silver is an event and extraction layer, not an additional Iceberg table.

Every curation outcome goes to the decisions topic and table. Gold additionally
requires risk tier 1, a trainable route, no reject reasons and no unresolved PII
flags. Verbatim exports independently enforce pretraining licence eligibility.

The writer appends decisions and curated rows separately. Delivery is
at-least-once, with deterministic replay keys and latest-per-document serving,
not a distributed exactly-once transaction.

[The serving index](../processor/serving_index.py) persists latest document
state and aggregate deltas. [DuckDB API](../processor/duckdb_api.py) serves
paginated documents, full-corpus totals, as-of queries and dataset exports.
Policy revision is audit metadata, not an implicit filter on corpus totals.

## Mixture branches

The mixture controller keeps a `main` corpus view and recipe-specific shadow
branches. The comparison interface is retained; proxy-LM training remains the
N3 scaffold described in [novelty](novelty.md).
