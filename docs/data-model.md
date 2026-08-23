# Stream2Pretrain - Data Model (the data passport)

The current data plane retains raw Bronze objects and structured scientific
artifacts in MinIO, transports typed records over Redpanda, and appends every
curation outcome to an Iceberg decision table. Only accepted outcomes are also
appended to Gold. Signed decontamination attestations describe each writer
batch. The wire shape is enforced by the Pydantic models in `schemas/`.

## Tier overview

| Tier | Purpose | On-disk format | Retention | Source of truth |
|---|---|---|---|---|
| Bronze | Raw fetched bytes + metadata pointer | gzipped HTML on MinIO + `BronzeRecord` JSON on `raw.fetched` | 30 days (prod) | `schemas/bronze.py` |
| Silver | Normalised + tagged but pre-quality-filter | `docs.normalized` in Redpanda; structured artifacts in `s2p-silver` | topic retention / aligned with source | `schemas/silver.py` |
| Scientific artifact | Structured sections, tables, equations, figures, citations, OCR and extractor provenance | JSON and figure assets in MinIO | aligned with source | `schemas/scientific.py` |
| Decisions | Every accepted or rejected scored outcome plus its full signal vector | Parquet in Iceberg | indefinite | `schemas/gold.py` |
| Gold | Curated, mixture-ready training shard | Parquet in Iceberg | indefinite | `schemas/gold.py` |
| Decon attestations | Signed snapshot certificates | Parquet in Iceberg | indefinite | `schemas/decon.py` |

## Bronze tier

Field-by-field (matches `BronzeRecord`):

| Field | Type | Required | Notes |
|---|---|---|---|
| `doc_id` | `sha256:<64 hex>` | yes | sha256 of the canonical URL |
| `url` | `HttpUrl` | yes | canonicalised before hashing (lower host, sorted query, drop fragment) |
| `fetched_at` | UTC datetime | yes | wall-clock at the fetcher |
| `http_status` | int 100-599 | yes | upstream status |
| `http_last_modified` | UTC datetime | no | from `Last-Modified` response header |
| `content_type` | string | yes | MIME-only, no charset |
| `raw_html_s3_uri` | `s3://...` | yes | pointer to gzipped bytes in MinIO |
| `source_feed` | string (1-128) | yes | SourceFeed CRD name |
| `trace_id` | 32-char hex | yes | W3C trace-id |
| `etag` | string | no | for conditional GET |
| `bytes_size` | int >=0 | no | uncompressed payload size |

Bronze partitioning uses
`s3://bronze/year=YYYY/month=MM/day=DD/source=<feed>/<doc_id>.<html|pdf>.gz`.
This Hive-style layout makes per-source pruning cheap when bisecting.

## Silver tier

Adds extracted text, language, heuristic tags, and the validity interval.
Required fields above bronze:

| Field | Type | Notes |
|---|---|---|
| `title` | string \| null | extracted by Resiliparse |
| `text` | string | clean text post-extraction |
| `lang` | ISO code | from fastText lid.176 |
| `lang_score` | float 0..1 | confidence |
| `extracted_with` | string | e.g. `resiliparse-0.14` |
| `tags.gopher_pass` | bool | FineWeb/Gopher signal; a gate only for ordinary web prose |
| `tags.c4_nopunc_pass` | bool | FineWeb punctuation signal; a gate only for ordinary web prose |
| `tags.perplexity` | float | KenLM perplexity for ordinary web prose; consult scorer applicability |
| `tags.perplexity_bucket` | enum | `head` / `middle` / `tail` for an applicable KenLM result |
| `minhash_sig` | bytes (112 perms) | Rensa MinHash |
| `near_dup_cluster_id` | string \| null | populated by LSHBloom |
| `valid_from` | UTC datetime | populated by enricher (see precedence below) |
| `valid_to` | UTC datetime \| null | usually null - retraction or supersession sets it |
| `valid_from_source` | enum | precedence trail (see below) |
| `trace_id` | 32-char hex | inherited from bronze |

### Validity-interval precedence

When multiple signals disagree, the enricher resolves in this order:

1. `license_effective_date` (manual override on the SourceFeed)
2. `retraction_date` (from arXiv withdrawals, journal retractions)
3. `schema.org/datePublished` in the page HTML
4. `http_last_modified`
5. Wayback Machine first-seen timestamp
6. `fetched_at` (last resort; logged as `valid_from_source = "fetched_at"`)

The chosen signal name is recorded in `valid_from_source` so a downstream
auditor can reconstruct the decision.

Silver partitioning: `PARTITION BY lang, bucket(16, doc_id)`.

## Gold tier - the data passport

This is the canonical training-shard row. Every field is intended to survive
into the Iceberg `gold` table and be queryable by DuckDB.

| Field | Type | Required | Notes |
|---|---|---|---|
| `doc_id` | `sha256:<64 hex>` | yes | inherited |
| `text` | string | yes | post-PII-scrubbed |
| `lang` | ISO code | yes | |
| `tokens` | int >=0 | yes | GPT-2-tokenizer token count |
| `edu_score` | float 0..5 | yes | Raw source-aware quality signal: FinePDFs v2, FineWeb-Edu, Stack/Dolma code rules, or OpenReview schema completeness |
| `quality_score` | float 0..5 | yes | Explainable composite of source quality, source-appropriate structure, language, and only applicable heuristic/KenLM signals |
| `lang_score` | float 0..1 | yes | language-confidence signal |
| `gopher_pass` | bool | yes | Gopher heuristic outcome |
| `c4_*` | bool/float | yes | individual C4 outcomes and punctuation fraction |
| `perplexity`, `perplexity_bucket`, `perplexity_scorer` | float/enum/string | yes | KenLM signal and exact scorer provenance; `perplexity_scorer=not-applicable` for non-web profiles and excludes typicality from the composite |
| `near_duplicate`, `near_dup_cluster_id` | bool/string \| null | yes/no | stateful deduplication outcome |
| `minhash_backend`, `minhash_num_perms`, `lsh_backend` | string/int/string | yes | deduplication implementation provenance |
| `license` | SPDX or `unknown` | yes | |
| `license_source` | enum | yes | `html_meta`, `robots_txt`, `sitemap`, `license_file`, `manual`, `unknown` |
| `risk_tier` | 1 / 2 / 3 | yes | MixtureVitae convention |
| `pii_flags` | list[enum] | no | `email`, `phone`, `ssn`, `credit_card`, `ipv4`, `ipv6`, `passport` |
| `contaminated_with` | list[string] | no | benchmark ids matched by Decon-Gate |
| `valid_from` | UTC datetime | yes | inherited |
| `valid_to` | UTC datetime \| null | no | |
| `reject_reasons` | list[enum] | no | Includes language, web heuristics, duplicate, source quality, PII/secret, licence, decontamination, validity, incomplete extraction, and `metadata_only` blockers |
| `scoring_version` | string | yes | recipe version |
| `classifier_revision` | string | yes | e.g. `fineweb-edu-onnx-int8-2026-05-31` |
| `policy_revision` | `git:<sha>` | yes | git commit of the policy bundle |
| `snapshot_id` | int \| null | no | populated by Iceberg commit |
| `_row_id` | int \| null | no | reserved; null in the current Iceberg V2 writer |
| `trace_id` | 32-char hex | yes | inherited |
| `scientific_artifact_s3_uri` | S3 URI \| null | no | canonical structured-document artifact |
| `figure_count`, `table_count`, `equation_count`, `citation_count` | int | yes | extraction completeness counters |
| `extraction_warnings` | list[string] | no | explicit degraded-extraction evidence |

Gold partitioning: `PARTITION BY lang, risk_tier, month(valid_from)`. The
`month(valid_from)` partition makes `as_of(timestamp)` pruning cheap.

### Risk-tier convention (MixtureVitae / Common Pile)

| Tier | Meaning | Example trigger |
|---|---|---|
| 1 | trainable under current policy | allowlisted content licence, no PII flag or contamination hit |
| 2 | caution | heuristic uncertainty or a rejected code licence |
| 3 | drop | hard fail: explicit dirty signal, dropped before mixture |

Every row is published to `curation.decisions` and committed to
`gold.curation_decisions`, including the full signal vector, risk tier, PII
flags, contamination matches, and rejection reasons. Only tier 1 rows with
empty `reject_reasons`, empty `pii_flags`, and no `contaminated_with` marks are
also published to `docs.curated` and committed to `gold.curated`. The two
Iceberg appends are not a distributed transaction; current delivery is
at-least-once and replay behavior remains a runtime measurement.

The strict admission policy records missing and excluded licences before body
retrieval. Such documents never enter Bronze processing. The query layer folds
these pre-fetch records into the same corpus route ledger as downstream
curation decisions, using `license_missing` or `license_not_permitted` as the
route reason. The internal pre-fetch table is not a second product ledger. The
defensive curation rule still represents a replayed legacy row as a rejected
decision with `license=unknown`; it never enters `gold.curated` or dataset
export.

## Decon attestations

One signed row per writer batch. Its counts are calculated from the complete
decision batch and it records the accepted Gold snapshot id when one exists.

| Field | Type | Required | Notes |
|---|---|---|---|
| `snapshot_id` | int >=0 | yes | matches Iceberg gold snapshot |
| `committed_at` | UTC datetime | yes | snapshot commit time |
| `benchmark_set_version` | string | yes | e.g. `v2026-06-01` |
| `benchmarks` | list[enum] | yes | subset of `MMLU`, `GSM8K`, `HumanEval`, `MATH`, `GPQA` |
| `tokens_scanned` | int >=0 | yes | total tokens in the snapshot delta |
| `tokens_flagged` | int >=0 | yes | tokens dropped by the gate |
| `rejected_doc_hashes` | list[string] | no | sha256 doc_ids that hit |
| `per_benchmark_hits` | map[bench, int] | yes | hit counts (zero entries included) |
| `signature` | base64 | yes | Ed25519 over canonical JSON of the body |
| `signer_cert` | PEM | yes | x509 cert binding the signing key |

Canonicalisation rule (verifier-side): drop `signature` and `signer_cert`,
JSON-encode with `sort_keys=True` and `separators=(",", ":")`, UTF-8 bytes.

Attestation table partitioning: `PARTITION BY month(committed_at)`.

## Iceberg branch and tag conventions

- `main`: production curated stream.
- `shadow-<recipe>`: shadow A/B comparison branch per `MixtureRecipe` CRD.
- Tags: `attest-<snapshot_id>` is created automatically when a Decon-Gate
  attestation is committed, so a verifier can pin the table state by tag.

## Read-side conveniences

DuckDB views:

- `gold_as_of(ts)` -> rows where `valid_from <= ts AND (valid_to IS NULL OR valid_to > ts)`.
- `gold_clean` -> defensive alias for the Gold table contract:
  `WHERE risk_tier = 1 AND cardinality(pii_flags) = 0 AND cardinality(reject_reasons) = 0`.
- `gold_uncontaminated` -> defensive alias for `WHERE cardinality(contaminated_with) = 0`.

The exact SQL lives in `ui/lib/duckdb-client.ts`.
