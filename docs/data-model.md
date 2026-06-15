# Stream2Pretrain - Data Model (the data passport)

Three append-only Iceberg tables (bronze / silver / gold) plus a fourth
auxiliary table for signed decontamination attestations. This document
expands `RESEARCH.md` section 6 into a field-by-field reference. The wire
shape is enforced by the Pydantic models in `schemas/`.

## Tier overview

| Tier | Purpose | On-disk format | Retention | Source of truth |
|---|---|---|---|---|
| Bronze | Raw fetched bytes + metadata pointer | gzipped HTML on MinIO + `BronzeRecord` JSON on `raw.fetched` | 30 days (prod) | `schemas/bronze.py` |
| Silver | Normalised + tagged but pre-quality-filter | Parquet in Iceberg | 90 days | `schemas/silver.py` |
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

Bronze partitioning: `s3://bronze/year=YYYY/month=MM/day=DD/source=<feed>/<doc_id>.html.gz`.
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
| `tags.gopher_pass` | bool | Dolma Gopher rules |
| `tags.c4_nopunc_pass` | bool | C4 punctuation rule |
| `tags.perplexity` | float | KenLM perplexity |
| `tags.perplexity_bucket` | enum | `head` / `middle` / `tail` |
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
| `quality_score` | float 0..5 | yes | FineWeb-Edu raw classifier |
| `edu_score` | float 0..5 | yes | distilled-classifier educational score |
| `license` | SPDX or `unknown` | yes | |
| `license_source` | enum | yes | `html_meta`, `robots_txt`, `sitemap`, `license_file`, `manual`, `unknown` |
| `risk_tier` | 1 / 2 / 3 | yes | MixtureVitae convention |
| `pii_flags` | list[enum] | no | `email`, `phone`, `ssn`, `credit_card`, `ipv4`, `ipv6`, `passport` |
| `contaminated_with` | list[string] | no | benchmark ids matched by Decon-Gate |
| `valid_from` | UTC datetime | yes | inherited |
| `valid_to` | UTC datetime \| null | no | |
| `reject_reasons` | list[enum] | no | `language_filter`, `gopher_filter`, `c4_nopunc_filter`, `near_duplicate`, `low_quality_score`, `high_perplexity`, `pii_detected`, `license_excluded`, `decontamination_hit`, `validity_interval_invalid` |
| `scoring_version` | string | yes | recipe version |
| `classifier_revision` | string | yes | e.g. `fineweb-edu-onnx-int8-2026-05-31` |
| `policy_revision` | `git:<sha>` | yes | git commit of the policy bundle |
| `snapshot_id` | int \| null | no | populated by Iceberg commit |
| `_row_id` | int \| null | no | Iceberg V3 row lineage id |
| `trace_id` | 32-char hex | yes | inherited |

Gold partitioning: `PARTITION BY lang, risk_tier, month(valid_from)`. The
`month(valid_from)` partition makes `as_of(timestamp)` pruning cheap.

### Risk-tier convention (MixtureVitae / Common Pile)

| Tier | Meaning | Example trigger |
|---|---|---|
| 1 | clean | permissive licence, no PII flag, no contamination hit |
| 2 | caution | heuristic uncertainty: missing licence, partial PII match, ambiguous date |
| 3 | drop | hard fail: explicit dirty signal, dropped before mixture |

Tier 3 rows still land in `gold` with `reject_reasons` populated; the
mixture controller filters them out at read time so the lineage survives.

## Decon attestations

One row per Iceberg snapshot of the gold table, signed and replayable.

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
- `gold_clean` -> `WHERE risk_tier = 1 AND cardinality(pii_flags) = 0`.
- `gold_uncontaminated` -> `WHERE cardinality(contaminated_with) = 0`.

The exact SQL lives in `ui/lib/duckdb-client.ts`.
