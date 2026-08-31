"""Gold-tier record: the curated, mixture-ready data passport.

This is the canonical training-shard row. Every field is intended to survive
all the way into the Iceberg ``gold`` table and be queryable by DuckDB. The
``snapshot_id`` and ``_row_id`` columns are populated by the Iceberg writer
on commit and may be ``None`` while the record is still in-flight on the
``docs.curated`` Redpanda topic.

v0.2.0 propagates ``source_format``, ``extraction_pipeline``, ``spdx_license``,
``spdx_license_source`` from the Silver record. The ``license`` and
``license_source`` columns from v0.1 stay for backwards compatibility, but
new writers SHOULD populate ``spdx_license`` (the canonical OSI-validated id)
and let the legacy fields mirror it on commit.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from schemas.bronze import DocId, SourceFormat, SpdxLicenseSource, TraceId, TrainingUsage

# Risk-tier follows the MixtureVitae / Common Pile convention:
#   1 = trainable under current policy (explicit allowlisted content licence,
#       low PII)
#   2 = caution (heuristic uncertainty, restricted licence, partial PII)
#   3 = drop (explicit dirty signal; should not enter training mixture)
RiskTier = Literal[1, 2, 3]
PiiFlag = Literal[
    "email",
    "phone",
    "ssn",
    "credit_card",
    "iban",
    "ipv4",
    "ipv6",
    "passport",
    "secret",
]
CorpusRoute = Literal[
    "pretrain",
    # Read compatibility for snapshots written before the route was renamed.
    "broad_pretraining",
    "posttrain_candidate",
    # Read compatibility for snapshots written before the foundry landed.
    "reasoning_candidate",
    "quarantine",
    "retry",
]
RejectReason = Literal[
    "metadata_only",
    "language_filter",
    "gopher_filter",
    "c4_nopunc_filter",
    "near_duplicate",
    "low_quality_score",
    "high_perplexity",
    "pii_detected",
    "license_excluded",
    "validity_interval_invalid",
    "minhash_backend_mismatch",
    "insufficient_body",
    "insufficient_scientific_body",
    "incomplete_scientific_extraction",
    "document_template",
    "hf_card_quality_filter",
]


class SegmentScore(BaseModel):
    """Per-section model signals retained for explainability."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    segment_id: str
    title: str
    role: str
    word_count: int = Field(..., ge=0)
    edu_score: float | None = Field(
        default=None,
        ge=0.0,
        le=5.0,
        description="Primary source-aware educational-quality model output.",
    )
    finepdfs_edu_score: float | None = Field(default=None, ge=0.0, le=5.0)
    quality_classifier_revision: str | None = None
    perplexity: float | None = Field(default=None, ge=0.0)
    perplexity_bucket: Literal["head", "middle", "tail"] | None = None
    c4_pass: bool = True
    pii_flags: list[PiiFlag] = Field(default_factory=list)
    decision: Literal["included", "excluded"] = "included"
    exclusion_reasons: list[str] = Field(default_factory=list)


class GoldRecord(BaseModel):
    """Curated document plus full provenance + scoring metadata."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    doc_id: DocId
    text: str
    lang: str = Field(..., min_length=2, max_length=8)
    tokens: int = Field(..., ge=0, description="GPT-2-tokenizer token count.")

    # Quality signals.
    quality_score: float = Field(
        ...,
        ge=0.0,
        le=5.0,
        description="Explainable composite corpus-quality score; not a model output.",
    )
    edu_score: float = Field(
        ...,
        ge=0.0,
        le=5.0,
        description=(
            "FinePDFs Edu v2 educational-quality output for every trainable source family."
        ),
    )
    structural_quality_score: float = Field(default=0.0, ge=0.0, le=5.0)
    extraction_completeness: float = Field(default=0.0, ge=0.0, le=1.0)
    reasoning_score: float = Field(default=0.0, ge=0.0, le=1.0)
    route: CorpusRoute = "quarantine"
    eligible_routes: list[CorpusRoute] = Field(default_factory=list)
    route_reasons: list[str] = Field(default_factory=list)
    content_tags: list[str] = Field(default_factory=list)
    segment_scores: list[SegmentScore] = Field(default_factory=list)
    projection_version: str = "document-v1"
    source_word_count: int = Field(default=0, ge=0)
    training_word_count: int = Field(default=0, ge=0)
    included_section_count: int = Field(default=0, ge=0)
    excluded_section_count: int = Field(default=0, ge=0)
    excluded_sections: list[str] = Field(default_factory=list)
    lang_score: float = Field(default=0.0, ge=0.0, le=1.0)
    lang_detector_revision: str = "unknown"
    tokenizer_revision: str = "unknown"
    gopher_pass: bool = True
    gopher_word_count: int = Field(default=0, ge=0)
    gopher_mean_word_len: float = Field(default=0.0, ge=0.0)
    gopher_stopword_ratio: float = Field(default=0.0, ge=0.0, le=1.0)
    gopher_bullet_line_ratio: float = Field(default=0.0, ge=0.0, le=1.0)
    gopher_ellipsis_line_ratio: float = Field(default=0.0, ge=0.0, le=1.0)
    gopher_symbol_word_ratio: float = Field(default=0.0, ge=0.0, le=1.0)
    gopher_alpha_word_ratio: float = Field(default=0.0, ge=0.0, le=1.0)
    c4_nopunc_pass: bool = True
    c4_curly_brace_pass: bool = True
    c4_lorem_ipsum_pass: bool = True
    c4_fraction_lines_with_punct: float = Field(default=1.0, ge=0.0, le=1.0)
    perplexity: float = Field(default=0.0, ge=0.0)
    perplexity_bucket: Literal["head", "middle", "tail"] = "head"
    perplexity_scorer: str = "unknown"
    near_duplicate: bool = False
    near_dup_cluster_id: str | None = None
    minhash_backend: str = "unknown"
    minhash_num_perms: int = Field(default=0, ge=0)
    lsh_backend: str = "unknown"

    # Licence + risk.
    license: str = Field(..., description="SPDX id or 'unknown'.")
    license_source: Literal[
        "html_meta", "robots_txt", "sitemap", "license_file", "manual", "unknown"
    ]
    risk_tier: RiskTier
    pii_flags: list[PiiFlag] = Field(default_factory=list)
    metadata_pii_flags: list[PiiFlag] = Field(default_factory=list)
    removed_body_pii_flags: list[PiiFlag] = Field(default_factory=list)
    pii_action: Literal[
        "none",
        "metadata_removed",
        "body_redacted",
        "segments_removed",
        "body_quarantine",
    ] = "none"
    pii_scanner_revision: str = "regex-only"

    # Temporal validity.
    valid_from: datetime
    valid_to: datetime | None = None

    # Pipeline outcome.
    reject_reasons: list[RejectReason] = Field(default_factory=list)

    # Versioning - identifies the deterministic recipe this record came from.
    scoring_version: str
    classifier_revision: str
    classifier_backend: str = "unknown"
    policy_revision: str = Field(..., description="Git SHA of the policy bundle, prefixed 'git:'.")

    # Iceberg-side identity. Populated on commit; pre-commit records carry None.
    snapshot_id: int | None = Field(default=None, ge=0)
    row_id: int | None = Field(
        default=None,
        ge=0,
        alias="_row_id",
        description="Reserved row-lineage id; null in the current Iceberg V2 writer.",
    )

    # Tracing.
    trace_id: TraceId
    source_feed: str = Field(
        default="unknown",
        min_length=1,
        max_length=128,
        description="SourceFeed CRD name propagated from Bronze/Silver.",
    )

    # v0.2.0 classifier columns. Mirrored forward from Silver so a single
    # ``SELECT * FROM gold`` carries the full provenance chain without joins.
    source_format: SourceFormat = Field(
        default="html",
        description="Wire shape carried forward from Bronze/Silver.",
    )
    extraction_pipeline: str = Field(
        default="resiliparse-0.14",
        min_length=1,
        max_length=128,
        description="Operator-chain identifier carried forward from Silver.",
    )
    spdx_license: str | None = Field(
        default=None,
        max_length=128,
        description=(
            "OSI-list verified SPDX id; the canonical license column for "
            "Apache-2.0-release filtering. Mirrors ``license`` for v0.1 "
            "writers; new writers populate this directly."
        ),
    )
    spdx_license_source: SpdxLicenseSource = Field(
        default="unknown",
        description="Where the SPDX id was read from.",
    )
    training_usage: TrainingUsage = Field(
        default="pretrain_and_posttrain",
        description="Purpose boundary inherited from the pre-fetch item licence decision.",
    )

    scientific_artifact_s3_uri: str | None = Field(
        default=None,
        pattern=r"^s3://[^/]+/.+",
        description="Structured scientific-document artifact retained in MinIO.",
    )
    figure_count: int = Field(default=0, ge=0)
    table_count: int = Field(default=0, ge=0)
    equation_count: int = Field(default=0, ge=0)
    citation_count: int = Field(default=0, ge=0)
    extraction_warnings: list[str] = Field(default_factory=list)
