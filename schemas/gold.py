"""Gold-tier record: the curated, mixture-ready data passport.

This is the canonical training-shard row. Every field is intended to survive
all the way into the Iceberg ``gold`` table and be queryable by DuckDB. The
``snapshot_id`` and ``_row_id`` columns are populated by the Iceberg writer
on commit and may be ``None`` while the record is still in-flight on the
``docs.curated`` Redpanda topic.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from schemas.bronze import DocId, TraceId

# Risk-tier follows the MixtureVitae / Common Pile convention:
#   1 = clean (permissive license, low PII, low contamination)
#   2 = caution (heuristic uncertainty, restricted licence, partial PII)
#   3 = drop (explicit dirty signal; should not enter training mixture)
RiskTier = Literal[1, 2, 3]
PiiFlag = Literal["email", "phone", "ssn", "credit_card", "ipv4", "ipv6", "passport"]
RejectReason = Literal[
    "language_filter",
    "gopher_filter",
    "c4_nopunc_filter",
    "near_duplicate",
    "low_quality_score",
    "high_perplexity",
    "pii_detected",
    "license_excluded",
    "decontamination_hit",
    "validity_interval_invalid",
    "minhash_backend_mismatch",
]


class GoldRecord(BaseModel):
    """Curated document plus full provenance + scoring metadata."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    doc_id: DocId
    text: str
    lang: str = Field(..., min_length=2, max_length=8)
    tokens: int = Field(..., ge=0, description="GPT-2-tokenizer token count.")

    # Quality signals.
    quality_score: float = Field(
        ..., ge=0.0, le=5.0, description="FineWeb-Edu raw classifier score."
    )
    edu_score: float = Field(
        ..., ge=0.0, le=5.0, description="Distilled-classifier educational score."
    )

    # Licence + risk.
    license: str = Field(..., description="SPDX id or 'unknown'.")
    license_source: Literal[
        "html_meta", "robots_txt", "sitemap", "license_file", "manual", "unknown"
    ]
    risk_tier: RiskTier
    pii_flags: list[PiiFlag] = Field(default_factory=list)

    # Decontamination.
    contaminated_with: list[str] = Field(
        default_factory=list,
        description="Benchmark identifiers this doc overlapped with, e.g. ['MMLU'].",
    )

    # Temporal validity.
    valid_from: datetime
    valid_to: datetime | None = None

    # Pipeline outcome.
    reject_reasons: list[RejectReason] = Field(default_factory=list)

    # Versioning - identifies the deterministic recipe this record came from.
    scoring_version: str
    classifier_revision: str
    policy_revision: str = Field(
        ..., description="Git SHA of the policy bundle, prefixed 'git:'."
    )

    # Iceberg-side identity. Populated on commit; pre-commit records carry None.
    snapshot_id: int | None = Field(default=None, ge=0)
    row_id: int | None = Field(
        default=None, ge=0, alias="_row_id", description="Iceberg V3 row lineage id."
    )

    # Tracing.
    trace_id: TraceId
