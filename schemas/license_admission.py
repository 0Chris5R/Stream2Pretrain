"""Immutable pre-fetch licence admission decisions.

Every content-bearing ingest path emits one of these records before it starts
the document fetch. The record proves that unknown or excluded licences never
reached extraction, OCR, classification, or curation.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from schemas.bronze import DocId, SourceFormat, TraceId

LicenseAdmissionStatus = Literal["admitted", "posttrain_transform_only", "quarantined"]


class LicenseAdmissionDecision(BaseModel):
    """One fail-closed licence decision made before content retrieval."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    decision_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    doc_id: DocId
    source_feed: str = Field(min_length=1, max_length=128)
    source_url: HttpUrl
    source_format: SourceFormat | None = None
    observed_at: datetime
    status: LicenseAdmissionStatus
    license_id: str = Field(min_length=1, max_length=128)
    license_source: str = Field(min_length=1, max_length=64)
    raw_license: str | None = Field(default=None, max_length=512)
    normalized_license: str = Field(default="unknown", min_length=1, max_length=128)
    resolver: str = Field(default="legacy", min_length=1, max_length=128)
    evidence_url: HttpUrl | None = None
    evidence_revision: str | None = Field(default=None, max_length=256)
    evidence_scope: Literal[
        "item",
        "file",
        "repository_ref",
        "source_terms",
        "dataset_wrapper",
        "unknown",
    ] = "unknown"
    policy_revision: str = Field(default="license-policy-legacy", min_length=1, max_length=128)
    resolved_at: datetime | None = None
    reason: str = Field(min_length=1, max_length=512)
    trace_id: TraceId
    content_fetch_started: Literal[False] = False
