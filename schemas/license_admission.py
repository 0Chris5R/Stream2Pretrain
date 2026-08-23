"""Immutable pre-fetch licence admission decisions.

Every content-bearing ingest path emits one of these records before it starts
the document fetch. The record proves that unknown or excluded licences never
reached extraction, OCR, classification, or curation.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from schemas.bronze import DocId, TraceId

LicenseAdmissionStatus = Literal["admitted", "posttrain_transform_only", "quarantined"]


class LicenseAdmissionDecision(BaseModel):
    """One fail-closed licence decision made before content retrieval."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    decision_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    doc_id: DocId
    source_feed: str = Field(min_length=1, max_length=128)
    source_url: HttpUrl
    observed_at: datetime
    status: LicenseAdmissionStatus
    license_id: str = Field(min_length=1, max_length=128)
    license_source: str = Field(min_length=1, max_length=64)
    reason: str = Field(min_length=1, max_length=512)
    trace_id: TraceId
    content_fetch_started: Literal[False] = False
