"""Pydantic request / response models for the submit API."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from schemas.sourcefeed import LicenseDefault


class SubmitRequest(BaseModel):
    """A single submission. Optionally overrides the license tag."""

    model_config = ConfigDict(extra="forbid")

    url: HttpUrl
    source_feed: str = Field(
        default="manual-submit",
        min_length=1,
        max_length=128,
        description=(
            "Logical SourceFeed name to attribute this submission to. Must "
            "exist in cluster (or in the YAML config in dev) - submissions "
            "to non-existent feeds are rejected so rate-limit budgets stay tied "
            "to declared sources."
        ),
    )
    license_override: LicenseDefault | None = Field(
        default=None,
        description=(
            "If set, replaces the SourceFeed's default license tag for this "
            "submission only. Useful for one-off CC-BY-4.0 documents pushed "
            "into a generic 'unknown' feed."
        ),
    )


class SubmitResponse(BaseModel):
    """Confirmation that a submission was accepted and emitted."""

    model_config = ConfigDict(extra="forbid")

    accepted: Literal[True] = True
    doc_id: str
    raw_topic: str
    bronze_uri: str
    source_feed: str
    license: LicenseDefault | None
    bytes_size: int


class HealthResponse(BaseModel):
    """``GET /healthz`` payload."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["ok"] = "ok"
    redpanda: bool
    minio: bool
    feeds_loaded: int
