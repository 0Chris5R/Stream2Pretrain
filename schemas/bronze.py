"""Bronze-tier record: raw fetched documents prior to extraction.

The bronze tier is the system's append-only event log of "we tried to fetch
this URL at this time and got this back". The raw bytes live in MinIO under
``s3://bronze/year=YYYY/month=MM/day=DD/source=<feed>/<doc_id>.html.gz`` and
the metadata pointer is what flows on the ``raw.fetched`` Redpanda topic.

Field semantics match RESEARCH.md section 6.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

DocId = Annotated[
    str,
    Field(
        pattern=r"^sha256:[0-9a-f]{64}$",
        description="Content-addressed document id, sha256 of the canonical URL.",
    ),
]
TraceId = Annotated[
    str,
    Field(
        pattern=r"^[0-9a-f]{32}$",
        description="W3C trace-id (lowercase hex, 32 chars).",
    ),
]


class BronzeRecord(BaseModel):
    """Pointer + provenance for a raw fetched document."""

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    doc_id: DocId
    url: HttpUrl
    fetched_at: datetime = Field(
        ..., description="UTC instant the fetcher received the response."
    )
    http_status: int = Field(..., ge=100, le=599)
    http_last_modified: datetime | None = Field(
        default=None,
        description="Value of the HTTP Last-Modified response header if present.",
    )
    content_type: str = Field(..., description="MIME type, e.g. 'text/html'.")
    raw_html_s3_uri: str = Field(
        ...,
        pattern=r"^s3://[^/]+/.+",
        description="Pointer to the gzipped raw bytes in MinIO.",
    )
    source_feed: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="SourceFeed CRD name that produced this fetch.",
    )
    trace_id: TraceId
    etag: str | None = Field(
        default=None, description="HTTP ETag, used by the poller for conditional GET."
    )
    bytes_size: int | None = Field(
        default=None, ge=0, description="Size of the stored raw payload in bytes."
    )
