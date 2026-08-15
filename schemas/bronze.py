"""Bronze-tier record: raw fetched documents prior to extraction.

The bronze tier is the system's append-only event log of "we tried to fetch
this URL at this time and got this back". The raw bytes live in MinIO under
``s3://bronze/year=YYYY/month=MM/day=DD/source=<feed>/<doc_id>.html.gz`` and
the metadata pointer is what flows on the ``raw.fetched`` Redpanda topic.

Field semantics match RESEARCH.md section 6.

v0.2.0 adds three classifier columns shared by Bronze, Silver, and Gold:

- ``source_format`` records the wire shape the document arrived in (HTML page
  vs PDF vs LaTeX source vs code file vs raw web vs metadata-only vs review
  text). Downstream operators dispatch on this column to pick the correct
  extractor pipeline.
- ``extraction_pipeline`` is a free-form identifier of the operator chain that
  produced the record (e.g. ``"arxiv-html-2026-06"`` for the native arXiv HTML
  fetcher, ``"marker-pdf-1.5"`` for a future PDF-Markdown sidecar). Together
  with ``minhash_backend`` on the Silver record, this gives forensic operators
  enough information to reproduce any document.
- ``spdx_license`` carries the OSI-list verified SPDX id that the source
  attached to the document; ``spdx_license_source`` records where the
  classifier read it from. Both default to ``unknown`` for sources that do not
  publish a machine-readable license.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

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

SourceFormat = Literal[
    "html",
    "pdf",
    "latex",
    "code",
    "web",
    "metadata",
    "review",
]
"""Wire shape of the source document.

- ``html``: a rendered HTML page (arXiv ``/html/<id>``, ar5iv, AI-lab blog).
- ``pdf``: a binary PDF (OpenReview, ACL Anthology, conference proceedings).
- ``latex``: TeX/LaTeX source from arXiv ``s3://arxiv/src``.
- ``code``: a single source file extracted from a release tarball or
  ``the-stack-v2`` blob.
- ``web``: an opaque crawled web page (CommonCrawl-derived seeds).
- ``metadata``: an OAI-PMH or REST JSON record with no body, only metadata.
- ``review``: peer-review prose from OpenReview (separate from the paper PDF
  it discusses).
"""

SpdxLicenseSource = Literal[
    "github_api",
    "html_meta",
    "dataset_metadata",
    "manual_override",
    "unknown",
]
"""Provenance of the SPDX id attached to the document.

- ``github_api``: GitHub ``/repos/{o}/{r}/license`` response.
- ``html_meta``: ``<meta name="dc.rights">`` / ``<meta name="license">`` tag
  on the source HTML page.
- ``dataset_metadata``: per-blob attestation in a HuggingFace dataset
  (``the-stack-v2``, ``stack-edu``, etc.).
- ``manual_override``: a SourceFeed CRD ``license_default`` value applied
  because the source publishes no machine-readable license.
- ``unknown``: classifier could not determine a license. The provisional
  policy admits unknown non-code content but keeps code on the SPDX allowlist.
"""


class BronzeRecord(BaseModel):
    """Pointer + provenance for a raw fetched document."""

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    doc_id: DocId
    url: HttpUrl
    fetched_at: datetime = Field(..., description="UTC instant the fetcher received the response.")
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

    # v0.2.0 classifier columns (carried forward to Silver and Gold).
    source_format: SourceFormat = Field(
        default="html",
        description=("Wire shape of the document; downstream extractors dispatch on this."),
    )
    extraction_pipeline: str = Field(
        default="raw-fetch",
        min_length=1,
        max_length=128,
        description=(
            "Operator-chain identifier (e.g. 'arxiv-html-2026-06', "
            "'marker-pdf-1.5', 'the-stack-v2'). 'raw-fetch' for plain bytes."
        ),
    )
    spdx_license: str | None = Field(
        default=None,
        max_length=128,
        description=(
            "OSI-list verified SPDX id (e.g. 'Apache-2.0'); None when no "
            "machine-readable license was attached to the source."
        ),
    )
    spdx_license_source: SpdxLicenseSource = Field(
        default="unknown",
        description="Where the SPDX id was read from.",
    )
