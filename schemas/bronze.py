"""Bronze-tier record: raw fetched documents prior to extraction.

The bronze tier is the system's append-only event log of "we tried to fetch
this URL at this time and got this back". The raw bytes live in MinIO under
``s3://bronze/year=YYYY/month=MM/day=DD/source=<feed>/<doc_id>.html.gz`` and
the metadata pointer is what flows on the ``raw.fetched`` Redpanda topic.

Field semantics match RESEARCH.md section 6.

Bronze, Silver and Gold share source provenance:

- ``source_format`` records the wire shape the document arrived in (HTML page
  vs PDF vs LaTeX source vs raw web vs metadata-only
  text). Downstream operators dispatch on this column to pick the correct
  extractor pipeline.
- ``extraction_pipeline`` is a free-form identifier of the operator chain that
  produced the record. Together
  with ``minhash_backend`` on the Silver record, this gives forensic operators
  enough information to reproduce any document.
- ``spdx_license`` carries the item-level licence identifier that the source
  attached to the document; ``spdx_license_source`` records where the
  resolver read it from. Missing identifiers are ``None`` with provenance
  ``unknown``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

DocId = Annotated[
    str,
    Field(
        pattern=r"^sha256:[0-9a-f]{64}$",
        description=(
            "Stable document-revision id. Usually the sha256 of the canonical URL; "
            "exact content projections may bind an immutable source object."
        ),
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
    "markdown",
    "web",
    "metadata",
]
"""Wire shape of the source document.

- ``html``: a rendered HTML page, such as arXiv ``/html/<id>``.
- ``pdf``: a binary paper PDF.
- ``latex``: TeX/LaTeX source.
- ``markdown``: structured Markdown text, including HF cards.
- ``web``: an ordinary web-prose document.
- ``metadata``: an OAI-PMH or REST JSON record with no body, only metadata.
"""

SpdxLicenseSource = Literal[
    "file_header",
    "http_link",
    "html_meta",
    "rss_entry",
    "oai_metadata",
    "arxiv_api",
    "hf_card",
    "archived_page",
    "source_terms",
    "dataset_metadata",
    "manual_override",
    "unknown",
]
"""Provenance of the SPDX id attached to the document.

- ``file_header``: a licence identifier attached to the individual file.
- ``http_link``: an RFC 8288 ``Link`` response header with ``rel=license``.
- ``html_meta``: ``<meta name="dc.rights">`` / ``<meta name="license">`` tag
  on the source HTML page.
- ``dataset_metadata``: an item-level attestation in a dataset record.
- ``manual_override``: explicit synthetic-fixture or audited administrative
  provenance. Normal SourceFeed CRDs accept only ``per-record`` resolution and
  cannot use this value as a source-wide default.
- ``unknown``: no machine-readable licence was available.
"""

TrainingUsage = Literal[
    "pretrain_and_posttrain",
    "posttrain_transform_only",
]
"""Purpose boundary attached before a source body is fetched.

``posttrain_transform_only`` content may be used as grounded input for
derived SFT/RL generation, but must never be selected by a verbatim
pretraining export.
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

    # Source provenance (carried forward to Silver and Gold).
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
            "Item-level licence identifier (e.g. 'Apache-2.0'); None when no "
            "machine-readable license was attached to the source."
        ),
    )
    spdx_license_source: SpdxLicenseSource = Field(
        default="unknown",
        description="Where the SPDX id was read from.",
    )
    training_usage: TrainingUsage = Field(
        default="pretrain_and_posttrain",
        description="Permitted downstream training purpose for this fetched body.",
    )
