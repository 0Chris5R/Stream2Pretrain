"""Silver-tier record: normalized + tagged documents.

A silver record is the output of HTML extraction (Resiliparse), language id,
heuristic taggers (Gopher / C4), and MinHash signature compute. Near-dup
cluster membership is filled in by the LSHBloom operator downstream and may
be ``None`` for the first occurrence in a band.

v0.2.0 propagates ``source_format``, ``extraction_pipeline``, ``spdx_license``,
and ``spdx_license_source`` from the Bronze record so the silver consumer no
longer has to join back to bronze to know which extractor produced the text
or which license the source attached.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from schemas.bronze import DocId, SourceFormat, SpdxLicenseSource, TraceId

ValidFromSource = Literal[
    "http_last_modified",
    "schema_org_date_published",
    "sitemap_lastmod",
    "wayback_first_seen",
    "license_effective_date",
    "fetched_at",
    "dataset_metadata",
    "release_published_at",
]
PerplexityBucket = Literal["head", "middle", "tail"]


class SilverTags(BaseModel):
    """Heuristic tagger outputs and perplexity score."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    gopher_pass: bool = Field(..., description="Passed the Gopher quality filter.")
    c4_nopunc_pass: bool = Field(
        ..., description="Passed the C4 fraction-of-lines-without-punctuation filter."
    )
    perplexity: float = Field(
        ..., ge=0.0, description="KenLM perplexity (lower is more typical text)."
    )
    perplexity_bucket: PerplexityBucket


class SilverRecord(BaseModel):
    """Normalized, language-tagged, heuristically-scored document."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    doc_id: DocId
    url: HttpUrl
    title: str | None = Field(default=None, max_length=2048)
    text: str = Field(..., description="Extracted plain text (Resiliparse output).")
    lang: str = Field(
        ..., min_length=2, max_length=8, description="ISO 639-1/3 language code."
    )
    lang_score: float = Field(..., ge=0.0, le=1.0)
    extracted_with: str = Field(
        ..., description="Extractor identity + version, e.g. 'resiliparse-0.14'."
    )
    tags: SilverTags
    minhash_sig: bytes = Field(
        ...,
        description="Binary MinHash signature, 112 permutations packed little-endian.",
    )
    minhash_backend: str = Field(
        default="rensa",
        description=(
            "Identity of the MinHash backend that produced ``minhash_sig`` "
            "(rensa | datasketch | pure-python). Consumers MUST refuse to "
            "LSH-band a signature whose backend differs from the one their "
            "in-process MinHasher expects, because the per-permutation byte "
            "layout is not interchangeable across backends."
        ),
    )
    minhash_num_perms: int = Field(
        default=112,
        ge=1,
        description="Number of permutations packed into ``minhash_sig``.",
    )
    near_dup_cluster_id: str | None = Field(
        default=None,
        description="Set by the LSHBloom operator; None for first occurrence in band.",
    )
    valid_from: datetime = Field(
        ..., description="Inclusive start of the document's validity interval."
    )
    valid_to: datetime | None = Field(
        default=None,
        description="Exclusive end of the validity interval; None means open-ended.",
    )
    valid_from_source: ValidFromSource
    trace_id: TraceId

    # v0.2.0 classifier columns (mirrored from Bronze; kept on Silver so
    # downstream Iceberg writers do not need to re-join with the bronze topic).
    source_format: SourceFormat = Field(
        default="html",
        description="Wire shape carried forward from the Bronze record.",
    )
    extraction_pipeline: str = Field(
        default="resiliparse-0.14",
        min_length=1,
        max_length=128,
        description=(
            "Operator-chain identifier of the extractor that produced ``text``. "
            "Distinct from ``extracted_with`` so a single extractor binary can "
            "ship multiple named pipelines (e.g. 'arxiv-html-2026-06' vs "
            "'fineweb-edu-html')."
        ),
    )
    spdx_license: str | None = Field(
        default=None,
        max_length=128,
        description="OSI-list verified SPDX id, or None if not attached.",
    )
    spdx_license_source: SpdxLicenseSource = Field(
        default="unknown",
        description="Where the SPDX id was read from.",
    )
