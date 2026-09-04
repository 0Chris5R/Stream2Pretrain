"""Silver-tier record: normalized + tagged documents.

A silver record is the output of HTML extraction (Resiliparse), language id,
heuristic taggers (Gopher / C4), and MinHash signature compute. Near-dup
cluster membership is filled in by the LSHBloom operator downstream and may
be ``None`` for the first occurrence in a band.

Source format, extraction provenance and item-level licence evidence propagate
from Bronze, so consumers can interpret the record without fetching raw bytes.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from schemas.bronze import DocId, SourceFormat, SpdxLicenseSource, TraceId, TrainingUsage
from schemas.scientific import SectionRole

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


class SilverSegment(BaseModel):
    """One included prose segment evaluated by CPU quality models."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    segment_id: str
    title: str
    role: SectionRole = "other"
    text: str
    word_count: int = Field(default=0, ge=0)


class SilverRecord(BaseModel):
    """Normalized, language-tagged, heuristically-scored document."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        ser_json_bytes="base64",
        val_json_bytes="base64",
    )

    doc_id: DocId
    url: HttpUrl
    title: str | None = Field(default=None, max_length=2048)
    text: str = Field(..., description="Extracted plain text (Resiliparse output).")
    model_text: str = Field(
        default="",
        description=(
            "Body-only prose used by web-quality, KenLM, PII, and dedup stages. "
            "Scientific equations/tables/figures remain in text but are excluded here."
        ),
    )
    source_metadata_text: str = Field(
        default="",
        max_length=32768,
        description="Bounded title/author metadata scanned separately from trainable body text.",
    )
    structured_text: str = Field(
        default="",
        description="Bounded tables/equations/figure surrogates appended after selected prose.",
    )
    segments: list[SilverSegment] = Field(default_factory=list)
    projection_version: str = "document-v1"
    source_word_count: int = Field(default=0, ge=0)
    training_word_count: int = Field(default=0, ge=0)
    included_section_count: int = Field(default=0, ge=0)
    excluded_section_count: int = Field(default=0, ge=0)
    excluded_sections: list[str] = Field(default_factory=list)
    lang: str = Field(..., min_length=2, max_length=8, description="ISO 639-1/3 language code.")
    lang_score: float = Field(..., ge=0.0, le=1.0)
    lang_detector_revision: str = "unknown"
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
    source_feed: str = Field(
        default="unknown",
        min_length=1,
        max_length=128,
        description="SourceFeed CRD name propagated from Bronze.",
    )

    # Source provenance (mirrored from Bronze; kept on Silver so
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
            "ship multiple named pipelines."
        ),
    )
    spdx_license: str | None = Field(
        default=None,
        max_length=128,
        description="Item-level licence identifier, or None if not attached.",
    )
    spdx_license_source: SpdxLicenseSource = Field(
        default="unknown",
        description="Where the SPDX id was read from.",
    )
    training_usage: TrainingUsage = Field(
        default="pretrain_and_posttrain",
        description="Purpose boundary propagated from the pre-fetch licence decision.",
    )
    raw_html_s3_uri: str | None = Field(
        default=None,
        pattern=r"^s3://[^/]+/.+",
        description="Original admitted Bronze body used only for bounded extraction retry.",
    )
    source_content_type: str = "application/octet-stream"
    source_http_status: int = Field(default=200, ge=100, le=599)
    source_fetched_at: datetime | None = None
    source_http_last_modified: datetime | None = None

    # Structured scientific artifact. The full nested object remains in
    # MinIO; compact counts and its pointer travel with every downstream row.
    scientific_artifact_s3_uri: str | None = Field(
        default=None,
        pattern=r"^s3://[^/]+/.+",
        description="Structured sections/tables/equations/figures JSON artifact.",
    )
    scientific_evidence_gzip: bytes | None = Field(
        default=None,
        description="Lossless extracted scientific JSON for durable Kafka handoff, not images.",
    )
    figure_count: int = Field(default=0, ge=0)
    table_count: int = Field(default=0, ge=0)
    equation_count: int = Field(default=0, ge=0)
    citation_count: int = Field(default=0, ge=0)
    extraction_warnings: list[str] = Field(default_factory=list)
