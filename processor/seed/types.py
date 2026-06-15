"""Shared in-memory dataclasses for the seed loaders.

A :class:`SeedDocument` is the lingua-franca between a per-source loader
(``pes2o.iter_documents``, ``redpajama_arxiv.iter_documents``, ...) and the
Bytewax dataflow in :mod:`processor.seed_loader`. Loaders never construct
SilverRecords directly: they emit ``SeedDocument`` and the dataflow maps
them onto the SilverRecord schema via :func:`processor.seed_loader.to_silver`.

Keeping this dataclass private to the seed loaders means the SilverRecord
contract can change without rewriting the per-source modules.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from schemas.bronze import SourceFormat, SpdxLicenseSource


@dataclass(frozen=True, slots=True)
class SeedDocument:
    """One document extracted from a seed dataset.

    The fields below are exactly what the Bytewax dataflow needs to build
    a SilverRecord; nothing more, nothing less. The validity-interval
    decision (precedence: dataset-metadata > nothing) is encoded in
    :attr:`valid_from` being the dataset-native publication date.
    """

    repo_id: str
    """HuggingFace dataset id this document came from, e.g. ``allenai/peS2o``.

    Wayback backfill uses a synthetic id ``wayback:<feed_name>`` so the
    cursor file path stays unique.
    """

    native_id: str
    """Source-stable identifier from inside the dataset.

    For peS2o this is the ``id`` column, for RedPajama-arxiv the arXiv
    identifier, for FineWeb-Edu the document URL, for Stack-Edu the blob
    SHA, for Wayback the timemap timestamp + URL pair.
    """

    url: str
    """Resolved http(s) URL the SilverRecord ``url`` field will point at.

    Synthetic for HF datasets without a per-document URL: the loader
    constructs a stable ``hf://<repo_id>/<native_id>`` URI in those cases
    and the dataflow rewrites it to the ``url`` HttpUrl shape.
    """

    title: str | None
    text: str
    lang: str
    """ISO 639-1 language code; ``en`` for every component except a future
    multilingual extension. Loaders should never lie about this; if they do
    not know they emit ``en`` because every Phase-1 component is English."""

    valid_from: datetime
    """Dataset-native publication date in UTC. Falls back to the dataset
    knowledge cutoff if a per-row date is not available; the loader records
    that decision in :attr:`valid_from_source`."""

    source_format: SourceFormat
    """Wire shape: ``html`` (FineWeb-Edu, blogs), ``latex`` (RedPajama-arxiv),
    ``code`` (Stack-Edu), ``web`` (Wayback)."""

    extraction_pipeline: str
    """Operator-chain id stamped onto Bronze + Silver. The seed loader uses
    a per-component identifier so forensic operators can tell a seed-derived
    record apart from a live-fetched one."""

    spdx_license: str | None
    spdx_license_source: SpdxLicenseSource

    extra: dict[str, str] = field(default_factory=dict)
    """Free-form per-source metadata.

    KNOWN-LIMITATION (v0.2.0): this map is **not propagated downstream**.
    SilverRecord has no extra column, the seed_loader.to_silver() mapper
    drops the field, and there is no Iceberg snapshot-property write path
    that would surface it. Per-component loaders still populate it
    (``pes2o_version``, ``redpajama_config``, ``wayback_timestamp``,
    ``feed_name``, ``repository_name``, ``fineweb_edu_score``, ``language``)
    so a future schema migration can pick them up without re-streaming
    the seed corpus, but as of v0.2.0 nothing reads it after the
    in-process SeedDocument goes out of scope. Tracked as a TODO in
    CLAUDE.md (extend SilverRecord with an extra map). Do not assume the
    tags are queryable in Gold or via ``as_of(timestamp)`` until then."""
