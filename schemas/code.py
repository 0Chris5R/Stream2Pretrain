"""Per-file code record emitted by the GitHub release tarball fetcher.

A :class:`CodeFileRecord` is what the v0.2.0 ``ingest/github_release_tarball_fetcher``
module emits for every source file extracted from a release tarball after the
license + path-extension filter has run. The record is then wrapped into a
standard :class:`schemas.bronze.BronzeRecord` with ``source_format='code'`` and
written to the existing ``raw.fetched`` topic, so the curation operators do not
need a new topic.

Design choice (per CLAUDE.md decision log, 2026-06-15):
    Do NOT introduce a fifth ``docs.code`` topic. Reuse ``docs.normalized`` and
    let downstream operators dispatch on ``source_format == 'code'``. This keeps
    the four-topic Redpanda contract stable across v0.1 -> v0.2.

The native publication-date metadata for code is the release ``published_at``
timestamp, which populates ``valid_from``. ``valid_to`` is left ``None`` until
a later release supersedes the file (handled by the validity-interval enricher
operator, out of scope for v0.2.0).
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from schemas.bronze import DocId, SpdxLicenseSource, TraceId


class CodeFileRecord(BaseModel):
    """One source file extracted from a GitHub release tarball."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    doc_id: DocId
    repo_full_name: str = Field(
        ...,
        min_length=3,
        max_length=140,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*$",
        description="GitHub ``owner/repo`` identifier.",
    )
    ref: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description=(
            "Release tag (e.g. ``v0.4.2``) or commit SHA (40 lowercase hex). "
            "Whichever the GitHub Releases Atom feed surfaced."
        ),
    )
    path: str = Field(
        ...,
        min_length=1,
        max_length=2048,
        description="Tarball-relative path of the file (e.g. ``src/foo.py``).",
    )
    language: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description=(
            "Programming language label (linguist / Pygments / enry). Lowercase. "
            "Examples: ``python``, ``cuda``, ``c++``, ``rust``."
        ),
    )
    sloc: int = Field(
        ...,
        ge=0,
        description="Source lines of code (non-blank, non-comment).",
    )
    license: str | None = Field(
        default=None,
        max_length=128,
        description=(
            "OSI-list verified SPDX id from the GitHub License API "
            "(``/repos/{o}/{r}/license``); None if the repo publishes no "
            "license file."
        ),
    )
    license_source: SpdxLicenseSource = Field(
        default="github_api",
        description="Provenance of ``license`` (defaults to GitHub License API).",
    )
    raw_s3_uri: str = Field(
        ...,
        pattern=r"^s3://[^/]+/.+",
        description=(
            "Pointer to the file's bytes inside the release-tarball blob in MinIO; "
            "format ``s3://bronze/code/<owner>/<repo>/<ref>/<path>``."
        ),
    )
    valid_from: datetime = Field(
        ...,
        description=(
            "Release ``published_at`` timestamp from the GitHub Releases Atom feed. "
            "Populates the per-document validity interval (N2 novelty)."
        ),
    )
    valid_to: datetime | None = Field(
        default=None,
        description=(
            "Set when a later release supersedes this file; None means the file "
            "is still the head version."
        ),
    )
    trace_id: TraceId
