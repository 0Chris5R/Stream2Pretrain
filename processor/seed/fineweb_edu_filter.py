"""Seed loader for ``HuggingFaceFW/fineweb-edu`` filtered by URL allowlist.

FineWeb-Edu is a ~1.3T-token CC-derived English subset (FineWeb-Edu
classifier score >= 3) under ODC-By-1.0. The allowlist comes from
:file:`charts/stream2pretrain/values.yaml` (``seedLoader.fineweb_url_allowlist``)
and matches a row's ``url`` column against substring + suffix membership.

Approximate yield is needs-measurement: research notes 5-50 GB.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from processor.seed.cursor import SeedCursor
from processor.seed.types import SeedDocument

REPO_ID: str = "HuggingFaceFW/fineweb-edu"
SPDX: str = "ODC-By-1.0"

# Default allowlist; values.yaml is the canonical source. Kept here so unit
# tests do not depend on Helm rendering.
DEFAULT_URL_ALLOWLIST: tuple[str, ...] = (
    "arxiv.org",
    "openai.com",
    "deepmind.google",
    "anthropic.com",
    "ai.meta.com",
    "huggingface.co",
    "distill.pub",
    "eleuther.ai",
    "bair.berkeley.edu",
    "lilianweng.github.io",
    "sebastianraschka.com",
    "magazine.sebastianraschka.com",
    "jalammar.github.io",
    "karpathy.ai",
    "pytorch.org",
    "tensorflow.org",
    "pytorch-lightning.readthedocs.io",
)


def url_matches_allowlist(url: str, allowlist: Iterable[str]) -> bool:
    """True if ``url``'s host equals or ends with any allowlist entry.

    Suffix match handles subdomains: ``magazine.sebastianraschka.com``
    matches ``sebastianraschka.com``.
    """
    if not url:
        return False
    try:
        host = (urlparse(url).hostname or "").lower()
    except ValueError:
        return False
    if not host:
        return False
    for entry in allowlist:
        e = entry.strip().lower()
        if not e:
            continue
        if host == e or host.endswith("." + e):
            return True
    return False


def derive_valid_from(row: dict[str, Any]) -> datetime:
    """Pick the best dataset-native publication date.

    Precedence: ``date`` (CC ``last_modified`` / crawl_date) > ``dump``
    quarter encoded in the ``dump`` column (``CC-MAIN-2024-30``) > FineWeb-Edu
    release cutoff (2024-04-01).
    """
    raw = row.get("date")
    if isinstance(raw, str) and raw:
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)
        except ValueError:
            pass
    dump = row.get("dump")
    if isinstance(dump, str) and dump.startswith("CC-MAIN-"):
        # CC-MAIN-YYYY-WW where WW is an ISO week.
        try:
            _, _, year_s, week_s = dump.split("-", 3)
            year = int(year_s)
            week = int(week_s)
            return datetime.fromisocalendar(year, max(1, min(52, week)), 1).replace(
                tzinfo=UTC
            )
        except (ValueError, IndexError):
            pass
    return datetime(2024, 4, 1, tzinfo=UTC)


def native_id_for(row: dict[str, Any]) -> str:
    """Stable native id; the ``id`` column on FineWeb is per-row."""
    raw = row.get("id") or row.get("url")
    return str(raw) if raw is not None else ""


def to_seed_document(
    row: dict[str, Any], *, allowlist: Iterable[str] = DEFAULT_URL_ALLOWLIST
) -> SeedDocument | None:
    """Convert one FineWeb-Edu row into a :class:`SeedDocument` if on-domain."""
    text = row.get("text")
    if not isinstance(text, str) or not text.strip():
        return None
    url = row.get("url")
    if not isinstance(url, str) or not url:
        return None
    if not url_matches_allowlist(url, allowlist):
        return None
    nid = native_id_for(row)
    if not nid:
        return None
    title = row.get("title") if isinstance(row.get("title"), str) else None
    valid_from = derive_valid_from(row)
    extra: dict[str, str] = {}
    score = row.get("score")
    if isinstance(score, (int, float)):
        extra["fineweb_edu_score"] = f"{float(score):.3f}"
    return SeedDocument(
        repo_id=REPO_ID,
        native_id=nid,
        url=url,
        title=title,
        text=text,
        lang="en",
        valid_from=valid_from,
        source_format="html",
        extraction_pipeline="fineweb-edu-2024",
        spdx_license=SPDX,
        spdx_license_source="dataset_metadata",
        extra=extra,
    )


def iter_documents(
    cursor: SeedCursor,
    *,
    rows: Iterable[dict[str, Any]] | None = None,
    allowlist: Iterable[str] = DEFAULT_URL_ALLOWLIST,
    max_docs: int | None = None,
) -> Iterator[SeedDocument]:
    """Stream FineWeb-Edu rows that match the URL allowlist."""
    if rows is None:
        rows = load_hf_stream()
    # Materialize the allowlist so we can iterate it for every row.
    allow = tuple(allowlist)
    emitted = 0
    for row in rows:
        if max_docs is not None and emitted >= max_docs:
            return
        doc = to_seed_document(row, allowlist=allow)
        if doc is None:
            continue
        if cursor.should_skip(doc.native_id):
            continue
        yield doc
        emitted += 1


def load_hf_stream() -> Iterable[dict[str, Any]]:
    """Construct the streaming iterator over fineweb-edu."""
    from datasets import load_dataset

    return load_dataset(  # type: ignore[return-value]
        REPO_ID,
        split="train",
        streaming=True,
    )


__all__ = [
    "DEFAULT_URL_ALLOWLIST",
    "REPO_ID",
    "SPDX",
    "derive_valid_from",
    "iter_documents",
    "load_hf_stream",
    "native_id_for",
    "to_seed_document",
    "url_matches_allowlist",
]
