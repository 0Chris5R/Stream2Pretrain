"""Seed loader for ``togethercomputer/RedPajama-Data-1T`` config ``arxiv``.

LaTeX-extracted arXiv papers, ~92 GB / ~28B tokens per Together AI's
release blog. Cutoff 2023-04 (the exact day is needs-measurement).

License: Apache-2.0 wrapper, with content under arXiv's per-paper licenses
(``arxiv-non-exclusive-distribution`` for the bulk; we record the wrapper
SPDX and surface the per-paper license via the ``meta.url`` field on the
SilverRecord ``extra`` map - downstream operators decide).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Iterator

from processor.seed.cursor import SeedCursor
from processor.seed.types import SeedDocument

REPO_ID: str = "togethercomputer/RedPajama-Data-1T"
CONFIG_NAME: str = "arxiv"
SPDX: str = "Apache-2.0"

# RedPajama v1 release date: April 17 2023. Used as a hard fallback when
# meta.timestamp is absent from a row.
_RELEASE_CUTOFF = datetime(2023, 4, 17, tzinfo=timezone.utc)


def parse_meta_timestamp(meta: object) -> datetime | None:
    """Read ``meta.timestamp`` from a RedPajama row.

    The ``meta`` column is sometimes a JSON-encoded string and sometimes a
    dict depending on the parquet writer that produced the shard. Both are
    handled.
    """
    payload: dict[str, Any] | None = None
    if isinstance(meta, dict):
        payload = meta
    elif isinstance(meta, str) and meta:
        import json

        try:
            obj = json.loads(meta)
        except json.JSONDecodeError:
            return None
        if isinstance(obj, dict):
            payload = obj
    if payload is None:
        return None
    ts = payload.get("timestamp")
    if not isinstance(ts, str) or not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def derive_valid_from(row: dict[str, Any]) -> datetime:
    """Pick the best per-row publication date.

    Precedence: ``meta.timestamp`` (arXiv submission) > release cutoff.
    """
    dt = parse_meta_timestamp(row.get("meta"))
    if dt is not None:
        return dt
    return _RELEASE_CUTOFF


def native_id_for(row: dict[str, Any]) -> str:
    """arXiv id derived from ``meta.url`` if present, else hash of text."""
    meta = row.get("meta")
    payload: dict[str, Any] | None = None
    if isinstance(meta, dict):
        payload = meta
    elif isinstance(meta, str):
        import json

        try:
            obj = json.loads(meta)
            if isinstance(obj, dict):
                payload = obj
        except json.JSONDecodeError:
            payload = None
    if payload is not None:
        url = payload.get("url")
        if isinstance(url, str) and "arxiv.org" in url:
            tail = url.rstrip("/").rsplit("/", 1)[-1]
            return tail or ""
        ident = payload.get("arxiv_id") or payload.get("id")
        if isinstance(ident, str) and ident:
            return ident
    text = row.get("text")
    if isinstance(text, str) and text:
        # Stable surrogate id when arxiv-id is missing.
        import hashlib

        return "sha:" + hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
    return ""


def to_seed_document(row: dict[str, Any]) -> SeedDocument | None:
    """Convert one RedPajama-arxiv row into a :class:`SeedDocument`."""
    text = row.get("text")
    if not isinstance(text, str) or not text.strip():
        return None
    nid = native_id_for(row)
    if not nid:
        return None
    valid_from = derive_valid_from(row)
    url = f"https://arxiv.org/abs/{nid}" if not nid.startswith("sha:") else f"hf://{REPO_ID}/{nid}"
    extra: dict[str, str] = {"redpajama_config": CONFIG_NAME}
    return SeedDocument(
        repo_id=REPO_ID,
        native_id=nid,
        url=url,
        title=None,  # RedPajama-arxiv does not surface a title column
        text=text,
        lang="en",
        valid_from=valid_from,
        source_format="latex",
        extraction_pipeline="redpajama-arxiv-2023-04",
        spdx_license=SPDX,
        spdx_license_source="dataset_metadata",
        extra=extra,
    )


def iter_documents(
    cursor: SeedCursor,
    *,
    rows: Iterable[dict[str, Any]] | None = None,
    max_docs: int | None = None,
) -> Iterator[SeedDocument]:
    """Stream RedPajama-arxiv rows as :class:`SeedDocument`."""
    if rows is None:
        rows = load_hf_stream()
    emitted = 0
    for row in rows:
        if max_docs is not None and emitted >= max_docs:
            return
        doc = to_seed_document(row)
        if doc is None:
            continue
        if cursor.should_skip(doc.native_id):
            continue
        yield doc
        emitted += 1


def load_hf_stream() -> Iterable[dict[str, Any]]:
    """Construct the HuggingFace streaming iterator for the arxiv config."""
    from datasets import load_dataset

    return load_dataset(  # type: ignore[return-value]
        REPO_ID,
        name=CONFIG_NAME,
        split="train",
        streaming=True,
    )


__all__ = [
    "REPO_ID",
    "CONFIG_NAME",
    "SPDX",
    "parse_meta_timestamp",
    "derive_valid_from",
    "native_id_for",
    "to_seed_document",
    "iter_documents",
    "load_hf_stream",
]
