"""Seed loader for ``allenai/peS2o`` filtered to cs.* domain.

Per :doc:`/docs/research-seed-corpus.md` the v3 directory holds 136 zst
shards, ~120 GB on Hub. v3 token count is needs-measurement; v2 reports
42.01B whitespace-separated tokens with knowledge cutoff 2023-01-03.

This loader prefers v3 (data/v3/) and falls back to v2 if v3 is missing
metadata. Filtering to cs.* relies on the dataset's per-row
``field_of_study`` / ``s2_fields_of_study`` metadata; rows that do not
expose that field are accepted (the downstream FineWeb-Edu classifier will
score them) but tagged with ``extra["cs_filter"] = "missing-field"``.

License: ODC-By-1.0 (inherited from the Dolma collection).
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from typing import Any

from processor.seed.cursor import SeedCursor
from processor.seed.types import SeedDocument

REPO_ID: str = "allenai/peS2o"
SPDX: str = "ODC-By-1.0"

# v2 knowledge cutoff per the dataset card.
_V2_CUTOFF = datetime(2023, 1, 3, tzinfo=UTC)

# Fields of study the cs.* filter accepts. Matches peS2o's S2ORC tagging:
# rows that contain ANY of these are kept.
CS_FIELDS: frozenset[str] = frozenset(
    {
        "Computer Science",
        "computer science",
        "cs",
    }
)

# arXiv categories we map onto cs.* if the row only carries ``categories``.
ARXIV_CS_PREFIXES: tuple[str, ...] = ("cs.", "stat.ML")


def is_cs_row(row: dict[str, Any]) -> bool:
    """True if ``row`` belongs to the cs.* slice we want to keep.

    The peS2o schema has shifted across versions; we therefore check three
    possible shapes:

    1. ``row["s2_fields_of_study"]``  - list[str] of S2ORC field names.
    2. ``row["field_of_study"]``      - same, alternative key.
    3. ``row["categories"]``          - arXiv-style ``cs.CL cs.LG`` string.

    Rows missing all three are accepted (returned ``True``); the downstream
    quality classifier will sort signal from noise. This matches the
    research note: "filter to cs.* if metadata available, else accept".
    """
    fields: Iterable[str] | None = None
    for key in ("s2_fields_of_study", "field_of_study", "fields_of_study"):
        val = row.get(key)
        if isinstance(val, list) and val:
            fields = [str(x) for x in val]
            break
    if fields is not None:
        return any(f in CS_FIELDS for f in fields)
    cats = row.get("categories")
    if isinstance(cats, str) and cats:
        tokens = cats.replace(",", " ").split()
        return any(t.startswith(ARXIV_CS_PREFIXES) for t in tokens)
    if isinstance(cats, list):
        return any(isinstance(t, str) and t.startswith(ARXIV_CS_PREFIXES) for t in cats)
    # No metadata: keep the row.
    return True


def derive_valid_from(row: dict[str, Any]) -> datetime:
    """Pick the best dataset-native publication date for ``row``.

    Precedence: ``created`` (ISO datetime) > ``year`` + ``month`` >
    ``year`` alone > v2 cutoff (2023-01-03). The v2 fallback is a
    coarse-but-honest signal; we never invent ``fetched_at`` for seed rows
    because that would defeat the validity-interval demo.
    """
    created = row.get("created")
    if isinstance(created, str) and created:
        try:
            dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)
        except ValueError:
            pass
    year_raw = row.get("year")
    month_raw = row.get("month")
    if isinstance(year_raw, (int, str)) and str(year_raw).isdigit():
        year = int(year_raw)
        month = 1
        if isinstance(month_raw, (int, str)) and str(month_raw).isdigit():
            month_int = int(month_raw)
            if 1 <= month_int <= 12:
                month = month_int
        try:
            return datetime(year, month, 1, tzinfo=UTC)
        except ValueError:
            pass
    return _V2_CUTOFF


def native_id_for(row: dict[str, Any]) -> str:
    """Stable native id for cursor advancement; zero-padded for safe sort."""
    raw = row.get("id") or row.get("paper_id") or row.get("corpus_id")
    if raw is None:
        return ""
    text = str(raw)
    # peS2o ids are typically integer-ish; zero-pad to 16 digits for
    # lexicographic sort. Non-numeric ids fall through unchanged.
    if text.isdigit():
        return text.zfill(16)
    return text


def to_seed_document(row: dict[str, Any]) -> SeedDocument | None:
    """Convert a peS2o row into a :class:`SeedDocument` or ``None`` if empty."""
    text = row.get("text")
    if not isinstance(text, str) or not text.strip():
        return None
    nid = native_id_for(row)
    if not nid:
        return None
    title = row.get("title")
    if title is not None and not isinstance(title, str):
        title = None
    valid_from = derive_valid_from(row)
    extra: dict[str, str] = {}
    if "version" in row and isinstance(row["version"], str):
        extra["pes2o_version"] = row["version"]
    if not is_cs_row(row):
        # Caller is expected to pre-filter via :func:`is_cs_row`; we hard-skip
        # if a non-cs row slipped through.
        return None
    return SeedDocument(
        repo_id=REPO_ID,
        native_id=nid,
        url=f"hf://{REPO_ID}/{nid}",
        title=title,
        text=text,
        lang="en",
        valid_from=valid_from,
        source_format="latex",
        extraction_pipeline="pes2o-seed-2026-06",
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
    """Stream :class:`SeedDocument` records from peS2o.

    ``rows`` is an injectable iterable for tests; in production it is
    constructed by :func:`load_hf_stream`.
    """
    if rows is None:
        rows = load_hf_stream()
    emitted = 0
    for row in rows:
        if max_docs is not None and emitted >= max_docs:
            return
        if not is_cs_row(row):
            continue
        doc = to_seed_document(row)
        if doc is None:
            continue
        if cursor.should_skip(doc.native_id):
            continue
        yield doc
        emitted += 1


def load_hf_stream() -> Iterable[dict[str, Any]]:
    """Construct the HuggingFace streaming iterator.

    Imported inside the function so unit tests can import this module
    without paying the ``datasets`` runtime cost.
    """
    from datasets import load_dataset

    # v3 lives under data/v3/; load_dataset auto-discovers but we pin
    # ``data_dir`` so a future Hub re-org cannot silently swap us back to
    # v2. Streaming=True keeps RAM bounded.
    try:
        ds = load_dataset(
            REPO_ID,
            data_dir="data/v3",
            split="train",
            streaming=True,
        )
    except Exception:
        # v3 layout missing -> fall back to v2 default config.
        ds = load_dataset(REPO_ID, split="train", streaming=True)
    return ds  # type: ignore[return-value]


__all__ = [
    "ARXIV_CS_PREFIXES",
    "CS_FIELDS",
    "REPO_ID",
    "SPDX",
    "derive_valid_from",
    "is_cs_row",
    "iter_documents",
    "load_hf_stream",
    "native_id_for",
    "to_seed_document",
]
