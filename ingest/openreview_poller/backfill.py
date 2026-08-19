"""REVIEWARENA HuggingFace dataset backfill (one-shot Job).

REVIEWARENA bundles full PDFs + reviews + rebuttals + decisions for a wide
range of OpenReview venues (ICLR 2020-2026, NeurIPS 2021-2025, ICML 2025,
COLM 2024-2025; see ``docs/research-fulltext-and-code.md``).

The exact HF dataset id is ``needs-measurement``: the cited research note
spells the dataset "REVIEWARENA" without a clear ``<owner>/<name>`` repo path,
and the Hugging Face Hub search must resolve it at runtime. The poller does
that resolution via ``huggingface_hub.HfApi.list_datasets(search=...)``,
records the chosen id in the JSON output of ``run_backfill``, and falls back
to a configurable override (``S2P_REVIEWARENA_DATASET`` env var) so an
operator can pin a known-good revision.

For every example in the streamed dataset we:

1. Persist the binary PDF (when present) under
   ``s3://<bronze>/openreview/backfill/<venue>/<note_id>.pdf`` and emit one
   ``source_format="pdf"`` BronzeRecord with
   ``extraction_pipeline="reviewarena-pdf-pending-marker"``.
2. Persist the concatenated review/decision/rebuttal text (when present) as
   ``source_format="review"`` with
   ``extraction_pipeline="reviewarena-review-text"``.

Streaming is configured via ``datasets.load_dataset(..., streaming=True)`` so
the worker never holds the full dataset in RAM.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ingest.common.config import IngestConfig, load_config
from ingest.common.hashing import doc_id_for_url
from ingest.common.kafka_producer import BronzeProducer, LicenseAdmissionProducer
from ingest.common.license_admission import decide_license_admission
from ingest.common.logging import configure_logging, get_logger
from ingest.common.minio_writer import MinioWriter
from ingest.common.otel import init_tracer
from schemas.bronze import BronzeRecord

log = get_logger(__name__)

SOURCE_FEED = "openreview-backfill"
PIPELINE_PDF_BACKFILL = "reviewarena-pdf-pending-marker"
PIPELINE_REVIEW_BACKFILL = "reviewarena-review-text"
DEFAULT_REVIEWARENA_QUERY = "REVIEWARENA"


@dataclass(slots=True)
class BackfillStats:
    """Counters returned from one backfill pass."""

    dataset_id: str = ""
    rows_seen: int = 0
    pdfs_emitted: int = 0
    reviews_emitted: int = 0
    skipped: int = 0


# ---------------------------------------------------------------------------
# Dataset id resolution
# ---------------------------------------------------------------------------


def _candidate_score(name: str) -> int:
    """Tiny heuristic: prefer dataset ids with REVIEWARENA in them."""
    lower = name.lower()
    score = 0
    if "reviewarena" in lower:
        score += 10
    if "review" in lower and "arena" in lower:
        score += 5
    if "openreview" in lower:
        score += 2
    return score


def resolve_reviewarena_id(
    *,
    override: str | None,
    search_fn: Any | None = None,
) -> str | None:
    """Pick the REVIEWARENA repo id, returning None if nothing matches.

    ``override`` wins. Otherwise we call ``search_fn(search=<query>)`` (which
    in production is ``HfApi().list_datasets``) and pick the highest-scoring
    candidate.
    """
    if override:
        return override
    if search_fn is None:
        try:
            from huggingface_hub import HfApi

            search_fn = HfApi().list_datasets
        except Exception as exc:
            log.warning("reviewarena.search_unavailable", err=str(exc))
            return None
    try:
        candidates = list(search_fn(search=DEFAULT_REVIEWARENA_QUERY))
    except Exception as exc:
        log.warning("reviewarena.search_failed", err=str(exc))
        return None
    best: tuple[int, str] | None = None
    for c in candidates:
        repo_id = getattr(c, "id", None) or getattr(c, "repo_id", None)
        if not isinstance(repo_id, str):
            continue
        score = _candidate_score(repo_id)
        if score <= 0:
            continue
        if best is None or score > best[0]:
            best = (score, repo_id)
    return best[1] if best else None


# ---------------------------------------------------------------------------
# Row coercion
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _RowView:
    """Adapter over the heterogeneous REVIEWARENA row shape.

    The dataset is community-curated and column names drift between snapshots.
    We probe a small set of likely keys and degrade gracefully when something
    is missing. Keys that did not appear in cited research are tagged with
    ``needs-measurement`` in tests.
    """

    note_id: str
    forum_id: str
    venue: str
    year: int | None
    title: str | None
    pdf_bytes: bytes | None
    review_text: str | None
    decision: str | None
    cdate: datetime | None
    paper_license: str | None = None
    review_license: str | None = None

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> _RowView:
        note_id = _first_str(row, ["note_id", "id", "openreview_id", "paper_id"])
        forum_id = _first_str(row, ["forum", "forum_id", "thread_id"]) or note_id or ""
        venue = _first_str(row, ["venue", "venue_id", "conference"]) or "unknown"
        year = _first_int(row, ["year", "venue_year"])
        title = _first_str(row, ["title", "paper_title"])
        review_text = _join_review(row)
        decision = _first_str(row, ["decision", "verdict"])
        cdate = _first_datetime(row, ["cdate", "submission_date", "created_at"])
        pdf_bytes = _first_bytes(row, ["pdf", "pdf_bytes", "pdf_content"])
        return cls(
            note_id=note_id or "",
            forum_id=forum_id,
            venue=venue,
            year=year,
            title=title,
            pdf_bytes=pdf_bytes,
            review_text=review_text,
            decision=decision,
            cdate=cdate,
            paper_license=_first_str(row, ["paper_license", "license", "license_url"]),
            review_license=_first_str(row, ["review_license", "review_license_url"]),
        )


def _first_str(row: dict[str, Any], keys: Iterable[str]) -> str | None:
    for k in keys:
        v = row.get(k)
        if isinstance(v, str) and v:
            return v
    return None


def _first_int(row: dict[str, Any], keys: Iterable[str]) -> int | None:
    for k in keys:
        v = row.get(k)
        if isinstance(v, int):
            return v
        if isinstance(v, str) and v.isdigit():
            return int(v)
    return None


def _first_bytes(row: dict[str, Any], keys: Iterable[str]) -> bytes | None:
    for k in keys:
        v = row.get(k)
        if isinstance(v, (bytes, bytearray)) and v:
            return bytes(v)
        # HuggingFace ``datasets`` returns large binary fields as
        # ``{"bytes": b"...", "path": ...}``.
        if isinstance(v, dict):
            inner = v.get("bytes")
            if isinstance(inner, (bytes, bytearray)) and inner:
                return bytes(inner)
    return None


def _first_datetime(row: dict[str, Any], keys: Iterable[str]) -> datetime | None:
    for k in keys:
        v = row.get(k)
        if isinstance(v, datetime):
            return v if v.tzinfo else v.replace(tzinfo=UTC)
        if isinstance(v, int):
            try:
                return datetime.fromtimestamp(v / 1000.0, tz=UTC)
            except (OSError, OverflowError, ValueError):
                continue
        if isinstance(v, str) and v:
            try:
                parsed = datetime.fromisoformat(v.replace("Z", "+00:00"))
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
            except ValueError:
                continue
    return None


def _join_review(row: dict[str, Any]) -> str | None:
    """Concatenate review-thread text from any of the likely shapes."""
    parts: list[str] = []
    for key in ("reviews", "review_texts", "official_reviews"):
        v = row.get(key)
        if isinstance(v, list):
            for item in v:
                if isinstance(item, str) and item:
                    parts.append(item)
                elif isinstance(item, dict):
                    text = item.get("review") or item.get("text") or item.get("comment")
                    if isinstance(text, str) and text:
                        parts.append(text)
        elif isinstance(v, str) and v:
            parts.append(v)
    rebuttal = row.get("rebuttal") or row.get("author_response")
    if isinstance(rebuttal, str) and rebuttal:
        parts.append(rebuttal)
    return "\n\n---\n\n".join(parts) if parts else None


# ---------------------------------------------------------------------------
# Bronze writers
# ---------------------------------------------------------------------------


def _trace_id() -> str:
    import secrets

    return secrets.token_hex(16)


def _safe(s: str) -> str:
    return s.replace("/", "_").replace(" ", "_")


def _backfill_pdf_key(view: _RowView) -> str:
    year = f"{view.year:04d}" if view.year else "unknown"
    return (
        f"openreview/backfill/venue={_safe(view.venue)}/year={year}/"
        f"{_safe(view.note_id) or 'unknown'}.pdf"
    )


def _backfill_review_key(view: _RowView) -> str:
    year = f"{view.year:04d}" if view.year else "unknown"
    return (
        f"openreview/backfill/venue={_safe(view.venue)}/year={year}/reviews/"
        f"{_safe(view.note_id) or 'unknown'}.json.gz"
    )


async def _emit_pdf(
    *,
    view: _RowView,
    cfg: IngestConfig,
    producer: BronzeProducer,
    minio: MinioWriter,
    admission_producer: LicenseAdmissionProducer,
) -> bool:
    if not view.pdf_bytes or not view.note_id:
        return False
    url = f"https://openreview.net/pdf?id={view.note_id}"
    admission = decide_license_admission(
        source_url=url,
        source_feed=SOURCE_FEED,
        license_value=view.paper_license,
        license_source="dataset_metadata" if view.paper_license else "unknown",
    )
    await admission_producer.send(admission.decision)
    if not admission.admitted:
        return False
    fetched_at = datetime.now(tz=UTC)
    cdate = view.cdate or fetched_at
    key = _backfill_pdf_key(view)
    metadata = {
        "doc_id": doc_id_for_url(url),
        "source_feed": SOURCE_FEED,
        "openreview_note_id": view.note_id,
        "openreview_forum": view.forum_id,
        "openreview_venue": view.venue,
        "valid_from": cdate.isoformat(),
    }
    bytes_size = await minio.put_bronze(
        key=key,
        payload=view.pdf_bytes,
        content_type="application/pdf",
        gzip_compress=False,
        metadata=metadata,
    )
    record = BronzeRecord(
        doc_id=doc_id_for_url(url),
        url=url,  # type: ignore[arg-type]
        fetched_at=fetched_at,
        http_status=200,
        content_type="application/pdf",
        raw_html_s3_uri=f"s3://{cfg.minio_bronze_bucket}/{key}",
        source_feed=SOURCE_FEED,
        trace_id=admission.decision.trace_id,
        bytes_size=bytes_size,
        source_format="pdf",
        extraction_pipeline=PIPELINE_PDF_BACKFILL,
        spdx_license=admission.license_id,
        spdx_license_source="dataset_metadata",
    )
    await producer.send(
        record,
        headers={
            "openreview_note_id": view.note_id,
            "openreview_venue": view.venue,
            "valid_from": cdate.isoformat(),
        },
    )
    return True


async def _emit_review(
    *,
    view: _RowView,
    cfg: IngestConfig,
    producer: BronzeProducer,
    minio: MinioWriter,
    admission_producer: LicenseAdmissionProducer,
) -> bool:
    if not view.review_text or not view.note_id:
        return False
    url = f"https://openreview.net/forum?id={view.note_id}"
    admission = decide_license_admission(
        source_url=url,
        source_feed=SOURCE_FEED,
        license_value=view.review_license,
        license_source="dataset_metadata" if view.review_license else "unknown",
    )
    await admission_producer.send(admission.decision)
    if not admission.admitted:
        return False
    fetched_at = datetime.now(tz=UTC)
    cdate = view.cdate or fetched_at
    payload = json.dumps(
        {
            "note_id": view.note_id,
            "forum": view.forum_id,
            "venue": view.venue,
            "year": view.year,
            "title": view.title,
            "decision": view.decision,
            "review_text": view.review_text,
        },
        sort_keys=True,
        ensure_ascii=False,
    ).encode("utf-8")
    key = _backfill_review_key(view)
    metadata = {
        "doc_id": doc_id_for_url(url),
        "source_feed": SOURCE_FEED,
        "openreview_note_id": view.note_id,
        "openreview_forum": view.forum_id,
        "openreview_venue": view.venue,
        "valid_from": cdate.isoformat(),
    }
    bytes_size = await minio.put_bronze(
        key=key,
        payload=payload,
        content_type="application/json",
        gzip_compress=True,
        metadata=metadata,
    )
    record = BronzeRecord(
        doc_id=doc_id_for_url(url),
        url=url,  # type: ignore[arg-type]
        fetched_at=fetched_at,
        http_status=200,
        content_type="application/json",
        raw_html_s3_uri=f"s3://{cfg.minio_bronze_bucket}/{key}",
        source_feed=SOURCE_FEED,
        trace_id=admission.decision.trace_id,
        bytes_size=bytes_size,
        source_format="review",
        extraction_pipeline=PIPELINE_REVIEW_BACKFILL,
        spdx_license=admission.license_id,
        spdx_license_source="dataset_metadata",
    )
    await producer.send(
        record,
        headers={
            "openreview_note_id": view.note_id,
            "openreview_venue": view.venue,
            "valid_from": cdate.isoformat(),
        },
    )
    return True


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def stream_dataset(
    dataset_id: str,
    *,
    split: str = "train",
    streaming_loader: Any | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield rows from ``dataset_id`` in streaming mode.

    ``streaming_loader`` is exposed so tests can pass a synthetic iterable in
    place of ``datasets.load_dataset``.
    """
    if streaming_loader is not None:
        yield from streaming_loader(dataset_id, split=split)
        return
    from datasets import load_dataset

    ds = load_dataset(dataset_id, split=split, streaming=True)
    for row in ds:
        if isinstance(row, dict):
            yield row


async def run_backfill(
    cfg: IngestConfig,
    *,
    dataset_id: str | None = None,
    max_rows: int | None = None,
    streaming_loader: Any | None = None,
    search_fn: Any | None = None,
    split: str = "train",
) -> BackfillStats:
    """Stream REVIEWARENA into bronze.

    ``max_rows`` is the small-scale-first knob from CLAUDE.md; pass e.g. 100
    for a smoke test, omit for the full run.
    """
    resolved = dataset_id or resolve_reviewarena_id(
        override=os.environ.get("S2P_REVIEWARENA_DATASET"),
        search_fn=search_fn,
    )
    if not resolved:
        log.warning("reviewarena.unresolved", note="dataset id is needs-measurement")
        return BackfillStats(dataset_id="")

    log.info("reviewarena.resolved", dataset_id=resolved)
    stats = BackfillStats(dataset_id=resolved)
    async with (
        BronzeProducer(
            cfg.redpanda_brokers, topic=cfg.raw_topic, client_id="s2p-openreview-backfill"
        ) as producer,
        LicenseAdmissionProducer(
            cfg.redpanda_brokers,
            topic=cfg.license_admissions_topic,
            client_id="s2p-openreview-backfill-license-admission",
        ) as admission_producer,
        MinioWriter(
            cfg.minio_endpoint,
            cfg.minio_access_key,
            cfg.minio_secret_key,
            bucket=cfg.minio_bronze_bucket,
        ) as minio,
    ):
        for raw in stream_dataset(resolved, split=split, streaming_loader=streaming_loader):
            stats.rows_seen += 1
            view = _RowView.from_dict(raw)
            try:
                if await _emit_pdf(
                    view=view,
                    cfg=cfg,
                    producer=producer,
                    minio=minio,
                    admission_producer=admission_producer,
                ):
                    stats.pdfs_emitted += 1
            except Exception as exc:
                log.warning("reviewarena.pdf_emit_failed", err=str(exc))
                stats.skipped += 1
            try:
                if await _emit_review(
                    view=view,
                    cfg=cfg,
                    producer=producer,
                    minio=minio,
                    admission_producer=admission_producer,
                ):
                    stats.reviews_emitted += 1
            except Exception as exc:
                log.warning("reviewarena.review_emit_failed", err=str(exc))
                stats.skipped += 1
            if max_rows is not None and stats.rows_seen >= max_rows:
                break
    return stats


def main() -> None:
    cfg = load_config()
    configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    init_tracer("ingest.openreview_poller.backfill", cfg)
    log.info("openreview_backfill.start")
    max_rows_env = os.environ.get("S2P_BACKFILL_MAX_ROWS")
    max_rows = int(max_rows_env) if max_rows_env and max_rows_env.isdigit() else None
    dataset_id = os.environ.get("S2P_REVIEWARENA_DATASET") or None
    stats = asyncio.run(run_backfill(cfg, dataset_id=dataset_id, max_rows=max_rows))
    log.info(
        "openreview_backfill.done",
        dataset=stats.dataset_id,
        rows_seen=stats.rows_seen,
        pdfs=stats.pdfs_emitted,
        reviews=stats.reviews_emitted,
        skipped=stats.skipped,
    )


if __name__ == "__main__":
    main()
