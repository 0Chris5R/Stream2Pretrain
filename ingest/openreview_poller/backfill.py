"""REVIEWARENA HuggingFace dataset backfill (one-shot Job).

ReviewArena bundles OCR Markdown + reviews + rebuttals + decisions for seven
OpenReview conference families. The default dataset and revision are pinned
to the snapshot whose schema this adapter implements. Operators may override
both through environment variables for an audited replacement snapshot.

For every example in the streamed dataset we:

1. Persist the source-provided OCR Markdown (when present) and emit one
   ``source_format="markdown"`` BronzeRecord with
   ``extraction_pipeline="reviewarena-ocr-markdown-v1"``.
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
OPENREVIEW_TERMS_URL = "https://openreview.net/legal/terms"
OPENREVIEW_TERMS_REVISION = "retrieved-2026-08-23"

SOURCE_FEED = "openreview-backfill"
PIPELINE_MARKDOWN_BACKFILL = "reviewarena-ocr-markdown-v1"
PIPELINE_REVIEW_BACKFILL = "reviewarena-review-text"
DEFAULT_REVIEWARENA_DATASET = "anonymousNeurIPS2026submission4281/reviewarena"
DEFAULT_REVIEWARENA_REVISION = "c2978add17c2099219eaddbc2599974d69d4d09b"
DEFAULT_REVIEWARENA_SPLITS: tuple[str, ...] = (
    "neurips",
    "iclr",
    "icml",
    "tmlr",
    "emnlp",
    "corl",
    "colm",
)


@dataclass(slots=True)
class BackfillStats:
    """Counters returned from one backfill pass."""

    dataset_id: str = ""
    rows_seen: int = 0
    papers_emitted: int = 0
    reviews_emitted: int = 0
    skipped: int = 0


# ---------------------------------------------------------------------------
# Dataset identity
# ---------------------------------------------------------------------------


def resolve_reviewarena_id(
    *,
    override: str | None,
    search_fn: Any | None = None,
) -> str:
    """Return an explicit override or the schema-pinned ReviewArena dataset."""
    if override:
        return override
    del search_fn
    return DEFAULT_REVIEWARENA_DATASET


# ---------------------------------------------------------------------------
# Row coercion
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _RowView:
    """Adapter over the pinned ReviewArena row schema plus legacy aliases."""

    note_id: str
    forum_id: str
    venue: str
    year: int | None
    title: str | None
    markdown: str | None
    review_text: str | None
    decision: str | None
    cdate: datetime | None
    paper_license: str | None = None
    review_license: str | None = None

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> _RowView:
        note_id = _first_str(row, ["forum_id", "note_id", "id", "openreview_id", "paper_id"])
        forum_id = _first_str(row, ["forum_id", "forum", "thread_id"]) or note_id or ""
        venue = _first_str(row, ["venue_id", "venue", "conference"]) or "unknown"
        year = _first_int(row, ["year", "venue_year"])
        title = _first_str(row, ["title", "paper_title"])
        review_text = _join_review(row)
        decision = _first_str(row, ["decision", "verdict"])
        cdate = _first_datetime(row, ["cdate", "submission_date", "created_at"])
        markdown = _first_str(row, ["markdown", "paper_markdown", "full_text"])
        return cls(
            note_id=note_id or "",
            forum_id=forum_id,
            venue=venue,
            year=year,
            title=title,
            markdown=markdown,
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
    """Concatenate ReviewArena's structured reviews and discussion fields."""
    parts: list[str] = []
    reviews_json = row.get("reviews_json")
    if isinstance(reviews_json, str) and reviews_json:
        try:
            decoded_reviews = json.loads(reviews_json)
        except json.JSONDecodeError:
            log.warning("reviewarena.reviews_json_invalid")
        else:
            if isinstance(decoded_reviews, list):
                for index, item in enumerate(decoded_reviews, start=1):
                    if isinstance(item, dict):
                        text = _review_dict_text(item)
                        if text:
                            parts.append(f"Review {index}\n{text}")
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
    decision_comment = row.get("decision_comment")
    if isinstance(decision_comment, str) and decision_comment:
        parts.append(f"Decision comment\n{decision_comment}")
    rebuttal = row.get("author_rebuttal") or row.get("rebuttal") or row.get("author_response")
    if isinstance(rebuttal, str) and rebuttal:
        parts.append(f"Author rebuttal\n{rebuttal}")
    return "\n\n---\n\n".join(parts) if parts else None


def _review_dict_text(review: dict[str, Any]) -> str:
    """Project one union-schema review into stable human-readable prose."""
    excluded = {"review_id", "reviewer"}
    parts: list[str] = []
    for key, value in review.items():
        if key in excluded or value is None or value == "" or value == [] or value == {}:
            continue
        label = key.replace("_", " ").strip().capitalize()
        if isinstance(value, (str, int, float, bool)):
            parts.append(f"{label}: {value}")
        elif isinstance(value, dict):
            nested = "; ".join(
                f"{str(k).replace('_', ' ')}: {v}"
                for k, v in value.items()
                if v is not None and v != ""
            )
            if nested:
                parts.append(f"{label}: {nested}")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Bronze writers
# ---------------------------------------------------------------------------


def _trace_id() -> str:
    import secrets

    return secrets.token_hex(16)


def _safe(s: str) -> str:
    return s.replace("/", "_").replace(" ", "_")


def _backfill_markdown_key(view: _RowView) -> str:
    year = f"{view.year:04d}" if view.year else "unknown"
    return (
        f"openreview/backfill/venue={_safe(view.venue)}/year={year}/"
        f"{_safe(view.note_id) or 'unknown'}.md.gz"
    )


def _backfill_review_key(view: _RowView) -> str:
    year = f"{view.year:04d}" if view.year else "unknown"
    return (
        f"openreview/backfill/venue={_safe(view.venue)}/year={year}/reviews/"
        f"{_safe(view.note_id) or 'unknown'}.json.gz"
    )


async def _emit_markdown(
    *,
    view: _RowView,
    cfg: IngestConfig,
    producer: BronzeProducer,
    minio: MinioWriter,
    admission_producer: LicenseAdmissionProducer,
) -> bool:
    if not view.markdown or not view.note_id:
        return False
    url = f"https://openreview.net/pdf?id={view.note_id}"
    license_source = "dataset_metadata" if view.paper_license else "unknown"
    admission = decide_license_admission(
        source_url=url,
        source_feed=SOURCE_FEED,
        license_value=view.paper_license,
        license_source=license_source,
        source_format="markdown",
        resolver="reviewarena-paper-item-field",
        evidence_url=url,
        evidence_revision=view.note_id,
        evidence_scope="item" if view.paper_license else "unknown",
    )
    await admission_producer.send(admission.decision)
    if not admission.fetch_allowed:
        return False
    fetched_at = datetime.now(tz=UTC)
    cdate = view.cdate or fetched_at
    key = _backfill_markdown_key(view)
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
        payload=view.markdown.encode("utf-8"),
        content_type="text/markdown; charset=utf-8",
        gzip_compress=True,
        metadata=metadata,
    )
    record = BronzeRecord(
        doc_id=doc_id_for_url(url),
        url=url,  # type: ignore[arg-type]
        fetched_at=fetched_at,
        http_status=200,
        content_type="text/markdown; charset=utf-8",
        raw_html_s3_uri=f"s3://{cfg.minio_bronze_bucket}/{key}",
        source_feed=SOURCE_FEED,
        trace_id=admission.decision.trace_id,
        bytes_size=bytes_size,
        source_format="markdown",
        extraction_pipeline=PIPELINE_MARKDOWN_BACKFILL,
        spdx_license=admission.license_id,
        training_usage=admission.training_usage,
        spdx_license_source=license_source,
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
    license_source = "dataset_metadata" if view.review_license else "openreview_terms"
    admission = decide_license_admission(
        source_url=url,
        source_feed=SOURCE_FEED,
        license_value=view.review_license or "CC-BY-4.0",
        license_source=license_source,
        source_format="review",
        resolver=(
            "reviewarena-review-item-field"
            if view.review_license
            else "openreview-public-comment-terms"
        ),
        evidence_url=url if view.review_license else OPENREVIEW_TERMS_URL,
        evidence_revision=view.note_id if view.review_license else OPENREVIEW_TERMS_REVISION,
        evidence_scope="item" if view.review_license else "source_terms",
    )
    await admission_producer.send(admission.decision)
    if not admission.fetch_allowed:
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
        training_usage=admission.training_usage,
        spdx_license_source=license_source,
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
    split: str,
    revision: str = DEFAULT_REVIEWARENA_REVISION,
    streaming_loader: Any | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield rows from ``dataset_id`` in streaming mode.

    ``streaming_loader`` is exposed so tests can pass a synthetic iterable in
    place of ``datasets.load_dataset``.
    """
    if streaming_loader is not None:
        yield from streaming_loader(dataset_id, revision=revision, split=split)
        return
    from datasets import load_dataset

    ds = load_dataset(dataset_id, revision=revision, split=split, streaming=True)
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
    revision: str = DEFAULT_REVIEWARENA_REVISION,
    splits: Iterable[str] | None = None,
) -> BackfillStats:
    """Stream REVIEWARENA into bronze.

    ``max_rows`` is the small-scale-first knob from CLAUDE.md; pass e.g. 100
    for a smoke test, omit for the full run.
    """
    resolved = dataset_id or resolve_reviewarena_id(
        override=os.environ.get("S2P_REVIEWARENA_DATASET"),
        search_fn=search_fn,
    )
    chosen_splits = tuple(splits or DEFAULT_REVIEWARENA_SPLITS)
    log.info(
        "reviewarena.resolved",
        dataset_id=resolved,
        revision=revision,
        splits=chosen_splits,
    )
    stats = BackfillStats(dataset_id=resolved)
    failures: list[str] = []
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
        for split in chosen_splits:
            for raw in stream_dataset(
                resolved,
                split=split,
                revision=revision,
                streaming_loader=streaming_loader,
            ):
                stats.rows_seen += 1
                view = _RowView.from_dict(raw)
                try:
                    if await _emit_markdown(
                        view=view,
                        cfg=cfg,
                        producer=producer,
                        minio=minio,
                        admission_producer=admission_producer,
                    ):
                        stats.papers_emitted += 1
                except Exception as exc:
                    log.warning("reviewarena.markdown_emit_failed", err=str(exc))
                    stats.skipped += 1
                    failures.append(f"{split}:{view.note_id}:paper")
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
                    failures.append(f"{split}:{view.note_id}:review")
                if max_rows is not None and stats.rows_seen >= max_rows:
                    break
            if max_rows is not None and stats.rows_seen >= max_rows:
                break
    if failures:
        raise RuntimeError(
            f"ReviewArena backfill had {len(failures)} durable-emission failures; "
            f"first={failures[0]}"
        )
    return stats


def main() -> None:
    cfg = load_config()
    configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    init_tracer("ingest.openreview_poller.backfill", cfg)
    log.info("openreview_backfill.start")
    max_rows_env = os.environ.get("S2P_BACKFILL_MAX_ROWS")
    max_rows = int(max_rows_env) if max_rows_env and max_rows_env.isdigit() else None
    dataset_id = os.environ.get("S2P_REVIEWARENA_DATASET") or None
    revision = os.environ.get("S2P_REVIEWARENA_REVISION", DEFAULT_REVIEWARENA_REVISION)
    splits_env = os.environ.get("S2P_REVIEWARENA_SPLITS")
    splits = (
        tuple(value.strip() for value in splits_env.split(",") if value.strip())
        if splits_env
        else None
    )
    stats = asyncio.run(
        run_backfill(
            cfg,
            dataset_id=dataset_id,
            max_rows=max_rows,
            revision=revision,
            splits=splits,
        )
    )
    log.info(
        "openreview_backfill.done",
        dataset=stats.dataset_id,
        rows_seen=stats.rows_seen,
        papers=stats.papers_emitted,
        reviews=stats.reviews_emitted,
        skipped=stats.skipped,
    )


if __name__ == "__main__":
    main()
