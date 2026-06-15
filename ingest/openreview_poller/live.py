"""OpenReview API v2 live poller (CronJob).

For each configured venue we call ``OpenReviewClient.get_all_notes`` against
``api2.openreview.net`` with the venue submission invitation
(e.g. ``ICLR.cc/2026/Conference/-/Submission``). The library handles
pagination internally (server-side streaming, page cap 1000 per call).

For each unseen submission note we:

1. Fetch the binary PDF over HTTPS via the shared httpx client (so the same
   retry/backoff/jitter as the rest of ingest) and PUT it under
   ``s3://<bronze>/openreview/venue=<v>/year=YYYY/<note_id>.pdf``.
2. Emit one ``BronzeRecord`` with ``source_format="pdf"`` and
   ``extraction_pipeline="openreview-pdf-pending-marker"``. The marker-pdf
   sidecar (Phase-2) consumes ``raw.fetched`` filtering on this pipeline tag
   and produces the Silver record once it lands.
3. Pull every reply note attached to the same ``forum`` (reviews, decisions,
   rebuttals) via ``get_notes(forum=<note_id>)``. Each reply is a separate
   ``source_format="review"`` BronzeRecord whose payload is the JSON-serialized
   note content; review prose is plain text and flows straight to Gold.

Validity-interval (N2 novelty) on the BronzeRecord is implicit:
``valid_from = note.cdate`` (UTC). ``valid_to`` stays ``None`` until the next
edit revision deprecates it. The Iceberg writer reads ``cdate`` from the
metadata blob and writes the validity columns.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ingest.common.config import IngestConfig, load_config
from ingest.common.hashing import doc_id_for_url
from ingest.common.http_client import build_async_client, build_headers
from ingest.common.kafka_producer import BronzeProducer
from ingest.common.logging import configure_logging, get_logger
from ingest.common.minio_writer import MinioWriter
from ingest.common.otel import init_tracer
from ingest.common.rate_limit import TokenBucket
from ingest.common.state import FeedStateStore
from schemas.bronze import BronzeRecord

log = get_logger(__name__)

OPENREVIEW_BASE_V2 = "https://api2.openreview.net"
OPENREVIEW_PDF_BASE = "https://openreview.net/pdf"
SOURCE_FEED = "openreview"
PIPELINE_PDF_PENDING = "openreview-pdf-pending-marker"
PIPELINE_REVIEW = "openreview-review-text"

# Default venue list. Each entry is (venue_path, year). The values.yaml
# ``openreviewPoller.venues`` block overrides this. Year coverage is
# ``needs-measurement``: REVIEWARENA cites ICLR 2020-2026, NeurIPS 2021-2025,
# ICML 2025, COLM 2024-2025; we mirror the upper bounds here.
DEFAULT_VENUES: tuple[tuple[str, int], ...] = (
    ("ICLR.cc", 2026),
    ("NeurIPS.cc", 2025),
    ("ICML.cc", 2025),
    ("COLM", 2025),
)


@dataclass(slots=True)
class VenueSpec:
    """One venue/year tuple to poll."""

    venue: str
    year: int

    @property
    def venue_id(self) -> str:
        # NeurIPS.cc/2025/Conference, ICLR.cc/2026/Conference,
        # ICML.cc/2025/Conference, COLM/2025/Conference. The "/Conference"
        # suffix is the OpenReview convention for the public submission group.
        return f"{self.venue}/{self.year}/Conference"

    @property
    def submission_invitation(self) -> str:
        return f"{self.venue_id}/-/Submission"

    @property
    def state_key(self) -> str:
        safe_venue = self.venue.replace("/", "_").replace(".", "_")
        return f"{SOURCE_FEED}:{safe_venue}:{self.year}"


@dataclass(slots=True)
class LiveStats:
    """Accumulated counters returned by ``run_pass``."""

    venues_polled: int = 0
    submissions_emitted: int = 0
    reviews_emitted: int = 0
    skipped_seen: int = 0
    pdf_failures: int = 0


@dataclass(slots=True)
class _NoteView:
    """Subset of OpenReview note fields the poller actually reads.

    We accept either ``openreview.api.Note`` instances or plain dicts (used in
    tests) by going through this view. ``content`` values in v2 are
    ``{ "value": ... }`` dicts; ``_extract_value`` flattens them.
    """

    id: str
    forum: str
    invitation: str
    cdate_ms: int | None
    mdate_ms: int | None
    content: dict[str, Any]
    pdf_path: str | None
    title: str | None
    venue_id: str

    @classmethod
    def from_obj(cls, obj: Any, *, venue_id: str) -> "_NoteView":
        if hasattr(obj, "to_json"):
            data = obj.to_json()
        elif isinstance(obj, dict):
            data = obj
        else:
            data = {
                k: getattr(obj, k, None)
                for k in ("id", "forum", "invitation", "cdate", "mdate", "content")
            }
        content = dict(data.get("content") or {})
        flat = {k: _extract_value(v) for k, v in content.items()}
        invitation = data.get("invitation") or ""
        if isinstance(invitation, list):
            invitation = invitation[0] if invitation else ""
        return cls(
            id=str(data.get("id") or ""),
            forum=str(data.get("forum") or data.get("id") or ""),
            invitation=str(invitation),
            cdate_ms=_int_or_none(data.get("cdate")),
            mdate_ms=_int_or_none(data.get("mdate")),
            content=flat,
            pdf_path=_pdf_path_from_content(flat),
            title=_str_or_none(flat.get("title")),
            venue_id=venue_id,
        )


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _str_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _extract_value(node: Any) -> Any:
    """OpenReview v2 wraps every content field as ``{"value": ...}``."""
    if isinstance(node, dict) and set(node.keys()) >= {"value"}:
        return node["value"]
    return node


def _pdf_path_from_content(content: dict[str, Any]) -> str | None:
    """Pull the ``/pdf/<id>.pdf`` relative path out of the note content."""
    pdf = content.get("pdf")
    if isinstance(pdf, str) and pdf:
        return pdf if pdf.startswith("/") else f"/{pdf}"
    return None


def _trace_id() -> str:
    import secrets

    return secrets.token_hex(16)


def _ms_to_dt(ms: int | None) -> datetime | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _venue_pdf_key(venue: VenueSpec, note_id: str) -> str:
    """Object key under the bronze bucket for a submission PDF."""
    safe_venue = venue.venue.replace("/", "_")
    return f"openreview/venue={safe_venue}/year={venue.year:04d}/{note_id}.pdf"


def _venue_review_key(venue: VenueSpec, note_id: str) -> str:
    safe_venue = venue.venue.replace("/", "_")
    return f"openreview/venue={safe_venue}/year={venue.year:04d}/reviews/{note_id}.json.gz"


# ---------------------------------------------------------------------------
# OpenReview client wrapper (so tests can inject a fake)
# ---------------------------------------------------------------------------


class OpenReviewClientProtocol:
    """Structural type implemented by ``openreview.api.OpenReviewClient``.

    Defined as a regular class (not ``typing.Protocol``) so we can also use it
    as a base for the fake client in tests without runtime metaclass clashes.
    """

    def get_all_notes(self, *, invitation: str, **kwargs: Any) -> Iterable[Any]:
        raise NotImplementedError

    def get_notes(self, **kwargs: Any) -> Iterable[Any]:
        raise NotImplementedError


def build_openreview_client(
    *,
    baseurl: str = OPENREVIEW_BASE_V2,
    token: str | None = None,
) -> OpenReviewClientProtocol:
    """Construct the real openreview-py v2 client.

    The poller works fine anonymously (the submission notes are public after
    the venue's release date), but a token unlocks pre-release access for
    venues that have already opened reviewing. ``OPENREVIEW_TOKEN`` env var is
    honored.
    """
    import openreview  # noqa: PLC0415 - optional dependency at import time

    return openreview.api.OpenReviewClient(
        baseurl=baseurl,
        token=token or os.environ.get("OPENREVIEW_TOKEN"),
    )


# ---------------------------------------------------------------------------
# Poller core
# ---------------------------------------------------------------------------


def parse_venues(raw: Iterable[str] | None) -> list[VenueSpec]:
    """Parse a list of ``Venue/YYYY/Conference`` strings into ``VenueSpec``s."""
    if raw is None:
        return [VenueSpec(v, y) for v, y in DEFAULT_VENUES]
    out: list[VenueSpec] = []
    for entry in raw:
        cleaned = entry.strip()
        if not cleaned:
            continue
        # Accept both "ICLR.cc/2026/Conference" and "ICLR.cc/2026".
        parts = cleaned.split("/")
        if len(parts) < 2:
            log.warning("openreview.bad_venue", entry=cleaned)
            continue
        venue = parts[0]
        try:
            year = int(parts[1])
        except ValueError:
            log.warning("openreview.bad_venue_year", entry=cleaned)
            continue
        out.append(VenueSpec(venue, year))
    return out


async def fetch_pdf_bytes(client_factory: Any, pdf_url: str) -> tuple[int, bytes]:
    """GET a PDF binary via the shared httpx client.

    ``client_factory`` is an already-built ``httpx.AsyncClient``. Returns
    (status, body). Raises on transport errors via the client's retry
    transport.
    """
    resp = await client_factory.get(pdf_url)
    return resp.status_code, resp.content


async def emit_submission(
    *,
    note: _NoteView,
    venue: VenueSpec,
    cfg: IngestConfig,
    producer: BronzeProducer,
    minio: MinioWriter,
    http: Any,
    bucket: TokenBucket,
) -> bool:
    """Fetch + persist one submission. Returns True iff a record was emitted."""
    if not note.pdf_path:
        log.warning("openreview.note_missing_pdf", note=note.id, venue=venue.venue_id)
        return False
    pdf_url = f"https://openreview.net{note.pdf_path}"
    await bucket.acquire()
    try:
        status, body = await fetch_pdf_bytes(http, pdf_url)
    except Exception as exc:  # noqa: BLE001 - network errors logged
        log.warning("openreview.pdf_fetch_failed", note=note.id, err=str(exc))
        return False
    if status != 200 or not body:
        log.warning("openreview.pdf_bad_status", note=note.id, status=status)
        return False

    fetched_at = datetime.now(tz=timezone.utc)
    cdate = _ms_to_dt(note.cdate_ms) or fetched_at
    key = _venue_pdf_key(venue, note.id)
    metadata = {
        "doc_id": doc_id_for_url(pdf_url),
        "source_feed": SOURCE_FEED,
        "openreview_note_id": note.id,
        "openreview_forum": note.forum,
        "openreview_venue_id": venue.venue_id,
        "openreview_invitation": note.invitation,
        "valid_from": cdate.isoformat(),
    }
    bytes_size = await minio.put_bronze(
        key=key,
        payload=body,
        content_type="application/pdf",
        gzip_compress=False,  # PDFs are already compressed.
        metadata=metadata,
    )
    record = BronzeRecord(
        doc_id=doc_id_for_url(pdf_url),
        url=pdf_url,  # type: ignore[arg-type]
        fetched_at=fetched_at,
        http_status=200,
        content_type="application/pdf",
        raw_html_s3_uri=f"s3://{cfg.minio_bronze_bucket}/{key}",
        source_feed=SOURCE_FEED,
        trace_id=_trace_id(),
        bytes_size=bytes_size,
        source_format="pdf",
        extraction_pipeline=PIPELINE_PDF_PENDING,
        spdx_license=None,
        spdx_license_source="unknown",
    )
    await producer.send(
        record,
        headers={
            "openreview_note_id": note.id,
            "openreview_venue_id": venue.venue_id,
            "valid_from": cdate.isoformat(),
        },
    )
    return True


async def emit_review_thread(
    *,
    forum_id: str,
    notes: Iterable[_NoteView],
    venue: VenueSpec,
    cfg: IngestConfig,
    producer: BronzeProducer,
    minio: MinioWriter,
) -> int:
    """Persist every reply note (review/decision/rebuttal) under a forum."""
    emitted = 0
    fetched_at = datetime.now(tz=timezone.utc)
    for note in notes:
        if note.id == forum_id:
            # The forum-root note is the submission itself; already handled.
            continue
        # Skip empty review payloads (some venues post auto-generated stubs).
        if not note.content:
            continue
        url = f"https://openreview.net/forum?id={forum_id}&noteId={note.id}"
        cdate = _ms_to_dt(note.cdate_ms) or fetched_at
        payload = json.dumps(
            {
                "id": note.id,
                "forum": forum_id,
                "invitation": note.invitation,
                "cdate": note.cdate_ms,
                "mdate": note.mdate_ms,
                "content": note.content,
            },
            sort_keys=True,
            ensure_ascii=False,
        ).encode("utf-8")
        key = _venue_review_key(venue, note.id)
        metadata = {
            "doc_id": doc_id_for_url(url),
            "source_feed": SOURCE_FEED,
            "openreview_note_id": note.id,
            "openreview_forum": forum_id,
            "openreview_venue_id": venue.venue_id,
            "openreview_invitation": note.invitation,
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
            trace_id=_trace_id(),
            bytes_size=bytes_size,
            source_format="review",
            extraction_pipeline=PIPELINE_REVIEW,
            spdx_license=None,
            spdx_license_source="unknown",
        )
        await producer.send(
            record,
            headers={
                "openreview_note_id": note.id,
                "openreview_venue_id": venue.venue_id,
                "valid_from": cdate.isoformat(),
            },
        )
        emitted += 1
    return emitted


def _iter_notes(items: Any, *, venue_id: str) -> Iterator[_NoteView]:
    """Coerce the openreview-py ``get_all_notes`` return to ``_NoteView``s."""
    for obj in items or ():
        try:
            yield _NoteView.from_obj(obj, venue_id=venue_id)
        except Exception as exc:  # noqa: BLE001
            log.warning("openreview.note_parse_failed", err=str(exc))
            continue


async def poll_venue(
    venue: VenueSpec,
    *,
    cfg: IngestConfig,
    producer: BronzeProducer,
    minio: MinioWriter,
    http: Any,
    or_client: OpenReviewClientProtocol,
    state_store: FeedStateStore,
    bucket: TokenBucket,
) -> tuple[int, int, int]:
    """One venue pass; returns (submissions_emitted, reviews_emitted, skipped)."""
    state = state_store.get(venue.state_key)
    seen: set[str] = set(state.get("seen_note_ids", []))

    # openreview-py is synchronous (uses requests, paginates server-side).
    # A venue with thousands of submissions can take many seconds; running
    # it directly inside the asyncio loop blocks the PDF downloader, MinIO
    # PUTs, Kafka heartbeats, and the rate-limit token bucket. Off-load to
    # the default executor so the event loop stays responsive.
    raw_submissions = await asyncio.to_thread(
        or_client.get_all_notes, invitation=venue.submission_invitation
    )
    submissions = list(_iter_notes(raw_submissions, venue_id=venue.venue_id))
    log.info(
        "openreview.venue_listed",
        venue=venue.venue_id,
        total=len(submissions),
        seen=len(seen),
    )

    submissions_emitted = 0
    reviews_emitted = 0
    skipped = 0
    for note in submissions:
        if note.id in seen:
            skipped += 1
            continue
        ok = await emit_submission(
            note=note,
            venue=venue,
            cfg=cfg,
            producer=producer,
            minio=minio,
            http=http,
            bucket=bucket,
        )
        if not ok:
            continue
        submissions_emitted += 1
        # Pull the review thread for the same forum; openreview-py's
        # get_notes(forum=...) returns the root note plus all replies.
        # Same async-blocking concern as the get_all_notes call above.
        try:
            raw_replies = await asyncio.to_thread(
                or_client.get_notes, forum=note.forum
            )
            forum_replies = list(
                _iter_notes(raw_replies, venue_id=venue.venue_id)
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("openreview.forum_fetch_failed", note=note.id, err=str(exc))
            forum_replies = []
        reviews_emitted += await emit_review_thread(
            forum_id=note.forum,
            notes=forum_replies,
            venue=venue,
            cfg=cfg,
            producer=producer,
            minio=minio,
        )
        seen.add(note.id)

    # Bound the seen-set; OpenReview venues stay below 10k submissions, so we
    # keep up to 20k ids to span two seasons.
    if len(seen) > 20000:
        seen = set(sorted(seen)[-10000:])
    state_store.put(venue.state_key, {"seen_note_ids": sorted(seen)})
    return submissions_emitted, reviews_emitted, skipped


async def run_pass(
    cfg: IngestConfig,
    *,
    venues: list[VenueSpec] | None = None,
    or_client: OpenReviewClientProtocol | None = None,
    rate_per_second: float = 1.0,
    burst: int = 4,
    state_dir: str | None = None,
) -> LiveStats:
    """Drive one full pass over every configured venue."""
    chosen = venues or [VenueSpec(v, y) for v, y in DEFAULT_VENUES]
    state_store = FeedStateStore(
        state_dir
        or ("./.s2p-state/openreview" if cfg.is_dev else "/var/lib/s2p-state/openreview_poller")
    )
    bucket = TokenBucket(rate=rate_per_second, burst=burst)
    headers = build_headers(cfg, accept="application/pdf")
    or_client = or_client or build_openreview_client()

    stats = LiveStats()
    async with (
        BronzeProducer(
            cfg.redpanda_brokers, topic=cfg.raw_topic, client_id="s2p-openreview-poller"
        ) as producer,
        MinioWriter(
            cfg.minio_endpoint,
            cfg.minio_access_key,
            cfg.minio_secret_key,
            bucket=cfg.minio_bronze_bucket,
        ) as minio,
        build_async_client(cfg, headers=headers) as http,
    ):
        for venue in chosen:
            try:
                submissions, reviews, skipped = await poll_venue(
                    venue,
                    cfg=cfg,
                    producer=producer,
                    minio=minio,
                    http=http,
                    or_client=or_client,
                    state_store=state_store,
                    bucket=bucket,
                )
            except Exception as exc:  # noqa: BLE001 - log + continue
                log.warning("openreview.venue_failed", venue=venue.venue_id, err=str(exc))
                continue
            stats.venues_polled += 1
            stats.submissions_emitted += submissions
            stats.reviews_emitted += reviews
            stats.skipped_seen += skipped
    return stats


def main() -> None:
    cfg = load_config()
    configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    init_tracer("ingest.openreview_poller.live", cfg)
    log.info("openreview_live.start")
    venues_env = os.environ.get("S2P_OPENREVIEW_VENUES")
    venues = parse_venues(venues_env.split(",")) if venues_env else None
    stats = asyncio.run(run_pass(cfg, venues=venues))
    log.info(
        "openreview_live.done",
        venues=stats.venues_polled,
        submissions=stats.submissions_emitted,
        reviews=stats.reviews_emitted,
        skipped=stats.skipped_seen,
    )


if __name__ == "__main__":
    main()
