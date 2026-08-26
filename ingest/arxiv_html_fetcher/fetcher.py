"""Live arXiv full-paper-body fetcher.

The worker subscribes to the discovery stream, selects arXiv records, and
fetches the full artifact. The processor downstream of ``raw.fetched`` then
builds the structured scientific projection.

Both modes reuse :mod:`ingest.common.http_client` (retry, polite UA),
:class:`ingest.common.rate_limit.TokenBucket` (4 req/s + 1 s sleep per
arXiv recommendation), and the standard MinIO + Bronze producer wiring.

Per-paper semantics:

- 200 from ``arxiv.org/html/<id>`` -> :class:`ExtractedDocument` from
  :mod:`extractor`, ``source_format="html"``,
  ``extraction_pipeline="arxiv-html-2026-06"``.
- 404 from ``arxiv.org/html/<id>`` -> retry against
  ``ar5iv.labs.arxiv.org/html/<id>``; on 200, ``extraction_pipeline``
  becomes ``"ar5iv-2026-06"``.
- both 404 -> fetch ``arxiv.org/pdf/<id>`` and emit ``source_format="pdf"``
  for the bounded CPU Docling fallback. A metadata diagnostic is emitted only
  when the PDF is unavailable too.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import secrets
from collections.abc import AsyncIterator, Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from ingest.arxiv_html_fetcher.extractor import (
    AR5IV_PIPELINE,
    ARXIV_PIPELINE,
    ExtractedDocument,
    extract_arxiv_html,
)
from ingest.common.arxiv_license import (
    fetch_arxiv_license as fetch_arxiv_license,
)
from ingest.common.arxiv_license import (
    fetch_arxiv_license_with_source,
)
from ingest.common.config import IngestConfig, load_config
from ingest.common.hashing import doc_id_for_url
from ingest.common.http_client import build_async_client, build_headers
from ingest.common.kafka_producer import BronzeProducer, LicenseAdmissionProducer
from ingest.common.license_admission import decide_license_admission, normalize_license
from ingest.common.logging import configure_logging, get_logger
from ingest.common.minio_writer import MinioWriter
from ingest.common.otel import init_tracer
from ingest.common.rate_limit import TokenBucket
from ingest.common.s3 import bronze_object_key, bronze_s3_uri
from schemas.bronze import BronzeRecord

log = get_logger(__name__)

ARXIV_HTML_BASE = "https://arxiv.org/html"
AR5IV_HTML_BASE = "https://ar5iv.labs.arxiv.org/html"
ARXIV_PDF_BASE = "https://arxiv.org/pdf"

# arXiv asks for max 4 req/s plus a 1 s sleep between requests. We honour
# both via the token bucket (rate=4/s, burst=4) and an explicit
# ``min_sleep`` floor inside the per-id loop.
_DEFAULT_RPS = 4.0
_DEFAULT_BURST = 4
_DEFAULT_MIN_SLEEP_S = 1.0
# arXiv discovery is sparse and bursty outside the weekday announcement
# window. A partially filled batch on a long-lived consumer would otherwise
# wait forever. Process one complete paper at a time and commit only after its
# admission/body/output path has finished.
_STREAM_BATCH_SIZE = 1


@dataclass(frozen=True, slots=True)
class ArxivCandidate:
    arxiv_id: str
    license_value: str | None = None
    license_source: str = "unknown"
    force_pdf: bool = False
    retry_attempt: int = 0


# ``2401.12345``, ``2401.12345v2`` (new scheme) and ``cs/0703123`` (legacy).
_ARXIV_ID_RE = re.compile(r"^(?:[a-z\-]+/)?\d{4}\.\d{4,6}(?:v\d+)?$|^[a-z\-]+/\d{7}(?:v\d+)?$")


def is_valid_arxiv_id(value: str) -> bool:
    """Light validator; reject obvious junk before hammering arXiv."""
    if not value:
        return False
    return _ARXIV_ID_RE.match(value.strip()) is not None


def canonical_arxiv_url(arxiv_id: str, *, mirror: str = "arxiv") -> str:
    """Build the canonical fetch URL for ``arxiv_id`` on the given mirror."""
    base = ARXIV_HTML_BASE if mirror == "arxiv" else AR5IV_HTML_BASE
    return f"{base}/{arxiv_id}"


class FetchOutcome:
    """Compact result from :func:`fetch_one`. Tagged-union via attributes."""

    __slots__ = (
        "etag",
        "extracted",
        "extraction_pipeline",
        "fallback_used",
        "fetched_at",
        "html",
        "last_modified",
        "source_format",
        "status",
        "url",
    )

    def __init__(
        self,
        *,
        status: int,
        url: str,
        html: bytes | None,
        extracted: ExtractedDocument | None,
        extraction_pipeline: str,
        fallback_used: bool,
        fetched_at: datetime,
        etag: str | None,
        last_modified: datetime | None,
        source_format: str = "html",
    ) -> None:
        self.status = status
        self.url = url
        self.html = html
        self.extracted = extracted
        self.extraction_pipeline = extraction_pipeline
        self.fallback_used = fallback_used
        self.fetched_at = fetched_at
        self.etag = etag
        self.last_modified = last_modified
        self.source_format = source_format


def _parse_last_modified(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        from email.utils import parsedate_to_datetime

        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


async def fetch_one(
    arxiv_id: str,
    client: httpx.AsyncClient,
    *,
    bucket: TokenBucket,
    min_sleep_s: float = _DEFAULT_MIN_SLEEP_S,
    force_pdf: bool = False,
) -> FetchOutcome:
    """Fetch a single arXiv id, falling back to ar5iv on 404.

    Returns a :class:`FetchOutcome`. The caller is responsible for turning
    that into a Bronze record - this function does **not** touch MinIO or
    the producer so it stays trivially testable with httpx mocks.
    """
    if force_pdf:
        return await _fetch_pdf(
            arxiv_id,
            client,
            bucket=bucket,
            min_sleep_s=min_sleep_s,
        )

    primary_url = canonical_arxiv_url(arxiv_id, mirror="arxiv")
    await bucket.acquire()
    if min_sleep_s > 0:
        await asyncio.sleep(min_sleep_s)
    fetched_at = datetime.now(tz=UTC)
    resp = await client.get(primary_url)

    if resp.status_code == 200 and resp.content and _looks_like_fulltext_html(resp.content):
        extracted = extract_arxiv_html(resp.content, pipeline=ARXIV_PIPELINE)
        return FetchOutcome(
            status=200,
            url=primary_url,
            html=resp.content,
            extracted=extracted,
            extraction_pipeline=ARXIV_PIPELINE,
            fallback_used=False,
            fetched_at=fetched_at,
            etag=resp.headers.get("etag"),
            last_modified=_parse_last_modified(resp.headers.get("last-modified")),
        )

    primary_missing = resp.status_code == 404 or (
        resp.status_code == 200 and not _looks_like_fulltext_html(resp.content)
    )
    if not primary_missing:
        # Anything other than 404 falls through to a metadata stub: the
        # retry transport already exhausted 5xx/429 paths, and 403 / 410 are
        # terminal.
        return FetchOutcome(
            status=resp.status_code,
            url=primary_url,
            html=None,
            extracted=None,
            extraction_pipeline=ARXIV_PIPELINE,
            fallback_used=False,
            fetched_at=fetched_at,
            etag=resp.headers.get("etag"),
            last_modified=_parse_last_modified(resp.headers.get("last-modified")),
            source_format="metadata",
        )

    fallback_url = canonical_arxiv_url(arxiv_id, mirror="ar5iv")
    await bucket.acquire()
    if min_sleep_s > 0:
        await asyncio.sleep(min_sleep_s)
    fb_fetched_at = datetime.now(tz=UTC)
    fb_resp = await client.get(fallback_url)

    if (
        fb_resp.status_code == 200
        and fb_resp.content
        and _looks_like_fulltext_html(fb_resp.content)
    ):
        extracted = extract_arxiv_html(fb_resp.content, pipeline=AR5IV_PIPELINE)
        return FetchOutcome(
            status=200,
            url=fallback_url,
            html=fb_resp.content,
            extracted=extracted,
            extraction_pipeline=AR5IV_PIPELINE,
            fallback_used=True,
            fetched_at=fb_fetched_at,
            etag=fb_resp.headers.get("etag"),
            last_modified=_parse_last_modified(fb_resp.headers.get("last-modified")),
        )

    fallback_missing = fb_resp.status_code == 404 or (
        fb_resp.status_code == 200 and not _looks_like_fulltext_html(fb_resp.content)
    )
    if not fallback_missing:
        return FetchOutcome(
            status=fb_resp.status_code,
            url=fallback_url,
            html=None,
            extracted=None,
            extraction_pipeline=AR5IV_PIPELINE,
            fallback_used=True,
            fetched_at=fb_fetched_at,
            etag=fb_resp.headers.get("etag"),
            last_modified=_parse_last_modified(fb_resp.headers.get("last-modified")),
            source_format="metadata",
        )

    return await _fetch_pdf(
        arxiv_id,
        client,
        bucket=bucket,
        min_sleep_s=min_sleep_s,
    )


async def _fetch_pdf(
    arxiv_id: str,
    client: httpx.AsyncClient,
    *,
    bucket: TokenBucket,
    min_sleep_s: float,
) -> FetchOutcome:
    """Fetch the immutable PDF path used by fallback and curation retry."""
    pdf_url = f"{ARXIV_PDF_BASE}/{arxiv_id}"
    await bucket.acquire()
    if min_sleep_s > 0:
        await asyncio.sleep(min_sleep_s)
    pdf_fetched_at = datetime.now(tz=UTC)
    pdf_resp = await client.get(pdf_url)
    content_type = pdf_resp.headers.get("content-type", "").lower()
    if (
        pdf_resp.status_code == 200
        and pdf_resp.content
        and ("application/pdf" in content_type or pdf_resp.content.startswith(b"%PDF"))
    ):
        return FetchOutcome(
            status=200,
            url=pdf_url,
            html=pdf_resp.content,
            extracted=None,
            extraction_pipeline="docling-pdf-cpu-2.114.0",
            fallback_used=True,
            fetched_at=pdf_fetched_at,
            etag=pdf_resp.headers.get("etag"),
            last_modified=_parse_last_modified(pdf_resp.headers.get("last-modified")),
            source_format="pdf",
        )

    return FetchOutcome(
        status=pdf_resp.status_code,
        url=pdf_url,
        html=None,
        extracted=None,
        extraction_pipeline="docling-pdf-cpu-2.114.0",
        fallback_used=True,
        fetched_at=pdf_fetched_at,
        etag=pdf_resp.headers.get("etag"),
        last_modified=_parse_last_modified(pdf_resp.headers.get("last-modified")),
        source_format="metadata",
    )


def _looks_like_fulltext_html(content: bytes) -> bool:
    """Reject arXiv abstract/landing pages returned through mirror redirects."""
    lowered = content[:262144].lower()
    return b"<article" in lowered or b"ltx_page_main" in lowered


def build_metadata_stub(arxiv_id: str, outcome: FetchOutcome) -> bytes:
    """Compact JSON body persisted to MinIO for fulltext-unavailable papers."""
    payload = {
        "arxiv_id": arxiv_id,
        "reason": "fulltext_unavailable",
        "primary_url": canonical_arxiv_url(arxiv_id, mirror="arxiv"),
        "ar5iv_url": canonical_arxiv_url(arxiv_id, mirror="ar5iv"),
        "ar5iv_status": outcome.status,
        "pdf_url": f"{ARXIV_PDF_BASE}/{arxiv_id}",
        "pdf_status": outcome.status,
        "schema_version": 1,
        "next_action": "manual_source_review",
    }
    return json.dumps(payload, sort_keys=True).encode("utf-8")


def make_bronze_record(
    *,
    arxiv_id: str,
    outcome: FetchOutcome,
    feed_name: str,
    bucket: str,
    admitted_license: str,
    admitted_license_source: str,
    training_usage: str = "pretrain_and_posttrain",
    bytes_size: int,
) -> tuple[BronzeRecord, str, str]:
    """Compose the BronzeRecord (and its MinIO key) for one outcome.

    Returns ``(record, key, content_type)`` so the caller can decide
    whether to gzip + upload before publishing.
    """
    if outcome.status == 200 and outcome.extracted is not None:
        extracted = outcome.extracted
        spdx = extracted.spdx_license or admitted_license
        spdx_source = "html_meta" if extracted.spdx_license else admitted_license_source
        source_format = "html"
        ext = "html.gz"
        content_type = "text/html"
    elif outcome.status == 200 and outcome.source_format == "pdf" and outcome.html:
        spdx = admitted_license
        spdx_source = admitted_license_source
        source_format = "pdf"
        ext = "pdf.gz"
        content_type = "application/pdf"
    else:
        spdx = admitted_license
        spdx_source = admitted_license_source
        source_format = "metadata"
        ext = "stub.json.gz"
        content_type = "application/json"

    # doc_id is content-anchored to the canonical arxiv.org URL regardless
    # of which mirror responded. ar5iv vs arxiv vs metadata-stub all map to
    # the same hash so re-runs (KEDA scale-up after an arxiv outage,
    # backfill replay, etc.) deduplicate at the bronze tier and produce a
    # single MinIO blob per arXiv id. The mirror that actually answered is
    # still recorded on the BronzeRecord ``url`` field via outcome.url.
    canonical_url = canonical_arxiv_url(arxiv_id, mirror="arxiv")
    doc_id = doc_id_for_url(canonical_url)
    key = bronze_object_key(
        source_feed=feed_name,
        doc_id=doc_id,
        fetched_at=outcome.fetched_at,
        extension=ext,
    )
    s3_uri = bronze_s3_uri(
        bucket=bucket,
        source_feed=feed_name,
        doc_id=doc_id,
        fetched_at=outcome.fetched_at,
        extension=ext,
    )
    record = BronzeRecord(
        doc_id=doc_id,
        url=outcome.url,  # type: ignore[arg-type]
        fetched_at=outcome.fetched_at,
        http_status=outcome.status,
        http_last_modified=outcome.last_modified,
        content_type=content_type,
        raw_html_s3_uri=s3_uri,
        source_feed=feed_name,
        trace_id=secrets.token_hex(16),
        etag=outcome.etag,
        bytes_size=bytes_size,
        source_format=source_format,  # type: ignore[arg-type]
        extraction_pipeline=outcome.extraction_pipeline,
        spdx_license=spdx,
        spdx_license_source=spdx_source,  # type: ignore[arg-type]
        training_usage=training_usage,  # type: ignore[arg-type]
    )
    return record, key, content_type


async def stream_ids_from_topic(
    cfg: IngestConfig,
    *,
    topic: str,
    consumer_group: str = "s2p-arxiv-html-fetcher-v2",
    auto_offset_reset: str = "latest",
    sources_filter: Iterable[str] | None = None,
    max_records: int | None = None,
    commit_callback: Callable[[Any], None] | None = None,
) -> AsyncIterator[ArxivCandidate]:
    """Yield arXiv ids from discovery envelopes on ``raw.fetched``.

    Offsets are NOT committed inside this generator: doing so would mean
    a pod kill between commit and successful emit/persist permanently
    drops the arXiv id (at-most-once semantics, contradicting the
    streaming-curator durability claim). Instead the caller is given a
    ``commit_callback`` it can invoke after :func:`run_for_ids` has
    successfully landed the bronze record. ``commit_callback`` receives
    the underlying ``aiokafka`` consumer; the caller picks the right
    moment (e.g. after a batch has been fully emitted) to call
    ``await consumer.commit()`` against it. Records that are filtered
    out (wrong ``source_feed`` or unparseable JSON) are skipped without
    yielding; the next successful commit_callback covers them too because
    Kafka commits highest-seen-offset semantics.
    """
    from aiokafka import AIOKafkaConsumer  # local import keeps tests light

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=cfg.redpanda_brokers,
        group_id=consumer_group,
        enable_auto_commit=False,
        auto_offset_reset=auto_offset_reset,
        client_id=consumer_group,
    )
    await consumer.start()
    emitted = 0
    if commit_callback is not None:
        commit_callback(consumer)
    try:
        async for msg in consumer:
            try:
                body = json.loads(msg.value)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            source = (body.get("source_feed") or "").strip()
            if not _is_arxiv_source_feed(source, sources_filter):
                continue
            url = body.get("url") or body.get("source_url") or ""
            arxiv_id = _arxiv_id_from_url(url) or body.get("arxiv_id")
            if not arxiv_id or not is_valid_arxiv_id(arxiv_id):
                continue
            pipeline = str(body.get("extraction_pipeline", ""))
            retry_match = re.search(r"\|curation-retry=(\d+)$", pipeline)
            yield ArxivCandidate(
                arxiv_id=arxiv_id,
                license_value=body.get("spdx_license") or body.get("license"),
                license_source=body.get("spdx_license_source") or "unknown",
                force_pdf=pipeline.startswith("arxiv-pdf-retry-v1"),
                retry_attempt=int(retry_match.group(1)) if retry_match else 0,
            )
            emitted += 1
            if max_records is not None and emitted >= max_records:
                return
    finally:
        await consumer.stop()


def _is_arxiv_source_feed(source: str, sources_filter: Iterable[str] | None) -> bool:
    """Match current SourceFeed names while retaining the older aliases."""
    if sources_filter is not None:
        return source in set(sources_filter)
    return source in {
        "oai-arxiv-cs",
        "arxiv-oai-cs",
        "arxiv-rss-cs",
        "arxiv-extraction-retry",
    } or source.startswith("rss-arxiv-cs-")


_ARXIV_URL_RE = re.compile(
    r"^https?://(?:arxiv\.org/(?:abs|pdf|html)|huggingface\.co/papers)/"
    r"([a-z\-]+/\d{7}(?:v\d+)?|\d{4}\.\d{4,6}(?:v\d+)?)",
    re.IGNORECASE,
)


def _arxiv_id_from_url(url: str) -> str | None:
    """Extract an arXiv id from any common arXiv URL shape."""
    if not url:
        return None
    m = _ARXIV_URL_RE.match(url.strip())
    return m.group(1) if m else None


async def run_for_ids(
    ids: Iterable[str | ArxivCandidate],
    cfg: IngestConfig,
    *,
    feed_name: str,
    rate_per_second: float = _DEFAULT_RPS,
    burst: int = _DEFAULT_BURST,
    min_sleep_s: float = _DEFAULT_MIN_SLEEP_S,
    transport: httpx.AsyncBaseTransport | None = None,
    producer_override: BronzeProducer | None = None,
    admission_producer_override: LicenseAdmissionProducer | None = None,
    minio_override: MinioWriter | None = None,
    raise_on_fetch_error: bool = False,
) -> int:
    """Fetch every arXiv id in ``ids`` and emit Bronze records. Returns count."""
    headers = build_headers(cfg, accept="text/html, application/xhtml+xml;q=0.9")
    bucket = TokenBucket(rate_per_second, burst)
    emitted = 0

    async with build_async_client(cfg, headers=headers, transport=transport) as client:
        producer_cm: BronzeProducer
        minio_cm: MinioWriter
        admission_cm: LicenseAdmissionProducer | None
        owns_producer = producer_override is None
        owns_minio = minio_override is None
        if producer_override is not None:
            producer_cm = producer_override
        else:
            producer_cm = BronzeProducer(
                cfg.redpanda_brokers,
                topic=cfg.raw_topic,
                client_id="s2p-arxiv-html-fetcher",
            )
        if admission_producer_override is not None:
            admission_cm = admission_producer_override
        elif producer_override is not None:
            admission_cm = None
        else:
            admission_cm = LicenseAdmissionProducer(
                cfg.redpanda_brokers,
                topic=cfg.license_admissions_topic,
                client_id="s2p-arxiv-license-admission",
            )
        if minio_override is not None:
            minio_cm = minio_override
        else:
            minio_cm = MinioWriter(
                cfg.minio_endpoint,
                cfg.minio_access_key,
                cfg.minio_secret_key,
                bucket=cfg.minio_bronze_bucket,
            )
        if owns_producer:
            await producer_cm.start()
        owns_admission_producer = admission_cm is not None and admission_producer_override is None
        if admission_cm is not None and admission_producer_override is None:
            await admission_cm.start()
        if owns_minio:
            await minio_cm.start()
        try:
            for candidate_value in ids:
                candidate = (
                    candidate_value
                    if isinstance(candidate_value, ArxivCandidate)
                    else ArxivCandidate(
                        arxiv_id=candidate_value,
                        license_value=None,
                        license_source="unknown",
                    )
                )
                arxiv_id = candidate.arxiv_id
                if not is_valid_arxiv_id(arxiv_id):
                    log.warning("arxiv_html.invalid_id", id=arxiv_id)
                    continue
                # Discovery decisions use the discovery record's identity. The
                # full-body record has a distinct canonical URL/doc_id, so it
                # resolves the canonical arXiv item again rather than copying a
                # licence value without its original evidence URL/revision.
                # This keeps the admission row joined to Gold independently
                # auditable and still occurs before any HTML/PDF request.
                license_value, license_source = await fetch_arxiv_license_with_source(
                    arxiv_id,
                    client,
                    bucket=bucket,
                    min_sleep_s=min_sleep_s,
                )
                admission = decide_license_admission(
                    source_url=canonical_arxiv_url(arxiv_id, mirror="arxiv"),
                    source_feed=feed_name,
                    license_value=license_value,
                    license_source=license_source,
                    resolver=f"arxiv:{license_source}",
                    evidence_url=f"https://arxiv.org/abs/{arxiv_id}",
                    evidence_revision=arxiv_id,
                    evidence_scope="item",
                )
                if admission_cm is not None:
                    await admission_cm.send(admission.decision)
                if not admission.fetch_allowed:
                    log.info(
                        "arxiv_html.license_quarantined",
                        arxiv_id=arxiv_id,
                        license=admission.license_id,
                    )
                    continue
                try:
                    outcome = await fetch_one(
                        arxiv_id,
                        client,
                        bucket=bucket,
                        min_sleep_s=min_sleep_s,
                        force_pdf=candidate.force_pdf,
                    )
                except httpx.HTTPError as exc:
                    log.exception(
                        "arxiv_html.http_error",
                        arxiv_id=arxiv_id,
                        err=str(exc),
                    )
                    if raise_on_fetch_error:
                        raise
                    continue
                if candidate.force_pdf:
                    outcome.extraction_pipeline = (
                        f"{outcome.extraction_pipeline}|curation-retry={candidate.retry_attempt}"
                    )

                fetched_license = (
                    outcome.extracted.spdx_license if outcome.extracted is not None else None
                )
                if fetched_license and normalize_license(fetched_license) != admission.license_id:
                    conflict = decide_license_admission(
                        source_url=canonical_arxiv_url(arxiv_id, mirror="arxiv"),
                        source_feed=feed_name,
                        license_value=(
                            f"conflict:{admission.license_id}|{normalize_license(fetched_license)}"
                        ),
                        license_source="html_meta",
                        resolver="arxiv:preflight-html-reconciliation",
                        evidence_url=f"https://arxiv.org/html/{arxiv_id}",
                        evidence_revision=arxiv_id,
                        evidence_scope="item",
                    )
                    if admission_cm is not None:
                        await admission_cm.send(conflict.decision)
                    log.warning(
                        "arxiv_html.license_conflict",
                        arxiv_id=arxiv_id,
                        preflight=admission.license_id,
                        fetched=normalize_license(fetched_license),
                    )
                    continue

                payload = (
                    outcome.html
                    if outcome.html is not None
                    else build_metadata_stub(arxiv_id, outcome)
                )
                record, key, content_type = make_bronze_record(
                    arxiv_id=arxiv_id,
                    outcome=outcome,
                    feed_name=feed_name,
                    bucket=cfg.minio_bronze_bucket,
                    admitted_license=admission.license_id,
                    admitted_license_source=license_source,
                    training_usage=admission.training_usage,
                    bytes_size=len(payload),
                )
                stored = await minio_cm.put_bronze(
                    key=key,
                    payload=payload,
                    content_type=content_type,
                    gzip_compress=True,
                    metadata={
                        "doc_id": record.doc_id,
                        "source_feed": feed_name,
                        "arxiv_id": arxiv_id,
                        "extraction_pipeline": record.extraction_pipeline,
                        "source_format": record.source_format,
                    },
                )
                # Refresh bytes_size with the gzipped count actually persisted.
                record = record.model_copy(
                    update={
                        "bytes_size": stored,
                        "trace_id": admission.decision.trace_id,
                        "spdx_license": admission.license_id,
                        "spdx_license_source": license_source,
                    }
                )
                await producer_cm.send(record)
                emitted += 1
                log.info(
                    "arxiv_html.fetched",
                    arxiv_id=arxiv_id,
                    pipeline=record.extraction_pipeline,
                    status=outcome.status,
                    fallback=outcome.fallback_used,
                    bytes=stored,
                )
        finally:
            if owns_producer:
                await producer_cm.stop()
            if owns_admission_producer and admission_cm is not None:
                await admission_cm.stop()
            if owns_minio:
                await minio_cm.stop()
    return emitted


def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="s2p-arxiv-html-fetcher",
        description="Fetch arXiv full-paper HTML and emit Bronze records.",
    )
    p.add_argument(
        "--feed-name",
        default="arxiv-html-fetcher",
        help="SourceFeed label written onto every Bronze record.",
    )
    p.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Stop after this many ids (stream mode only).",
    )
    p.add_argument(
        "--stream-topic",
        default="raw.fetched",
        help="Stream-mode subscription topic.",
    )
    p.add_argument(
        "--consumer-group",
        default="s2p-arxiv-html-fetcher-v2",
        help="Kafka consumer group for the live enrichment stream.",
    )
    p.add_argument(
        "--auto-offset-reset",
        choices=("earliest", "latest"),
        default="earliest",
        help="Initial offset for a new live consumer group.",
    )
    return p


async def _async_main(args: argparse.Namespace) -> int:
    cfg = load_config()
    configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    init_tracer("ingest.arxiv_html_fetcher", cfg)

    log.info("arxiv_html.stream.start", topic=args.stream_topic)
    total = await _run_stream(args, cfg)
    log.info("arxiv_html.stream.done", emitted=total)
    return total


async def _run_stream(args: argparse.Namespace, cfg: IngestConfig) -> int:
    """Drain one long-lived Kafka consumer in durable, bounded batches."""
    consumer_ref: dict[str, Any] = {}

    def _capture_consumer(c: Any) -> None:
        consumer_ref["consumer"] = c

    async def _drain(ids: list[ArxivCandidate]) -> int:
        emitted = await run_for_ids(
            ids,
            cfg,
            feed_name=args.feed_name,
            raise_on_fetch_error=True,
        )
        # A zero-emission batch can still be fully handled when every paper
        # was durably written to the licence-admission ledger. Commit after
        # successful batch handling, not only after an admitted paper.
        consumer = consumer_ref.get("consumer")
        if consumer is not None:
            await consumer.commit()
        return emitted

    total = 0
    batch: list[ArxivCandidate] = []
    async for candidate in stream_ids_from_topic(
        cfg,
        topic=args.stream_topic,
        consumer_group=args.consumer_group,
        auto_offset_reset=args.auto_offset_reset,
        max_records=args.max_records,
        commit_callback=_capture_consumer,
    ):
        batch.append(candidate)
        if len(batch) >= _STREAM_BATCH_SIZE:
            total += await _drain(batch)
            batch = []
    if batch:
        total += await _drain(batch)
    return total


def main() -> None:
    """CLI entrypoint."""
    args = _build_argparser().parse_args()
    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()
