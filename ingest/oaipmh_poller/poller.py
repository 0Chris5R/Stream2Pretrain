"""OAI-PMH CronJob entrypoint.

For each ``oai-pmh`` SourceFeed, do an incremental ``ListRecords`` from the
last seen ``until`` timestamp; for every record write the metadata XML directly
into bronze (no second HTTP fetch needed - OAI already returns the abstract
inline) and emit a BronzeRecord pointing at the OAI canonical URL.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from typing import Any

from ingest.common.config import IngestConfig, load_config
from ingest.common.feeds import (
    feeds_by_protocol,
    load_feeds_from_kube,
    load_feeds_from_yaml,
)
from ingest.common.http_client import build_async_client, build_headers
from ingest.common.kafka_producer import BronzeProducer, LicenseAdmissionProducer
from ingest.common.license_admission import decide_license_admission, normalize_license
from ingest.common.logging import configure_logging, get_logger
from ingest.common.minio_writer import MinioWriter
from ingest.common.otel import init_tracer
from ingest.common.rate_limit import TokenBucket
from ingest.common.s3 import bronze_object_key, bronze_s3_uri
from ingest.common.state import FeedStateStore
from ingest.oaipmh_poller.client import OAIClient
from schemas.bronze import BronzeRecord
from schemas.sourcefeed import SourceFeedSpec

log = get_logger(__name__)


def _today_iso() -> str:
    return datetime.now(tz=UTC).strftime("%Y-%m-%d")


async def poll_feed(
    feed: SourceFeedSpec,
    cfg: IngestConfig,
    *,
    set_spec: str = "cs",
    metadata_prefix: str = "arXiv",
    state_store: FeedStateStore,
    max_records: int | None = None,
    max_pages: int | None = None,
) -> int:
    """Run one OAI-PMH pass. Returns number of bronze records emitted."""
    feed_state = state_store.get(feed.name)
    window_from = str(feed_state.get("window_from") or feed_state.get("until") or "2024-01-01")
    window_until = str(feed_state.get("window_until") or _today_iso())
    resumption_token = feed_state.get("resumption_token")
    if not isinstance(resumption_token, str) or not resumption_token:
        resumption_token = None

    headers = build_headers(cfg, accept="application/xml, text/xml;q=0.9")
    bucket = TokenBucket(feed.rate_limit.requests_per_second, feed.rate_limit.burst)
    emitted = 0
    handled = 0
    pages_handled = 0
    async with (
        build_async_client(cfg, headers=headers) as client,
        BronzeProducer(
            cfg.redpanda_brokers, topic=cfg.raw_topic, client_id="s2p-oai-poller"
        ) as producer,
        LicenseAdmissionProducer(
            cfg.redpanda_brokers,
            topic=cfg.license_admissions_topic,
            client_id="s2p-oai-license-admission",
        ) as admission_producer,
        MinioWriter(
            cfg.minio_endpoint,
            cfg.minio_access_key,
            cfg.minio_secret_key,
            bucket=cfg.minio_bronze_bucket,
        ) as minio,
    ):
        oai = OAIClient(str(feed.endpoint), client)
        async for page in oai.list_pages(
            metadata_prefix=metadata_prefix,
            set_spec=set_spec,
            from_=window_from,
            until=window_until,
            resumption_token=resumption_token,
        ):
            for index, record in enumerate(page.records):
                await bucket.acquire()
                handled += 1
                if not record.deleted:
                    url = record.arxiv_abs_url() or f"oai://{feed.endpoint}/{record.identifier}"
                    per_record_license = normalize_license(record.license_value())
                    license_source = "oai_metadata"
                    if per_record_license == "unknown":
                        per_record_license = normalize_license(feed.license_default)
                        license_source = (
                            "manual_override" if per_record_license != "unknown" else "unknown"
                        )
                    trace_id = _random_trace_id()
                    decision_url = (
                        url
                        if url.startswith("http")
                        else f"{str(feed.endpoint).rstrip('/')}#record={record.identifier}"
                    )
                    admission = decide_license_admission(
                        source_url=decision_url,
                        source_feed=feed.name,
                        license_value=per_record_license,
                        license_source=license_source,
                        trace_id=trace_id,
                    )
                    await admission_producer.send(admission.decision)
                    if not admission.admitted:
                        log.info(
                            "oai.license_quarantined",
                            feed=feed.name,
                            identifier=record.identifier,
                            license=admission.license_id,
                        )
                    else:
                        fetched_at = datetime.now(tz=UTC)
                        key = bronze_object_key(
                            source_feed=feed.name,
                            doc_id=admission.decision.doc_id,
                            fetched_at=fetched_at,
                            extension="oai.xml.gz",
                        )
                        stored = await minio.put_bronze(
                            key=key,
                            payload=record.raw,
                            content_type="application/xml",
                            gzip_compress=True,
                            metadata={
                                "doc_id": admission.decision.doc_id,
                                "source_feed": feed.name,
                                "oai_identifier": record.identifier,
                            },
                        )
                        br = BronzeRecord(
                            doc_id=admission.decision.doc_id,
                            url=decision_url,  # type: ignore[arg-type]
                            fetched_at=fetched_at,
                            http_status=200,
                            content_type="application/xml",
                            raw_html_s3_uri=bronze_s3_uri(
                                bucket=cfg.minio_bronze_bucket,
                                source_feed=feed.name,
                                doc_id=admission.decision.doc_id,
                                fetched_at=fetched_at,
                                extension="oai.xml.gz",
                            ),
                            source_feed=feed.name,
                            trace_id=trace_id,
                            bytes_size=stored,
                            spdx_license=admission.license_id,
                            spdx_license_source=license_source,  # type: ignore[arg-type]
                        )
                        await producer.send(br)
                        emitted += 1

                if (
                    max_records is not None
                    and handled >= max_records
                    and index + 1 < len(page.records)
                ):
                    # A test/local bounded read stopped inside a page. Do not
                    # advance the page token; replaying the page is the only
                    # way to avoid skipping its unhandled records.
                    return emitted

            pages_handled += 1
            if page.resumption_token:
                state_store.put(
                    feed.name,
                    {
                        "window_from": window_from,
                        "window_until": window_until,
                        "resumption_token": page.resumption_token,
                    },
                )
                if max_pages is not None and pages_handled >= max_pages:
                    log.info(
                        "oai.window_paused",
                        feed=feed.name,
                        pages=pages_handled,
                        next_token=True,
                    )
                    return emitted
            else:
                state_store.put(feed.name, {"until": window_until})
                return emitted

    return emitted


def _random_trace_id() -> str:
    import secrets

    return secrets.token_hex(16)


def _sha256(text: str) -> str:
    import hashlib

    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _load_feeds(cfg: IngestConfig) -> list[SourceFeedSpec]:
    feeds = (
        load_feeds_from_yaml(cfg.feed_config_path)
        if cfg.feed_config_path
        else load_feeds_from_kube()
    )
    return feeds_by_protocol(feeds, "oai-pmh")


async def _run(cfg: IngestConfig, feeds: list[SourceFeedSpec], **kw: Any) -> int:
    state_root = "/var/lib/s2p-state/oaipmh_poller" if not cfg.is_dev else "./.s2p-state/oaipmh"
    state_store = FeedStateStore(state_root)
    total = 0
    for feed in feeds:
        try:
            total += await poll_feed(feed, cfg, state_store=state_store, **kw)
        except Exception as exc:
            log.exception("oai.feed.error", feed=feed.name, err=str(exc))
    return total


def main() -> None:
    """CronJob entrypoint."""
    cfg = load_config()
    configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    init_tracer("ingest.oaipmh_poller", cfg)
    feeds = _load_feeds(cfg)
    log.info("oaipmh_poller.start", feeds=len(feeds))
    if not feeds:
        log.warning("oaipmh_poller.no_feeds")
        return
    raw_max_pages = os.environ.get("S2P_OAI_MAX_PAGES_PER_RUN", "").strip()
    max_pages = int(raw_max_pages) if raw_max_pages else None
    if max_pages is not None and max_pages <= 0:
        max_pages = None
    emitted = asyncio.run(_run(cfg, feeds, max_pages=max_pages))
    log.info("oaipmh_poller.done", emitted=emitted)


if __name__ == "__main__":
    main()
