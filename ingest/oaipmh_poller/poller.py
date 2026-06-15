"""OAI-PMH CronJob entrypoint.

For each ``oai-pmh`` SourceFeed, do an incremental ``ListRecords`` from the
last seen ``until`` timestamp; for every record write the metadata XML directly
into bronze (no second HTTP fetch needed - OAI already returns the abstract
inline) and emit a BronzeRecord pointing at the OAI canonical URL.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from ingest.common.config import IngestConfig, load_config
from ingest.common.feeds import (
    feeds_by_protocol,
    load_feeds_from_kube,
    load_feeds_from_yaml,
)
from ingest.common.hashing import doc_id_for_url
from ingest.common.http_client import build_async_client, build_headers
from ingest.common.kafka_producer import BronzeProducer
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
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")


async def poll_feed(
    feed: SourceFeedSpec,
    cfg: IngestConfig,
    *,
    set_spec: str = "cs",
    metadata_prefix: str = "arXiv",
    state_store: FeedStateStore,
    max_records: int | None = None,
) -> int:
    """Run one OAI-PMH pass. Returns number of bronze records emitted."""
    feed_state = state_store.get(feed.name)
    from_ts = feed_state.get("until") or "2024-01-01"

    headers = build_headers(cfg, accept="application/xml, text/xml;q=0.9")
    bucket = TokenBucket(
        feed.rate_limit.requests_per_second, feed.rate_limit.burst
    )
    new_until = _today_iso()

    emitted = 0
    async with build_async_client(cfg, headers=headers) as client, BronzeProducer(
        cfg.redpanda_brokers, topic=cfg.raw_topic, client_id="s2p-oai-poller"
    ) as producer, MinioWriter(
        cfg.minio_endpoint,
        cfg.minio_access_key,
        cfg.minio_secret_key,
        bucket=cfg.minio_bronze_bucket,
    ) as minio:
        oai = OAIClient(str(feed.endpoint), client)
        async for record in oai.list_records(
            metadata_prefix=metadata_prefix,
            set_spec=set_spec,
            from_=from_ts,
            max_records=max_records,
        ):
            await bucket.acquire()
            if record.deleted:
                continue
            url = record.arxiv_abs_url() or f"oai://{feed.endpoint}/{record.identifier}"
            doc_id = doc_id_for_url(url) if url.startswith("http") else f"sha256:{_sha256(url)}"
            fetched_at = datetime.now(tz=timezone.utc)
            key = bronze_object_key(
                source_feed=feed.name,
                doc_id=doc_id,
                fetched_at=fetched_at,
                extension="oai.xml.gz",
            )
            stored = await minio.put_bronze(
                key=key,
                payload=record.raw,
                content_type="application/xml",
                gzip_compress=True,
                metadata={
                    "doc_id": doc_id,
                    "source_feed": feed.name,
                    "oai_identifier": record.identifier,
                },
            )
            br = BronzeRecord(
                doc_id=doc_id,
                url=url,  # type: ignore[arg-type]
                fetched_at=fetched_at,
                http_status=200,
                content_type="application/xml",
                raw_html_s3_uri=bronze_s3_uri(
                    bucket=cfg.minio_bronze_bucket,
                    source_feed=feed.name,
                    doc_id=doc_id,
                    fetched_at=fetched_at,
                    extension="oai.xml.gz",
                ),
                source_feed=feed.name,
                trace_id=_random_trace_id(),
                bytes_size=stored,
            )
            await producer.send(br)
            emitted += 1

    feed_state["until"] = new_until
    state_store.put(feed.name, feed_state)
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
    state_root = (
        "/var/lib/s2p-state/oaipmh_poller" if not cfg.is_dev else "./.s2p-state/oaipmh"
    )
    state_store = FeedStateStore(state_root)
    total = 0
    for feed in feeds:
        try:
            total += await poll_feed(feed, cfg, state_store=state_store, **kw)
        except Exception as exc:  # noqa: BLE001
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
    emitted = asyncio.run(_run(cfg, feeds))
    log.info("oaipmh_poller.done", emitted=emitted)


if __name__ == "__main__":
    main()
