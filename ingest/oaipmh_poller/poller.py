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

from ingest.common.bronze_pipeline import publish_discovery_payload
from ingest.common.config import IngestConfig, load_config
from ingest.common.feeds import (
    feeds_by_protocol,
    load_feeds_from_kube,
    load_feeds_from_yaml,
)
from ingest.common.http_client import build_async_client, build_headers
from ingest.common.kafka_producer import BronzeProducer
from ingest.common.logging import configure_logging, get_logger
from ingest.common.minio_writer import MinioWriter
from ingest.common.otel import init_tracer
from ingest.common.state import FeedStateStore
from ingest.oaipmh_poller.client import OAIClient
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
    # The epoch suffix intentionally abandons the old historical cursor. OAI
    # is a current-frontier no-gap discovery path, not a backfill source.
    state_key = f"{feed.name}:live-v1"
    feed_state = state_store.get(state_key)
    window_from = str(feed_state.get("window_from") or feed_state.get("until") or _today_iso())
    window_until = str(feed_state.get("window_until") or _today_iso())
    resumption_token = feed_state.get("resumption_token")
    if not isinstance(resumption_token, str) or not resumption_token:
        resumption_token = None

    headers = build_headers(cfg, accept="application/xml, text/xml;q=0.9")
    emitted = 0
    handled = 0
    pages_handled = 0
    async with (
        build_async_client(cfg, headers=headers) as client,
        BronzeProducer(
            cfg.redpanda_brokers, topic=cfg.raw_topic, client_id="s2p-oai-poller"
        ) as producer,
        MinioWriter(
            cfg.minio_endpoint,
            cfg.minio_access_key,
            cfg.minio_secret_key,
            bucket=cfg.minio_bronze_bucket,
        ) as minio,
    ):
        # OAI-PMH returns hundreds or thousands of records per HTTP response.
        # Rate-limit the page requests in OAIClient, never the records already
        # present in a fetched response.  arXiv asks clients to leave at least
        # one second between requests, even when a SourceFeed allows more.
        request_interval = max(1.0, 1.0 / feed.rate_limit.requests_per_second)
        oai = OAIClient(
            str(feed.endpoint),
            client,
            sleep_between_requests=request_interval,
        )
        async for page in oai.list_pages(
            metadata_prefix=metadata_prefix,
            set_spec=set_spec,
            from_=window_from,
            until=window_until,
            resumption_token=resumption_token,
        ):
            for index, record in enumerate(page.records):
                handled += 1
                if not record.deleted:
                    url = record.arxiv_abs_url() or f"oai://{feed.endpoint}/{record.identifier}"
                    arxiv_id = record.arxiv_id()
                    if arxiv_id is None:
                        continue
                    await publish_discovery_payload(
                        payload=record.raw,
                        url=url,
                        source_feed=feed.name,
                        producer=producer,
                        minio=minio,
                        bucket=cfg.minio_bronze_bucket,
                        extension="oai.xml.gz",
                        content_type="application/xml",
                        extraction_pipeline="oai-pmh-discovery-v2",
                        metadata={
                            "arxiv_id": arxiv_id,
                            "oai_identifier": record.identifier,
                            "oai_datestamp": record.datestamp,
                        },
                    )
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
                    state_key,
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
                state_store.put(state_key, {"until": window_until})
                return emitted

    return emitted


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
    failures: list[str] = []
    for feed in feeds:
        try:
            total += await poll_feed(feed, cfg, state_store=state_store, **kw)
        except Exception as exc:
            log.exception("oai.feed.error", feed=feed.name, err=str(exc))
            failures.append(feed.name)
    if failures:
        raise RuntimeError(f"OAI-PMH feed polling failed: {', '.join(failures)}")
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
