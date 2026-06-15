"""RSS / Atom one-pass CronJob entrypoint.

For each enabled feed of protocol ``rss`` or ``atom``:

1. Conditional GET the feed using the previous run's ``ETag`` /
   ``Last-Modified`` headers; on 304, stop early.
2. Parse with feedparser; extract entry links.
3. For each entry, fetch the linked HTML, gzip into MinIO, emit a
   ``BronzeRecord`` on the ``raw.fetched`` topic.

Honours per-feed ``RateLimitSpec`` via a ``TokenBucket``. Persists ETag /
Last-Modified state under ``/var/lib/s2p-state``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from typing import Any

import feedparser
import httpx

from ingest.common.bronze_pipeline import fetch_and_publish
from ingest.common.config import IngestConfig, load_config
from ingest.common.feeds import (
    feeds_by_protocol,
    load_feeds_from_kube,
    load_feeds_from_yaml,
)
from ingest.common.http_client import build_async_client
from ingest.common.kafka_producer import BronzeProducer
from ingest.common.logging import configure_logging, get_logger
from ingest.common.minio_writer import MinioWriter
from ingest.common.otel import init_tracer
from ingest.common.rate_limit import TokenBucket
from ingest.common.state import FeedStateStore
from schemas.sourcefeed import SourceFeedSpec

log = get_logger(__name__)


def discover_entry_urls(feed_text: str) -> list[str]:
    """Return de-duplicated entry URLs from a parsed RSS/Atom payload."""
    parsed: Any = feedparser.parse(feed_text)
    seen: set[str] = set()
    urls: list[str] = []
    for entry in parsed.get("entries", []):
        link = entry.get("link")
        if not link:
            for ln in entry.get("links", []) or []:
                href = ln.get("href")
                if href:
                    link = href
                    break
        if link and link not in seen:
            seen.add(link)
            urls.append(link)
    return urls


async def poll_feed(
    feed: SourceFeedSpec,
    *,
    client: httpx.AsyncClient,
    producer: BronzeProducer,
    minio: MinioWriter,
    bucket: str,
    state_store: FeedStateStore,
) -> int:
    """Run one polling pass over a single feed. Returns docs emitted."""
    cfg_state = state_store.get(feed.name)
    bucket_limit = TokenBucket(
        feed.rate_limit.requests_per_second, feed.rate_limit.burst
    )

    headers: dict[str, str] = {
        "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9",
    }
    if ua := client.headers.get("User-Agent"):
        headers["User-Agent"] = ua
    if etag := cfg_state.get("etag"):
        headers["If-None-Match"] = etag
    if lm := cfg_state.get("last_modified"):
        headers["If-Modified-Since"] = lm

    await bucket_limit.acquire()
    resp = await client.get(str(feed.endpoint), headers=headers)
    if resp.status_code == 304:
        log.info("feed.not_modified", feed=feed.name)
        return 0
    if resp.status_code >= 400:
        log.warning(
            "feed.fetch_failed",
            feed=feed.name,
            status=resp.status_code,
        )
        return 0

    new_state: dict[str, Any] = {}
    if et := resp.headers.get("etag"):
        new_state["etag"] = et
    if lm := resp.headers.get("last-modified"):
        new_state["last_modified"] = lm

    urls = discover_entry_urls(resp.text)
    log.info("feed.parsed", feed=feed.name, entries=len(urls))

    seen_in_pass: set[str] = set()
    emitted = 0
    for url in urls:
        await bucket_limit.acquire()
        try:
            rec = await fetch_and_publish(
                client,
                url,
                source_feed=feed.name,
                producer=producer,
                minio=minio,
                bucket=bucket,
                expected_content_type="text/html",
                seen=seen_in_pass,
            )
        except httpx.HTTPError as exc:
            log.warning("entry.fetch_failed", feed=feed.name, url=url, err=str(exc))
            continue
        if rec is not None:
            emitted += 1

    state_store.put(feed.name, new_state)
    return emitted


async def run_pass(cfg: IngestConfig, feeds: Iterable[SourceFeedSpec]) -> int:
    """Run one pass over all enabled RSS+Atom feeds. Returns total emitted."""
    state_root = "/var/lib/s2p-state/rss_poller" if not cfg.is_dev else "./.s2p-state/rss"
    state_store = FeedStateStore(state_root)
    total = 0
    async with build_async_client(cfg) as client, BronzeProducer(
        cfg.redpanda_brokers, topic=cfg.raw_topic, client_id="s2p-rss-poller"
    ) as producer, MinioWriter(
        cfg.minio_endpoint,
        cfg.minio_access_key,
        cfg.minio_secret_key,
        bucket=cfg.minio_bronze_bucket,
    ) as minio:
        for feed in feeds:
            try:
                total += await poll_feed(
                    feed,
                    client=client,
                    producer=producer,
                    minio=minio,
                    bucket=cfg.minio_bronze_bucket,
                    state_store=state_store,
                )
            except Exception as exc:  # noqa: BLE001 - one bad feed must not kill the pass
                log.exception("feed.unhandled_error", feed=feed.name, err=str(exc))
    return total


def _load_feeds(cfg: IngestConfig) -> list[SourceFeedSpec]:
    if cfg.feed_config_path:
        feeds = load_feeds_from_yaml(cfg.feed_config_path)
    else:
        feeds = load_feeds_from_kube()
    return feeds_by_protocol(feeds, "rss") + feeds_by_protocol(feeds, "atom")


def main() -> None:
    """CronJob entrypoint: one full pass, then exit."""
    cfg = load_config()
    configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    init_tracer("ingest.rss_poller", cfg)
    feeds = _load_feeds(cfg)
    log.info("rss_poller.start", feeds=len(feeds))
    if not feeds:
        log.warning("rss_poller.no_feeds")
        return
    emitted = asyncio.run(run_pass(cfg, feeds))
    log.info("rss_poller.done", emitted=emitted)


if __name__ == "__main__":
    main()
