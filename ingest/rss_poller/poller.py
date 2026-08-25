"""arXiv RSS / Atom discovery CronJob entrypoint.

For each enabled feed of protocol ``rss`` or ``atom``:

1. Conditional GET the feed using the previous run's ``ETag`` /
   ``Last-Modified`` headers; on 304, stop early.
2. Parse with feedparser and require canonical arXiv entry links.
3. Emit internal discovery envelopes for the arXiv full-text worker. The RSS
   payload and abstract page never become corpus documents.

Honours per-feed ``RateLimitSpec`` via a ``TokenBucket``. Persists ETag /
Last-Modified state under ``/var/lib/s2p-state``.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import feedparser
import httpx

from ingest.common.arxiv_license import arxiv_id_from_url
from ingest.common.bronze_pipeline import publish_discovery_payload
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


@dataclass(frozen=True, slots=True)
class FeedEntry:
    url: str


def discover_entry_urls(feed_text: str) -> list[str]:
    """Return de-duplicated entry URLs from a parsed RSS/Atom payload."""
    return [entry.url for entry in discover_entries(feed_text)]


def discover_entries(feed_text: str) -> list[FeedEntry]:
    """Return de-duplicated arXiv discovery URLs."""
    parsed: Any = feedparser.parse(feed_text)
    return _entries_from_parsed(parsed)


def _entries_from_parsed(parsed: Any) -> list[FeedEntry]:
    """Extract normalized entries from a feedparser result."""
    seen: set[str] = set()
    entries: list[FeedEntry] = []
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
            entries.append(FeedEntry(url=str(link)))
    return entries


async def poll_feed(
    feed: SourceFeedSpec,
    *,
    client: httpx.AsyncClient,
    producer: BronzeProducer,
    minio: MinioWriter,
    bucket: str,
    state_store: FeedStateStore,
) -> int:
    """Run one arXiv discovery pass. Returns discovery envelopes emitted."""
    cfg_state = state_store.get(feed.name)
    bucket_limit = TokenBucket(feed.rate_limit.requests_per_second, feed.rate_limit.burst)

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
        resp.raise_for_status()

    new_state: dict[str, Any] = {}
    if et := resp.headers.get("etag"):
        new_state["etag"] = et
    if lm := resp.headers.get("last-modified"):
        new_state["last_modified"] = lm

    parsed: Any = feedparser.parse(resp.text)
    entries = _entries_from_parsed(parsed)
    if not parsed.get("version"):
        raise ValueError(f"invalid RSS/Atom payload for feed {feed.name}")
    log.info("feed.parsed", feed=feed.name, entries=len(entries))

    emitted = 0
    entry_failures: list[str] = []
    for entry in entries:
        url = entry.url
        arxiv_id = arxiv_id_from_url(url)
        if arxiv_id is None:
            log.warning("entry.non_arxiv", feed=feed.name, url=url)
            entry_failures.append(url)
            continue
        try:
            await publish_discovery_payload(
                payload=json.dumps(
                    {"arxiv_id": arxiv_id, "source_url": url}, sort_keys=True
                ).encode("utf-8"),
                url=url,
                source_feed=feed.name,
                producer=producer,
                minio=minio,
                bucket=bucket,
                extension="discovery.json.gz",
                content_type="application/json",
                extraction_pipeline="arxiv-rss-discovery-v2",
                metadata={"arxiv_id": arxiv_id},
            )
        except Exception as exc:
            log.warning("entry.publish_failed", feed=feed.name, url=url, err=str(exc))
            entry_failures.append(url)
            continue
        emitted += 1

    if entry_failures:
        raise RuntimeError(
            f"{len(entry_failures)} of {len(entries)} RSS entries failed for {feed.name}"
        )

    state_store.put(feed.name, new_state)
    return emitted


async def run_pass(cfg: IngestConfig, feeds: Iterable[SourceFeedSpec]) -> int:
    """Run one pass over all enabled RSS+Atom feeds. Returns total emitted."""
    state_root = "/var/lib/s2p-state/rss_poller" if not cfg.is_dev else "./.s2p-state/rss"
    state_store = FeedStateStore(state_root)
    total = 0
    failures: list[str] = []
    async with (
        build_async_client(cfg) as client,
        BronzeProducer(
            cfg.redpanda_brokers, topic=cfg.raw_topic, client_id="s2p-rss-poller"
        ) as producer,
        MinioWriter(
            cfg.minio_endpoint,
            cfg.minio_access_key,
            cfg.minio_secret_key,
            bucket=cfg.minio_bronze_bucket,
        ) as minio,
    ):
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
            except Exception as exc:
                log.exception("feed.unhandled_error", feed=feed.name, err=str(exc))
                failures.append(feed.name)
    if failures:
        raise RuntimeError(f"RSS feed polling failed: {', '.join(failures)}")
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
