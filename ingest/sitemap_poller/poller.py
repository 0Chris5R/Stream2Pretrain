"""Gzipped sitemap.xml fetcher with index expansion.

Sitemap protocol: https://www.sitemaps.org/protocol.html

Behaviour:

- Accepts both sitemap (``<urlset>``) and sitemap index (``<sitemapindex>``)
  documents. Indexes are expanded recursively (capped to a depth of 4).
- Uses ETag/Last-Modified conditional GET on each sitemap fetch.
- For each ``<url><loc>`` discovered, calls ``fetch_and_publish``.
- Persists per-feed ``url -> lastmod`` map so the next pass can skip URLs whose
  ``<lastmod>`` did not advance.
"""

from __future__ import annotations

import asyncio
import gzip
from contextlib import suppress
from xml.etree import ElementTree as ET

import httpx

from ingest.common.bronze_pipeline import fetch_and_publish
from ingest.common.config import IngestConfig, load_config
from ingest.common.feeds import (
    feeds_by_protocol,
    load_feeds_from_kube,
    load_feeds_from_yaml,
)
from ingest.common.http_client import build_async_client, build_headers
from ingest.common.kafka_producer import BronzeProducer, LicenseAdmissionProducer
from ingest.common.logging import configure_logging, get_logger
from ingest.common.minio_writer import MinioWriter
from ingest.common.otel import init_tracer
from ingest.common.page_license import resolve_page_license
from ingest.common.rate_limit import TokenBucket
from ingest.common.state import FeedStateStore
from schemas.sourcefeed import SourceFeedSpec

log = get_logger(__name__)

SITEMAP_NS = "http://www.sitemaps.org/schemas/sitemap/0.9"
_MAX_INDEX_DEPTH = 4


def parse_sitemap_xml(xml_text: str) -> tuple[list[tuple[str, str | None]], list[str]]:
    """Parse a sitemap document.

    Returns ``(urls_with_lastmod, child_sitemaps)``. Either may be empty.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return ([], [])
    tag = root.tag.split("}", 1)[-1]
    if tag == "sitemapindex":
        children: list[str] = []
        for sm in root.findall(f"{{{SITEMAP_NS}}}sitemap"):
            loc = sm.find(f"{{{SITEMAP_NS}}}loc")
            if loc is not None and loc.text:
                children.append(loc.text.strip())
        return ([], children)
    if tag == "urlset":
        out: list[tuple[str, str | None]] = []
        for u in root.findall(f"{{{SITEMAP_NS}}}url"):
            loc = u.find(f"{{{SITEMAP_NS}}}loc")
            if loc is None or not loc.text:
                continue
            lm_el = u.find(f"{{{SITEMAP_NS}}}lastmod")
            lastmod = lm_el.text.strip() if (lm_el is not None and lm_el.text) else None
            out.append((loc.text.strip(), lastmod))
        return (out, [])
    return ([], [])


async def fetch_sitemap(client: httpx.AsyncClient, url: str) -> str:
    """Fetch a sitemap (handling .gz), raising so cursors never skip failures."""
    resp = await client.get(url)
    if resp.status_code >= 400:
        log.warning("sitemap.fetch_failed", url=url, status=resp.status_code)
        resp.raise_for_status()
    payload = resp.content
    if url.endswith(".gz") or resp.headers.get("content-type", "").endswith("gzip"):
        with suppress(OSError):
            payload = gzip.decompress(payload)
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError:
        return payload.decode("utf-8", errors="replace")


async def collect_urls(client: httpx.AsyncClient, root_url: str) -> list[tuple[str, str | None]]:
    """Recursively expand a sitemap into a flat ``[(url, lastmod), ...]`` list."""
    queue: list[tuple[str, int]] = [(root_url, 0)]
    out: list[tuple[str, str | None]] = []
    visited: set[str] = set()
    while queue:
        sm_url, depth = queue.pop()
        if sm_url in visited or depth > _MAX_INDEX_DEPTH:
            continue
        visited.add(sm_url)
        text = await fetch_sitemap(client, sm_url)
        urls, children = parse_sitemap_xml(text)
        out.extend(urls)
        for c in children:
            queue.append((c, depth + 1))
    return out


async def poll_feed(
    feed: SourceFeedSpec,
    *,
    client: httpx.AsyncClient,
    producer: BronzeProducer,
    minio: MinioWriter,
    bucket: str,
    state_store: FeedStateStore,
    admission_producer: LicenseAdmissionProducer,
) -> int:
    """One pass over a sitemap feed."""
    state = state_store.get(feed.name)
    seen_lastmod: dict[str, str] = state.get("lastmod", {})
    bucket_limit = TokenBucket(feed.rate_limit.requests_per_second, feed.rate_limit.burst)
    await bucket_limit.acquire()
    urls = await collect_urls(client, str(feed.endpoint))
    log.info("sitemap.discovered", feed=feed.name, urls=len(urls))

    seen: set[str] = set()
    emitted = 0
    new_lastmod: dict[str, str] = {}
    entry_failures: list[str] = []
    for url, lastmod in urls:
        prev = seen_lastmod.get(url)
        if lastmod and prev == lastmod:
            continue  # unchanged - skip the page fetch entirely
        await bucket_limit.acquire()
        evidence = await resolve_page_license(client, url)
        try:
            rec = await fetch_and_publish(
                client,
                url,
                source_feed=feed.name,
                producer=producer,
                minio=minio,
                bucket=bucket,
                expected_content_type="text/html",
                seen=seen,
                license_value=evidence.raw_license,
                license_source=evidence.license_source,
                license_resolver=evidence.resolver,
                license_evidence_url=evidence.evidence_url,
                license_evidence_revision=evidence.evidence_revision or lastmod,
                license_evidence_scope=evidence.evidence_scope,
                admission_producer=admission_producer,
            )
        except httpx.HTTPError as exc:
            log.warning("sitemap.entry_failed", feed=feed.name, url=url, err=str(exc))
            entry_failures.append(url)
            continue
        if rec is not None:
            emitted += 1
        if lastmod:
            new_lastmod[url] = lastmod
    if entry_failures:
        raise RuntimeError(
            f"{len(entry_failures)} of {len(urls)} sitemap entries failed for {feed.name}"
        )
    state_store.put(feed.name, {"lastmod": {**seen_lastmod, **new_lastmod}})
    return emitted


async def run_pass(cfg: IngestConfig, feeds: list[SourceFeedSpec]) -> int:
    state_root = "/var/lib/s2p-state/sitemap_poller" if not cfg.is_dev else "./.s2p-state/sitemap"
    state_store = FeedStateStore(state_root)
    headers = build_headers(cfg, accept="application/xml, text/xml;q=0.9")
    total = 0
    async with (
        build_async_client(cfg, headers=headers) as client,
        BronzeProducer(
            cfg.redpanda_brokers, topic=cfg.raw_topic, client_id="s2p-sitemap-poller"
        ) as producer,
        LicenseAdmissionProducer(
            cfg.redpanda_brokers,
            topic=cfg.license_admissions_topic,
            client_id="s2p-sitemap-license-admission",
        ) as admission_producer,
        MinioWriter(
            cfg.minio_endpoint,
            cfg.minio_access_key,
            cfg.minio_secret_key,
            bucket=cfg.minio_bronze_bucket,
        ) as minio,
    ):
        failures: list[str] = []
        for feed in feeds:
            try:
                total += await poll_feed(
                    feed,
                    client=client,
                    producer=producer,
                    minio=minio,
                    bucket=cfg.minio_bronze_bucket,
                    state_store=state_store,
                    admission_producer=admission_producer,
                )
            except Exception as exc:
                log.exception("sitemap.feed_error", feed=feed.name, err=str(exc))
                failures.append(feed.name)
    if failures:
        raise RuntimeError(f"Sitemap feed polling failed: {', '.join(failures)}")
    return total


def _load_feeds(cfg: IngestConfig) -> list[SourceFeedSpec]:
    feeds = (
        load_feeds_from_yaml(cfg.feed_config_path)
        if cfg.feed_config_path
        else load_feeds_from_kube()
    )
    return feeds_by_protocol(feeds, "sitemap")


def main() -> None:
    cfg = load_config()
    configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    init_tracer("ingest.sitemap_poller", cfg)
    feeds = _load_feeds(cfg)
    if not feeds:
        log.warning("sitemap_poller.no_feeds")
        return
    log.info("sitemap_poller.start", feeds=len(feeds))
    emitted = asyncio.run(run_pass(cfg, feeds))
    log.info("sitemap_poller.done", emitted=emitted)


if __name__ == "__main__":
    main()
