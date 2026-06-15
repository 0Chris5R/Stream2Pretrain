"""Tests for the sitemap poller."""

from __future__ import annotations

import gzip

import httpx
import pytest

from ingest.common.http_client import build_async_client
from ingest.sitemap_poller.poller import collect_urls, parse_sitemap_xml

SITEMAP_INDEX = """<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>https://example.com/sitemap-pages.xml.gz</loc></sitemap>
</sitemapindex>
"""

SITEMAP_URLSET = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://example.com/a</loc><lastmod>2026-06-14T08:00:00Z</lastmod></url>
  <url><loc>https://example.com/b</loc></url>
</urlset>
"""


def _cfg() -> "IngestConfig":  # type: ignore[name-defined]
    from ingest.common.config import IngestConfig

    return IngestConfig(
        env="dev",
        log_level="DEBUG",
        redpanda_brokers="",
        minio_endpoint="",
        minio_access_key="",
        minio_secret_key="",
        minio_bronze_bucket="bronze",
        otel_endpoint=None,
        otel_protocol="grpc",
        github_token=None,
        hf_token=None,
        user_agent="ua",
        http_timeout_seconds=2.0,
        http_max_retries=0,
    )


def test_parse_sitemap_index() -> None:
    urls, children = parse_sitemap_xml(SITEMAP_INDEX)
    assert urls == []
    assert children == ["https://example.com/sitemap-pages.xml.gz"]


def test_parse_urlset() -> None:
    urls, children = parse_sitemap_xml(SITEMAP_URLSET)
    assert children == []
    assert urls == [
        ("https://example.com/a", "2026-06-14T08:00:00Z"),
        ("https://example.com/b", None),
    ]


def test_parse_garbage_returns_empty() -> None:
    assert parse_sitemap_xml("<not-sitemap/>") == ([], [])


@pytest.mark.asyncio
async def test_collect_urls_expands_index_and_decompresses_gzip() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("sitemap.xml"):
            return httpx.Response(200, text=SITEMAP_INDEX, headers={"content-type": "application/xml"})
        if request.url.path.endswith("sitemap-pages.xml.gz"):
            data = gzip.compress(SITEMAP_URLSET.encode("utf-8"))
            return httpx.Response(
                200,
                content=data,
                headers={"content-type": "application/gzip"},
            )
        return httpx.Response(404)

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        urls = await collect_urls(client, "https://example.com/sitemap.xml")
    finally:
        await client.aclose()
    assert sorted(u for u, _ in urls) == ["https://example.com/a", "https://example.com/b"]
