"""Unit tests for the RSS poller."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from ingest.common.config import IngestConfig
from ingest.common.http_client import build_async_client
from ingest.common.state import FeedStateStore
from ingest.common.tests.conftest import FakeMinio, FakeProducer  # type: ignore[attr-defined]
from ingest.rss_poller.poller import discover_entry_urls, poll_feed
from schemas.sourcefeed import RateLimitSpec, SourceFeedSpec

RSS_BODY = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/"><channel>
<title>arXiv cs.CL</title>
<link>https://arxiv.org/list/cs.CL/recent</link>
<description>arXiv cs.CL</description>
<item>
  <title>Paper A</title>
  <link>https://arxiv.org/abs/2608.00001</link>
  <guid>https://arxiv.org/abs/2608.00001</guid>
  <dc:rights>https://creativecommons.org/licenses/by/4.0/</dc:rights>
</item>
<item>
  <title>Paper B</title>
  <link>https://arxiv.org/abs/2608.00002</link>
  <guid>https://arxiv.org/abs/2608.00002</guid>
  <dc:rights>https://creativecommons.org/licenses/by-sa/4.0/</dc:rights>
</item>
</channel></rss>
"""


def _feed() -> SourceFeedSpec:
    return SourceFeedSpec(
        name="rss-test",
        protocol="rss",
        endpoint="https://example.com/rss",  # type: ignore[arg-type]
        poll_interval_seconds=600,
        rate_limit=RateLimitSpec(requests_per_second=20.0, burst=4),
    )


def _cfg() -> IngestConfig:
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
        hf_token=None,
        user_agent="ua",
        http_timeout_seconds=2.0,
        http_max_retries=0,
    )


def test_discover_entry_urls_finds_links() -> None:
    urls = discover_entry_urls(RSS_BODY)
    assert urls == [
        "https://arxiv.org/abs/2608.00001",
        "https://arxiv.org/abs/2608.00002",
    ]


def test_discover_handles_garbage() -> None:
    assert discover_entry_urls("<not-rss/>") == []


@pytest.mark.asyncio
async def test_poll_feed_emits_records(tmp_path: Path) -> None:
    state = FeedStateStore(tmp_path)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/rss":
            return httpx.Response(
                200,
                content=RSS_BODY.encode("utf-8"),
                headers={
                    "content-type": "application/rss+xml",
                    "etag": '"feed-v1"',
                },
            )
        return httpx.Response(
            200,
            content=b"<html>article</html>",
            headers={"content-type": "text/html"},
        )

    fake_producer = FakeProducer()
    fake_minio = FakeMinio()
    await fake_producer.start()
    await fake_minio.start()
    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        emitted = await poll_feed(
            _feed(),
            client=client,
            producer=fake_producer,  # type: ignore[arg-type]
            minio=fake_minio,  # type: ignore[arg-type]
            bucket="bronze",
            state_store=state,
        )
    finally:
        await client.aclose()
    assert emitted == 2
    assert len(fake_producer.sent) == 2
    assert {sent["record"].source_format for sent in fake_producer.sent} == {"metadata"}
    saved = state.get("rss-test")
    assert saved.get("etag") == '"feed-v1"'


@pytest.mark.asyncio
async def test_poll_feed_handles_304(tmp_path: Path) -> None:
    state = FeedStateStore(tmp_path)
    state.put("rss-test", {"etag": '"feed-v1"'})

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(304)

    fake_producer = FakeProducer()
    fake_minio = FakeMinio()
    await fake_producer.start()
    await fake_minio.start()
    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        emitted = await poll_feed(
            _feed(),
            client=client,
            producer=fake_producer,  # type: ignore[arg-type]
            minio=fake_minio,  # type: ignore[arg-type]
            bucket="bronze",
            state_store=state,
        )
    finally:
        await client.aclose()
    assert emitted == 0
    assert not fake_producer.sent


@pytest.mark.asyncio
async def test_poll_feed_raises_when_feed_is_unavailable(tmp_path: Path) -> None:
    state = FeedStateStore(tmp_path)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, request=request)

    fake_producer = FakeProducer()
    fake_minio = FakeMinio()
    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(httpx.HTTPStatusError, match="503"):
            await poll_feed(
                _feed(),
                client=client,
                producer=fake_producer,  # type: ignore[arg-type]
                minio=fake_minio,  # type: ignore[arg-type]
                bucket="bronze",
                state_store=state,
            )
    finally:
        await client.aclose()

    assert state.get("rss-test") == {}


@pytest.mark.asyncio
async def test_poll_feed_rejects_non_feed_success_response(tmp_path: Path) -> None:
    state = FeedStateStore(tmp_path)

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html><body>challenge</body></html>")

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(ValueError, match="invalid RSS/Atom payload"):
            await poll_feed(
                _feed(),
                client=client,
                producer=FakeProducer(),  # type: ignore[arg-type]
                minio=FakeMinio(),  # type: ignore[arg-type]
                bucket="bronze",
                state_store=state,
            )
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_arxiv_rss_record_is_discovery_metadata_not_duplicate_corpus(
    tmp_path: Path,
) -> None:
    body = """<rss version="2.0"><channel><title>arXiv</title>
    <item><title>Paper</title><link>https://arxiv.org/abs/2608.01234</link></item>
    </channel></rss>"""

    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        if request.url.path == "/rss":
            return httpx.Response(200, text=body, request=request)
        return httpx.Response(500, request=request)

    producer = FakeProducer()
    minio = FakeMinio()
    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        emitted = await poll_feed(
            _feed(),
            client=client,
            producer=producer,  # type: ignore[arg-type]
            minio=minio,  # type: ignore[arg-type]
            bucket="bronze",
            state_store=FeedStateStore(tmp_path),
        )
    finally:
        await client.aclose()

    assert emitted == 1
    record = producer.sent[0]["record"]
    assert record.source_format == "metadata"
    assert record.extraction_pipeline == "arxiv-rss-discovery-v2"
    assert requests == ["/rss"]
