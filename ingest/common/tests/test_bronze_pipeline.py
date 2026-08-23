"""End-to-end fetch+store+publish pipeline tests."""

from __future__ import annotations

import httpx
import pytest

from ingest.common.bronze_pipeline import fetch_and_publish, parse_http_date
from ingest.common.config import IngestConfig
from ingest.common.http_client import build_async_client

from .conftest import FakeMinio, FakeProducer


@pytest.mark.asyncio
async def test_fetch_and_publish_happy_path(
    fake_producer: FakeProducer, fake_minio: FakeMinio
) -> None:
    admissions = FakeProducer()
    body = b"<html><body>hello</body></html>"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=body,
            headers={
                "content-type": "text/html; charset=utf-8",
                "etag": '"abc"',
                "last-modified": "Mon, 15 Jun 2026 08:00:00 GMT",
            },
        )

    transport = httpx.MockTransport(handler)
    client = build_async_client(_cfg(), transport=transport)
    try:
        rec = await fetch_and_publish(
            client,
            "https://example.com/post/1",
            source_feed="rss-test",
            producer=fake_producer,  # type: ignore[arg-type]
            minio=fake_minio,  # type: ignore[arg-type]
            bucket="bronze",
            license_value="CC-BY-4.0",
            license_source="rss_entry",
            admission_producer=admissions,  # type: ignore[arg-type]
        )
    finally:
        await client.aclose()

    assert rec is not None
    assert rec.http_status == 200
    assert rec.content_type == "text/html"
    assert rec.etag == '"abc"'
    assert rec.raw_html_s3_uri.startswith("s3://bronze/")
    assert len(fake_producer.sent) == 1
    assert admissions.sent[0]["record"].status == "admitted"
    stored = next(iter(fake_minio.objects.values()))
    assert stored["payload"] == body


@pytest.mark.asyncio
async def test_fetch_and_publish_skips_304(
    fake_producer: FakeProducer, fake_minio: FakeMinio
) -> None:
    admissions = FakeProducer()

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(304)

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        rec = await fetch_and_publish(
            client,
            "https://example.com/p",
            source_feed="rss-test",
            producer=fake_producer,  # type: ignore[arg-type]
            minio=fake_minio,  # type: ignore[arg-type]
            bucket="bronze",
            license_value="CC-BY-4.0",
            license_source="rss_entry",
            admission_producer=admissions,  # type: ignore[arg-type]
        )
    finally:
        await client.aclose()
    assert rec is None
    assert not fake_producer.sent
    assert not fake_minio.objects


@pytest.mark.asyncio
async def test_fetch_and_publish_raises_on_upstream_http_error(
    fake_producer: FakeProducer, fake_minio: FakeMinio
) -> None:
    admissions = FakeProducer()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, request=request)

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(httpx.HTTPStatusError, match="503"):
            await fetch_and_publish(
                client,
                "https://example.com/unavailable",
                source_feed="rss-test",
                producer=fake_producer,  # type: ignore[arg-type]
                minio=fake_minio,  # type: ignore[arg-type]
                bucket="bronze",
                license_value="CC-BY-4.0",
                license_source="rss_entry",
                admission_producer=admissions,  # type: ignore[arg-type]
            )
    finally:
        await client.aclose()

    assert not fake_producer.sent
    assert not fake_minio.objects


@pytest.mark.asyncio
async def test_fetch_and_publish_dedups_seen(
    fake_producer: FakeProducer, fake_minio: FakeMinio
) -> None:
    admissions = FakeProducer()

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x", headers={"content-type": "text/html"})

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    seen: set[str] = set()
    try:
        r1 = await fetch_and_publish(
            client,
            "https://x.io/a",
            source_feed="t",
            producer=fake_producer,
            minio=fake_minio,
            bucket="bronze",
            seen=seen,
            license_value="CC-BY-4.0",
            license_source="rss_entry",
            admission_producer=admissions,  # type: ignore[arg-type]
        )
        r2 = await fetch_and_publish(
            client,
            "https://x.io/a",
            source_feed="t",
            producer=fake_producer,
            minio=fake_minio,
            bucket="bronze",
            seen=seen,
            license_value="CC-BY-4.0",
            license_source="rss_entry",
            admission_producer=admissions,  # type: ignore[arg-type]
        )
    finally:
        await client.aclose()
    assert r1 is not None
    assert r2 is None


@pytest.mark.asyncio
async def test_missing_license_is_fetched_for_transform_only_posttraining(
    fake_producer: FakeProducer, fake_minio: FakeMinio
) -> None:
    admissions = FakeProducer()
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, content=b"transform-only source material")

    client = build_async_client(_cfg(), transport=httpx.MockTransport(handler))
    try:
        record = await fetch_and_publish(
            client,
            "https://example.com/unlicensed",
            source_feed="rss-test",
            producer=fake_producer,  # type: ignore[arg-type]
            minio=fake_minio,  # type: ignore[arg-type]
            bucket="bronze",
            admission_producer=admissions,  # type: ignore[arg-type]
        )
    finally:
        await client.aclose()
    assert record is not None
    assert record.training_usage == "posttrain_transform_only"
    assert calls == 1
    assert fake_minio.objects
    assert admissions.sent[0]["record"].status == "posttrain_transform_only"


def test_parse_http_date_handles_none() -> None:
    assert parse_http_date(None) is None
    assert parse_http_date("garbage") is None
    dt = parse_http_date("Mon, 15 Jun 2026 08:00:00 GMT")
    assert dt is not None
    assert dt.year == 2026 and dt.month == 6 and dt.day == 15


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
        github_token=None,
        hf_token=None,
        user_agent="ua",
        http_timeout_seconds=2.0,
        http_max_retries=0,
    )
