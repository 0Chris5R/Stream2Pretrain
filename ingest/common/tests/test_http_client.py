"""Tests for the retry transport in http_client."""

from __future__ import annotations

import httpx
import pytest

from ingest.common.config import IngestConfig
from ingest.common.http_client import build_async_client


def _cfg(retries: int = 2) -> IngestConfig:
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
        http_max_retries=retries,
    )


@pytest.mark.asyncio
async def test_retries_on_503_then_succeeds() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503, headers={"retry-after": "0"})
        return httpx.Response(200, text="ok")

    transport = httpx.MockTransport(handler)
    client = build_async_client(_cfg(retries=4), transport=transport)
    try:
        resp = await client.get("https://example.com/")
        assert resp.status_code == 200
        assert calls["n"] == 3
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_returns_last_response_when_retries_exhausted() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"retry-after": "0"})

    transport = httpx.MockTransport(handler)
    client = build_async_client(_cfg(retries=1), transport=transport)
    try:
        resp = await client.get("https://example.com/")
        assert resp.status_code == 429
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_non_retryable_status_passes_through() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="not found")

    transport = httpx.MockTransport(handler)
    client = build_async_client(_cfg(retries=3), transport=transport)
    try:
        resp = await client.get("https://example.com/missing")
        assert resp.status_code == 404
    finally:
        await client.aclose()
