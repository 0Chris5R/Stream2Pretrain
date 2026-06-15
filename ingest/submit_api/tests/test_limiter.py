"""Tests for the per-source token-bucket limiter."""

from __future__ import annotations

import time

import pytest

from ingest.submit_api.limiter import PerSourceLimiter
from schemas.sourcefeed import RateLimitSpec, SourceFeedSpec


def _feed(name: str, rps: float = 1.0, burst: int = 1) -> SourceFeedSpec:
    return SourceFeedSpec(
        name=name,
        protocol="manual",
        endpoint="https://example.com/",  # type: ignore[arg-type]
        poll_interval_seconds=600,
        rate_limit=RateLimitSpec(requests_per_second=rps, burst=burst),
    )


@pytest.mark.asyncio
async def test_per_source_isolation() -> None:
    limiter = PerSourceLimiter()
    limiter.configure([_feed("a", rps=20.0, burst=1), _feed("b", rps=20.0, burst=1)])
    await limiter.acquire("a")
    t0 = time.monotonic()
    await limiter.acquire("b")  # b's bucket is full; should be instant
    elapsed = time.monotonic() - t0
    assert elapsed < 0.05


@pytest.mark.asyncio
async def test_unknown_feed_uses_default_bucket() -> None:
    limiter = PerSourceLimiter()
    await limiter.acquire("unknown")
    assert limiter.has("unknown")
