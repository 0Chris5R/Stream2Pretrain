"""Token-bucket rate limiter tests."""

from __future__ import annotations

import asyncio
import time

import pytest

from ingest.common.rate_limit import TokenBucket


@pytest.mark.asyncio
async def test_burst_admitted_immediately() -> None:
    bucket = TokenBucket(rate=1.0, burst=3)
    t0 = time.monotonic()
    for _ in range(3):
        await bucket.acquire()
    assert time.monotonic() - t0 < 0.05


@pytest.mark.asyncio
async def test_subsequent_acquire_blocks_until_refill() -> None:
    bucket = TokenBucket(rate=20.0, burst=1)
    await bucket.acquire()
    t0 = time.monotonic()
    await bucket.acquire()
    elapsed = time.monotonic() - t0
    # Refill at 20/s -> ~50ms; allow generous slack.
    assert 0.02 <= elapsed <= 0.5


@pytest.mark.asyncio
async def test_concurrent_acquire_serialises() -> None:
    bucket = TokenBucket(rate=50.0, burst=1)
    async def waiter() -> None:
        await bucket.acquire()
    t0 = time.monotonic()
    await asyncio.gather(waiter(), waiter(), waiter())
    elapsed = time.monotonic() - t0
    # Three tokens at 50/s with burst=1 -> at least ~40ms of waiting.
    assert elapsed >= 0.03


def test_invalid_arguments() -> None:
    with pytest.raises(ValueError):
        TokenBucket(rate=0.0, burst=1)
    with pytest.raises(ValueError):
        TokenBucket(rate=1.0, burst=0)
