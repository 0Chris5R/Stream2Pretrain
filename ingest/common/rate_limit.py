"""Token-bucket rate limiter.

Used by every poller to enforce per-feed politeness limits in the same shape
as ``RateLimitSpec``: ``requests_per_second`` + ``burst``. Single-process only;
in a multi-pod deployment each pod owns its own bucket and the global rate is
the per-pod rate times pod count - matching the expectation in SOURCES.md
("single fetcher pod per arXiv source").
"""

from __future__ import annotations

import asyncio
import time


class TokenBucket:
    """Async-friendly token bucket.

    ``acquire()`` blocks just long enough to keep the rolling rate under
    ``rate`` requests/sec while permitting bursts up to ``burst`` tokens.
    """

    def __init__(self, rate: float, burst: int) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        if burst <= 0:
            raise ValueError("burst must be positive")
        self._rate = float(rate)
        self._capacity = float(burst)
        self._tokens = float(burst)
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> None:
        """Acquire ``tokens`` from the bucket; block until they are available."""
        if tokens <= 0:
            return
        if tokens > self._capacity:
            raise ValueError("requested tokens exceed bucket capacity")
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._updated
                self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
                self._updated = now
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                deficit = tokens - self._tokens
                wait = deficit / self._rate
            await asyncio.sleep(wait)
