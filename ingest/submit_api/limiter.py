"""Per-source rate limiter for submit_api.

Sources have already-declared ``RateLimitSpec``s; we honour them on the submit
path so a single attacker cannot blow through arXiv's 4 req/s budget by
hammering ``POST /submit``.
"""

from __future__ import annotations

from ingest.common.rate_limit import TokenBucket
from schemas.sourcefeed import SourceFeedSpec


class PerSourceLimiter:
    """Maintains one ``TokenBucket`` per source feed name."""

    def __init__(self) -> None:
        self._buckets: dict[str, TokenBucket] = {}

    def configure(self, feeds: list[SourceFeedSpec]) -> None:
        """Re-build the bucket map from a SourceFeed list."""
        self._buckets = {
            f.name: TokenBucket(f.rate_limit.requests_per_second, f.rate_limit.burst)
            for f in feeds
        }

    async def acquire(self, source_feed: str) -> None:
        """Block until the bucket for ``source_feed`` admits one token.

        Falls back to a generous default bucket for the special ``manual-submit``
        feed so the submit API has a sensible behaviour even when no SourceFeed
        CRD declares it.
        """
        bucket = self._buckets.get(source_feed)
        if bucket is None:
            bucket = self._buckets.setdefault(
                source_feed, TokenBucket(rate=2.0, burst=4)
            )
        await bucket.acquire()

    def has(self, source_feed: str) -> bool:
        return source_feed in self._buckets
