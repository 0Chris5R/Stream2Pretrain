"""In-process robots.txt cache.

We deliberately hand-roll a tiny RobotFileParser wrapper rather than depend on
``reppy``: reppy carries a libcurl dependency that bloats the ingest image, and
the standard library parser is sufficient for the small set of hosts we poll.

Cache TTL defaults to 1 hour. Hostnames are case-folded.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

import httpx


@dataclass(slots=True)
class _Entry:
    parser: RobotFileParser
    fetched_at: float
    found: bool


class RobotsCache:
    """Async robots.txt cache shared between pollers."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        ttl_seconds: float = 3600.0,
        user_agent: str = "*",
    ) -> None:
        self._client = client
        self._ttl = ttl_seconds
        self._ua = user_agent
        self._cache: dict[str, _Entry] = {}

    async def can_fetch(self, url: str) -> bool:
        """Return True if ``url`` is allowed by its host's robots.txt.

        On any fetch error we fail-open (return True) and let the upstream
        rate-limiter throttle us. This matches arXiv / HF / GitHub guidance:
        their robots.txt are present but the API endpoints are documented as
        public; we still honour explicit Disallow rules when they exist.
        """
        parts = urlsplit(url)
        host_key = f"{parts.scheme}://{parts.netloc}".lower()
        entry = self._cache.get(host_key)
        now = time.monotonic()
        if entry is None or (now - entry.fetched_at) > self._ttl:
            entry = await self._refresh(host_key)
            self._cache[host_key] = entry
        if not entry.found:
            return True
        return entry.parser.can_fetch(self._ua, url)

    async def _refresh(self, host_key: str) -> _Entry:
        robots_url = f"{host_key}/robots.txt"
        parser = RobotFileParser()
        parser.set_url(robots_url)
        try:
            resp = await self._client.get(robots_url, timeout=10.0)
        except (httpx.HTTPError, httpx.TimeoutException):
            return _Entry(parser=parser, fetched_at=time.monotonic(), found=False)
        if resp.status_code >= 400:
            return _Entry(parser=parser, fetched_at=time.monotonic(), found=False)
        parser.parse(resp.text.splitlines())
        return _Entry(parser=parser, fetched_at=time.monotonic(), found=True)
