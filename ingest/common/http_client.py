"""Async HTTP client builder with retry + timeout defaults.

Every poller fetches via ``build_async_client(cfg)``. The builder wires:

- a polite User-Agent identifying Stream2Pretrain and a contact URL
- per-request timeout from config (default 30s)
- HTTP/2 + connection pooling
- transparent retries on 429/5xx with exponential backoff and jitter
"""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from ingest.common.config import IngestConfig


_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


class _RetryTransport(httpx.AsyncBaseTransport):
    """Wrap an httpx transport with bounded retries on transient failures.

    Honors ``Retry-After`` when present; otherwise uses exponential backoff with
    a small uniform jitter. Network errors and timeouts retry the same way.
    """

    def __init__(
        self,
        inner: httpx.AsyncBaseTransport,
        *,
        max_retries: int,
        backoff_base: float = 0.5,
        max_jitter: float = 0.5,
    ) -> None:
        self._inner = inner
        self._max_retries = max(0, max_retries)
        self._backoff_base = backoff_base
        self._max_jitter = max_jitter

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        last_exc: Exception | None = None
        last_resp: httpx.Response | None = None
        for attempt in range(self._max_retries + 1):
            try:
                resp = await self._inner.handle_async_request(request)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                last_exc = exc
                if attempt == self._max_retries:
                    raise
                await self._sleep(attempt, retry_after=None)
                continue
            if resp.status_code not in _RETRY_STATUSES or attempt == self._max_retries:
                return resp
            last_resp = resp
            retry_after = resp.headers.get("retry-after")
            await resp.aclose()
            await self._sleep(attempt, retry_after=retry_after)
        # Should be unreachable given the loop conditions.
        if last_resp is not None:
            return last_resp
        assert last_exc is not None
        raise last_exc

    async def aclose(self) -> None:
        await self._inner.aclose()

    async def _sleep(self, attempt: int, *, retry_after: str | None) -> None:
        import asyncio

        if retry_after:
            try:
                delay = float(retry_after)
            except ValueError:
                delay = self._backoff_base * (2**attempt)
        else:
            delay = self._backoff_base * (2**attempt)
        delay += random.uniform(0.0, self._max_jitter)
        await asyncio.sleep(delay)


def build_headers(
    cfg: IngestConfig,
    *,
    accept: str | None = None,
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
    """Build the polite header dict every poller starts with."""
    headers: dict[str, str] = {
        "User-Agent": cfg.user_agent,
        "Accept-Encoding": "gzip, deflate",
    }
    if accept:
        headers["Accept"] = accept
    if extra:
        headers.update(extra)
    return headers


def build_async_client(
    cfg: IngestConfig,
    *,
    headers: dict[str, str] | None = None,
    base_url: str | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> httpx.AsyncClient:
    """Create an ``httpx.AsyncClient`` configured per ingest defaults.

    ``transport`` is exposed for tests (respx). When None, we use the default
    ``httpx.AsyncHTTPTransport`` wrapped with ``_RetryTransport``.
    """
    inner = transport or httpx.AsyncHTTPTransport(retries=0, http2=False)
    retry = _RetryTransport(inner, max_retries=cfg.http_max_retries)
    timeout = httpx.Timeout(cfg.http_timeout_seconds, connect=10.0)
    return httpx.AsyncClient(
        timeout=timeout,
        transport=retry,
        headers=headers or build_headers(cfg),
        base_url=base_url or "",
        follow_redirects=True,
    )
