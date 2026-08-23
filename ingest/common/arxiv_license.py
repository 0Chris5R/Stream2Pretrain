"""Shared, fail-closed arXiv item-license discovery."""

from __future__ import annotations

import asyncio
import re
import xml.etree.ElementTree as ET
from html.parser import HTMLParser

import httpx

from ingest.common.logging import get_logger
from ingest.common.rate_limit import TokenBucket

log = get_logger(__name__)

ARXIV_ABS_BASE = "https://arxiv.org/abs"
ARXIV_API_QUERY = "https://export.arxiv.org/api/query"
_ARXIV_URL_RE = re.compile(
    r"^https?://(?:www\.)?arxiv\.org/(?:abs|pdf|html)/"
    r"([a-z\-]+/\d{7}(?:v\d+)?|\d{4}\.\d{4,6}(?:v\d+)?)",
    re.IGNORECASE,
)


def arxiv_id_from_url(url: str) -> str | None:
    """Return the individual arXiv identifier carried by a canonical URL."""
    match = _ARXIV_URL_RE.match(url.strip()) if url else None
    return match.group(1) if match else None


class _AbsLicenseParser(HTMLParser):
    """Extract the rights link from an arXiv abstract page."""

    def __init__(self) -> None:
        super().__init__()
        self._license_depth = 0
        self.value: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        classes = set((attributes.get("class") or "").split())
        if tag == "div" and "abs-license" in classes:
            self._license_depth = 1
            return
        if self._license_depth:
            self._license_depth += 1
            if tag == "a" and attributes.get("href"):
                self.value = attributes["href"]

    def handle_endtag(self, tag: str) -> None:
        if self._license_depth:
            self._license_depth -= 1


def _license_from_abs_html(payload: bytes) -> str | None:
    parser = _AbsLicenseParser()
    parser.feed(payload.decode("utf-8", errors="replace"))
    return parser.value


def license_from_arxiv_abs_html(payload: bytes) -> str | None:
    """Public bounded-parser entry point for archived arXiv abs pages."""
    return _license_from_abs_html(payload)


async def fetch_arxiv_license_with_source(
    arxiv_id: str,
    client: httpx.AsyncClient,
    *,
    bucket: TokenBucket,
    min_sleep_s: float = 1.0,
) -> tuple[str | None, str]:
    """Return item-level rights and their exact metadata provenance.

    The abstract page is tried first because it exposes the same per-paper
    rights link and is materially more reliable than issuing one Atom API
    query per paper. The Atom API remains a fallback for unusual pages.
    """
    await bucket.acquire()
    if min_sleep_s > 0:
        await asyncio.sleep(min_sleep_s)
    try:
        response = await client.get(f"{ARXIV_ABS_BASE}/{arxiv_id}")
        response.raise_for_status()
    except httpx.HTTPError as exc:
        log.warning("arxiv.license_abs_failed", arxiv_id=arxiv_id, err=str(exc))
    else:
        value = _license_from_abs_html(response.content)
        if value:
            return value, "html_meta"

    await bucket.acquire()
    if min_sleep_s > 0:
        await asyncio.sleep(min_sleep_s)
    try:
        response = await client.get(
            ARXIV_API_QUERY,
            params={"id_list": arxiv_id, "start": "0", "max_results": "1"},
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)
    except (httpx.HTTPError, ET.ParseError) as exc:
        log.warning("arxiv.license_metadata_failed", arxiv_id=arxiv_id, err=str(exc))
        return None, "unknown"
    node = root.find(".//{http://arxiv.org/schemas/atom}license")
    if node is not None and node.text and node.text.strip():
        return node.text.strip(), "arxiv_api"
    return None, "unknown"


async def fetch_arxiv_license(
    arxiv_id: str,
    client: httpx.AsyncClient,
    *,
    bucket: TokenBucket,
    min_sleep_s: float = 1.0,
) -> str | None:
    """Return only the item-level license value."""
    value, _ = await fetch_arxiv_license_with_source(
        arxiv_id,
        client,
        bucket=bucket,
        min_sleep_s=min_sleep_s,
    )
    return value
