"""Validity-interval enricher.

Implements Stream2Pretrain's per-document ``[valid_from, valid_to)``
column. The enricher picks ``valid_from`` from the first available source
in this precedence order (highest authority first):

    1. HTTP Last-Modified header
    2. ``schema.org`` ``datePublished`` JSON-LD
    3. Sitemap ``lastmod`` field
    4. Wayback Machine first-seen capture
    5. Fetcher ``fetched_at`` (fallback - never preferred)

``valid_to`` is the smallest of:

    - Licence expiry date (if the licence is time-bounded - rare)
    - Retraction date (when the publisher / arxiv flagged the paper)
    - ``None`` for open-ended intervals

The chosen source is recorded in ``valid_from_source`` so downstream
consumers can rebuild the chronology.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx

from schemas.silver import ValidFromSource

_SCHEMA_DATE_PATTERN = re.compile(
    r'"datePublished"\s*:\s*"([^"]+)"', re.IGNORECASE | re.DOTALL
)
_LASTMOD_PATTERN = re.compile(r"<lastmod>([^<]+)</lastmod>", re.IGNORECASE)


def parse_iso8601(value: str | None) -> datetime | None:
    """Robust ISO 8601 parser - tolerates trailing ``Z`` and offsets."""
    if not value:
        return None
    candidate = value.strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError:
        # Common alt format: "Wed, 12 Jun 2026 08:00:00 GMT"
        try:
            from email.utils import parsedate_to_datetime

            dt = parsedate_to_datetime(value)
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def first_schema_date(html: str) -> datetime | None:
    """Pick the first ``schema.org`` ``datePublished`` out of HTML/JSON-LD."""
    for m in _SCHEMA_DATE_PATTERN.finditer(html):
        dt = parse_iso8601(m.group(1))
        if dt:
            return dt
    # Search any inline JSON-LD blocks too.
    for block in re.findall(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        try:
            payload = json.loads(block)
        except Exception:
            continue
        for value in _walk_for_key(payload, "datePublished"):
            dt = parse_iso8601(value)
            if dt:
                return dt
    return None


def _walk_for_key(obj: object, target: str) -> Iterable[str]:
    """Yield every string value under ``target`` keys in a JSON tree."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == target and isinstance(v, str):
                yield v
            else:
                yield from _walk_for_key(v, target)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk_for_key(item, target)


def first_sitemap_lastmod(xml: str) -> datetime | None:
    """Read the first ``<lastmod>`` value from a sitemap XML string."""
    for m in _LASTMOD_PATTERN.finditer(xml):
        dt = parse_iso8601(m.group(1))
        if dt:
            return dt
    return None


@dataclass(frozen=True, slots=True)
class ValidityInterval:
    """Output of :meth:`ValidityEnricher.enrich`."""

    valid_from: datetime
    valid_to: datetime | None
    valid_from_source: ValidFromSource


class ValidityEnricher:
    """Picks ``valid_from`` from the highest-authority source available."""

    def __init__(
        self,
        *,
        wayback_lookup: WaybackLookup | None = None,
    ) -> None:
        self._wayback = wayback_lookup

    def enrich(
        self,
        *,
        url: str,
        fetched_at: datetime,
        http_last_modified: datetime | None = None,
        html: str | None = None,
        sitemap_xml: str | None = None,
        license_effective_date: datetime | None = None,
        retraction_date: datetime | None = None,
    ) -> ValidityInterval:
        """Compute the validity interval for one document.

        Sources are evaluated in precedence order; the first hit wins.
        """
        valid_from, source = self._pick_valid_from(
            fetched_at=fetched_at,
            http_last_modified=http_last_modified,
            html=html,
            sitemap_xml=sitemap_xml,
            url=url,
            license_effective_date=license_effective_date,
        )
        valid_to = self._pick_valid_to(
            license_effective_date=None,  # licence expiry is a separate field
            retraction_date=retraction_date,
        )
        # Sanity: clamp to a non-zero interval so downstream sorting on
        # ``valid_from`` never collapses with the upper bound.
        if valid_to is not None and valid_to <= valid_from:
            valid_to = None
        return ValidityInterval(
            valid_from=valid_from,
            valid_to=valid_to,
            valid_from_source=source,
        )

    def _pick_valid_from(
        self,
        *,
        fetched_at: datetime,
        http_last_modified: datetime | None,
        html: str | None,
        sitemap_xml: str | None,
        url: str,
        license_effective_date: datetime | None,
    ) -> tuple[datetime, ValidFromSource]:
        if http_last_modified is not None:
            return _ensure_utc(http_last_modified), "http_last_modified"
        if html is not None:
            schema_dt = first_schema_date(html)
            if schema_dt is not None:
                return schema_dt, "schema_org_date_published"
        if sitemap_xml is not None:
            sm_dt = first_sitemap_lastmod(sitemap_xml)
            if sm_dt is not None:
                return sm_dt, "sitemap_lastmod"
        if self._wayback is not None:
            wb = self._wayback.first_seen(url)
            if wb is not None:
                return wb, "wayback_first_seen"
        if license_effective_date is not None:
            return _ensure_utc(license_effective_date), "license_effective_date"
        return _ensure_utc(fetched_at), "fetched_at"

    @staticmethod
    def _pick_valid_to(
        *,
        license_effective_date: datetime | None,
        retraction_date: datetime | None,
    ) -> datetime | None:
        candidates = [d for d in (license_effective_date, retraction_date) if d is not None]
        if not candidates:
            return None
        return min(_ensure_utc(d) for d in candidates)


def _ensure_utc(dt: datetime) -> datetime:
    """Force a tz-aware UTC datetime; treat naive as already UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


class WaybackLookup:
    """Wayback Machine first-capture helper.

    Uses the public ``api.archive.org/wayback/available`` endpoint. Cached
    in-memory by URL so a Bytewax worker that revisits a URL does not re-pay
    the round trip.
    """

    def __init__(
        self,
        client: httpx.Client | None = None,
        *,
        timeout: float = 5.0,
        endpoint: str = "https://archive.org/wayback/available",
    ) -> None:
        self._client = client or httpx.Client(timeout=timeout)
        self._endpoint = endpoint
        self._cache: dict[str, datetime | None] = {}

    def first_seen(self, url: str) -> datetime | None:
        """Return the first archived timestamp for ``url`` if any."""
        if url in self._cache:
            return self._cache[url]
        try:
            resp = self._client.get(self._endpoint, params={"url": url})
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            self._cache[url] = None
            return None
        snap = (data.get("archived_snapshots") or {}).get("closest")
        if not snap or not snap.get("timestamp"):
            self._cache[url] = None
            return None
        ts = snap["timestamp"]  # YYYYMMDDhhmmss
        try:
            dt = datetime.strptime(ts, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
        except Exception:
            dt = None
        self._cache[url] = dt
        return dt

    def close(self) -> None:
        """Release the underlying httpx client."""
        with suppress(Exception):
            self._client.close()
