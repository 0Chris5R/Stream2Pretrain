"""Seed loader for the 24-month Wayback backfill of Phase-1 RSS/Atom feeds.

The Wayback walk turns each Phase-1 feed URL into a list of historical
snapshots via the Wayback ``timemap`` endpoint::

    https://web.archive.org/web/timemap/link/<feed_url>

For each snapshot we then fetch::

    https://web.archive.org/web/<timestamp>id_/<feed_url>

The ``id_`` modifier asks Wayback to return the original payload without
the toolbar HTML header, which is what we want for downstream extraction.

The loader is intentionally **synchronous** and self-contained (it does
NOT pull in :mod:`ingest.common.http_client` because that lives in the
ingest package and has its own async config object). The processor lane
only depends on httpx, which is already in the runtime dependency set.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterable, Iterator
from urllib.parse import quote

import httpx

from processor.seed.cursor import SeedCursor
from processor.seed.types import SeedDocument

REPO_ID_PREFIX: str = "wayback"

# Public Wayback endpoints. ``id_`` returns the raw archived response.
TIMEMAP_TEMPLATE: str = "https://web.archive.org/web/timemap/link/{url}"
PLAYBACK_TEMPLATE: str = "https://web.archive.org/web/{ts}id_/{url}"

_LINK_DATETIME_RE = re.compile(r'datetime="([^"]+)"', re.IGNORECASE)
_LINK_HREF_RE = re.compile(r'<([^>]+)>', re.IGNORECASE)
_TIMESTAMP_IN_HREF_RE = re.compile(r"/web/(\d{14})/")


@dataclass(frozen=True, slots=True)
class WaybackFeed:
    """One Phase-1 RSS/Atom feed configured for backfill."""

    name: str
    url: str
    spdx_license: str | None = "unknown"


# Default Phase-1 feed list mirrors values.yaml (sources.blogs +
# arXiv RSS). Tests can override.
DEFAULT_FEEDS: tuple[WaybackFeed, ...] = (
    WaybackFeed(name="rss-arxiv-cs-cl", url="https://rss.arxiv.org/rss/cs.CL"),
    WaybackFeed(name="rss-arxiv-cs-lg", url="https://rss.arxiv.org/rss/cs.LG"),
    WaybackFeed(name="rss-arxiv-cs-ai", url="https://rss.arxiv.org/rss/cs.AI"),
    WaybackFeed(name="rss-arxiv-cs-cv", url="https://rss.arxiv.org/rss/cs.CV"),
    WaybackFeed(name="openai-news", url="https://openai.com/news/rss.xml"),
    WaybackFeed(name="deepmind-blog", url="https://deepmind.google/blog/rss.xml"),
    WaybackFeed(name="hf-blog", url="https://huggingface.co/blog/feed.xml"),
    WaybackFeed(name="bair-blog", url="https://bair.berkeley.edu/blog/feed.xml"),
    WaybackFeed(name="eleuther-blog", url="https://blog.eleuther.ai/index.xml"),
)


@dataclass(frozen=True, slots=True)
class Snapshot:
    """One Wayback timemap entry."""

    timestamp: str  # YYYYMMDDhhmmss
    url: str  # the original (non-Wayback) URL the snapshot was for

    def captured_at(self) -> datetime:
        """Parse :attr:`timestamp` to a timezone-aware UTC datetime."""
        return datetime.strptime(self.timestamp, "%Y%m%d%H%M%S").replace(
            tzinfo=timezone.utc
        )

    def playback_url(self) -> str:
        """Return the ``id_`` playback URL for the original payload."""
        return PLAYBACK_TEMPLATE.format(ts=self.timestamp, url=self.url)


def parse_timemap(body: str, *, original_url: str) -> list[Snapshot]:
    """Parse a Wayback timemap-link body into :class:`Snapshot` records.

    The link format is::

        <https://web.archive.org/web/20240105123456/https://example.com>; rel="memento"; datetime="..."; ,
        <...>; rel="..."; ...

    We deliberately do not pull in a Link-header parser; this is line-based
    and the regex pair is sufficient for the public timemap output.
    """
    out: list[Snapshot] = []
    for line in body.splitlines():
        href_match = _LINK_HREF_RE.search(line)
        ts_match = _TIMESTAMP_IN_HREF_RE.search(line)
        if not href_match or not ts_match:
            continue
        if "memento" not in line.lower():
            continue
        out.append(Snapshot(timestamp=ts_match.group(1), url=original_url))
    return out


def filter_recent(
    snapshots: Iterable[Snapshot], *, months: int, now: datetime
) -> list[Snapshot]:
    """Keep only snapshots within the last ``months`` months."""
    cutoff = now - timedelta(days=months * 30)
    return [s for s in snapshots if s.captured_at() >= cutoff]


@dataclass(slots=True)
class WaybackClient:
    """Tiny synchronous Wayback wrapper.

    Tests inject a ``client`` that already returns canned responses; in
    production the constructor builds an :class:`httpx.Client` with a 30s
    timeout.
    """

    client: httpx.Client = field(default_factory=lambda: httpx.Client(timeout=30.0))
    user_agent: str = "Stream2Pretrain-SeedLoader/0.2 (+https://github.com/stream2pretrain)"

    def fetch_timemap(self, feed_url: str) -> list[Snapshot]:
        """Return the Wayback snapshots for ``feed_url``; empty on error."""
        url = TIMEMAP_TEMPLATE.format(url=quote(feed_url, safe=":/?&="))
        try:
            resp = self.client.get(url, headers={"User-Agent": self.user_agent})
            resp.raise_for_status()
        except Exception:
            return []
        return parse_timemap(resp.text, original_url=feed_url)

    def fetch_playback(self, snapshot: Snapshot) -> str:
        """Fetch the raw payload for ``snapshot``; empty string on error."""
        try:
            resp = self.client.get(
                snapshot.playback_url(),
                headers={"User-Agent": self.user_agent},
            )
            resp.raise_for_status()
        except Exception:
            return ""
        return resp.text

    def close(self) -> None:
        """Release the underlying httpx client."""
        try:
            self.client.close()
        except Exception:
            pass


def native_id_for(feed: WaybackFeed, snapshot: Snapshot) -> str:
    """Stable native id: ``<feed.name>:<timestamp>``."""
    return f"{feed.name}:{snapshot.timestamp}"


def to_seed_document(
    feed: WaybackFeed, snapshot: Snapshot, body: str
) -> SeedDocument | None:
    """Build a :class:`SeedDocument` for one snapshot.

    The body is left as-is (HTML / RSS / Atom string); downstream operators
    in the curate dataflow already know how to extract from it. The seed
    loader does not pre-extract because that would couple us to the
    extractor version and break replays.
    """
    if not body.strip():
        return None
    nid = native_id_for(feed, snapshot)
    return SeedDocument(
        repo_id=f"{REPO_ID_PREFIX}:{feed.name}",
        native_id=nid,
        url=feed.url,
        title=feed.name,
        text=body,
        lang="en",
        valid_from=snapshot.captured_at(),
        source_format="web",
        extraction_pipeline="wayback-backfill-2026-06",
        spdx_license=feed.spdx_license,
        spdx_license_source="manual_override" if feed.spdx_license else "unknown",
        extra={
            "wayback_timestamp": snapshot.timestamp,
            "feed_name": feed.name,
        },
    )


def iter_documents(
    cursor: SeedCursor,
    *,
    feeds: Iterable[WaybackFeed] = DEFAULT_FEEDS,
    months: int = 24,
    now: datetime | None = None,
    client: WaybackClient | None = None,
    max_docs: int | None = None,
) -> Iterator[SeedDocument]:
    """Stream :class:`SeedDocument` records for each Phase-1 feed's history.

    The cursor is shared across feeds; this matches the rest of the seed
    loaders' contract that one cursor file backs one ``repo_id``. For
    Wayback we use a synthetic ``repo_id = wayback:multi-feed`` to keep
    that contract.
    """
    own_client = False
    if client is None:
        client = WaybackClient()
        own_client = True
    when = now or datetime.now(tz=timezone.utc)
    emitted = 0
    try:
        for feed in feeds:
            snapshots = client.fetch_timemap(feed.url)
            recent = filter_recent(snapshots, months=months, now=when)
            for snap in recent:
                if max_docs is not None and emitted >= max_docs:
                    return
                nid = native_id_for(feed, snap)
                if cursor.should_skip(nid):
                    continue
                body = client.fetch_playback(snap)
                doc = to_seed_document(feed, snap, body)
                if doc is None:
                    continue
                yield doc
                emitted += 1
    finally:
        if own_client:
            client.close()


__all__ = [
    "REPO_ID_PREFIX",
    "TIMEMAP_TEMPLATE",
    "PLAYBACK_TEMPLATE",
    "WaybackFeed",
    "Snapshot",
    "DEFAULT_FEEDS",
    "WaybackClient",
    "parse_timemap",
    "filter_recent",
    "native_id_for",
    "to_seed_document",
    "iter_documents",
]
