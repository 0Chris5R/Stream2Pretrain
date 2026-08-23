"""Seed loader for the 24-month Wayback backfill of Phase-1 RSS/Atom feeds.

The Wayback walk turns each Phase-1 feed URL into a list of historical
snapshots via the Wayback ``timemap`` endpoint::

    https://web.archive.org/web/timemap/link/<feed_url>

For each snapshot we fetch the archived feed only as a discovery envelope::

    https://web.archive.org/web/<timestamp>id_/<feed_url>

The ``id_`` modifier asks Wayback to return the original payload without the
toolbar. Individual item rights are resolved from archived item evidence.
Only after the admission record is durable does the in-process seed runner
invoke a deferred fetch for the archived item page or arXiv HTML body. Feed XML
and entry summaries never become training text.

The loader is intentionally **synchronous** and self-contained (it does
NOT pull in :mod:`ingest.common.http_client` because that lives in the
ingest package and has its own async config object). The processor lane
only depends on httpx, which is already in the runtime dependency set.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Iterator
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from urllib.parse import quote

import feedparser
import httpx

from ingest.arxiv_html_fetcher.extractor import extract_arxiv_html
from ingest.common.arxiv_license import arxiv_id_from_url, license_from_arxiv_abs_html
from ingest.common.license_admission import normalize_license
from ingest.common.page_license import license_from_html_head, license_from_link_header
from processor.operators.extract import ResiliparseExtractor
from processor.seed.cursor import SeedCursor
from processor.seed.types import SeedDocument
from schemas.bronze import SpdxLicenseSource

REPO_ID_PREFIX: str = "wayback"

# Public Wayback endpoints. ``id_`` returns the raw archived response.
TIMEMAP_TEMPLATE: str = "https://web.archive.org/web/timemap/link/{url}"
PLAYBACK_TEMPLATE: str = "https://web.archive.org/web/{ts}id_/{url}"

_LINK_DATETIME_RE = re.compile(r'datetime="([^"]+)"', re.IGNORECASE)
_LINK_HREF_RE = re.compile(r"<([^>]+)>", re.IGNORECASE)
_TIMESTAMP_IN_HREF_RE = re.compile(r"/web/(\d{14})/")


@dataclass(frozen=True, slots=True)
class WaybackFeed:
    """One Phase-1 RSS/Atom feed configured for backfill."""

    name: str
    url: str


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
        return datetime.strptime(self.timestamp, "%Y%m%d%H%M%S").replace(tzinfo=UTC)

    def playback_url(self) -> str:
        """Return the ``id_`` playback URL for the original payload."""
        return PLAYBACK_TEMPLATE.format(ts=self.timestamp, url=self.url)

    def playback_url_for(self, original_url: str) -> str:
        """Return the immutable playback URL for an item discovered here."""
        return PLAYBACK_TEMPLATE.format(ts=self.timestamp, url=original_url)


@dataclass(frozen=True, slots=True)
class ArchivedItem:
    """One item discovered inside an archived RSS/Atom envelope."""

    url: str
    title: str | None
    license_value: str | None


@dataclass(frozen=True, slots=True)
class ArchivedLicenseEvidence:
    raw_license: str | None
    license_source: SpdxLicenseSource
    resolver: str
    evidence_url: str
    evidence_revision: str


def discover_archived_items(body: str) -> list[ArchivedItem]:
    """Parse item URLs and item-level rights from archived feed XML.

    Channel copyright and feed ownership are deliberately ignored. The feed
    payload is a discovery envelope and is never returned as a SeedDocument.
    """
    parsed = feedparser.parse(body)
    seen: set[str] = set()
    output: list[ArchivedItem] = []
    for entry in parsed.get("entries", []):
        link = entry.get("link")
        if not link:
            for candidate in entry.get("links", []) or []:
                href = candidate.get("href")
                if href:
                    link = href
                    break
        if not isinstance(link, str) or not link.startswith(("http://", "https://")):
            continue
        if link in seen:
            continue
        seen.add(link)
        rights = entry.get("rights") or entry.get("dc_rights") or entry.get("license")
        if isinstance(rights, dict):
            rights = rights.get("href") or rights.get("value")
        title = entry.get("title")
        output.append(
            ArchivedItem(
                url=link,
                title=str(title) if title else None,
                license_value=str(rights) if rights else None,
            )
        )
    return output


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


def filter_recent(snapshots: Iterable[Snapshot], *, months: int, now: datetime) -> list[Snapshot]:
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
        """Fetch an archived feed discovery envelope; empty string on error."""
        try:
            resp = self.client.get(
                snapshot.playback_url(),
                headers={"User-Agent": self.user_agent},
            )
            resp.raise_for_status()
        except Exception:
            return ""
        return resp.text

    def probe_item_license(
        self,
        snapshot: Snapshot,
        item: ArchivedItem,
        *,
        max_probe_bytes: int = 65_536,
    ) -> ArchivedLicenseEvidence:
        """Resolve archived item rights using a bounded capture probe."""
        arxiv_id = arxiv_id_from_url(item.url)
        evidence_original = f"https://arxiv.org/abs/{arxiv_id}" if arxiv_id else item.url
        evidence_url = snapshot.playback_url_for(evidence_original)
        if normalize_license(item.license_value) != "unknown":
            return ArchivedLicenseEvidence(
                raw_license=item.license_value,
                license_source="rss_entry",
                resolver="wayback-archived-feed-item-rights",
                evidence_url=snapshot.playback_url(),
                evidence_revision=snapshot.timestamp,
            )

        headers = {
            "User-Agent": self.user_agent,
            "Accept": "text/html, application/xhtml+xml;q=0.9",
            "Range": f"bytes=0-{max_probe_bytes - 1}",
        }
        try:
            with self.client.stream("GET", evidence_url, headers=headers) as response:
                response.raise_for_status()
                chunks: list[bytes] = []
                remaining = max_probe_bytes
                for chunk in response.iter_bytes():
                    if remaining <= 0:
                        break
                    part = chunk[:remaining]
                    chunks.append(part)
                    remaining -= len(part)
                    if remaining <= 0:
                        break
                payload = b"".join(chunks)
                link_value = license_from_link_header(
                    response.headers.get("link"),
                    base_url=evidence_original,
                )
        except Exception:
            return ArchivedLicenseEvidence(
                raw_license=None,
                license_source="unknown",
                resolver="wayback-bounded-item-license-probe",
                evidence_url=evidence_url,
                evidence_revision=snapshot.timestamp,
            )
        value = link_value
        if arxiv_id and normalize_license(value) == "unknown":
            value = license_from_arxiv_abs_html(payload)
        if normalize_license(value) == "unknown":
            value = license_from_html_head(payload, base_url=evidence_original)
        source: SpdxLicenseSource = (
            "http_link"
            if link_value and normalize_license(link_value) != "unknown"
            else "html_meta"
            if normalize_license(value) != "unknown"
            else "unknown"
        )
        return ArchivedLicenseEvidence(
            raw_license=value,
            license_source=source,
            resolver=(
                "wayback-archived-http-license-link"
                if source == "http_link"
                else "wayback-archived-item-license-metadata"
                if source == "html_meta"
                else "wayback-bounded-item-license-probe"
            ),
            evidence_url=evidence_url,
            evidence_revision=snapshot.timestamp,
        )

    def fetch_item_body(self, snapshot: Snapshot, item: ArchivedItem) -> str:
        """Fetch and extract one retained item after admission is durable.

        The seed path emits directly to Silver, so archived HTML markup must
        not become training text. arXiv captures use the same math-preserving
        scientific extractor as the live worker. Ordinary pages use the
        production Resiliparse main-content profile.
        """
        arxiv_id = arxiv_id_from_url(item.url)
        original_url = f"https://arxiv.org/html/{arxiv_id}" if arxiv_id else item.url
        try:
            resp = self.client.get(
                snapshot.playback_url_for(original_url),
                headers={"User-Agent": self.user_agent},
            )
            resp.raise_for_status()
        except Exception:
            return ""
        if arxiv_id:
            return extract_arxiv_html(
                resp.content,
                pipeline="wayback-arxiv-html-item-v1",
            ).text
        return ResiliparseExtractor().extract(resp.content).text

    def close(self) -> None:
        """Release the underlying httpx client."""
        with suppress(Exception):
            self.client.close()


def native_id_for(feed: WaybackFeed, snapshot: Snapshot, item_url: str = "") -> str:
    """Stable per-item id inside one feed capture."""
    suffix = hashlib.sha256(item_url.encode()).hexdigest()[:16] if item_url else "feed"
    return f"{feed.name}:{snapshot.timestamp}:{suffix}"


def to_seed_document(
    feed: WaybackFeed,
    snapshot: Snapshot,
    item: ArchivedItem,
    evidence: ArchivedLicenseEvidence,
    *,
    client: WaybackClient,
) -> SeedDocument:
    """Build a deferred item body; archived feed XML never becomes corpus text."""
    nid = native_id_for(feed, snapshot, item.url)
    arxiv_id = arxiv_id_from_url(item.url)
    content_url = f"https://arxiv.org/html/{arxiv_id}" if arxiv_id else item.url
    return SeedDocument(
        repo_id=f"{REPO_ID_PREFIX}:{feed.name}",
        native_id=nid,
        url=content_url,
        title=item.title,
        text="",
        lang="en",
        valid_from=snapshot.captured_at(),
        source_format="html",
        extraction_pipeline=(
            "wayback-arxiv-html-item-v1" if arxiv_id else "wayback-page-html-item-v1"
        ),
        spdx_license=evidence.raw_license,
        spdx_license_source=evidence.license_source,
        license_resolver=evidence.resolver,
        license_evidence_url=evidence.evidence_url,
        license_evidence_revision=evidence.evidence_revision,
        license_evidence_scope=(
            "item" if normalize_license(evidence.raw_license) != "unknown" else "unknown"
        ),
        body_loader=lambda: client.fetch_item_body(snapshot, item),
        extra={
            "wayback_timestamp": snapshot.timestamp,
            "feed_name": feed.name,
            "discovery_feed_url": feed.url,
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
    when = now or datetime.now(tz=UTC)
    emitted = 0
    try:
        for feed in feeds:
            snapshots = client.fetch_timemap(feed.url)
            recent = sorted(
                filter_recent(snapshots, months=months, now=when),
                key=lambda value: value.timestamp,
            )
            for snap in recent:
                if max_docs is not None and emitted >= max_docs:
                    return
                discovery = client.fetch_playback(snap)
                if not discovery.strip():
                    continue
                items = sorted(
                    discover_archived_items(discovery),
                    key=lambda value: native_id_for(feed, snap, value.url),
                )
                for item in items:
                    if max_docs is not None and emitted >= max_docs:
                        return
                    nid = native_id_for(feed, snap, item.url)
                    if cursor.should_skip(nid):
                        continue
                    evidence = client.probe_item_license(snap, item)
                    yield to_seed_document(
                        feed,
                        snap,
                        item,
                        evidence,
                        client=client,
                    )
                    emitted += 1
    finally:
        if own_client:
            client.close()


__all__ = [
    "DEFAULT_FEEDS",
    "PLAYBACK_TEMPLATE",
    "REPO_ID_PREFIX",
    "TIMEMAP_TEMPLATE",
    "ArchivedItem",
    "ArchivedLicenseEvidence",
    "Snapshot",
    "WaybackClient",
    "WaybackFeed",
    "discover_archived_items",
    "filter_recent",
    "iter_documents",
    "native_id_for",
    "parse_timemap",
    "to_seed_document",
]
