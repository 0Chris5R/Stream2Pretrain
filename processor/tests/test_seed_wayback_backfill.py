"""Tests for :mod:`processor.seed.wayback_backfill`."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx

from processor.seed import wayback_backfill as wb
from processor.seed.cursor import SeedCursor

_TIMEMAP = (
    "<https://web.archive.org/web/timemap/link/https://example.com/feed.xml>;"
    ' rel="self"; type="application/link-format",\n'
    "<https://web.archive.org/web/20240101000000/https://example.com/feed.xml>;"
    ' rel="memento"; datetime="Mon, 01 Jan 2024 00:00:00 GMT",\n'
    "<https://web.archive.org/web/20250515000000/https://example.com/feed.xml>;"
    ' rel="memento"; datetime="Thu, 15 May 2025 00:00:00 GMT",\n'
)


def _rss(item_url: str, *, license_value: str | None = "CC-BY-4.0") -> str:
    rights = f"<dc:rights>{license_value}</dc:rights>" if license_value else ""
    return (
        "<rss xmlns:dc='http://purl.org/dc/elements/1.1/'><channel><title>Feed</title><item>"
        f"<title>Archived item</title><link>{item_url}</link>{rights}"
        "</item></channel></rss>"
    )


def test_parse_timemap_returns_only_mementos() -> None:
    snaps = wb.parse_timemap(_TIMEMAP, original_url="https://example.com/feed.xml")
    assert len(snaps) == 2
    assert snaps[0].timestamp == "20240101000000"
    assert snaps[1].timestamp == "20250515000000"
    assert all(s.url == "https://example.com/feed.xml" for s in snaps)


def test_snapshot_captured_at_round_trip() -> None:
    snap = wb.Snapshot(timestamp="20250515123456", url="https://x")
    assert snap.captured_at() == datetime(2025, 5, 15, 12, 34, 56, tzinfo=UTC)


def test_snapshot_playback_url_uses_id_modifier() -> None:
    snap = wb.Snapshot(timestamp="20240101000000", url="https://example.com/feed.xml")
    assert "id_/https://example.com/feed.xml" in snap.playback_url()
    assert "/web/20240101000000id_/" in snap.playback_url()


def test_filter_recent_drops_old_snapshots() -> None:
    now = datetime(2026, 6, 15, tzinfo=UTC)
    snaps = [
        wb.Snapshot(timestamp="20200101000000", url="https://x"),  # 6+ years ago
        wb.Snapshot(timestamp="20250101000000", url="https://x"),
    ]
    out = wb.filter_recent(snaps, months=24, now=now)
    assert len(out) == 1
    assert out[0].timestamp == "20250101000000"


def test_archived_feed_is_discovery_only_and_ignores_channel_copyright() -> None:
    body = (
        "<rss xmlns:dc='http://purl.org/dc/elements/1.1/'><channel>"
        "<copyright>CC-BY-4.0</copyright><item><title>Item</title>"
        "<link>https://example.com/item</link></item></channel></rss>"
    )
    items = wb.discover_archived_items(body)
    assert len(items) == 1
    assert items[0].url == "https://example.com/item"
    assert items[0].license_value is None


def test_archived_item_license_probe_is_bounded_and_capture_scoped() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            206,
            text=(
                "<html><head><link rel='license' "
                "href='https://creativecommons.org/licenses/by/4.0/'></head></html>"
            ),
        )

    http = httpx.Client(transport=httpx.MockTransport(handler))
    client = wb.WaybackClient(client=http)
    snapshot = wb.Snapshot(
        timestamp="20251001000000",
        url="https://example.com/feed.xml",
    )
    item = wb.ArchivedItem(
        url="https://example.com/item",
        title="Item",
        license_value=None,
    )
    try:
        evidence = client.probe_item_license(snapshot, item, max_probe_bytes=1024)
    finally:
        client.close()

    assert evidence.raw_license == "https://creativecommons.org/licenses/by/4.0/"
    assert evidence.license_source == "html_meta"
    assert evidence.evidence_revision == snapshot.timestamp
    assert "/web/20251001000000id_/https://example.com/item" in evidence.evidence_url
    assert requests[0].headers["range"] == "bytes=0-1023"


def test_arxiv_wayback_candidate_targets_archived_full_html() -> None:
    feed = wb.WaybackFeed(name="rss-arxiv-cs-ai", url="https://rss.arxiv.org/rss/cs.AI")
    snapshot = wb.Snapshot(timestamp="20251001000000", url=feed.url)
    item = wb.ArchivedItem(
        url="https://arxiv.org/abs/2510.01234",
        title="Paper",
        license_value="CC-BY-4.0",
    )
    evidence = wb.ArchivedLicenseEvidence(
        raw_license="CC-BY-4.0",
        license_source="rss_entry",
        resolver="wayback-archived-feed-item-rights",
        evidence_url=snapshot.playback_url(),
        evidence_revision=snapshot.timestamp,
    )
    client = _FakeWaybackClient(
        {},
        {},
        {item.url: "<html><article>Scientific body</article></html>"},
    )
    doc = wb.to_seed_document(feed, snapshot, item, evidence, client=client)  # type: ignore[arg-type]

    assert str(doc.url) == "https://arxiv.org/html/2510.01234"
    assert doc.extraction_pipeline == "wayback-arxiv-html-item-v1"


def test_retained_blog_body_is_extracted_after_admission() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            text=(
                "<html><head><title>Archived</title></head><body>"
                "<nav>Navigation</nav><main>Retained article text.</main>"
                "</body></html>"
            ),
        )

    http = httpx.Client(transport=httpx.MockTransport(handler))
    client = wb.WaybackClient(client=http)
    snapshot = wb.Snapshot(
        timestamp="20251001000000",
        url="https://example.com/feed.xml",
    )
    item = wb.ArchivedItem(
        url="https://example.com/item",
        title="Archived",
        license_value="CC-BY-4.0",
    )
    try:
        text = client.fetch_item_body(snapshot, item)
    finally:
        client.close()

    assert "Retained article text" in text
    assert "<main>" not in text
    assert "/web/20251001000000id_/https://example.com/item" in str(requests[0].url)


def test_retained_arxiv_body_preserves_scientific_text() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text=(
                "<html><head><title>Paper</title></head><body><article>"
                "<h1>Methods</h1><p>Scientific result.</p>"
                "</article></body></html>"
            ),
        )

    http = httpx.Client(transport=httpx.MockTransport(handler))
    client = wb.WaybackClient(client=http)
    snapshot = wb.Snapshot(
        timestamp="20251001000000",
        url="https://rss.arxiv.org/rss/cs.AI",
    )
    item = wb.ArchivedItem(
        url="https://arxiv.org/abs/2510.01234",
        title="Paper",
        license_value="CC-BY-4.0",
    )
    try:
        text = client.fetch_item_body(snapshot, item)
    finally:
        client.close()

    assert "Scientific result" in text
    assert "<article>" not in text


class _FakeWaybackClient:
    """Stand-in :class:`wb.WaybackClient` that returns canned data."""

    def __init__(
        self,
        snapshots: dict[str, list[wb.Snapshot]],
        discovery_bodies: dict[str, str],
        item_bodies: dict[str, str] | None = None,
    ) -> None:
        self._snapshots = snapshots
        self._discovery_bodies = discovery_bodies
        self._item_bodies = item_bodies or {}
        self.discovery_fetch_calls: int = 0
        self.body_fetch_calls: int = 0
        self.closed = False

    def fetch_timemap(self, feed_url: str) -> list[wb.Snapshot]:
        return self._snapshots.get(feed_url, [])

    def fetch_playback(self, snapshot: wb.Snapshot) -> str:
        self.discovery_fetch_calls += 1
        return self._discovery_bodies.get(snapshot.timestamp, "")

    def probe_item_license(
        self, snapshot: wb.Snapshot, item: wb.ArchivedItem
    ) -> wb.ArchivedLicenseEvidence:
        return wb.ArchivedLicenseEvidence(
            raw_license=item.license_value,
            license_source="rss_entry" if item.license_value else "unknown",
            resolver="wayback-archived-feed-item-rights",
            evidence_url=snapshot.playback_url(),
            evidence_revision=snapshot.timestamp,
        )

    def fetch_item_body(self, snapshot: wb.Snapshot, item: wb.ArchivedItem) -> str:
        self.body_fetch_calls += 1
        return self._item_bodies.get(item.url, "")

    def close(self) -> None:
        self.closed = True


def test_iter_documents_emits_recent_snapshots_only() -> None:
    feed = wb.WaybackFeed(name="bair", url="https://bair.berkeley.edu/blog/feed.xml")
    snaps_by_url: dict[str, list[wb.Snapshot]] = {
        feed.url: [
            wb.Snapshot(timestamp="20200101000000", url=feed.url),
            wb.Snapshot(timestamp="20250101000000", url=feed.url),
            wb.Snapshot(timestamp="20251001000000", url=feed.url),
        ]
    }
    discovery_bodies = {
        "20250101000000": _rss("https://example.com/old"),
        "20251001000000": _rss("https://example.com/new"),
    }
    client = _FakeWaybackClient(
        snaps_by_url,
        discovery_bodies,
        {
            "https://example.com/old": "<html><main>Old article body</main></html>",
            "https://example.com/new": "<html><main>New article body</main></html>",
        },
    )
    cursor = SeedCursor(repo_id="wayback:multi-feed")
    out = list(
        wb.iter_documents(
            cursor,
            feeds=[feed],
            months=24,
            now=datetime(2026, 6, 15, tzinfo=UTC),
            client=client,
        )
    )
    assert len(out) == 2
    assert out[0].source_format == "html"
    assert out[0].extraction_pipeline == "wayback-page-html-item-v1"
    assert out[0].extra["feed_name"] == "bair"
    assert out[0].text == ""
    assert client.body_fetch_calls == 0
    assert out[0].materialize_body() is not None
    assert client.body_fetch_calls == 1
    # We do not own the client when we passed it in -> we do not close it.
    assert client.closed is False


def test_iter_documents_never_emits_archived_feed_xml_as_body() -> None:
    feed = wb.WaybackFeed(name="bair", url="https://bair.berkeley.edu/blog/feed.xml")
    snaps_by_url: dict[str, list[wb.Snapshot]] = {
        feed.url: [wb.Snapshot(timestamp="20251001000000", url=feed.url)]
    }
    client = _FakeWaybackClient(
        snaps_by_url,
        {"20251001000000": _rss("https://example.com/item")},
        {"https://example.com/item": ""},
    )
    cursor = SeedCursor(repo_id="wayback:multi-feed")
    out = list(
        wb.iter_documents(
            cursor,
            feeds=[feed],
            months=24,
            now=datetime(2026, 6, 15, tzinfo=UTC),
            client=client,
        )
    )
    assert len(out) == 1
    assert out[0].text == ""
    assert out[0].materialize_body() is None


def test_native_id_for_format() -> None:
    feed = wb.WaybackFeed(name="bair", url="https://x")
    snap = wb.Snapshot(timestamp="20251001000000", url="https://x")
    value = wb.native_id_for(feed, snap, "https://example.com/item")
    assert value.startswith("bair:20251001000000:")


def test_unknown_wayback_feed_never_claims_manual_override() -> None:
    feed = wb.WaybackFeed(name="bair", url="https://x")
    snap = wb.Snapshot(timestamp="20251001000000", url="https://x")

    item = wb.ArchivedItem(url="https://example.com/item", title="Item", license_value=None)
    evidence = wb.ArchivedLicenseEvidence(
        raw_license=None,
        license_source="unknown",
        resolver="wayback-bounded-item-license-probe",
        evidence_url=snap.playback_url_for(item.url),
        evidence_revision=snap.timestamp,
    )
    client = _FakeWaybackClient({}, {}, {item.url: "<html>body</html>"})
    doc = wb.to_seed_document(feed, snap, item, evidence, client=client)  # type: ignore[arg-type]

    assert doc.spdx_license is None
    assert doc.spdx_license_source == "unknown"
    assert doc.license_evidence_scope == "unknown"
    assert doc.license_evidence_revision == "20251001000000"


def test_iter_documents_respects_cursor() -> None:
    feed = wb.WaybackFeed(name="bair", url="https://x")
    snaps_by_url: dict[str, list[wb.Snapshot]] = {
        feed.url: [
            wb.Snapshot(timestamp="20250601000000", url="https://x"),
            wb.Snapshot(timestamp="20250701000000", url="https://x"),
        ]
    }
    bodies = {
        "20250601000000": _rss("https://example.com/old"),
        "20250701000000": _rss("https://example.com/new"),
    }
    client = _FakeWaybackClient(snaps_by_url, bodies)
    cursor = SeedCursor(repo_id="wayback:multi-feed")
    old_id = wb.native_id_for(feed, snaps_by_url[feed.url][0], "https://example.com/old")
    cursor.advance(old_id)
    out = list(
        wb.iter_documents(
            cursor,
            feeds=[feed],
            months=24,
            now=datetime(2026, 6, 15, tzinfo=UTC),
            client=client,
        )
    )
    assert [d.url for d in out] == ["https://example.com/new"]
