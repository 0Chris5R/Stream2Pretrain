"""Tests for :mod:`processor.seed.wayback_backfill`."""

from __future__ import annotations

from datetime import UTC, datetime

from processor.seed import wayback_backfill as wb
from processor.seed.cursor import SeedCursor

_TIMEMAP = (
    '<https://web.archive.org/web/timemap/link/https://example.com/feed.xml>;'
    ' rel="self"; type="application/link-format",\n'
    '<https://web.archive.org/web/20240101000000/https://example.com/feed.xml>;'
    ' rel="memento"; datetime="Mon, 01 Jan 2024 00:00:00 GMT",\n'
    '<https://web.archive.org/web/20250515000000/https://example.com/feed.xml>;'
    ' rel="memento"; datetime="Thu, 15 May 2025 00:00:00 GMT",\n'
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


class _FakeWaybackClient:
    """Stand-in :class:`wb.WaybackClient` that returns canned data."""

    def __init__(self, snapshots: dict[str, list[wb.Snapshot]], bodies: dict[str, str]) -> None:
        self._snapshots = snapshots
        self._bodies = bodies
        self.fetch_calls: int = 0
        self.closed = False

    def fetch_timemap(self, feed_url: str) -> list[wb.Snapshot]:
        return self._snapshots.get(feed_url, [])

    def fetch_playback(self, snapshot: wb.Snapshot) -> str:
        self.fetch_calls += 1
        return self._bodies.get(snapshot.timestamp, "")

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
    bodies = {
        "20250101000000": "<rss>old</rss>",
        "20251001000000": "<rss>new</rss>",
    }
    client = _FakeWaybackClient(snaps_by_url, bodies)
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
    assert out[0].source_format == "web"
    assert out[0].extraction_pipeline == "wayback-backfill-2026-06"
    assert out[0].extra["feed_name"] == "bair"
    # We do not own the client when we passed it in -> we do not close it.
    assert client.closed is False


def test_iter_documents_drops_empty_body() -> None:
    feed = wb.WaybackFeed(name="bair", url="https://bair.berkeley.edu/blog/feed.xml")
    snaps_by_url: dict[str, list[wb.Snapshot]] = {
        feed.url: [wb.Snapshot(timestamp="20251001000000", url=feed.url)]
    }
    client = _FakeWaybackClient(snaps_by_url, {"20251001000000": ""})
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
    assert out == []


def test_native_id_for_format() -> None:
    feed = wb.WaybackFeed(name="bair", url="https://x")
    snap = wb.Snapshot(timestamp="20251001000000", url="https://x")
    assert wb.native_id_for(feed, snap) == "bair:20251001000000"


def test_iter_documents_respects_cursor() -> None:
    feed = wb.WaybackFeed(name="bair", url="https://x")
    snaps_by_url: dict[str, list[wb.Snapshot]] = {
        feed.url: [
            wb.Snapshot(timestamp="20250601000000", url="https://x"),
            wb.Snapshot(timestamp="20250701000000", url="https://x"),
        ]
    }
    bodies = {"20250601000000": "old", "20250701000000": "new"}
    client = _FakeWaybackClient(snaps_by_url, bodies)
    cursor = SeedCursor(repo_id="wayback:multi-feed")
    cursor.last_native_id = "bair:20250601000000"
    out = list(
        wb.iter_documents(
            cursor,
            feeds=[feed],
            months=24,
            now=datetime(2026, 6, 15, tzinfo=UTC),
            client=client,
        )
    )
    assert [d.native_id for d in out] == ["bair:20250701000000"]
