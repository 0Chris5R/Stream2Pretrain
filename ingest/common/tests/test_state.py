"""Tests for the per-feed state store."""

from __future__ import annotations

from pathlib import Path

from ingest.common.state import FeedStateStore


def test_round_trip(tmp_path: Path) -> None:
    store = FeedStateStore(tmp_path)
    store.put("feed-a", {"etag": "abc", "last_modified": "Mon, 14 Jun 2026"})
    assert store.get("feed-a") == {"etag": "abc", "last_modified": "Mon, 14 Jun 2026"}


def test_missing_returns_empty(tmp_path: Path) -> None:
    store = FeedStateStore(tmp_path)
    assert store.get("nope") == {}


def test_corrupt_file_returns_empty(tmp_path: Path) -> None:
    store = FeedStateStore(tmp_path)
    p = tmp_path / "feed-a.json"
    p.write_text("{not json", encoding="utf-8")
    assert store.get("feed-a") == {}


def test_overwrite_atomic(tmp_path: Path) -> None:
    store = FeedStateStore(tmp_path)
    store.put("f", {"v": 1})
    store.put("f", {"v": 2})
    assert store.get("f") == {"v": 2}
