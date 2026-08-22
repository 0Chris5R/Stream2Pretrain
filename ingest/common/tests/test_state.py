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


def test_dev_state_root_override(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("S2P_STATE_ROOT", str(tmp_path / "state"))
    store = FeedStateStore(".s2p-state/hf")
    store.put("feed", {"cursor": "x"})

    assert (tmp_path / "state" / "hf" / "feed.json").exists()


def test_state_keys_are_portable_file_names(tmp_path: Path) -> None:
    store = FeedStateStore(tmp_path)
    store.put("openreview:ICLR.cc:2026", {"cursor": "abc"})

    assert store.get("openreview:ICLR.cc:2026") == {"cursor": "abc"}
    assert [path.name for path in tmp_path.iterdir()] == ["openreview%3AICLR.cc%3A2026.json"]


def test_legacy_state_file_is_read_during_filename_migration(tmp_path: Path) -> None:
    legacy = tmp_path / "github-releases_huggingface_transformers.json"
    legacy.write_text('{"etag": "old"}', encoding="utf-8")

    store = FeedStateStore(tmp_path)

    assert store.get("github-releases/huggingface_transformers") == {"etag": "old"}
