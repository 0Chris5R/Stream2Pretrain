"""Tests for the per-feed state store."""

from __future__ import annotations

from pathlib import Path
from typing import Any

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


class _S3Error(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}


class _FakeS3:
    def __init__(self, *, bucket_exists: bool = True) -> None:
        self.bucket_exists = bucket_exists
        self.objects: dict[tuple[str, str], bytes] = {}
        self.created: list[str] = []

    def head_bucket(self, **kwargs: str) -> None:
        assert kwargs["Bucket"]
        if not self.bucket_exists:
            raise _S3Error("NoSuchBucket")

    def create_bucket(self, **kwargs: str) -> None:
        self.bucket_exists = True
        self.created.append(kwargs["Bucket"])

    def get_object(self, **kwargs: str) -> dict[str, Any]:
        try:
            value = self.objects[(kwargs["Bucket"], kwargs["Key"])]
        except KeyError as exc:
            raise _S3Error("NoSuchKey") from exc
        from io import BytesIO

        return {"Body": BytesIO(value)}

    def put_object(self, **kwargs: Any) -> None:
        assert kwargs["ContentType"] == "application/json"
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = kwargs["Body"].read()


def test_s3_round_trip_uses_component_scoped_objects(monkeypatch) -> None:
    monkeypatch.setenv("S2P_COMPONENT", "ingest-rss")
    fake = _FakeS3()
    store = FeedStateStore(
        "/var/lib/s2p-state/rss_poller",
        backend="s3",
        bucket="s2p-state",
        s3_client=fake,
    )

    assert store.get("rss-bair-blog") == {}
    store.put("rss-bair-blog", {"etag": "abc"})

    assert store.get("rss-bair-blog") == {"etag": "abc"}
    assert (
        "s2p-state",
        "ingest-cursors/ingest-rss/rss-bair-blog.json",
    ) in fake.objects


def test_s3_backend_creates_missing_state_bucket() -> None:
    fake = _FakeS3(bucket_exists=False)

    FeedStateStore("state", backend="s3", bucket="s2p-state", s3_client=fake)

    assert fake.created == ["s2p-state"]


def test_unknown_state_backend_is_rejected(tmp_path: Path) -> None:
    import pytest

    with pytest.raises(ValueError, match="unsupported feed-state backend"):
        FeedStateStore(tmp_path, backend="database")
