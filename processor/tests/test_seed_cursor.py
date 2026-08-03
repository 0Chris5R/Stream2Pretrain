"""Tests for :mod:`processor.seed.cursor`."""

from __future__ import annotations

import io
from datetime import UTC, datetime
from typing import Any

import pytest

from processor.seed.cursor import CursorStore, SeedCursor


class _FakeS3:
    """In-memory boto3 stand-in (key/value, no bucket separation)."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.put_calls: int = 0

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        if (Bucket, Key) not in self.objects:
            raise KeyError(f"NoSuchKey: {Bucket}/{Key}")
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def put_object(
        self, *, Bucket: str, Key: str, Body: bytes, ContentType: str = "application/json"  # noqa: N803
    ) -> dict[str, Any]:
        self.put_calls += 1
        self.objects[(Bucket, Key)] = Body
        return {"ETag": '"deadbeef"'}


def test_seed_cursor_zero_value_does_not_skip() -> None:
    cursor = SeedCursor(repo_id="allenai/peS2o")
    assert cursor.should_skip("anything") is False


def test_seed_cursor_advance_and_skip() -> None:
    cursor = SeedCursor(repo_id="allenai/peS2o")
    cursor.advance("0000000000000010")
    assert cursor.last_native_id == "0000000000000010"
    assert cursor.rows_emitted == 1
    assert cursor.should_skip("0000000000000005")
    assert cursor.should_skip("0000000000000010")
    assert cursor.should_skip("0000000000000011") is False


def test_seed_cursor_round_trip_json() -> None:
    cursor = SeedCursor(
        repo_id="HuggingFaceTB/stack-edu",
        last_native_id="abcdef",
        rows_emitted=42,
        updated_at=datetime(2026, 6, 15, 12, 0, 0, tzinfo=UTC),
    )
    blob = cursor.to_json()
    parsed = SeedCursor.from_json(blob, repo_id="HuggingFaceTB/stack-edu")
    assert parsed.repo_id == cursor.repo_id
    assert parsed.last_native_id == cursor.last_native_id
    assert parsed.rows_emitted == 42
    assert parsed.updated_at == cursor.updated_at


def test_seed_cursor_from_json_tolerates_garbage() -> None:
    parsed = SeedCursor.from_json(b"not-json", repo_id="x/y")
    assert parsed.repo_id == "x/y"
    assert parsed.last_native_id == ""


def test_cursor_store_load_missing_returns_zero() -> None:
    store = CursorStore(_FakeS3(), bucket="state", prefix="seed-loader")
    cursor = store.load("allenai/peS2o")
    assert cursor.repo_id == "allenai/peS2o"
    assert cursor.rows_emitted == 0


def test_cursor_store_save_then_load() -> None:
    s3 = _FakeS3()
    store = CursorStore(s3, bucket="state", prefix="seed-loader")
    cursor = SeedCursor(repo_id="allenai/peS2o")
    cursor.advance("0000000000000001")
    store.save(cursor)
    assert s3.put_calls == 1
    reloaded = store.load("allenai/peS2o")
    assert reloaded.last_native_id == "0000000000000001"
    assert reloaded.rows_emitted == 1


def test_cursor_store_key_is_safe() -> None:
    store = CursorStore(_FakeS3())
    assert store.key_for("allenai/peS2o") == "seed-loader/allenai__peS2o.cursor.json"


def test_cursor_store_load_handles_partial_json() -> None:
    s3 = _FakeS3()
    s3.objects[("state", "seed-loader/x__y.cursor.json")] = b'{"repo_id":"x/y"}'
    store = CursorStore(s3, bucket="state", prefix="seed-loader")
    cursor = store.load("x/y")
    assert cursor.repo_id == "x/y"
    assert cursor.rows_emitted == 0


@pytest.mark.parametrize("native_id", ["", " ", "abc"])
def test_seed_cursor_should_skip_with_empty_state(native_id: str) -> None:
    cursor = SeedCursor(repo_id="x")
    assert cursor.should_skip(native_id) is False


def test_seed_cursor_namespaces_are_independent() -> None:
    """Cross-namespace native_ids must not skip each other.

    Regression for the v0.2.0 lex-skip bug where Wayback's per-feed ids
    (``rss-arxiv-cs-cl:20260101000000``) would silently mask later ids
    from feeds whose name sorted earlier (``bair-blog:...``).
    """
    cursor = SeedCursor(repo_id="wayback:multi-feed")
    cursor.advance("rss-arxiv-cs-cl:20260101000000")
    # bair-blog ids must remain reachable even though "bair-blog" < "rss-arxiv-cs-cl".
    assert cursor.should_skip("bair-blog:20260101000000") is False
    assert cursor.should_skip("openai-news:20250101000000") is False
    # Within the same namespace, lex order still applies.
    assert cursor.should_skip("rss-arxiv-cs-cl:20260101000000") is True
    assert cursor.should_skip("rss-arxiv-cs-cl:20260102000000") is False


def test_seed_cursor_redpajama_arxiv_vs_sha_branches() -> None:
    """sha:* and plain arxiv ids must not interfere with each other."""
    cursor = SeedCursor(repo_id="togethercomputer/RedPajama-Data-1T")
    cursor.advance("sha:0e1f0e1f0e1f0e1f")
    # A subsequent plain arXiv id ("2304.12345", namespace == "") must not
    # be skipped just because it lex-sorts below "sha:...".
    assert cursor.should_skip("2304.12345") is False
    cursor.advance("2304.12345")
    # Earlier arxiv ids in the same namespace are skipped.
    assert cursor.should_skip("2304.12000") is True
    # sha branch advances independently.
    assert cursor.should_skip("sha:0e1f0e1f0e1f0e1f") is True
    assert cursor.should_skip("sha:ffffffffffffffff") is False


def test_seed_cursor_legacy_cursor_file_migrates() -> None:
    """Cursor files written before the namespace map still load correctly."""
    legacy = b'{"repo_id":"x/y","last_native_id":"feed-a:00010","rows_emitted":7}'
    cursor = SeedCursor.from_json(legacy, repo_id="x/y")
    assert cursor.namespaces == {"feed-a": "00010"}
    assert cursor.should_skip("feed-a:00005") is True
    # A different namespace must remain reachable post-migration.
    assert cursor.should_skip("feed-b:00005") is False
