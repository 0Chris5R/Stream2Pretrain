"""Unit tests for the bronze S3 layout helpers."""

from __future__ import annotations

from datetime import UTC, datetime

from ingest.common.s3 import bronze_object_key, bronze_s3_uri


def test_object_key_layout() -> None:
    fetched = datetime(2026, 6, 15, 8, 30, tzinfo=UTC)
    key = bronze_object_key(
        source_feed="rss-arxiv-cs-cl",
        doc_id="sha256:abc",
        fetched_at=fetched,
        extension="html.gz",
    )
    assert key == "year=2026/month=06/day=15/source=rss-arxiv-cs-cl/abc.html.gz"


def test_s3_uri_includes_bucket() -> None:
    fetched = datetime(2026, 1, 2, tzinfo=UTC)
    uri = bronze_s3_uri(
        bucket="bronze",
        source_feed="manual",
        doc_id="sha256:xyz",
        fetched_at=fetched,
    )
    assert uri == "s3://bronze/year=2026/month=01/day=02/source=manual/xyz.html.gz"


def test_naive_datetime_is_treated_as_utc() -> None:
    fetched = datetime(2026, 1, 2)
    key = bronze_object_key(source_feed="x", doc_id="d", fetched_at=fetched)
    assert key.startswith("year=2026/month=01/day=02")


def test_source_feed_with_slash_is_safe() -> None:
    fetched = datetime(2026, 1, 2, tzinfo=UTC)
    key = bronze_object_key(
        source_feed="orgs/feed", doc_id="sha256:y", fetched_at=fetched
    )
    assert "source=orgs_feed" in key
