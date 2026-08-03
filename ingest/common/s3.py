"""S3 (MinIO) URI helpers for the bronze layout.

The Hive-style bronze layout from RESEARCH.md section 6 is::

    s3://<bucket>/year=YYYY/month=MM/day=DD/source=<feed>/<doc_id>.<ext>

We expose builder helpers and a parser so the writer and the topic record
producer never disagree on the path scheme.
"""

from __future__ import annotations

from datetime import UTC, datetime


def _safe_doc_id(doc_id: str) -> str:
    """Strip the ``sha256:`` prefix to keep object keys URL-safe."""
    if doc_id.startswith("sha256:"):
        return doc_id.split(":", 1)[1]
    return doc_id


def bronze_object_key(
    *,
    source_feed: str,
    doc_id: str,
    fetched_at: datetime,
    extension: str = "html.gz",
) -> str:
    """Compute the object key (no bucket prefix) for a bronze artifact."""
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=UTC)
    fetched_utc = fetched_at.astimezone(UTC)
    safe_id = _safe_doc_id(doc_id)
    safe_feed = source_feed.replace("/", "_")
    return (
        f"year={fetched_utc:%Y}/month={fetched_utc:%m}/day={fetched_utc:%d}/"
        f"source={safe_feed}/{safe_id}.{extension}"
    )


def bronze_s3_uri(
    *,
    bucket: str,
    source_feed: str,
    doc_id: str,
    fetched_at: datetime,
    extension: str = "html.gz",
) -> str:
    """Compute the full ``s3://...`` URI for a bronze artifact."""
    key = bronze_object_key(
        source_feed=source_feed,
        doc_id=doc_id,
        fetched_at=fetched_at,
        extension=extension,
    )
    return f"s3://{bucket}/{key}"
