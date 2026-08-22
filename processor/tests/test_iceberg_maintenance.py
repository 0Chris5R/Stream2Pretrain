"""Tests for bounded Iceberg object metadata cleanup."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from processor.iceberg_maintenance import ObjectInfo, _cleanup_candidates, _s3_location


def test_cleanup_candidates_exclude_current_and_recent_metadata() -> None:
    now = datetime(2026, 8, 22, tzinfo=UTC)
    objects = [
        ObjectInfo("warehouse/gold/t/metadata/00001-a.metadata.json", 100, now - timedelta(days=2)),
        ObjectInfo("warehouse/gold/t/metadata/00002-b.metadata.json", 200, now - timedelta(days=2)),
        ObjectInfo("warehouse/gold/t/metadata/00003-c.metadata.json", 300, now),
        ObjectInfo("warehouse/gold/t/metadata/snap-1.avro", 400, now - timedelta(days=2)),
    ]

    candidates = _cleanup_candidates(
        objects,
        protected_keys={"warehouse/gold/t/metadata/00002-b.metadata.json"},
        older_than=now - timedelta(hours=24),
    )

    assert [item.key for item in candidates] == ["warehouse/gold/t/metadata/00001-a.metadata.json"]


def test_s3_location_rejects_non_object_storage_paths() -> None:
    assert _s3_location("s3://gold/warehouse/gold/curated") == (
        "gold",
        "warehouse/gold/curated",
    )
