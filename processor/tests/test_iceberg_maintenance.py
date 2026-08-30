"""Tests for bounded Iceberg object metadata cleanup."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from processor import iceberg_maintenance
from processor.iceberg_maintenance import (
    ObjectInfo,
    _cleanup_candidates,
    _delete_objects,
    _maintain_table,
    _maintenance_properties,
    _s3_location,
)


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


def test_hot_commits_never_own_metadata_deletion() -> None:
    properties = _maintenance_properties()

    assert properties["write.metadata.delete-after-commit.enabled"] == "false"
    assert properties["write.metadata.previous-versions-max"] == "20"
    assert properties["history.expire.max-snapshot-age-ms"] == str(168 * 60 * 60 * 1000)
    assert properties["history.expire.min-snapshots-to-keep"] == "10"


def test_metadata_delete_retries_only_retryable_keys(monkeypatch) -> None:
    class _RetryingS3:
        def __init__(self) -> None:
            self.requests: list[list[str]] = []

        def delete_objects(self, **kwargs: object) -> dict[str, object]:
            delete = kwargs["Delete"]
            assert isinstance(delete, dict)
            keys = [str(item["Key"]) for item in delete["Objects"]]  # type: ignore[index]
            self.requests.append(keys)
            if len(self.requests) == 1:
                return {
                    "Errors": [
                        {"Key": "old-a", "Code": "SlowDown"},
                        {"Key": "already-gone", "Code": "NoSuchKey"},
                    ]
                }
            return {}

    s3 = _RetryingS3()
    monkeypatch.setattr(iceberg_maintenance.time, "sleep", lambda _seconds: None)
    now = datetime(2026, 8, 22, tzinfo=UTC)

    _delete_objects(
        s3,
        bucket="gold",
        objects=[
            ObjectInfo("old-a", 1, now),
            ObjectInfo("already-gone", 1, now),
        ],
    )

    assert s3.requests == [["already-gone", "old-a"], ["old-a"]]


def test_register_only_never_runs_snapshot_or_metadata_cleanup(monkeypatch) -> None:
    table = SimpleNamespace(metadata_location="s3://gold/warehouse/gold/curated/metadata/v1.json")
    monkeypatch.setattr(
        iceberg_maintenance,
        "_load_or_register_table",
        lambda *args, **kwargs: (table, None),
    )

    result = _maintain_table(
        object(),
        object(),
        namespace="gold",
        table_name="curated",
        bucket="gold",
        apply=True,
        register_missing=True,
        register_only=True,
        snapshot_cutoff=datetime(2026, 8, 22, tzinfo=UTC),
        metadata_cutoff=datetime(2026, 8, 22, tzinfo=UTC),
    )

    assert result == {
        "table": "gold.curated",
        "status": "reconciled",
        "current_metadata": table.metadata_location,
    }
