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
    _protected_table_objects,
    _s3_location,
    _snapshot_ids_to_expire,
)


def test_retained_snapshot_graph_protects_manifests_and_data() -> None:
    manifest = SimpleNamespace(
        manifest_path="s3://gold/warehouse/gold/t/metadata/m1.avro",
        fetch_manifest_entry=lambda _io, discard_deleted: [
            SimpleNamespace(
                data_file=SimpleNamespace(file_path="s3://gold/warehouse/gold/t/data/live.parquet")
            )
        ],
    )
    snapshot = SimpleNamespace(
        manifest_list="s3://gold/warehouse/gold/t/metadata/snap.avro",
        manifests=lambda _io: [manifest],
    )
    table = SimpleNamespace(
        io=object(),
        metadata_location="s3://gold/warehouse/gold/t/metadata/current.metadata.json",
        metadata=SimpleNamespace(
            metadata_log=[],
            snapshots=[snapshot],
            statistics=[],
            partition_statistics=[],
        ),
    )

    bucket, protected = _protected_table_objects(table)

    assert bucket == "gold"
    assert protected == {
        "warehouse/gold/t/metadata/current.metadata.json",
        "warehouse/gold/t/metadata/snap.avro",
        "warehouse/gold/t/metadata/m1.avro",
        "warehouse/gold/t/data/live.parquet",
    }


def test_retained_snapshot_graph_reads_shared_manifest_once() -> None:
    fetches = 0

    def fetch_manifest_entry(_io: object, *, discard_deleted: bool) -> list[object]:
        nonlocal fetches
        assert discard_deleted is True
        fetches += 1
        return []

    manifest = SimpleNamespace(
        manifest_path="s3://gold/warehouse/gold/t/metadata/shared.avro",
        fetch_manifest_entry=fetch_manifest_entry,
    )
    snapshots = [
        SimpleNamespace(
            manifest_list=f"s3://gold/warehouse/gold/t/metadata/snap-{index}.avro",
            manifests=lambda _io: [manifest],
        )
        for index in range(2)
    ]
    table = SimpleNamespace(
        io=object(),
        metadata_location="s3://gold/warehouse/gold/t/metadata/current.metadata.json",
        metadata=SimpleNamespace(
            metadata_log=[],
            snapshots=snapshots,
            statistics=[],
            partition_statistics=[],
        ),
    )

    _protected_table_objects(table)

    assert fetches == 1


def test_cleanup_candidates_exclude_current_and_recent_metadata() -> None:
    now = datetime(2026, 8, 22, tzinfo=UTC)
    objects = [
        ObjectInfo("warehouse/gold/t/metadata/00001-a.metadata.json", 100, now - timedelta(days=2)),
        ObjectInfo("warehouse/gold/t/metadata/00002-b.metadata.json", 200, now - timedelta(days=2)),
        ObjectInfo("warehouse/gold/t/metadata/00003-c.metadata.json", 300, now),
        ObjectInfo("warehouse/gold/t/metadata/snap-1.avro", 400, now - timedelta(days=2)),
        ObjectInfo("warehouse/gold/t/metadata/orphan.avro", 500, now - timedelta(days=2)),
    ]

    candidates = _cleanup_candidates(
        objects,
        protected_keys={
            "warehouse/gold/t/metadata/00002-b.metadata.json",
            "warehouse/gold/t/metadata/snap-1.avro",
        },
        older_than=now - timedelta(hours=24),
    )

    assert [item.key for item in candidates] == [
        "warehouse/gold/t/metadata/00001-a.metadata.json",
        "warehouse/gold/t/metadata/orphan.avro",
    ]


def test_s3_location_rejects_non_object_storage_paths() -> None:
    assert _s3_location("s3://gold/warehouse/gold/curated") == (
        "gold",
        "warehouse/gold/curated",
    )


def test_hot_commits_bound_previous_metadata_history() -> None:
    properties = _maintenance_properties()

    assert properties["write.metadata.delete-after-commit.enabled"] == "false"
    assert properties["write.metadata.previous-versions-max"] == "20"
    assert properties["history.expire.max-snapshot-age-ms"] == str(24 * 60 * 60 * 1000)
    assert properties["history.expire.min-snapshots-to-keep"] == "10"


def test_snapshot_expiration_keeps_recent_floor_and_reference_heads() -> None:
    snapshots = [
        SimpleNamespace(snapshot_id=value, timestamp_ms=value * 1_000) for value in range(1, 13)
    ]
    table = SimpleNamespace(
        metadata=SimpleNamespace(
            snapshots=snapshots,
            refs={"audit": SimpleNamespace(snapshot_id=1)},
        )
    )

    expired = _snapshot_ids_to_expire(
        table,
        older_than=datetime.fromtimestamp(10, tz=UTC),
        minimum_to_keep=3,
    )

    assert expired == [9, 8, 7, 6, 5, 4, 3, 2]


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


def test_unchanged_catalog_pointer_avoids_second_manifest_walk(monkeypatch) -> None:
    now = datetime(2026, 8, 22, tzinfo=UTC)
    table = SimpleNamespace(
        metadata_location="s3://gold/warehouse/gold/curated/metadata/v1.metadata.json",
        metadata=SimpleNamespace(snapshots=[]),
    )
    catalog = SimpleNamespace(load_table=lambda _identifier: table)
    walks = 0

    def protected(_table: object) -> tuple[str, set[str]]:
        nonlocal walks
        walks += 1
        return "gold", {"warehouse/gold/curated/metadata/v1.metadata.json"}

    monkeypatch.setattr(
        iceberg_maintenance,
        "_load_or_register_table",
        lambda *args, **kwargs: (table, None),
    )
    monkeypatch.setattr(iceberg_maintenance, "_ensure_maintenance_properties", lambda _table: None)
    monkeypatch.setattr(iceberg_maintenance, "_protected_table_objects", protected)
    monkeypatch.setattr(iceberg_maintenance, "_iter_objects", lambda *args, **kwargs: [])
    monkeypatch.setattr(iceberg_maintenance, "_delete_objects", lambda *args, **kwargs: None)

    result = _maintain_table(
        catalog,
        object(),
        namespace="gold",
        table_name="curated",
        bucket="gold",
        apply=True,
        register_missing=False,
        snapshot_cutoff=now - timedelta(hours=24),
        metadata_cutoff=now - timedelta(hours=24),
    )

    assert result["status"] == "applied"
    assert walks == 1
