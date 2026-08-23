from __future__ import annotations

import pickle
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest
from confluent_kafka import TopicPartition

from scripts.migrate_fetcher_offsets import migrate_offsets, read_bytewax_offsets


def _recovery_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE snaps (created_at TEXT, step_id TEXT, state_key TEXT, "
        "snap_epoch INTEGER, ser_change BLOB)"
    )
    connection.executemany(
        "INSERT INTO snaps VALUES ('now', ?, ?, ?, ?)",
        [
            ("s2p-fetcher.raw_fetched", "0-raw.fetched", 4, pickle.dumps(120)),
            ("s2p-fetcher.raw_fetched", "0-raw.fetched", 5, pickle.dumps(125)),
            ("s2p-fetcher.raw_fetched", "1-raw.fetched", 5, pickle.dumps(30)),
            ("other.step", "2-raw.fetched", 6, pickle.dumps(99)),
        ],
    )
    connection.commit()
    connection.close()


def test_read_bytewax_offsets_uses_latest_epoch(tmp_path: Path) -> None:
    database = tmp_path / "bytewax" / "fetcher" / "part-0.sqlite3"
    database.parent.mkdir(parents=True)
    _recovery_database(database)

    assert read_bytewax_offsets(tmp_path, "raw.fetched") == {0: 125, 1: 30}


class _FakeConsumer:
    def __init__(self, existing: dict[int, int], watermarks: dict[int, tuple[int, int]]):
        self.existing = existing
        self.watermarks = watermarks
        self.commits: list[list[TopicPartition]] = []

    def list_topics(self, topic: str, timeout: float):
        del timeout
        return SimpleNamespace(
            topics={
                topic: SimpleNamespace(
                    error=None,
                    partitions={number: object() for number in self.watermarks},
                )
            }
        )

    def committed(self, partitions, timeout: float):
        del timeout
        return [
            TopicPartition(item.topic, item.partition, self.existing.get(item.partition, -1))
            for item in partitions
        ]

    def get_watermark_offsets(self, partition, timeout: float):
        del timeout
        return self.watermarks[partition.partition]

    def commit(self, *, offsets, asynchronous: bool):
        assert asynchronous is False
        self.commits.append(offsets)


def test_migrate_offsets_preserves_native_and_bounds_recovery() -> None:
    consumer = _FakeConsumer(existing={0: 140}, watermarks={0: (100, 200), 1: (50, 90)})

    result = migrate_offsets(
        consumer=consumer,
        topic="raw.fetched",
        recovered={0: 125, 1: 30},
    )

    assert result["migrated_partitions"] == 1
    assert consumer.commits[0][0].partition == 1
    assert consumer.commits[0][0].offset == 50
    assert result["partitions"] == {
        0: {"action": "preserved", "offset": 140},
        1: {"action": "migrated", "offset": 50},
    }


def test_migrate_offsets_refuses_uncheckpointed_retained_partition() -> None:
    consumer = _FakeConsumer(existing={}, watermarks={0: (10, 20)})

    with pytest.raises(RuntimeError, match="retained data but no"):
        migrate_offsets(consumer=consumer, topic="raw.fetched", recovered={})
