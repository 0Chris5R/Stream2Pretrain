"""Migrate a legacy Bytewax source checkpoint into Kafka group offsets.

The migration is intentionally monotonic and idempotent: the furthest valid
broker or Bytewax offset wins. This also handles Bytewax Kafka groups that
expose a stale broker offset while their authoritative progress lives in the
pod-local recovery database.
"""

from __future__ import annotations

import json
import os
import pickle
import sqlite3
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, TopicPartition


def read_bytewax_offsets(
    state_dir: Path,
    topic: str,
    *,
    step_id: str = "s2p-fetcher.raw_fetched",
) -> dict[int, int]:
    """Read the newest recoverable source offset for each topic partition."""
    offsets: dict[int, tuple[int, int]] = {}
    for database in sorted(state_dir.rglob("part-*.sqlite3")):
        connection = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True, timeout=10)
        try:
            tables = {
                str(row[0])
                for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            if "snaps" not in tables:
                continue
            rows = connection.execute(
                "SELECT state_key, snap_epoch, ser_change FROM snaps "
                "WHERE step_id = ? ORDER BY snap_epoch DESC",
                (step_id,),
            )
            for state_key, epoch, serialized in rows:
                suffix = f"-{topic}"
                key = str(state_key)
                if not key.endswith(suffix):
                    continue
                partition_text = key[: -len(suffix)]
                try:
                    partition = int(partition_text)
                    offset = pickle.loads(bytes(serialized))
                except (TypeError, ValueError, pickle.UnpicklingError):
                    continue
                if not isinstance(offset, int) or offset < 0:
                    continue
                previous = offsets.get(partition)
                if previous is None or int(epoch) > previous[0]:
                    offsets[partition] = (int(epoch), offset)
        finally:
            connection.close()
    return {partition: value[1] for partition, value in offsets.items()}


def migrate_offsets(
    *,
    consumer: Any,
    topic: str,
    recovered: dict[int, int],
) -> dict[str, object]:
    """Commit missing group offsets without overwriting native progress."""
    metadata = consumer.list_topics(topic, timeout=10.0)
    topic_metadata = metadata.topics.get(topic)
    if topic_metadata is None or topic_metadata.error is not None:
        raise RuntimeError(f"cannot inspect fetcher topic {topic}: {topic_metadata}")
    partitions = [TopicPartition(topic, number) for number in sorted(topic_metadata.partitions)]
    existing = consumer.committed(partitions, timeout=10.0)
    existing_by_partition = {item.partition: item.offset for item in existing}
    commits: list[TopicPartition] = []
    decisions: dict[int, dict[str, int | str]] = {}
    for partition in partitions:
        candidate = recovered.get(partition.partition)
        low, high = consumer.get_watermark_offsets(partition, timeout=10.0)
        current = int(existing_by_partition.get(partition.partition, -1))
        if current < 0 and candidate is None:
            if high == low:
                decisions[partition.partition] = {"action": "empty", "offset": high}
                continue
            raise RuntimeError(
                f"partition {topic}[{partition.partition}] has retained data but no "
                "broker or Bytewax checkpoint offset"
            )
        furthest = max(current, candidate if candidate is not None else -1)
        bounded = min(max(furthest, low), high)
        if current == bounded:
            decisions[partition.partition] = {"action": "preserved", "offset": bounded}
        else:
            commits.append(TopicPartition(topic, partition.partition, bounded))
            decisions[partition.partition] = {"action": "migrated", "offset": bounded}
    if commits:
        consumer.commit(offsets=commits, asynchronous=False)
    return {
        "topic": topic,
        "migrated_partitions": len(commits),
        "partitions": decisions,
    }


def main() -> None:
    brokers = os.environ.get("REDPANDA_BROKERS", "").strip()
    if not brokers:
        raise RuntimeError("REDPANDA_BROKERS is required")
    topic = os.environ.get("S2P_RAW_TOPIC", "raw.fetched")
    group = os.environ.get("S2P_CONSUMER_GROUP", "s2p-fetcher")
    step_id = os.environ.get("S2P_BYTEWAX_STEP_ID", "s2p-fetcher.raw_fetched")
    state_dir = Path(os.environ.get("S2P_STATE_DIR", "/var/lib/s2p"))
    recovered = read_bytewax_offsets(state_dir, topic, step_id=step_id)
    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        result = migrate_offsets(consumer=consumer, topic=topic, recovered=recovered)
    finally:
        consumer.close()
    result["group"] = group
    result["step_id"] = step_id
    result["recovery_offsets_found"] = len(recovered)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
