"""Tests for :mod:`processor.operators.lshbloom`."""

from __future__ import annotations

from pathlib import Path

import pytest

from processor.operators.lshbloom import LSHBloomIndex, PostgresLSHIndex
from processor.operators.minhash import MinHasher, MinHashSignature


def test_first_observation_is_not_dup() -> None:
    h = MinHasher(num_perms=64)
    sig = h.signature("alpha beta gamma delta")
    idx = LSHBloomIndex(num_bands=16, bits_per_band=1 << 14)
    res = idx.observe("sha256:" + "a" * 64, sig)
    assert res.is_near_duplicate is False
    assert res.cluster_id is not None


def test_identical_text_is_near_dup() -> None:
    h = MinHasher(num_perms=64)
    sig = h.signature(
        "the streaming pipeline curates documents into training shards "
        "deterministically without duplicates"
    )
    idx = LSHBloomIndex(num_bands=16, bits_per_band=1 << 14)
    first = idx.observe("sha256:" + "a" * 64, sig)
    second = idx.observe("sha256:" + "b" * 64, sig)
    assert first.is_near_duplicate is False
    assert second.is_near_duplicate is True
    assert second.cluster_id == first.cluster_id


def test_probe_reports_duplicate_without_inserting_a_new_anchor() -> None:
    h = MinHasher(num_perms=64)
    sig = h.signature("probe before model inference must not mutate durable dedup state")
    idx = LSHBloomIndex(num_bands=16, bits_per_band=1 << 14)

    before_insert = idx.probe("sha256:" + "a" * 64, sig)
    first = idx.observe("sha256:" + "b" * 64, sig)
    after_insert = idx.probe("sha256:" + "c" * 64, sig)

    assert before_insert == type(before_insert)(is_near_duplicate=False, cluster_id=None)
    assert first.is_near_duplicate is False
    assert after_insert.is_near_duplicate is True
    assert after_insert.cluster_id == first.cluster_id


def test_lightly_edited_repository_card_is_near_duplicate() -> None:
    h = MinHasher(num_perms=64)
    common = (
        "This technical model card documents architecture training data evaluation results "
        "runtime usage limitations checkpoints and reproducible benchmark measurements "
    )
    first_text = (common * 20) + "repository owner alpha"
    edited_text = (common * 20) + "repository owner beta"
    idx = LSHBloomIndex(
        num_bands=16,
        bits_per_band=1 << 14,
        similarity_threshold=0.80,
    )

    first = idx.observe("sha256:" + "a" * 64, h.signature(first_text))
    edited = idx.observe("sha256:" + "b" * 64, h.signature(edited_text))

    assert first.is_near_duplicate is False
    assert edited.is_near_duplicate is True
    assert edited.cluster_id == first.cluster_id


def test_single_band_collision_is_confirmed_before_rejection() -> None:
    first_values = list(range(64))
    collision_values = first_values[:4] + list(range(100, 160))
    first_sig = MinHashSignature(
        digest=b"".join(value.to_bytes(4, "little") for value in first_values),
        num_perms=64,
        backend="test",
    )
    collision_sig = MinHashSignature(
        digest=b"".join(value.to_bytes(4, "little") for value in collision_values),
        num_perms=64,
        backend="test",
    )
    idx = LSHBloomIndex(num_bands=16, bits_per_band=1 << 14)

    first = idx.observe("sha256:" + "a" * 64, first_sig)
    collision = idx.observe("sha256:" + "b" * 64, collision_sig)
    repeated_collision = idx.observe("sha256:" + "c" * 64, collision_sig)

    assert first.is_near_duplicate is False
    assert collision.is_near_duplicate is False
    assert collision.cluster_id != first.cluster_id
    assert repeated_collision.is_near_duplicate is True
    assert repeated_collision.cluster_id == collision.cluster_id


def test_replaying_anchor_document_is_idempotent() -> None:
    h = MinHasher(num_perms=64)
    sig = h.signature("a durable source replay must preserve its original curation decision")
    doc_id = "sha256:" + "a" * 64
    idx = LSHBloomIndex(num_bands=16, bits_per_band=1 << 14)

    first = idx.observe(doc_id, sig)
    replay = idx.observe(doc_id, sig)

    assert first.is_near_duplicate is False
    assert replay.is_near_duplicate is False
    assert replay.cluster_id == first.cluster_id


def test_replaying_anchor_after_index_restart_is_idempotent(tmp_path: Path) -> None:
    h = MinHasher(num_perms=64)
    sig = h.signature("the same anchor remains the same document after a worker restart")
    doc_id = "sha256:" + "c" * 64
    first_index = LSHBloomIndex(
        num_bands=16,
        bits_per_band=1 << 14,
        state_dir=tmp_path,
    )
    if first_index.backend == "memory":
        first_index.close()
        pytest.skip("durable LSH backend is unavailable in the host test environment")
    first = first_index.observe(doc_id, sig)
    first_index.close()

    replay_index = LSHBloomIndex(
        num_bands=16,
        bits_per_band=1 << 14,
        state_dir=tmp_path,
    )
    replay = replay_index.observe(doc_id, sig)
    replay_index.close()

    assert replay.is_near_duplicate is False
    assert replay.cluster_id == first.cluster_id


def test_durable_backend_writes_incremental_cluster_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored: dict[bytes, bytes] = {}

    class _Batch:
        def __init__(self) -> None:
            self.pending: dict[bytes, bytes] = {}

        def __enter__(self) -> _Batch:
            return self

        def put(self, key: bytes, value: bytes) -> None:
            self.pending[key] = value

        def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
            if exc_type is None:
                stored.update(self.pending)

    class _Database:
        def get(self, key: bytes) -> bytes | None:
            return stored.get(key)

        def put(self, key: bytes, value: bytes) -> None:
            stored[key] = value

        def write_batch(self, *, transaction: bool) -> _Batch:
            assert transaction is True
            return _Batch()

        def close(self) -> None:
            return None

    def open_state(index: LSHBloomIndex, _backend: str | None):
        database = _Database()
        index._restore_from(database)
        return database, "fake"

    monkeypatch.setattr(LSHBloomIndex, "_open_state", open_state)
    h = MinHasher(num_perms=64)
    sig = h.signature("an incremental durable lsh record avoids quadratic state rewrites")
    doc_id = "sha256:" + "d" * 64
    first_index = LSHBloomIndex(num_bands=16, bits_per_band=1 << 14, state_dir="unused")
    first = first_index.observe(doc_id, sig)
    first_index.close()

    assert b"__clusters__" not in stored
    assert b"__cluster_anchors__" not in stored
    assert sum(key.startswith(b"cluster:") for key in stored) == 16
    assert sum(key.startswith(b"anchor:") for key in stored) == 1
    assert sum(key.startswith(b"signature:") for key in stored) == 1

    replay_index = LSHBloomIndex(num_bands=16, bits_per_band=1 << 14, state_dir="unused")
    replay = replay_index.observe(doc_id, sig)
    duplicate_after_restart = replay_index.observe("sha256:" + "e" * 64, sig)
    replay_index.close()
    assert replay.is_near_duplicate is False
    assert replay.cluster_id == first.cluster_id
    assert duplicate_after_restart.is_near_duplicate is True
    assert duplicate_after_restart.cluster_id == first.cluster_id


def test_different_text_not_near_dup() -> None:
    h = MinHasher(num_perms=64)
    a = h.signature("alpha beta gamma delta epsilon zeta eta theta iota")
    b = h.signature("totally different vocabulary here apple orange banana mango")
    idx = LSHBloomIndex(num_bands=16, bits_per_band=1 << 14)
    res_a = idx.observe("sha256:" + "1" * 64, a)
    res_b = idx.observe("sha256:" + "2" * 64, b)
    assert res_a.is_near_duplicate is False
    assert res_b.is_near_duplicate is False
    assert res_a.cluster_id != res_b.cluster_id


def test_postgres_index_shares_atomic_clusters_between_replicas() -> None:
    clusters: dict[tuple[str, str], tuple[str, bytes]] = {}
    bands: dict[tuple[str, str], str] = {}
    advisory_locks: list[int] = []

    class Result:
        def __init__(self, rows: list[tuple[str, str, bytes]] | None = None) -> None:
            self._rows = rows or []

        def fetchall(self) -> list[tuple[str, str, bytes]]:
            return self._rows

    class Transaction:
        def __enter__(self) -> None:
            return None

        def __exit__(self, *_exc: object) -> None:
            return None

    class Connection:
        def transaction(self) -> Transaction:
            return Transaction()

        def execute(self, sql: str, params: tuple[object, ...] = ()) -> Result:
            if "pg_advisory_xact_lock" in sql:
                advisory_locks.append(int(params[0]))
            elif "SELECT DISTINCT c.cluster_id" in sql:
                generation = str(params[0])
                keys = list(params[1])
                cluster_ids = sorted(
                    {
                        bands[(generation, str(key))]
                        for key in keys
                        if (generation, str(key)) in bands
                    }
                )
                return Result(
                    [
                        (cluster_id, *clusters[(generation, cluster_id)])
                        for cluster_id in cluster_ids
                    ]
                )
            elif "INSERT INTO curator_lsh_clusters" in sql:
                generation, cluster_id, doc_id, signature = params
                clusters.setdefault(
                    (str(generation), str(cluster_id)),
                    (str(doc_id), bytes(signature)),
                )
            elif "INSERT INTO curator_lsh_bands" in sql:
                generation, cluster_key, cluster_id = params
                bands.setdefault((str(generation), str(cluster_key)), str(cluster_id))
            return Result()

        def close(self) -> None:
            return None

    def connect(_url: str, *, autocommit: bool) -> Connection:
        assert autocommit is True
        return Connection()

    h = MinHasher(num_perms=64)
    signature = h.signature("shared transactional near duplicate state across curator replicas")
    first_index = PostgresLSHIndex(
        "postgresql://coordination",
        generation="test-v1",
        num_bands=16,
        connect=connect,
    )
    second_index = PostgresLSHIndex(
        "postgresql://coordination",
        generation="test-v1",
        num_bands=16,
        connect=connect,
    )

    first = first_index.observe("sha256:" + "a" * 64, signature)
    duplicate = second_index.observe("sha256:" + "b" * 64, signature)
    replay = second_index.observe("sha256:" + "a" * 64, signature)

    assert first.is_near_duplicate is False
    assert duplicate.is_near_duplicate is True
    assert duplicate.cluster_id == first.cluster_id
    assert replay.is_near_duplicate is False
    assert replay.cluster_id == first.cluster_id
    assert len(advisory_locks) == 16 * 3


def test_postgres_index_replays_transaction_after_primary_failover() -> None:
    clusters: dict[tuple[str, str], tuple[str, bytes]] = {}
    bands: dict[tuple[str, str], str] = {}
    connections: list[Connection] = []

    class FailoverError(Exception):
        pass

    class Result:
        def __init__(self, rows: list[tuple[str, str, bytes]] | None = None) -> None:
            self._rows = rows or []

        def fetchall(self) -> list[tuple[str, str, bytes]]:
            return self._rows

    class Transaction:
        def __enter__(self) -> None:
            return None

        def __exit__(self, *_exc: object) -> None:
            return None

    class Connection:
        def __init__(self, *, fail_first_lock: bool) -> None:
            self.fail_first_lock = fail_first_lock
            self.closed = False

        def transaction(self) -> Transaction:
            return Transaction()

        def execute(self, sql: str, params: tuple[object, ...] = ()) -> Result:
            if "pg_advisory_xact_lock" in sql and self.fail_first_lock:
                self.fail_first_lock = False
                raise FailoverError("primary changed")
            if "SELECT DISTINCT c.cluster_id" in sql:
                generation = str(params[0])
                keys = list(params[1])
                cluster_ids = sorted(
                    {
                        bands[(generation, str(key))]
                        for key in keys
                        if (generation, str(key)) in bands
                    }
                )
                return Result(
                    [
                        (cluster_id, *clusters[(generation, cluster_id)])
                        for cluster_id in cluster_ids
                    ]
                )
            if "INSERT INTO curator_lsh_clusters" in sql:
                generation, cluster_id, doc_id, signature = params
                clusters.setdefault(
                    (str(generation), str(cluster_id)),
                    (str(doc_id), bytes(signature)),
                )
            if "INSERT INTO curator_lsh_bands" in sql:
                generation, cluster_key, cluster_id = params
                bands.setdefault((str(generation), str(cluster_key)), str(cluster_id))
            return Result()

        def close(self) -> None:
            self.closed = True

    def connect(_url: str, *, autocommit: bool) -> Connection:
        assert autocommit is True
        connection = Connection(fail_first_lock=not connections)
        connections.append(connection)
        return connection

    signature = MinHasher(num_perms=64).signature(
        "transaction replay preserves the global duplicate decision"
    )
    index = PostgresLSHIndex(
        "postgresql://coordination",
        generation="test-v1",
        num_bands=16,
        connect=connect,
        connection_errors=(FailoverError,),
    )

    first = index.observe("sha256:" + "a" * 64, signature)
    duplicate = index.probe("sha256:" + "b" * 64, signature)

    assert first.is_near_duplicate is False
    assert duplicate.is_near_duplicate is True
    assert duplicate.cluster_id == first.cluster_id
    assert len(connections) == 2
    assert connections[0].closed
