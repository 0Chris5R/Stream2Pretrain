"""Tests for :mod:`processor.iceberg_writer`."""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from typing import Any

import pytest

from processor.iceberg_writer import (
    IcebergWriter,
    LicenseAdmissionWriter,
    _create_table_or_load,
    _ensure_source_quality_column,
    _kafka_partition_key,
    gold_identifier,
)
from schemas.gold import GoldRecord
from schemas.license_admission import LicenseAdmissionDecision


def _gold() -> GoldRecord:
    return GoldRecord(
        doc_id="sha256:" + "a" * 64,
        text="A compact training-data document.",
        lang="en",
        tokens=6,
        quality_score=4.0,
        source_quality_score=4.0,
        license="Apache-2.0",
        license_source="unknown",
        risk_tier=1,
        valid_from=datetime(2026, 6, 15, tzinfo=UTC),
        scoring_version="v-test",
        classifier_revision="classifier-test",
        policy_revision="git:test",
        trace_id="0" * 32,
        source_format="web",
        extraction_pipeline="hf-model-card-markdown-v1",
        spdx_license="Apache-2.0",
        spdx_license_source="source_terms",
    )


def _admission() -> LicenseAdmissionDecision:
    return LicenseAdmissionDecision(
        decision_id="sha256:" + "b" * 64,
        doc_id="sha256:" + "a" * 64,
        source_feed="arxiv-cs-ai",
        source_url="https://arxiv.org/abs/2608.00001",
        observed_at=datetime(2026, 8, 19, tzinfo=UTC),
        status="admitted",
        license_id="CC-BY-4.0",
        license_source="rss_entry",
        reason="CC-BY-4.0 is on the training allowlist",
        trace_id="0" * 32,
    )


def test_to_arrow_includes_v2_provenance_columns() -> None:
    writer = IcebergWriter(
        catalog=object(),
    )

    table = writer._to_arrow([_gold()])

    assert table.column("source_format").to_pylist() == ["web"]
    assert table.column("extraction_pipeline").to_pylist() == ["hf-model-card-markdown-v1"]
    assert table.column("spdx_license").to_pylist() == ["Apache-2.0"]
    assert table.column("spdx_license_source").to_pylist() == ["source_terms"]


class _Snapshot:
    snapshot_id = 11


class _MemoryTable:
    def __init__(self) -> None:
        self.rows = 0
        self.tables: list[object] = []
        self.properties: dict[str, str] = {}

    def append(self, table: object) -> None:
        self.rows += int(table.num_rows)
        self.tables.append(table)

    def current_snapshot(self) -> _Snapshot:
        return _Snapshot()

    def transaction(self) -> object:
        table = self

        class _Transaction:
            def __enter__(self) -> _Transaction:
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def set_properties(self, **properties: str) -> None:
                table.properties.update(properties)

        return _Transaction()

    def scan(self, *, selected_fields: tuple[str, ...]) -> object:
        import pyarrow as pa

        selected = [table.select(selected_fields) for table in self.tables]
        arrow = (
            pa.concat_tables(selected)
            if selected
            else pa.table({name: pa.array([], type=pa.string()) for name in selected_fields})
        )

        class _Scan:
            def to_arrow(self) -> object:
                return arrow

        return _Scan()


class _MemoryWriter(IcebergWriter):
    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.decisions = _MemoryTable()
        self.gold = _MemoryTable()

    def _ensure_decisions_table(self) -> _MemoryTable:
        return self.decisions

    def _ensure_table(self) -> _MemoryTable:
        return self.gold


class _AdmissionCatalog:
    def __init__(self) -> None:
        self.table = _MemoryTable()

    def load_table(self, _identifier: object) -> _MemoryTable:
        return self.table


class _MemoryLicenseAdmissionWriter(LicenseAdmissionWriter):
    def _ensure_table(self) -> _MemoryTable:
        return self._catalog.table  # type: ignore[attr-defined]


def test_license_admission_writer_batches_and_deduplicates_decisions() -> None:
    catalog = _AdmissionCatalog()
    writer = _MemoryLicenseAdmissionWriter(catalog)  # type: ignore[arg-type]
    decision = _admission()

    second = decision.model_copy(
        update={
            "decision_id": "sha256:" + "c" * 64,
            "doc_id": "sha256:" + "d" * 64,
        }
    )

    assert writer.add_batch([decision, second, decision]) == 2
    assert writer.add_batch([decision, second]) == 0
    assert catalog.table.rows == 2
    assert len(catalog.table.tables) == 1


def test_license_admission_writer_reloads_after_unknown_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CommitStateUnknownException(Exception):  # noqa: N818 - PyIceberg's public name
        pass

    class _UnknownCommitTable(_MemoryTable):
        def __init__(self) -> None:
            super().__init__()
            self.append_calls = 0

        def append(self, table: object) -> None:
            self.append_calls += 1
            super().append(table)
            raise CommitStateUnknownException("catalog acknowledgement was lost")

    catalog = _AdmissionCatalog()
    catalog.table = _UnknownCommitTable()
    writer = _MemoryLicenseAdmissionWriter(catalog)  # type: ignore[arg-type]
    monkeypatch.setattr("processor.iceberg_writer.time.sleep", lambda _seconds: None)

    assert writer.add_batch([_admission()]) == 0
    assert catalog.table.rows == 1
    assert catalog.table.append_calls == 1


def test_kafka_partition_key_distributes_by_source_partition() -> None:
    message = type("Message", (), {"topic": "curation.decisions", "partition": 3})()

    assert _kafka_partition_key(message) == "curation.decisions:3"


def test_writer_persists_rejected_decision_without_adding_it_to_gold() -> None:
    writer = _MemoryWriter(
        catalog=object(),
    )
    rejected = _gold().model_copy(update={"risk_tier": 2, "reject_reasons": ["license_excluded"]})

    assert writer.add(rejected) is None
    stats = writer.flush()
    assert stats.rows_committed == 0
    assert stats.decisions_committed == 1
    assert writer.decisions.rows == 1
    assert writer.gold.rows == 0


def test_writer_ignores_replayed_decision_recipe() -> None:
    writer = _MemoryWriter(
        catalog=object(),
    )
    record = _gold().model_copy(update={"route": "broad_pretraining"})

    writer.add(record)
    first = writer.flush()
    writer.add(record)
    replay = writer.flush()

    assert first.decisions_committed == 1
    assert replay.decisions_committed == 0
    assert replay.rows_committed == 0
    assert writer.decisions.rows == 1
    assert writer.gold.rows == 1


def test_writer_ignores_replay_after_restart_by_scanning_iceberg_keys() -> None:
    first_writer = _MemoryWriter(
        catalog=object(),
    )
    record = _gold().model_copy(update={"route": "broad_pretraining"})
    first_writer.add(record)
    first_writer.flush()

    restarted_writer = _MemoryWriter(
        catalog=object(),
    )
    restarted_writer.decisions = first_writer.decisions
    restarted_writer.gold = first_writer.gold
    restarted_writer.add(record)
    replay = restarted_writer.flush()

    assert replay.decisions_committed == 0
    assert replay.rows_committed == 0
    assert restarted_writer.decisions.rows == 1
    assert restarted_writer.gold.rows == 1


def test_writer_reloads_unknown_decision_commit_before_writing_gold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CommitStateUnknownException(Exception):  # noqa: N818 - PyIceberg's public name
        pass

    class _UnknownCommitTable(_MemoryTable):
        def __init__(self) -> None:
            super().__init__()
            self.append_calls = 0

        def append(self, table: object) -> None:
            self.append_calls += 1
            super().append(table)
            if self.append_calls == 1:
                raise CommitStateUnknownException("catalog acknowledgement was lost")

    writer = _MemoryWriter(catalog=object())
    writer.decisions = _UnknownCommitTable()
    record = _gold().model_copy(update={"route": "broad_pretraining"})
    monkeypatch.setattr("processor.iceberg_writer.time.sleep", lambda _seconds: None)

    writer.add(record)
    stats = writer.flush()

    assert stats.decisions_committed == 0
    assert stats.rows_committed == 1
    assert writer.decisions.rows == 1
    assert writer.decisions.append_calls == 1
    assert writer.gold.rows == 1


def test_concurrent_writers_reload_after_optimistic_commit_conflict() -> None:
    class CommitFailedException(Exception):  # noqa: N818 - PyIceberg's public name
        pass

    class _ConcurrentCatalog:
        def __init__(self) -> None:
            self.lock = threading.Lock()
            self.initial_scans = threading.Barrier(2, timeout=2)
            self.tables: list[Any] = []
            self.generation = 0
            self.loads = 0
            self.conflicts = 0

        def load_table(self) -> _ConcurrentTable:
            with self.lock:
                self.loads += 1
                generation = self.generation
            return _ConcurrentTable(self, generation)

    class _ConcurrentTable:
        def __init__(self, catalog: _ConcurrentCatalog, generation: int) -> None:
            self.catalog = catalog
            self.generation = generation
            self.properties: dict[str, str] = {}

        def transaction(self) -> object:
            table = self

            class _Transaction:
                def __enter__(self) -> _Transaction:
                    return self

                def __exit__(self, *_args: object) -> None:
                    return None

                def set_properties(self, **properties: str) -> None:
                    table.properties.update(properties)

            return _Transaction()

        def refresh(self) -> None:
            return None

        def scan(self, *, selected_fields: tuple[str, ...]) -> object:
            import pyarrow as pa

            with self.catalog.lock:
                generation = self.catalog.generation
                tables = list(self.catalog.tables)
            if generation == 0:
                self.catalog.initial_scans.wait()
            selected = [table.select(selected_fields) for table in tables]
            arrow = (
                pa.concat_tables(selected)
                if selected
                else pa.table({name: pa.array([], type=pa.string()) for name in selected_fields})
            )

            class _Scan:
                def to_arrow(self) -> object:
                    return arrow

            return _Scan()

        def append(self, table: object) -> None:
            with self.catalog.lock:
                if self.generation != self.catalog.generation:
                    self.catalog.conflicts += 1
                    raise CommitFailedException("table metadata advanced")
                self.catalog.tables.append(table)
                self.catalog.generation += 1

        def current_snapshot(self) -> _Snapshot:
            return _Snapshot()

    class _ConcurrentWriter(IcebergWriter):
        def __init__(self, catalog: _ConcurrentCatalog) -> None:
            super().__init__(catalog=catalog)
            self.concurrent_catalog = catalog

        def _ensure_decisions_table(self) -> _ConcurrentTable:
            return self.concurrent_catalog.load_table()

        def _ensure_table(self) -> object:
            raise AssertionError("rejected records must not reach the clean Gold table")

    catalog = _ConcurrentCatalog()
    record = _gold().model_copy(update={"risk_tier": 2, "reject_reasons": ["license_excluded"]})
    writers = [_ConcurrentWriter(catalog), _ConcurrentWriter(catalog)]
    errors: list[BaseException] = []
    stats = []

    def flush(writer: _ConcurrentWriter) -> None:
        try:
            writer.add(record)
            stats.append(writer.flush())
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=flush, args=(writer,)) for writer in writers]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert sorted(item.decisions_committed for item in stats) == [0, 1]
    total_rows = 0
    for table in catalog.tables:
        total_rows += int(table.num_rows)
    assert total_rows == 1
    assert catalog.conflicts == 1
    assert catalog.loads >= 3


def test_concurrent_table_creation_loads_the_winning_table() -> None:
    class TableAlreadyExistsError(Exception):
        pass

    class _Catalog:
        def __init__(self) -> None:
            self.barrier = threading.Barrier(2, timeout=2)
            self.lock = threading.Lock()
            self.table: object | None = None
            self.create_calls = 0
            self.load_calls = 0

        def create_table(self, **_kwargs: object) -> object:
            self.barrier.wait()
            with self.lock:
                self.create_calls += 1
                if self.table is not None:
                    raise TableAlreadyExistsError("table already exists")
                self.table = object()
                return self.table

        def load_table(self, _identifier: object) -> object:
            with self.lock:
                self.load_calls += 1
                assert self.table is not None
                return self.table

    catalog = _Catalog()
    results: list[object] = []
    errors: list[BaseException] = []

    def create() -> None:
        try:
            results.append(
                _create_table_or_load(
                    catalog,  # type: ignore[arg-type]
                    ("gold", "curated"),
                    schema=object(),
                )
            )
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=create) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert len(results) == 2
    assert results[0] is results[1]
    assert catalog.create_calls == 2
    assert catalog.load_calls == 1


def test_gold_identifier_follows_helm_namespace_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("S2P_ICEBERG_NAMESPACE", raising=False)
    monkeypatch.delenv("S2P_ICEBERG_GOLD_TABLE", raising=False)
    monkeypatch.setenv("ICEBERG_NAMESPACE", "gold")

    assert gold_identifier() == ("gold", "curated")

    monkeypatch.setenv("S2P_ICEBERG_NAMESPACE", "research")
    monkeypatch.setenv("S2P_ICEBERG_GOLD_TABLE", "trainable")

    assert gold_identifier() == ("research", "trainable")


def test_source_quality_schema_migration_renames_in_place() -> None:
    class _Schema:
        def find_field(self, name: str) -> object:
            if name == "edu_score":
                return object()
            raise ValueError(name)

    class _Update:
        def __init__(self) -> None:
            self.renames: list[tuple[str, str]] = []
            self.committed = False

        def rename_column(self, old: str, new: str) -> None:
            self.renames.append((old, new))

        def commit(self) -> None:
            self.committed = True

    update = _Update()
    table = type(
        "HistoryTable",
        (),
        {
            "schema": lambda _self: _Schema(),
            "update_schema": lambda _self: update,
        },
    )()

    _ensure_source_quality_column(table)  # type: ignore[arg-type]

    assert update.renames == [("edu_score", "source_quality_score")]
    assert update.committed


def test_source_quality_schema_migration_is_noop_after_rename() -> None:
    class _Schema:
        def find_field(self, name: str) -> object:
            if name == "source_quality_score":
                return object()
            raise ValueError(name)

    class _Table:
        def schema(self) -> _Schema:
            return _Schema()

        def update_schema(self) -> object:
            raise AssertionError("an already migrated table must not be updated")

    _ensure_source_quality_column(_Table())  # type: ignore[arg-type]
