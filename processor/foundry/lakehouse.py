"""Buffered Iceberg sinks for append-only foundry events and artifacts."""

from __future__ import annotations

import os
from contextlib import suppress
from typing import Any

from processor.foundry.util import canonical_json
from processor.iceberg_catalog import load_runtime_catalog
from schemas.foundry import FoundryArtifactRecord, FoundryEvent


class FoundryLakehouseSink:
    def __init__(self, *, batch_size: int = 50) -> None:
        self._catalog = load_runtime_catalog()
        self._batch_size = batch_size
        self._events: list[FoundryEvent] = []
        self._artifacts: list[FoundryArtifactRecord] = []
        self._buffered_event_ids: set[str] = set()
        self._buffered_artifact_ids: set[str] = set()
        self._known_event_ids: set[str] | None = None
        self._known_artifact_ids: set[str] | None = None

    def add_event(self, event: FoundryEvent) -> None:
        if event.event_id in self._buffered_event_ids:
            return
        self._events.append(event)
        self._buffered_event_ids.add(event.event_id)
        if len(self._events) >= self._batch_size:
            self.flush_events()

    def add_artifact(self, artifact: FoundryArtifactRecord) -> None:
        if artifact.artifact_id in self._buffered_artifact_ids:
            return
        self._artifacts.append(artifact)
        self._buffered_artifact_ids.add(artifact.artifact_id)
        if len(self._artifacts) >= self._batch_size:
            self.flush_artifacts()

    def flush(self) -> None:
        self.flush_events()
        self.flush_artifacts()

    def flush_events(self) -> None:
        if not self._events:
            return
        table = self._ensure_events_table()
        self._known_event_ids = self._known_event_ids or _load_ids(table, "event_id")
        pending = [value for value in self._events if value.event_id not in self._known_event_ids]
        if pending:
            table.append(_events_arrow(pending))
            self._known_event_ids.update(value.event_id for value in pending)
        self._events.clear()
        self._buffered_event_ids.clear()

    def flush_artifacts(self) -> None:
        if not self._artifacts:
            return
        table = self._ensure_artifacts_table()
        self._known_artifact_ids = self._known_artifact_ids or _load_ids(table, "artifact_id")
        pending = [
            value for value in self._artifacts if value.artifact_id not in self._known_artifact_ids
        ]
        if pending:
            table.append(_artifacts_arrow(pending))
            self._known_artifact_ids.update(value.artifact_id for value in pending)
        self._artifacts.clear()
        self._buffered_artifact_ids.clear()

    def _ensure_events_table(self) -> Any:
        return self._ensure(
            os.environ.get("S2P_FOUNDRY_EVENTS_TABLE", "foundry_events"),
            _events_schema(),
        )

    def _ensure_artifacts_table(self) -> Any:
        table = self._ensure(
            os.environ.get("S2P_FOUNDRY_ARTIFACTS_TABLE", "posttrain_artifacts"),
            _artifacts_schema(),
        )
        names = _schema_column_names(table.schema())
        if {"pool", "dataset_split"} - names:
            from pyiceberg.types import StringType

            with table.update_schema() as update:
                if "pool" not in names:
                    update.add_column("pool", StringType(), required=False)
                if "dataset_split" not in names:
                    update.add_column("dataset_split", StringType(), required=False)
        return table

    def _ensure(self, table_name: str, schema: Any) -> Any:
        namespace = os.environ.get("S2P_ICEBERG_NAMESPACE") or os.environ.get(
            "ICEBERG_NAMESPACE", "gold"
        )
        identifier = (namespace, table_name)
        try:
            return self._catalog.load_table(identifier)
        except Exception:
            with suppress(Exception):
                self._catalog.create_namespace((namespace,))
            return self._catalog.create_table(
                identifier=identifier,
                schema=schema,
                properties={"format-version": "2", "write.format.default": "parquet"},
            )


def _schema_column_names(schema: Any) -> set[str]:
    """Return names across current and older PyIceberg Schema releases."""
    column_names = getattr(schema, "column_names", None)
    if column_names is not None:
        return {str(value) for value in column_names}
    return {
        str(field.name)
        for field in getattr(schema, "fields", ())
        if getattr(field, "name", None) is not None
    }


def _events_schema() -> Any:
    from pyiceberg.schema import Schema
    from pyiceberg.types import IntegerType, NestedField, StringType, TimestamptzType

    return Schema(
        NestedField(1, "event_id", StringType(), required=True),
        NestedField(2, "job_id", StringType(), required=True),
        NestedField(3, "paper_id", StringType(), required=True),
        NestedField(4, "sequence", IntegerType(), required=True),
        NestedField(5, "state", StringType(), required=True),
        NestedField(6, "occurred_at", TimestamptzType(), required=True),
        NestedField(7, "provider_trace_id", StringType(), required=False),
        NestedField(8, "artifact_hash", StringType(), required=False),
        NestedField(9, "reason", StringType(), required=False),
        NestedField(10, "event_json", StringType(), required=True),
    )


def _artifacts_schema() -> Any:
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType, TimestamptzType

    return Schema(
        NestedField(1, "artifact_id", StringType(), required=True),
        NestedField(2, "job_id", StringType(), required=True),
        NestedField(3, "paper_id", StringType(), required=True),
        NestedField(4, "task_id", StringType(), required=True),
        NestedField(5, "family", StringType(), required=True),
        NestedField(6, "kind", StringType(), required=True),
        NestedField(7, "status", StringType(), required=True),
        NestedField(8, "quality_label", StringType(), required=True),
        NestedField(9, "package_uri", StringType(), required=False),
        NestedField(10, "package_hash", StringType(), required=True),
        NestedField(11, "environment_hash", StringType(), required=True),
        NestedField(12, "created_at", TimestamptzType(), required=True),
        NestedField(13, "artifact_json", StringType(), required=True),
        NestedField(14, "pool", StringType(), required=True),
        NestedField(15, "dataset_split", StringType(), required=True),
    )


def _events_arrow(values: list[FoundryEvent]) -> Any:
    import pyarrow as pa

    return pa.Table.from_pylist(
        [
            {
                "event_id": value.event_id,
                "job_id": value.job_id,
                "paper_id": value.paper_id,
                "sequence": value.sequence,
                "state": value.state,
                "occurred_at": value.occurred_at,
                "provider_trace_id": value.provider_trace_id,
                "artifact_hash": value.artifact_hash,
                "reason": value.reason,
                "event_json": canonical_json(value).decode("utf-8"),
            }
            for value in values
        ],
        schema=pa.schema(
            [
                pa.field("event_id", pa.string(), nullable=False),
                pa.field("job_id", pa.string(), nullable=False),
                pa.field("paper_id", pa.string(), nullable=False),
                pa.field("sequence", pa.int32(), nullable=False),
                pa.field("state", pa.string(), nullable=False),
                pa.field("occurred_at", pa.timestamp("us", tz="UTC"), nullable=False),
                pa.field("provider_trace_id", pa.string()),
                pa.field("artifact_hash", pa.string()),
                pa.field("reason", pa.string()),
                pa.field("event_json", pa.string(), nullable=False),
            ]
        ),
    )


def _artifacts_arrow(values: list[FoundryArtifactRecord]) -> Any:
    import pyarrow as pa

    return pa.Table.from_pylist(
        [
            {
                "artifact_id": value.artifact_id,
                "job_id": value.job_id,
                "paper_id": value.paper_id,
                "task_id": value.task_id,
                "family": value.family,
                "kind": value.kind,
                "status": value.status,
                "quality_label": value.quality_label,
                "package_uri": value.package_uri,
                "package_hash": value.package_hash,
                "environment_hash": value.environment_hash,
                "created_at": value.created_at,
                "artifact_json": canonical_json(value).decode("utf-8"),
                "pool": value.pool,
                "dataset_split": value.dataset_split,
            }
            for value in values
        ],
        schema=pa.schema(
            [
                pa.field("artifact_id", pa.string(), nullable=False),
                pa.field("job_id", pa.string(), nullable=False),
                pa.field("paper_id", pa.string(), nullable=False),
                pa.field("task_id", pa.string(), nullable=False),
                pa.field("family", pa.string(), nullable=False),
                pa.field("kind", pa.string(), nullable=False),
                pa.field("status", pa.string(), nullable=False),
                pa.field("quality_label", pa.string(), nullable=False),
                pa.field("package_uri", pa.string()),
                pa.field("package_hash", pa.string(), nullable=False),
                pa.field("environment_hash", pa.string(), nullable=False),
                pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=False),
                pa.field("artifact_json", pa.string(), nullable=False),
                pa.field("pool", pa.string(), nullable=False),
                pa.field("dataset_split", pa.string(), nullable=False),
            ]
        ),
    )


def _load_ids(table: Any, field: str) -> set[str]:
    values = table.scan(selected_fields=(field,)).to_arrow()[field].to_pylist()
    return {str(value) for value in values}


__all__ = ["FoundryLakehouseSink"]
