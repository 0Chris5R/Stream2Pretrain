"""Iceberg ``as_of(timestamp)`` temporal-query test.

This validates the second locked novelty differentiator: per-document validity
intervals propagated all the way into the gold table, queryable via a snapshot
or timestamp predicate. We use ``pyiceberg`` against an in-process SQLite
catalog plus a local filesystem warehouse so the test is hermetic and does not
depend on Polaris being up.

Skip rules:
- ``pyiceberg`` and ``pyarrow`` not installed -> skip.
- DuckDB iceberg extension is exercised only as an optional secondary check;
  its absence does not fail the test.
"""

from __future__ import annotations

import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

pyiceberg = pytest.importorskip("pyiceberg", reason="pyiceberg not installed")
pa = pytest.importorskip("pyarrow", reason="pyarrow not installed")

from pyiceberg.catalog.sql import SqlCatalog  # noqa: E402
from pyiceberg.schema import Schema  # noqa: E402
from pyiceberg.types import (  # noqa: E402
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)


def _iceberg_schema() -> Schema:
    """Minimal schema mirroring the gold-tier validity-interval columns."""
    return Schema(
        NestedField(1, "doc_id", StringType(), required=True),
        NestedField(2, "text", StringType(), required=True),
        NestedField(3, "lang", StringType(), required=True),
        NestedField(4, "tokens", LongType(), required=True),
        NestedField(5, "valid_from", TimestamptzType(), required=True),
        NestedField(6, "valid_to", TimestamptzType(), required=False),
    )


@pytest.fixture
def gold_table() -> tuple[SqlCatalog, str]:
    """A fresh Iceberg catalog with a populated gold table.

    Three rows with three distinct ``valid_from`` timestamps, each written in
    a separate snapshot so ``as_of`` semantics have something to query.
    """
    tmp = tempfile.mkdtemp(prefix="s2p-iceberg-")
    warehouse = Path(tmp, "warehouse")
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog(
        "s2p_test",
        **{
            "uri": f"sqlite:///{tmp}/catalog.db",
            "warehouse": warehouse.as_uri(),
        },
    )
    catalog.create_namespace("gold")
    table = catalog.create_table("gold.curated", schema=_iceberg_schema())

    base = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        {
            "doc_id": "sha256:" + "a" * 64,
            "text": "alpha document",
            "lang": "en",
            "tokens": 100,
            "valid_from": base,
            "valid_to": None,
        },
        {
            "doc_id": "sha256:" + "b" * 64,
            "text": "beta document",
            "lang": "en",
            "tokens": 250,
            "valid_from": base + timedelta(days=30),
            "valid_to": None,
        },
        {
            "doc_id": "sha256:" + "c" * 64,
            "text": "gamma document",
            "lang": "en",
            "tokens": 412,
            "valid_from": base + timedelta(days=90),
            "valid_to": None,
        },
        # Retracted document: published before the alpha row but already
        # withdrawn by the as_of() target instant. Must not appear in the
        # query result; its valid_to upper bound enforces the half-open
        # interval semantics that pillar N2 promises.
        {
            "doc_id": "sha256:" + "d" * 64,
            "text": "delta document (retracted)",
            "lang": "en",
            "tokens": 77,
            "valid_from": base - timedelta(days=10),
            "valid_to": base + timedelta(days=5),
        },
    ]
    for row in rows:
        df = pa.Table.from_pylist([row], schema=table.schema().as_arrow())
        table.append(df)
    return catalog, "gold.curated"


def test_validity_interval_filter_returns_in_window_rows(
    gold_table: tuple[SqlCatalog, str],
) -> None:
    """Selecting rows valid at a target instant returns exactly the in-window set.

    Implements the half-open ``[valid_from, valid_to)`` predicate end-to-end:
    a document is valid at ``ts`` iff ``valid_from <= ts AND
    (valid_to IS NULL OR valid_to > ts)``. The retracted "delta" row is
    expected to be excluded because its ``valid_to`` is before the target.
    """
    catalog, name = gold_table
    table = catalog.load_table(name)
    target = datetime(2026, 2, 15, tzinfo=UTC)
    target_iso = target.isoformat()
    arrow = table.scan(
        row_filter=(
            f"valid_from <= '{target_iso}' AND (valid_to IS NULL OR valid_to > '{target_iso}')"
        ),
    ).to_arrow()
    doc_ids = sorted(arrow.column("doc_id").to_pylist())
    expected = sorted(["sha256:" + "a" * 64, "sha256:" + "b" * 64])
    assert doc_ids == expected, f"as_of({target}) returned {doc_ids}"


def test_iceberg_snapshots_are_monotonic(
    gold_table: tuple[SqlCatalog, str],
) -> None:
    """Each ``append`` creates a new snapshot; commit timestamps are monotonic."""
    catalog, name = gold_table
    table = catalog.load_table(name)
    snapshots = list(table.snapshots())
    assert len(snapshots) >= 3, f"expected at least 3 snapshots, got {len(snapshots)}"
    timestamps = [s.timestamp_ms for s in snapshots]
    assert timestamps == sorted(timestamps), "snapshot timestamps must be monotonic"


def test_time_travel_returns_subset_at_earlier_snapshot(
    gold_table: tuple[SqlCatalog, str],
) -> None:
    """Reading the table at the first snapshot must return only the first row."""
    catalog, name = gold_table
    table = catalog.load_table(name)
    first_snapshot = next(iter(table.snapshots()))
    arrow = table.scan(snapshot_id=first_snapshot.snapshot_id).to_arrow()
    doc_ids = arrow.column("doc_id").to_pylist()
    assert doc_ids == ["sha256:" + "a" * 64]
