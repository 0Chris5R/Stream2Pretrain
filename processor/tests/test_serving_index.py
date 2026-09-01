from __future__ import annotations

from datetime import UTC, datetime

import duckdb

from processor.serving_index import ServingIndex
from schemas.gold import GoldRecord
from schemas.license_admission import LicenseAdmissionDecision


def _gold(index: int, *, route: str = "pretrain") -> GoldRecord:
    return GoldRecord(
        doc_id="sha256:" + format(index, "064x"),
        text=f"# Paper {index}\n\nComplete training projection.",
        lang="en",
        tokens=8,
        quality_score=4.0,
        edu_score=4.0,
        license="Apache-2.0",
        license_source="unknown",
        risk_tier=1,
        valid_from=datetime(2026, 9, 1, 12, index, tzinfo=UTC),
        scoring_version="pretrain-content-v3",
        classifier_revision="finepdfs-edu-v2",
        policy_revision="git:test",
        trace_id=format(index, "032x"),
        source_feed="arxiv-html-fetcher",
        route=route,  # type: ignore[arg-type]
    )


def _admission() -> LicenseAdmissionDecision:
    return LicenseAdmissionDecision(
        decision_id="sha256:" + "f" * 64,
        doc_id="sha256:" + "e" * 64,
        source_feed="arxiv-html-fetcher",
        source_url="https://arxiv.org/abs/2609.00001",
        source_format="html",
        observed_at=datetime(2026, 9, 1, tzinfo=UTC),
        status="quarantined",
        license_id="CC-BY-ND-4.0",
        license_source="arxiv_atom",
        reason="derivatives prohibited",
        trace_id="f" * 32,
        policy_revision="license-policy-2026-08-25",
    )


def test_index_upserts_current_rows_and_drives_cursor_queries(tmp_path) -> None:
    path = tmp_path / "serving.duckdb"
    index = ServingIndex(
        database_path=str(path), brokers="unused", decisions_topic="d", admissions_topic="a"
    )
    connection = duckdb.connect(str(path))
    for value in range(3):
        index.apply_decision(connection, _gold(value))
    index.apply_decision(connection, _gold(1))

    service = index.query_service()
    first = service.documents(page_size=2)
    second = service.documents(page_size=2, cursor=first["next_cursor"])

    assert index.counts()["decisions"] == 3
    assert first["total"] == 3
    assert first["has_more"] is True
    assert [row["title"] for row in first["items"]] == ["Paper 2", "Paper 1"]
    assert [row["title"] for row in second["items"]] == ["Paper 0"]
    assert service.corpus_overview()["training_export_documents"] == 3
    connection.close()


def test_index_batches_decisions_and_keeps_smallest_trace_for_iceberg_parity(
    tmp_path,
) -> None:
    path = tmp_path / "serving.duckdb"
    index = ServingIndex(
        database_path=str(path), brokers="unused", decisions_topic="d", admissions_topic="a"
    )
    connection = duckdb.connect(str(path))
    first = _gold(1).model_copy(update={"trace_id": "f" * 32, "text": "later"})
    chosen = _gold(1).model_copy(update={"trace_id": "0" * 32, "text": "chosen"})

    index.apply_decisions(connection, [_gold(0), first, chosen, _gold(2)])

    selected = connection.execute(
        "SELECT text, trace_id FROM serving_decisions WHERE doc_id = ?", [chosen.doc_id]
    ).fetchone()
    assert index.counts()["decisions"] == 3
    assert selected == ("chosen", "0" * 32)
    connection.close()


def test_index_keeps_pre_body_quarantine_in_same_monitoring_view(tmp_path) -> None:
    path = tmp_path / "serving.duckdb"
    index = ServingIndex(
        database_path=str(path), brokers="unused", decisions_topic="d", admissions_topic="a"
    )
    connection = duckdb.connect(str(path))
    index.apply_admission(connection, _admission())

    overview = index.query_service().corpus_overview()

    assert overview["durable_decisions"] == 1
    assert overview["rejected_by_reason"] == {"license_not_permitted": 1}
    assert index.counts()["license_admissions"] == 1
    connection.close()


def test_index_batches_admissions_by_durable_decision_id(tmp_path) -> None:
    path = tmp_path / "serving.duckdb"
    index = ServingIndex(
        database_path=str(path), brokers="unused", decisions_topic="d", admissions_topic="a"
    )
    connection = duckdb.connect(str(path))
    admission = _admission()
    updated = admission.model_copy(update={"reason": "updated audit reason"})

    index.apply_admissions(connection, [admission, updated])

    row = connection.execute(
        "SELECT reason FROM serving_license_admissions WHERE decision_id = ?",
        [admission.decision_id],
    ).fetchone()
    assert row == ("updated audit reason",)
    assert index.counts()["license_admissions"] == 1
    connection.close()
