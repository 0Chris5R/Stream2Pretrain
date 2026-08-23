"""Tests for the REVIEWARENA backfill bridge."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from ingest.common.config import IngestConfig
from ingest.common.tests.conftest import FakeMinio, FakeProducer  # type: ignore[attr-defined]
from ingest.openreview_poller import backfill


def _cfg() -> IngestConfig:
    return IngestConfig(
        env="dev",
        log_level="DEBUG",
        redpanda_brokers="",
        minio_endpoint="",
        minio_access_key="",
        minio_secret_key="",
        minio_bronze_bucket="bronze",
        otel_endpoint=None,
        otel_protocol="grpc",
        github_token=None,
        hf_token="hf_test",
        user_agent="ua",
        http_timeout_seconds=2.0,
        http_max_retries=0,
        request_jitter_max_seconds=0.0,
    )


def test_resolve_reviewarena_id_prefers_override() -> None:
    chosen = backfill.resolve_reviewarena_id(override="me/REVIEWARENA-iclr2026")
    assert chosen == "me/REVIEWARENA-iclr2026"


def test_resolve_reviewarena_id_uses_schema_pinned_default() -> None:
    chosen = backfill.resolve_reviewarena_id(override=None)
    assert chosen == backfill.DEFAULT_REVIEWARENA_DATASET


def test_row_view_extracts_minimal_fields() -> None:
    row = {
        "forum_id": "noteX",
        "conference": "iclr",
        "venue_id": "ICLR.cc/2026/Conference",
        "year": 2026,
        "title": "Awesome paper",
        "markdown": "# Awesome paper\n\nScientific body.",
        "reviews_json": (
            '[{"review_id":"r1","summary":"Looks good.","weaknesses":"Needs work.","rating":"6"}]'
        ),
        "author_rebuttal": "Thanks for the comments.",
        "decision": "Accept",
        "decision_comment": "The contribution is sound.",
    }
    view = backfill._RowView.from_dict(row)
    assert view.note_id == "noteX"
    assert view.venue == "ICLR.cc/2026/Conference"
    assert view.year == 2026
    assert view.markdown == "# Awesome paper\n\nScientific body."
    assert view.review_text is not None
    assert "Looks good." in view.review_text
    assert "Thanks for the comments." in view.review_text
    assert "The contribution is sound." in view.review_text
    assert view.decision == "Accept"
    assert view.cdate is None


def test_row_view_returns_none_when_review_missing() -> None:
    view = backfill._RowView.from_dict({"forum_id": "n", "venue_id": "v"})
    assert view.review_text is None
    assert view.markdown is None


@pytest.mark.asyncio
async def test_run_backfill_streams_rows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)

    rows: list[dict[str, Any]] = [
        {
            "forum_id": "p1",
            "venue_id": "ICLR.cc/2026/Conference",
            "year": 2026,
            "title": "Paper one",
            "markdown": "# Paper one\n\nFull scientific text.",
            "reviews_json": '[{"summary":"R1"},{"summary":"R2"}]',
            "decision": "Accept",
        },
        {
            "forum_id": "p2",
            "venue_id": "NeurIPS.cc/2025/Conference",
            "year": 2025,
            "title": "Paper two",
            "markdown": "# Paper two\n\nAnother scientific text.",
            "reviews_json": '[{"summary":"R3"}]',
            "decision": "Reject",
        },
        # Row missing both Markdown and review - counted but emits nothing.
        {"forum_id": "p3", "venue_id": "ICML.cc/2025/Conference", "year": 2025},
    ]

    def loader(dataset_id: str, *, revision: str, split: str) -> list[dict[str, Any]]:
        assert dataset_id == "alice/REVIEWARENA"
        assert revision == backfill.DEFAULT_REVIEWARENA_REVISION
        assert split == "iclr"
        return rows

    fake_producer = FakeProducer()
    fake_admissions = FakeProducer()
    fake_minio = FakeMinio()

    # Patch the producer/minio context managers used inside run_backfill.
    monkeypatch.setattr(
        backfill,
        "BronzeProducer",
        lambda *a, **kw: fake_producer,  # type: ignore[arg-type]
    )
    monkeypatch.setattr(
        backfill,
        "MinioWriter",
        lambda *a, **kw: fake_minio,  # type: ignore[arg-type]
    )
    monkeypatch.setattr(
        backfill,
        "LicenseAdmissionProducer",
        lambda *a, **kw: fake_admissions,  # type: ignore[arg-type]
    )

    stats = await backfill.run_backfill(
        _cfg(),
        dataset_id="alice/REVIEWARENA",
        streaming_loader=loader,
        splits=("iclr",),
    )

    assert stats.dataset_id == "alice/REVIEWARENA"
    assert stats.rows_seen == 3
    assert stats.papers_emitted == 2
    assert stats.reviews_emitted == 2
    assert stats.skipped == 0
    formats = [m["record"].source_format for m in fake_producer.sent]
    assert formats.count("markdown") == 2
    assert formats.count("review") == 2
    assert {m["record"].training_usage for m in fake_producer.sent} == {"posttrain_transform_only"}
    assert {m["record"].spdx_license for m in fake_producer.sent} == {"unknown"}
    pipelines = [m["record"].extraction_pipeline for m in fake_producer.sent]
    assert backfill.PIPELINE_MARKDOWN_BACKFILL in pipelines
    assert backfill.PIPELINE_REVIEW_BACKFILL in pipelines


@pytest.mark.asyncio
async def test_run_backfill_uses_pinned_default_when_not_overridden(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("S2P_REVIEWARENA_DATASET", raising=False)
    monkeypatch.setattr(backfill, "BronzeProducer", lambda *a, **kw: FakeProducer())
    monkeypatch.setattr(backfill, "LicenseAdmissionProducer", lambda *a, **kw: FakeProducer())
    monkeypatch.setattr(backfill, "MinioWriter", lambda *a, **kw: FakeMinio())
    stats = await backfill.run_backfill(
        _cfg(),
        dataset_id=None,
        streaming_loader=lambda *a, **k: iter(()),
        splits=("neurips",),
    )
    assert stats.dataset_id == backfill.DEFAULT_REVIEWARENA_DATASET
    assert stats.rows_seen == 0
    assert stats.papers_emitted == 0


@pytest.mark.asyncio
async def test_run_backfill_respects_max_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    fake_producer = FakeProducer()
    fake_admissions = FakeProducer()
    fake_minio = FakeMinio()
    monkeypatch.setattr(backfill, "BronzeProducer", lambda *a, **kw: fake_producer)
    monkeypatch.setattr(backfill, "MinioWriter", lambda *a, **kw: fake_minio)
    monkeypatch.setattr(backfill, "LicenseAdmissionProducer", lambda *a, **kw: fake_admissions)

    rows = [
        {
            "forum_id": f"n{i}",
            "venue_id": "ICLR.cc/2026/Conference",
            "year": 2026,
            "markdown": f"# Paper {i}\n\nScientific text {i}.",
            "reviews_json": json.dumps([{"summary": f"R{i}"}]),
        }
        for i in range(10)
    ]

    def loader(dataset_id: str, *, revision: str, split: str) -> list[dict[str, Any]]:
        return rows

    stats = await backfill.run_backfill(
        _cfg(),
        dataset_id="alice/REVIEWARENA",
        streaming_loader=loader,
        max_rows=3,
        splits=("iclr",),
    )
    assert stats.rows_seen == 3
    assert stats.papers_emitted == 3
    assert stats.reviews_emitted == 3
