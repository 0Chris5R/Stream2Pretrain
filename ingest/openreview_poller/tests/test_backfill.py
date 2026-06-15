"""Tests for the REVIEWARENA backfill bridge."""

from __future__ import annotations

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


def test_resolve_reviewarena_id_picks_best_candidate() -> None:
    class _C:
        def __init__(self, repo_id: str) -> None:
            self.id = repo_id

    def search(*, search: str) -> list[_C]:
        assert search == backfill.DEFAULT_REVIEWARENA_QUERY
        return [
            _C("foo/random-dataset"),
            _C("alice/openreview-extras"),
            _C("alice/REVIEWARENA-iclr"),
            _C("bob/review-arena-v2"),
        ]

    chosen = backfill.resolve_reviewarena_id(override=None, search_fn=search)
    assert chosen == "alice/REVIEWARENA-iclr"


def test_resolve_reviewarena_id_returns_none_when_no_match() -> None:
    chosen = backfill.resolve_reviewarena_id(
        override=None, search_fn=lambda *, search: []
    )
    assert chosen is None


def test_row_view_extracts_minimal_fields() -> None:
    row = {
        "id": "noteX",
        "venue": "ICLR.cc/2026/Conference",
        "year": 2026,
        "title": "Awesome paper",
        "pdf": {"bytes": b"%PDF body", "path": "x.pdf"},
        "reviews": [
            {"review": "Looks good."},
            {"review": "Needs work."},
        ],
        "rebuttal": "Thanks for the comments.",
        "decision": "Accept",
        "cdate": 1718457600000,
    }
    view = backfill._RowView.from_dict(row)
    assert view.note_id == "noteX"
    assert view.venue == "ICLR.cc/2026/Conference"
    assert view.year == 2026
    assert view.pdf_bytes == b"%PDF body"
    assert view.review_text is not None
    assert "Looks good." in view.review_text
    assert "Thanks for the comments." in view.review_text
    assert view.decision == "Accept"
    assert view.cdate is not None


def test_row_view_returns_none_when_review_missing() -> None:
    view = backfill._RowView.from_dict({"id": "n", "venue": "v"})
    assert view.review_text is None
    assert view.pdf_bytes is None


@pytest.mark.asyncio
async def test_run_backfill_streams_rows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)

    rows: list[dict[str, Any]] = [
        {
            "id": "p1",
            "venue": "ICLR.cc/2026/Conference",
            "year": 2026,
            "title": "Paper one",
            "pdf": {"bytes": b"%PDF p1"},
            "reviews": [{"review": "R1"}, {"review": "R2"}],
            "decision": "Accept",
            "cdate": 1718457600000,
        },
        {
            "id": "p2",
            "venue": "NeurIPS.cc/2025/Conference",
            "year": 2025,
            "title": "Paper two",
            "pdf": {"bytes": b"%PDF p2"},
            "reviews": [{"review": "R3"}],
            "decision": "Reject",
            "cdate": 1719000000000,
        },
        # Row missing both pdf and review - should be counted but emit nothing.
        {"id": "p3", "venue": "ICML.cc/2025/Conference", "year": 2025},
    ]

    def loader(dataset_id: str, *, split: str) -> list[dict[str, Any]]:
        assert dataset_id == "alice/REVIEWARENA"
        assert split == "train"
        return rows

    fake_producer = FakeProducer()
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

    stats = await backfill.run_backfill(
        _cfg(),
        dataset_id="alice/REVIEWARENA",
        streaming_loader=loader,
    )

    assert stats.dataset_id == "alice/REVIEWARENA"
    assert stats.rows_seen == 3
    assert stats.pdfs_emitted == 2
    assert stats.reviews_emitted == 2
    assert stats.skipped == 0
    formats = [m["record"].source_format for m in fake_producer.sent]
    assert formats.count("pdf") == 2
    assert formats.count("review") == 2
    pipelines = [m["record"].extraction_pipeline for m in fake_producer.sent]
    assert backfill.PIPELINE_PDF_BACKFILL in pipelines
    assert backfill.PIPELINE_REVIEW_BACKFILL in pipelines


@pytest.mark.asyncio
async def test_run_backfill_returns_empty_when_unresolved(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("S2P_REVIEWARENA_DATASET", raising=False)
    stats = await backfill.run_backfill(
        _cfg(),
        dataset_id=None,
        search_fn=lambda *, search: [],
        streaming_loader=lambda *a, **k: iter(()),
    )
    assert stats.dataset_id == ""
    assert stats.rows_seen == 0
    assert stats.pdfs_emitted == 0


@pytest.mark.asyncio
async def test_run_backfill_respects_max_rows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    fake_producer = FakeProducer()
    fake_minio = FakeMinio()
    monkeypatch.setattr(backfill, "BronzeProducer", lambda *a, **kw: fake_producer)
    monkeypatch.setattr(backfill, "MinioWriter", lambda *a, **kw: fake_minio)

    rows = [
        {
            "id": f"n{i}",
            "venue": "ICLR.cc/2026/Conference",
            "year": 2026,
            "pdf": {"bytes": b"%PDF" + str(i).encode()},
            "reviews": [{"review": f"R{i}"}],
            "cdate": 1718457600000 + i,
        }
        for i in range(10)
    ]

    def loader(dataset_id: str, *, split: str) -> list[dict[str, Any]]:
        return rows

    stats = await backfill.run_backfill(
        _cfg(),
        dataset_id="alice/REVIEWARENA",
        streaming_loader=loader,
        max_rows=3,
    )
    assert stats.rows_seen == 3
    assert stats.pdfs_emitted == 3
    assert stats.reviews_emitted == 3
