"""Admission ordering for seed adapters with deferred retained bodies."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from processor import seed_loader
from processor.seed.cursor import SeedCursor
from processor.seed.types import SeedDocument


class _CursorStore:
    def __init__(self) -> None:
        self.cursor = SeedCursor(repo_id="fixture/deferred")

    def load(self, _repo_id: str) -> SeedCursor:
        return self.cursor

    def save(self, cursor: SeedCursor) -> None:
        self.cursor = cursor


def test_deferred_body_fetch_happens_after_admission_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    def body_loader() -> str:
        events.append("body")
        return "Licensed archived item body with enough text for downstream processing."

    document = SeedDocument(
        repo_id="fixture/deferred",
        native_id="item:20260823000000:0001",
        url="https://example.com/item",
        title="Item",
        text="",
        lang="en",
        valid_from=datetime(2026, 8, 23, tzinfo=UTC),
        source_format="html",
        extraction_pipeline="fixture-deferred-body-v1",
        spdx_license="CC-BY-4.0",
        spdx_license_source="rss_entry",
        license_resolver="fixture-item-rights",
        license_evidence_url="https://example.com/item#license",
        license_evidence_revision="20260823000000",
        license_evidence_scope="item",
        body_loader=body_loader,
    )

    def factory(_cursor: SeedCursor, _cfg: seed_loader.SeedLoaderConfig):
        yield document

    monkeypatch.setitem(
        seed_loader.COMPONENTS,
        "deferred-fixture",
        ("fixture/deferred", factory),
    )
    cfg = seed_loader.SeedLoaderConfig(
        components=("deferred-fixture",),
        max_docs_per_component=None,
        dry_run=False,
        state_bucket="state",
        fineweb_url_allowlist=(),
        wayback_months=24,
    )

    seed_loader.stream_component(
        "deferred-fixture",
        cursor_store=_CursorStore(),  # type: ignore[arg-type]
        cfg=cfg,
        on_admission=lambda _decision: events.append("admission"),
        on_record=lambda _repo_id, _record: events.append("record"),
    )

    assert events == ["admission", "body", "record"]


def test_rejected_deferred_seed_never_fetches_body(monkeypatch: pytest.MonkeyPatch) -> None:
    body_calls = 0

    def body_loader() -> str:
        nonlocal body_calls
        body_calls += 1
        return "must not be fetched"

    document = SeedDocument(
        repo_id="fixture/deferred",
        native_id="item:20260823000000:0002",
        url="https://example.com/rejected",
        title="Rejected",
        text="",
        lang="en",
        valid_from=datetime(2026, 8, 23, tzinfo=UTC),
        source_format="html",
        extraction_pipeline="fixture-deferred-body-v1",
        spdx_license=None,
        spdx_license_source="unknown",
        body_loader=body_loader,
    )

    def factory(_cursor: SeedCursor, _cfg: seed_loader.SeedLoaderConfig):
        yield document

    monkeypatch.setitem(
        seed_loader.COMPONENTS,
        "deferred-fixture",
        ("fixture/deferred", factory),
    )
    cfg = seed_loader.SeedLoaderConfig(
        components=("deferred-fixture",),
        max_docs_per_component=None,
        dry_run=False,
        state_bucket="state",
        fineweb_url_allowlist=(),
        wayback_months=24,
    )

    admissions = []
    records = []
    seed_loader.stream_component(
        "deferred-fixture",
        cursor_store=_CursorStore(),  # type: ignore[arg-type]
        cfg=cfg,
        on_admission=admissions.append,
        on_record=lambda *_args: records.append(_args),
    )

    assert admissions[0].status == "quarantined"
    assert body_calls == 0
    assert records == []


def test_dry_run_never_fetches_deferred_body_or_persists_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body_calls = 0

    def body_loader() -> str:
        nonlocal body_calls
        body_calls += 1
        return "licensed archived body"

    document = SeedDocument(
        repo_id="fixture/deferred",
        native_id="item:20260823000000:0003",
        url="https://example.com/dry-run",
        title="Dry run",
        text="",
        lang="en",
        valid_from=datetime(2026, 8, 23, tzinfo=UTC),
        source_format="html",
        extraction_pipeline="fixture-deferred-body-v1",
        spdx_license="CC-BY-4.0",
        spdx_license_source="rss_entry",
        body_loader=body_loader,
    )

    def factory(_cursor: SeedCursor, _cfg: seed_loader.SeedLoaderConfig):
        yield document

    monkeypatch.setitem(
        seed_loader.COMPONENTS,
        "deferred-fixture",
        ("fixture/deferred", factory),
    )
    cfg = seed_loader.SeedLoaderConfig(
        components=("deferred-fixture",),
        max_docs_per_component=None,
        dry_run=True,
        state_bucket="state",
        fineweb_url_allowlist=(),
        wayback_months=24,
    )
    store = _CursorStore()
    save_calls = 0
    original_save = store.save

    def count_save(cursor: SeedCursor) -> None:
        nonlocal save_calls
        save_calls += 1
        original_save(cursor)

    store.save = count_save  # type: ignore[method-assign]
    records = []
    seed_loader.stream_component(
        "deferred-fixture",
        cursor_store=store,  # type: ignore[arg-type]
        cfg=cfg,
        on_admission=lambda _decision: None,
        on_record=lambda *_args: records.append(_args),
    )

    assert body_calls == 0
    assert save_calls == 0
    assert records == []
