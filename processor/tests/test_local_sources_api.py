from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from processor.local_sources_api import LocalSourceStore
from schemas.sourcefeed import RateLimitSpec, SourceFeedSpec


def _spec(name: str = "arxiv-test") -> SourceFeedSpec:
    return SourceFeedSpec(
        name=name,
        protocol="rss",
        endpoint="https://rss.arxiv.org/rss/cs.CL",
        poll_interval_seconds=900,
        rate_limit=RateLimitSpec(requests_per_second=1, burst=2),
        license_default="per-record",
    )


@pytest.mark.asyncio
async def test_local_source_store_crud_is_persistent(tmp_path: Path) -> None:
    state = tmp_path / "sources.json"
    store = LocalSourceStore(path=state, seed_path=tmp_path / "missing.json")

    created = await store.upsert(_spec())
    assert created["name"] == "arxiv-test"
    assert created["spec"]["enabled"] is True

    disabled = await store.set_enabled("arxiv-test", False)
    assert disabled is not None
    assert disabled["spec"]["enabled"] is False

    reloaded = LocalSourceStore(path=state, seed_path=tmp_path / "missing.json")
    assert reloaded.list_sources()[0]["spec"]["enabled"] is False
    assert await reloaded.delete("arxiv-test") is True
    assert reloaded.list_sources() == []


@pytest.mark.asyncio
async def test_local_source_store_tracks_run_state(tmp_path: Path) -> None:
    store = LocalSourceStore(path=tmp_path / "sources.json", seed_path=tmp_path / "missing.json")
    await store.upsert(_spec())
    assert await store.mark_polling("arxiv-test") == _spec()
    assert store.list_sources()[0]["poll_state"] == "polling"

    await store.finish("arxiv-test", emitted=3, seen_ids=["2608.00001"])
    status = store.list_sources()[0]
    assert status["poll_state"] == "idle"
    assert status["documents_24h"] == 3
    assert status["last_success_at"] is not None


@pytest.mark.asyncio
async def test_local_source_store_schedules_only_due_enabled_feeds(tmp_path: Path) -> None:
    store = LocalSourceStore(path=tmp_path / "sources.json", seed_path=tmp_path / "missing.json")
    await store.upsert(_spec("due"))
    await store.upsert(_spec("recent"))
    await store.upsert(_spec("disabled"))
    await store.set_enabled("disabled", False)

    now = datetime(2026, 8, 15, 12, tzinfo=UTC)
    store.data["due"]["last_attempt_at"] = (now - timedelta(seconds=901)).isoformat()
    store.data["recent"]["last_attempt_at"] = (now - timedelta(seconds=899)).isoformat()

    assert store.due_names(now) == ["due"]
