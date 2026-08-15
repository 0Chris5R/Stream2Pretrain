"""Local SourceFeed control plane with real bounded ingestion runs.

The Kubernetes profile stores SourceFeed specs as CRDs. The Podman profile
uses this small API instead: specs and run status are persisted on its named
volume, and ``Run once`` executes the same ingest code against local Redpanda
and MinIO. It deliberately does not impersonate Kubernetes.
"""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from aiohttp import web

from ingest.arxiv_html_fetcher.fetcher import _arxiv_id_from_url, run_for_ids
from ingest.common.config import IngestConfig, load_config
from ingest.common.http_client import build_async_client
from ingest.oaipmh_poller.poller import _run as run_oaipmh
from ingest.rss_poller.poller import discover_entry_urls
from ingest.rss_poller.poller import run_pass as run_rss
from ingest.sitemap_poller.poller import run_pass as run_sitemap
from schemas.sourcefeed import SourceFeedSpec

STATE_PATH = Path(os.environ.get("S2P_LOCAL_SOURCES_STATE", "/var/lib/s2p/sources.json"))
SEED_PATH = Path(os.environ.get("S2P_LOCAL_SOURCES_SEED", "/config/source_feeds.json"))
MAX_RECORDS = int(os.environ.get("S2P_LOCAL_SOURCE_MAX_RECORDS", "8"))
SCHEDULER_TICK_SECONDS = float(os.environ.get("S2P_LOCAL_SOURCE_SCHEDULER_TICK", "10"))


def _now() -> str:
    return datetime.now(tz=UTC).isoformat()


class LocalSourceStore:
    """Atomic local spec/status store used by the Podman control API."""

    def __init__(self, path: Path = STATE_PATH, seed_path: Path = SEED_PATH) -> None:
        self.path = path
        self.seed_path = seed_path
        self._lock = asyncio.Lock()
        self.data = self._load()

    def _load(self) -> dict[str, dict[str, Any]]:
        source = self.path if self.path.exists() else self.seed_path
        if not source.exists():
            return {}
        raw = json.loads(source.read_text(encoding="utf-8"))
        items = raw.get("sources", raw) if isinstance(raw, dict) else raw
        output: dict[str, dict[str, Any]] = {}
        for item in items:
            spec_raw = item.get("spec", item)
            spec = SourceFeedSpec.model_validate(spec_raw)
            output[spec.name] = {
                "spec": spec.model_dump(mode="json"),
                "last_success_at": item.get("last_success_at"),
                "last_attempt_at": item.get("last_attempt_at"),
                "last_error": item.get("last_error"),
                "documents_24h": int(item.get("documents_24h", 0)),
                "error_rate_24h": float(item.get("error_rate_24h", 0.0)),
                "poll_state": item.get("poll_state", "idle"),
                "seen_ids": list(item.get("seen_ids", [])),
            }
        return output

    def _persist(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps({"sources": list(self.data.values())}, indent=2),
            encoding="utf-8",
        )
        temporary.replace(self.path)

    @staticmethod
    def public(name: str, item: dict[str, Any]) -> dict[str, Any]:
        return {
            "name": name,
            "spec": item["spec"],
            "last_success_at": item.get("last_success_at"),
            "last_attempt_at": item.get("last_attempt_at"),
            "last_error": item.get("last_error"),
            "documents_24h": int(item.get("documents_24h", 0)),
            "error_rate_24h": float(item.get("error_rate_24h", 0.0)),
            "poll_state": item.get("poll_state", "idle"),
        }

    def list_sources(self) -> list[dict[str, Any]]:
        return [self.public(name, item) for name, item in sorted(self.data.items())]

    def due_names(self, now: datetime | None = None) -> list[str]:
        """Return enabled, idle feeds whose configured interval has elapsed."""

        instant = now or datetime.now(tz=UTC)
        due: list[str] = []
        for name, item in sorted(self.data.items()):
            spec = SourceFeedSpec.model_validate(item["spec"])
            if not spec.enabled or item.get("poll_state") == "polling":
                continue
            attempted = item.get("last_attempt_at")
            if not attempted:
                due.append(name)
                continue
            try:
                elapsed = (instant - datetime.fromisoformat(attempted)).total_seconds()
            except (TypeError, ValueError):
                due.append(name)
                continue
            if elapsed >= spec.poll_interval_seconds:
                due.append(name)
        return due

    async def upsert(self, spec: SourceFeedSpec) -> dict[str, Any]:
        async with self._lock:
            previous = self.data.get(spec.name, {})
            self.data[spec.name] = {
                "spec": spec.model_dump(mode="json"),
                "last_success_at": previous.get("last_success_at"),
                "last_attempt_at": previous.get("last_attempt_at"),
                "last_error": previous.get("last_error"),
                "documents_24h": int(previous.get("documents_24h", 0)),
                "error_rate_24h": float(previous.get("error_rate_24h", 0.0)),
                "poll_state": "idle",
                "seen_ids": list(previous.get("seen_ids", [])),
            }
            self._persist()
            return self.public(spec.name, self.data[spec.name])

    async def delete(self, name: str) -> bool:
        async with self._lock:
            if name not in self.data:
                return False
            del self.data[name]
            self._persist()
            return True

    async def set_enabled(self, name: str, enabled: bool) -> dict[str, Any] | None:
        async with self._lock:
            item = self.data.get(name)
            if item is None:
                return None
            spec = SourceFeedSpec.model_validate(item["spec"]).model_copy(
                update={"enabled": enabled}
            )
            item["spec"] = spec.model_dump(mode="json")
            item["poll_state"] = "idle"
            self._persist()
            return self.public(name, item)

    async def mark_polling(self, name: str) -> SourceFeedSpec | None:
        async with self._lock:
            item = self.data.get(name)
            if item is None:
                return None
            spec = SourceFeedSpec.model_validate(item["spec"])
            if not spec.enabled or item.get("poll_state") == "polling":
                return None
            item["last_attempt_at"] = _now()
            item["last_error"] = None
            item["poll_state"] = "polling"
            self._persist()
            return spec

    async def finish(self, name: str, *, emitted: int, seen_ids: list[str] | None = None) -> None:
        async with self._lock:
            item = self.data[name]
            item["last_success_at"] = _now()
            item["last_error"] = None
            item["poll_state"] = "idle"
            item["documents_24h"] = int(item.get("documents_24h", 0)) + emitted
            item["error_rate_24h"] = 0.0
            if seen_ids:
                item["seen_ids"] = list(dict.fromkeys([*item.get("seen_ids", []), *seen_ids]))[
                    -5000:
                ]
            self._persist()

    async def fail(self, name: str, error: str) -> None:
        async with self._lock:
            item = self.data[name]
            item["last_error"] = error[:500]
            item["poll_state"] = "error"
            item["error_rate_24h"] = 1.0
            self._persist()


async def _run_arxiv_rss(
    spec: SourceFeedSpec, cfg: IngestConfig, seen: set[str]
) -> tuple[int, list[str]]:
    async with build_async_client(cfg) as client:
        response = await client.get(str(spec.endpoint))
        response.raise_for_status()
        ids: list[str] = []
        for url in discover_entry_urls(response.text):
            arxiv_id = _arxiv_id_from_url(url)
            if arxiv_id and arxiv_id not in seen and arxiv_id not in ids:
                ids.append(arxiv_id)
            if len(ids) >= MAX_RECORDS:
                break
    emitted = await run_for_ids(
        ids,
        cfg,
        feed_name=spec.name,
        license_default=spec.license_default,
    )
    return emitted, ids[:emitted]


async def _execute(store: LocalSourceStore, name: str, spec: SourceFeedSpec) -> None:
    try:
        cfg = load_config()
        seen_ids = set(store.data[name].get("seen_ids", []))
        host = (urlparse(str(spec.endpoint)).hostname or "").lower()
        newly_seen: list[str] = []
        if spec.protocol in {"rss", "atom"} and host.endswith("arxiv.org"):
            emitted, newly_seen = await _run_arxiv_rss(spec, cfg, seen_ids)
        elif spec.protocol in {"rss", "atom"}:
            emitted = await run_rss(cfg, [spec])
        elif spec.protocol == "sitemap":
            emitted = await run_sitemap(cfg, [spec])
        elif spec.protocol == "oai-pmh":
            emitted = await run_oaipmh(cfg, [spec], max_records=MAX_RECORDS)
        else:
            raise ValueError(f"Run once is not available for {spec.protocol} sources")
        await store.finish(name, emitted=emitted, seen_ids=newly_seen)
    except Exception as exc:
        await store.fail(name, f"{type(exc).__name__}: {exc}")


def build_app(store: LocalSourceStore | None = None) -> web.Application:
    source_store = store or LocalSourceStore()
    background_tasks: set[asyncio.Task[None]] = set()

    def start_execution(name: str, spec: SourceFeedSpec) -> None:
        task = asyncio.create_task(_execute(source_store, name, spec))
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)

    async def scheduler() -> None:
        while True:
            for name in source_store.due_names():
                spec = await source_store.mark_polling(name)
                if spec is not None:
                    start_execution(name, spec)
            await asyncio.sleep(SCHEDULER_TICK_SECONDS)

    async def start_scheduler(app: web.Application) -> None:
        app["source_scheduler"] = asyncio.create_task(scheduler())

    async def stop_scheduler(app: web.Application) -> None:
        scheduler_task: asyncio.Task[None] = app["source_scheduler"]
        scheduler_task.cancel()
        with suppress(asyncio.CancelledError):
            await scheduler_task
        for task in list(background_tasks):
            task.cancel()
        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)

    async def list_sources(_: web.Request) -> web.Response:
        return web.json_response(source_store.list_sources())

    async def upsert_source(request: web.Request) -> web.Response:
        spec = SourceFeedSpec.model_validate(await request.json())
        return web.json_response(await source_store.upsert(spec), status=201)

    async def patch_source(request: web.Request) -> web.Response:
        body = await request.json()
        if not isinstance(body.get("enabled"), bool):
            raise web.HTTPBadRequest(text="enabled must be a boolean")
        status = await source_store.set_enabled(request.match_info["name"], body["enabled"])
        if status is None:
            raise web.HTTPNotFound(text="source not found")
        return web.json_response(status)

    async def delete_source(request: web.Request) -> web.Response:
        if not await source_store.delete(request.match_info["name"]):
            raise web.HTTPNotFound(text="source not found")
        return web.json_response({"deleted": True})

    async def run_source(request: web.Request) -> web.Response:
        name = request.match_info["name"]
        spec = await source_store.mark_polling(name)
        if spec is None:
            if name not in source_store.data:
                raise web.HTTPNotFound(text="source not found")
            raise web.HTTPConflict(text="source is disabled or already polling")
        start_execution(name, spec)
        return web.json_response(LocalSourceStore.public(name, source_store.data[name]), status=202)

    async def probe(_: web.Request) -> web.Response:
        return web.Response(text="ok\n")

    app = web.Application()
    app.router.add_get("/healthz", probe)
    app.router.add_get("/readyz", probe)
    app.router.add_get("/v1/sources", list_sources)
    app.router.add_post("/v1/sources", upsert_source)
    app.router.add_patch("/v1/sources/{name}", patch_source)
    app.router.add_delete("/v1/sources/{name}", delete_source)
    app.router.add_post("/v1/sources/{name}/run", run_source)
    app.on_startup.append(start_scheduler)
    app.on_cleanup.append(stop_scheduler)
    return app


def main() -> None:
    web.run_app(
        build_app(),
        host=os.environ.get("S2P_BIND_HOST", "0.0.0.0"),
        port=int(os.environ.get("S2P_LOCAL_SOURCES_API_PORT", "8083")),
    )


if __name__ == "__main__":
    main()
