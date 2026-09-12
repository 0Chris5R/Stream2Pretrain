"""Readiness contracts for the split Foundry API and worker."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from aiohttp.test_utils import make_mocked_request

from processor.foundry.api import build_app
from processor.foundry.worker import WorkerRuntime


class _DatabaseState:
    def __init__(self, ready: bool) -> None:
        self.ready = ready
        self.calls = 0

    def database_ready(self) -> bool:
        self.calls += 1
        return self.ready


def _get(app: Any, path: str) -> Any:
    route = next(
        route
        for route in app.router.routes()
        if route.method == "GET" and route.resource.canonical == path
    )
    return asyncio.run(route.handler(make_mocked_request("GET", path)))


def test_foundry_api_readiness_requires_both_coordination_connections() -> None:
    store = _DatabaseState(False)
    quota = _DatabaseState(True)
    app = build_app(store=store, quota=quota, s3_client=object())  # type: ignore[arg-type]

    health = _get(app, "/healthz")
    readiness = _get(app, "/readyz")

    assert health.status == 200
    assert readiness.status == 503
    assert store.calls == 1
    assert quota.calls == 1


def test_foundry_api_is_ready_when_both_coordination_connections_respond() -> None:
    store = _DatabaseState(True)
    quota = _DatabaseState(True)
    app = build_app(store=store, quota=quota, s3_client=object())  # type: ignore[arg-type]

    assert _get(app, "/readyz").status == 200


def test_foundry_worker_readiness_checks_control_and_quota_connections() -> None:
    store = _DatabaseState(False)
    quota = _DatabaseState(True)
    runtime = object.__new__(WorkerRuntime)
    runtime.store = store  # type: ignore[assignment]
    runtime.quota = quota  # type: ignore[assignment]

    assert not runtime.database_ready()
    assert store.calls == 1
    assert quota.calls == 1

    runtime.store = SimpleNamespace(database_ready=lambda: True)  # type: ignore[assignment]
    assert runtime.database_ready()


def test_foundry_queue_loop_retries_after_coordination_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StopAfterTwoIterations:
        def __init__(self) -> None:
            self.calls = 0

        def wait(self, _seconds: float) -> bool:
            self.calls += 1
            return self.calls > 2

    warnings: list[str] = []
    iterations: list[int] = []
    runtime = object.__new__(WorkerRuntime)
    runtime.config = SimpleNamespace(queue_poll_seconds=0)  # type: ignore[assignment]
    runtime._drain_stop = StopAfterTwoIterations()  # type: ignore[assignment]

    def iteration(_log: Any) -> None:
        iterations.append(len(iterations) + 1)
        if len(iterations) == 1:
            raise RuntimeError("database failover")

    runtime._queue_iteration = iteration  # type: ignore[method-assign]
    monkeypatch.setattr(
        "structlog.get_logger",
        lambda **_kwargs: SimpleNamespace(warning=lambda event, **_fields: warnings.append(event)),
    )

    runtime._queue_loop()

    assert iterations == [1, 2]
    assert warnings == ["foundry_queue_iteration_retry_pending"]
