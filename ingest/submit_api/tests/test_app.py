"""Tests for the FastAPI submit endpoint."""

from __future__ import annotations

from typing import Any

import httpx
from fastapi.testclient import TestClient

from ingest.common.config import IngestConfig
from ingest.common.tests.conftest import FakeMinio, FakeProducer  # type: ignore[attr-defined]
from ingest.submit_api.app import AppState, create_app
from ingest.submit_api.limiter import PerSourceLimiter
from schemas.sourcefeed import RateLimitSpec, SourceFeedSpec


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
        hf_token=None,
        user_agent="ua",
        http_timeout_seconds=2.0,
        http_max_retries=0,
    )


def _wire_app(*, fetch_status: int = 200, fetch_body: bytes = b"<html>x</html>") -> tuple[Any, AppState]:
    """Build a FastAPI app pre-wired with fakes; the lifespan keeps them as-is."""
    app = create_app(cfg=_cfg())
    state: AppState = app.state.s2p
    state.cfg = _cfg()
    feed = SourceFeedSpec(
        name="manual-submit",
        protocol="manual",
        endpoint="https://example.com/",  # type: ignore[arg-type]
        poll_interval_seconds=600,
        rate_limit=RateLimitSpec(requests_per_second=20.0, burst=4),
    )
    state.feeds = {feed.name: feed}
    state.limiter = PerSourceLimiter()
    state.limiter.configure([feed])
    state.producer = FakeProducer()  # type: ignore[assignment]
    state.minio = FakeMinio()  # type: ignore[assignment]

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            fetch_status,
            content=fetch_body,
            headers={"content-type": "text/html; charset=utf-8"},
        )

    state.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return app, state


def test_submit_happy_path() -> None:
    app, state = _wire_app()
    with TestClient(app) as client:
        # Pre-start fakes that lifespan would normally bring up.
        # The fakes' start() is a no-op flag flip, so the lifespan branch sees
        # them as already-owned and leaves them alone.
        resp = client.post("/submit", json={"url": "https://example.com/p/1"})
    assert resp.status_code in {200, 201}
    body = resp.json()
    assert body["accepted"] is True
    assert body["doc_id"].startswith("sha256:")
    assert body["raw_topic"] == "raw.fetched"
    assert body["bytes_size"] > 0
    assert body["source_feed"] == "manual-submit"
    assert state.producer is not None
    assert len(state.producer.sent) == 1  # type: ignore[attr-defined]


def test_submit_rejects_unknown_feed() -> None:
    app, _ = _wire_app()
    with TestClient(app) as client:
        resp = client.post(
            "/submit",
            json={"url": "https://example.com/p", "source_feed": "not-declared"},
        )
    assert resp.status_code == 400
    assert "unknown source_feed" in resp.json()["detail"]


def test_submit_502_on_fetch_failure() -> None:
    app, _ = _wire_app(fetch_status=503)
    with TestClient(app) as client:
        resp = client.post("/submit", json={"url": "https://example.com/p"})
    assert resp.status_code == 502


def test_metrics_returns_prometheus_text() -> None:
    app, _ = _wire_app()
    with TestClient(app) as client:
        client.post("/submit", json={"url": "https://example.com/p"})
        resp = client.get("/metrics")
    assert resp.status_code == 200
    assert "s2p_submit_total" in resp.text


def test_healthz_reports_state() -> None:
    app, _ = _wire_app()
    with TestClient(app) as client:
        resp = client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["redpanda"] is True
    assert body["minio"] is True
    assert body["feeds_loaded"] == 1
