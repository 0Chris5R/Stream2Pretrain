"""FastAPI app: ``POST /submit``, ``GET /healthz``, ``GET /metrics``.

Lifecycle:

- on startup: configure logging, init OTel, load SourceFeed list, start the
  shared ``BronzeProducer`` and ``MinioWriter``.
- on shutdown: close producer + writer.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
)

from ingest.common.bronze_pipeline import parse_http_date
from ingest.common.config import IngestConfig, load_config
from ingest.common.feeds import (
    load_feeds_from_kube,
    load_feeds_from_yaml,
)
from ingest.common.hashing import canonical_url, doc_id_for_url
from ingest.common.http_client import build_async_client, build_headers
from ingest.common.kafka_producer import BronzeProducer
from ingest.common.logging import configure_logging, get_logger
from ingest.common.minio_writer import MinioWriter
from ingest.common.otel import init_tracer
from ingest.common.s3 import bronze_object_key, bronze_s3_uri
from ingest.submit_api.limiter import PerSourceLimiter
from ingest.submit_api.models import HealthResponse, SubmitRequest, SubmitResponse
from schemas.bronze import BronzeRecord
from schemas.sourcefeed import SourceFeedSpec

log = get_logger(__name__)


# Prometheus metrics. Exposed at /metrics; ServiceMonitor picks them up.
_METRICS = CollectorRegistry()
SUBMIT_TOTAL = Counter(
    "s2p_submit_total",
    "Submissions accepted by the submit API.",
    labelnames=("source_feed", "outcome"),
    registry=_METRICS,
)
SUBMIT_DURATION = Histogram(
    "s2p_submit_duration_seconds",
    "End-to-end latency of POST /submit.",
    labelnames=("source_feed",),
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
    registry=_METRICS,
)


class AppState:
    """Mutable container of long-lived resources for the FastAPI app."""

    def __init__(self) -> None:
        self.cfg: IngestConfig | None = None
        self.feeds: dict[str, SourceFeedSpec] = {}
        self.producer: BronzeProducer | None = None
        self.minio: MinioWriter | None = None
        self.client: httpx.AsyncClient | None = None
        self.limiter: PerSourceLimiter = PerSourceLimiter()


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    state: AppState = app.state.s2p
    cfg = state.cfg or load_config()
    state.cfg = cfg
    configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    init_tracer("ingest.submit_api", cfg)

    # Tests pre-wire state.producer / state.minio / state.client. Only initialize
    # what is missing - this keeps the lifespan a clean no-op when tests inject
    # fakes, and a full bring-up in production.
    if not state.feeds:
        try:
            feeds = (
                load_feeds_from_yaml(cfg.feed_config_path)
                if cfg.feed_config_path
                else load_feeds_from_kube()
            )
        except FileNotFoundError:
            feeds = []
        state.feeds = {f.name: f for f in feeds}
        state.limiter.configure(list(state.feeds.values()))

    owns_producer = state.producer is None
    owns_minio = state.minio is None
    owns_client = state.client is None
    if owns_producer:
        state.producer = BronzeProducer(
            cfg.redpanda_brokers, topic=cfg.raw_topic, client_id="s2p-submit-api"
        )
        await state.producer.start()
    if owns_minio:
        state.minio = MinioWriter(
            cfg.minio_endpoint,
            cfg.minio_access_key,
            cfg.minio_secret_key,
            bucket=cfg.minio_bronze_bucket,
        )
        await state.minio.start()
    if owns_client:
        state.client = build_async_client(cfg, headers=build_headers(cfg))
    log.info("submit_api.ready", feeds=len(state.feeds))
    try:
        yield
    finally:
        if owns_producer and state.producer is not None:
            await state.producer.stop()
        if owns_minio and state.minio is not None:
            await state.minio.stop()
        if owns_client and state.client is not None:
            await state.client.aclose()


def create_app(cfg: IngestConfig | None = None) -> FastAPI:
    """Create the FastAPI app. Tests inject ``cfg`` plus monkey-patched state."""
    app = FastAPI(
        title="Stream2Pretrain Submit API",
        version="0.1.0",
        lifespan=_lifespan,
        docs_url="/docs",
        redoc_url=None,
    )
    state = AppState()
    state.cfg = cfg
    app.state.s2p = state

    @app.get("/healthz", response_model=HealthResponse)
    async def healthz() -> HealthResponse:
        s: AppState = app.state.s2p
        rp_ok = s.producer is not None
        mn_ok = s.minio is not None
        return HealthResponse(redpanda=rp_ok, minio=mn_ok, feeds_loaded=len(s.feeds))

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(generate_latest(_METRICS), media_type=CONTENT_TYPE_LATEST)

    @app.post("/submit", response_model=SubmitResponse, status_code=status.HTTP_201_CREATED)
    async def submit(req: SubmitRequest, request: Request) -> SubmitResponse:
        s: AppState = request.app.state.s2p
        if s.producer is None or s.minio is None or s.client is None or s.cfg is None:
            raise HTTPException(status_code=503, detail="submit_api not ready")
        # Reject submissions to undeclared source feeds, except the allowlisted
        # default 'manual-submit'. The PerSourceLimiter still applies in both cases.
        feed = s.feeds.get(req.source_feed)
        if feed is None and req.source_feed != "manual-submit":
            raise HTTPException(
                status_code=400,
                detail=f"unknown source_feed: {req.source_feed}",
            )
        await s.limiter.acquire(req.source_feed)

        canon = canonical_url(str(req.url))
        doc_id = doc_id_for_url(canon)
        with SUBMIT_DURATION.labels(req.source_feed).time():
            try:
                resp = await s.client.get(canon)
            except httpx.HTTPError as exc:
                SUBMIT_TOTAL.labels(req.source_feed, "fetch_error").inc()
                raise HTTPException(status_code=502, detail=f"fetch failed: {exc}") from exc
            if resp.status_code >= 400:
                SUBMIT_TOTAL.labels(req.source_feed, "fetch_error").inc()
                raise HTTPException(
                    status_code=502,
                    detail=f"upstream returned {resp.status_code}",
                )
            content_type = resp.headers.get(
                "content-type", "application/octet-stream"
            ).split(";")[0]
            payload = resp.content
            fetched_at = datetime.now(tz=timezone.utc)
            key = bronze_object_key(
                source_feed=req.source_feed,
                doc_id=doc_id,
                fetched_at=fetched_at,
            )
            stored = await s.minio.put_bronze(
                key=key,
                payload=payload,
                content_type=content_type,
                gzip_compress=True,
                metadata={
                    "doc_id": doc_id,
                    "source_feed": req.source_feed,
                    "url": canon,
                    "manual_submit": "true",
                },
            )
            record = BronzeRecord(
                doc_id=doc_id,
                url=canon,  # type: ignore[arg-type]
                fetched_at=fetched_at,
                http_status=resp.status_code,
                http_last_modified=parse_http_date(resp.headers.get("last-modified")),
                content_type=content_type,
                raw_html_s3_uri=bronze_s3_uri(
                    bucket=s.cfg.minio_bronze_bucket,
                    source_feed=req.source_feed,
                    doc_id=doc_id,
                    fetched_at=fetched_at,
                ),
                source_feed=req.source_feed,
                trace_id=_trace_id(),
                etag=resp.headers.get("etag"),
                bytes_size=stored,
            )
            await s.producer.send(record)
            SUBMIT_TOTAL.labels(req.source_feed, "ok").inc()

        license_tag = req.license_override or (feed.license_default if feed else None)
        return SubmitResponse(
            doc_id=doc_id,
            raw_topic=s.cfg.raw_topic,
            bronze_uri=record.raw_html_s3_uri,
            source_feed=req.source_feed,
            license=license_tag,
            bytes_size=stored,
        )

    return app


def _trace_id() -> str:
    import secrets

    return secrets.token_hex(16)


# uvicorn entrypoint when run as ``uvicorn ingest.submit_api.app:app``.
app = create_app()
