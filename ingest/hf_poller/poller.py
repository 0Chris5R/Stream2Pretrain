"""Hugging Face Hub REST poller (CronJob).

Two endpoints:

- ``GET /api/models?sort=lastModified&direction=-1&limit=100`` - new/updated
  models. Dedup by ``(id, lastModified)``.
- ``GET /api/daily_papers?sort=publishedAt&limit=100`` - bearer-only,
  community-curated daily papers.

Politeness: HF Hub anonymous quota is 500 req / 5-min window; with the
optional ``HF_TOKEN`` it bumps to 1000-2500. We poll once per CronJob run so
the budget is comfortable.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import UTC, datetime

from ingest.common.config import IngestConfig, load_config
from ingest.common.hashing import doc_id_for_url
from ingest.common.http_client import build_async_client, build_headers
from ingest.common.kafka_producer import BronzeProducer, LicenseAdmissionProducer
from ingest.common.license_admission import decide_license_admission
from ingest.common.logging import configure_logging, get_logger
from ingest.common.metrics import INGEST_METRICS
from ingest.common.minio_writer import MinioWriter
from ingest.common.otel import init_tracer
from ingest.common.probes import start_probe_server
from ingest.common.s3 import bronze_object_key, bronze_s3_uri
from ingest.common.state import FeedStateStore
from schemas.bronze import BronzeRecord

log = get_logger(__name__)

HF_API_BASE = "https://huggingface.co"
MODELS_ENDPOINT = "/api/models"
DAILY_PAPERS_ENDPOINT = "/api/daily_papers"
SOURCE_FEED_MODELS = "hf-models"
SOURCE_FEED_PAPERS = "hf-daily-papers"


def _trace_id() -> str:
    import secrets

    return secrets.token_hex(16)


async def _emit_payload(
    *,
    payload: bytes,
    url: str,
    source_feed: str,
    extension: str,
    cfg: IngestConfig,
    producer: BronzeProducer,
    minio: MinioWriter,
    extra_meta: dict[str, str] | None = None,
    license_value: str | None = None,
    admission_producer: LicenseAdmissionProducer | None = None,
) -> bool:
    admission = decide_license_admission(
        source_url=url,
        source_feed=source_feed,
        license_value=license_value,
        license_source="dataset_metadata" if license_value else "unknown",
    )
    if admission_producer is not None:
        await admission_producer.send(admission.decision)
    if not admission.admitted:
        return False
    doc_id = doc_id_for_url(url)
    fetched_at = datetime.now(tz=UTC)
    key = bronze_object_key(
        source_feed=source_feed,
        doc_id=doc_id,
        fetched_at=fetched_at,
        extension=extension,
    )
    metadata = {"doc_id": doc_id, "source_feed": source_feed, "url": url}
    if extra_meta:
        metadata.update(extra_meta)
    stored = await minio.put_bronze(
        key=key,
        payload=payload,
        content_type="application/json",
        gzip_compress=True,
        metadata=metadata,
    )
    record = BronzeRecord(
        doc_id=doc_id,
        url=url,  # type: ignore[arg-type]
        fetched_at=fetched_at,
        http_status=200,
        content_type="application/json",
        raw_html_s3_uri=bronze_s3_uri(
            bucket=cfg.minio_bronze_bucket,
            source_feed=source_feed,
            doc_id=doc_id,
            fetched_at=fetched_at,
            extension=extension,
        ),
        source_feed=source_feed,
        trace_id=admission.decision.trace_id,
        bytes_size=stored,
        spdx_license=admission.license_id,
        spdx_license_source="dataset_metadata",
    )
    await producer.send(record)
    return True


async def poll_models(
    cfg: IngestConfig,
    *,
    producer: BronzeProducer,
    minio: MinioWriter,
    admission_producer: LicenseAdmissionProducer | None = None,
    limit: int = 100,
) -> int:
    """Fetch the most recently modified models. Dedup by id+lastModified."""
    state = FeedStateStore(
        "/var/lib/s2p-state/hf_poller" if not cfg.is_dev else "./.s2p-state/hf"
    ).get(SOURCE_FEED_MODELS)
    seen: dict[str, str] = state.get("seen", {})

    headers_extra: dict[str, str] = {}
    if cfg.hf_token:
        headers_extra["Authorization"] = f"Bearer {cfg.hf_token}"
    headers = build_headers(cfg, accept="application/json", extra=headers_extra)

    emitted = 0
    async with build_async_client(cfg, headers=headers) as client:
        params = {"sort": "lastModified", "direction": "-1", "limit": str(limit)}
        resp = await client.get(f"{HF_API_BASE}{MODELS_ENDPOINT}", params=params)
        if resp.status_code >= 400:
            log.warning("hf_models.bad_status", status=resp.status_code)
            INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED_MODELS, outcome="error")
            return 0
        INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED_MODELS, outcome="success")
        items = resp.json()
        if not isinstance(items, list):
            INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED_MODELS, outcome="error")
            return 0
        for item in items:
            if not isinstance(item, dict):
                continue
            model_id = item.get("id") or item.get("modelId")
            last_modified = item.get("lastModified")
            if not isinstance(model_id, str) or not isinstance(last_modified, str):
                continue
            if seen.get(model_id) == last_modified:
                continue
            url = f"https://huggingface.co/{model_id}"
            payload = json.dumps(item, sort_keys=True).encode("utf-8")
            card_data = item.get("cardData") if isinstance(item.get("cardData"), dict) else {}
            license_value = card_data.get("license") or item.get("license")
            try:
                accepted = await _emit_payload(
                    payload=payload,
                    url=url,
                    source_feed=SOURCE_FEED_MODELS,
                    extension="model.json.gz",
                    cfg=cfg,
                    producer=producer,
                    minio=minio,
                    extra_meta={"hf_model_id": model_id, "hf_last_modified": last_modified},
                    license_value=license_value if isinstance(license_value, str) else None,
                    admission_producer=admission_producer,
                )
            except Exception as exc:
                log.warning("hf_models.emit_failed", model=model_id, err=str(exc))
                continue
            if not accepted:
                seen[model_id] = last_modified
                continue
            seen[model_id] = last_modified
            emitted += 1

    # Persist state.
    if len(seen) > 5000:
        items = sorted(seen.items(), key=lambda kv: kv[1], reverse=True)[:2500]
        seen = dict(items)
    FeedStateStore("/var/lib/s2p-state/hf_poller" if not cfg.is_dev else "./.s2p-state/hf").put(
        SOURCE_FEED_MODELS, {"seen": seen}
    )
    return emitted


async def poll_daily_papers(
    cfg: IngestConfig,
    *,
    producer: BronzeProducer,
    minio: MinioWriter,
    admission_producer: LicenseAdmissionProducer | None = None,
    limit: int = 100,
) -> int:
    """HF Daily Papers (bearer required)."""
    if not cfg.hf_token:
        log.warning("hf_daily_papers.skipped_no_token")
        INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED_PAPERS, outcome="skipped")
        return 0
    headers = build_headers(
        cfg,
        accept="application/json",
        extra={"Authorization": f"Bearer {cfg.hf_token}"},
    )
    state_store = FeedStateStore(
        "/var/lib/s2p-state/hf_poller" if not cfg.is_dev else "./.s2p-state/hf"
    )
    state = state_store.get(SOURCE_FEED_PAPERS)
    seen_ids: set[str] = set(state.get("seen_ids", []))

    emitted = 0
    async with build_async_client(cfg, headers=headers) as client:
        params = {"sort": "publishedAt", "limit": str(limit)}
        resp = await client.get(f"{HF_API_BASE}{DAILY_PAPERS_ENDPOINT}", params=params)
        if resp.status_code >= 400:
            log.warning("hf_papers.bad_status", status=resp.status_code)
            INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED_PAPERS, outcome="error")
            return 0
        INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED_PAPERS, outcome="success")
        items = resp.json()
        if not isinstance(items, list):
            INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED_PAPERS, outcome="error")
            return 0
        for item in items:
            if not isinstance(item, dict):
                continue
            paper = item.get("paper") if isinstance(item.get("paper"), dict) else item
            arxiv_id = paper.get("id") if isinstance(paper, dict) else None
            if not isinstance(arxiv_id, str):
                continue
            if arxiv_id in seen_ids:
                continue
            url = f"https://huggingface.co/papers/{arxiv_id}"
            payload = json.dumps(item, sort_keys=True).encode("utf-8")
            try:
                accepted = await _emit_payload(
                    payload=payload,
                    url=url,
                    source_feed=SOURCE_FEED_PAPERS,
                    extension="paper.json.gz",
                    cfg=cfg,
                    producer=producer,
                    minio=minio,
                    extra_meta={"hf_paper_id": arxiv_id},
                    license_value=(
                        paper.get("license")
                        if isinstance(paper, dict) and isinstance(paper.get("license"), str)
                        else None
                    ),
                    admission_producer=admission_producer,
                )
            except Exception as exc:
                log.warning("hf_papers.emit_failed", paper=arxiv_id, err=str(exc))
                continue
            if not accepted:
                seen_ids.add(arxiv_id)
                continue
            seen_ids.add(arxiv_id)
            emitted += 1

    if len(seen_ids) > 5000:
        seen_ids = set(list(seen_ids)[-2500:])
    state_store.put(SOURCE_FEED_PAPERS, {"seen_ids": sorted(seen_ids)})
    return emitted


async def run_pass(cfg: IngestConfig, *, mode: str = "all") -> tuple[int, int]:
    """Run exactly the source selected by the Helm workload.

    The models Deployment is long-lived, while daily papers are a CronJob.
    Keeping selection here prevents either workload from silently polling the
    other source (the old CLI accepted ``--mode`` but ignored it).
    """
    if mode not in {"all", "models", "daily-papers"}:
        raise ValueError(f"unsupported Hugging Face poll mode: {mode}")
    async with (
        BronzeProducer(
            cfg.redpanda_brokers, topic=cfg.raw_topic, client_id="s2p-hf-poller"
        ) as producer,
        LicenseAdmissionProducer(
            cfg.redpanda_brokers,
            topic=cfg.license_admissions_topic,
            client_id="s2p-hf-license-admission",
        ) as admission_producer,
        MinioWriter(
            cfg.minio_endpoint,
            cfg.minio_access_key,
            cfg.minio_secret_key,
            bucket=cfg.minio_bronze_bucket,
        ) as minio,
    ):
        models = 0
        papers = 0
        if mode in {"all", "models"}:
            models = await poll_models(
                cfg, producer=producer, minio=minio, admission_producer=admission_producer
            )
        if mode in {"all", "daily-papers"}:
            papers = await poll_daily_papers(
                cfg, producer=producer, minio=minio, admission_producer=admission_producer
            )
        return models, papers


async def run_models_forever(cfg: IngestConfig, *, poll_interval_seconds: int) -> None:
    """Continuously poll model metadata without turning success into CrashLoopBackOff."""
    interval = max(60, poll_interval_seconds)
    while True:
        try:
            models, _ = await run_pass(cfg, mode="models")
            log.info("hf_poller.pass_done", mode="models", models=models)
        except Exception as exc:
            # A transient upstream/Kafka/MinIO failure must not terminate a
            # continuously supervised Deployment. The next bounded pass retries.
            log.exception("hf_poller.pass_failed", mode="models", err=str(exc))
        await asyncio.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="Poll Hugging Face metadata")
    parser.add_argument("--mode", choices=("all", "models", "daily-papers"), default="all")
    # Retained for chart/CLI compatibility; feed configuration is supplied by
    # validated Helm values and IngestConfig environment variables today.
    parser.add_argument("--config", default=None)
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=int(os.environ.get("S2P_HF_POLL_INTERVAL_SECONDS", "600")),
    )
    args = parser.parse_args()
    cfg = load_config()
    configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    init_tracer("ingest.hf_poller", cfg)
    log.info("hf_poller.start", mode=args.mode)
    start_probe_server()
    if args.mode == "models":
        asyncio.run(run_models_forever(cfg, poll_interval_seconds=args.poll_interval_seconds))
        return
    models, papers = asyncio.run(run_pass(cfg, mode=args.mode))
    log.info("hf_poller.done", models=models, papers=papers)


if __name__ == "__main__":
    main()
