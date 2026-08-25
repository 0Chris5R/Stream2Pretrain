"""Hugging Face Hub model-card and dataset-card poller.

The list responses are internal discovery metadata. Model and dataset prose is
retrieved only from ``README.md`` at the exact commit SHA returned by the Hub
API and only after the card-level licence decision has been published.

Politeness: HF Hub anonymous quota is 500 req / 5-min window; with the
optional ``HF_TOKEN`` it bumps to 1000-2500. We poll once per CronJob run so
the budget is comfortable.
"""

from __future__ import annotations

import argparse
import asyncio
import os
from datetime import UTC, datetime

from ingest.common.config import IngestConfig, load_config
from ingest.common.hashing import doc_id_for_url
from ingest.common.http_client import build_async_client, build_headers
from ingest.common.kafka_producer import BronzeProducer, LicenseAdmissionProducer
from ingest.common.license_admission import (
    AdmissionResult,
    decide_license_admission,
)
from ingest.common.logging import configure_logging, get_logger
from ingest.common.metrics import INGEST_METRICS
from ingest.common.minio_writer import MinioWriter
from ingest.common.otel import init_tracer
from ingest.common.probes import start_probe_server
from ingest.common.s3 import bronze_object_key, bronze_s3_uri
from ingest.common.state import FeedStateStore
from schemas.bronze import BronzeRecord, SourceFormat

log = get_logger(__name__)

HF_API_BASE = "https://huggingface.co"
HF_PUBLIC_REPOSITORY_TERMS = "HF-Public-Repository-Terms-2022-09-15"
HF_TERMS_URL = "https://huggingface.co/terms-of-service"
MODELS_ENDPOINT = "/api/models"
DATASETS_ENDPOINT = "/api/datasets"
SOURCE_FEED_MODELS = "hf-models"
SOURCE_FEED_DATASETS = "hf-datasets"


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
    license_source: str = "dataset_metadata",
    admission_producer: LicenseAdmissionProducer | None = None,
    source_format: SourceFormat = "metadata",
    content_type: str = "application/json",
    extraction_pipeline: str = "hf-api-json-v1",
    resolver: str | None = None,
    evidence_url: str | None = None,
    evidence_revision: str | None = None,
    evidence_scope: str | None = None,
    admission_override: AdmissionResult | None = None,
) -> bool:
    admission = admission_override or decide_license_admission(
        source_url=url,
        source_feed=source_feed,
        license_value=license_value,
        license_source=license_source if license_value else "unknown",
        source_format=source_format,
        resolver=resolver,
        evidence_url=evidence_url,
        evidence_revision=evidence_revision,
        evidence_scope=evidence_scope,
    )
    if admission_override is None and admission_producer is None:
        raise RuntimeError("licence admission producer is required before HF payload storage")
    if admission_producer is not None and admission_override is None:
        await admission_producer.send(admission.decision)
    if not admission.fetch_allowed:
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
        content_type=content_type,
        gzip_compress=True,
        metadata=metadata,
    )
    record = BronzeRecord(
        doc_id=doc_id,
        url=url,  # type: ignore[arg-type]
        fetched_at=fetched_at,
        http_status=200,
        content_type=content_type,
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
        source_format=source_format,  # type: ignore[arg-type]
        extraction_pipeline=extraction_pipeline,
        spdx_license=admission.license_id,
        spdx_license_source=license_source if license_value else "unknown",  # type: ignore[arg-type]
        training_usage=admission.training_usage,
    )
    await producer.send(record)
    return True


async def poll_models(
    cfg: IngestConfig,
    *,
    producer: BronzeProducer,
    minio: MinioWriter,
    admission_producer: LicenseAdmissionProducer,
    limit: int = 100,
) -> int:
    """Fetch versioned, licensed model-card prose rather than list API JSON."""
    state_store = FeedStateStore(
        "/var/lib/s2p-state/hf_poller" if not cfg.is_dev else "./.s2p-state/hf"
    )
    seen: dict[str, str] = state_store.get(SOURCE_FEED_MODELS).get("seen", {})
    headers_extra = {"Authorization": f"Bearer {cfg.hf_token}"} if cfg.hf_token else {}
    headers = build_headers(cfg, accept="application/json", extra=headers_extra)

    emitted = 0
    emit_failures: list[str] = []
    async with build_async_client(cfg, headers=headers) as client:
        response = await client.get(
            f"{HF_API_BASE}{MODELS_ENDPOINT}",
            params={
                "sort": "lastModified",
                "direction": "-1",
                "limit": str(limit),
                "full": "true",
                "cardData": "true",
            },
        )
        if response.status_code >= 400:
            INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED_MODELS, outcome="error")
            response.raise_for_status()
        items = response.json()
        if not isinstance(items, list):
            INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED_MODELS, outcome="error")
            raise ValueError("Hugging Face models response must be a JSON list")
        INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED_MODELS, outcome="success")

        for item in items[:limit]:
            if not isinstance(item, dict):
                continue
            model_id = item.get("id") or item.get("modelId")
            last_modified = item.get("lastModified")
            if not isinstance(model_id, str) or not isinstance(last_modified, str):
                continue
            if seen.get(model_id) == last_modified:
                continue
            revision = item.get("sha")
            revision_value = revision if isinstance(revision, str) and revision else None
            # The catalogue response is discovery metadata. A mutable or
            # unresolved branch is not a corpus item and must not create an
            # admission row or a README fetch.
            if revision_value is None or item.get("private") is True:
                continue
            revision_path = revision_value
            card_url = f"https://huggingface.co/{model_id}/blob/{revision_path}/README.md"
            # Only README prose enters this source. The model artefact licence
            # does not license that prose and must not control its route.
            admission = decide_license_admission(
                source_url=card_url,
                source_feed=SOURCE_FEED_MODELS,
                license_value=HF_PUBLIC_REPOSITORY_TERMS,
                license_source="source_terms",
                source_format="web",
                resolver="hf-public-repository-terms",
                evidence_url=HF_TERMS_URL,
                evidence_revision=revision_value,
                evidence_scope="source_terms",
            )
            if admission_producer is not None:
                await admission_producer.send(admission.decision)
            if not admission.fetch_allowed:
                # Retry unresolved catalogue rows: a later response may add
                # the immutable SHA without changing ``lastModified``.
                if revision_value is not None:
                    seen[model_id] = last_modified
                continue
            card_response = await client.get(
                f"https://huggingface.co/{model_id}/resolve/{revision_path}/README.md"
            )
            if card_response.status_code >= 400:
                log.warning(
                    "hf_models.card_fetch_failed",
                    model=model_id,
                    revision=revision_value,
                    status=card_response.status_code,
                )
                emit_failures.append(model_id)
                continue
            try:
                await _emit_payload(
                    payload=card_response.content,
                    url=card_url,
                    source_feed=SOURCE_FEED_MODELS,
                    extension="README.md.gz",
                    cfg=cfg,
                    producer=producer,
                    minio=minio,
                    extra_meta={
                        "hf_model_id": model_id,
                        "hf_last_modified": last_modified,
                        "hf_revision": revision_value or "unresolved",
                    },
                    license_value=HF_PUBLIC_REPOSITORY_TERMS,
                    license_source="source_terms",
                    source_format="web",
                    content_type="text/markdown; charset=utf-8",
                    extraction_pipeline="hf-model-card-markdown-v1",
                    admission_override=admission,
                )
            except Exception as exc:
                log.warning("hf_models.emit_failed", model=model_id, err=str(exc))
                emit_failures.append(model_id)
                continue
            seen[model_id] = last_modified
            emitted += 1

    if len(seen) > 5000:
        seen = dict(sorted(seen.items(), key=lambda pair: pair[1], reverse=True)[:2500])
    state_store.put(SOURCE_FEED_MODELS, {"seen": seen})
    if emit_failures:
        INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED_MODELS, outcome="error")
        raise RuntimeError(
            f"incomplete Hugging Face model-card pass: emit_failures={len(emit_failures)}"
        )
    return emitted


async def poll_hub_cards(
    cfg: IngestConfig,
    *,
    kind: str,
    producer: BronzeProducer,
    minio: MinioWriter,
    admission_producer: LicenseAdmissionProducer,
    limit: int = 100,
) -> int:
    """Fetch immutable dataset-card prose.

    The list response is discovery metadata only. The emitted corpus item is
    the README at the exact Hub commit returned by the API.
    """
    if kind != "dataset":
        raise ValueError("only Hugging Face dataset cards are an active source")
    endpoint = DATASETS_ENDPOINT
    source_feed = SOURCE_FEED_DATASETS
    route_prefix = "datasets"
    pipeline = "hf-dataset-card-markdown-v1"
    state_store = FeedStateStore(
        "/var/lib/s2p-state/hf_poller" if not cfg.is_dev else "./.s2p-state/hf"
    )
    seen: dict[str, str] = state_store.get(source_feed).get("seen", {})
    headers_extra = {"Authorization": f"Bearer {cfg.hf_token}"} if cfg.hf_token else {}
    headers = build_headers(cfg, accept="application/json", extra=headers_extra)

    emitted = 0
    failures: list[str] = []
    async with build_async_client(cfg, headers=headers) as client:
        response = await client.get(
            f"{HF_API_BASE}{endpoint}",
            params={
                "sort": "lastModified",
                "direction": "-1",
                "limit": str(limit),
                "full": "true",
            },
        )
        if response.status_code >= 400:
            INGEST_METRICS.record_feed_poll(source_feed=source_feed, outcome="error")
            response.raise_for_status()
        items = response.json()
        if not isinstance(items, list):
            INGEST_METRICS.record_feed_poll(source_feed=source_feed, outcome="error")
            raise ValueError(f"Hugging Face {kind} response must be a JSON list")
        INGEST_METRICS.record_feed_poll(source_feed=source_feed, outcome="success")

        for item in items[:limit]:
            if not isinstance(item, dict):
                continue
            repo_id = item.get("id")
            last_modified = item.get("lastModified")
            revision = item.get("sha")
            if not all(
                isinstance(value, str) and value for value in (repo_id, last_modified, revision)
            ):
                # Unresolved catalogue metadata is not a content item.
                continue
            assert isinstance(repo_id, str)
            assert isinstance(last_modified, str)
            assert isinstance(revision, str)
            if seen.get(repo_id) == last_modified:
                continue
            if item.get("private") is True:
                continue
            card_url = f"https://huggingface.co/{route_prefix}/{repo_id}/blob/{revision}/README.md"
            # Dataset rows and files are outside this source. Only the exact
            # README revision is admitted under the Hub repository terms.
            admission = decide_license_admission(
                source_url=card_url,
                source_feed=source_feed,
                license_value=HF_PUBLIC_REPOSITORY_TERMS,
                license_source="source_terms",
                source_format="web",
                resolver="hf-public-repository-terms",
                evidence_url=HF_TERMS_URL,
                evidence_revision=revision,
                evidence_scope="source_terms",
            )
            if admission_producer is not None:
                await admission_producer.send(admission.decision)
            if not admission.fetch_allowed:
                seen[repo_id] = last_modified
                continue
            card_response = await client.get(
                f"https://huggingface.co/{route_prefix}/{repo_id}/resolve/{revision}/README.md"
            )
            if card_response.status_code >= 400:
                log.warning(
                    "hf_cards.card_fetch_failed",
                    kind=kind,
                    repo=repo_id,
                    revision=revision,
                    status=card_response.status_code,
                )
                failures.append(repo_id)
                continue
            try:
                await _emit_payload(
                    payload=card_response.content,
                    url=card_url,
                    source_feed=source_feed,
                    extension="README.md.gz",
                    cfg=cfg,
                    producer=producer,
                    minio=minio,
                    extra_meta={
                        f"hf_{kind}_id": repo_id,
                        "hf_last_modified": last_modified,
                        "hf_revision": revision,
                    },
                    license_value=HF_PUBLIC_REPOSITORY_TERMS,
                    license_source="source_terms",
                    source_format="web",
                    content_type="text/markdown; charset=utf-8",
                    extraction_pipeline=pipeline,
                    admission_override=admission,
                )
            except Exception as exc:
                log.warning("hf_cards.emit_failed", kind=kind, repo=repo_id, err=str(exc))
                failures.append(repo_id)
                continue
            seen[repo_id] = last_modified
            emitted += 1

    if len(seen) > 5000:
        seen = dict(sorted(seen.items(), key=lambda pair: pair[1], reverse=True)[:2500])
    state_store.put(source_feed, {"seen": seen})
    if failures:
        INGEST_METRICS.record_feed_poll(source_feed=source_feed, outcome="error")
        raise RuntimeError(f"incomplete Hugging Face {kind}-card pass: failures={len(failures)}")
    return emitted


async def run_pass(cfg: IngestConfig, *, mode: str = "all") -> tuple[int, int]:
    """Run exactly the source selected by the Helm workload.

    The Hub-card Deployment is long-lived. Keeping selection here prevents a
    workload from silently polling another source.
    """
    if mode not in {"all", "hub-cards", "models", "datasets"}:
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
        datasets = 0
        if mode in {"all", "hub-cards", "models"}:
            models = await poll_models(
                cfg, producer=producer, minio=minio, admission_producer=admission_producer
            )
        if mode in {"all", "hub-cards", "datasets"}:
            datasets = await poll_hub_cards(
                cfg,
                kind="dataset",
                producer=producer,
                minio=minio,
                admission_producer=admission_producer,
            )
        return models, datasets


async def run_hub_cards_forever(cfg: IngestConfig, *, poll_interval_seconds: int) -> None:
    """Continuously poll licensed Hub cards without turning success into CrashLoopBackOff."""
    interval = max(60, poll_interval_seconds)
    while True:
        try:
            models, datasets = await run_pass(cfg, mode="hub-cards")
            log.info(
                "hf_poller.pass_done",
                mode="hub-cards",
                models=models,
                datasets=datasets,
            )
        except Exception as exc:
            # A transient upstream/Kafka/MinIO failure must not terminate a
            # continuously supervised Deployment. The next bounded pass retries.
            log.exception("hf_poller.pass_failed", mode="hub-cards", err=str(exc))
        await asyncio.sleep(interval)


async def run_models_forever(cfg: IngestConfig, *, poll_interval_seconds: int) -> None:
    """Compatibility wrapper for the pre-card-catalog command name."""
    interval = max(60, poll_interval_seconds)
    while True:
        try:
            models, _ = await run_pass(cfg, mode="models")
            log.info("hf_poller.pass_done", mode="models", models=models)
        except Exception as exc:
            log.exception("hf_poller.pass_failed", mode="models", err=str(exc))
        await asyncio.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="Poll licensed Hugging Face card prose")
    parser.add_argument(
        "--mode",
        choices=("all", "hub-cards", "models", "datasets"),
        default="all",
    )
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
    if args.mode == "hub-cards":
        asyncio.run(run_hub_cards_forever(cfg, poll_interval_seconds=args.poll_interval_seconds))
        return
    if args.mode == "models":
        asyncio.run(run_models_forever(cfg, poll_interval_seconds=args.poll_interval_seconds))
        return
    models, datasets = asyncio.run(run_pass(cfg, mode=args.mode))
    log.info(
        "hf_poller.done",
        models=models,
        datasets=datasets,
    )


if __name__ == "__main__":
    main()
