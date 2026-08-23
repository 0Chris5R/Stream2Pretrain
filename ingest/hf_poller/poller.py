"""Hugging Face Hub REST poller (CronJob).

Four Hub catalogue endpoints:

- ``GET /api/models?sort=lastModified&direction=-1&limit=100`` - new/updated
  models. Dedup by ``(id, lastModified)``.
- ``GET /api/daily_papers?sort=publishedAt&limit=100`` - community-curated
  daily papers. Authentication is optional.

The list responses are discovery metadata. Model, dataset, and Space prose is
retrieved only from ``README.md`` at the exact commit SHA returned by the Hub
API and only after the card-level licence decision has been published.

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
from typing import Literal

from ingest.common.arxiv_license import fetch_arxiv_license_with_source
from ingest.common.config import IngestConfig, load_config
from ingest.common.hashing import doc_id_for_url
from ingest.common.http_client import build_async_client, build_headers
from ingest.common.kafka_producer import BronzeProducer, LicenseAdmissionProducer
from ingest.common.license_admission import (
    AdmissionResult,
    decide_license_admission,
    normalize_license,
)
from ingest.common.logging import configure_logging, get_logger
from ingest.common.metrics import INGEST_METRICS
from ingest.common.minio_writer import MinioWriter
from ingest.common.otel import init_tracer
from ingest.common.probes import start_probe_server
from ingest.common.rate_limit import TokenBucket
from ingest.common.s3 import bronze_object_key, bronze_s3_uri
from ingest.common.state import FeedStateStore
from schemas.bronze import BronzeRecord

log = get_logger(__name__)

HF_API_BASE = "https://huggingface.co"
MODELS_ENDPOINT = "/api/models"
DATASETS_ENDPOINT = "/api/datasets"
SPACES_ENDPOINT = "/api/spaces"
DAILY_PAPERS_ENDPOINT = "/api/daily_papers"
SOURCE_FEED_MODELS = "hf-models"
SOURCE_FEED_DATASETS = "hf-datasets"
SOURCE_FEED_SPACES = "hf-spaces"
SOURCE_FEED_PAPERS = "hf-daily-papers"

HubCardKind = Literal["dataset", "space"]


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
    source_format: str = "metadata",
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
            revision_path = revision_value or "unresolved"
            card_url = f"https://huggingface.co/{model_id}/blob/{revision_path}/README.md"
            license_value = _model_license(item)
            exact_license = license_value if revision_value is not None else None
            admission = decide_license_admission(
                source_url=card_url,
                source_feed=SOURCE_FEED_MODELS,
                license_value=exact_license,
                license_source="hf_card" if exact_license else "unknown",
                source_format="web",
                resolver="hf-model-card-metadata",
                evidence_url=card_url,
                evidence_revision=revision_value,
                evidence_scope="item" if revision_value and exact_license else "unknown",
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
                    license_value=exact_license,
                    license_source="hf_card",
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
    kind: HubCardKind,
    producer: BronzeProducer,
    minio: MinioWriter,
    admission_producer: LicenseAdmissionProducer,
    limit: int = 100,
) -> int:
    """Fetch immutable, licensed dataset or Space card prose.

    The list response is discovery metadata only. The emitted corpus item is
    the README at the exact Hub commit returned by the API.
    """
    endpoint = DATASETS_ENDPOINT if kind == "dataset" else SPACES_ENDPOINT
    source_feed = SOURCE_FEED_DATASETS if kind == "dataset" else SOURCE_FEED_SPACES
    route_prefix = "datasets" if kind == "dataset" else "spaces"
    pipeline = f"hf-{kind}-card-markdown-v1"
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
                # An unresolved branch is mutable and cannot meet the corpus
                # provenance contract. Record the fail-closed outcome before
                # waiting for a later API response with an immutable SHA.
                if isinstance(repo_id, str) and repo_id and isinstance(last_modified, str):
                    unresolved_url = (
                        f"https://huggingface.co/{route_prefix}/{repo_id}/blob/unresolved/README.md"
                    )
                    unresolved = decide_license_admission(
                        source_url=unresolved_url,
                        source_feed=source_feed,
                        license_value=None,
                        license_source="unknown",
                        source_format="web",
                        resolver=f"hf-{kind}-card-metadata",
                        evidence_url=unresolved_url,
                        evidence_revision=None,
                        evidence_scope="unknown",
                    )
                    if admission_producer is not None:
                        await admission_producer.send(unresolved.decision)
                continue
            assert isinstance(repo_id, str)
            assert isinstance(last_modified, str)
            assert isinstance(revision, str)
            if seen.get(repo_id) == last_modified:
                continue
            card_url = f"https://huggingface.co/{route_prefix}/{repo_id}/blob/{revision}/README.md"
            license_value = _model_license(item)
            admission = decide_license_admission(
                source_url=card_url,
                source_feed=source_feed,
                license_value=license_value,
                license_source="hf_card" if license_value else "unknown",
                source_format="web",
                resolver=f"hf-{kind}-card-metadata",
                evidence_url=card_url,
                evidence_revision=revision,
                evidence_scope="item" if license_value else "unknown",
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
                    license_value=license_value,
                    license_source="hf_card",
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


async def poll_daily_papers(
    cfg: IngestConfig,
    *,
    producer: BronzeProducer,
    minio: MinioWriter,
    admission_producer: LicenseAdmissionProducer,
    limit: int = 100,
) -> int:
    """Poll HF Daily Papers and resolve each paper's arXiv license."""
    extra_headers = {"Authorization": f"Bearer {cfg.hf_token}"} if cfg.hf_token else None
    headers = build_headers(cfg, accept="application/json", extra=extra_headers)
    state_store = FeedStateStore(
        "/var/lib/s2p-state/hf_poller" if not cfg.is_dev else "./.s2p-state/hf"
    )
    state = state_store.get(SOURCE_FEED_PAPERS)
    seen_ids: set[str] = set(state.get("seen_ids", []))

    emitted = 0
    license_bucket = TokenBucket(rate=1.0, burst=1)
    async with build_async_client(cfg, headers=headers) as client:
        params = {"sort": "publishedAt", "limit": str(limit)}
        resp = await client.get(f"{HF_API_BASE}{DAILY_PAPERS_ENDPOINT}", params=params)
        if resp.status_code >= 400:
            log.warning("hf_papers.bad_status", status=resp.status_code)
            INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED_PAPERS, outcome="error")
            resp.raise_for_status()
        INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED_PAPERS, outcome="success")
        items = resp.json()
        if not isinstance(items, list):
            INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED_PAPERS, outcome="error")
            raise ValueError("Hugging Face daily papers response must be a JSON list")
        emit_failures: list[str] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            paper = item.get("paper") if isinstance(item.get("paper"), dict) else item
            arxiv_id = paper.get("id") if isinstance(paper, dict) else None
            if not isinstance(arxiv_id, str):
                continue
            if arxiv_id in seen_ids:
                continue
            url = f"https://arxiv.org/abs/{arxiv_id}"
            payload = json.dumps(item, sort_keys=True).encode("utf-8")
            license_value = (
                paper.get("license")
                if isinstance(paper, dict) and isinstance(paper.get("license"), str)
                else None
            )
            license_source = "dataset_metadata"
            resolver = "hf-daily-paper-item-field"
            evidence_url = f"https://huggingface.co/papers/{arxiv_id}"
            if normalize_license(license_value) == "unknown":
                license_value, license_source = await fetch_arxiv_license_with_source(
                    arxiv_id,
                    client,
                    bucket=license_bucket,
                )
                resolver = f"arxiv:{license_source}"
                evidence_url = f"https://arxiv.org/abs/{arxiv_id}"
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
                    license_value=license_value,
                    license_source=license_source,
                    admission_producer=admission_producer,
                    source_format="metadata",
                    resolver=resolver,
                    evidence_url=evidence_url,
                    evidence_revision=arxiv_id,
                    evidence_scope="item",
                )
            except Exception as exc:
                log.warning("hf_papers.emit_failed", paper=arxiv_id, err=str(exc))
                emit_failures.append(arxiv_id)
                continue
            if not accepted:
                seen_ids.add(arxiv_id)
                continue
            seen_ids.add(arxiv_id)
            emitted += 1

    if len(seen_ids) > 5000:
        seen_ids = set(list(seen_ids)[-2500:])
    state_store.put(SOURCE_FEED_PAPERS, {"seen_ids": sorted(seen_ids)})
    if emit_failures:
        INGEST_METRICS.record_feed_poll(source_feed=SOURCE_FEED_PAPERS, outcome="error")
        raise RuntimeError(f"failed to emit {len(emit_failures)} Hugging Face daily papers")
    return emitted


def _model_license(item: dict[str, object]) -> str | None:
    """Read the license from every shape returned by the Hub list API."""
    card_data = item.get("cardData")
    if isinstance(card_data, dict) and isinstance(card_data.get("license"), str):
        return normalize_license(card_data["license"])
    direct = item.get("license")
    if isinstance(direct, str):
        return normalize_license(direct)
    tags = item.get("tags")
    if isinstance(tags, list):
        for tag in tags:
            if isinstance(tag, str) and tag.lower().startswith("license:"):
                return normalize_license(tag.split(":", 1)[1])
    return None


async def run_pass(cfg: IngestConfig, *, mode: str = "all") -> tuple[int, int, int, int]:
    """Run exactly the source selected by the Helm workload.

    The Hub-card Deployment is long-lived, while daily papers are a CronJob.
    Keeping selection here prevents either workload from silently polling the
    other source (the old CLI accepted ``--mode`` but ignored it).
    """
    if mode not in {"all", "hub-cards", "models", "datasets", "spaces", "daily-papers"}:
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
        spaces = 0
        papers = 0
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
        if mode in {"all", "hub-cards", "spaces"}:
            spaces = await poll_hub_cards(
                cfg,
                kind="space",
                producer=producer,
                minio=minio,
                admission_producer=admission_producer,
            )
        if mode in {"all", "daily-papers"}:
            papers = await poll_daily_papers(
                cfg, producer=producer, minio=minio, admission_producer=admission_producer
            )
        return models, datasets, spaces, papers


async def run_hub_cards_forever(cfg: IngestConfig, *, poll_interval_seconds: int) -> None:
    """Continuously poll licensed Hub cards without turning success into CrashLoopBackOff."""
    interval = max(60, poll_interval_seconds)
    while True:
        try:
            models, datasets, spaces, _ = await run_pass(cfg, mode="hub-cards")
            log.info(
                "hf_poller.pass_done",
                mode="hub-cards",
                models=models,
                datasets=datasets,
                spaces=spaces,
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
            models, _, _, _ = await run_pass(cfg, mode="models")
            log.info("hf_poller.pass_done", mode="models", models=models)
        except Exception as exc:
            log.exception("hf_poller.pass_failed", mode="models", err=str(exc))
        await asyncio.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="Poll licensed Hugging Face card prose")
    parser.add_argument(
        "--mode",
        choices=("all", "hub-cards", "models", "datasets", "spaces", "daily-papers"),
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
    models, datasets, spaces, papers = asyncio.run(run_pass(cfg, mode=args.mode))
    log.info(
        "hf_poller.done",
        models=models,
        datasets=datasets,
        spaces=spaces,
        papers=papers,
    )


if __name__ == "__main__":
    main()
