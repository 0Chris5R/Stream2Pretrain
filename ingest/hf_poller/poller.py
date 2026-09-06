"""Hugging Face Hub model-card and dataset-card poller.

The list responses are internal discovery metadata. Model and dataset prose is
retrieved only from ``README.md`` at the exact commit SHA returned by the Hub
API and only after the card-level licence decision has been published. Corpus
revision identity is the immutable README Git blob, not the whole-repository
commit, so weight or dataset-file updates do not duplicate unchanged prose.

Politeness: HF Hub anonymous quota is 500 req / 5-min window; with the
optional ``HF_TOKEN`` it bumps to 1000-2500. Link pagination resumes from a
durable completed watermark; incomplete scans never advance that watermark.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from ingest.common.config import IngestConfig, load_config
from ingest.common.hashing import content_sha256, doc_id_for_url
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
HF_SCAN_STATE_VERSION = 2
_INITIAL_PAGE = "__initial__"
_HEX_OBJECT_ID = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
_UTC_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _root_readme_in_siblings(item: dict[str, Any]) -> bool | None:
    """Return README presence when the Hub listing included a file inventory."""
    siblings = item.get("siblings")
    if not isinstance(siblings, list):
        return None
    return any(
        isinstance(sibling, dict) and sibling.get("rfilename") == "README.md"
        for sibling in siblings
    )


def _readme_resolve_url(*, route_prefix: str, repo_id: str, revision: str) -> str:
    route = f"{route_prefix}/" if route_prefix else ""
    return f"{HF_API_BASE}/{route}{repo_id}/resolve/{revision}/README.md"


async def _root_readme_blob(
    client: httpx.AsyncClient,
    *,
    route_prefix: str,
    repo_id: str,
    revision: str,
    item: dict[str, Any],
) -> str | None:
    """Resolve the immutable root-README blob id without fetching its body.

    A HEAD request to the exact repository revision exposes the immutable Git
    blob in ETag/X-Linked-ETag. This uses the Hub's resolver quota rather than
    spending list-API quota on one tree request per repository. When a model
    listing supplies a complete sibling inventory without README.md, no
    resolver request is necessary.
    """
    listed = _root_readme_in_siblings(item)
    if listed is False:
        return None

    response = await client.head(
        _readme_resolve_url(
            route_prefix=route_prefix,
            repo_id=repo_id,
            revision=revision,
        )
    )
    if response.status_code in {401, 403, 404}:
        return None
    response.raise_for_status()
    raw_etag = response.headers.get("x-linked-etag") or response.headers.get("etag")
    blob = (raw_etag or "").strip().removeprefix("W/").strip('"').lower()
    if not _HEX_OBJECT_ID.fullmatch(blob):
        raise ValueError(
            f"Hugging Face README response did not expose an immutable blob id for {repo_id}"
        )
    return blob


@dataclass(frozen=True, slots=True)
class _HubSource:
    kind: str
    endpoint: str
    source_feed: str
    route_prefix: str
    extraction_pipeline: str
    id_fields: tuple[str, ...]
    list_params: dict[str, str]


@dataclass(frozen=True, slots=True)
class _CatalogueItem:
    repo_id: str
    last_modified: datetime
    last_modified_raw: str
    revision: str | None
    raw: dict[str, Any]

    @property
    def key(self) -> str:
        return json.dumps(
            [self.repo_id, self.revision or ""],
            ensure_ascii=True,
            separators=(",", ":"),
        )


MODEL_SOURCE = _HubSource(
    kind="model",
    endpoint=MODELS_ENDPOINT,
    source_feed=SOURCE_FEED_MODELS,
    route_prefix="",
    extraction_pipeline="hf-model-card-markdown-v2",
    id_fields=("id", "modelId"),
    list_params={"full": "true", "cardData": "true"},
)
DATASET_SOURCE = _HubSource(
    kind="dataset",
    endpoint=DATASETS_ENDPOINT,
    source_feed=SOURCE_FEED_DATASETS,
    route_prefix="datasets",
    extraction_pipeline="hf-dataset-card-markdown-v2",
    id_fields=("id",),
    list_params={"full": "true"},
)


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Hugging Face lastModified must include a timezone")
    return parsed.astimezone(UTC)


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _catalogue_item(raw: object, source: _HubSource) -> _CatalogueItem | None:
    if not isinstance(raw, dict):
        return None
    repo_id = next(
        (
            value
            for field in source.id_fields
            if isinstance((value := raw.get(field)), str) and value
        ),
        None,
    )
    last_modified = raw.get("lastModified")
    if not isinstance(repo_id, str) or not isinstance(last_modified, str):
        return None
    try:
        parsed = _parse_timestamp(last_modified)
    except ValueError:
        return None
    revision = raw.get("sha")
    return _CatalogueItem(
        repo_id=repo_id,
        last_modified=parsed,
        last_modified_raw=last_modified,
        revision=revision if isinstance(revision, str) and revision else None,
        raw=raw,
    )


def _catalogue_order(item: _CatalogueItem) -> tuple[int, str, str]:
    # Descending time, then stable source identity for deterministic ties.
    delta = item.last_modified - _UTC_EPOCH
    micros = ((delta.days * 86_400) + delta.seconds) * 1_000_000 + delta.microseconds
    return (-micros, item.repo_id, item.revision or "")


def _readme_state_key(source_feed: str, repo_id: str, blob_oid: str) -> str:
    digest = hashlib.sha256(repo_id.encode("utf-8")).hexdigest()
    return f"{source_feed}.readme.{digest}.{blob_oid}"


def _readme_doc_id(source_feed: str, repo_id: str, blob_oid: str) -> str:
    identity = f"hf-readme-v1\0{source_feed}\0{repo_id}\0README.md\0{blob_oid}"
    return f"sha256:{hashlib.sha256(identity.encode('utf-8')).hexdigest()}"


def _git_blob_oid(payload: bytes, *, algorithm: str) -> str:
    framed = f"blob {len(payload)}\0".encode("ascii") + payload
    return hashlib.new(algorithm, framed).hexdigest()


def _verify_blob(payload: bytes, blob_oid: str) -> None:
    if len(blob_oid) == 40:
        candidates = {_git_blob_oid(payload, algorithm="sha1")}
        algorithm = "Git SHA-1"
    else:
        # HF exposes raw SHA-256 for LFS/Xet objects. A future SHA-256 Git
        # object id is framed, so accept either exact immutable convention.
        candidates = {
            hashlib.sha256(payload).hexdigest(),
            _git_blob_oid(payload, algorithm="sha256"),
        }
        algorithm = "SHA-256"
    if blob_oid not in candidates:
        raise ValueError(
            f"Hugging Face README body does not match advertised {algorithm} blob {blob_oid}"
        )


def _legacy_watermark(raw_state: dict[str, Any]) -> dict[str, Any] | None:
    seen = raw_state.get("seen")
    if not isinstance(seen, dict):
        return None
    valid = {
        repo_id: timestamp
        for repo_id, timestamp in seen.items()
        if isinstance(repo_id, str) and isinstance(timestamp, str)
    }
    parsed: list[tuple[str, datetime]] = []
    for repo_id, timestamp in valid.items():
        try:
            parsed.append((repo_id, _parse_timestamp(timestamp)))
        except ValueError:
            continue
    if not parsed:
        return None
    newest = max(timestamp for _, timestamp in parsed)
    return {
        "last_modified": _format_timestamp(newest),
        "catalogue_revisions": [],
        "legacy_repositories": sorted(
            repo_id for repo_id, timestamp in parsed if timestamp == newest
        ),
    }


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
    document_id: str | None = None,
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
    doc_id = document_id or doc_id_for_url(url)
    if admission.decision.doc_id != doc_id:
        raise ValueError("licence admission and emitted HF document identities must match")
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


def _new_scan_state(raw_state: dict[str, Any]) -> dict[str, Any]:
    if raw_state.get("version") == HF_SCAN_STATE_VERSION:
        scan = raw_state.get("scan")
        if scan is not None:
            if not isinstance(scan, dict) or not isinstance(scan.get("request_url"), str):
                raise ValueError("invalid durable Hugging Face scan state")
            return raw_state
        completed = raw_state.get("completed")
        if completed is not None and not isinstance(completed, dict):
            raise ValueError("invalid completed Hugging Face watermark")
    else:
        completed = _legacy_watermark(raw_state)

    return {
        "version": HF_SCAN_STATE_VERSION,
        "completed": completed,
        "scan": {
            "boundary": completed,
            "bootstrap": completed is None,
            "request_url": _INITIAL_PAGE,
            "processed_catalogue_revisions": [],
            "head_last_modified": None,
            "head_catalogue_revisions": [],
            "pages_completed": 0,
        },
    }


def _next_page_url(response: httpx.Response) -> str | None:
    next_link = response.links.get("next")
    if not isinstance(next_link, dict) or not isinstance(next_link.get("url"), str):
        return None
    url = response.request.url.join(next_link["url"])
    if url.scheme != "https" or url.host != "huggingface.co":
        raise ValueError("Hugging Face pagination link changed origin")
    return str(url)


def _boundary_relation(item: _CatalogueItem, boundary: dict[str, Any] | None) -> int:
    """Return 1 for new, 0 for an already completed tie, and -1 for older."""
    if boundary is None:
        return 1
    raw_timestamp = boundary.get("last_modified")
    if not isinstance(raw_timestamp, str):
        raise ValueError("completed Hugging Face watermark lacks last_modified")
    boundary_time = _parse_timestamp(raw_timestamp)
    if item.last_modified > boundary_time:
        return 1
    if item.last_modified < boundary_time:
        return -1
    exact = boundary.get("catalogue_revisions", [])
    legacy = boundary.get("legacy_repositories", [])
    if item.key in exact or item.repo_id in legacy:
        return 0
    return 1


def _update_scan_head(scan: dict[str, Any], items: list[_CatalogueItem]) -> None:
    current_raw = scan.get("head_last_modified")
    current = _parse_timestamp(current_raw) if isinstance(current_raw, str) else None
    ties = {value for value in scan.get("head_catalogue_revisions", []) if isinstance(value, str)}
    for item in items:
        if current is None or item.last_modified > current:
            current = item.last_modified
            ties = {item.key}
        elif item.last_modified == current:
            ties.add(item.key)
    scan["head_last_modified"] = _format_timestamp(current) if current else None
    scan["head_catalogue_revisions"] = sorted(ties)


async def _process_readme_revision(
    *,
    cfg: IngestConfig,
    client: httpx.AsyncClient,
    source: _HubSource,
    item: _CatalogueItem,
    state_store: FeedStateStore,
    producer: BronzeProducer,
    minio: MinioWriter,
    admission_producer: LicenseAdmissionProducer,
) -> bool:
    revision = item.revision
    if revision is None or item.raw.get("private") is True:
        return False

    blob_oid = await _root_readme_blob(
        client,
        route_prefix=source.route_prefix,
        repo_id=item.repo_id,
        revision=revision,
        item=item.raw,
    )
    if blob_oid is None:
        log.info(
            "hf_cards.card_absent",
            kind=source.kind,
            repo=item.repo_id,
            revision=revision,
        )
        return False

    repository_state_key = _readme_state_key(source.source_feed, item.repo_id, blob_oid)
    repository_state = state_store.get(repository_state_key)
    state_repo = repository_state.get("repo_id")
    if state_repo is not None and state_repo != item.repo_id:
        raise ValueError("Hugging Face README state-key collision")
    if repository_state.get("blob_oid") == blob_oid:
        return False

    route = f"{source.route_prefix}/" if source.route_prefix else ""
    card_url = f"{HF_API_BASE}/{route}{item.repo_id}/blob/{revision}/README.md"
    document_id = _readme_doc_id(source.source_feed, item.repo_id, blob_oid)
    admission = decide_license_admission(
        source_url=card_url,
        source_feed=source.source_feed,
        license_value=HF_PUBLIC_REPOSITORY_TERMS,
        license_source="source_terms",
        source_format="web",
        resolver="hf-public-repository-terms",
        evidence_url=HF_TERMS_URL,
        evidence_revision=f"git-blob:{blob_oid}",
        evidence_scope="source_terms",
        document_id=document_id,
    )
    await admission_producer.send(admission.decision)
    if not admission.fetch_allowed:
        return False

    card_response = await client.get(
        _readme_resolve_url(
            route_prefix=source.route_prefix,
            repo_id=item.repo_id,
            revision=revision,
        )
    )
    if card_response.status_code in {401, 403, 404}:
        log.warning(
            "hf_cards.card_disappeared",
            kind=source.kind,
            repo=item.repo_id,
            revision=revision,
            status=card_response.status_code,
        )
        return False
    card_response.raise_for_status()
    payload = card_response.content
    _verify_blob(payload, blob_oid)
    payload_hash = content_sha256(payload)
    await _emit_payload(
        payload=payload,
        url=card_url,
        source_feed=source.source_feed,
        extension="README.md.gz",
        cfg=cfg,
        producer=producer,
        minio=minio,
        extra_meta={
            f"hf_{source.kind}_id": item.repo_id,
            "hf_last_modified": item.last_modified_raw,
            "hf_revision": revision,
            "hf_readme_blob": blob_oid,
            "hf_readme_content_sha256": payload_hash,
        },
        license_value=HF_PUBLIC_REPOSITORY_TERMS,
        license_source="source_terms",
        source_format="web",
        content_type="text/markdown; charset=utf-8",
        extraction_pipeline=source.extraction_pipeline,
        admission_override=admission,
        document_id=document_id,
    )
    # This checkpoint is separate per repository and immutable README blob and
    # is never truncated. Weight/data-only commits and a later return to an old
    # README revision therefore remain no-ops across process or pod restarts.
    state_store.put(
        repository_state_key,
        {
            "version": 1,
            "repo_id": item.repo_id,
            "blob_oid": blob_oid,
            "content_sha256": payload_hash,
            "repository_revision": revision,
            "last_modified": _format_timestamp(item.last_modified),
        },
    )
    return True


async def _poll_source(
    cfg: IngestConfig,
    *,
    source: _HubSource,
    producer: BronzeProducer,
    minio: MinioWriter,
    admission_producer: LicenseAdmissionProducer,
    limit: int,
) -> int:
    state_store = FeedStateStore(
        "/var/lib/s2p-state/hf_poller" if not cfg.is_dev else "./.s2p-state/hf"
    )
    state = _new_scan_state(state_store.get(source.source_feed))
    state_store.put(source.source_feed, state)
    scan = state["scan"]
    assert isinstance(scan, dict)
    processed = {
        value for value in scan.get("processed_catalogue_revisions", []) if isinstance(value, str)
    }
    boundary = scan.get("boundary")
    if boundary is not None and not isinstance(boundary, dict):
        raise ValueError("invalid Hugging Face scan boundary")

    headers_extra = {"Authorization": f"Bearer {cfg.hf_token}"} if cfg.hf_token else {}
    headers = build_headers(cfg, accept="application/json", extra=headers_extra)
    emitted = 0
    try:
        async with build_async_client(cfg, headers=headers) as client:
            while True:
                request_url = scan.get("request_url")
                if request_url == _INITIAL_PAGE:
                    params = {
                        "sort": "lastModified",
                        "direction": "-1",
                        "limit": str(limit),
                        **source.list_params,
                    }
                    response = await client.get(f"{HF_API_BASE}{source.endpoint}", params=params)
                elif isinstance(request_url, str):
                    response = await client.get(request_url)
                else:
                    raise ValueError("invalid Hugging Face resume URL")
                response.raise_for_status()
                raw_items = response.json()
                if not isinstance(raw_items, list):
                    raise ValueError(f"Hugging Face {source.kind} response must be a JSON list")
                items = sorted(
                    (
                        parsed
                        for raw in raw_items
                        if (parsed := _catalogue_item(raw, source)) is not None
                    ),
                    key=_catalogue_order,
                )
                next_url = _next_page_url(response)
                _update_scan_head(scan, items)
                state_store.put(source.source_feed, state)

                crossed_boundary = False
                for item in items:
                    relation = _boundary_relation(item, boundary)
                    if relation < 0:
                        crossed_boundary = True
                        continue
                    if item.key in processed or relation == 0:
                        processed.add(item.key)
                    else:
                        if await _process_readme_revision(
                            cfg=cfg,
                            client=client,
                            source=source,
                            item=item,
                            state_store=state_store,
                            producer=producer,
                            minio=minio,
                            admission_producer=admission_producer,
                        ):
                            emitted += 1
                        processed.add(item.key)
                    scan["processed_catalogue_revisions"] = sorted(processed)
                    state_store.put(source.source_feed, state)

                bootstrap = scan.get("bootstrap") is True
                if bootstrap or crossed_boundary:
                    break
                if next_url is None:
                    # A full page without Link: rel=next before the old durable
                    # watermark would silently create a gap. Never advance it.
                    if len(raw_items) >= limit and boundary is not None:
                        raise RuntimeError(
                            "Hugging Face pagination ended before the durable watermark"
                        )
                    break
                scan["request_url"] = next_url
                scan["pages_completed"] = int(scan.get("pages_completed", 0)) + 1
                processed.clear()
                scan["processed_catalogue_revisions"] = []
                state_store.put(source.source_feed, state)

        completed = state.get("completed")
        head_timestamp = scan.get("head_last_modified")
        if isinstance(head_timestamp, str):
            completed = {
                "last_modified": head_timestamp,
                "catalogue_revisions": sorted(
                    value
                    for value in scan.get("head_catalogue_revisions", [])
                    if isinstance(value, str)
                ),
                "legacy_repositories": [],
            }
        state_store.put(
            source.source_feed,
            {"version": HF_SCAN_STATE_VERSION, "completed": completed},
        )
    except Exception:
        INGEST_METRICS.record_feed_poll(source_feed=source.source_feed, outcome="error")
        raise
    INGEST_METRICS.record_feed_poll(source_feed=source.source_feed, outcome="success")
    return emitted


async def poll_models(
    cfg: IngestConfig,
    *,
    producer: BronzeProducer,
    minio: MinioWriter,
    admission_producer: LicenseAdmissionProducer,
    limit: int = 100,
) -> int:
    """Emit only README blob changes while traversing to the durable watermark."""
    return await _poll_source(
        cfg,
        source=MODEL_SOURCE,
        producer=producer,
        minio=minio,
        admission_producer=admission_producer,
        limit=limit,
    )


async def poll_hub_cards(
    cfg: IngestConfig,
    *,
    kind: str,
    producer: BronzeProducer,
    minio: MinioWriter,
    admission_producer: LicenseAdmissionProducer,
    limit: int = 100,
) -> int:
    """Emit only changed exact-blob dataset-card README prose."""
    if kind != "dataset":
        raise ValueError("only Hugging Face dataset cards are an active source")
    return await _poll_source(
        cfg,
        source=DATASET_SOURCE,
        producer=producer,
        minio=minio,
        admission_producer=admission_producer,
        limit=limit,
    )


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
