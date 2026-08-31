"""Asynchronous, non-gating public-classifier shadow evaluation over Gold text."""

from __future__ import annotations

import hashlib
import os
import time
from dataclasses import dataclass
from typing import Any

import boto3
import httpx
import orjson
from botocore.exceptions import ClientError
from prometheus_client import Counter, Gauge, Histogram, generate_latest

from processor import common
from processor.probes import start_probe_server
from schemas.gold import GoldRecord

SHADOW_GENERATION = "public-shadow-v1"
SHADOW_MODEL_BUNDLE = "meta-rater@0072a9a+finemath@bd0b0e3+cso-classifier-4.0.1+ontology-3.5"

SHADOW_DOCUMENTS = Counter(
    "s2p_shadow_documents_total",
    "Documents processed by the non-gating public-classifier shadow lane.",
    ["source", "status"],
)
SHADOW_SECONDS = Histogram(
    "s2p_shadow_document_seconds",
    "End-to-end public shadow-classification latency per document.",
    ["source"],
    buckets=(1, 5, 15, 30, 60, 120, 300, 600),
)
SHADOW_SCORE = Histogram(
    "s2p_shadow_score",
    "Observed public shadow-classifier scores. These do not gate curation.",
    ["source", "classifier"],
    buckets=(0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5),
)
SHADOW_COVERAGE = Gauge(
    "s2p_shadow_input_coverage_ratio",
    "Last observed fraction of full-document chunks scored by a shadow model.",
    ["source", "classifier"],
)


def _source_family(record: GoldRecord) -> str:
    if record.source_format in {"html", "pdf", "latex"} and record.scientific_artifact_s3_uri:
        return "arxiv"
    if record.source_feed == "hf-models":
        return "hf-models"
    if record.source_feed == "hf-datasets":
        return "hf-datasets"
    return "other"


@dataclass(slots=True)
class ShadowRuntime:
    cfg: common.ProcessorConfig
    model_url: str
    s3: Any
    client: httpx.Client
    bucket: str
    prefix: str = "classifier-shadow/public-shadow-v1"

    @classmethod
    def from_config(cls, cfg: common.ProcessorConfig) -> ShadowRuntime:
        model_url = os.environ.get(
            "S2P_SHADOW_MODEL_SERVICE_URL",
            "http://stream2pretrain-processor-model-service-shadow:8094",
        ).rstrip("/")
        timeout = float(os.environ.get("S2P_SHADOW_MODEL_TIMEOUT_SECONDS", "900"))
        if timeout <= 0:
            raise RuntimeError("S2P_SHADOW_MODEL_TIMEOUT_SECONDS must be positive")
        client = httpx.Client(
            base_url=model_url,
            timeout=httpx.Timeout(timeout, connect=10.0),
            limits=httpx.Limits(max_keepalive_connections=1, max_connections=2),
            trust_env=False,
        )
        response = client.get("/v1/metadata")
        response.raise_for_status()
        metadata = response.json()
        if metadata.get("ready") is not True or metadata.get("profile") != "shadow":
            raise RuntimeError("public shadow model service did not report the shadow profile")
        return cls(
            cfg=cfg,
            model_url=model_url,
            s3=boto3.client(
                "s3",
                endpoint_url=cfg.minio_endpoint,
                aws_access_key_id=cfg.minio_access_key,
                aws_secret_access_key=cfg.minio_secret_key,
                region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            ),
            client=client,
            bucket=os.environ.get("S2P_SHADOW_BUCKET", cfg.gold_bucket),
            prefix=os.environ.get("S2P_SHADOW_PREFIX", "classifier-shadow/public-shadow-v1").strip(
                "/"
            ),
        )

    def score(self, payload: bytes) -> str | None:
        record = common.gold_loads(payload)
        source = _source_family(record)
        if source == "other" or record.route in {"quarantine", "retry"}:
            SHADOW_DOCUMENTS.labels(source, "skipped").inc()
            return None
        key = self._key(record)
        if self._exists(key):
            SHADOW_DOCUMENTS.labels(source, "cached").inc()
            return f"s3://{self.bucket}/{key}"
        started = time.monotonic()
        try:
            response = self.client.post("/v1/shadow", json={"text": record.text})
            response.raise_for_status()
            classifiers = response.json().get("classifiers")
            if not isinstance(classifiers, dict):
                raise RuntimeError("shadow model response omitted classifier results")
            body = orjson.dumps(
                {
                    "schema_version": 1,
                    "shadow_generation": SHADOW_GENERATION,
                    "model_bundle_revision": SHADOW_MODEL_BUNDLE,
                    "doc_id": record.doc_id,
                    "trace_id": record.trace_id,
                    "source_feed": record.source_feed,
                    "source_family": source,
                    "source_format": record.source_format,
                    "route": record.route,
                    "input_sha256": hashlib.sha256(record.text.encode("utf-8")).hexdigest(),
                    "input_characters": len(record.text),
                    "input_tokens": record.tokens,
                    "classifiers": classifiers,
                },
                option=orjson.OPT_SORT_KEYS,
            )
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
            )
        except Exception:
            SHADOW_DOCUMENTS.labels(source, "error").inc()
            raise
        elapsed = time.monotonic() - started
        SHADOW_DOCUMENTS.labels(source, "scored").inc()
        SHADOW_SECONDS.labels(source).observe(elapsed)
        for family, result in classifiers.items():
            if not isinstance(result, dict):
                continue
            score = result.get("score")
            if isinstance(score, int | float):
                SHADOW_SCORE.labels(source, family).observe(float(score))
            coverage = result.get("coverage_ratio")
            if isinstance(coverage, int | float):
                SHADOW_COVERAGE.labels(source, family).set(float(coverage))
        return f"s3://{self.bucket}/{key}"

    def _key(self, record: GoldRecord) -> str:
        identity = hashlib.sha256(record.doc_id.encode("utf-8")).hexdigest()
        return f"{self.prefix}/source={_source_family(record)}/{identity}.json"

    def _exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True


def build_dataflow(
    cfg: common.ProcessorConfig,
    *,
    runtime_status: common.BytewaxRuntimeStatus | None = None,
    runtime: ShadowRuntime | None = None,
) -> object:
    from bytewax import operators as op
    from bytewax.dataflow import Dataflow

    active_runtime = runtime or ShadowRuntime.from_config(cfg)
    flow = Dataflow(os.environ.get("S2P_BYTEWAX_FLOW_NAME", "s2p-public-shadow-v1"))
    source = common.tracked_kafka_source(
        runtime_status=runtime_status,
        source_name="public_classifier_shadow",
        brokers=cfg.redpanda_brokers.split(","),
        topics=[cfg.curated_topic],
        starting_offset=common.kafka_starting_offset(),
        add_config=common.kafka_consumer_config(
            os.environ.get("S2P_CONSUMER_GROUP", "s2p-public-shadow-v1")
        ),
        batch_size=1,
    )
    messages = op.input("curated", flow, source)

    def _score(message: object) -> str | None:
        payload = getattr(message, "value", None)
        if payload is None:
            return None
        return active_runtime.score(bytes(payload))

    results = op.filter_map("score_public_shadow", messages, _score)
    op.inspect("record_public_shadow", results, lambda _step, _uri: None)
    return flow


def main() -> None:
    cfg = common.load_config()
    common.configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    runtime_status = common.BytewaxRuntimeStatus()
    flow = build_dataflow(cfg, runtime_status=runtime_status)
    start_probe_server(
        metrics_provider=generate_latest,
        readiness_provider=runtime_status.is_ready,
    )
    common.run_bytewax_flow(
        flow,
        cfg,
        os.environ.get("S2P_BYTEWAX_RECOVERY_NAME", "public-shadow-v1"),
        runtime_status=runtime_status,
    )


if __name__ == "__main__":
    main()


__all__ = ["SHADOW_GENERATION", "ShadowRuntime", "build_dataflow"]
