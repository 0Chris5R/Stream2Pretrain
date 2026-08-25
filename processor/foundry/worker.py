"""Bytewax foundry worker: posttrain Gold records to SFT/RL artifacts."""

from __future__ import annotations

import os
import threading
import time
from datetime import UTC, date, datetime
from typing import Any
from urllib.parse import urlparse

from processor import common
from processor.foundry.config import FoundryConfig
from processor.foundry.control import (
    ProviderControlPlane,
    ProviderDiscoveryError,
)
from processor.foundry.lakehouse import FoundryLakehouseSink
from processor.foundry.metrics import (
    ARTIFACTS,
    JOBS,
    MUTATION_KILL_RATE,
    PROVIDER_AVAILABLE,
    PROVIDER_CALLS,
    PROVIDER_LATENCY,
    PROVIDER_OUTPUT_RATE,
    PROVIDER_TOKENS,
    PROVIDER_TTFT,
    QUEUED_CANDIDATES,
    QUOTA_REMAINING,
    STAGES,
    VALIDATION,
)
from processor.foundry.oracles import S3OracleRegistry, build_oracle_coordinator
from processor.foundry.packaging import MinioPackageSink
from processor.foundry.paper_adapter import (
    ScientificArtifactUnavailableError,
    load_scientific_artifact_payload,
    validate_scientific_artifact_payload,
)
from processor.foundry.pipeline import FoundryPipeline
from processor.foundry.providers import ProviderBudgetExhaustedError, build_providers
from processor.foundry.quota import QuotaExceededError, QuotaLedger
from processor.foundry.store import FoundryStore
from processor.foundry.util import canonical_json, sha256
from processor.probes import start_probe_server
from processor.source_policy import resolve_source_policy
from schemas.foundry import FoundryArtifactRecord, FoundryEvent
from schemas.gold import GoldRecord
from schemas.topics import FOUNDRY_ARTIFACTS, FOUNDRY_EVENTS, FOUNDRY_JOBS


class KafkaPublisher:
    def __init__(self, brokers: list[str]) -> None:
        from confluent_kafka import Producer

        self._producer = Producer(
            {
                "bootstrap.servers": ",".join(brokers),
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "zstd",
                "client.id": "s2p-foundry",
            }
        )
        self._staged_event_ids: set[str] = set()
        self._staged_artifact_ids: set[str] = set()

    def event(self, value: FoundryEvent) -> None:
        if value.event_id in self._staged_event_ids:
            return
        self._producer.produce(
            FOUNDRY_EVENTS,
            key=value.job_id.encode(),
            value=canonical_json(value),
        )
        self._staged_event_ids.add(value.event_id)
        self._producer.poll(0)

    def artifact(self, value: FoundryArtifactRecord) -> None:
        if value.artifact_id in self._staged_artifact_ids:
            return
        self._producer.produce(
            FOUNDRY_ARTIFACTS,
            key=value.artifact_id.encode(),
            value=canonical_json(value),
        )
        self._staged_artifact_ids.add(value.artifact_id)
        self._producer.poll(0)

    def job(self, value: dict[str, Any]) -> None:
        key = str(value.get("job_id") or value.get("doc_id") or "foundry").encode()
        self._producer.produce(
            FOUNDRY_JOBS,
            key=key,
            value=canonical_json(value),
        )
        self._producer.poll(0)

    def flush(self) -> None:
        remaining = self._producer.flush(30)
        if remaining:
            raise RuntimeError(f"{remaining} foundry messages were not delivered")
        self._staged_event_ids.clear()
        self._staged_artifact_ids.clear()


class WorkerRuntime:
    def __init__(self, cfg: common.ProcessorConfig) -> None:
        self.cfg = cfg
        self.config = FoundryConfig.from_env()
        state_dir = self.config.state_dir
        # Only the single-writer worker owns crash recovery. Read-only API
        # sidecars must never requeue a candidate that this worker is handling.
        self.store = FoundryStore(
            os.path.join(state_dir, "control.sqlite3"), recover_processing=True
        )
        self.quota = QuotaLedger(
            os.path.join(state_dir, "quota.sqlite3"),
            self.config.providers,
        )
        abandoned_reservations = self.quota.reconcile_abandoned_reservations()
        self.providers = build_providers(
            self.config.providers,
            mode=self.config.provider_mode,
            replay_fixture=self.config.replay_fixture,
            timeout_seconds=self.config.timeout_seconds,
            max_retries=self.config.max_retries,
        )
        self.control = ProviderControlPlane(
            config=self.config,
            providers=self.providers,
            quota=self.quota,
            store=self.store,
        )
        try:
            snapshots = self.control.discover_models()
        except Exception:
            self.store.close()
            self.quota.close()
            for provider in self.providers.values():
                client = getattr(provider, "_client", None)
                if client is not None:
                    client.close()
            raise
        for name in snapshots:
            PROVIDER_AVAILABLE.labels(provider=name).set(1.0)
        self.kafka = KafkaPublisher(cfg.redpanda_brokers.split(","))
        self.lakehouse = FoundryLakehouseSink(
            batch_size=int(os.environ.get("S2P_FOUNDRY_ICEBERG_BATCH_SIZE", "50"))
        )
        self.s3 = _s3_client(cfg)
        _require_bucket(self.s3, self.config.minio_bucket)
        self.oracle_registry = S3OracleRegistry(
            s3_client=self.s3,
            bucket=self.config.minio_bucket,
        )
        self.control.event_sink = self._event
        self.pipeline = FoundryPipeline(
            config=self.config,
            store=self.store,
            control=self.control,
            package_sink=MinioPackageSink(s3_client=self.s3, bucket=self.config.minio_bucket),
            event_sink=self._event,
            artifact_sink=self._artifact,
            asset_loader=lambda uri: _load_s3_uri(self.s3, uri),
            oracle_coordinator=(
                build_oracle_coordinator()
                if os.environ.get("S2P_FOUNDRY_ENABLE_ORACLES", "0") == "1"
                else None
            ),
        )
        self._recover_interrupted_calls(abandoned_reservations)
        self._drain_lock = threading.Lock()
        self._drain_stop = threading.Event()
        self._drain_thread = threading.Thread(
            target=self._queue_loop,
            name="foundry-candidate-queue",
            daemon=True,
        )
        self._drain_thread.start()

    def process(self, payload: bytes) -> dict[str, Any]:
        incoming = GoldRecord.model_validate_json(payload)
        accepted_routes = {"posttrain_candidate"}
        if os.environ.get("S2P_FOUNDRY_ACCEPT_LEGACY_REASONING") == "1":
            accepted_routes.add("reasoning_candidate")
        if not accepted_routes.intersection({incoming.route, *incoming.eligible_routes}):
            self.store.remove_queued_candidate(incoming.doc_id)
            return {"doc_id": incoming.doc_id, "status": "not_posttrain_candidate"}
        source_policy = resolve_source_policy(
            source_feed=incoming.source_feed,
            source_format=incoming.source_format,
            extraction_pipeline=incoming.extraction_pipeline,
        )
        if source_policy.family != "scientific_paper":
            self.store.remove_queued_candidate(incoming.doc_id)
            return {"doc_id": incoming.doc_id, "status": "unsupported_posttrain_source"}
        if not incoming.scientific_artifact_s3_uri or incoming.training_word_count < 1:
            parsed_artifact_uri = urlparse(incoming.scientific_artifact_s3_uri or "")
            exc = ScientificArtifactUnavailableError(
                uri=incoming.scientific_artifact_s3_uri or "",
                bucket=parsed_artifact_uri.netloc or "unknown",
                key=parsed_artifact_uri.path.lstrip("/") or "unknown",
                reason=(
                    "URI is absent"
                    if not incoming.scientific_artifact_s3_uri
                    else "Gold has no retained scientific body"
                ),
            )
            job_result = self._record_candidate_preflight_rejection(incoming, exc)
            self._flush_job_outbox(job_result)
            self.store.remove_queued_candidate(incoming.doc_id)
            QUEUED_CANDIDATES.set(self.store.queued_candidates())
            JOBS.labels(state=str(job_result["state"])).inc()
            return {**job_result, "queued_candidates": self.store.queued_candidates()}
        try:
            scientific_payload = self.store.candidate_scientific_payload(
                incoming.doc_id,
                expected_gold_payload=payload,
            )
            if scientific_payload is None:
                _, scientific_payload = load_scientific_artifact_payload(
                    incoming,
                    s3_client=self.s3,
                )
            else:
                validate_scientific_artifact_payload(incoming, scientific_payload)
        except ScientificArtifactUnavailableError as exc:
            job_result = self._record_candidate_preflight_rejection(incoming, exc)
            self._flush_job_outbox(job_result)
            self.store.remove_queued_candidate(incoming.doc_id)
            QUEUED_CANDIDATES.set(self.store.queued_candidates())
            JOBS.labels(state=str(job_result["state"])).inc()
            return {**job_result, "queued_candidates": self.store.queued_candidates()}
        self.store.enqueue_candidate(
            doc_id=incoming.doc_id,
            payload=payload,
            reasoning_score=incoming.reasoning_score,
            quality_score=incoming.quality_score,
            valid_from=incoming.valid_from,
            scientific_payload=scientific_payload,
        )
        QUEUED_CANDIDATES.set(self.store.queued_candidates())
        return {
            "doc_id": incoming.doc_id,
            "status": "queued_for_daily_ranking",
            "queued_candidates": self.store.queued_candidates(),
        }

    def _drain_one(
        self,
        *,
        run_day: date | None,
        cutoff_at: datetime,
        cutoff_ordinal: int,
        fallback_doc_id: str = "queued",
        manual_run_id: str | None = None,
    ) -> dict[str, Any]:
        with self._drain_lock:
            return self._drain_one_locked(
                run_day=run_day,
                cutoff_at=cutoff_at,
                cutoff_ordinal=cutoff_ordinal,
                fallback_doc_id=fallback_doc_id,
                manual_run_id=manual_run_id,
            )

    def _drain_one_locked(
        self,
        *,
        run_day: date | None,
        cutoff_at: datetime,
        cutoff_ordinal: int,
        fallback_doc_id: str,
        manual_run_id: str | None,
    ) -> dict[str, Any]:
        claimed = self.store.claim_candidate(
            cutoff_at=cutoff_at,
            cutoff_ordinal=cutoff_ordinal,
        )
        if claimed is None:
            return {"doc_id": fallback_doc_id, "status": "queue_empty"}
        claimed_doc_id, claimed_payload = claimed
        try:
            gold = GoldRecord.model_validate_json(claimed_payload)
            scientific_payload = self.store.candidate_scientific_payload(claimed_doc_id)
            if scientific_payload is None:
                scientific, scientific_payload = load_scientific_artifact_payload(
                    gold,
                    s3_client=self.s3,
                )
                self.store.cache_candidate_scientific_payload(
                    claimed_doc_id,
                    scientific_payload,
                )
            else:
                scientific, _ = validate_scientific_artifact_payload(
                    gold,
                    scientific_payload,
                )
            official_artifacts = self.oracle_registry.load(
                scientific.source_identifier or gold.doc_id
            )
            result = self.pipeline.process(
                gold,
                scientific,
                official_artifacts=official_artifacts,
            )
            job_result = {
                "job_id": result.job_id,
                "paper_id": result.paper_id,
                "state": result.final_state,
                "artifacts": len(result.artifacts),
                "rejection_reason": result.rejection_reason,
                "queued_candidates": self.store.queued_candidates(),
            }
        except ScientificArtifactUnavailableError as exc:
            gold = GoldRecord.model_validate_json(claimed_payload)
            job_result = self._record_candidate_preflight_rejection(gold, exc)
        except Exception:
            self.store.release_candidate(claimed_doc_id)
            QUEUED_CANDIDATES.set(self.store.queued_candidates())
            raise
        # SQLite is the durable outbox. Restage all job outputs so a worker
        # restart after a sink failure cannot strand an accepted artifact or
        # an auditable terminal candidate preflight rejection.
        try:
            self._flush_job_outbox(job_result)
        except Exception:
            self.store.release_candidate(claimed_doc_id)
            QUEUED_CANDIDATES.set(self.store.queued_candidates())
            raise
        # Advance the queue and run counters only after both durable sinks
        # acknowledge the complete job outbox.
        self.store.finish_candidate(claimed_doc_id)
        if run_day is not None:
            self.store.record_daily_processed(run_day)
        if manual_run_id is not None:
            self.store.record_manual_processed(manual_run_id)
        QUEUED_CANDIDATES.set(self.store.queued_candidates())
        JOBS.labels(state=str(job_result["state"])).inc()
        return {**job_result, "queued_candidates": self.store.queued_candidates()}

    def _flush_job_outbox(self, job_result: dict[str, Any]) -> None:
        job_id = str(job_result["job_id"])
        for event in self.store.event_records(job_id):
            self.kafka.event(event)
            self.lakehouse.add_event(event)
        for artifact in self.store.artifact_records(job_id):
            self.kafka.artifact(artifact)
            self.lakehouse.add_artifact(artifact)
        self.kafka.job(job_result)
        self.lakehouse.flush()
        self.kafka.flush()

    def _record_candidate_preflight_rejection(
        self,
        gold: GoldRecord,
        exc: ScientificArtifactUnavailableError,
    ) -> dict[str, Any]:
        """Create a terminal, replay-safe audit job for an unusable candidate."""
        paper_hash = sha256(
            {
                "doc_id": gold.doc_id,
                "gold": sha256(gold),
                "scientific_artifact_s3_uri": gold.scientific_artifact_s3_uri,
                "stage": "candidate_preflight",
            }
        )
        job_id, created = self.store.start_job(
            paper_id=gold.doc_id,
            paper_hash=paper_hash,
            doc_id=gold.doc_id,
            policy_version=f"{self.config.policy_version}:candidate-preflight-v1",
        )
        metadata = {
            "stage": "candidate_preflight",
            "source_feed": gold.source_feed,
            "scientific_artifact_bucket": exc.bucket,
            "scientific_artifact_key": exc.key,
            "failure_kind": exc.reason,
        }
        received = self.store.append_event(
            job_id=job_id,
            paper_id=gold.doc_id,
            state="RECEIVED",
            metadata=metadata,
            idempotency_suffix="candidate-preflight",
        )
        rejected = self.store.append_event(
            job_id=job_id,
            paper_id=gold.doc_id,
            state="REJECTED",
            reason=str(exc),
            metadata=metadata,
            idempotency_suffix="candidate-preflight",
        )
        if created:
            STAGES.labels(state=received.state).inc()
            STAGES.labels(state=rejected.state).inc()
        return {
            "job_id": job_id,
            "paper_id": gold.doc_id,
            "state": "REJECTED",
            "artifacts": 0,
            "status": "posttrain_preflight_rejected",
            "rejection_reason": str(exc),
            "scientific_artifact_bucket": exc.bucket,
            "scientific_artifact_key": exc.key,
        }

    def _recover_interrupted_calls(self, abandoned_reservations: int) -> None:
        """Close prior-process call events before the queue resumes them."""
        recovered = self.store.interrupted_provider_calls()
        for call in recovered:
            event = self.store.append_event(
                job_id=str(call["job_id"]),
                paper_id=str(call["paper_id"]),
                state="CALL_FAILED",
                reason="worker restarted before the provider call reached a terminal state",
                metadata={
                    "provider": call["provider"],
                    "role": call["role"],
                    "restart_recovery": True,
                    "was_started": call["was_started"],
                    "abandoned_reservations_reconciled": abandoned_reservations,
                },
                attempt=int(call["attempt"]),
                idempotency_suffix=f"restart-recovery:{call['role']}",
            )
            self._event(event)
        if recovered:
            self.lakehouse.flush()
            self.kafka.flush()

    def _queue_loop(self) -> None:
        import structlog

        log = structlog.get_logger(component="foundry-queue")
        while not self._drain_stop.wait(self.config.queue_poll_seconds):
            if self._run_pending_manual(log):
                continue
            now = datetime.now(UTC)
            if now.hour < self.config.daily_run_hour_utc:
                continue
            run_day = now.date()
            run = self.store.start_daily_run(run_day)
            if run["state"] == "waiting":
                continue
            if run["state"] in {"completed", "quota_exhausted"}:
                continue
            self._run_daily_snapshot(run_day, run, log)

    def _run_pending_manual(self, log: Any) -> bool:
        """Run an active control-plane snapshot at the next safe paper boundary."""
        manual = self.store.claim_manual_run()
        if manual is None:
            return False
        self._run_manual_snapshot(manual, log)
        return True

    def _run_daily_snapshot(self, run_day: date, run: dict[str, Any], log: Any) -> None:
        cutoff_at = datetime.fromisoformat(str(run["cutoff_at"]))
        cutoff_ordinal = int(run["cutoff_ordinal"])
        while not self._drain_stop.is_set():
            # Provider calls are not interrupted, but a bounded manual run must
            # not wait behind the rest of a potentially large daily snapshot.
            if self._run_pending_manual(log):
                if self._drain_stop.wait(self.config.queue_poll_seconds):
                    return
                continue
            try:
                result = self._drain_one(
                    run_day=run_day,
                    cutoff_at=cutoff_at,
                    cutoff_ordinal=cutoff_ordinal,
                )
            except ProviderBudgetExhaustedError as exc:
                self.store.finish_daily_run(
                    run_day,
                    state="quota_exhausted",
                    reason=str(exc),
                )
                log.info(
                    "foundry_daily_provider_budget_exhausted",
                    run_date=run_day.isoformat(),
                    provider=exc.provider,
                )
                return
            except QuotaExceededError as exc:
                if exc.window == "day":
                    self.store.finish_daily_run(
                        run_day,
                        state="quota_exhausted",
                        reason=str(exc),
                    )
                    log.info(
                        "foundry_daily_quota_exhausted",
                        run_date=run_day.isoformat(),
                        reason=str(exc),
                    )
                else:
                    log.info(
                        "foundry_minute_quota_wait",
                        run_date=run_day.isoformat(),
                        reason=str(exc),
                    )
                return
            except Exception as exc:
                log.warning("foundry_queue_retry_pending", reason=str(exc))
                return
            if result.get("status") == "queue_empty":
                self.store.finish_daily_run(
                    run_day,
                    state="completed",
                    reason="ranked snapshot exhausted",
                )
                return

    def _run_manual_snapshot(self, run: dict[str, Any], log: Any) -> None:
        run_id = str(run["run_id"])
        cutoff_at = datetime.fromisoformat(str(run["cutoff_at"]))
        cutoff_ordinal = int(run["cutoff_ordinal"])
        while not self._drain_stop.is_set():
            current = next(
                (value for value in self.store.manual_runs() if value["run_id"] == run_id),
                run,
            )
            max_candidates = current.get("max_candidates")
            if max_candidates is not None and int(current["processed_count"]) >= int(
                max_candidates
            ):
                self.store.finish_manual_run(
                    run_id,
                    state="completed",
                    reason="requested candidate limit reached",
                )
                return
            try:
                result = self._drain_one(
                    run_day=None,
                    cutoff_at=cutoff_at,
                    cutoff_ordinal=cutoff_ordinal,
                    fallback_doc_id="manual-run",
                    manual_run_id=run_id,
                )
            except ProviderBudgetExhaustedError as exc:
                self.store.finish_manual_run(
                    run_id,
                    state="quota_exhausted",
                    reason=str(exc),
                )
                log.info(
                    "foundry_manual_provider_budget_exhausted",
                    run_id=run_id,
                    provider=exc.provider,
                )
                return
            except QuotaExceededError as exc:
                if exc.window == "day":
                    self.store.finish_manual_run(
                        run_id,
                        state="quota_exhausted",
                        reason=str(exc),
                    )
                else:
                    log.info("foundry_manual_minute_quota_wait", run_id=run_id, reason=str(exc))
                return
            except Exception as exc:
                log.warning("foundry_manual_retry_pending", run_id=run_id, reason=str(exc))
                return
            if result.get("status") == "queue_empty":
                self.store.finish_manual_run(
                    run_id,
                    state="completed",
                    reason="ranked snapshot exhausted",
                )
                return

    def close(self) -> None:
        self._drain_stop.set()
        self._drain_thread.join(timeout=5)
        self.lakehouse.flush()
        self.kafka.flush()
        self.store.close()
        self.quota.close()
        for provider in self.providers.values():
            client = getattr(provider, "_client", None)
            if client is not None:
                client.close()
        close_s3 = getattr(self.s3, "close", None)
        if callable(close_s3):
            close_s3()

    def _event(self, event: FoundryEvent) -> None:
        self.kafka.event(event)
        self.lakehouse.add_event(event)
        STAGES.labels(state=event.state).inc()
        if event.state in {"CALL_SUCCEEDED", "CALL_FAILED", "CALL_RATE_LIMITED"}:
            provider = str(event.metadata.get("provider", "unknown"))
            role = str(event.metadata.get("role", "unknown"))
            model = str(event.metadata.get("returned_model", "unknown"))
            status = event.state.removeprefix("CALL_").lower()
            PROVIDER_CALLS.labels(provider=provider, role=role, model=model, status=status).inc()
            if event.state == "CALL_SUCCEEDED":
                PROVIDER_TOKENS.labels(provider=provider, role=role, direction="input").inc(
                    float(event.metadata.get("input_tokens", 0))
                )
                PROVIDER_TOKENS.labels(provider=provider, role=role, direction="output").inc(
                    float(event.metadata.get("output_tokens", 0))
                )
                PROVIDER_LATENCY.labels(provider=provider, role=role).observe(
                    float(event.metadata.get("latency_ms", 0)) / 1000
                )
                ttft = event.metadata.get("time_to_first_token_ms")
                if ttft is not None:
                    PROVIDER_TTFT.labels(provider=provider, role=role).observe(float(ttft) / 1000)
                output_rate = event.metadata.get("output_tokens_per_second")
                if output_rate is not None:
                    PROVIDER_OUTPUT_RATE.labels(provider=provider, role=role).observe(
                        float(output_rate)
                    )
        if event.state == "QUOTA_RECONCILED":
            for quota in self.quota.states():
                for kind, remaining in {
                    "requests": quota.estimated_remaining_requests,
                    "input": quota.estimated_remaining_input,
                    "output": quota.estimated_remaining_output,
                }.items():
                    if remaining is not None:
                        QUOTA_REMAINING.labels(
                            provider=quota.provider,
                            window=quota.window,
                            kind=kind,
                        ).set(remaining)

    def _artifact(self, artifact: FoundryArtifactRecord) -> None:
        self.kafka.artifact(artifact)
        self.lakehouse.add_artifact(artifact)
        ARTIFACTS.labels(
            kind=artifact.kind,
            family=artifact.family,
            status=artifact.status,
        ).inc()
        validation = artifact.validation
        for gate, passed in {
            "positive": validation.positive_pass,
            "equivalent": validation.equivalent_pass,
            "adversarial": validation.adversarial_pass,
            "metamorphic": validation.metamorphic_pass,
            "replay": validation.replay_pass,
            "security": validation.security_pass,
        }.items():
            VALIDATION.labels(task_family=artifact.family, gate=gate).set(float(passed))
        if validation.mutation_total:
            MUTATION_KILL_RATE.labels(task_family=artifact.family).observe(
                validation.mutation_killed / validation.mutation_total
            )


def build_dataflow(cfg: common.ProcessorConfig | None = None) -> object:
    from bytewax import operators as op
    from bytewax.connectors.kafka import KafkaSink, KafkaSinkMessage, KafkaSource
    from bytewax.dataflow import Dataflow

    runtime_cfg = cfg or common.load_config()
    runtime = WorkerRuntime(runtime_cfg)
    flow = Dataflow("s2p-foundry")
    source = KafkaSource(
        brokers=runtime_cfg.redpanda_brokers.split(","),
        topics=[runtime_cfg.curated_topic],
        starting_offset=common.kafka_starting_offset(),
        add_config=common.kafka_consumer_config(
            os.environ.get("S2P_CONSUMER_GROUP", "s2p-foundry")
        ),
    )
    messages = op.input("curated", flow, source)

    def process_message(message: Any) -> Any:
        result = runtime.process(bytes(message.value))
        key = str(result.get("job_id") or result.get("doc_id") or "foundry").encode()
        return KafkaSinkMessage(key=key, value=canonical_json(result))

    results = op.map("build_foundry_artifacts", messages, process_message)
    op.output(
        "job_results",
        results,
        KafkaSink(
            brokers=runtime_cfg.redpanda_brokers.split(","),
            topic=FOUNDRY_JOBS,
            add_config=common.kafka_producer_config(),
        ),
    )
    return flow


def _s3_client(cfg: common.ProcessorConfig) -> object:
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=cfg.minio_endpoint,
        aws_access_key_id=cfg.minio_access_key,
        aws_secret_access_key=cfg.minio_secret_key,
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    )


def _require_bucket(s3: object, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)  # type: ignore[attr-defined]
    except Exception as exc:
        raise RuntimeError(f"required post-training bucket {bucket!r} is unavailable") from exc


def _load_s3_uri(s3: object, uri: str) -> bytes:
    from urllib.parse import urlparse

    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise ValueError(f"invalid foundry asset URI: {uri}")
    response = s3.get_object(  # type: ignore[attr-defined]
        Bucket=parsed.netloc,
        Key=parsed.path.lstrip("/"),
    )
    return bytes(response["Body"].read())


def main() -> None:
    import structlog
    from prometheus_client import generate_latest

    cfg = common.load_config()
    common.configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    ready = threading.Event()
    start_probe_server(
        metrics_provider=generate_latest,
        readiness_provider=ready.is_set,
    )
    log = structlog.get_logger(component="foundry")
    while True:
        try:
            flow = build_dataflow(cfg)
        except ProviderDiscoveryError as exc:
            ready.clear()
            log.warning("foundry_waiting_for_provider", reason=str(exc))
            time.sleep(30)
            continue
        ready.set()
        common.run_bytewax_flow(flow, cfg, "foundry")
        return


if __name__ == "__main__":
    main()
