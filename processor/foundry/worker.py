"""Bytewax foundry worker: posttrain Gold records to SFT/RL artifacts."""

from __future__ import annotations

import os
import socket
import threading
import time
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any
from urllib.parse import urlparse

from processor import common
from processor.foundry.config import FoundryConfig
from processor.foundry.control import (
    ProviderControlPlane,
    ProviderDiscoveryError,
)
from processor.foundry.database import coordination_database_target
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
from processor.foundry.providers import ProviderBudgetExhaustedError, ProviderError, build_providers
from processor.foundry.quota import QuotaExceededError, QuotaLedger
from processor.foundry.store import CandidateClaim, CandidateLeaseLostError, FoundryStore
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


class CandidateLeaseHeartbeat:
    def __init__(
        self,
        *,
        store: FoundryStore,
        doc_id: str,
        owner_id: str,
        claim_token: str,
        lease_seconds: float,
    ) -> None:
        self._store = store
        self._doc_id = doc_id
        self._owner_id = owner_id
        self._claim_token = claim_token
        self._lease_seconds = lease_seconds
        self._interval = max(0.05, lease_seconds / 3.0)
        self._stopped = threading.Event()
        self._lost = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=f"foundry-candidate-lease-{doc_id}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stopped.set()
        self._thread.join(timeout=min(5.0, self._interval))

    def assert_owned(self) -> None:
        if self._lost.is_set() or not self._store.candidate_claim_owned(
            self._doc_id,
            owner_id=self._owner_id,
            claim_token=self._claim_token,
        ):
            raise CandidateLeaseLostError(self._doc_id)

    def _run(self) -> None:
        while not self._stopped.wait(self._interval):
            try:
                renewed = self._store.renew_candidate_lease(
                    self._doc_id,
                    owner_id=self._owner_id,
                    claim_token=self._claim_token,
                    lease_seconds=self._lease_seconds,
                )
            except Exception:
                continue
            if not renewed:
                self._lost.set()
                return


class WorkerRuntime:
    def __init__(self, cfg: common.ProcessorConfig) -> None:
        self.cfg = cfg
        self.config = FoundryConfig.from_env()
        state_dir = self.config.state_dir
        coordination_target = coordination_database_target(state_dir, "control.sqlite3")
        self.worker_id = os.environ.get("S2P_FOUNDRY_WORKER_ID", "").strip() or (
            f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4()}"
        )
        self.candidate_lease_seconds = max(
            float(self.config.queue_poll_seconds * 3),
            self.config.timeout_seconds * 2,
        )
        self.store = FoundryStore(
            coordination_target,
            recover_processing=True,
            candidate_generation="source-gates-v1",
        )
        self.quota = QuotaLedger(
            coordination_database_target(state_dir, "quota.sqlite3"),
            self.config.providers,
            reservation_lease_seconds=self.candidate_lease_seconds,
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
            batch_size=int(os.environ.get("S2P_FOUNDRY_ICEBERG_BATCH_SIZE", "5000")),
            flush_interval_seconds=float(
                os.environ.get("S2P_FOUNDRY_ICEBERG_FLUSH_INTERVAL_SECONDS", "3600")
            ),
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
        self._recover_lakehouse_outbox()
        self._drain_lock = threading.Lock()
        self._drain_stop = threading.Event()
        self._drain_thread = threading.Thread(
            target=self._queue_loop,
            name="foundry-candidate-queue",
            daemon=True,
        )
        self._drain_thread.start()

    def process(self, payload: bytes) -> dict[str, Any]:
        incoming = common.gold_loads(payload)
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
        quality = incoming.quality_diagnostics or {}
        # A historical Kafka replay is not a newly eligible paper. Do this
        # before any S3 fetch or preflight job, so reset queues stay reset.
        if quality.get("mode") != "active" or not quality.get("passed", False):
            return {"doc_id": incoming.doc_id, "status": "outside_active_candidate_generation"}
        admission_identity = sha256(
            {
                "doc_id": incoming.doc_id,
                "evidence": incoming.scientific_artifact_s3_uri,
                "classifier": incoming.classifier_revision,
                "generation": "source-gates-v1",
            }
        )
        if self.store.candidate_admission_seen(admission_identity):
            return {"doc_id": incoming.doc_id, "status": "already_observed_candidate"}
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
            self.store.record_candidate_admission(admission_identity, incoming.doc_id, str(exc))
            self.store.remove_queued_candidate(incoming.doc_id)
            QUEUED_CANDIDATES.set(self.store.queued_candidates())
            return {"doc_id": incoming.doc_id, "status": "evidence_unavailable", "reason": str(exc)}
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
            self.store.record_candidate_admission(admission_identity, incoming.doc_id, str(exc))
            self.store.remove_queued_candidate(incoming.doc_id)
            QUEUED_CANDIDATES.set(self.store.queued_candidates())
            return {"doc_id": incoming.doc_id, "status": "evidence_unavailable", "reason": str(exc)}
        self.store.enqueue_candidate(
            doc_id=incoming.doc_id,
            payload=payload,
            reasoning_score=incoming.reasoning_score,
            quality_score=incoming.quality_score,
            valid_from=incoming.valid_from,
            scientific_payload=scientific_payload,
            ranking_score=_candidate_ranking_score(incoming),
            domain_key=(
                incoming.content_tags[0] if incoming.content_tags else "general_scientific"
            ),
        )
        self.store.record_candidate_admission(admission_identity, incoming.doc_id, "queued")
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
        if not hasattr(self, "worker_id"):
            self.worker_id = f"local-test:{uuid.uuid4()}"
        if not hasattr(self, "candidate_lease_seconds"):
            self.candidate_lease_seconds = max(
                float(self.config.queue_poll_seconds * 3),
                self.config.timeout_seconds * 2,
            )
        claimed = self.store.claim_candidate_lease(
            cutoff_at=cutoff_at,
            cutoff_ordinal=cutoff_ordinal,
            daily_run_date=run_day,
            owner_id=self.worker_id,
            lease_seconds=self.candidate_lease_seconds,
        )
        if claimed is None:
            retry_after = self.store.next_candidate_retry_delay(
                cutoff_at=cutoff_at,
                cutoff_ordinal=cutoff_ordinal,
                daily_run_date=run_day,
            )
            if retry_after is not None:
                return {
                    "doc_id": fallback_doc_id,
                    "status": "queue_waiting",
                    "retry_after_seconds": retry_after,
                }
            return {"doc_id": fallback_doc_id, "status": "queue_empty"}
        assert claimed.claim_token is not None
        lease = CandidateLeaseHeartbeat(
            store=self.store,
            doc_id=claimed.doc_id,
            owner_id=self.worker_id,
            claim_token=claimed.claim_token,
            lease_seconds=self.candidate_lease_seconds,
        )
        lease.start()
        try:
            return self._process_claimed_candidate(
                claim=claimed,
                lease=lease,
                run_day=run_day,
                manual_run_id=manual_run_id,
            )
        finally:
            lease.stop()

    def _process_claimed_candidate(
        self,
        *,
        claim: CandidateClaim,
        lease: CandidateLeaseHeartbeat,
        run_day: date | None,
        manual_run_id: str | None,
    ) -> dict[str, Any]:
        assert claim.claim_token is not None
        claimed_doc_id = claim.doc_id
        claimed_payload = claim.payload
        try:
            gold = common.gold_loads(claimed_payload)
            scientific_payload = self.store.candidate_scientific_payload(claimed_doc_id)
            if scientific_payload is None:
                scientific, scientific_payload = load_scientific_artifact_payload(
                    gold,
                    s3_client=self.s3,
                )
                self.store.cache_candidate_scientific_payload(
                    claimed_doc_id,
                    scientific_payload,
                    owner_id=self.worker_id,
                    claim_token=claim.claim_token,
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
            gold = common.gold_loads(claimed_payload)
            self.store.record_candidate_admission(
                sha256({"doc_id": gold.doc_id, "evidence": gold.scientific_artifact_s3_uri}),
                gold.doc_id,
                str(exc),
            )
            job_result = {
                "doc_id": gold.doc_id,
                "status": "evidence_unavailable",
                "reason": str(exc),
            }
        except ProviderBudgetExhaustedError:
            self._release_candidate_if_owned(claim)
            QUEUED_CANDIDATES.set(self.store.queued_candidates())
            raise
        except ProviderError as exc:
            lease.assert_owned()
            retry_after = self.store.defer_candidate(
                claimed_doc_id,
                reason=str(exc),
                owner_id=self.worker_id,
                claim_token=claim.claim_token,
            )
            QUEUED_CANDIDATES.set(self.store.queued_candidates())
            return {
                "doc_id": claimed_doc_id,
                "status": "provider_retry_deferred",
                "reason": str(exc),
                "retry_after_seconds": retry_after,
            }
        except Exception:
            self._release_candidate_if_owned(claim)
            QUEUED_CANDIDATES.set(self.store.queued_candidates())
            raise
        # The coordination database is the durable outbox. Restage all job outputs so a worker
        # restart after a sink failure cannot strand an accepted artifact or
        # an auditable terminal candidate preflight rejection.
        try:
            lease.assert_owned()
            if "job_id" in job_result:
                self._flush_job_outbox(job_result)
        except Exception:
            self._release_candidate_if_owned(claim)
            QUEUED_CANDIDATES.set(self.store.queued_candidates())
            raise
        # Advance the queue and run counters only after both durable sinks
        # acknowledge the complete job outbox.
        lease.assert_owned()
        self.store.finish_candidate(
            claimed_doc_id,
            owner_id=self.worker_id,
            claim_token=claim.claim_token,
        )
        if run_day is not None:
            self.store.record_daily_processed(run_day)
        if manual_run_id is not None:
            self.store.record_manual_processed(manual_run_id)
        QUEUED_CANDIDATES.set(self.store.queued_candidates())
        if "state" in job_result:
            JOBS.labels(state=str(job_result["state"])).inc()
        return {**job_result, "queued_candidates": self.store.queued_candidates()}

    def _release_candidate_if_owned(self, claim: CandidateClaim) -> None:
        try:
            self.store.release_candidate(
                claim.doc_id,
                owner_id=self.worker_id,
                claim_token=claim.claim_token,
            )
        except CandidateLeaseLostError:
            return

    def _flush_job_outbox(self, job_result: dict[str, Any]) -> None:
        job_id = str(job_result["job_id"])
        for event in self.store.event_records(job_id):
            self.kafka.event(event)
            self.store.mark_lakehouse_published(self.lakehouse.add_event(event))
        for artifact in self.store.artifact_records(job_id):
            self.kafka.artifact(artifact)
            self.store.mark_lakehouse_published(self.lakehouse.add_artifact(artifact))
        self.kafka.job(job_result)
        self.kafka.flush()
        self.store.mark_lakehouse_published(self.lakehouse.flush(force=False))

    def _recover_lakehouse_outbox(self) -> None:
        for job_id in self.store.pending_lakehouse_job_ids():
            for event in self.store.event_records(job_id):
                self.store.mark_lakehouse_published(self.lakehouse.add_event(event))
            for artifact in self.store.artifact_records(job_id):
                self.store.mark_lakehouse_published(self.lakehouse.add_artifact(artifact))
        self.store.mark_lakehouse_published(self.lakehouse.flush(force=True))

    def _recover_interrupted_calls(self, abandoned_reservations: int) -> None:
        """Close prior-process call events before the queue resumes them."""
        recovered = self.store.interrupted_provider_calls(recoverable_only=True)
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
            self.kafka.flush()

    def _queue_loop(self) -> None:
        import structlog

        log = structlog.get_logger(component="foundry-queue")
        while not self._drain_stop.wait(self.config.queue_poll_seconds):
            try:
                self._queue_iteration(log)
            except Exception as exc:
                log.warning("foundry_queue_iteration_retry_pending", reason=str(exc))

    def _queue_iteration(self, log: Any) -> None:
        """Run one scheduler iteration so transient state loss cannot kill the loop."""
        try:
            self.store.mark_lakehouse_published(self.lakehouse.flush(force=False))
        except Exception as exc:
            log.warning("foundry_lakehouse_flush_pending", reason=str(exc))
        now = datetime.now(UTC)
        if self.config.daily_not_before_utc is not None and now < self.config.daily_not_before_utc:
            # A schedule migration must not back-run the preceding day's
            # cohort before its explicitly chosen first boundary.
            self.store.expire_active_manual_runs(reason="superseded by scheduled 24-hour cohort")
            return
        run_day, boundary_at = _daily_cohort_boundary(
            now,
            self.config.daily_run_hour_utc,
            self.config.daily_run_minute_utc,
        )
        existing = self.store.daily_run(run_day)
        boundary_changed = existing is None or str(existing["cutoff_at"]) != boundary_at.isoformat()
        if boundary_changed:
            expired = self.store.expire_active_manual_runs(
                reason="superseded by scheduled 24-hour cohort"
            )
            if expired:
                log.info(
                    "foundry_manual_runs_superseded",
                    count=expired,
                    run_date=run_day.isoformat(),
                )
        run = self.store.start_daily_run(
            run_day,
            boundary_at=boundary_at,
        )
        if run["state"] not in {"completed", "quota_exhausted"}:
            self._run_daily_snapshot(run_day, run, log)
            return
        self._run_pending_manual(log)

    def database_ready(self) -> bool:
        """Require both shared control and quota connections for readiness."""
        control_ready = self.store.database_ready()
        quota_ready = self.quota.database_ready()
        return control_ready and quota_ready

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
            current_day, _ = _daily_cohort_boundary(
                datetime.now(UTC),
                getattr(self.config, "daily_run_hour_utc", 0),
                getattr(self.config, "daily_run_minute_utc", 0),
            )
            if current_day > run_day:
                self.store.finish_daily_run(
                    run_day,
                    state="completed",
                    reason="replaced at the next 24-hour cohort boundary",
                )
                return
            current = self.store.daily_run(run_day) or run
            if int(current["processed_count"]) >= int(current["candidate_count"]):
                self.store.finish_daily_run(
                    run_day,
                    state="completed",
                    reason="ranked 24-hour candidate cohort completed",
                )
                return
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
            if result.get("status") in {"queue_waiting", "provider_retry_deferred"}:
                delay = min(
                    float(result.get("retry_after_seconds", self.config.queue_poll_seconds)),
                    float(self.config.queue_poll_seconds),
                )
                if self._drain_stop.wait(max(1.0, delay)):
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
            if result.get("status") in {"queue_waiting", "provider_retry_deferred"}:
                delay = min(
                    float(result.get("retry_after_seconds", self.config.queue_poll_seconds)),
                    float(self.config.queue_poll_seconds),
                )
                if self._drain_stop.wait(max(1.0, delay)):
                    return

    def close(self) -> None:
        self._drain_stop.set()
        self._drain_thread.join(timeout=5)
        self.store.mark_lakehouse_published(self.lakehouse.flush(force=True))
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
        self.store.mark_lakehouse_published(self.lakehouse.add_event(event))
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
        self.store.mark_lakehouse_published(self.lakehouse.add_artifact(artifact))
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


def _candidate_ranking_score(record: GoldRecord) -> float:
    """Learned mean suitability ranks fresh papers, never API cost or length."""
    diagnostics = record.quality_diagnostics or {}
    classifiers = diagnostics.get("classifiers")
    suitability = (
        classifiers.get("arxiv-posttrain-suitability") if isinstance(classifiers, dict) else None
    )
    if diagnostics.get("mode") == "active" and isinstance(suitability, dict):
        return float(suitability["weighted_mean"]) / 5.0
    evidence_richness = (
        sum(count > 0 for count in (record.equation_count, record.table_count, record.figure_count))
        / 3.0
    )
    signals = [
        record.quality_score / 5.0,
        record.structural_quality_score / 5.0,
        record.extraction_completeness,
        record.reasoning_score,
        evidence_richness,
    ]
    if not record.quality_diagnostics or record.quality_diagnostics.get("mode") != "diagnostic":
        signals.append(record.source_quality_score / 5.0)
    return sum(signals) / len(signals)


def _daily_cohort_boundary(
    now: datetime, hour_utc: int, minute_utc: int = 0
) -> tuple[date, datetime]:
    """Return the most recent configured UTC cohort boundary."""
    if now.tzinfo is None:
        raise ValueError("daily cohort clock must be timezone-aware")
    utc_now = now.astimezone(UTC)
    boundary = utc_now.replace(hour=hour_utc, minute=minute_utc, second=0, microsecond=0)
    if utc_now < boundary:
        boundary -= timedelta(days=1)
    return boundary.date(), boundary


def build_dataflow(
    cfg: common.ProcessorConfig | None = None,
    *,
    runtime: WorkerRuntime | None = None,
    runtime_status: common.BytewaxRuntimeStatus | None = None,
) -> object:
    from bytewax import operators as op
    from bytewax.connectors.kafka import KafkaSink, KafkaSinkMessage
    from bytewax.dataflow import Dataflow

    runtime_cfg = cfg or common.load_config()
    active_runtime = runtime or WorkerRuntime(runtime_cfg)
    flow = Dataflow("s2p-foundry")
    source: Any = common.tracked_kafka_source(
        runtime_status=runtime_status,
        source_name="docs_curated",
        brokers=runtime_cfg.redpanda_brokers.split(","),
        topics=[runtime_cfg.curated_topic],
        starting_offset=common.kafka_starting_offset(),
        add_config=common.kafka_consumer_config(
            os.environ.get("S2P_CONSUMER_GROUP", "s2p-foundry")
        ),
        batch_size=common.kafka_source_batch_size(),
    )
    messages: Any = op.input("curated", flow, source)

    def process_message(message: Any) -> Any:
        result = active_runtime.process(bytes(message.value))
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
    runtime_status = common.BytewaxRuntimeStatus()
    runtime: WorkerRuntime | None = None

    def ready() -> bool:
        return runtime is not None and runtime_status.is_ready() and runtime.database_ready()

    start_probe_server(
        metrics_provider=generate_latest,
        readiness_provider=ready,
    )
    log = structlog.get_logger(component="foundry")
    while True:
        try:
            runtime = WorkerRuntime(cfg)
            flow = build_dataflow(
                cfg,
                runtime=runtime,
                runtime_status=runtime_status,
            )
        except ProviderDiscoveryError as exc:
            runtime = None
            log.warning("foundry_waiting_for_provider", reason=str(exc))
            time.sleep(30)
            continue
        common.run_bytewax_flow(
            flow,
            cfg,
            "foundry",
            runtime_status=runtime_status,
        )
        return


if __name__ == "__main__":
    main()
