"""End-to-end PaperBundle to accepted SFT/RL artifact pipeline."""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from processor.foundry.config import FoundryConfig
from processor.foundry.control import ProviderControlPlane
from processor.foundry.graph import EvidenceGraphCompiler
from processor.foundry.oracles import OracleCoordinator, OracleRuntimeError
from processor.foundry.packaging import EnvironmentPackager, PackageResult
from processor.foundry.paper_adapter import bundle_json, paper_bundle_from_gold
from processor.foundry.providers import ProviderError, ProviderOutputError
from processor.foundry.quota import QuotaExceededError
from processor.foundry.store import FoundryStore
from processor.foundry.tasking import SolvedTask, TaskFactory, TaskOutputError
from processor.foundry.util import canonical_json, sha256, stable_id
from processor.foundry.validation import run_acceptance_suite, suite_passes
from processor.foundry.verifier import VerifierCompiler
from schemas.foundry import (
    DatasetSplit,
    FoundryArtifactRecord,
    FoundryEvent,
    OfficialArtifact,
    OracleResult,
    PaperBundle,
    PaperEvidenceGraph,
    PosttrainPool,
    ProviderTrace,
    TaskSpec,
    Trajectory,
    ValidationReport,
)
from schemas.gold import GoldRecord
from schemas.scientific import ScientificDocument


class PackageSink(Protocol):
    def write(self, package: PackageResult, *, paper_id: str, task_id: str) -> str: ...


@dataclass(frozen=True, slots=True)
class PipelineResult:
    job_id: str
    paper_id: str
    final_state: str
    artifacts: list[FoundryArtifactRecord]
    rejection_reason: str | None = None


class FoundryPipeline:
    def __init__(
        self,
        *,
        config: FoundryConfig,
        store: FoundryStore,
        control: ProviderControlPlane,
        package_sink: PackageSink,
        event_sink: Callable[[FoundryEvent], None] | None = None,
        artifact_sink: Callable[[FoundryArtifactRecord], None] | None = None,
        asset_loader: Callable[[str], bytes] | None = None,
        oracle_coordinator: OracleCoordinator | None = None,
    ) -> None:
        self.config = config
        self.store = store
        self.control = control
        self.package_sink = package_sink
        self.event_sink = event_sink
        self.artifact_sink = artifact_sink
        self.graph_compiler = EvidenceGraphCompiler(control)
        self.task_factory = TaskFactory(control)
        self.verifier_compiler = VerifierCompiler(control)
        self.packager = EnvironmentPackager(asset_loader=asset_loader)
        self.oracle_coordinator = oracle_coordinator

    def process(
        self,
        gold: GoldRecord,
        scientific: ScientificDocument,
        *,
        official_artifacts: list[OfficialArtifact] | None = None,
    ) -> PipelineResult:
        bundle = paper_bundle_from_gold(
            gold,
            scientific,
            official_artifacts=official_artifacts,
        )
        job_id, created = self.store.start_job(
            paper_id=bundle.paper_id,
            paper_hash=bundle.paper_hash,
            doc_id=gold.doc_id,
            policy_version=self.config.policy_version,
        )
        if not created:
            existing = self.store.job(job_id)
            state = str(existing["state"]) if existing else "REJECTED"
            return PipelineResult(
                job_id=job_id, paper_id=bundle.paper_id, final_state=state, artifacts=[]
            )
        persisted_bundle = self.store.load_bundle(job_id)
        if persisted_bundle is not None:
            bundle = PaperBundle.model_validate_json(persisted_bundle)
        self._transition(job_id, bundle.paper_id, "RECEIVED")
        self.store.save_bundle(job_id, bundle_json(bundle))
        oracle_results: list[OracleResult] = []
        if any(artifact.oracle_recipe is not None for artifact in bundle.official_artifacts):
            if self.oracle_coordinator is None:
                raise OracleRuntimeError(
                    "an audited official-artifact oracle is configured but no isolated runtime is enabled"
                )
            oracle_results = self.oracle_coordinator.run_all(bundle.official_artifacts)
        quota_state = [value.model_dump(mode="json") for value in self.control.quota.states()]
        self._transition(
            job_id,
            bundle.paper_id,
            "PROVIDER_CAPACITY_RESERVED",
            metadata={"quota_state": quota_state, "reservation_scope": "per-call transactional"},
        )
        try:
            graph, graph_traces = self.graph_compiler.compile(
                job_id=job_id,
                bundle=bundle,
                oracle_results=oracle_results,
            )
            self.store.save_graph(job_id, canonical_json(graph))
            self._transition(
                job_id,
                bundle.paper_id,
                "GRAPH_COMPILED",
                artifact_hash=sha256(graph),
                metadata={"nodes": len(graph.nodes), "edges": len(graph.edges)},
            )
            self._transition(
                job_id,
                bundle.paper_id,
                "GRAPH_CRITIQUED",
                metadata={
                    "uncertainties": len(graph.uncertainties),
                    "conflicts": len(graph.conflicts),
                },
            )
            tasks, task_traces = self.task_factory.propose(
                job_id=job_id,
                bundle=bundle,
                graph=graph,
                oracle_results=oracle_results,
            )
            self._transition(
                job_id,
                bundle.paper_id,
                "TASKS_PROPOSED",
                metadata={"accepted_proposals": len(tasks)},
            )
            self._transition(
                job_id,
                bundle.paper_id,
                "TASKS_ROUTED",
                metadata={
                    "sft": sum(task.route == "sft" for task in tasks),
                    "rl": sum(task.route == "rl" for task in tasks),
                },
            )
            solved: list[SolvedTask] = []
            task_failures: list[str] = []
            for task in tasks:
                try:
                    solved.append(
                        self.task_factory.solve(
                            job_id=job_id,
                            bundle=bundle,
                            graph=graph,
                            task=task,
                        )
                    )
                except TaskOutputError as exc:
                    task_failures.append(f"{task.task_id}: {exc}")
            if not solved:
                raise ValueError("no task produced a valid solution after bounded repairs")
            self._transition(
                job_id,
                bundle.paper_id,
                "SOLUTIONS_GENERATED",
                metadata={
                    "tasks": len(solved),
                    "trajectories": sum(len(value.trajectories) for value in solved),
                    "skipped_tasks": len(task_failures),
                    "degraded_solutions": sum(len(value.solution_failures) for value in solved),
                },
            )
            artifacts = self._validate_and_package(
                job_id=job_id,
                bundle=bundle,
                graph=graph,
                solved=solved,
                common_traces=[*graph_traces, *task_traces],
                oracle_results=oracle_results,
            )
        except ProviderOutputError as exc:
            # A completed but malformed structured response is deterministic
            # candidate-level failure, not a transient transport failure. Mark
            # it rejected so the ranked snapshot can advance to later papers.
            reason = str(exc)
            self._transition(job_id, bundle.paper_id, "REJECTED", reason=reason)
            return PipelineResult(
                job_id=job_id,
                paper_id=bundle.paper_id,
                final_state="REJECTED",
                artifacts=[],
                rejection_reason=reason,
            )
        except (ProviderError, QuotaExceededError):
            # Bytewax retries from the last immutable stage. Completed provider
            # calls are replayed from the durable call cache, never billed twice.
            raise
        except ValueError as exc:
            self._transition(job_id, bundle.paper_id, "REJECTED", reason=str(exc))
            return PipelineResult(
                job_id=job_id,
                paper_id=bundle.paper_id,
                final_state="REJECTED",
                artifacts=[],
                rejection_reason=str(exc),
            )
        accepted_rl = any(
            artifact.kind == "rl_environment" and artifact.status == "accepted"
            for artifact in artifacts
        )
        accepted_sft = any(
            artifact.kind == "sft_trajectory" and artifact.status == "accepted"
            for artifact in artifacts
        )
        if accepted_sft:
            self._transition(
                job_id,
                bundle.paper_id,
                "ACCEPTED_SFT",
                metadata={
                    "accepted_trajectories": sum(
                        a.kind == "sft_trajectory" and a.status == "accepted" for a in artifacts
                    )
                },
            )
        final_state = "ACCEPTED_SFT"
        if accepted_rl:
            self._transition(
                job_id,
                bundle.paper_id,
                "ACCEPTED_RL",
                metadata={
                    "accepted_environments": sum(
                        a.kind == "rl_environment" and a.status == "accepted" for a in artifacts
                    )
                },
            )
            final_state = "ACCEPTED_RL"
        elif not accepted_sft:
            self._transition(
                job_id, bundle.paper_id, "REJECTED", reason="no task passed final gates"
            )
            final_state = "REJECTED"
        return PipelineResult(
            job_id=job_id,
            paper_id=bundle.paper_id,
            final_state=final_state,
            artifacts=artifacts,
            rejection_reason="no task passed final gates" if final_state == "REJECTED" else None,
        )

    def _validate_and_package(
        self,
        *,
        job_id: str,
        bundle: PaperBundle,
        graph: PaperEvidenceGraph,
        solved: list[SolvedTask],
        common_traces: list[ProviderTrace],
        oracle_results: list[OracleResult],
    ) -> list[FoundryArtifactRecord]:
        artifacts: list[FoundryArtifactRecord] = []
        verifiers_compiled = 0
        adversarial_validated = 0
        for value in solved:
            task_traces = [*common_traces, *value.traces]
            if not value.critic.accepted:
                artifacts.append(
                    self._rejected_artifact(
                        job_id=job_id,
                        bundle=bundle,
                        task=value.task,
                        traces=task_traces,
                        reason="grounding critic gate failed",
                    )
                )
                continue
            if value.task.route == "sft":
                report, trajectories = _validate_sft(
                    value,
                    bundle,
                    graph,
                )
                if not report.positive_pass:
                    artifacts.append(
                        self._rejected_artifact(
                            job_id=job_id,
                            bundle=bundle,
                            task=value.task,
                            traces=task_traces,
                            reason="SFT grounding/manifest gate failed",
                            validation=report,
                        )
                    )
                    continue
                pool: PosttrainPool = "sft"
                dataset_split, _pool_ordinal = self.store.assign_pool_split(
                    allocation_key=f"{pool}:{bundle.paper_family_id}",
                    pool=pool,
                )
                package = self.packager.build(
                    bundle=bundle,
                    graph=graph,
                    task=value.task,
                    trajectories=trajectories,
                    validation=report,
                    traces=task_traces,
                    verifier=None,
                    pool=pool,
                    dataset_split=dataset_split,
                    oracle_results=oracle_results,
                )
                uri = self.package_sink.write(
                    package, paper_id=bundle.paper_id, task_id=value.task.task_id
                )
                artifacts.extend(
                    self._accepted_sft_records(
                        job_id=job_id,
                        bundle=bundle,
                        task=value.task,
                        trajectories=trajectories,
                        validation=report,
                        traces=task_traces,
                        package=package,
                        package_uri=uri,
                        pool=pool,
                        dataset_split=dataset_split,
                    )
                )
                continue

            try:
                verifier, verifier_traces = self.verifier_compiler.compile(
                    job_id=job_id,
                    bundle=bundle,
                    graph=graph,
                    task=value.task,
                )
            except ValueError as exc:
                artifacts.append(
                    self._rejected_artifact(
                        job_id=job_id,
                        bundle=bundle,
                        task=value.task,
                        traces=task_traces,
                        reason=f"verifier construction failed: {exc}",
                    )
                )
                continue
            verifiers_compiled += 1
            all_traces = [*task_traces, *verifier_traces]
            report, trajectories, validation_cases = run_acceptance_suite(
                task=value.task,
                spec=verifier,
                bundle=bundle,
                graph=graph,
                trajectories=value.trajectories,
            )
            adversarial_validated += 1
            if not suite_passes(report):
                artifacts.append(
                    self._rejected_artifact(
                        job_id=job_id,
                        bundle=bundle,
                        task=value.task,
                        traces=all_traces,
                        reason="deterministic acceptance suite failed",
                        validation=report,
                    )
                )
                continue
            pool = "rl"
            dataset_split, _pool_ordinal = self.store.assign_pool_split(
                allocation_key=f"{pool}:{bundle.paper_family_id}",
                pool=pool,
            )
            package = self.packager.build(
                bundle=bundle,
                graph=graph,
                task=value.task,
                trajectories=trajectories,
                validation=report,
                traces=all_traces,
                verifier=verifier,
                pool=pool,
                dataset_split=dataset_split,
                validation_cases=validation_cases,
                oracle_results=oracle_results,
            )
            uri = self.package_sink.write(
                package, paper_id=bundle.paper_id, task_id=value.task.task_id
            )
            rl_record = self._artifact_record(
                artifact_id=stable_id(
                    "rl-environment", value.task.task_id, package.environment_hash
                ),
                job_id=job_id,
                bundle=bundle,
                task=value.task,
                kind="rl_environment",
                status="accepted",
                validation=report,
                traces=all_traces,
                package=package,
                package_uri=uri,
                pool=pool,
                dataset_split=dataset_split,
            )
            artifacts.append(rl_record)
        if verifiers_compiled:
            self._transition(
                job_id,
                bundle.paper_id,
                "VERIFIERS_COMPILED",
                metadata={"count": verifiers_compiled},
            )
        if adversarial_validated:
            self._transition(
                job_id,
                bundle.paper_id,
                "ADVERSARIAL_VALIDATED",
                metadata={"count": adversarial_validated},
            )
        for artifact in artifacts:
            self.store.record_artifact(artifact)
            if self.artifact_sink is not None:
                self.artifact_sink(artifact)
        return artifacts

    def _accepted_sft_records(
        self,
        *,
        job_id: str,
        bundle: PaperBundle,
        task: TaskSpec,
        trajectories: list[Trajectory],
        validation: ValidationReport,
        traces: list[ProviderTrace],
        package: PackageResult,
        package_uri: str,
        pool: PosttrainPool,
        dataset_split: DatasetSplit,
    ) -> list[FoundryArtifactRecord]:
        return [
            self._artifact_record(
                artifact_id=trajectory.trajectory_id,
                job_id=job_id,
                bundle=bundle,
                task=task,
                kind="sft_trajectory",
                status="accepted",
                validation=validation,
                traces=traces,
                package=package,
                package_uri=package_uri,
                pool=pool,
                dataset_split=dataset_split,
            )
            for trajectory in trajectories
            if trajectory.accepted
        ]

    def _rejected_artifact(
        self,
        *,
        job_id: str,
        bundle: PaperBundle,
        task: TaskSpec,
        traces: list[ProviderTrace],
        reason: str,
        validation: ValidationReport | None = None,
    ) -> FoundryArtifactRecord:
        report = validation or ValidationReport(
            task_id=task.task_id,
            positive_pass=False,
            equivalent_pass=False,
            adversarial_pass=False,
            mutation_killed=0,
            mutation_total=0,
            metamorphic_pass=False,
            replay_pass=False,
            security_pass=True,
            false_positive_count=0,
            false_negative_count=0,
            details={"rejection_reason": reason},
        )
        digest = sha256({"task": task, "validation": report, "reason": reason})
        record = FoundryArtifactRecord(
            artifact_id=stable_id("rejected-artifact", task.task_id, digest),
            job_id=job_id,
            paper_id=bundle.paper_id,
            task_id=task.task_id,
            family=task.family,
            kind="rl_environment" if task.route == "rl" else "sft_trajectory",
            pool="rl" if task.route == "rl" else "sft",
            dataset_split="none",
            status="rejected",
            quality_label="generated_unverified",
            package_hash=digest,
            environment_hash=digest,
            paper_hash=bundle.paper_hash,
            provider_trace_ids=[trace.trace_id for trace in traces],
            constructor_family=_family_for_role(traces, "task_designer"),
            critic_family=_family_for_role(traces, "grounding_critic"),
            validation=report,
            created_at=datetime.now(UTC),
        )
        return record

    def _artifact_record(
        self,
        *,
        artifact_id: str,
        job_id: str,
        bundle: PaperBundle,
        task: TaskSpec,
        kind: str,
        status: str,
        validation: ValidationReport,
        traces: list[ProviderTrace],
        package: PackageResult,
        package_uri: str,
        pool: PosttrainPool,
        dataset_split: DatasetSplit,
    ) -> FoundryArtifactRecord:
        return FoundryArtifactRecord(
            artifact_id=artifact_id,
            job_id=job_id,
            paper_id=bundle.paper_id,
            task_id=task.task_id,
            family=task.family,
            kind=kind,  # type: ignore[arg-type]
            pool=pool,
            dataset_split=dataset_split,
            status=status,  # type: ignore[arg-type]
            quality_label=package.manifest.quality_label,
            package_uri=package_uri,
            signature_uri=f"{package_uri}.sig.json",
            signature_backend=package.signature_backend,
            signer_cert_hash=sha256(package.signer_cert_pem),
            package_hash=package.package_hash,
            environment_hash=package.environment_hash,
            paper_hash=bundle.paper_hash,
            provider_trace_ids=[trace.trace_id for trace in traces],
            constructor_family=_family_for_role(traces, "task_designer"),
            critic_family=_family_for_role(traces, "grounding_critic"),
            validation=validation,
            created_at=datetime.now(UTC),
        )

    def _transition(
        self,
        job_id: str,
        paper_id: str,
        state: str,
        *,
        reason: str | None = None,
        artifact_hash: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> FoundryEvent:
        event = self.store.append_event(
            job_id=job_id,
            paper_id=paper_id,
            state=state,
            reason=reason,
            artifact_hash=artifact_hash,
            metadata=metadata,
            idempotency_suffix=state,
        )
        if self.event_sink is not None:
            self.event_sink(event)
        return event


def _validate_sft(
    value: SolvedTask,
    bundle: PaperBundle,
    graph: PaperEvidenceGraph,
) -> tuple[ValidationReport, list[Trajectory]]:
    span_ids = {span.span_id for span in bundle.stable_spans}
    node_ids = {node.id for node in graph.nodes}
    public_span_ids = {
        *value.task.public_context_policy.included_spans,
        *value.task.public_context_policy.same_paper_distractors,
    }
    required_nodes = set(value.task.hidden_targets.required_nodes)
    required_relations = {
        (edge.source, edge.relation, edge.target)
        for edge in value.task.hidden_targets.required_relations
    }
    accepted_evidence_sets = [
        set(item) for item in value.task.hidden_targets.accepted_evidence_sets
    ]
    numeric_targets = {
        key: float(expected)
        for key, expected in value.task.hidden_targets.expected_values.items()
        if isinstance(expected, (int, float)) and not isinstance(expected, bool)
    }
    validated: list[Trajectory] = []
    for trajectory in value.trajectories:
        manifest = trajectory.answer.answer_manifest
        manifest_node_ids = {
            value
            for value in [
                *manifest.claims,
                *manifest.method_nodes,
                *manifest.faults,
                *manifest.qualifications,
                *(eq.id for eq in manifest.equations),
            ]
            if value in node_ids
        }
        submitted_evidence = set(manifest.evidence)
        submitted_relations = {
            (edge.source, edge.relation, edge.target) for edge in manifest.relations
        }
        numeric_results = {result.id: result.value for result in manifest.numeric_results}
        checks = {
            "report_present": bool(trajectory.answer.report.strip()),
            "manifest_present": bool(manifest_node_ids or submitted_evidence),
            "evidence_resolves": bool(submitted_evidence)
            and submitted_evidence <= span_ids
            and submitted_evidence <= public_span_ids,
            "evidence_covers_target": not accepted_evidence_sets
            or any(expected <= submitted_evidence for expected in accepted_evidence_sets),
            "required_nodes": required_nodes <= manifest_node_ids,
            "required_relations": required_relations <= submitted_relations,
            "numeric_targets": all(
                key in numeric_results
                and math.isclose(
                    numeric_results[key],
                    expected,
                    rel_tol=1e-6,
                    abs_tol=1e-9,
                )
                for key, expected in numeric_targets.items()
            ),
            "tool_execution": all(call.error is None for call in trajectory.tool_calls),
        }
        passed = all(checks.values())
        validated.append(
            trajectory.model_copy(
                update={
                    "accepted": passed,
                    "reward": 1.0 if passed else 0.0,
                    "validation": checks,
                }
            )
        )
    positive = bool(validated) and all(value.accepted for value in validated)
    report = ValidationReport(
        task_id=value.task.task_id,
        positive_pass=positive,
        equivalent_pass=True,
        adversarial_pass=True,
        mutation_killed=0,
        mutation_total=0,
        metamorphic_pass=True,
        replay_pass=True,
        security_pass=True,
        false_positive_count=0,
        false_negative_count=sum(not item.accepted for item in validated),
        details={"route": "sft", "grounding_critic": value.critic.model_dump(mode="json")},
    )
    return report, validated


def _family_for_role(traces: list[ProviderTrace], role: str) -> str:
    return next((trace.model_family for trace in reversed(traces) if trace.role == role), "unknown")


__all__ = ["FoundryPipeline", "PipelineResult"]
