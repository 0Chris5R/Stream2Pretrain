"""Task proposal, routing, independent solution, and grounding pipeline."""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, model_validator

from processor.foundry.control import ProviderControlPlane
from processor.foundry.paper_adapter import bundle_prompt_json
from processor.foundry.symbolic import symbolic_expression_is_checkable
from processor.foundry.tools import PaperRuntime, ToolError
from processor.foundry.util import canonical_json, sha256, stable_id
from schemas.foundry import (
    AnswerManifest,
    FoundryAnswer,
    OracleResult,
    PaperBundle,
    PaperEvidenceGraph,
    ProviderTrace,
    TaskSpec,
    ToolCall,
    Trajectory,
    TrajectoryTurn,
)


class TaskBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tasks: list[TaskSpec]


class AnswerabilityDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")
    task_id: str
    answerable: bool
    leakage_free: bool
    unique_enough_for_rl: bool
    findings: list[str] = Field(default_factory=list)


class AnswerabilityBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    decisions: list[AnswerabilityDecision]


class SolutionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    report: str
    answer_manifest: AnswerManifest
    tool_calls: list[ToolCall] = Field(default_factory=list)


class SolverTurn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["tool_request", "final"]
    report: str | None = None
    answer_manifest: AnswerManifest | None = None
    tool_calls: list[ToolCall] = Field(default_factory=list)

    @model_validator(mode="after")
    def status_payload_is_complete(self) -> SolverTurn:
        if self.status == "final":
            if self.report is None or self.answer_manifest is None:
                raise ValueError("final solver turn omitted report or answer manifest")
            if self.tool_calls:
                raise ValueError("final solver turn contains unexecuted tool calls")
        elif not self.tool_calls:
            raise ValueError("tool-request solver turn did not request a tool")
        return self


class TrajectoryGroundingDecision(BaseModel):
    """A scientific-content audit for exactly one generated trajectory.

    Manifest-shape and hidden-target checks deliberately do not belong here.
    They are executable deterministic gates, while this decision is restricted
    to unsupported or contradictory scientific content in the readable answer.
    """

    model_config = ConfigDict(extra="forbid")

    trajectory_id: str
    scientifically_grounded: bool
    findings: list[str] = Field(default_factory=list)
    unsupported_claims: list[str] = Field(default_factory=list)
    contradictory_claims: list[str] = Field(default_factory=list)


class GroundingCritique(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decisions: list[TrajectoryGroundingDecision]
    findings: list[str] = Field(default_factory=list)


@dataclass(frozen=True, slots=True)
class SolverFailure:
    """One failed solver attempt with the exact durable provider-call lineage."""

    role: str
    reason: str
    traces: tuple[ProviderTrace, ...] = ()

    def audit(self) -> dict[str, object]:
        return {
            "role": self.role,
            "reason": self.reason,
            "provider_trace_ids": [trace.trace_id for trace in self.traces],
            "prompt_traces": [
                {
                    "trace_id": trace.trace_id,
                    "prompt_version": trace.prompt_version,
                    "request_hash": trace.request_hash,
                    "response_hash": trace.response_hash,
                    "returned_model": trace.returned_model,
                }
                for trace in self.traces
            ],
        }


def _dedupe_traces(traces: Sequence[ProviderTrace]) -> list[ProviderTrace]:
    return list({trace.trace_id: trace for trace in traces}.values())


class TaskOutputError(ValueError):
    """A model-authored task artifact remained invalid after one bounded repair."""

    def __init__(
        self,
        message: str,
        *,
        traces: Sequence[ProviderTrace] = (),
        solver_failures: Sequence[SolverFailure] = (),
    ) -> None:
        super().__init__(message)
        self.traces = tuple(_dedupe_traces(traces))
        self.solver_failures = tuple(solver_failures)


_MAX_SOLVER_TOOL_TURNS = 8
CONTENT_POLICY_REVISION = "scientific-reasoning-v2"


@dataclass(frozen=True, slots=True)
class SolvedTask:
    task: TaskSpec
    trajectories: list[Trajectory]
    traces: list[ProviderTrace]
    critic: GroundingCritique
    solution_failures: tuple[SolverFailure, ...] = ()


ModelT = TypeVar("ModelT", bound=BaseModel)


class TaskFactory:
    def __init__(self, control: ProviderControlPlane) -> None:
        self.control = control

    def propose(
        self,
        *,
        job_id: str,
        bundle: PaperBundle,
        graph: PaperEvidenceGraph,
        oracle_results: list[OracleResult] | None = None,
    ) -> tuple[list[TaskSpec], list[ProviderTrace]]:
        oracle_ids = {result.oracle_id for result in oracle_results or []}
        proposal_data, designer_trace = self.control.call(
            job_id=job_id,
            paper_id=bundle.paper_id,
            role="task_designer",
            system=_designer_system(),
            user=_designer_prompt(
                bundle,
                graph,
                self.control.config.tasks_per_paper,
                oracle_ids,
            ),
            max_output_tokens=12_000,
        )
        batch, proposal_repair = _validate_or_repair(
            control=self.control,
            model=TaskBatch,
            data=proposal_data,
            job_id=job_id,
            paper_id=bundle.paper_id,
            call_key="task_designer",
            context="Preserve the proposed scientific tasks and repair only schema violations.",
        )
        normalized = [
            _normalize_task(task, bundle, graph, designer_trace, oracle_ids)
            for task in batch.tasks[: self.control.config.tasks_per_paper]
        ]
        validated: list[TaskSpec] = []
        for task in normalized:
            if task.route == "reject":
                continue
            try:
                validate_task(
                    task,
                    bundle,
                    graph,
                    oracle_result_ids=oracle_ids,
                )
            except ValueError:
                continue
            validated.append(task)
        if not validated:
            raise ValueError("no proposed task passed deterministic specification checks")
        critique_data, critic_trace = self.control.call(
            job_id=job_id,
            paper_id=bundle.paper_id,
            role="answerability_critic",
            system=_answerability_system(),
            user=_answerability_prompt(bundle, graph, validated),
            max_output_tokens=8_000,
        )
        decisions, answerability_repair = _validate_or_repair(
            control=self.control,
            model=AnswerabilityBatch,
            data=critique_data,
            job_id=job_id,
            paper_id=bundle.paper_id,
            call_key="answerability_critic",
            context="Preserve each audit decision and repair only schema violations.",
        )
        by_id = {decision.task_id: decision for decision in decisions.decisions}
        accepted: list[TaskSpec] = []
        for task in validated:
            decision = by_id.get(task.task_id)
            if decision is None or not decision.answerable or not decision.leakage_free:
                continue
            route = task.route
            if route == "rl" and not decision.unique_enough_for_rl:
                route = "sft"
            accepted.append(
                task.model_copy(
                    update={
                        "route": route,
                        "ambiguity_risks": [*task.ambiguity_risks, *decision.findings],
                        "construction_provenance": [
                            *task.construction_provenance,
                            critic_trace.trace_id,
                        ],
                    }
                )
            )
        selected = _select_diverse_tasks(
            accepted,
            limit=self.control.config.accepted_tasks_per_paper,
        )
        return selected, [
            designer_trace,
            *([proposal_repair] if proposal_repair is not None else []),
            critic_trace,
            *([answerability_repair] if answerability_repair is not None else []),
        ]

    def solve(
        self,
        *,
        job_id: str,
        bundle: PaperBundle,
        graph: PaperEvidenceGraph,
        task: TaskSpec,
    ) -> SolvedTask:
        traces: list[ProviderTrace] = []
        trajectories: list[Trajectory] = []
        solution_failures: list[SolverFailure] = []
        for role, plan in (
            ("solver_a", "Use a direct constructive plan and verify every structured commitment."),
            (
                "solver_b",
                "Use a structurally different plan and independently recompute the answer.",
            ),
        ):
            try:
                payload, solver_traces, turns, tool_calls = self._solve_one(
                    job_id=job_id,
                    bundle=bundle,
                    graph=graph,
                    task=task,
                    role=role,
                    plan=plan,
                )
            except TaskOutputError as exc:
                failure_traces = list(exc.traces)
                traces.extend(failure_traces)
                solution_failures.append(
                    SolverFailure(role=role, reason=str(exc), traces=tuple(failure_traces))
                )
                continue
            traces.extend(solver_traces)
            trace = solver_traces[-1]
            trajectories.append(
                Trajectory(
                    trajectory_id=stable_id("trajectory", task.task_id, trace.trace_id),
                    task_id=task.task_id,
                    provider_trace_id=trace.trace_id,
                    provider_trace_ids=[value.trace_id for value in solver_traces],
                    answer=FoundryAnswer(
                        report=payload.report,
                        answer_manifest=payload.answer_manifest,
                    ),
                    tool_calls=tool_calls,
                    turns=turns,
                    accepted=False,
                    reward=0.0,
                    validation={"stage": "awaiting_deterministic_verifier"},
                    loss_masked_turns=[turn.index for turn in turns if turn.role == "tool"],
                )
            )
        if not trajectories:
            raise TaskOutputError(
                f"task {task.task_id} produced no valid solution after bounded repairs",
                traces=traces,
                solver_failures=solution_failures,
            )
        critic_data, critic_trace = self.control.call(
            job_id=job_id,
            paper_id=bundle.paper_id,
            role="grounding_critic",
            system=_grounding_system(),
            user=_grounding_prompt(bundle, graph, task, trajectories),
            max_output_tokens=6_000,
            call_key=f"grounding_critic:{task.task_id}",
        )
        traces.append(critic_trace)
        try:
            critique, repair_trace = _validate_or_repair(
                control=self.control,
                model=GroundingCritique,
                data=critic_data,
                job_id=job_id,
                paper_id=bundle.paper_id,
                call_key=f"grounding_critic:{task.task_id}",
                context="Preserve the grounding audit and repair only schema violations.",
            )
        except TaskOutputError as exc:
            raise TaskOutputError(
                f"grounding critique failed: {exc}",
                traces=[*traces, *exc.traces],
                solver_failures=solution_failures,
            ) from exc
        if repair_trace is not None:
            traces.append(repair_trace)
        critique = _complete_grounding_decisions(critique, trajectories)
        return SolvedTask(
            task=task,
            trajectories=trajectories,
            traces=traces,
            critic=critique,
            solution_failures=tuple(solution_failures),
        )

    def _solve_one(
        self,
        *,
        job_id: str,
        bundle: PaperBundle,
        graph: PaperEvidenceGraph,
        task: TaskSpec,
        role: str,
        plan: str,
    ) -> tuple[SolutionPayload, list[ProviderTrace], list[TrajectoryTurn], list[ToolCall]]:
        runtime = _paper_runtime(bundle, task)
        transcript: list[dict[str, Any]] = []
        turns = [
            TrajectoryTurn(index=0, role="system", content=_solver_system()),
            TrajectoryTurn(
                index=1,
                role="user",
                content=_solver_prompt(bundle, graph, task, plan),
            ),
        ]
        traces: list[ProviderTrace] = []
        executed: list[ToolCall] = []
        failed_tool_requests: set[bytes] = set()
        turn_index = 0
        while True:
            data, trace = self.control.call(
                job_id=job_id,
                paper_id=bundle.paper_id,
                role=role,
                system=_solver_system(),
                user=_solver_turn_prompt(bundle, graph, task, plan, transcript),
                max_output_tokens=16_000,
                call_key=f"{role}:{task.task_id}:turn:{turn_index}",
            )
            traces.append(trace)
            try:
                turn, repair_trace = _validate_or_repair(
                    control=self.control,
                    model=SolverTurn,
                    data=data,
                    job_id=job_id,
                    paper_id=bundle.paper_id,
                    call_key=f"{role}:{task.task_id}:turn:{turn_index}",
                    context=(
                        "Preserve the scientific solution and tool intent. Omit symbolic or unknown "
                        "quantities from numeric_results instead of assigning null."
                    ),
                    normalizer=_normalize_solver_turn_data,
                )
            except TaskOutputError as exc:
                raise TaskOutputError(str(exc), traces=[*traces, *exc.traces]) from exc
            if repair_trace is not None:
                traces.append(repair_trace)
            turns.append(
                TrajectoryTurn(
                    index=len(turns),
                    role="assistant",
                    content=turn.model_dump(mode="json"),
                )
            )
            if turn.status == "final":
                try:
                    turn, contract_repair_trace = _ensure_solution_contract(
                        control=self.control,
                        turn=turn,
                        job_id=job_id,
                        paper_id=bundle.paper_id,
                        role=role,
                        task=task,
                        graph=graph,
                    )
                except TaskOutputError as exc:
                    raise TaskOutputError(str(exc), traces=[*traces, *exc.traces]) from exc
                if contract_repair_trace is not None:
                    traces.append(contract_repair_trace)
                    turns.append(
                        TrajectoryTurn(
                            index=len(turns),
                            role="assistant",
                            content=turn.model_dump(mode="json"),
                        )
                    )
                assert turn.report is not None
                assert turn.answer_manifest is not None
                return (
                    SolutionPayload(
                        report=turn.report,
                        answer_manifest=turn.answer_manifest,
                        tool_calls=executed,
                    ),
                    traces,
                    turns,
                    executed,
                )
            observations = [_execute_tool(runtime, task, call) for call in turn.tool_calls]
            for observation in observations:
                if observation.error is None:
                    continue
                signature = canonical_json(
                    {"tool": observation.tool, "arguments": observation.arguments}
                )
                if signature in failed_tool_requests:
                    raise TaskOutputError(
                        "solver repeated the same invalid frozen-tool request", traces=traces
                    )
                failed_tool_requests.add(signature)
            executed.extend(observations)
            tool_payload = [value.model_dump(mode="json") for value in observations]
            transcript.append(
                {
                    "assistant": turn.model_dump(mode="json"),
                    "tool_observations": tool_payload,
                }
            )
            turn_index += 1
            if turn_index >= _MAX_SOLVER_TOOL_TURNS:
                raise TaskOutputError(
                    "solver exceeded the bounded frozen-tool interaction budget", traces=traces
                )
            turns.append(TrajectoryTurn(index=len(turns), role="tool", content=tool_payload))


def _complete_grounding_decisions(
    critique: GroundingCritique,
    trajectories: list[Trajectory],
) -> GroundingCritique:
    """Fail closed for missing IDs while keeping every trajectory independently auditable."""
    by_id = {decision.trajectory_id: decision for decision in critique.decisions}
    completed = [
        by_id.get(trajectory.trajectory_id)
        or TrajectoryGroundingDecision(
            trajectory_id=trajectory.trajectory_id,
            scientifically_grounded=False,
            findings=["grounding critic omitted this trajectory"],
            unsupported_claims=["trajectory has no independent scientific grounding decision"],
        )
        for trajectory in trajectories
    ]
    return critique.model_copy(update={"decisions": completed})


def grounding_decision_blocks(decision: TrajectoryGroundingDecision) -> bool:
    """Only substantive scientific errors block a trajectory.

    A critic sometimes emits a negative boolean while its prose says the answer
    is scientifically correct and complains only about an internal identifier or
    manifest shape. Executable validators own those concerns. Requiring an
    format-only finding prevents that proven false-negative mode, while empty
    or substantive negative findings still fail closed.
    """
    if decision.scientifically_grounded:
        return False
    if decision.unsupported_claims or decision.contradictory_claims:
        return True
    if not decision.findings:
        return True
    format_object = (
        r"(?:manifest|schema|node ids?|span ids?|evidence spans?|citations?|relation labels?|"
        r"evidence order|configuration keys?|structured fields?|internal identifiers?)"
    )
    format_problem = r"(?:missing|omits?|absent|format(?:ting)?|schema|order(?:ing)?|requires?)"
    format_only_patterns = (
        re.compile(rf"\b{format_problem}\b.{{0,100}}\b{format_object}\b", re.IGNORECASE),
        re.compile(rf"\b{format_object}\b.{{0,100}}\b{format_problem}\b", re.IGNORECASE),
        re.compile(rf"\bscientifically correct\b.{{0,120}}\b{format_object}\b", re.IGNORECASE),
    )
    return not all(
        any(pattern.search(finding) for pattern in format_only_patterns)
        for finding in decision.findings
    )


def _validate_or_repair(
    *,
    control: ProviderControlPlane,
    model: type[ModelT],
    data: dict[str, Any] | list[Any],
    job_id: str,
    paper_id: str,
    call_key: str,
    context: str,
    normalizer: Callable[[dict[str, Any] | list[Any]], dict[str, Any] | list[Any]] | None = None,
) -> tuple[ModelT, ProviderTrace | None]:
    normalized = normalizer(data) if normalizer is not None else data
    try:
        return model.model_validate(normalized), None
    except ValueError as initial_error:
        repair_data, repair_trace = control.call(
            job_id=job_id,
            paper_id=paper_id,
            role="final_repair",
            system=_structured_repair_system(model),
            user=_structured_repair_prompt(
                model=model,
                data=normalized,
                validation_error=str(initial_error),
                context=context,
            ),
            max_output_tokens=16_000,
            call_key=f"schema_repair:{call_key}",
        )
        repaired = normalizer(repair_data) if normalizer is not None else repair_data
        try:
            return model.model_validate(repaired), repair_trace
        except ValueError as repair_error:
            raise TaskOutputError(
                f"{model.__name__} remained invalid after schema repair: {repair_error}",
                traces=[repair_trace],
            ) from repair_error


def _normalize_solver_turn_data(
    data: dict[str, Any] | list[Any],
) -> dict[str, Any] | list[Any]:
    """Remove non-committal manifest entries that cannot represent executable answers."""
    if not isinstance(data, dict):
        return data
    normalized = dict(data)
    raw_manifest = normalized.get("answer_manifest")
    if not isinstance(raw_manifest, dict):
        return normalized
    manifest = dict(raw_manifest)
    for key in ("claims", "evidence", "method_nodes", "faults", "qualifications"):
        values = manifest.get(key)
        if isinstance(values, list):
            manifest[key] = [value for value in values if isinstance(value, str) and value]

    equations = manifest.get("equations")
    if isinstance(equations, list):
        manifest["equations"] = [
            value
            for value in equations
            if isinstance(value, dict)
            and isinstance(value.get("id"), str)
            and isinstance(value.get("latex"), str)
        ]

    numeric_results = manifest.get("numeric_results")
    if isinstance(numeric_results, list):
        cleaned_results: list[dict[str, Any]] = []
        for value in numeric_results:
            if not isinstance(value, dict) or not isinstance(value.get("id"), str):
                continue
            raw_number = value.get("value")
            if raw_number is None or isinstance(raw_number, bool):
                continue
            try:
                number = float(raw_number)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(number):
                continue
            cleaned_results.append({**value, "value": number})
        manifest["numeric_results"] = cleaned_results

    configuration = manifest.get("configuration")
    if isinstance(configuration, dict):
        manifest["configuration"] = {
            key: value
            for key, value in configuration.items()
            if isinstance(value, (bool, int, float, str))
            and not (isinstance(value, float) and not math.isfinite(value))
        }
    normalized["answer_manifest"] = manifest
    return normalized


def _ensure_solution_contract(
    *,
    control: ProviderControlPlane,
    turn: SolverTurn,
    job_id: str,
    paper_id: str,
    role: str,
    task: TaskSpec,
    graph: PaperEvidenceGraph,
) -> tuple[SolverTurn, ProviderTrace | None]:
    if turn.answer_manifest is None:
        raise TaskOutputError("final solver turn has no answer manifest")
    violations = _solution_contract_violations(turn.answer_manifest, task, graph)
    if not violations:
        return turn, None
    repair_data, repair_trace = control.call(
        job_id=job_id,
        paper_id=paper_id,
        role="final_repair",
        system=_structured_repair_system(SolverTurn),
        user=(
            "Repair this final reference solution's structured manifest without changing its "
            "scientific conclusion. Claims and method nodes use graph node IDs. Numeric expected "
            "values use numeric_results entries with the exact target key. For a derivation task, "
            "string expected values use equations entries with the exact target key; for all "
            "other task families, string expected values use configuration entries. Preserve the "
            "readable report, evidence, and required relations.\n"
            f"CONTRACT_VIOLATIONS:\n{canonical_json(violations).decode()}\n"
            f"TASK:\n{canonical_json(task).decode()}\n"
            f"CURRENT_FINAL_TURN:\n{canonical_json(turn).decode()}"
        ),
        max_output_tokens=16_000,
        call_key=f"solution_contract_repair:{role}:{task.task_id}",
    )
    try:
        repaired = SolverTurn.model_validate(_normalize_solver_turn_data(repair_data))
    except ValueError as exc:
        raise TaskOutputError(
            f"solution-contract repair returned invalid JSON: {exc}", traces=[repair_trace]
        ) from exc
    if repaired.status != "final" or repaired.answer_manifest is None:
        raise TaskOutputError(
            "solution-contract repair did not return a final answer", traces=[repair_trace]
        )
    remaining = _solution_contract_violations(repaired.answer_manifest, task, graph)
    if remaining:
        raise TaskOutputError(
            "solution-contract repair remained incomplete: " + ", ".join(remaining),
            traces=[repair_trace],
        )
    return repaired, repair_trace


def _solution_contract_violations(
    manifest: AnswerManifest,
    task: TaskSpec,
    graph: PaperEvidenceGraph,
) -> list[str]:
    node_ids = {node.id for node in graph.nodes}
    committed_nodes = {
        value
        for value in [
            *manifest.claims,
            *manifest.method_nodes,
            *manifest.faults,
            *manifest.qualifications,
            *(equation.id for equation in manifest.equations),
        ]
        if value in node_ids
    }
    violations: list[str] = []
    missing_nodes = set(task.hidden_targets.required_nodes) - committed_nodes
    if missing_nodes:
        violations.append("missing required node IDs: " + ", ".join(sorted(missing_nodes)))
    if task.family == "derivation_completion":
        node_types = {node.id: node.type for node in graph.nodes}
        submitted_equations = {equation.id for equation in manifest.equations}
        missing_equations = {
            node_id
            for node_id in task.hidden_targets.required_nodes
            if node_types.get(node_id) == "equation" and node_id not in submitted_equations
        }
        if missing_equations:
            violations.append(
                "missing required equation outputs: " + ", ".join(sorted(missing_equations))
            )
    required_relations = {
        (edge.source, edge.relation, edge.target) for edge in task.hidden_targets.required_relations
    }
    submitted_relations = {(edge.source, edge.relation, edge.target) for edge in manifest.relations}
    missing_relations = required_relations - submitted_relations
    if missing_relations:
        violations.append(f"missing {len(missing_relations)} required relations")
    public_spans = {
        *task.public_context_policy.included_spans,
        *task.public_context_policy.same_paper_distractors,
    }
    evidence = set(manifest.evidence)
    if evidence - public_spans:
        violations.append("evidence is outside the public same-paper context")
    if _requires_explicit_evidence(task) and not evidence:
        violations.append("evidence is empty")

    numeric_results = {value.id: value.value for value in manifest.numeric_results}
    equations = {value.id: value.latex for value in manifest.equations}
    for target, expected in task.hidden_targets.expected_values.items():
        if isinstance(expected, (int, float)) and not isinstance(expected, bool):
            actual = numeric_results.get(target)
            if actual is None or not math.isclose(
                actual,
                float(expected),
                rel_tol=1e-6,
                abs_tol=1e-9,
            ):
                violations.append(f"missing or incorrect numeric result {target}")
        elif task.family == "derivation_completion":
            if not equations.get(target):
                violations.append(f"missing symbolic equation {target}")
        elif manifest.configuration.get(target) != expected:
            violations.append(f"missing discrete result {target}")
    return violations


def validate_task(
    task: TaskSpec,
    bundle: PaperBundle,
    graph: PaperEvidenceGraph,
    *,
    oracle_result_ids: set[str] | None = None,
) -> None:
    if task.paper_id != bundle.paper_id:
        raise ValueError(f"task {task.task_id} references a different paper")
    span_ids = {span.span_id for span in bundle.stable_spans}
    node_ids = {node.id for node in graph.nodes}
    context_ids = set(task.public_context_policy.included_spans)
    distractor_ids = set(task.public_context_policy.same_paper_distractors)
    if not context_ids or context_ids - span_ids:
        raise ValueError(f"task {task.task_id} has unresolved public spans")
    if distractor_ids - span_ids:
        raise ValueError(f"task {task.task_id} has external distractors")
    if set(task.hidden_targets.required_nodes) - node_ids:
        raise ValueError(f"task {task.task_id} has unresolved target nodes")
    graph_relations = {(edge.source, edge.relation, edge.target) for edge in graph.edges}
    required_relations = {
        (edge.source, edge.relation, edge.target) for edge in task.hidden_targets.required_relations
    }
    if required_relations - graph_relations:
        raise ValueError(f"task {task.task_id} has unresolved target relations")
    if set(task.hidden_targets.required_qualifications) - node_ids:
        raise ValueError(f"task {task.task_id} has unresolved qualifications")
    if set(task.hidden_targets.required_oracle_results) - (oracle_result_ids or set()):
        raise ValueError(f"task {task.task_id} has unresolved oracle results")
    for evidence_set in task.hidden_targets.accepted_evidence_sets:
        if set(evidence_set) - span_ids:
            raise ValueError(f"task {task.task_id} has unresolved accepted evidence")
    quality_violations = _task_quality_violations(task, graph)
    if quality_violations:
        raise ValueError(f"task {task.task_id} is low value: {'; '.join(quality_violations)}")
    if task.route == "rl" and not _machine_verifiable(
        task,
        graph,
        bundle,
        oracle_result_ids,
    ):
        raise ValueError(f"task {task.task_id} is not sufficiently specified for RL")
    if set(task.hidden_targets.required_faults) & set(task.hidden_targets.forbidden_faults):
        raise ValueError(f"task {task.task_id} requires and forbids the same fault")


def _normalize_task(
    task: TaskSpec,
    bundle: PaperBundle,
    graph: PaperEvidenceGraph,
    trace: ProviderTrace,
    oracle_result_ids: set[str],
) -> TaskSpec:
    task_id = stable_id(
        "paper-task",
        bundle.paper_id,
        task.family,
        sha256(task.public_instruction),
    )
    revised = task.model_copy(
        update={
            "schema_version": "task-spec-v2",
            "content_policy_revision": CONTENT_POLICY_REVISION,
        }
    )
    if _task_quality_violations(revised, graph):
        route = "reject"
    else:
        route = "rl" if _machine_verifiable(revised, graph, bundle, oracle_result_ids) else "sft"
    if revised.family == "grounded_explanation":
        route = "sft"
    return revised.model_copy(
        update={
            "task_id": task_id,
            "paper_id": bundle.paper_id,
            "route": route,
            "construction_provenance": [*task.construction_provenance, trace.trace_id],
        }
    )


def _machine_verifiable(
    task: TaskSpec,
    graph: PaperEvidenceGraph,
    bundle: PaperBundle,
    oracle_result_ids: set[str] | None = None,
) -> bool:
    targets = task.hidden_targets
    if not any(
        (
            targets.required_nodes,
            targets.expected_values,
            targets.required_relations,
            targets.configuration_constraints,
        )
    ):
        return False
    nodes = {node.id: node for node in graph.nodes}
    strict_reasoning = task.content_policy_revision == CONTENT_POLICY_REVISION
    if strict_reasoning and task.difficulty.estimated < 4:
        return False
    if task.family == "derivation_completion":
        equation_nodes = [
            node_id
            for node_id in targets.required_nodes
            if nodes.get(node_id) and nodes[node_id].type == "equation"
        ]
        has_equation_node = bool(equation_nodes)
        derivation_relations = [
            edge
            for edge in targets.required_relations
            if edge.relation in {"derives", "depends_on", "uses", "produces", "enables"}
            and nodes.get(edge.source)
            and nodes.get(edge.target)
            and nodes[edge.source].type == "equation"
            and nodes[edge.target].type == "equation"
        ]
        if strict_reasoning and (len(equation_nodes) < 2 or not derivation_relations):
            return False
        expected_expressions = [
            value for value in targets.expected_values.values() if isinstance(value, str)
        ]
        expected_numbers = [
            value
            for value in targets.expected_values.values()
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        ]
        return bool(
            has_equation_node
            and (expected_numbers or expected_expressions)
            and all(symbolic_expression_is_checkable(value) for value in expected_expressions)
        )
    if task.family == "method_dag":
        method_nodes = [
            node_id
            for node_id in targets.required_nodes
            if nodes.get(node_id) and nodes[node_id].type == "method_step"
        ]
        if strict_reasoning:
            return bool(
                len(method_nodes) >= 4
                and len(targets.required_relations) >= 3
                and (targets.expected_values or targets.configuration_constraints)
            )
        return len(method_nodes) >= 2
    if task.family == "figure_table_reasoning":
        numeric_values = [
            value
            for value in targets.expected_values.values()
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        ]
        if strict_reasoning:
            return bool(len(numeric_values) >= 2 and len(targets.required_nodes) >= 2)
        return bool(targets.expected_values) or any(
            nodes.get(node_id) and nodes[node_id].type in {"figure_value", "table_value", "metric"}
            for node_id in targets.required_nodes
        )
    if task.family == "corruption_diagnosis":
        if strict_reasoning:
            return bool(
                targets.required_faults
                and len(targets.required_relations) >= 2
                and len(targets.required_nodes) >= 3
                and (targets.expected_values or targets.configuration_constraints)
            )
        return bool(targets.required_faults and targets.required_relations)
    if task.family == "assumption_consequence":
        if strict_reasoning:
            return bool(
                len(targets.required_relations) >= 2
                and len(targets.required_nodes) >= 3
                and (targets.expected_values or targets.configuration_constraints)
            )
        return bool(targets.required_relations and targets.required_nodes)
    if task.family == "single_paper_research":
        return bool(
            task.public_context_policy.tool_access
            and (targets.required_nodes or targets.expected_values)
            and targets.accepted_evidence_sets
        )
    if task.family == "experiment_configuration":
        return bool(
            bundle.official_artifacts
            and targets.configuration_constraints
            and targets.required_oracle_results
            and set(targets.required_oracle_results) <= (oracle_result_ids or set())
        )
    if task.family == "result_reproduction":
        return bool(
            bundle.official_artifacts
            and targets.expected_values
            and targets.required_oracle_results
            and set(targets.required_oracle_results) <= (oracle_result_ids or set())
            and all(
                isinstance(value, (int, float)) and not isinstance(value, bool)
                for value in targets.expected_values.values()
            )
        )
    if strict_reasoning:
        return bool(
            targets.accepted_evidence_sets
            and len(targets.required_nodes) >= 2
            and (targets.expected_values or targets.configuration_constraints)
        )
    return bool(targets.accepted_evidence_sets or targets.required_nodes)


_INTERNAL_FORMAT_PATTERNS = (
    re.compile(
        r"\b(?:span|node|equation|method|fault|claim|table|figure|result)\s*ids?\b",
        re.IGNORECASE,
    ),
    re.compile(r"\binternal identifiers?\b", re.IGNORECASE),
    re.compile(r"\[fault:[^\]]+\]", re.IGNORECASE),
    re.compile(r"\bstable paper evidence\b", re.IGNORECASE),
    re.compile(r"\breconstruct the supported direction\b", re.IGNORECASE),
)


def _task_quality_violations(task: TaskSpec, graph: PaperEvidenceGraph) -> list[str]:
    """Reject low-value v2 tasks before spending solver calls."""
    if task.content_policy_revision != CONTENT_POLICY_REVISION:
        return []
    violations: list[str] = []
    instruction = task.public_instruction.strip()
    if task.difficulty.estimated < 3:
        violations.append("difficulty below scientific post-training floor")
    if len(set(task.reasoning_operations)) < 2:
        violations.append("task does not require at least two distinct reasoning operations")
    if any(pattern.search(instruction) for pattern in _INTERNAL_FORMAT_PATTERNS):
        violations.append(
            "public task asks for internal identifiers instead of scientific reasoning"
        )

    nodes = {node.id: node for node in graph.nodes}
    targets = task.hidden_targets
    if task.family == "derivation_completion":
        equation_nodes = [
            node_id
            for node_id in targets.required_nodes
            if nodes.get(node_id) and nodes[node_id].type == "equation"
        ]
        if len(equation_nodes) < 2 or not targets.required_relations:
            violations.append(
                "derivation is direct formula lookup rather than a multi-step derivation"
            )
    elif task.family == "figure_table_reasoning":
        numeric_targets = [
            value
            for value in targets.expected_values.values()
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        ]
        if len(numeric_targets) < 2 or len(targets.required_nodes) < 2:
            violations.append("numeric task is a single lookup or one-step arithmetic exercise")
    elif task.family == "method_dag":
        method_nodes = [
            node_id
            for node_id in targets.required_nodes
            if nodes.get(node_id) and nodes[node_id].type == "method_step"
        ]
        if len(method_nodes) < 3 or len(targets.required_relations) < 2:
            violations.append("method task is simple node or edge listing")
    elif task.family == "corruption_diagnosis":
        if len(targets.required_nodes) < 3 or len(targets.required_relations) < 2:
            violations.append("corruption task is simple graph-edge reversal")
    return violations


def _select_diverse_tasks(tasks: list[TaskSpec], *, limit: int) -> list[TaskSpec]:
    family_priority = {
        "derivation_completion": 0,
        "assumption_consequence": 1,
        "single_paper_research": 2,
        "result_reproduction": 3,
        "experiment_configuration": 4,
        "figure_table_reasoning": 5,
        "claim_evidence": 6,
        "method_dag": 7,
        "grounded_explanation": 7,
        "corruption_diagnosis": 9,
    }
    family_limits = {
        "derivation_completion": 2,
        "assumption_consequence": 2,
        "single_paper_research": 1,
        "result_reproduction": 1,
        "experiment_configuration": 1,
        "figure_table_reasoning": 1,
        "claim_evidence": 1,
        "method_dag": 1,
        "grounded_explanation": 1,
        "corruption_diagnosis": 1,
    }

    def depth(task: TaskSpec) -> int:
        targets = task.hidden_targets
        return (
            task.difficulty.estimated * 10
            + len(set(task.reasoning_operations)) * 4
            + len(targets.expected_values) * 3
            + len(targets.required_relations) * 2
            + len(targets.required_nodes)
        )

    ranked = sorted(
        tasks,
        key=lambda task: (
            family_priority.get(task.family, len(family_priority)),
            -depth(task),
            task.task_id,
        ),
    )
    selected: list[TaskSpec] = []
    family_counts: dict[str, int] = {}
    for task in ranked:
        if family_counts.get(task.family, 0) >= family_limits.get(task.family, 1):
            continue
        selected.append(task)
        family_counts[task.family] = family_counts.get(task.family, 0) + 1
        if len(selected) >= limit:
            return selected
    return selected


def _designer_system() -> str:
    schema = canonical_json(TaskBatch.model_json_schema()).decode()
    return f"""Design frontier-model scientific reasoning TaskSpecs from one evidence graph.
Return strict JSON {{"tasks": [...]}} and use only supplied paper evidence. Optimize first for:
1. multi-step derivations in which at least two equations are transformed or composed;
2. scaling-law inference that derives exponents, coefficients, regimes, or consequences;
3. numerical synthesis across multiple rows, columns, figures, ablations, or conditions;
4. assumption and failure-mode analysis that propagates a changed premise through several claims;
5. algorithm analysis comparing complexity, convergence, invariants, or design tradeoffs.

An RL proposal must be difficulty 4 or 5, require at least two distinct reasoning operations, expose a
finite answer-facing numeric, symbolic, or discrete outcome in hidden_targets.expected_values or
configuration_constraints, and support a deterministic verifier. A derivation must contain a parseable
canonical expression and a genuine equation dependency chain. Public instructions must read like natural
scientific questions. Never ask the learner to list node IDs, equation IDs, span IDs, reverse one graph edge,
copy one value, identify the largest cell, or perform a single subtraction or percentage calculation. Do not
manufacture complexity by demanding internal manifest fields. Route valuable open-ended synthesis to SFT;
omit low-value or underspecified tasks entirely. Method DAG and corruption families are last-resort tasks and
must involve a multi-step scientific failure or algorithmic chain, not schema reconstruction. Official-artifact
configuration or reproduction tasks require audited oracle results. Keep answers hidden, context paper-local,
and distractors same-paper only. The response must validate exactly against REQUIRED_JSON_SCHEMA.
REQUIRED_JSON_SCHEMA:
{schema}"""


def _structured_repair_system(model: type[BaseModel]) -> str:
    schema = canonical_json(model.model_json_schema()).decode()
    return f"""Repair one model-authored structured response so it validates exactly against the
required JSON schema. Preserve supported scientific content and identifiers. Do not introduce new
claims, evidence, calculations, tool observations, or external knowledge. Remove fields that cannot
be represented truthfully, including unknown or symbolic values placed in numeric-only fields.
Return only the repaired JSON object.
REQUIRED_JSON_SCHEMA:
{schema}"""


def _structured_repair_prompt(
    *,
    model: type[BaseModel],
    data: dict[str, Any] | list[Any],
    validation_error: str,
    context: str,
) -> str:
    return (
        f"TARGET_TYPE: {model.__name__}\n"
        f"REPAIR_CONSTRAINT: {context}\n"
        f"VALIDATION_ERROR:\n{validation_error}\n"
        f"INVALID_RESPONSE:\n{canonical_json(data).decode()}"
    )


def _designer_prompt(
    bundle: PaperBundle,
    graph: PaperEvidenceGraph,
    count: int,
    oracle_result_ids: set[str],
) -> str:
    supporting_spans = {span_id for node in graph.nodes for span_id in node.supporting_spans}
    return (
        f"Propose exactly {count} materially different TaskSpecs. Cover the strongest supported "
        "scientific problems, prioritizing derivation, scaling-law or regime inference, multi-result "
        "numerical synthesis, assumption consequences, and algorithm analysis. At least half of the "
        "proposals should come from those high-value categories when the graph supports them. Do not "
        "fill the count with low-value tasks: return fewer tasks when necessary. Configuration or "
        "result-reproduction tasks require an audited official artifact. Route valuable but non-finite "
        "work to SFT.\n"
        f"AVAILABLE_PRIVATE_ORACLE_RESULT_IDS:\n{canonical_json(sorted(oracle_result_ids)).decode()}\n"
        f"PAPER_BUNDLE:\n{bundle_prompt_json(bundle, span_ids=supporting_spans).decode()}\n"
        f"EVIDENCE_GRAPH:\n{canonical_json(graph).decode()}"
    )


def _answerability_system() -> str:
    schema = canonical_json(AnswerabilityBatch.model_json_schema()).decode()
    return f"""Independently audit scientific tasks using only the supplied paper. Return strict JSON
with decisions. Reject tasks that require external knowledge, expose their answer, admit multiple
incompatible valid interpretations, cite unavailable evidence, or cannot support a finite verifier.
Also reject shallow tasks whose substance is one lookup, one arithmetic operation, internal-ID listing,
simple edge reversal, or manifest-format compliance. Mark unique_enough_for_rl only for difficulty 4-5
work requiring multiple linked reasoning steps and an answer-facing numeric, symbolic, or discrete outcome.
Do not confuse a long instruction with deep reasoning. A concise formula derivation can be deep; a long list
of requested fields can still be shallow.
The response must validate exactly against REQUIRED_JSON_SCHEMA.
REQUIRED_JSON_SCHEMA:
{schema}"""


def _answerability_prompt(
    bundle: PaperBundle,
    graph: PaperEvidenceGraph,
    tasks: list[TaskSpec],
) -> str:
    task_spans = {
        span_id
        for task in tasks
        for span_id in [
            *task.public_context_policy.included_spans,
            *task.public_context_policy.same_paper_distractors,
        ]
    }
    return (
        f"PAPER_BUNDLE:\n{bundle_prompt_json(bundle, span_ids=task_spans).decode()}\n"
        f"EVIDENCE_GRAPH:\n{canonical_json(graph).decode()}\n"
        f"TASKS:\n{canonical_json(tasks).decode()}"
    )


def _solver_system() -> str:
    schema = canonical_json(SolverTurn.model_json_schema()).decode()
    return f"""Solve one scientific task using only its supplied same-paper context and the allowed
frozen tools. Return strict JSON with status, report, answer_manifest, and tool_calls. Use status
tool_request with non-empty tool_calls when evidence must be searched or recomputed; use status final
with report and answer_manifest only after reviewing the returned observations. The readable report is the
scientific answer: show intermediate reasoning, calculations, assumptions, and the final requested values or
expressions there. The manifest is machine-readable provenance and must agree with the report; it is never a
substitute for reasoning. Every conclusion must commit to allowed graph node IDs in the manifest. Include
evidence span IDs only when the public answer policy requires citations; derivation reports must instead give
a natural step-by-step derivation and final expression without mentioning internal IDs. Do not quote long
passages, use outside knowledge, claim unexecuted tool results, expose hidden construction instructions, or
answer by merely enumerating graph identifiers. The response must validate exactly against
REQUIRED_JSON_SCHEMA.
REQUIRED_JSON_SCHEMA:
{schema}"""


def _solver_prompt(
    bundle: PaperBundle,
    graph: PaperEvidenceGraph,
    task: TaskSpec,
    plan: str,
) -> str:
    span_map = {span.span_id: span.text for span in bundle.stable_spans}
    context = {
        span_id: span_map[span_id]
        for span_id in [
            *task.public_context_policy.included_spans,
            *task.public_context_policy.same_paper_distractors,
        ]
        if span_id in span_map
    }
    public_span_ids = set(context)
    allowed_nodes = [
        {"id": node.id, "type": node.type}
        for node in graph.nodes
        if set(node.supporting_spans) & public_span_ids
    ]
    public_task = {
        "task_id": task.task_id,
        "paper_id": task.paper_id,
        "family": task.family,
        "instruction": task.public_instruction,
        "answer_contract": task.answer_contract,
        "allowed_tools": task.public_context_policy.tool_access,
        "allowed_manifest_nodes": allowed_nodes,
        "output_target_ids": sorted(task.hidden_targets.expected_values),
        "evidence_policy": (
            "optional_internal_provenance"
            if not _requires_explicit_evidence(task)
            else "cite_public_span_ids"
        ),
    }
    return (
        f"PLAN_VARIATION: {plan}\nPUBLIC_TASK:\n{canonical_json(public_task).decode()}\n"
        f"PUBLIC_CONTEXT:\n{canonical_json(context).decode()}"
    )


def _requires_explicit_evidence(task: TaskSpec) -> bool:
    """Citations are an answer skill, not a universal scientific-task tax."""
    return task.family != "derivation_completion"


def _solver_turn_prompt(
    bundle: PaperBundle,
    graph: PaperEvidenceGraph,
    task: TaskSpec,
    plan: str,
    transcript: list[dict[str, Any]],
) -> str:
    return (
        _solver_prompt(bundle, graph, task, plan)
        + "\nALLOWED_TOOLS:\n"
        + canonical_json(task.public_context_policy.tool_access).decode()
        + "\nPRIOR_TOOL_TURNS:\n"
        + canonical_json(transcript).decode()
    )


def _paper_runtime(bundle: PaperBundle, task: TaskSpec) -> PaperRuntime:
    span_ids = {
        *task.public_context_policy.included_spans,
        *task.public_context_policy.same_paper_distractors,
    }
    return PaperRuntime(
        spans={span.span_id: span.text for span in bundle.stable_spans if span.span_id in span_ids},
        equations={
            value.equation_id: value.model_dump(mode="json")
            for value in bundle.equations
            if set(value.source_span_ids) & span_ids
        },
        tables={
            value.table_id: value.model_dump(mode="json")
            for value in bundle.tables
            if set(value.source_span_ids) & span_ids
        },
    )


def _execute_tool(runtime: PaperRuntime, task: TaskSpec, call: ToolCall) -> ToolCall:
    if call.tool not in task.public_context_policy.tool_access:
        return call.model_copy(
            update={"observation": None, "error": f"tool {call.tool} is not allowed"}
        )
    try:
        observation: Any
        if call.tool == "search":
            observation = runtime.search(**call.arguments)
        elif call.tool == "open":
            observation = runtime.open(**call.arguments)
        elif call.tool == "find":
            observation = runtime.find(**call.arguments)
        elif call.tool == "calculator":
            observation = runtime.calculator(**call.arguments)
        elif call.tool == "symbolic":
            observation = runtime.symbolic(**call.arguments)
        else:
            raise ToolError(f"unsupported tool {call.tool}")
        return call.model_copy(update={"observation": observation, "error": None})
    except (ToolError, TypeError, ValueError) as exc:
        return call.model_copy(update={"observation": None, "error": str(exc)})


def _grounding_system() -> str:
    schema = canonical_json(GroundingCritique.model_json_schema()).decode()
    return f"""Audit each independently generated reference trajectory against one paper and task.
Return one decisions entry for every supplied trajectory_id plus task-level findings. For each trajectory,
set scientifically_grounded=false only when its readable scientific answer contains a specific unsupported
claim, contradiction, wrong calculation, or materially incomplete conclusion, and list that concrete error
under unsupported_claims or contradictory_claims. Judge trajectories independently: one bad solution must
never reject a good paired solution. Do not reject for missing node IDs, relation labels, evidence ordering,
configuration keys, manifest shape, or other format concerns; executable deterministic validators own those
checks. Do not require two solutions to agree. A correct but differently worded solution is grounded. Your
vote cannot override deterministic security, replay, adversarial, mutation, symbolic, or numeric checks.
The response must validate exactly against
REQUIRED_JSON_SCHEMA.
REQUIRED_JSON_SCHEMA:
{schema}"""


def _grounding_prompt(
    bundle: PaperBundle,
    graph: PaperEvidenceGraph,
    task: TaskSpec,
    trajectories: list[Trajectory],
) -> str:
    task_spans = {
        *task.public_context_policy.included_spans,
        *task.public_context_policy.same_paper_distractors,
    }
    return (
        f"PAPER_BUNDLE:\n{bundle_prompt_json(bundle, span_ids=task_spans).decode()}\n"
        f"GRAPH:\n{canonical_json(graph).decode()}\nTASK:\n{canonical_json(task).decode()}\n"
        f"SOLUTIONS:\n{canonical_json(trajectories).decode()}"
    )


__all__ = [
    "CONTENT_POLICY_REVISION",
    "AnswerabilityBatch",
    "GroundingCritique",
    "SolutionPayload",
    "SolvedTask",
    "SolverFailure",
    "TaskBatch",
    "TaskFactory",
    "TaskOutputError",
    "TrajectoryGroundingDecision",
    "grounding_decision_blocks",
    "validate_task",
]
