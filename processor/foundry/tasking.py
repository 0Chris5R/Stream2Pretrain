"""Task proposal, routing, independent solution, and grounding pipeline."""

from __future__ import annotations

import math
from collections.abc import Callable
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
    Difficulty,
    EvidenceEdge,
    FoundryAnswer,
    HiddenTargets,
    OracleResult,
    PaperBundle,
    PaperEvidenceGraph,
    ProviderTrace,
    PublicContextPolicy,
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


class GroundingCritique(BaseModel):
    model_config = ConfigDict(extra="forbid")
    accepted: bool
    findings: list[str] = Field(default_factory=list)
    unsupported_claims: list[str] = Field(default_factory=list)
    contradictory_claims: list[str] = Field(default_factory=list)


class TaskOutputError(ValueError):
    """A model-authored task artifact remained invalid after one bounded repair."""


_MAX_SOLVER_TOOL_TURNS = 8


@dataclass(frozen=True, slots=True)
class SolvedTask:
    task: TaskSpec
    trajectories: list[Trajectory]
    traces: list[ProviderTrace]
    critic: GroundingCritique
    solution_failures: tuple[str, ...] = ()


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
        if not any(task.family == "corruption_diagnosis" for task in normalized):
            corruption = _deterministic_corruption_task(
                bundle=bundle,
                graph=graph,
                designer_trace=designer_trace,
            )
            if corruption is not None:
                normalized.append(corruption)
        validated: list[TaskSpec] = []
        for task in normalized:
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
        solution_failures: list[str] = []
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
                solution_failures.append(f"{role}: {exc}")
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
                f"task {task.task_id} produced no valid solution after bounded repairs"
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
        critique, repair_trace = _validate_or_repair(
            control=self.control,
            model=GroundingCritique,
            data=critic_data,
            job_id=job_id,
            paper_id=bundle.paper_id,
            call_key=f"grounding_critic:{task.task_id}",
            context="Preserve the grounding audit and repair only schema violations.",
        )
        if repair_trace is not None:
            traces.append(repair_trace)
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
                turn, contract_repair_trace = _ensure_solution_contract(
                    control=self.control,
                    turn=turn,
                    job_id=job_id,
                    paper_id=bundle.paper_id,
                    role=role,
                    task=task,
                    graph=graph,
                )
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
                    raise TaskOutputError("solver repeated the same invalid frozen-tool request")
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
                raise TaskOutputError("solver exceeded the bounded frozen-tool interaction budget")
            turns.append(TrajectoryTurn(index=len(turns), role="tool", content=tool_payload))


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
                f"{model.__name__} remained invalid after schema repair: {repair_error}"
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
        raise TaskOutputError(f"solution-contract repair returned invalid JSON: {exc}") from exc
    if repaired.status != "final" or repaired.answer_manifest is None:
        raise TaskOutputError("solution-contract repair did not return a final answer")
    remaining = _solution_contract_violations(repaired.answer_manifest, task, graph)
    if remaining:
        raise TaskOutputError(
            "solution-contract repair remained incomplete: " + ", ".join(remaining)
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
    route = "rl" if _machine_verifiable(task, graph, bundle, oracle_result_ids) else "sft"
    if task.family == "grounded_explanation":
        route = "sft"
    return task.model_copy(
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
    if task.family == "derivation_completion":
        has_equation_node = any(
            nodes.get(node_id) and nodes[node_id].type == "equation"
            for node_id in targets.required_nodes
        )
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
        return (
            len(
                [
                    node_id
                    for node_id in targets.required_nodes
                    if nodes.get(node_id) and nodes[node_id].type == "method_step"
                ]
            )
            >= 2
        )
    if task.family == "figure_table_reasoning":
        return bool(targets.expected_values) or any(
            nodes.get(node_id) and nodes[node_id].type in {"figure_value", "table_value", "metric"}
            for node_id in targets.required_nodes
        )
    if task.family == "corruption_diagnosis":
        return bool(targets.required_faults and targets.required_relations)
    if task.family == "assumption_consequence":
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
    return bool(targets.accepted_evidence_sets or targets.required_nodes)


def _deterministic_corruption_task(
    *,
    bundle: PaperBundle,
    graph: PaperEvidenceGraph,
    designer_trace: ProviderTrace,
) -> TaskSpec | None:
    nodes = {node.id: node for node in graph.nodes}
    edge = next(
        (
            value
            for value in graph.edges
            if value.relation in {"precedes", "derives", "depends_on", "enables"}
            and value.source in nodes
            and value.target in nodes
        ),
        None,
    )
    if edge is None:
        return None
    source = nodes[edge.source]
    target = nodes[edge.target]
    spans = list(dict.fromkeys([*source.supporting_spans, *target.supporting_spans]))
    if not spans:
        return None
    fault_id = stable_id(
        "fault",
        bundle.paper_id,
        edge.source,
        edge.relation,
        edge.target,
    )
    false_relation = EvidenceEdge(
        source=edge.target,
        relation=edge.relation,
        target=edge.source,
    )
    instruction = (
        f"The candidate relation [{fault_id}] asserts that {false_relation.source!r} "
        f"{false_relation.relation} {false_relation.target!r}. Identify the planted fault, "
        "reconstruct the supported direction, and cite the stable paper evidence."
    )
    return TaskSpec(
        task_id=stable_id("paper-task", bundle.paper_id, "corruption_diagnosis", fault_id),
        paper_id=bundle.paper_id,
        family="corruption_diagnosis",
        public_instruction=instruction,
        public_context_policy=PublicContextPolicy(included_spans=spans),
        hidden_targets=HiddenTargets(
            required_nodes=[edge.source, edge.target],
            required_relations=[edge],
            accepted_evidence_sets=[spans],
            required_faults=[fault_id],
        ),
        verifier_class="planted_relation_fault_v1",
        difficulty=Difficulty(estimated=3, sources=["deterministic_relation_corruption"]),
        reasoning_operations=["fault_identification", "relation_reconstruction"],
        construction_provenance=[
            designer_trace.trace_id,
            "deterministic-corruption-v1",
        ],
        route="rl",
    )


def _select_diverse_tasks(tasks: list[TaskSpec], *, limit: int) -> list[TaskSpec]:
    family_priority = {
        "derivation_completion": 0,
        "figure_table_reasoning": 1,
        "assumption_consequence": 2,
        "corruption_diagnosis": 3,
        "claim_evidence": 4,
        "single_paper_research": 5,
        "method_dag": 6,
        "grounded_explanation": 7,
        "experiment_configuration": 8,
        "result_reproduction": 9,
    }
    ranked = sorted(
        tasks,
        key=lambda task: (
            family_priority.get(task.family, len(family_priority)),
            -task.difficulty.estimated,
            task.task_id,
        ),
    )
    selected: list[TaskSpec] = []
    seen_families: set[str] = set()
    for task in ranked:
        if task.family in seen_families:
            continue
        selected.append(task)
        seen_families.add(task.family)
        if len(selected) >= limit:
            return selected
    for task in ranked:
        if task not in selected:
            selected.append(task)
        if len(selected) >= limit:
            break
    return selected


def _designer_system() -> str:
    schema = canonical_json(TaskBatch.model_json_schema()).decode()
    return f"""Design high-value scientific post-training TaskSpecs from one hidden evidence graph.
Return strict JSON {{"tasks": [...]}}. Use only supplied stable spans. Propose the requested mixture
across claim/evidence, derivation, method DAG, figure/table, corruption diagnosis,
assumption/consequence, long single-paper research, and grounded SFT reasoning where evidence
permits. Prefer answerable formula derivations, scaling-law calculations, numeric transformations,
factorizations, approximations, and figure/table synthesis over another routine method DAG when the
paper supports them. When audited official artifacts are present, also consider experiment configuration and
result reproduction. Separate public context from hidden targets, add same-paper distractors only,
avoid answer leakage, and reject underspecified families rather than inventing. A derivation task must
provide a canonical, parseable LaTeX expected expression or equality for deterministic checking, but its
public instruction must ask for a normal mathematical derivation rather than span-ID citations. The response must
validate exactly against REQUIRED_JSON_SCHEMA.
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
        "families, including the five deterministic RL templates and a long paper-local tool task. "
        "Configuration or result-reproduction tasks require an audited official artifact. Route "
        "valuable but non-finite work to SFT.\n"
        f"AVAILABLE_PRIVATE_ORACLE_RESULT_IDS:\n{canonical_json(sorted(oracle_result_ids)).decode()}\n"
        f"PAPER_BUNDLE:\n{bundle_prompt_json(bundle, span_ids=supporting_spans).decode()}\n"
        f"EVIDENCE_GRAPH:\n{canonical_json(graph).decode()}"
    )


def _answerability_system() -> str:
    schema = canonical_json(AnswerabilityBatch.model_json_schema()).decode()
    return f"""Independently audit scientific tasks using only the supplied paper. Return strict JSON
with decisions. Reject tasks that require external knowledge, expose their answer, admit multiple
incompatible valid interpretations, cite unavailable evidence, or cannot support a finite verifier.
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
with report and answer_manifest only after reviewing the returned observations. Every conclusion must
commit to allowed graph node IDs. Include evidence span IDs only when the public answer policy requires
citations; derivation tasks instead require a natural step-by-step derivation and final expression. Do not
quote long passages, use outside knowledge, claim unexecuted
tool results, or expose hidden construction instructions. The response must validate exactly against
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
    return f"""Audit the available independently generated reference solutions against one paper
and task. Return strict
JSON with accepted, findings, unsupported_claims, contradictory_claims. Check
manifest/prose consistency, exact evidence support, calculations, completeness, and scientific value.
Your vote cannot override deterministic checks. The response must validate exactly against
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
    "AnswerabilityBatch",
    "GroundingCritique",
    "SolutionPayload",
    "SolvedTask",
    "TaskBatch",
    "TaskFactory",
    "TaskOutputError",
    "validate_task",
]
