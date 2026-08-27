"""Multi-pass hidden PaperEvidenceGraph compiler and independent critic."""

from __future__ import annotations

from collections.abc import Callable, Hashable
from typing import TypeVar

from pydantic import BaseModel, ConfigDict, Field

from processor.foundry.control import ProviderControlPlane
from processor.foundry.paper_adapter import bundle_prompt_json
from processor.foundry.util import canonical_json, sha256, stable_id
from schemas.foundry import (
    CompilerRun,
    EvidenceEdge,
    EvidenceNode,
    OracleResult,
    PaperBundle,
    PaperEvidenceGraph,
    ProviderTrace,
)

_MAX_PATCH_NODES = 24
_MAX_PATCH_EDGES = 40
_MAX_PATCH_NOTES = 12
_T = TypeVar("_T")


class GraphPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    nodes: list[EvidenceNode] = Field(default_factory=list)
    edges: list[EvidenceEdge] = Field(default_factory=list)
    uncertainties: list[str] = Field(default_factory=list)
    conflicts: list[str] = Field(default_factory=list)
    remove_node_ids: list[str] = Field(default_factory=list)
    remove_edges: list[EvidenceEdge] = Field(default_factory=list)


class BoundedGraphPatch(GraphPatch):
    """One prioritized incremental compiler delta sized for structured output."""

    nodes: list[EvidenceNode] = Field(default_factory=list, max_length=_MAX_PATCH_NODES)
    edges: list[EvidenceEdge] = Field(default_factory=list, max_length=_MAX_PATCH_EDGES)
    uncertainties: list[str] = Field(default_factory=list, max_length=_MAX_PATCH_NOTES)
    conflicts: list[str] = Field(default_factory=list, max_length=_MAX_PATCH_NOTES)
    remove_node_ids: list[str] = Field(default_factory=list, max_length=_MAX_PATCH_NODES)
    remove_edges: list[EvidenceEdge] = Field(default_factory=list, max_length=_MAX_PATCH_EDGES)


class GraphCritique(BaseModel):
    model_config = ConfigDict(extra="forbid")

    accepted: bool
    findings: list[str] = Field(default_factory=list)
    invalid_node_ids: list[str] = Field(default_factory=list)
    missing_evidence: list[str] = Field(default_factory=list)
    invalid_relations: list[str] = Field(default_factory=list)
    repair_instructions: list[str] = Field(default_factory=list)


_PASSES: tuple[tuple[str, str, str], ...] = (
    (
        "structure",
        "structure_compiler",
        "Recover only explicit entities, equations, figures, tables, method steps, findings, limitations, inputs, outputs, and resources.",
    ),
    (
        "atomic_claim",
        "claim_compiler",
        "Add atomic independently checkable claims and assumptions. Split compound statements and mark inference explicitly in metadata.",
    ),
    (
        "evidence",
        "evidence_compiler",
        "Attach exact supporting and qualifying stable span IDs to every claim, finding, limitation, and method step. Remove unsupported nodes.",
    ),
    (
        "dependency",
        "dependency_compiler",
        "Add derivation, prerequisite, causal, comparison, assumption, method-order, input, and output edges using only existing node IDs.",
    ),
    (
        "canonicalization",
        "canonicalization_compiler",
        "Canonicalize equations, units, identifiers, method names, table values, and accepted equivalence classes.",
    ),
    (
        "conflict",
        "conflict_compiler",
        "Identify caveats, changing definitions, contradictory results, negative evidence, and unresolved ambiguity without forcing agreement.",
    ),
)


class EvidenceGraphCompiler:
    def __init__(self, control: ProviderControlPlane) -> None:
        self.control = control

    def compile(
        self,
        *,
        job_id: str,
        bundle: PaperBundle,
        oracle_results: list[OracleResult] | None = None,
    ) -> tuple[PaperEvidenceGraph, list[ProviderTrace]]:
        graph = PaperEvidenceGraph(
            graph_id=stable_id("evidence-graph", bundle.paper_id, bundle.paper_hash),
            paper_id=bundle.paper_id,
            nodes=[],
            edges=[],
        )
        traces: list[ProviderTrace] = []
        for pass_name, role, instruction in _PASSES:
            data, trace = self.control.call(
                job_id=job_id,
                paper_id=bundle.paper_id,
                role=role,
                system=_pass_system_prompt(),
                user=_pass_prompt(
                    bundle,
                    graph,
                    pass_name,
                    instruction,
                    oracle_results or [],
                ),
                max_output_tokens=8_000,
            )
            traces.append(trace)
            patch, bounding_findings = _normalize_bounded_patch(data, graph)
            graph = _merge_patch(
                graph,
                patch,
                pass_name,
                trace,
                findings=bounding_findings,
            )
            validate_graph_against_bundle(graph, bundle)

        critique_data, critic_trace = self.control.call(
            job_id=job_id,
            paper_id=bundle.paper_id,
            role="graph_critic",
            system=_critic_system_prompt(),
            user=_critic_prompt(bundle, graph, oracle_results or []),
            max_output_tokens=6_000,
        )
        traces.append(critic_trace)
        critique = GraphCritique.model_validate(critique_data)
        if critique.invalid_node_ids or critique.missing_evidence or critique.invalid_relations:
            critique = critique.model_copy(update={"accepted": False})
        graph = graph.model_copy(
            update={
                "compiler_runs": [
                    *graph.compiler_runs,
                    CompilerRun(
                        pass_name="independent_critic",
                        prompt_version=self.control.config.prompt_version,
                        provider_trace_id=critic_trace.trace_id,
                        output_hash=sha256(critique_data),
                        accepted=critique.accepted,
                        findings=critique.findings,
                    ),
                ]
            }
        )
        if not critique.accepted:
            repaired_data, repair_trace = self.control.call(
                job_id=job_id,
                paper_id=bundle.paper_id,
                role="graph_repair",
                system=_repair_system_prompt(),
                user=_repair_prompt(bundle, graph, critique, oracle_results or []),
                max_output_tokens=6_000,
            )
            traces.append(repair_trace)
            repaired, bounding_findings = _normalize_bounded_patch(repaired_data, graph)
            graph = _merge_patch(
                graph,
                repaired,
                "repair",
                repair_trace,
                findings=[*critique.repair_instructions, *bounding_findings],
            )
            validate_graph_against_bundle(graph, bundle)
            recheck_data, recheck_trace = self.control.call(
                job_id=job_id,
                paper_id=bundle.paper_id,
                role="graph_critic",
                system=_critic_system_prompt(),
                user=_critic_prompt(bundle, graph, oracle_results or []),
                max_output_tokens=6_000,
                call_key="graph_critic:post_repair",
            )
            traces.append(recheck_trace)
            recheck = GraphCritique.model_validate(recheck_data)
            if (
                not recheck.accepted
                or recheck.invalid_node_ids
                or recheck.missing_evidence
                or recheck.invalid_relations
            ):
                raise ValueError("independent graph critic rejected the bounded repair")
        if not graph.nodes:
            raise ValueError("evidence compiler produced an empty graph")
        return graph, traces


def _normalize_bounded_patch(
    data: object,
    graph: PaperEvidenceGraph,
) -> tuple[BoundedGraphPatch, list[str]]:
    """Validate, deduplicate, and bound a provider patch without inventing content.

    The compiler prompt requires the provider to return entries in priority order. Some
    providers do not enforce JSON Schema array limits during generation, so validate the
    complete unbounded shape first and preserve that declared order while retaining the
    bounded prefix. Edges that cannot resolve after the same bounded patch are skipped
    before they consume the edge budget.
    """

    raw = GraphPatch.model_validate(data)
    nodes = _first_unique(raw.nodes, key=lambda node: node.id)[:_MAX_PATCH_NODES]
    remove_node_ids = _first_unique(raw.remove_node_ids, key=lambda value: value)[:_MAX_PATCH_NODES]

    node_ids = {node.id for node in graph.nodes}
    node_ids.difference_update(remove_node_ids)
    node_ids.update(node.id for node in nodes)
    resolvable_edges = [
        edge for edge in raw.edges if edge.source in node_ids and edge.target in node_ids
    ]
    edges = _first_unique(
        resolvable_edges,
        key=lambda edge: (edge.source, edge.relation, edge.target),
    )[:_MAX_PATCH_EDGES]
    remove_edges = _first_unique(
        raw.remove_edges,
        key=lambda edge: (edge.source, edge.relation, edge.target),
    )[:_MAX_PATCH_EDGES]
    uncertainties = _first_unique(raw.uncertainties, key=lambda value: value)[:_MAX_PATCH_NOTES]
    conflicts = _first_unique(raw.conflicts, key=lambda value: value)[:_MAX_PATCH_NOTES]

    patch = BoundedGraphPatch(
        nodes=nodes,
        edges=edges,
        uncertainties=uncertainties,
        conflicts=conflicts,
        remove_node_ids=remove_node_ids,
        remove_edges=remove_edges,
    )
    findings: list[str] = []
    _record_bound(findings, "nodes", len(raw.nodes), len(nodes))
    _record_bound(findings, "edges", len(raw.edges), len(edges))
    _record_bound(findings, "uncertainties", len(raw.uncertainties), len(uncertainties))
    _record_bound(findings, "conflicts", len(raw.conflicts), len(conflicts))
    _record_bound(
        findings,
        "remove_node_ids",
        len(raw.remove_node_ids),
        len(remove_node_ids),
    )
    _record_bound(findings, "remove_edges", len(raw.remove_edges), len(remove_edges))
    return patch, findings


def _first_unique(
    values: list[_T],
    *,
    key: Callable[[_T], Hashable],
) -> list[_T]:
    """Return exact provider values in their declared priority order once each."""

    seen: set[Hashable] = set()
    unique: list[_T] = []
    for value in values:
        identity = key(value)
        if identity in seen:
            continue
        seen.add(identity)
        unique.append(value)
    return unique


def _record_bound(findings: list[str], field: str, returned: int, retained: int) -> None:
    if returned != retained:
        findings.append(
            f"deterministically bounded provider patch {field}: retained {retained} "
            f"of {returned} returned entries"
        )


def validate_graph_against_bundle(graph: PaperEvidenceGraph, bundle: PaperBundle) -> None:
    span_ids = {span.span_id for span in bundle.stable_spans}
    node_ids: set[str] = set()
    for node in graph.nodes:
        if node.id in node_ids:
            raise ValueError(f"duplicate graph node id {node.id}")
        node_ids.add(node.id)
        unknown = set(node.supporting_spans) - span_ids
        if unknown:
            raise ValueError(f"node {node.id} cites unknown spans: {sorted(unknown)}")
        if (
            node.type in {"claim", "finding", "limitation", "method_step"}
            and not node.supporting_spans
        ):
            raise ValueError(f"groundable node {node.id} has no source span")
    for edge in graph.edges:
        if edge.source not in node_ids or edge.target not in node_ids:
            raise ValueError(f"edge {edge.source}->{edge.target} does not resolve")


def _merge_patch(
    graph: PaperEvidenceGraph,
    patch: GraphPatch,
    pass_name: str,
    trace: ProviderTrace,
    *,
    findings: list[str] | None = None,
) -> PaperEvidenceGraph:
    nodes = {node.id: node for node in graph.nodes}
    for node_id in patch.remove_node_ids:
        nodes.pop(node_id, None)
    nodes.update({node.id: node for node in patch.nodes})
    edges = {
        (edge.source, edge.relation, edge.target): edge for edge in [*graph.edges, *patch.edges]
    }
    for edge in patch.remove_edges:
        edges.pop((edge.source, edge.relation, edge.target), None)
    edges = {
        key: edge for key, edge in edges.items() if edge.source in nodes and edge.target in nodes
    }
    run = CompilerRun(
        pass_name=pass_name,
        prompt_version=trace.prompt_version,
        provider_trace_id=trace.trace_id,
        output_hash=trace.response_hash,
        accepted=True,
        findings=findings or [],
    )
    return PaperEvidenceGraph(
        graph_id=graph.graph_id,
        paper_id=graph.paper_id,
        nodes=list(nodes.values()),
        edges=list(edges.values()),
        uncertainties=list(dict.fromkeys([*graph.uncertainties, *patch.uncertainties])),
        conflicts=list(dict.fromkeys([*graph.conflicts, *patch.conflicts])),
        compiler_runs=[*graph.compiler_runs, run],
    )


def _pass_system_prompt() -> str:
    schema = canonical_json(BoundedGraphPatch.model_json_schema()).decode()
    return f"""You compile a hidden scientific evidence graph. Use only the supplied PaperBundle.
Return one prioritized incremental JSON patch that validates exactly against REQUIRED_JSON_SCHEMA
below. Add at most 24 nodes and 40 edges in this pass, do not restate existing nodes, and prefer
the evidence most useful for difficult grounded reasoning tasks over exhaustive transcription.
Do not rename fields. In particular, EvidenceNode uses canonical_text and supporting_spans, never
text or spans. Use only the node-type and edge-relation enum values in the schema. uncertainties
and conflicts are arrays of strings, not objects. Never use outside knowledge, invent missing
experimental details, or cite a span ID absent from the bundle. Separate explicit statements from
inference and leave ambiguous regions uncertain.
REQUIRED_JSON_SCHEMA:
{schema}"""


def _repair_system_prompt() -> str:
    schema = canonical_json(BoundedGraphPatch.model_json_schema()).decode()
    return f"""You repair a hidden scientific evidence graph. Use only the supplied PaperBundle.
Return one prioritized incremental JSON patch that validates exactly against REQUIRED_JSON_SCHEMA
below. Correct only the critic findings: omit unchanged nodes and edges, replace a node by emitting
its corrected version with the same ID, and use remove_node_ids or remove_edges for deletions. Add
at most 24 nodes and 40 edges. Do not rename fields. In particular, EvidenceNode uses
canonical_text and supporting_spans, never text or spans. Use only the node-type and edge-relation
enum values in the schema. uncertainties and conflicts are arrays of strings, not objects. Never
use outside knowledge, invent missing experimental details, or cite a span ID absent from the
bundle. Separate explicit statements from inference and leave ambiguous regions uncertain.
REQUIRED_JSON_SCHEMA:
{schema}"""


def _critic_system_prompt() -> str:
    schema = canonical_json(GraphCritique.model_json_schema()).decode()
    return f"""You are a fresh scientific grounding critic with no access to compiler reasoning.
Return one JSON object that validates exactly against REQUIRED_JSON_SCHEMA. Check source-span
grounding, atomicity, overclaims, missing qualifiers, equation dependencies, method order,
conflicts, and suitability for deterministic verification.
REQUIRED_JSON_SCHEMA:
{schema}"""


def _pass_prompt(
    bundle: PaperBundle,
    graph: PaperEvidenceGraph,
    pass_name: str,
    instruction: str,
    oracle_results: list[OracleResult],
) -> str:
    role_focus = {
        "claim": {"abstract", "introduction", "results", "discussion", "conclusion"},
        "method": {"abstract", "methods", "results"},
        "quantitative": {"methods", "results", "discussion", "other"},
        "conflict": {"results", "discussion", "limitations", "conclusion"},
    }.get(pass_name)
    return (
        f"PASS: {pass_name}\nINSTRUCTION: {instruction}\n"
        f"PAPER_BUNDLE:\n{bundle_prompt_json(bundle, section_roles=role_focus).decode()}\n"
        f"PRIVATE_OFFICIAL_ORACLE_RESULTS:\n{canonical_json(oracle_results).decode()}\n"
        f"CURRENT_GRAPH:\n{canonical_json(graph).decode()}"
    )


def _critic_prompt(
    bundle: PaperBundle,
    graph: PaperEvidenceGraph,
    oracle_results: list[OracleResult],
) -> str:
    supporting_spans = {span_id for node in graph.nodes for span_id in node.supporting_spans}
    return (
        f"PAPER_BUNDLE:\n{bundle_prompt_json(bundle, span_ids=supporting_spans).decode()}\n"
        f"PRIVATE_OFFICIAL_ORACLE_RESULTS:\n{canonical_json(oracle_results).decode()}\n"
        f"CANDIDATE_GRAPH:\n{canonical_json(graph).decode()}"
    )


def _repair_prompt(
    bundle: PaperBundle,
    graph: PaperEvidenceGraph,
    critique: GraphCritique,
    oracle_results: list[OracleResult],
) -> str:
    supporting_spans = {span_id for node in graph.nodes for span_id in node.supporting_spans}
    return (
        "Return only a bounded delta against GRAPH that resolves the CRITIQUE; do not restate "
        "unchanged graph content.\n"
        f"PAPER_BUNDLE:\n{bundle_prompt_json(bundle, span_ids=supporting_spans).decode()}\n"
        f"PRIVATE_OFFICIAL_ORACLE_RESULTS:\n{canonical_json(oracle_results).decode()}\n"
        f"GRAPH:\n{canonical_json(graph).decode()}\n"
        f"CRITIQUE:\n{canonical_json(critique).decode()}"
    )


__all__ = [
    "BoundedGraphPatch",
    "EvidenceGraphCompiler",
    "GraphCritique",
    "GraphPatch",
    "validate_graph_against_bundle",
]
