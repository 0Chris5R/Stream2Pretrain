"""Typed contracts for the scientific-paper post-training foundry.

Construction may call external models, but accepted environments are frozen,
hashed, deterministic, and contain no provider credentials or runtime network
dependency.  The models in this module are the wire contract shared by the
worker, event stream, package builder, API, and UI.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

FoundryState = Literal[
    "RECEIVED",
    "PROVIDER_CAPACITY_RESERVED",
    "GRAPH_COMPILED",
    "GRAPH_CRITIQUED",
    "TASKS_PROPOSED",
    "TASKS_ROUTED",
    "SOLUTIONS_GENERATED",
    "VERIFIERS_COMPILED",
    "ADVERSARIAL_VALIDATED",
    "ACCEPTED_SFT",
    "ACCEPTED_RL",
    "REJECTED",
    "DEPRECATED",
]

ProviderCallState = Literal[
    "CALL_PLANNED",
    "QUOTA_RESERVED",
    "CALL_STARTED",
    "STREAM_CHECKPOINTED",
    "CALL_SUCCEEDED",
    "CALL_FAILED",
    "CALL_RATE_LIMITED",
    "QUOTA_RECONCILED",
]

TaskFamily = Literal[
    "grounded_explanation",
    "claim_evidence",
    "derivation_completion",
    "method_dag",
    "figure_table_reasoning",
    "corruption_diagnosis",
    "assumption_consequence",
    "single_paper_research",
    "experiment_configuration",
    "result_reproduction",
]

ArtifactQualityLabel = Literal[
    "generated_unverified",
    "verified_automatic",
    "verified_adversarial",
    "human_sampled",
    "human_audited",
    "deprecated_verifier_bug",
]

PosttrainPool = Literal["sft", "rl"]
DatasetSplit = Literal["train", "benchmark", "none"]


class FrozenModel(BaseModel):
    """Strict immutable base for all foundry contracts."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class StableSpan(FrozenModel):
    span_id: str
    section_id: str
    section_role: str
    ordinal: int = Field(..., ge=0)
    text: str
    text_hash: str = Field(..., pattern=r"^sha256:[0-9a-f]{64}$")
    public: bool = True


class OracleRecipe(FrozenModel):
    oracle_id: str
    image: str = Field(..., pattern=r"^.+@sha256:[0-9a-f]{64}$")
    embedded_artifact_hash: str = Field(..., pattern=r"^sha256:[0-9a-f]{64}$")
    command: list[str] = Field(..., min_length=1)
    cpu_millis: int = Field(..., ge=1)
    memory_mib: int = Field(..., ge=1)
    timeout_seconds: int = Field(..., ge=1)
    network: Literal[False] = False
    expected_output_hash: str | None = Field(
        default=None,
        pattern=r"^sha256:[0-9a-f]{64}$",
    )


class OfficialArtifact(FrozenModel):
    artifact_id: str
    kind: Literal["code", "data", "checkpoint", "supplement", "other"]
    source_uri: str
    immutable_ref: str
    content_hash: str = Field(..., pattern=r"^sha256:[0-9a-f]{64}$")
    build_recipe: str | None = None
    oracle_recipe: OracleRecipe | None = None


class OracleResult(FrozenModel):
    oracle_id: str
    artifact_id: str
    runner: Literal["podman", "kubernetes"]
    output: dict[str, Any] | list[Any]
    output_hash: str = Field(..., pattern=r"^sha256:[0-9a-f]{64}$")
    stdout_hash: str = Field(..., pattern=r"^sha256:[0-9a-f]{64}$")
    duration_ms: int = Field(..., ge=0)
    completed_at: datetime


class BundleEquation(FrozenModel):
    equation_id: str
    latex: str | None = None
    mathml: str | None = None
    source_span_ids: list[str] = Field(default_factory=list)


class BundleTableCell(FrozenModel):
    cell_id: str
    row: int = Field(..., ge=0)
    column: int = Field(..., ge=0)
    value: str


class BundleTable(FrozenModel):
    table_id: str
    caption: str | None = None
    rows: list[list[str]] = Field(default_factory=list)
    cells: list[BundleTableCell] = Field(default_factory=list)
    source_span_ids: list[str] = Field(default_factory=list)


class BundleFigure(FrozenModel):
    figure_id: str
    caption: str | None = None
    alt_text: str | None = None
    ocr_text: str | None = None
    asset_uri: str | None = None
    image_hash: str | None = None
    source_span_ids: list[str] = Field(default_factory=list)


class PaperBundle(FrozenModel):
    schema_version: str = "paper-bundle-v2"
    paper_id: str
    paper_family_id: str
    paper_hash: str = Field(..., pattern=r"^sha256:[0-9a-f]{64}$")
    source_uri: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    sections: list[dict[str, Any]] = Field(default_factory=list)
    stable_spans: list[StableSpan]
    equations: list[BundleEquation] = Field(default_factory=list)
    tables: list[BundleTable] = Field(default_factory=list)
    figures: list[BundleFigure] = Field(default_factory=list)
    captions: list[dict[str, str]] = Field(default_factory=list)
    official_artifacts: list[OfficialArtifact] = Field(default_factory=list)
    quality_labels: dict[str, Any] = Field(default_factory=dict)
    split: Literal["posttrain_candidate"] = "posttrain_candidate"
    source_gold_hash: str = Field(..., pattern=r"^sha256:[0-9a-f]{64}$")
    scientific_artifact_hash: str = Field(..., pattern=r"^sha256:[0-9a-f]{64}$")


EvidenceNodeType = Literal[
    "claim",
    "assumption",
    "equation",
    "method_step",
    "data",
    "artifact",
    "metric",
    "finding",
    "limitation",
    "figure_value",
    "table_value",
    "fault",
]


class EvidenceNode(FrozenModel):
    id: str
    type: EvidenceNodeType
    canonical_text: str | None = None
    supporting_spans: list[str] = Field(default_factory=list)
    qualifiers: list[str] = Field(default_factory=list)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    latex: str | None = None
    canonical_symbolic_form: str | None = None
    free_symbols: list[str] = Field(default_factory=list)
    inputs: list[str] = Field(default_factory=list)
    outputs: list[str] = Field(default_factory=list)
    value: float | str | None = None
    unit: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


EvidenceRelation = Literal[
    "supports",
    "qualifies",
    "contradicts",
    "enables",
    "precedes",
    "derives",
    "depends_on",
    "compares_with",
    "invalidates_if_removed",
    "uses",
    "produces",
]


class EvidenceEdge(FrozenModel):
    source: str
    relation: EvidenceRelation
    target: str


class CompilerRun(FrozenModel):
    pass_name: str
    prompt_version: str
    provider_trace_id: str
    output_hash: str = Field(..., pattern=r"^sha256:[0-9a-f]{64}$")
    accepted: bool
    findings: list[str] = Field(default_factory=list)


class PaperEvidenceGraph(FrozenModel):
    schema_version: str = "paper-evidence-graph-v1"
    graph_id: str
    paper_id: str
    nodes: list[EvidenceNode]
    edges: list[EvidenceEdge]
    uncertainties: list[str] = Field(default_factory=list)
    conflicts: list[str] = Field(default_factory=list)
    compiler_runs: list[CompilerRun] = Field(default_factory=list)

    @model_validator(mode="after")
    def references_resolve(self) -> PaperEvidenceGraph:
        node_ids = {node.id for node in self.nodes}
        for edge in self.edges:
            if edge.source not in node_ids or edge.target not in node_ids:
                raise ValueError(f"unresolved graph edge: {edge.source}->{edge.target}")
        return self


class PublicContextPolicy(FrozenModel):
    included_spans: list[str]
    same_paper_distractors: list[str] = Field(default_factory=list)
    tool_access: list[Literal["search", "open", "find", "calculator", "symbolic"]] = Field(
        default_factory=list
    )


class HiddenTargets(FrozenModel):
    required_nodes: list[str] = Field(default_factory=list)
    required_relations: list[EvidenceEdge] = Field(default_factory=list)
    accepted_evidence_sets: list[list[str]] = Field(default_factory=list)
    expected_values: dict[str, float | str] = Field(default_factory=dict)
    forbidden_nodes: list[str] = Field(default_factory=list)
    required_qualifications: list[str] = Field(default_factory=list)
    required_faults: list[str] = Field(default_factory=list)
    forbidden_faults: list[str] = Field(default_factory=list)
    configuration_constraints: dict[str, Any] = Field(default_factory=dict)
    required_oracle_results: list[str] = Field(default_factory=list)


class Difficulty(FrozenModel):
    estimated: int = Field(..., ge=1, le=5)
    sources: list[str] = Field(default_factory=list)


class TaskSpec(FrozenModel):
    schema_version: str = "task-spec-v1"
    content_policy_revision: str = "scientific-reasoning-v1"
    task_id: str
    paper_id: str
    family: TaskFamily
    public_instruction: str
    public_context_policy: PublicContextPolicy
    hidden_targets: HiddenTargets
    answer_contract: Literal["report_plus_manifest_v1"] = "report_plus_manifest_v1"
    verifier_class: str
    difficulty: Difficulty
    reasoning_operations: list[str] = Field(default_factory=list)
    ambiguity_risks: list[str] = Field(default_factory=list)
    construction_provenance: list[str] = Field(default_factory=list)
    route: Literal["sft", "rl", "reject"]


PredicateType = Literal[
    "nonempty_report",
    "manifest_required",
    "required_nodes",
    "forbidden_nodes",
    "required_dependency_nodes",
    "evidence_membership",
    "evidence_coverage",
    "symbolic_equivalence",
    "numeric_tolerance",
    "method_partial_order",
    "derivation_partial_order",
    "fault_identification",
    "required_relations",
    "required_qualifications",
    "configuration_constraints",
    "report_manifest_consistency",
]


class VerifierPredicate(FrozenModel):
    id: str
    type: PredicateType
    target: str | None = None
    targets: list[str] = Field(default_factory=list)
    allowed_spans: list[str] = Field(default_factory=list)
    expected: float | str | None = None
    tolerance: float | None = Field(default=None, ge=0.0)
    weight: float = Field(..., ge=0.0, le=1.0)
    required: bool = True
    config: dict[str, Any] = Field(default_factory=dict)


class VerifierSpec(FrozenModel):
    schema_version: str = "verifier-spec-v1"
    verifier_id: str
    task_id: str
    version: int = Field(..., ge=1)
    predicates: list[VerifierPredicate]
    normalization: Literal["weighted_sum_with_hard_gates"] = "weighted_sum_with_hard_gates"
    runtime_dependencies: list[str] = Field(default_factory=list)
    network_required: Literal[False] = False
    determinism_seed: int = Field(..., ge=0)
    critic_audit: list[dict[str, Any]] = Field(default_factory=list)


class SubmittedEquation(FrozenModel):
    id: str
    latex: str


class NumericResult(FrozenModel):
    id: str
    value: float
    unit: str | None = None


class AnswerManifest(FrozenModel):
    claims: list[str] = Field(default_factory=list)
    evidence: list[str] = Field(default_factory=list)
    equations: list[SubmittedEquation] = Field(default_factory=list)
    method_nodes: list[str] = Field(default_factory=list)
    faults: list[str] = Field(default_factory=list)
    numeric_results: list[NumericResult] = Field(default_factory=list)
    relations: list[EvidenceEdge] = Field(default_factory=list)
    qualifications: list[str] = Field(default_factory=list)
    configuration: dict[str, bool | int | float | str] = Field(default_factory=dict)


class FoundryAnswer(FrozenModel):
    report: str
    answer_manifest: AnswerManifest


class ToolCall(FrozenModel):
    tool: Literal["search", "open", "find", "calculator", "symbolic"]
    arguments: dict[str, Any]
    observation: Any = None
    error: str | None = None


class TrajectoryTurn(FrozenModel):
    index: int = Field(..., ge=0)
    role: Literal["system", "user", "assistant", "tool"]
    content: Any


class ProviderTrace(FrozenModel):
    trace_id: str
    provider: Literal["hetzner", "replay"]
    credential_label: str
    role: str
    base_url: str
    requested_model: str
    returned_model: str
    upstream_provider: str | None = None
    provider_request_id: str | None = None
    dynamic_route: bool = False
    model_family: str
    model_license: str | None = None
    model_license_source: str | None = None
    prompt_version: str
    request_hash: str = Field(..., pattern=r"^sha256:[0-9a-f]{64}$")
    response_hash: str = Field(..., pattern=r"^sha256:[0-9a-f]{64}$")
    input_tokens: int = Field(..., ge=0)
    output_tokens: int = Field(..., ge=0)
    request_attempts: int = Field(default=1, ge=1)
    latency_ms: int = Field(..., ge=0)
    time_to_first_token_ms: int | None = Field(default=None, ge=0)
    output_tokens_per_second: float | None = Field(default=None, ge=0.0)
    sampling: dict[str, float | int | str | bool | None] = Field(default_factory=dict)
    terms_snapshot_hash: str = Field(..., pattern=r"^sha256:[0-9a-f]{64}$")
    started_at: datetime
    completed_at: datetime


class QuotaState(FrozenModel):
    provider: Literal["hetzner"]
    window: Literal["minute", "day"]
    observed_requests_used: int = Field(default=0, ge=0)
    observed_input_used: int = Field(default=0, ge=0)
    observed_output_used: int = Field(default=0, ge=0)
    locally_reserved_requests: int = Field(default=0, ge=0)
    locally_reserved_input: int = Field(default=0, ge=0)
    locally_reserved_output: int = Field(default=0, ge=0)
    estimated_remaining_requests: int | None = Field(default=None, ge=0)
    estimated_remaining_input: int | None = Field(default=None, ge=0)
    estimated_remaining_output: int | None = Field(default=None, ge=0)
    reset_at: datetime
    confidence: Literal["provider_reported", "local_exact", "local_estimate"]


class Trajectory(FrozenModel):
    trajectory_id: str
    task_id: str
    provider_trace_id: str
    provider_trace_ids: list[str] = Field(default_factory=list)
    answer: FoundryAnswer
    tool_calls: list[ToolCall] = Field(default_factory=list)
    turns: list[TrajectoryTurn] = Field(default_factory=list)
    accepted: bool
    reward: float = Field(..., ge=0.0, le=1.0)
    validation: dict[str, Any] = Field(default_factory=dict)
    loss_masked_turns: list[int] = Field(default_factory=list)


class ValidationReport(FrozenModel):
    task_id: str
    positive_pass: bool
    equivalent_pass: bool
    adversarial_pass: bool
    mutation_killed: int = Field(..., ge=0)
    mutation_total: int = Field(..., ge=0)
    metamorphic_pass: bool
    replay_pass: bool
    security_pass: bool
    false_positive_count: int = Field(..., ge=0)
    false_negative_count: int = Field(..., ge=0)
    details: dict[str, Any] = Field(default_factory=dict)


class FoundryEvent(FrozenModel):
    event_id: str
    job_id: str
    paper_id: str
    sequence: int = Field(..., ge=0)
    state: FoundryState | ProviderCallState
    occurred_at: datetime
    attempt: int = Field(default=1, ge=1)
    idempotency_key: str
    provider_trace_id: str | None = None
    artifact_hash: str | None = None
    reason: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class FoundryArtifactRecord(FrozenModel):
    artifact_id: str
    job_id: str
    paper_id: str
    task_id: str
    family: TaskFamily
    kind: Literal["sft_trajectory", "rl_environment"]
    pool: PosttrainPool
    dataset_split: DatasetSplit
    status: Literal["accepted", "rejected", "deprecated"]
    quality_label: ArtifactQualityLabel
    package_uri: str | None = None
    signature_uri: str | None = None
    signature_backend: str | None = None
    signer_cert_hash: str | None = None
    package_hash: str = Field(..., pattern=r"^sha256:[0-9a-f]{64}$")
    environment_hash: str = Field(..., pattern=r"^sha256:[0-9a-f]{64}$")
    paper_hash: str = Field(..., pattern=r"^sha256:[0-9a-f]{64}$")
    provider_trace_ids: list[str]
    constructor_family: str
    critic_family: str
    validation: ValidationReport
    created_at: datetime
    deprecated_at: datetime | None = None
    deprecation_reason: str | None = None


class ArtifactAuditRecord(FrozenModel):
    audit_id: str
    artifact_id: str
    job_id: str
    decision: Literal["approved", "rejected"]
    reviewer: str = Field(..., min_length=1, max_length=200)
    reason: str | None = Field(default=None, max_length=2_000)
    created_at: datetime


class EnvironmentManifest(FrozenModel):
    schema_version: str = "paper-environment-v2"
    environment_id: str
    task_id: str
    content_policy_revision: str = "scientific-reasoning-v1"
    paper_id: str
    family: TaskFamily
    pool: PosttrainPool
    dataset_split: DatasetSplit
    quality_label: ArtifactQualityLabel
    public_files: list[str]
    hidden_files: list[str]
    verifier_version: int
    runtime_network: Literal[False] = False
    determinism_seed: int
    created_at: datetime
    construction_trace_ids: list[str]
    requirements_lock_hash: str


class ProviderModelSnapshot(FrozenModel):
    provider: Literal["hetzner"]
    discovered_at: datetime
    response_hash: str = Field(..., pattern=r"^sha256:[0-9a-f]{64}$")
    models: list[dict[str, Any]]
    configured_model_ids: list[str] = Field(default_factory=list)
    drifted: bool = False
    previous_response_hash: str | None = None


__all__ = [
    "AnswerManifest",
    "ArtifactAuditRecord",
    "ArtifactQualityLabel",
    "BundleEquation",
    "BundleFigure",
    "BundleTable",
    "CompilerRun",
    "Difficulty",
    "EnvironmentManifest",
    "EvidenceEdge",
    "EvidenceNode",
    "FoundryAnswer",
    "FoundryArtifactRecord",
    "FoundryEvent",
    "FoundryState",
    "HiddenTargets",
    "NumericResult",
    "OfficialArtifact",
    "OracleRecipe",
    "OracleResult",
    "PaperBundle",
    "PaperEvidenceGraph",
    "ProviderCallState",
    "ProviderModelSnapshot",
    "ProviderTrace",
    "PublicContextPolicy",
    "QuotaState",
    "StableSpan",
    "SubmittedEquation",
    "TaskFamily",
    "TaskSpec",
    "ToolCall",
    "Trajectory",
    "TrajectoryTurn",
    "ValidationReport",
    "VerifierPredicate",
    "VerifierSpec",
]
