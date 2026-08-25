"""Single source-of-truth dispatch for extraction and curation profiles.

Wire format is not a quality domain. For example, an arXiv HTML document and
an AI-lab HTML page need different classifiers, while a Hugging Face card and
a GitHub README are Markdown documents whose prose can use the web classifier
only after card/front-matter and code blocks are removed.

The resolver is deliberately dependency-free so ingest, fetcher, curator, UI
metadata, and tests can share the same decisions without loading any model.
Exact model revisions and upstream evidence are recorded in
``docs/SOURCE_PROCESSING_POLICY.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

SourceFamily = Literal[
    "scientific_paper",
    "web_prose",
    "repository_documentation",
    "source_code",
    "peer_review",
    "hf_model_card",
    "hf_dataset_card",
    "hf_space_card",
    "discovery_metadata",
]
QualityProfile = Literal[
    "finepdfs_edu_v2",
    "fineweb_edu",
    "stack_v2_dolma_rules",
    "openreview_schema",
    "not_applicable",
]
KenlmMode = Literal["gate", "diagnostic", "off"]


@dataclass(frozen=True, slots=True)
class SourceProcessingPolicy:
    """Resolved processing contract for one record."""

    policy_id: str
    family: SourceFamily
    extraction_profile: str
    quality_profile: QualityProfile
    training_text: bool
    language_gate: bool
    web_heuristic_gate: bool
    kenlm_mode: KenlmMode


SCIENTIFIC_PAPER = SourceProcessingPolicy(
    policy_id="scientific-finepdfs-v2",
    family="scientific_paper",
    extraction_profile="scientific-structured",
    quality_profile="finepdfs_edu_v2",
    training_text=True,
    language_gate=True,
    web_heuristic_gate=False,
    # The English Wikipedia KenLM is out of domain for equations and dense
    # research prose. Scientific quality is decided by FinePDFs and structure.
    kenlm_mode="off",
)
WEB_PROSE = SourceProcessingPolicy(
    policy_id="web-fineweb-datatrove",
    family="web_prose",
    extraction_profile="resiliparse-main-content",
    quality_profile="fineweb_edu",
    training_text=True,
    language_gate=True,
    web_heuristic_gate=True,
    kenlm_mode="gate",
)
REPOSITORY_DOCUMENTATION = SourceProcessingPolicy(
    policy_id="repository-docs-fineweb",
    family="repository_documentation",
    extraction_profile="markdown-prose",
    quality_profile="fineweb_edu",
    training_text=True,
    language_gate=True,
    # Cards and READMEs legitimately contain lists, front matter, templates,
    # and fenced code. Common-Crawl page-shape gates are not valid blockers.
    web_heuristic_gate=False,
    kenlm_mode="off",
)
SOURCE_CODE = SourceProcessingPolicy(
    policy_id="code-stack-v2-dolma",
    family="source_code",
    extraction_profile="repository-file",
    quality_profile="stack_v2_dolma_rules",
    training_text=True,
    # Natural-language ID is not a valid source-language classifier.
    language_gate=False,
    web_heuristic_gate=False,
    kenlm_mode="off",
)
PEER_REVIEW = SourceProcessingPolicy(
    policy_id="peer-review-openreview-schema",
    family="peer_review",
    extraction_profile="openreview-public-fields",
    quality_profile="openreview_schema",
    training_text=True,
    language_gate=True,
    web_heuristic_gate=False,
    kenlm_mode="off",
)
HF_MODEL_CARD = SourceProcessingPolicy(
    policy_id="hf-model-card-fineweb",
    family="hf_model_card",
    extraction_profile="hf-card-markdown-prose",
    quality_profile="fineweb_edu",
    training_text=True,
    language_gate=True,
    web_heuristic_gate=False,
    kenlm_mode="off",
)
HF_DATASET_CARD = SourceProcessingPolicy(
    policy_id="hf-dataset-card-fineweb",
    family="hf_dataset_card",
    extraction_profile="hf-card-markdown-prose",
    quality_profile="fineweb_edu",
    training_text=True,
    language_gate=True,
    web_heuristic_gate=False,
    kenlm_mode="off",
)
HF_SPACE_CARD = SourceProcessingPolicy(
    policy_id="hf-space-card-fineweb",
    family="hf_space_card",
    extraction_profile="hf-card-markdown-prose",
    quality_profile="fineweb_edu",
    training_text=True,
    language_gate=True,
    web_heuristic_gate=False,
    kenlm_mode="off",
)
DISCOVERY_METADATA = SourceProcessingPolicy(
    policy_id="metadata-discovery-only",
    family="discovery_metadata",
    extraction_profile="discovery-envelope",
    quality_profile="not_applicable",
    training_text=False,
    language_gate=False,
    web_heuristic_gate=False,
    kenlm_mode="off",
)

_SCIENTIFIC_MARKERS = (
    "arxiv",
    "openreview",
    "pes2o",
    "redpajama-arxiv",
    "s2orc",
    "acl-ocl",
)


def resolve_source_policy(
    *, source_feed: str, source_format: str, extraction_pipeline: str
) -> SourceProcessingPolicy:
    """Resolve a stable policy from record provenance.

    Explicit wire formats win over names so legacy scientific artifacts cannot
    make a replayed review or code record inherit the paper classifier.
    """
    feed = source_feed.lower()
    pipeline = extraction_pipeline.lower()
    identity = f"{feed} {pipeline}"

    if source_format == "metadata":
        return DISCOVERY_METADATA
    if source_format == "review":
        return PEER_REVIEW
    if source_format == "code":
        return SOURCE_CODE

    if "hf-model-card" in pipeline or feed == "hf-models":
        return HF_MODEL_CARD
    if "hf-dataset-card" in pipeline or feed == "hf-datasets":
        return HF_DATASET_CARD
    if "hf-space-card" in pipeline or feed == "hf-spaces":
        return HF_SPACE_CARD
    if any(
        marker in pipeline
        for marker in ("github-readme", "repository-readme", "repository-documentation")
    ):
        return REPOSITORY_DOCUMENTATION
    # The synthetic cluster canary is technical documentation, not randomly
    # crawled web prose. It still exercises the real FineWeb-Edu service, but
    # uses the documentation policy where that score is an audit signal rather
    # than an out-of-domain hard gate.
    if feed == "cluster-smoke":
        return REPOSITORY_DOCUMENTATION

    if source_format in {"pdf", "latex", "markdown"}:
        return SCIENTIFIC_PAPER
    if source_format == "html" and any(marker in identity for marker in _SCIENTIFIC_MARKERS):
        return SCIENTIFIC_PAPER
    return WEB_PROSE


def source_policy_catalog() -> tuple[SourceProcessingPolicy, ...]:
    """Return each concrete policy once for audit and UI metadata."""
    return (
        SCIENTIFIC_PAPER,
        WEB_PROSE,
        REPOSITORY_DOCUMENTATION,
        SOURCE_CODE,
        PEER_REVIEW,
        HF_MODEL_CARD,
        HF_DATASET_CARD,
        HF_SPACE_CARD,
        DISCOVERY_METADATA,
    )
