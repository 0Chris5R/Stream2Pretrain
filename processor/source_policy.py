"""Single source-of-truth dispatch for extraction and curation profiles.

Wire format is not a quality domain. For example, an arXiv HTML document and
an ordinary web page need different classifiers, while Hugging Face cards use
their own Markdown projection before card prose is scored.

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
    "hf_model_card",
    "hf_dataset_card",
    "discovery_metadata",
]
QualityProfile = Literal[
    "source_modernbert",
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
    policy_id="scientific-source-modernbert-diagnostic",
    family="scientific_paper",
    extraction_profile="scientific-structured",
    quality_profile="source_modernbert",
    training_text=True,
    language_gate=True,
    web_heuristic_gate=False,
    # The English Wikipedia KenLM is out of domain for equations and dense
    # research prose. Scientific quality is decided by source-specific ModernBERT and structure.
    kenlm_mode="off",
)
WEB_PROSE = SourceProcessingPolicy(
    policy_id="web-source-modernbert-diagnostic",
    family="web_prose",
    extraction_profile="resiliparse-main-content",
    quality_profile="source_modernbert",
    training_text=True,
    language_gate=True,
    web_heuristic_gate=True,
    kenlm_mode="gate",
)
TECHNICAL_DOCUMENTATION = SourceProcessingPolicy(
    policy_id="technical-docs-source-modernbert-diagnostic",
    family="web_prose",
    extraction_profile="markdown-prose",
    quality_profile="source_modernbert",
    training_text=True,
    language_gate=True,
    web_heuristic_gate=False,
    kenlm_mode="off",
)
HF_MODEL_CARD = SourceProcessingPolicy(
    policy_id="hf-model-card-source-modernbert-diagnostic",
    family="hf_model_card",
    extraction_profile="hf-card-markdown-prose",
    quality_profile="source_modernbert",
    training_text=True,
    language_gate=True,
    web_heuristic_gate=False,
    kenlm_mode="off",
)
HF_DATASET_CARD = SourceProcessingPolicy(
    policy_id="hf-dataset-card-source-modernbert-diagnostic",
    family="hf_dataset_card",
    extraction_profile="hf-card-markdown-prose",
    quality_profile="source_modernbert",
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
    "pes2o",
    "redpajama-arxiv",
    "s2orc",
    "acl-ocl",
)


def resolve_source_policy(
    *, source_feed: str, source_format: str, extraction_pipeline: str
) -> SourceProcessingPolicy:
    """Resolve a stable policy from record provenance.

    Explicit wire formats win over names so legacy metadata cannot inherit the
    paper classifier.
    """
    feed = source_feed.lower()
    pipeline = extraction_pipeline.lower()
    identity = f"{feed} {pipeline}"

    if source_format == "metadata":
        return DISCOVERY_METADATA
    if "hf-model-card" in pipeline or feed == "hf-models":
        return HF_MODEL_CARD
    if "hf-dataset-card" in pipeline or feed == "hf-datasets":
        return HF_DATASET_CARD
    # The synthetic cluster canary is technical documentation, not randomly
    # crawled web prose. It exercises the same source-specific ModernBERT service through the
    # documentation policy, where the score remains an audit signal.
    if feed == "cluster-smoke":
        return TECHNICAL_DOCUMENTATION

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
        TECHNICAL_DOCUMENTATION,
        HF_MODEL_CARD,
        HF_DATASET_CARD,
        DISCOVERY_METADATA,
    )
