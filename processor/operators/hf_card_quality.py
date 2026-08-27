"""Deterministic structure gate for Hugging Face model and dataset cards.

The gate follows the sections recommended by Hugging Face's official model-
and dataset-card documentation. It does not treat YAML metadata, repository
popularity, or a model licence as evidence that README prose is useful.
FineWeb-Edu and FinePDFs remain independent learned signals in the curator.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

from schemas.silver import SilverSegment

CardKind = Literal["model", "dataset"]

_OVERVIEW = {
    "summary",
    "description",
    "overview",
    "model description",
    "dataset description",
    "about",
}
_MODEL_DETAILS = {
    "uses",
    "intended use",
    "direct use",
    "limitations",
    "training",
    "training details",
    "training data",
    "evaluation",
    "evaluation results",
    "results",
    "architecture",
    "usage",
    "inference",
    "performance",
}
_DATASET_DETAILS = {
    "dataset structure",
    "data structure",
    "dataset content",
    "data fields",
    "data instances",
    "data splits",
    "dataset creation",
    "collection process",
    "source data",
    "uses",
    "usage",
    "limitations",
    "bias",
    "curation rationale",
}
_PLACEHOLDERS = (
    "more information needed",
    "provide a longer summary",
    "developers should write",
    "content goes here",
    "fill in this section",
    "insert description",
    "todo:",
    "tbd",
)
_UPLOAD_STUBS = (
    "uploaded model",
    "uploaded with",
    "checkpoint converted",
    "automatic model card",
    "this is a model card",
    "this should be a paper title",
    "static quants of",
    "weighted/imatrix quants of",
)
_MARKETING = (
    "revolutionary",
    "best model ever",
    "state-of-the-art solution for everyone",
    "download now",
    "join our discord",
    "contact us for pricing",
)
_GENERIC_BENCHMARK_MARKERS = (
    "model1-v2",
    "other leading models",
    "benchmark evaluations, including mathematics, programming, and general logic",
)
_TRAINER_TEMPLATE_MARKERS = (
    "this model is a fine-tuned version of",
    "it has been trained using trl",
    "framework versions",
    "cite trl as",
)
_TECHNICAL_DIMENSIONS: dict[str, tuple[str, ...]] = {
    "architecture": ("architecture", "parameters", "layers", "attention", "backbone"),
    "training": ("training", "fine-tun", "optimizer", "learning rate", "epoch"),
    "evaluation": ("evaluation", "benchmark", "accuracy", "f1", "perplexity", "results"),
    "data": ("dataset", "samples", "instances", "split", "collection", "annotation"),
    "usage": ("inference", "usage", "intended use", "input", "output"),
    "limitations": ("limitation", "bias", "risk", "out-of-scope", "failure mode"),
    "artifact_format": ("safetensors", "checkpoint", "projection keys", "key-conversion"),
    "runtime": ("automodelforcausallm", "vllm", "transformers", "pytorch", "onnx"),
}


@dataclass(frozen=True, slots=True)
class HFCardAssessment:
    accepted: bool
    categories: tuple[str, ...]
    evidence: tuple[str, ...]


def assess_hf_card(
    *, kind: CardKind, title: str | None, text: str, segments: list[SilverSegment]
) -> HFCardAssessment:
    """Classify card prose using auditable section and content evidence."""
    normalized = " ".join(text.lower().split())
    headings = {_normalize_heading(segment.title) for segment in segments if segment.text.strip()}
    technical_dimensions = {
        name
        for name, markers in _TECHNICAL_DIMENSIONS.items()
        if any(marker in normalized for marker in markers)
    }
    expected_details = _MODEL_DETAILS if kind == "model" else _DATASET_DETAILS
    has_overview = bool(headings & _OVERVIEW) or f"this {kind}" in normalized
    detail_sections = headings & expected_details
    placeholder = any(marker in normalized for marker in _PLACEHOLDERS)
    upload_stub = any(marker in normalized for marker in _UPLOAD_STUBS)
    marketing = any(marker in normalized for marker in _MARKETING)
    generic_benchmark = sum(marker in normalized for marker in _GENERIC_BENCHMARK_MARKERS) >= 2
    trainer_template = sum(
        marker in normalized for marker in _TRAINER_TEMPLATE_MARKERS
    ) >= 2 and not ({"architecture", "evaluation", "usage", "limitations"} & technical_dimensions)
    wrong_type = (
        kind == "model"
        and bool(headings & _DATASET_DETAILS)
        and not bool(headings & _MODEL_DETAILS)
    ) or (
        kind == "dataset"
        and bool(headings & _MODEL_DETAILS)
        and not bool(headings & _DATASET_DETAILS)
    )

    # Two independently evidenced technical dimensions are a useful fallback
    # for older cards that predate the current template headings. This is a
    # structural rule, not a learned-quality threshold.
    technically_grounded = len(technical_dimensions) >= 2
    accepted = (
        not placeholder
        and not upload_stub
        and not generic_benchmark
        and not trainer_template
        and not wrong_type
        and not (marketing and not technically_grounded)
        and ((has_overview and bool(detail_sections)) or technically_grounded)
    )

    categories: list[str] = []
    if placeholder:
        categories.append("template_boilerplate")
    if trainer_template:
        categories.append("template_boilerplate")
    if upload_stub or not normalized:
        categories.append("stub_checkpoint_upload")
    if generic_benchmark:
        categories.append("generic_marketing_benchmark")
    if marketing:
        categories.append("marketing")
    if wrong_type:
        categories.append("wrong_repository_type")
    if accepted:
        if {"architecture", "training", "evaluation"} <= technical_dimensions:
            categories.append("dense_scientific_card")
        else:
            categories.append("substantive_technical_card")
    elif not categories:
        categories.append("insufficient_card_documentation")

    evidence = [
        *(f"section:{heading}" for heading in sorted(detail_sections)),
        *(f"dimension:{name}" for name in sorted(technical_dimensions)),
    ]
    if title:
        evidence.append("title_present")
    return HFCardAssessment(
        accepted=accepted,
        categories=tuple(categories),
        evidence=tuple(evidence),
    )


def is_hf_placeholder_section(text: str) -> bool:
    """Return whether a complete card section is unfilled template prose."""
    normalized = " ".join(text.lower().split())
    return bool(normalized) and any(marker in normalized for marker in _PLACEHOLDERS)


def _normalize_heading(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 -]+", "", value.lower())).strip()


__all__ = ["HFCardAssessment", "assess_hf_card", "is_hf_placeholder_section"]
