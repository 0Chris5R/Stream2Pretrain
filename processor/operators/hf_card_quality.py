"""Deterministic structure gate for Hugging Face model and dataset cards.

The gate follows the sections recommended by Hugging Face's official model-
and dataset-card documentation. It does not treat YAML metadata, repository
popularity, or a model licence as evidence that README prose is useful.
FinePDFs Edu v2 is evaluated only after this cheaper deterministic gate passes.
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
_SCRIPT_CARD_MARKERS = (
    "main artifact of this repository",
    "fusion strategy",
    "task head",
    "initialization",
)
_ARTIFACT_ONLY_MARKERS = (
    "available model files",
    "checkpoint backup",
    "checkpoint backups",
    "checkpoint archive",
    "contains the checkpoints",
    "repo containing the checkpoints",
    "ollama modelfile",
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

_MEASURED_EVIDENCE = re.compile(
    r"\b\d+(?:[.,]\d+)?\s*(?:[kmbt]|million|billion)?\+?\s*"
    r"(?:[A-Za-z][A-Za-z-]*\s+){0,4}"
    r"(?:parameters?|params?|samples?|examples?|rows?|instances?|items?|cases?|fields?|"
    r"columns?|categories?|subjects?|tasks?|trials?|environments?|runs?|files?|steps?|epochs?|"
    r"tokens?|"
    r"layers?|heads?|hz|khz|mb|gb|tb|ms|seconds?|hours?|%|f1|accuracy|loss|wer|bleu|rouge)"
    r"(?![A-Za-z0-9_])",
    re.IGNORECASE,
)
_IMMUTABLE_EVIDENCE = re.compile(
    r"\b(?:revision|commit|sha-?256|checksum)\b.{0,96}\b[0-9a-f]{7,64}\b"
)
_CODE_EVIDENCE = re.compile(
    r"(?:```|\b(?:from_pretrained|pipeline\(|load_dataset\(|AutoModel|vllm|onnxruntime|"
    r"llama-cli|ollama run)\b)",
    re.IGNORECASE,
)
_NAMED_SOURCE_EVIDENCE = re.compile(
    r"(?i:\b(?:dataset|base model|backbone|trained (?:on|with|from scratch on)|"
    r"fine-tuned (?:on|from))\b.{1,100})"
    r"(?:[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+|[A-Z][A-Z0-9_.-]{2,}|"
    r"[A-Z][a-z0-9_.-]+[A-Z][A-Za-z0-9_.-]*)"
)
_METRIC_EVIDENCE = re.compile(
    r"\b(?:accuracy|f1|perplexity|loss|wer|bleu|rouge|map|latency|throughput|rtf)\b"
    r".{0,48}\b\d+(?:[.,]\d+)?",
    re.IGNORECASE,
)
_REPOSITORY_EVIDENCE = re.compile(r"\b[A-Za-z0-9_.-]{2,}/[A-Za-z0-9_.-]{2,}\b")
_SCRIPT_TITLE = re.compile(r"^(?:pipeline|inference|finetune|train|dataloader|clean)\.py$", re.I)


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
    grounded_evidence = _grounded_evidence(normalized)
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
    script_shell = (
        bool(_SCRIPT_TITLE.fullmatch((title or "").strip()))
        and sum(marker in normalized for marker in _SCRIPT_CARD_MARKERS) >= 3
    ) or sum(marker in normalized for marker in _SCRIPT_CARD_MARKERS) == len(_SCRIPT_CARD_MARKERS)
    word_count = len(re.findall(r"\b\w+\b", text))
    artifact_only = (
        word_count < 120
        and any(marker in normalized for marker in _ARTIFACT_ONLY_MARKERS)
        and not grounded_evidence.intersection({"measured", "immutable", "metric", "named_source"})
    )
    wrong_type = (
        kind == "model"
        and bool(headings & _DATASET_DETAILS)
        and not bool(headings & _MODEL_DETAILS)
    ) or (
        kind == "dataset"
        and bool(headings & _MODEL_DETAILS)
        and not bool(headings & _DATASET_DETAILS)
    )

    # Heading vocabulary changes over time and many strong legacy cards use no
    # template headings. Accept prose evidence rather than the spelling of a
    # section title. Two technical dimensions still need at least one concrete
    # measurement, source, immutable revision, metric, or executable example.
    technically_grounded = len(technical_dimensions) >= 2 and bool(grounded_evidence)
    compact_but_grounded = (
        word_count >= 25
        and len(technical_dimensions) >= 1
        and len(grounded_evidence) >= 3
        and "measured" in grounded_evidence
    )
    substantial_legacy_prose = (
        word_count >= 180 and len(technical_dimensions) >= 1 and len(grounded_evidence) >= 2
    )
    accepted = (
        not placeholder
        and not upload_stub
        and not generic_benchmark
        and not trainer_template
        and not script_shell
        and not artifact_only
        and not wrong_type
        and not (marketing and not technically_grounded)
        and (
            (has_overview and bool(detail_sections) and bool(grounded_evidence))
            or technically_grounded
            or compact_but_grounded
            or substantial_legacy_prose
        )
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
    if script_shell:
        categories.append("synthetic_script_card")
    if artifact_only:
        categories.append("minimal_artifact_listing")
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
        *(f"grounding:{name}" for name in sorted(grounded_evidence)),
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


def _grounded_evidence(normalized: str) -> set[str]:
    """Return concrete evidence kinds without treating headings as evidence."""
    evidence: set[str] = set()
    if _MEASURED_EVIDENCE.search(normalized):
        evidence.add("measured")
    if _IMMUTABLE_EVIDENCE.search(normalized):
        evidence.add("immutable")
    if _CODE_EVIDENCE.search(normalized):
        evidence.add("executable")
    if _NAMED_SOURCE_EVIDENCE.search(normalized):
        evidence.add("named_source")
    if _METRIC_EVIDENCE.search(normalized):
        evidence.add("metric")
    if _REPOSITORY_EVIDENCE.search(normalized):
        evidence.add("repository_reference")
    return evidence


__all__ = ["HFCardAssessment", "assess_hf_card", "is_hf_placeholder_section"]
