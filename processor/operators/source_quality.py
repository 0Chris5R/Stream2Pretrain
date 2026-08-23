"""Transparent quality policies for structured metadata and peer reviews.

FineWeb-Edu is trained on rendered web pages and FinePDFs-Edu on extracted
PDF documents. Applying either model to API JSON values or review forms would
be an out-of-distribution score with a misleading model label. These bounded
rules keep those source families useful while making the decision auditable.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from processor.operators.quality import QualityScore

_WORD = re.compile(r"[A-Za-z][A-Za-z'-]*")
_RESEARCH_TERMS = frozenset(
    {
        "abstract",
        "architecture",
        "benchmark",
        "dataset",
        "evaluation",
        "experiment",
        "license",
        "method",
        "model",
        "paper",
        "release",
        "repository",
        "results",
        "task",
        "training",
        "version",
    }
)
_REVIEW_TERMS = frozenset(
    {
        "clarity",
        "concern",
        "evidence",
        "experiment",
        "limitation",
        "method",
        "novelty",
        "reproducibility",
        "result",
        "strength",
        "weakness",
    }
)
_REASONING_TERMS = frozenset({"although", "because", "however", "therefore", "unless", "whereas"})
_ACTION_TERMS = frozenset(
    {"clarify", "compare", "explain", "include", "report", "revise", "suggest"}
)


def _tokens(text: str) -> list[str]:
    return [value.lower() for value in _WORD.findall(text)]


@dataclass(frozen=True, slots=True)
class MetadataQualityPolicy:
    """Score whether flattened API/OAI metadata carries useful information."""

    revision: str = "metadata-quality-rules-v1"
    backend: str = "rules"

    def score(self, text: str) -> QualityScore:
        tokens = _tokens(text)
        if not tokens:
            return QualityScore(edu_score=0.0, revision=self.revision)
        unique_fraction = len(set(tokens)) / len(tokens)
        score = 0.0
        score += 1.0 if len(tokens) >= 8 else 0.0
        score += 1.0 if len(tokens) >= 20 else 0.0
        score += 1.0 if len(tokens) >= 4 and unique_fraction >= 0.45 else 0.0
        score += 1.0 if set(tokens) & _RESEARCH_TERMS else 0.0
        score += 1.0 if any(char.isdigit() for char in text) else 0.0
        return QualityScore(edu_score=score, revision=self.revision)


@dataclass(frozen=True, slots=True)
class PeerReviewQualityPolicy:
    """Score substantive, reasoned, and actionable peer-review prose."""

    revision: str = "peer-review-quality-rules-v1"
    backend: str = "rules"

    def score(self, text: str) -> QualityScore:
        tokens = _tokens(text)
        if not tokens:
            return QualityScore(edu_score=0.0, revision=self.revision)
        vocabulary = set(tokens)
        score = 0.0
        score += 1.0 if len(tokens) >= 50 else 0.0
        score += 1.0 if len(tokens) >= 150 else 0.0
        score += 1.0 if vocabulary & _REVIEW_TERMS else 0.0
        score += 1.0 if vocabulary & _REASONING_TERMS else 0.0
        score += 1.0 if "?" in text or vocabulary & _ACTION_TERMS else 0.0
        return QualityScore(edu_score=score, revision=self.revision)
