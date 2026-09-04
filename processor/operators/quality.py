"""Source-quality result contract and deterministic development fixture."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class QualityScore:
    """One scored section with ordinal uncertainty and exact provenance."""

    edu_score: float
    revision: str
    confidence: float | None = None
    score_class: int | None = None
    probabilities: tuple[float, ...] = ()
    tokens: int = 0
    chunks: int = 0
    model_revision: str | None = None
    # Independent arXiv head outputs share the scalar/provenance structure.
    diagnostic_scores: dict[str, dict[str, Any]] | None = None


class DevelopmentQualityScorer:
    """Deterministic fixture for dependency-light tests, never strict profiles."""

    revision = "proxy-heuristic-0.1"
    backend = "proxy"
    is_model_loaded = False

    def score(self, text: str) -> QualityScore:
        words = text.split()
        if not words:
            return QualityScore(edu_score=0.0, revision=self.revision)
        mean_length = sum(map(len, words)) / len(words)
        score = 5.0 / (1.0 + math.exp(-(mean_length - 4.0)))
        if len(words) < 50:
            score *= 0.5
        return QualityScore(edu_score=max(0.0, min(5.0, score)), revision=self.revision)
