"""Grounded policy for non-trainable discovery metadata."""

from __future__ import annotations

from dataclasses import dataclass

from processor.operators.quality import QualityScore


@dataclass(frozen=True, slots=True)
class MetadataDiscoveryPolicy:
    """Mark structured API/OAI payloads as non-trainable discovery state."""

    revision: str = "metadata-discovery-only-v1"
    backend: str = "not-applicable"

    def score(self, text: str) -> QualityScore:
        del text
        return QualityScore(edu_score=0.0, revision=self.revision)
