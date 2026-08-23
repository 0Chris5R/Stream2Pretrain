"""Grounded policies for discovery metadata and OpenReview public fields.

No general public quality classifier exists for Hub API metadata or peer
reviews. Metadata is therefore a discovery envelope and never training text.
Review scoring reports form completeness only: it counts distinct substantive
OpenReview field families preserved by the extractor and does not infer paper
quality from rating, confidence, decision, or keyword sentiment.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from processor.operators.quality import QualityScore

_FIELD = re.compile(r"^\[FIELD ([^\]]+)\]$", re.MULTILINE)
_FIELD_FAMILIES: tuple[tuple[str, ...], ...] = (
    ("review", "summary"),
    ("strength",),
    ("weakness", "limitation"),
    ("question", "suggestion"),
    ("rebuttal", "response"),
)
_SUBSTANTIVE_INVITATION_MARKERS = (
    "official_review",
    "meta_review",
    "metareview",
    "author_rebuttal",
    "rebuttal",
    "author_response",
    "official_response",
)


@dataclass(frozen=True, slots=True)
class MetadataDiscoveryPolicy:
    """Mark structured API/OAI payloads as non-trainable discovery state."""

    revision: str = "metadata-discovery-only-v1"
    backend: str = "not-applicable"

    def score(self, text: str) -> QualityScore:
        del text
        return QualityScore(edu_score=0.0, revision=self.revision)


@dataclass(frozen=True, slots=True)
class PeerReviewQualityPolicy:
    """Return an auditable OpenReview form-completeness vector count.

    The 0 to 5 value is the number of represented substantive field families,
    not a learned educational or acceptance score. Generic comments and legacy
    unstructured strings receive zero because prose alone does not prove that
    the artifact is an official review or response.
    """

    revision: str = "openreview-schema-completeness-v1"
    backend: str = "schema-rules"

    def score(self, text: str) -> QualityScore:
        field_names = {
            value.strip().lower().replace(" ", "_") for value in _FIELD.findall(text)
        }
        represented = sum(
            any(alias in field_name for field_name in field_names for alias in family)
            for family in _FIELD_FAMILIES
        )
        return QualityScore(edu_score=float(represented), revision=self.revision)


def is_substantive_review(text: str, source_metadata_text: str) -> bool:
    """Recognize official review-form content without inventing length limits.

    OpenReview Invitations define the form and artifact type. A record is
    substantive when it carries a recognized review field family, or when its
    Invitation identifies an official review, meta-review, rebuttal, or official
    response and the extracted public body is non-empty. A generic public
    comment containing only ``[FIELD comment]`` is deliberately excluded.
    """
    if not text.strip():
        return False
    field_names = {
        value.strip().lower().replace(" ", "_") for value in _FIELD.findall(text)
    }
    if any(
        alias in field_name
        for field_name in field_names
        for family in _FIELD_FAMILIES
        for alias in family
    ):
        return True
    metadata = source_metadata_text.lower().replace("-", "_")
    return any(marker in metadata for marker in _SUBSTANTIVE_INVITATION_MARKERS)


# Read compatibility for older imports. New code uses the explicit discovery
# name so the policy is never presented as a metadata quality classifier.
MetadataQualityPolicy = MetadataDiscoveryPolicy
