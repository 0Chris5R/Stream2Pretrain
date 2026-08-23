"""Tests for source-format-specific quality policies."""

from processor.operators.source_quality import (
    MetadataDiscoveryPolicy,
    PeerReviewQualityPolicy,
    is_substantive_review,
)


def test_metadata_policy_never_turns_discovery_fields_into_training_quality() -> None:
    text = (
        "model release version 2 benchmark evaluation dataset license Apache 2.0 "
        "training architecture results repository 2026 task paper abstract"
    )

    result = MetadataDiscoveryPolicy().score(text)

    assert result.edu_score == 0.0
    assert result.revision == "metadata-discovery-only-v1"


def test_metadata_policy_is_independent_of_envelope_length() -> None:
    policy = MetadataDiscoveryPolicy()

    assert policy.score("").edu_score == 0.0
    assert policy.score("updated").edu_score == 0.0


def test_peer_review_policy_counts_openreview_field_families() -> None:
    text = "\n\n".join(
        [
            "[FIELD summary]\nThe paper studies a relevant problem.",
            "[FIELD strengths]\nThe evaluation is broad.",
            "[FIELD weaknesses]\nThe ablation is incomplete.",
            "[FIELD questions]\nHow sensitive is the method?",
        ]
    )

    result = PeerReviewQualityPolicy().score(text)

    assert result.edu_score == 4.0
    assert result.revision == "openreview-schema-completeness-v1"


def test_peer_review_policy_does_not_interpret_rating_or_decision_metadata() -> None:
    assert PeerReviewQualityPolicy().score("").edu_score == 0.0
    assert PeerReviewQualityPolicy().score("Looks good").edu_score == 0.0


def test_review_substance_uses_form_fields_or_official_invitation() -> None:
    assert is_substantive_review("[FIELD summary]\nClear contribution.", "")
    assert is_substantive_review(
        "[FIELD comment]\nWe address the concern.",
        "invitation: ICLR.cc/2026/Conference/-/Author_Rebuttal",
    )


def test_generic_public_comment_is_not_promoted_as_a_review() -> None:
    assert not is_substantive_review(
        "[FIELD comment]\nLooks good",
        "invitation: ICLR.cc/2026/Conference/-/Public_Comment",
    )
