"""Tests for source-format-specific quality policies."""

from processor.operators.source_quality import (
    MetadataQualityPolicy,
    PeerReviewQualityPolicy,
)


def test_metadata_policy_rewards_informative_research_metadata() -> None:
    text = (
        "model release version 2 benchmark evaluation dataset license Apache 2.0 "
        "training architecture results repository 2026 task paper abstract"
    )

    result = MetadataQualityPolicy().score(text)

    assert result.edu_score >= 4.0
    assert result.revision == "metadata-quality-rules-v1"


def test_metadata_policy_rejects_empty_or_trivial_values() -> None:
    policy = MetadataQualityPolicy()

    assert policy.score("").edu_score == 0.0
    assert policy.score("updated").edu_score < 1.0


def test_peer_review_policy_rewards_reasoned_actionable_review() -> None:
    text = " ".join(
        [
            "The method has a clear strength because the evaluation compares robust baselines.",
            "However, the evidence leaves a reproducibility concern and a limitation in the ablation.",
            "I suggest the authors report variance, clarify the dataset split, and revise the claim.",
        ]
        * 3
    )

    result = PeerReviewQualityPolicy().score(text)

    assert result.edu_score >= 4.0
    assert result.revision == "peer-review-quality-rules-v1"


def test_peer_review_policy_does_not_reward_empty_decision_label() -> None:
    assert PeerReviewQualityPolicy().score("accept").edu_score == 0.0
