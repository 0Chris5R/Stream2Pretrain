"""Tests for source-format-specific quality policies."""

from processor.operators.source_quality import MetadataDiscoveryPolicy


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
