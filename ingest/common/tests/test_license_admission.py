from __future__ import annotations

from ingest.common.license_admission import (
    decide_license_admission,
    effective_license,
    is_training_permitted,
    normalize_license,
)


def test_normalizes_creative_commons_url() -> None:
    assert normalize_license("https://creativecommons.org/licenses/by/4.0/") == "CC-BY-4.0"


def test_unknown_item_license_is_quarantined_with_provenance() -> None:
    for value in (None, ""):
        result = decide_license_admission(
            source_url="https://arxiv.org/abs/2608.00001",
            source_feed="arxiv-cs-ai",
            license_value=value,
            license_source="rss_entry",
        )
        assert result.admitted is False
        assert result.fetch_allowed is False
        assert result.decision.status == "quarantined"
        assert result.training_usage == "pretrain_and_posttrain"
        assert result.license_id == "unknown"
        assert "unresolved" in result.decision.reason
        assert result.decision.content_fetch_started is False


def test_unknown_code_is_quarantined_and_noncommercial_is_transform_only() -> None:
    unknown_code = decide_license_admission(
        source_url="https://github.com/example/repo/releases/tag/v1",
        source_feed="github-release-tarball",
        license_value=None,
        license_source="unknown",
        source_format="code",
    )
    assert unknown_code.admitted is False
    assert unknown_code.fetch_allowed is False
    assert unknown_code.decision.source_format == "code"

    for value in ("CC-BY-NC-4.0", "CC-BY-NC-SA-4.0"):
        result = decide_license_admission(
            source_url="https://arxiv.org/abs/2608.00001",
            source_feed="arxiv-cs-ai",
            license_value=value,
            license_source="rss_entry",
        )
        assert result.admitted is False
        assert result.fetch_allowed is True
        assert result.decision.status == "posttrain_transform_only"

    no_derivatives = decide_license_admission(
        source_url="https://arxiv.org/abs/2608.00002",
        source_feed="arxiv-cs-ai",
        license_value="CC-BY-NC-ND-4.0",
        license_source="rss_entry",
    )
    assert no_derivatives.fetch_allowed is False
    assert no_derivatives.decision.status == "quarantined"

    arxiv = decide_license_admission(
        source_url="https://arxiv.org/abs/2608.00001",
        source_feed="arxiv-cs-ai",
        license_value="arxiv-non-exclusive-distribution",
        license_source="rss_entry",
    )
    assert arxiv.decision.status == "posttrain_transform_only"
    assert arxiv.fetch_allowed is True


def test_dataset_wrapper_does_not_admit_document_content() -> None:
    assert is_training_permitted("ODC-By-1.0") is False
    result = decide_license_admission(
        source_url="https://example.org/paper",
        source_feed="seed:example",
        license_value="ODC-By-1.0",
        license_source="dataset_metadata",
    )
    assert result.decision.status == "quarantined"
    assert result.fetch_allowed is False


def test_per_record_license_beats_explicit_feed_default() -> None:
    assert effective_license("MIT", "CC-BY-4.0") == ("MIT", "rss_entry")


def test_source_default_never_substitutes_for_item_evidence() -> None:
    assert effective_license(None, "CC-BY-4.0") == ("unknown", "unknown")
