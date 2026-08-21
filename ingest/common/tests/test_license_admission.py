from __future__ import annotations

from ingest.common.license_admission import (
    decide_license_admission,
    effective_license,
    is_training_permitted,
    normalize_license,
)


def test_normalizes_creative_commons_url() -> None:
    assert normalize_license("https://creativecommons.org/licenses/by/4.0/") == "CC-BY-4.0"


def test_unknown_license_is_quarantined_with_provenance() -> None:
    for value in (None, ""):
        result = decide_license_admission(
            source_url="https://arxiv.org/abs/2608.00001",
            source_feed="arxiv-cs-ai",
            license_value=value,
            license_source="rss_entry",
        )
        assert result.admitted is False
        assert result.license_id == "unknown"
        assert "machine-readable licence is missing" in result.decision.reason
        assert result.decision.content_fetch_started is False


def test_unknown_code_and_explicitly_disallowed_licenses_are_quarantined() -> None:
    unknown_code = decide_license_admission(
        source_url="https://github.com/example/repo/releases/tag/v1",
        source_feed="github-release-tarball",
        license_value=None,
        license_source="unknown",
        source_format="code",
    )
    assert unknown_code.admitted is False

    for value in ("CC-BY-NC-4.0", "arxiv-non-exclusive-distribution"):
        result = decide_license_admission(
            source_url="https://arxiv.org/abs/2608.00001",
            source_feed="arxiv-cs-ai",
            license_value=value,
            license_source="rss_entry",
        )
        assert result.admitted is False


def test_dataset_wrapper_does_not_admit_document_content() -> None:
    assert is_training_permitted("ODC-By-1.0") is False
    result = decide_license_admission(
        source_url="https://example.org/paper",
        source_feed="seed:example",
        license_value="ODC-By-1.0",
        license_source="dataset_metadata",
    )
    assert result.decision.status == "quarantined"
    assert "not on the training allowlist" in result.decision.reason


def test_per_record_license_beats_explicit_feed_default() -> None:
    assert effective_license("MIT", "CC-BY-4.0") == ("MIT", "rss_entry")


def test_explicit_source_default_is_logged_as_manual_override() -> None:
    assert effective_license(None, "CC-BY-4.0") == ("CC-BY-4.0", "manual_override")
