from __future__ import annotations

import pytest

from scripts.validate_corpus_live import (
    ValidationError,
    validate_corpus_overview,
    validate_dataset_summary,
    validate_license_admissions,
)


def test_live_corpus_contract_accepts_mixed_production_data() -> None:
    validate_license_admissions(
        {
            "admitted": 7,
            "posttrain_transform_only": 5,
            "quarantined": 5,
            "by_license": [
                {"license_id": "CC-BY-4.0", "status": "admitted", "count": 7},
                {
                    "license_id": "arxiv-non-exclusive-distribution",
                    "status": "posttrain_transform_only",
                    "count": 5,
                },
            ],
        }
    )
    validate_corpus_overview(
        {
            "durable_decisions": 12,
            "training_export_documents": 4,
            "rejected_by_reason": {"license_missing": 5, "low_quality_score": 3},
            "per_source_acceptance": [
                {"source": "rss-arxiv-cs-ai", "accepted": 4, "total": 9},
                {"source": "cluster-smoke", "accepted": 1, "total": 1},
            ],
        }
    )
    revisions = {
        key: [f"{key}-v1"]
        for key in (
            "policy_revision",
            "scoring_version",
            "classifier_revision",
            "classifier_backend",
            "projection_version",
            "extraction_pipeline",
            "lang_detector_revision",
            "tokenizer_revision",
            "perplexity_scorer",
            "minhash_backend",
            "lsh_backend",
        )
    }
    validate_dataset_summary(
        {
            "documents": 4,
            "source_count": 1,
            "selection": {"license_policy": "strict_allowlist", "fixtures_included": False},
            "manifest": {"revisions": revisions},
        }
    )


@pytest.mark.parametrize(
    "payload, message",
    [
        (
            {
                "admitted": 0,
                "posttrain_transform_only": 5,
                "quarantined": 5,
                "by_license": [
                    {
                        "license_id": "unknown",
                        "status": "posttrain_transform_only",
                        "count": 5,
                    }
                ],
            },
            "admitted no documents",
        ),
        (
            {
                "admitted": 5,
                "posttrain_transform_only": 0,
                "quarantined": 0,
                "by_license": [{"license_id": "CC-BY-4.0", "status": "admitted", "count": 5}],
            },
            "no posttrain-transform-only documents",
        ),
    ],
)
def test_live_license_contract_rejects_one_sided_results(
    payload: dict[str, object], message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        validate_license_admissions(payload)


def test_live_corpus_contract_rejects_fixture_only_acceptance() -> None:
    with pytest.raises(ValidationError, match="every production source is rejected"):
        validate_corpus_overview(
            {
                "durable_decisions": 3,
                "training_export_documents": 1,
                "rejected_by_reason": {"license_missing": 2},
                "per_source_acceptance": [
                    {"source": "cluster-smoke", "accepted": 1, "total": 1},
                    {"source": "rss-arxiv-cs-ai", "accepted": 0, "total": 2},
                ],
            }
        )


def test_dataset_contract_requires_concrete_classifier_provenance() -> None:
    revisions = {
        key: [f"{key}-v1"]
        for key in (
            "policy_revision",
            "scoring_version",
            "classifier_revision",
            "classifier_backend",
            "projection_version",
            "extraction_pipeline",
            "lang_detector_revision",
            "tokenizer_revision",
            "perplexity_scorer",
            "minhash_backend",
            "lsh_backend",
        )
    }
    revisions["classifier_revision"] = ["unknown"]
    with pytest.raises(ValidationError, match="classifier_revision"):
        validate_dataset_summary(
            {
                "documents": 1,
                "source_count": 1,
                "selection": {
                    "license_policy": "strict_allowlist",
                    "fixtures_included": False,
                },
                "manifest": {"revisions": revisions},
            }
        )
