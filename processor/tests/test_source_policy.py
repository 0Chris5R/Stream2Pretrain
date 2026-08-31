"""Exhaustive dispatch tests for every configured source family."""

import pytest

from processor.source_policy import resolve_source_policy


@pytest.mark.parametrize(
    ("feed", "source_format", "pipeline", "family", "quality", "training_text"),
    [
        (
            "arxiv-html-fetcher",
            "html",
            "arxiv-html-2026-06",
            "scientific_paper",
            "finepdfs_edu_v2",
            True,
        ),
        (
            "cluster-smoke",
            "html",
            "resiliparse-0.14",
            "web_prose",
            "finepdfs_edu_v2",
            True,
        ),
        ("hf-models", "web", "hf-model-card-markdown-v1", "hf_model_card", "finepdfs_edu_v2", True),
        (
            "hf-datasets",
            "web",
            "hf-dataset-card-markdown-v1",
            "hf_dataset_card",
            "finepdfs_edu_v2",
            True,
        ),
        (
            "arxiv-oai-cs",
            "metadata",
            "oai-pmh-metadata-v1",
            "discovery_metadata",
            "not_applicable",
            False,
        ),
        (
            "rss-arxiv-cs-cl",
            "metadata",
            "rss-arxiv-discovery-v1",
            "discovery_metadata",
            "not_applicable",
            False,
        ),
        (
            "rss-arxiv-cs-lg",
            "metadata",
            "rss-arxiv-discovery-v1",
            "discovery_metadata",
            "not_applicable",
            False,
        ),
        (
            "rss-arxiv-cs-ai",
            "metadata",
            "rss-arxiv-discovery-v1",
            "discovery_metadata",
            "not_applicable",
            False,
        ),
        (
            "rss-arxiv-cs-cv",
            "metadata",
            "rss-arxiv-discovery-v1",
            "discovery_metadata",
            "not_applicable",
            False,
        ),
    ],
)
def test_configured_source_dispatch(
    feed: str,
    source_format: str,
    pipeline: str,
    family: str,
    quality: str,
    training_text: bool,
) -> None:
    policy = resolve_source_policy(
        source_feed=feed,
        source_format=source_format,
        extraction_pipeline=pipeline,
    )

    assert policy.family == family
    assert policy.quality_profile == quality
    assert policy.training_text is training_text


def test_only_ordinary_web_enables_web_shape_and_kenlm_gates() -> None:
    web = resolve_source_policy(
        source_feed="rss-openai-news",
        source_format="html",
        extraction_pipeline="rss-page-html-v1",
    )
    scientific = resolve_source_policy(
        source_feed="arxiv-html-fetcher",
        source_format="html",
        extraction_pipeline="arxiv-html-2026-06",
    )
    card = resolve_source_policy(
        source_feed="hf-models",
        source_format="web",
        extraction_pipeline="hf-model-card-markdown-v1",
    )
    smoke = resolve_source_policy(
        source_feed="cluster-smoke",
        source_format="html",
        extraction_pipeline="cluster-smoke-1.0",
    )

    assert web.web_heuristic_gate is True
    assert web.kenlm_mode == "gate"
    assert scientific.web_heuristic_gate is False
    assert scientific.kenlm_mode == "off"
    assert card.web_heuristic_gate is False
    assert card.kenlm_mode == "off"
    assert smoke.web_heuristic_gate is False
    assert smoke.kenlm_mode == "off"
