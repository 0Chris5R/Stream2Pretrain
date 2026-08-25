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
            "arxiv-html-backfill",
            "pdf",
            "docling-pdf-cpu-v1",
            "scientific_paper",
            "finepdfs_edu_v2",
            True,
        ),
        (
            "openreview",
            "pdf",
            "openreview-pdf-pending-marker",
            "scientific_paper",
            "finepdfs_edu_v2",
            True,
        ),
        (
            "openreview-live",
            "pdf",
            "docling-pdf-cpu-v1",
            "scientific_paper",
            "finepdfs_edu_v2",
            True,
        ),
        (
            "openreview-backfill",
            "markdown",
            "reviewarena-ocr-markdown-v1",
            "scientific_paper",
            "finepdfs_edu_v2",
            True,
        ),
        ("seed:allenai/peS2o", "latex", "pes2o-v3", "scientific_paper", "finepdfs_edu_v2", True),
        (
            "seed:togethercomputer/RedPajama-Data-1T",
            "latex",
            "redpajama-arxiv-v1",
            "scientific_paper",
            "finepdfs_edu_v2",
            True,
        ),
        ("rss-openai-news", "html", "resiliparse-0.14", "web_prose", "fineweb_edu", True),
        ("rss-deepmind-blog", "html", "resiliparse-0.14", "web_prose", "fineweb_edu", True),
        ("rss-deepmind", "html", "rss-page-html-v1", "web_prose", "fineweb_edu", True),
        ("rss-hf-blog", "html", "resiliparse-0.14", "web_prose", "fineweb_edu", True),
        ("rss-bair-blog", "html", "resiliparse-0.14", "web_prose", "fineweb_edu", True),
        ("rss-bair", "html", "rss-page-html-v1", "web_prose", "fineweb_edu", True),
        ("rss-eleuther-blog", "html", "resiliparse-0.14", "web_prose", "fineweb_edu", True),
        ("rss-eleuther", "html", "rss-page-html-v1", "web_prose", "fineweb_edu", True),
        ("sitemap-lab", "web", "resiliparse-0.14", "web_prose", "fineweb_edu", True),
        ("sitemap-poller", "html", "sitemap-page-html-v1", "web_prose", "fineweb_edu", True),
        (
            "seed:HuggingFaceFW/fineweb-edu",
            "html",
            "fineweb-edu-seed-v1",
            "web_prose",
            "fineweb_edu",
            True,
        ),
        ("seed:wayback", "web", "wayback-backfill-2026-06", "web_prose", "fineweb_edu", True),
        (
            "github-release-tarballs",
            "code",
            "github-release-tarball-2026-06",
            "source_code",
            "stack_v2_dolma_rules",
            True,
        ),
        (
            "seed:HuggingFaceTB/stack-edu",
            "code",
            "stack-edu-seed-v1",
            "source_code",
            "stack_v2_dolma_rules",
            True,
        ),
        (
            "github-release-tarballs",
            "web",
            "github-readme-markdown-v1",
            "repository_documentation",
            "fineweb_edu",
            True,
        ),
        (
            "cluster-smoke",
            "html",
            "resiliparse-0.14",
            "repository_documentation",
            "fineweb_edu",
            True,
        ),
        ("hf-models", "web", "hf-model-card-markdown-v1", "hf_model_card", "fineweb_edu", True),
        (
            "hf-datasets",
            "web",
            "hf-dataset-card-markdown-v1",
            "hf_dataset_card",
            "fineweb_edu",
            True,
        ),
        ("hf-spaces", "web", "hf-space-card-markdown-v1", "hf_space_card", "fineweb_edu", True),
        (
            "openreview",
            "review",
            "openreview-review-text",
            "peer_review",
            "openreview_schema",
            True,
        ),
        (
            "openreview-live",
            "review",
            "openreview-review-json-v1",
            "peer_review",
            "openreview_schema",
            True,
        ),
        (
            "openreview-backfill",
            "review",
            "reviewarena-review-text",
            "peer_review",
            "openreview_schema",
            True,
        ),
        (
            "hf-daily-papers",
            "metadata",
            "hf-api-json-v1",
            "discovery_metadata",
            "not_applicable",
            False,
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
        (
            "github-events",
            "metadata",
            "github-events-api-json-v1",
            "discovery_metadata",
            "not_applicable",
            False,
        ),
        (
            "github-releases",
            "metadata",
            "github-releases-atom-v1",
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


def test_wire_format_cannot_override_explicit_review_or_code_family() -> None:
    review = resolve_source_policy(
        source_feed="openreview-live",
        source_format="review",
        extraction_pipeline="legacy-scientific-artifact",
    )
    code = resolve_source_policy(
        source_feed="arxiv-import",
        source_format="code",
        extraction_pipeline="legacy-html",
    )

    assert review.family == "peer_review"
    assert code.family == "source_code"


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

    assert web.web_heuristic_gate is True
    assert web.kenlm_mode == "gate"
    assert scientific.web_heuristic_gate is False
    assert scientific.kenlm_mode == "off"
    assert card.web_heuristic_gate is False
    assert card.kenlm_mode == "off"


def test_code_does_not_use_natural_language_as_a_source_language_gate() -> None:
    code = resolve_source_policy(
        source_feed="github-release-tarballs",
        source_format="code",
        extraction_pipeline="github-release-tarball-2026-06",
    )

    assert code.language_gate is False
