"""Unit tests for the AI-repo allow-list."""

from __future__ import annotations

from ingest.github_events.repo_filter import is_relevant_repo


def test_curated_repo_allowed() -> None:
    assert is_relevant_repo("huggingface/transformers")
    assert is_relevant_repo("vllm-project/vllm")


def test_curated_org_allowed() -> None:
    assert is_relevant_repo("EleutherAI/some-experiment")
    assert is_relevant_repo("openai/sdk")


def test_unrelated_repo_rejected() -> None:
    assert not is_relevant_repo("acme/widgets")
    assert not is_relevant_repo("")
    assert not is_relevant_repo("not-a-slash-pair")
