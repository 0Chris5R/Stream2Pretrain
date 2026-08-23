"""Unit tests for the AI-repo allow-list."""

from __future__ import annotations

from pathlib import Path

import yaml

from ingest.github_events.repo_filter import CURATED_REPOS, is_relevant_repo

ROOT = Path(__file__).resolve().parents[3]


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


def test_every_live_release_repo_is_in_the_event_filter() -> None:
    values = yaml.safe_load(
        (ROOT / "charts" / "stream2pretrain" / "values.yaml").read_text(encoding="utf-8")
    )
    release_repos = set(values["sources"]["github"]["releases"]["repos"])

    assert len(release_repos) == 30
    assert release_repos <= CURATED_REPOS
