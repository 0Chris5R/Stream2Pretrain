"""Contracts for independently packaged curator model services."""

from __future__ import annotations

from pathlib import Path

import pytest

from processor import model_service


class _Quality:
    backend = "transformers-cpu"

    def __init__(self, *_args: object, revision: str | None = None, **_kwargs: object) -> None:
        self.revision = revision or "quality@pinned"

    def score(self, text: str) -> object:
        from processor.operators.quality import QualityScore

        return QualityScore(edu_score=float(len(text)), revision=self.revision)


class _KenLM:
    scorer = "kenlm-sentencepiece:en.arpa.bin"

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        return


@pytest.mark.parametrize(
    ("profile", "expected_keys"),
    [
        ("quality", {"ready", "profile", "quality", "classifier_protocol"}),
        ("kenlm", {"ready", "profile", "kenlm"}),
        ("all", {"ready", "profile", "quality", "kenlm", "classifier_protocol"}),
    ],
)
def test_runtime_loads_only_its_selected_model_family(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    profile: model_service.ModelProfile,
    expected_keys: set[str],
) -> None:
    monkeypatch.setattr(model_service, "SourceQualityClassifier", _Quality)
    monkeypatch.setattr(model_service, "KenLMScorer", _KenLM)

    runtime = model_service.CuratorModelRuntime(tmp_path, profile=profile)

    assert set(runtime.metadata()) == expected_keys
    assert (runtime.quality is not None) is (profile in {"quality", "all"})
    assert (runtime.kenlm is not None) is (profile in {"kenlm", "all"})


def test_quality_batch_matches_ordered_one_by_one_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(model_service, "SourceQualityClassifier", _Quality)
    runtime = model_service.CuratorModelRuntime(tmp_path, profile="quality")

    batch = runtime.quality_many("source-pretrain-quality", ["a", "three", "twelve chars"])
    singletons = [
        runtime.quality_many("source-pretrain-quality", [text])[0]
        for text in ["a", "three", "twelve chars"]
    ]

    assert batch == singletons
    assert [item["revision"] for item in batch] == [
        runtime.quality.revision,
        runtime.quality.revision,
        runtime.quality.revision,
    ]


def test_quality_batch_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(model_service, "SourceQualityClassifier", _Quality)
    monkeypatch.setenv("S2P_MODEL_SERVICE_MAX_BATCH_ITEMS", "2")
    runtime = model_service.CuratorModelRuntime(tmp_path, profile="quality")

    with pytest.raises(ValueError, match="between 1 and 2"):
        runtime.quality_many("source-pretrain-quality", ["one", "two", "three"])
