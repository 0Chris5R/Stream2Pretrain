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


class _ShadowScalar:
    def __init__(self, *_args: object, family: str, revision: str, **_kwargs: object) -> None:
        self.family = family
        self.revision = revision

    def score(self, text: str) -> dict[str, object]:
        return {"model_family": self.family, "score": float(len(text))}


class _Cso:
    revision = "cso-test"

    def score(self, text: str) -> dict[str, object]:
        return {"model_family": "cso-topics", "topics": [text]}


@pytest.mark.parametrize(
    ("profile", "expected_keys"),
    [
        ("finepdfs", {"ready", "profile", "quality"}),
        ("quality", {"ready", "profile", "quality"}),
        ("kenlm", {"ready", "profile", "kenlm"}),
        ("shadow", {"ready", "profile", "shadow"}),
        ("all", {"ready", "profile", "quality", "kenlm", "shadow"}),
    ],
)
def test_runtime_loads_only_its_selected_model_family(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    profile: model_service.ModelProfile,
    expected_keys: set[str],
) -> None:
    monkeypatch.setattr(model_service, "QualityClassifier", _Quality)
    monkeypatch.setattr(model_service, "KenLMScorer", _KenLM)
    monkeypatch.setattr(model_service, "TransformerShadowScorer", _ShadowScalar)
    monkeypatch.setattr(model_service, "CsoShadowClassifier", _Cso)

    runtime = model_service.CuratorModelRuntime(tmp_path, profile=profile)

    assert set(runtime.metadata()) == expected_keys
    assert (runtime.finepdfs is not None) is (profile in {"finepdfs", "quality", "all"})
    assert (runtime.kenlm is not None) is (profile in {"kenlm", "all"})
    assert (runtime.meta_rater is not None) is (profile in {"shadow", "all"})
    assert (runtime.finemath is not None) is (profile in {"shadow", "all"})
    assert (runtime.cso is not None) is (profile in {"shadow", "all"})


def test_shadow_runtime_returns_all_public_results(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(model_service, "TransformerShadowScorer", _ShadowScalar)
    monkeypatch.setattr(model_service, "CsoShadowClassifier", _Cso)
    runtime = model_service.CuratorModelRuntime(tmp_path, profile="shadow")

    assert set(runtime.shadow_scores("paper body")) == {
        "meta-rater-reasoning",
        "finemath",
        "cso-topics",
    }


def test_quality_batch_matches_ordered_one_by_one_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(model_service, "QualityClassifier", _Quality)
    runtime = model_service.CuratorModelRuntime(tmp_path, profile="finepdfs")

    batch = runtime.quality_many("finepdfs-edu-v2", ["a", "three", "twelve chars"])
    singletons = [
        runtime.quality_many("finepdfs-edu-v2", [text])[0]
        for text in ["a", "three", "twelve chars"]
    ]

    assert batch == singletons
    assert [item["revision"] for item in batch] == [
        runtime.finepdfs.revision,
        runtime.finepdfs.revision,
        runtime.finepdfs.revision,
    ]


def test_quality_batch_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(model_service, "QualityClassifier", _Quality)
    monkeypatch.setenv("S2P_MODEL_SERVICE_MAX_BATCH_ITEMS", "2")
    runtime = model_service.CuratorModelRuntime(tmp_path, profile="finepdfs")

    with pytest.raises(ValueError, match="between 1 and 2"):
        runtime.quality_many("finepdfs-edu-v2", ["one", "two", "three"])
