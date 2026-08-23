"""Contracts for independently packaged curator model services."""

from __future__ import annotations

from pathlib import Path

import pytest

from processor import model_service


class _Quality:
    backend = "transformers-cpu"

    def __init__(self, *_args: object, revision: str | None = None, **_kwargs: object) -> None:
        self.revision = revision or "quality@pinned"


class _KenLM:
    scorer = "kenlm-sentencepiece:en.arpa.bin"

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        return


class _Embedding:
    backend = "onnxruntime-cpu"

    def __init__(self, *_args: object, revision: str | None = None, **_kwargs: object) -> None:
        self.revision = revision or "e5@pinned"


@pytest.mark.parametrize(
    ("profile", "expected_keys"),
    [
        ("quality", {"ready", "profile", "quality"}),
        ("kenlm", {"ready", "profile", "kenlm"}),
        ("embedding", {"ready", "profile", "embedding"}),
        ("all", {"ready", "profile", "quality", "kenlm", "embedding"}),
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
    monkeypatch.setattr(model_service, "_EmbeddingSketch", _Embedding)

    runtime = model_service.CuratorModelRuntime(tmp_path, profile=profile)

    assert set(runtime.metadata()) == expected_keys
    assert (runtime.finepdfs is not None) is (profile in {"quality", "all"})
    assert (runtime.kenlm is not None) is (profile in {"kenlm", "all"})
    assert (runtime.embedding is not None) is (profile in {"embedding", "all"})
