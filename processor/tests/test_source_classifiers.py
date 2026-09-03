"""Pinned training-to-production input, output and non-gating contracts."""

from __future__ import annotations

from dataclasses import asdict, replace

import pytest

from processor.operators.classifier_input import model_input, parse_sections
from processor.operators.source_classifiers import ordinal_output
from scripts.pretrain_judge_batch import parse_sections as judge_sections


@pytest.mark.parametrize("source", ["arxiv-html-fetcher", "hf-models", "hf-datasets"])
def test_production_sections_match_the_labeling_parser(source: str) -> None:
    text = "# Example\nIntroductory text.\n## Abstract\nA summary.\n## Methods\nAn equation.\n### Evaluation\n| x | y |\n| 1 | 2 |\n## Limitations\nA limitation."
    _, expected = judge_sections(text, source=source)
    _, actual = parse_sections(text, source=source)
    assert [asdict(section) for section in actual] == [asdict(section) for section in expected]
    assert model_input(actual[0], source=source).endswith(actual[0].text)


def test_ordinal_score_and_confidence() -> None:
    score, confidence, score_class, probabilities = ordinal_output([0.0] * 6)
    assert score == pytest.approx(2.5)
    assert confidence == pytest.approx(0.0, abs=1e-12)
    assert score_class == 2
    assert sum(probabilities) == pytest.approx(1.0)
    score, confidence, score_class, _ = ordinal_output([-20, -20, -20, -20, 20, -20])
    assert score == pytest.approx(4.0)
    assert confidence == pytest.approx(1.0)
    assert score_class == 4


@pytest.mark.parametrize("values", [[0, 1], [float("nan")] * 6, [float("inf")] * 6])
def test_invalid_ordinal_outputs_fail_closed(values: list[float]) -> None:
    with pytest.raises(ValueError):
        ordinal_output(values)


def test_diagnostic_score_cannot_change_routes_or_posttrain_rank(cfg, long_english_text) -> None:
    from processor.curate import build_state, curate_one
    from processor.foundry.worker import _candidate_ranking_score
    from processor.operators.quality import QualityScore
    from processor.tests.test_curate import _silver

    class Scorer:
        revision = "diagnostic-test"
        backend = "test"

        def __init__(self, value: float) -> None:
            self.value = value

        def score(self, text: str) -> QualityScore:
            return QualityScore(self.value, self.revision, confidence=0.8, tokens=100)

    outputs = []
    for value in (0.0, 5.0):
        state = build_state(replace(cfg, state_dir=cfg.state_dir + str(value)))
        state.source_quality = Scorer(value)
        try:
            record = curate_one(state, _silver(long_english_text))
            outputs.append(record)
        finally:
            state.close()
    low, high = outputs
    assert low.edu_score == 0.0 and high.edu_score == 5.0
    assert low.route == high.route
    assert low.reject_reasons == high.reject_reasons
    assert low.quality_score == high.quality_score
    assert low.reasoning_score == high.reasoning_score
    assert _candidate_ranking_score(low) == _candidate_ranking_score(high)
