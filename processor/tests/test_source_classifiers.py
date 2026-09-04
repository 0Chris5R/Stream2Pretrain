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


@pytest.mark.parametrize("source, count", [("arxiv", 1), ("hf", 1)])
def test_independent_heads_cover_every_arxiv_section_but_never_hf(source, count) -> None:
    from processor.operators.quality import QualityScore
    from processor.operators.source_classifiers import (
        ARXIV_DIAGNOSTIC_TASKS,
        SourceQualityClassifier,
    )

    scorer = object.__new__(SourceQualityClassifier)
    calls = []

    def score_task(task, text):
        calls.append((task, text))
        return QualityScore(3.0, "bundle", model_revision=task)

    scorer._score_task = score_task
    text = f"[SOURCE={source}] [SECTION_TYPE=limitations] [SECTION_TITLE=Limitations]\nComplete section."
    result = scorer.score(text)
    assert len(calls) == count
    assert all(value == text for _, value in calls)
    assert not result.diagnostic_scores
    assert result.model_revision == f"{source}-pretrain-quality"
    if source == "arxiv":
        extra = scorer.score_posttrain(text)
        assert set(extra.diagnostic_scores) == set(ARXIV_DIAGNOSTIC_TASKS)
        assert len(calls) == 3
        assert all(value == text for _, value in calls)
    else:
        with pytest.raises(ValueError):
            scorer.score_posttrain(text)


def test_diagnostic_heads_retain_section_scores_and_both_aggregations(silver_record) -> None:
    from processor.curate import _source_quality_report
    from processor.operators.quality import QualityScore
    from processor.operators.source_classifiers import ARXIV_DIAGNOSTIC_TASKS

    class Scorer:
        revision = "bundle"

        def score(self, text):
            high = "Methods" in text
            value = 5.0 if high else 1.0
            tokens = 10 if high else 30
            extra = {
                task: asdict(
                    QualityScore(
                        value,
                        self.revision,
                        confidence=0.9,
                        score_class=int(value),
                        tokens=tokens,
                        model_revision=task,
                    )
                )
                for task in ARXIV_DIAGNOSTIC_TASKS
            }
            return QualityScore(4.0, self.revision, tokens=tokens, diagnostic_scores=extra)

    result = _source_quality_report(
        Scorer(), silver_record, "# Paper\n## Methods\nDerivation.\n## Limitations\nDiscussion."
    )
    assert result["score"] == 4.0
    assert len(result["sections"]) == 2
    for task in ARXIV_DIAGNOSTIC_TASKS:
        head = result["classifiers"][task]
        assert head["score"] == 5.0
        assert head["weighted_mean"] == 2.0
        assert head["mean"] == 3.0
        assert head["best_section_id"] == "section-1"
        assert head["class_5_sections"] == 1
        assert head["sections"] == 2
        assert result["sections"][1]["classifiers"][task]["edu_score"] == 1.0


def test_active_quality_gate_rejects_low_scores_without_changing_sections(
    cfg, long_english_text
) -> None:
    from processor.curate import build_state, curate_one
    from processor.operators.quality import QualityScore
    from processor.tests.test_curate import _silver

    class Scorer:
        revision = "diagnostic-test"
        backend = "test"

        def __init__(self, value: float) -> None:
            self.value = value

        def score(self, text: str) -> QualityScore:
            extra = asdict(
                QualityScore(
                    self.value,
                    self.revision,
                    confidence=0.8,
                    score_class=round(self.value),
                    tokens=100,
                    model_revision="math@test",
                )
            )
            return QualityScore(
                self.value,
                self.revision,
                confidence=0.8,
                tokens=100,
                diagnostic_scores={"arxiv-math-reasoning": extra},
            )

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
    assert low.route == "quarantine"
    assert high.route == "pretrain"
    assert "low_quality_score" in low.reject_reasons
    assert low.quality_score == 0.0 and high.quality_score == 5.0
    assert low.reasoning_score == high.reasoning_score
    assert low.text == high.text
