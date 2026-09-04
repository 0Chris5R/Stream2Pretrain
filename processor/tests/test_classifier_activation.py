"""Deterministic contracts for the owner-approved operational classifiers."""

from dataclasses import asdict, replace
from datetime import UTC, datetime

import pytest

from processor.curate import (
    _prefetched_curate_state,
    _source_quality_report,
    build_state,
    curate_one,
)
from processor.foundry.paper_adapter import classifier_section_hints
from processor.foundry.store import FoundryStore
from processor.foundry.worker import _candidate_ranking_score
from processor.operators.quality import QualityScore
from processor.tests.test_curate import _silver
from processor.tests.test_serving_index import _gold


class Scorer:
    revision = "test-model"
    backend = "test"

    def __init__(self, score, *, posttrain=False):
        self.value = score
        self.posttrain = posttrain
        self.calls = []

    def score(self, text):
        self.calls.append(text)
        value = QualityScore(
            self.value,
            self.revision,
            tokens=20,
            confidence=0.8,
            score_class=round(self.value),
            model_revision=self.revision,
        )
        return (
            replace(
                value,
                diagnostic_scores={
                    task: asdict(value)
                    for task in ("arxiv-math-reasoning", "arxiv-posttrain-suitability")
                },
            )
            if self.posttrain
            else value
        )


@pytest.mark.parametrize(
    "source,score,passed",
    [
        ("arxiv-html-fetcher", 2.99, False),
        ("arxiv-html-fetcher", 3.0, True),
        ("hf-models", 3.49, False),
        ("hf-models", 3.5, True),
        ("hf-datasets", 3.49, False),
        ("hf-datasets", 3.5, True),
    ],
)
def test_exact_cutoffs_and_second_stage_calls(silver_record, source, score, passed):
    silver = silver_record.model_copy(update={"source_feed": source})
    post = Scorer(4.0, posttrain=True)
    report = _source_quality_report(
        Scorer(score), silver, "## Methods\nComplete derivation.\n## Limitations\nCaveats.", post
    )
    assert report["passed"] is passed
    assert len(post.calls) == (2 if passed and source == "arxiv-html-fetcher" else 0)
    assert len(report["sections"]) == 2


def test_microbatch_quality_failure_never_runs_posttrain(cfg, long_english_text):
    state = build_state(cfg)
    state.source_quality = Scorer(2.0)
    post = state.posttrain_quality = Scorer(5.0, posttrain=True)
    silver = _silver(long_english_text).model_copy(update={"source_feed": "arxiv-html-fetcher"})
    try:
        _prefetched_curate_state(state, [silver])
        assert post.calls == []
    finally:
        state.close()


def test_mean_ranks_and_max_only_creates_hint():
    doc = _gold(1).model_copy(
        update={
            "quality_diagnostics": {
                "mode": "active",
                "passed": True,
                "classifiers": {
                    "arxiv-posttrain-suitability": {"score": 5.0, "weighted_mean": 2.5}
                },
                "sections": [
                    {
                        "title": "Derivation",
                        "classifiers": {
                            "arxiv-posttrain-suitability": {"edu_score": 5.0},
                            "arxiv-math-reasoning": {"edu_score": 4.5},
                        },
                    }
                ],
            }
        }
    )
    assert _candidate_ranking_score(doc) == 0.5
    hints = classifier_section_hints(doc)
    assert '"Derivation"' in hints and "especially relevant" in hints
    assert "potentially creating a derivation" in hints


def test_composite_metric_is_not_overwritten_by_source_classifier(cfg, long_english_text):
    state = build_state(cfg)
    state.source_quality = Scorer(0.25)
    silver = _silver(long_english_text).model_copy(update={"source_feed": "arxiv-html-fetcher"})
    try:
        result = curate_one(state, silver)
        assert result.edu_score == 0.25
        assert result.quality_score != result.edu_score
        assert "low_quality_score" in result.reject_reasons
    finally:
        state.close()


def test_queue_reset_is_once_preserves_processing_and_replay_memory(tmp_path):
    store = FoundryStore(str(tmp_path / "queue.sqlite"))
    for index in range(2):
        store.enqueue_candidate(
            doc_id=str(index),
            payload=b"{}",
            reasoning_score=0.5,
            quality_score=4,
            valid_from=datetime.now(UTC),
        )
    store._conn.execute("UPDATE candidate_queue SET state='processing' WHERE doc_id='0'")
    assert store.reset_pending_candidates("active-v1") == 1
    assert store.reset_pending_candidates("active-v1") == 0
    assert store._conn.execute("SELECT state FROM candidate_queue").fetchone()[0] == "processing"
    store.record_candidate_admission("identity", "1", "queued")
    assert store.candidate_admission_seen("identity")
    store.close()


def test_expired_input_replay_does_not_retry_a_known_missing_pointer(tmp_path):
    from processor.expired_inputs import ExpiredInputIndex

    path = str(tmp_path / "expired.sqlite")
    index = ExpiredInputIndex(path)
    index.record("s3://bronze/immutable-old-body")
    index.close()
    index = ExpiredInputIndex(path)
    assert index.contains("s3://bronze/immutable-old-body")
    assert not index.contains("s3://bronze/new-body")
    index.close()


def test_posttrain_http_family_reaches_second_stage_without_local_server(monkeypatch):
    from types import SimpleNamespace

    from processor.model_service import _Handler

    calls = []
    writes = []
    handler = object.__new__(_Handler)
    handler.path = "/v1/quality"
    handler.headers = {}
    handler.server = SimpleNamespace(
        runtime=SimpleNamespace(
            quality=object(),
            max_batch_items=2,
            quality_many=lambda family, texts: calls.append((family, texts)) or [{"edu_score": 4}],
        )
    )
    monkeypatch.setattr(
        handler,
        "_read_payload",
        lambda: {
            "model_family": "source-arxiv-posttrain",
            "text": "full section",
        },
    )
    monkeypatch.setattr(handler, "_write", lambda status, payload: writes.append((status, payload)))
    handler.do_POST()
    assert calls == [("source-arxiv-posttrain", ["full section"])]
    assert writes == [(200, {"edu_score": 4})]
