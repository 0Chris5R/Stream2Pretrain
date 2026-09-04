"""Regression tests for timeout, retention, and concurrent commit failures."""

from __future__ import annotations

import io
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from processor.foundry import lakehouse
from processor.model_client import ModelServiceError, _post_json
from processor.model_jobs import InferenceJobs
from processor.operators.quality import QualityScore
from processor.quality_cache import CachedQualityScorer
from processor.scientific_handoff import ScientificHandoff, evidence_capsule
from schemas.scientific import ScientificDocument


def test_curator_retries_transient_model_failure_without_advancing_or_restarting(
    monkeypatch,
    cfg,
    silver_record,
) -> None:
    from processor import common, curate

    state = curate.build_state(cfg)
    attempts = []

    def prefetch(current, _papers):
        attempts.append(1)
        if len(attempts) == 1:
            raise ModelServiceError("Pod replaced")
        return current

    monkeypatch.setattr(curate, "_prefetched_curate_state", prefetch)
    monkeypatch.setattr(curate.time, "sleep", lambda _: None)
    monkeypatch.setattr(curate, "_materialize_uncached_decision", lambda *a, **k: (b"done", True))
    try:
        results = curate.process_silver_decision_payloads(
            state, [common.silver_dumps(silver_record)]
        )
        assert results[0].value == (b"done", True)
        assert len(attempts) >= 2
    finally:
        state.close()


def test_long_job_is_not_cancelled_by_poll_and_duplicate_submission() -> None:
    jobs = InferenceJobs()
    release = threading.Event()
    calls = []

    def work():
        calls.append(1)
        assert release.wait(5)
        return {"results": [4.1]}

    try:
        key = jobs.submit(b"exact request and model revision", work)
        assert jobs.result(key, wait_seconds=0.001) is None
        assert jobs.submit(b"exact request and model revision", work) == key
        release.set()
        assert jobs.result(key) == {"results": [4.1]}
        assert calls == [1]
    finally:
        release.set()
        jobs.close()


def test_model_job_capacity_never_evicts_active_work() -> None:
    jobs = InferenceJobs(capacity=1)
    release = threading.Event()
    try:
        key = jobs.submit(b"first", lambda: {"done": release.wait(5)})
        with pytest.raises(RuntimeError, match="full"):
            jobs.submit(b"second", lambda: {})
        release.set()
        assert jobs.result(key) == {"done": True}
        assert jobs.result(jobs.submit(b"second", lambda: {"ok": True})) == {"ok": True}
    finally:
        release.set()
        jobs.close()


@pytest.mark.parametrize("disappeared", [False, True])
def test_quality_client_polls_the_same_backend(disappeared: bool) -> None:
    requests = []
    key = "a" * 64

    def handler(request):
        requests.append(request)
        assert request.url.host == "pod-one"
        if request.method == "POST":
            assert request.headers["Prefer"] == "respond-async"
            return httpx.Response(202, json={"job_id": key})
        assert request.url.path == f"/v1/quality-jobs/{key}"
        if disappeared:
            return httpx.Response(404)
        return httpx.Response(200, json={"results": [4.1]})

    with httpx.Client(base_url="http://pod-one", transport=httpx.MockTransport(handler)) as client:
        if disappeared:
            with pytest.raises(ModelServiceError, match="replaced"):
                _post_json(client, "/v1/quality:batch", {"texts": ["complete section"]})
        else:
            assert _post_json(client, "/v1/quality:batch", {})[0] == {"results": [4.1]}
    assert len(requests) == 2


def test_section_cache_survives_restart_and_invalidates_exact_inputs(tmp_path: Path) -> None:
    calls = []

    def score_many(texts):
        calls.extend(texts)
        return [
            QualityScore(
                edu_score=4.2,
                revision="r1",
                probabilities=(0.1, 0.9),
                diagnostic_scores={
                    "arxiv-math-reasoning": {
                        "edu_score": 3.2,
                        "probabilities": [0.1, 0.9],
                        "model_revision": "math@pinned",
                    }
                },
            )
            for _ in texts
        ]

    scorer = SimpleNamespace(revision="r1", backend="test", score_many=score_many)
    path = str(tmp_path / "scores.sqlite3")
    cache = CachedQualityScorer(scorer, path)
    expected = cache.score("full section")
    cache.close()
    cache = CachedQualityScorer(scorer, path)
    try:
        assert cache.score("full section") == expected
        assert calls == ["full section"]
        cache.score("full section changed")
        scorer.revision = "r2"
        cache.score("full section")
        assert len(calls) == 3
        with ThreadPoolExecutor(max_workers=4) as pool:
            assert len(list(pool.map(cache.score, ["other"] * 8))) == 8
    finally:
        cache.close()


def test_evidence_survives_missing_transient_object_without_truncation() -> None:
    document = ScientificDocument(
        doc_id="sha256:" + "f" * 64,
        source_url="https://arxiv.org/abs/2609.00001",
        text_sha256="f" * 64,
        extraction_pipeline="test",
    )
    stored = []

    def put(**kwargs):
        stored.append(kwargs)

    handoff = ScientificHandoff(SimpleNamespace(put_object=put), "gold")
    uri = handoff.preserve(document.doc_id, evidence_capsule(document), "s3://silver/expired")
    assert uri.startswith("s3://gold/scientific-evidence/")
    assert ScientificDocument.model_validate_json(stored[0]["Body"]) == document
    assert handoff.preserve(document.doc_id, None, uri) == uri
    with pytest.raises(ValueError, match="mismatch"):
        handoff.preserve("sha256:" + "a" * 64, evidence_capsule(document), None)


def test_legacy_evidence_is_copied_exactly() -> None:
    document = ScientificDocument(
        doc_id="sha256:" + "f" * 64,
        source_url="https://arxiv.org/abs/2609.00001",
        text_sha256="f" * 64,
        extraction_pipeline="test",
    )
    payload = document.model_dump_json().encode()
    stored = []
    s3 = SimpleNamespace(
        get_object=lambda **_: {"Body": io.BytesIO(payload)},
        put_object=lambda **kwargs: stored.append(kwargs),
    )
    ScientificHandoff(s3, "gold").preserve(document.doc_id, None, "s3://silver/paper.json")
    assert stored[0]["Body"] == payload


def test_foundry_refreshes_and_deduplicates_after_unknown_commit(monkeypatch) -> None:
    class CommitStateUnknownException(Exception):  # noqa: N818 - PyIceberg's public name
        pass

    durable = set()
    loads = []
    appends = []

    def append(values):
        appends.append(values)
        durable.update(v.event_id for v in values)
        raise CommitStateUnknownException("acknowledgement lost")

    def load():
        loads.append(1)
        return SimpleNamespace(append=append)

    monkeypatch.setattr(lakehouse, "_load_ids", lambda *_: set(durable))
    monkeypatch.setattr(lakehouse.time, "sleep", lambda _: None)
    result = lakehouse.FoundryLakehouseSink._append_unique(
        load,
        [SimpleNamespace(event_id="e1")],
        "event_id",
        lambda v: v,
        None,
    )
    assert result == {"e1"}
    assert len(appends) == 1
    assert len(loads) == 2


def test_foundry_serializes_concurrent_buffer_writers(monkeypatch) -> None:
    monkeypatch.setattr(lakehouse, "load_runtime_catalog", lambda: object())
    sink = lakehouse.FoundryLakehouseSink(batch_size=1)
    durable = set()
    monkeypatch.setattr(lakehouse, "_load_ids", lambda *_: set(durable))
    monkeypatch.setattr(lakehouse, "_events_arrow", lambda v: v)
    table = SimpleNamespace(append=lambda values: durable.update(v.event_id for v in values))
    monkeypatch.setattr(sink, "_ensure_events_table", lambda: table)
    values = [SimpleNamespace(event_id=f"e{i}") for i in range(20)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(sink.add_event, values * 2))
    assert durable == {v.event_id for v in values}
    assert not sink._events
