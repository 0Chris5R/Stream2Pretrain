from __future__ import annotations

import json
import sys
from itertools import count
from typing import Any

from scripts import benchmark_model_service


def test_distribution_gate_runs_real_inference_and_checks_batch_parity(
    monkeypatch,
    capsys,
) -> None:
    inference_calls: list[dict[str, Any]] = []
    call_numbers = count(1)

    def fake_request(
        _opener: object,
        url: str,
        *,
        payload: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], str]:
        if url.endswith("/v1/metadata"):
            return (
                {
                    "ready": True,
                    "quality": {
                        "finepdfs-edu-v2": {"revision": "finepdfs@test"},
                    },
                },
                "backend-a",
            )
        assert payload is not None
        inference_calls.append(payload)
        backend = "backend-a" if next(call_numbers) % 2 else "backend-b"
        if url.endswith("/v1/quality:batch"):
            texts = payload["texts"]
            assert isinstance(texts, list)
            return (
                {
                    "results": [
                        {"edu_score": len(str(text)), "revision": "finepdfs@test"} for text in texts
                    ]
                },
                backend,
            )
        text = str(payload["text"])
        return (
            {"edu_score": len(text), "revision": "finepdfs@test"},
            backend,
        )

    monkeypatch.setattr(benchmark_model_service, "_request", fake_request)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "benchmark_model_service.py",
            "--base-url",
            "http://quality",
            "--model-family",
            "finepdfs-edu-v2",
            "--expected-backends",
            "2",
        ],
    )

    benchmark_model_service.main()

    result = json.loads(capsys.readouterr().out)
    assert result["backend_requests"] == {"backend-a": 30, "backend-b": 30}
    assert result["batch_parity_backend"] == "backend-a"
    assert result["ordered_batch_matches_singletons"] is True
    assert result["revision"] == "finepdfs@test"
    assert result["distribution_requests"] == 60
    assert result["minimum_required_backend_share"] == 0.10
    assert len(inference_calls) == 65


def test_distribution_gate_scales_sample_and_fairness_with_backend_count(
    monkeypatch,
    capsys,
) -> None:
    backends = [f"backend-{index}" for index in range(6)]
    call_numbers = count(0)

    def fake_request(
        _opener: object,
        url: str,
        *,
        payload: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], str]:
        if url.endswith("/v1/metadata"):
            return (
                {
                    "ready": True,
                    "quality": {
                        "finepdfs-edu-v2": {"revision": "finepdfs@test"},
                    },
                },
                backends[0],
            )
        assert payload is not None
        backend = backends[next(call_numbers) % len(backends)]
        if url.endswith("/v1/quality:batch"):
            texts = payload["texts"]
            assert isinstance(texts, list)
            return (
                {
                    "results": [
                        {"edu_score": len(str(text)), "revision": "finepdfs@test"} for text in texts
                    ]
                },
                backend,
            )
        text = str(payload["text"])
        return ({"edu_score": len(text), "revision": "finepdfs@test"}, backend)

    monkeypatch.setattr(benchmark_model_service, "_request", fake_request)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "benchmark_model_service.py",
            "--base-url",
            "http://quality",
            "--model-family",
            "finepdfs-edu-v2",
            "--expected-backends",
            "6",
        ],
    )

    benchmark_model_service.main()

    result = json.loads(capsys.readouterr().out)
    assert result["distribution_requests"] == 120
    assert result["backend_requests"] == {backend: 20 for backend in backends}
    assert result["minimum_backend_share"] == 1 / 6
    assert result["minimum_required_backend_share"] == 1 / 12
    assert result["ordered_batch_matches_singletons"] is True
