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
    assert result["observed_backends"] == 2
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
    assert result["observed_backends"] == 6
    assert result["ordered_batch_matches_singletons"] is True


def test_distribution_gate_resamples_backend_that_joins_mid_probe(
    monkeypatch,
    capsys,
) -> None:
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
                "backend-a",
            )
        assert payload is not None
        call_number = next(call_numbers)
        if url.endswith("/v1/quality:batch"):
            texts = payload["texts"]
            assert isinstance(texts, list)
            return (
                {
                    "results": [
                        {"edu_score": len(str(text)), "revision": "finepdfs@test"} for text in texts
                    ]
                },
                "backend-a",
            )
        if call_number < 58:
            backend = "backend-a" if call_number % 2 == 0 else "backend-b"
        elif call_number < 60:
            backend = "backend-c"
        else:
            backend = ("backend-a", "backend-b", "backend-c")[(call_number - 60) % 3]
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
            "2",
        ],
    )

    benchmark_model_service.main()

    result = json.loads(capsys.readouterr().out)
    assert result["distribution_requests"] == 120
    assert result["observed_backends"] == 3
    assert result["minimum_backend_share"] >= result["minimum_required_backend_share"]


def test_headless_gate_probes_each_ready_endpoint_directly(
    monkeypatch,
    capsys,
) -> None:
    cluster_calls = count(0)

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
        if url.startswith("http://10.0.0.1"):
            backend = "backend-a"
        elif url.startswith("http://10.0.0.2"):
            backend = "backend-b"
        else:
            backend = "backend-a" if next(cluster_calls) % 2 == 0 else "backend-b"
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
        benchmark_model_service,
        "resolved_endpoint_urls",
        lambda *_args, **_kwargs: ["http://10.0.0.1:8094", "http://10.0.0.2:8094"],
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "benchmark_model_service.py",
            "--base-url",
            "http://quality:8094",
            "--headless-host",
            "quality-headless",
            "--model-family",
            "finepdfs-edu-v2",
            "--expected-backends",
            "2",
        ],
    )

    benchmark_model_service.main()

    result = json.loads(capsys.readouterr().out)
    assert result["direct_endpoint_count"] == 2
    assert result["direct_backends"] == ["backend-a", "backend-b"]
    assert result["ordered_batch_matches_singletons"] is True
