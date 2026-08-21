from __future__ import annotations

from scripts.capacity_probe import (
    parse_byte_quantity,
    parse_cpu_quantity,
    render_markdown,
    workload_resources,
)


def test_parse_cpu_quantity_returns_millicores() -> None:
    assert parse_cpu_quantity("500m") == 500
    assert parse_cpu_quantity("1") == 1000
    assert parse_cpu_quantity("2.5") == 2500
    assert parse_cpu_quantity("250000u") == 250
    assert parse_cpu_quantity("bad") is None


def test_parse_byte_quantity_returns_bytes() -> None:
    assert parse_byte_quantity("512Mi") == 512 * 1024**2
    assert parse_byte_quantity("2Gi") == 2 * 1024**3
    assert parse_byte_quantity("1000") == 1000
    assert parse_byte_quantity("bad") is None


def test_workload_resources_sums_pod_requests(monkeypatch) -> None:
    def fake_kubectl_json(args: list[str], *, namespace: str | None = None):
        assert args == ["get", "pods"]
        assert namespace == "stream2pretrain"
        return (
            {
                "items": [
                    {
                        "metadata": {"name": "processor-curate-0"},
                        "status": {"phase": "Running"},
                        "spec": {
                            "containers": [
                                {
                                    "resources": {
                                        "requests": {"cpu": "500m", "memory": "1Gi"},
                                        "limits": {"cpu": "2", "memory": "2Gi"},
                                    }
                                }
                            ]
                        },
                    }
                ]
            },
            None,
        )

    monkeypatch.setattr("scripts.capacity_probe.kubectl_json", fake_kubectl_json)

    report = workload_resources("stream2pretrain")

    assert report["status"] == "measured"
    assert report["request_cpu_m"] == 500
    assert report["request_memory_bytes"] == 1024**3
    assert report["limit_cpu_m"] == 2000
    assert report["limit_memory_bytes"] == 2 * 1024**3


def test_render_markdown_keeps_missing_values_as_needs_measurement() -> None:
    report = {
        "generated_at": "2026-06-17T10:00:00Z",
        "namespace": "stream2pretrain",
        "node_capacity": {"status": "needs-measurement", "reason": "no cluster"},
        "workload_resources": {
            "status": "measured",
            "request_cpu_m": 500,
            "request_memory_bytes": 1024,
            "limit_cpu_m": 1000,
            "limit_memory_bytes": 2048,
        },
        "pvc_capacity": {"status": "measured", "claims": []},
        "redpanda_topics": {"status": "needs-measurement"},
        "minio_capacity": {"status": "needs-measurement"},
    }

    rendered = render_markdown(report)

    assert "needs-measurement" in rendered
    assert "Stream2Pretrain Capacity Report" in rendered
