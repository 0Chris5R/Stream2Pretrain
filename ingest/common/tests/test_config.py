"""Runtime config contract tests for ingest components."""

from __future__ import annotations

from ingest.common.config import load_config


def test_load_config_reads_kubernetes_env_contract(monkeypatch) -> None:
    monkeypatch.setenv("S2P_ENV", "prod")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("REDPANDA_BROKERS", "redpanda:9092")
    monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "access")
    monkeypatch.setenv("MINIO_SECRET_KEY", "secret")
    monkeypatch.setenv("MINIO_BRONZE_BUCKET", "bronze-prod")
    monkeypatch.setenv("S2P_RAW_TOPIC", "raw.prod")
    monkeypatch.setenv("S2P_GITHUB_RELEASE_JOBS_TOPIC", "github.release.prod")

    cfg = load_config()

    assert cfg.env == "prod"
    assert cfg.log_level == "DEBUG"
    assert cfg.redpanda_brokers == "redpanda:9092"
    assert cfg.minio_endpoint == "http://minio:9000"
    assert cfg.minio_access_key == "access"
    assert cfg.minio_secret_key == "secret"
    assert cfg.minio_bronze_bucket == "bronze-prod"
    assert cfg.raw_topic == "raw.prod"
    assert cfg.github_release_jobs_topic == "github.release.prod"
