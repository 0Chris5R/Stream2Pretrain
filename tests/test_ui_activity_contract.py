from __future__ import annotations

from pathlib import Path

ROUTE = Path(__file__).parents[1] / "ui/app/api/activity/route.ts"
DASHBOARD_ROUTE = Path(__file__).parents[1] / "ui/app/api/dashboard/route.ts"
DUCKDB_API = Path(__file__).parents[1] / "processor/duckdb_api.py"
DUCKDB_CHART = (
    Path(__file__).parents[1] / "charts/stream2pretrain/templates/processor-duckdb-api.yaml"
)


def test_activity_uses_content_only_processor_stage_counters() -> None:
    source = ROUTE.read_text()

    assert "redpanda_kafka_records_produced_total" not in source
    assert "redpanda_kafka_max_offset" not in source
    assert "s2p_processor_received_total" in source
    assert 's2p_documents_emitted_total{stage="normalize"}' in source
    assert "s2p_processor_routed_total" in source
    assert "s2p_processor_curated_total" in source
    assert 'redpanda_topic="docs.normalized"' not in source
    assert 'redpanda_topic="curation.decisions"' not in source
    assert 'redpanda_topic="docs.curated"' not in source


def test_static_dashboard_uses_only_durable_corpus_apis() -> None:
    source = DASHBOARD_ROUTE.read_text()

    assert "/corpus-overview" in source
    assert "/quality-histogram" in source
    assert "/curation-summary" in source
    assert "PROMETHEUS_URL" not in source
    assert "s2p_processor_" not in source
    assert "s2p_documents_emitted_total" not in source


def test_monitoring_queries_are_isolated_from_explicit_history_queries() -> None:
    source = DUCKDB_API.read_text()
    chart = DUCKDB_CHART.read_text()

    assert 'thread_name_prefix="serving-query"' in source
    assert 'thread_name_prefix="history-query"' in source
    assert "historical_service or service" in source
    assert "S2P_SERVING_INDEX_ENABLED" in chart
    assert "mountPath: /var/lib/s2p-serving" in chart
    assert "helm.sh/resource-policy: keep" in chart
