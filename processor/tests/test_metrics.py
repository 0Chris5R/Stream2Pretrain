from __future__ import annotations

from processor.metrics import ProcessorMetrics


def test_processor_metrics_render_dashboard_contract() -> None:
    metrics = ProcessorMetrics(namespace="stream2pretrain")

    metrics.record_received(source_feed="arxiv-rss")
    metrics.record_normalized(source_feed="arxiv-rss")
    metrics.record_curated(source_feed="arxiv-rss", quality_score=3.25, edu_score=4.0)
    metrics.record_dropped(reasons=["license_excluded"], quality_score=0.75, edu_score=1.0)
    metrics.record_route(route="reasoning_candidate")
    metrics.record_failure(stage="normalize", reason="payload_too_large")
    metrics.record_iceberg_flush(rows=2, decisions=3, seconds=0.12)

    body = metrics.render_prometheus().decode("utf-8")

    assert (
        's2p_processor_received_total{namespace="stream2pretrain",source="arxiv-rss"} 1.0' in body
    )
    assert (
        's2p_processor_ingested_total{namespace="stream2pretrain",source="arxiv-rss"} 1.0' in body
    )
    assert 's2p_processor_curated_total{namespace="stream2pretrain",source="arxiv-rss"} 1.0' in body
    assert (
        's2p_processor_dropped_total{namespace="stream2pretrain",reason="license_excluded"} 1.0'
        in body
    )
    assert 's2p_documents_emitted_total{namespace="stream2pretrain",stage="normalize"} 1.0' in body
    assert 's2p_documents_emitted_total{namespace="stream2pretrain",stage="curate"} 1.0' in body
    assert 's2p_documents_emitted_total{namespace="stream2pretrain",stage="iceberg"} 2.0' in body
    assert "s2p_quality_score_bucket" in body
    assert "s2p_fineweb_edu_score_bucket" in body
    assert (
        's2p_processor_routed_total{namespace="stream2pretrain",route="reasoning_candidate"} 1.0'
        in body
    )
    assert "s2p_iceberg_flush_seconds_bucket" in body
    assert (
        's2p_processor_failures_total{namespace="stream2pretrain",reason="payload_too_large",stage="normalize"} 1.0'
        in body
    )
