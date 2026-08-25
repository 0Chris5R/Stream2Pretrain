from __future__ import annotations

from ingest.common.metrics import IngestMetrics


def test_ingest_metrics_render_feed_poll_contract() -> None:
    metrics = IngestMetrics(namespace="stream2pretrain")

    metrics.record_feed_poll(source_feed="github-releases", outcome="success")
    metrics.record_feed_poll(source_feed="github-releases", outcome="error")

    body = metrics.render_prometheus().decode("utf-8")

    assert "s2p_process_up 1" in body
    assert (
        's2p_feed_poll_total{namespace="stream2pretrain",source_feed="github-releases",outcome="success"} 1'
        in body
    )
    assert (
        's2p_feed_poll_total{namespace="stream2pretrain",source_feed="github-releases",outcome="error"} 1'
        in body
    )


def test_ingest_metrics_escape_labels() -> None:
    metrics = IngestMetrics(namespace='dev"cluster')

    metrics.record_feed_poll(source_feed='feed"name', outcome="line\nbreak")

    body = metrics.render_prometheus().decode("utf-8")

    assert 'namespace="dev\\"cluster"' in body
    assert 'source_feed="feed\\"name"' in body
    assert 'outcome="line\\nbreak"' in body
