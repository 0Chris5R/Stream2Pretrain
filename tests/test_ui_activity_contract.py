from __future__ import annotations

from pathlib import Path

ROUTE = Path(__file__).parents[1] / "ui/app/api/activity/route.ts"


def test_activity_uses_supported_redpanda_public_metric_per_stage() -> None:
    source = ROUTE.read_text()

    assert "redpanda_kafka_records_produced_total" not in source
    for topic in (
        "raw.fetched",
        "docs.normalized",
        "curation.decisions",
        "docs.curated",
    ):
        assert (
            f'sum(redpanda_kafka_max_offset{{redpanda_namespace="kafka",redpanda_topic="{topic}"}})'
        ) in source
