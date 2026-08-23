# Stream2Pretrain - ingest layer

The ingest layer turns external feeds into ``BronzeRecord`` events on the
``raw.fetched`` Redpanda topic, with the raw bytes parked in MinIO under
``s3://bronze/year=YYYY/month=MM/day=DD/source=<feed>/<doc_id>.<ext>``.

See ``../SOURCES.md`` for the full feed catalogue and rate limits, and
``../RESEARCH.md`` section 7 for the architectural plan.

## Components

| Path | Shape | Trigger |
|------|-------|---------|
| ``rss_poller/`` | RSS / Atom CronJob | every 2 h (configurable per SourceFeed) |
| ``oaipmh_poller/`` | OAI-PMH 2.0 CronJob (arXiv default) | every 2 h |
| ``sitemap_poller/`` | Gzipped sitemap.xml CronJob with index expansion | every 24 h |
| ``github_events/`` | Long-running Deployment (60 s ``X-Poll-Interval``) | continuous |
| ``github_releases/`` | Atom feed CronJob across curated AI repos | every 2 h |
| ``github_release_tarball_fetcher/`` | Per-release source tarball expander, one ``CodeFileRecord`` per allow-listed file | event-driven on ``github.release.jobs`` |
| ``hf_poller/`` | HF Hub REST CronJob (models + daily_papers) | every 10-15 min / 6 h |
| ``arxiv_html_fetcher/`` | Native arXiv ``/html/<id>`` fulltext fetcher with ``ar5iv.labs.arxiv.org`` fallback | event-driven on ``docs.normalized`` |
| ``openreview_poller/`` | OpenReview API v2 venue poll + REVIEWARENA backfill | every 6 h / one-shot |
| ``common/`` | Shared HTTP client, Kafka producer, MinIO writer, OTel, structlog | n/a |

Every poller emits identical ``BronzeRecord`` shapes per ``schemas/bronze.py``.

## Local dev loop

```
docker compose -f ../docker-compose.dev.yml up -d
export S2P_FEED_CONFIG=$(pwd)/feeds.dev.yaml
export S2P_ENV=dev
export GITHUB_TOKEN=ghp_...
export HF_TOKEN=hf_...

# One-pass pollers
uv run python -m ingest.rss_poller.poller
uv run python -m ingest.sitemap_poller.poller
uv run python -m ingest.oaipmh_poller.poller
uv run python -m ingest.github_releases.poller
uv run python -m ingest.hf_poller.poller

# Long-running pollers
uv run python -m ingest.github_events.poller &

# v0.2.0 fulltext + code fetchers (one-shot mode)
uv run python -m ingest.arxiv_html_fetcher.fetcher --once
uv run python -m ingest.openreview_poller.poller --once
uv run python -m ingest.github_release_tarball_fetcher.fetcher --once
```

## Tests

```
uv run pytest ingest/
```

Every poller has unit tests with HTTP fan-out mocked via ``httpx.MockTransport``;
no test reaches a real network. ``BronzeProducer`` and ``MinioWriter`` are
substituted with in-memory fakes from ``ingest/common/tests/conftest.py``.
