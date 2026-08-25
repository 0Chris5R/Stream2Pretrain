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
| ``github_releases/`` | Atom feed CronJob across curated AI repos | every 2 h |
| ``github_release_tarball_fetcher/`` | Per-release source tarball expander, one ``CodeFileRecord`` per allow-listed file | event-driven on ``github.release.jobs`` |
| ``hf_poller/`` | HF Hub exact-version model and dataset cards | every 10-15 min |
| ``arxiv_html_fetcher/`` | Native arXiv ``/html/<id>`` fulltext fetcher with ar5iv and bounded CPU PDF fallbacks | event-driven |
| ``common/`` | Shared HTTP client, Kafka producer, MinIO writer, OTel, structlog | n/a |

Content pollers emit identical ``BronzeRecord`` shapes per
``schemas/bronze.py``. Discovery-only records schedule the corresponding
full-text or release worker and are excluded from training-body curation.

## Local dev loop

```
docker compose -f ../docker-compose.dev.yml up -d
export S2P_FEED_CONFIG=$(pwd)/feeds.dev.yaml
export S2P_ENV=dev
export GITHUB_TOKEN=ghp_...
export HF_TOKEN=hf_...

# One-pass pollers
uv run python -m ingest.rss_poller.poller
uv run python -m ingest.oaipmh_poller.poller
uv run python -m ingest.github_releases.poller
uv run python -m ingest.hf_poller.poller

# Event-driven content workers are deployed through Helm.
```

## Tests

```
uv run pytest ingest/
```

Every poller has unit tests with HTTP fan-out mocked via ``httpx.MockTransport``;
no test reaches a real network. ``BronzeProducer`` and ``MinioWriter`` are
substituted with in-memory fakes from ``ingest/common/tests/conftest.py``.
