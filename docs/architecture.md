# Architecture

## Kappa data flow

![Stream2Pretrain Kappa architecture](architecture.svg)

The companion Mermaid definition is in [`architecture.mmd`](architecture.mmd)
and provides a more detailed editable view of the same data flow.

The system is streaming-only. Live input and explicitly approved replay use the
same Redpanda and Bytewax path. Discovery envelopes only schedule content
retrieval and never enter document, route, or acceptance totals.

## Topics

| Topic | Producer | Consumer |
|---|---|---|
| `license.admissions` | content ingest workers | Iceberg writer and monitoring |
| `raw.fetched` | content ingest workers | Bytewax fetcher |
| `docs.normalized` | Bytewax fetcher | Bytewax curator |
| `curation.decisions` | Bytewax curator | Iceberg writer and monitoring |
| `docs.curated` | Bytewax curator | Iceberg writer and Foundry intake |
| smoke variants | isolated deployment smoke | isolated smoke consumers |

## Processing components

- arXiv full-text worker: licence resolution, native HTML, ar5iv fallback,
  bounded CPU PDF fallback, immutable Bronze publication.
- Hugging Face workers: durable paginated watermark, exact repository
  provenance, immutable README-blob identity, README-only body fetch, and
  immutable Bronze publication.
- Fetcher: source dispatch, structured paper extraction, Markdown card
  projection, language metadata, scientific artifact persistence, Silver emit.
- Curator: source-specific quality scoring, PII, exact and near duplicate state,
  composite and reasoning signals, route decision.
- Iceberg writer: durable licence-admission, curation-decision, and accepted
  corpus tables with idempotent identity handling.
- Foundry: ranked 24-hour scientific-paper cohort, task and evidence graph
  generation, two solver trajectories, verifier compilation, deterministic
  validation, signed packaging, and per-artifact human audit.

## Storage

- `s2p-bronze`: short-retention source bodies.
- `s2p-silver`: structured scientific JSON and assets with bounded retention.
- `s2p-gold`: Iceberg data and metadata for route decisions and accepted text.
- `s2p-posttrain`: generated tasks, trajectories, environments, packages, and
  audit evidence.
- retained PVCs: Bytewax recovery, dedup state, and Foundry control state.

The repository-owned [`charts/minio`](../charts/minio) release creates the
single-node course object store, persistent volume, monitoring endpoint and
required buckets. A distributed MinIO or external S3 service is the documented
larger-deployment replacement.

The physical Iceberg tables are `license_admissions`, `curation_decisions`, and
`curated`. DuckDB combines admission and curation rows into the logical corpus
route ledger used by the cockpit; the ledger is a serving view, not a fourth
physical table.

## References

- [Kappa Architecture proposal](https://www.oreilly.com/radar/questioning-the-lambda-architecture/)
- [Redpanda architecture](https://docs.redpanda.com/current/get-started/architecture/)
- [Bytewax project and documentation](https://github.com/bytewax/bytewax)
- [Apache Iceberg specification](https://iceberg.apache.org/spec/)
- [Included container-orchestration lecture](../lecture_slides/04%20-%20Container%20Orchestration.md)
- [Included storage and networking lecture](../lecture_slides/04c%20-%20Storage%20and%20Networking.md)

## UI

Dashboard, Sources, Documents, and Datasets are monitoring/export
views backed by declaratively configured workloads. Document and job detail
opens in a dialog. Only named approval or rejection of a generated SFT/RL
artifact mutates product state.

## Scaling

Independently committing ingest workers and stateless model services can scale
horizontally. The core Bytewax flows currently run as one coordinated execution
per stage because recovery and global near-duplicate state must not be forked by
ordinary replica scaling. CPU, memory, lag, and object growth are measured in
Prometheus. The external Foundry model API is provider managed, so the project
controls request concurrency rather than its replica count. Any production
throughput claim remains `needs-measurement` until a target-cluster run records
it.
