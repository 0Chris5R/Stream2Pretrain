# Architecture

## Kappa data flow

![Stream2Pretrain Kappa architecture](diagram-architecture.svg)

The companion Mermaid definition is in
[`diagram-architecture.mmd`](diagram-architecture.mmd)
and provides a more detailed editable view of the same data flow.

The system is streaming-only. Live input and explicitly approved replay use the
same Redpanda and Bytewax path. Discovery envelopes only schedule content
retrieval and never enter document, route, or acceptance totals.

## Topics

| Topic | Producer | Consumer |
|---|---|---|
| `arxiv.discovery` | RSS and OAI-PMH discovery workers | partitioned arXiv full-text workers |
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
- retained PVCs: Bytewax and Foundry recovery state.
- PostgreSQL: Polaris metadata, global curator duplicate and decision state,
  and Foundry queue, quota, call, and audit coordination.

The repository-owned [`charts/minio`](../charts/minio) release creates the
four-member distributed object store, one retained volume per member, client
and peer Services, monitoring endpoint, and required buckets. This topology is
render-validated. The frozen live evidence shows the earlier one-member layout,
so migration time, node-loss recovery, and usable capacity remain
`needs-measurement`.

The physical Iceberg tables are `license_admissions`, `curation_decisions`, and
`curated`. DuckDB combines admission and curation rows into the logical corpus
route ledger used by the cockpit. The ledger is a serving view, not a fourth
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

Every application component has an explicit horizontal-scaling contract.
SourceFeed reconciliation is active-passive through a Kubernetes Lease, feed
pollers lease each cursor, and arXiv full-text workers use consumer-group
partition ownership on `arxiv.discovery`. Stateless model, API, and UI
Deployments scale through replica settings or KEDA.

Fetcher, curator, Iceberg writer, and Foundry worker replicas each form one
distributed Bytewax execution with stable peer identities and shared RWX
recovery. The curator coordinates global duplicate and decision state in
PostgreSQL. Iceberg writers retry optimistic commit conflicts with deterministic
row identities. Foundry workers use fenced PostgreSQL candidate and quota
leases. Each DuckDB API replica rebuilds a private serving index from Iceberg
and retained Kafka topics.

The horizontal profile renders two replicas for every application component,
and deterministic tests exercise the coordination primitives. This is offline
contract evidence, not a live failover or throughput result. The frozen cluster
evidence demonstrates UI and classifier replicas only. The external Foundry
model API is provider managed, so the project controls request concurrency
rather than its replica count. Live multi-replica recovery, safe replica limits,
and production throughput remain `needs-measurement`.
