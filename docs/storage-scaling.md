# Storage ownership and scaling

The object store and compute workers scale independently. Object APIs allow an
external S3-compatible store or distributed MinIO deployment without changing
the training-data format.

## Retention

| Data | Owner | Lifecycle |
|---|---|---|
| Admitted source bytes | `s2p-bronze` | One-day audit retention |
| Transient extracted assets and scientific JSON | `s2p-silver` | One-day audit retention |
| Accepted paper evidence capsule | `s2p-gold/scientific-evidence/` | Durable gzip JSON, persisted before candidate publication |
| Licence decisions and curation outcomes | Iceberg tables in `s2p-gold` | Durable latest state and policy provenance |
| Training text | Iceberg Parquet in `s2p-gold` | Durable, never subject to raw-object expiry |
| Generated SFT/RL packages and audits | `s2p-posttrain` plus PostgreSQL | Durable artifacts and named audit history |
| In-flight events | Redpanda | Topic-specific bounded retention |
| Bytewax and Foundry recovery | Component checkpoint PVCs | Retained progress state and backups |
| Polaris catalog and application coordination | PostgreSQL | Durable table pointers plus curator duplicate and decision state and Foundry queue, quota, outbox, and audit state |
| DuckDB serving index | Per-Pod `emptyDir` | Derived state rebuilt from Iceberg and retained Kafka topics |

Explicitly incompatible items stop before content fetch. Structured evidence
travels in normalized events, is materialized durably before candidate
publication, and is cached with the Foundry queue entry. Transient raw or figure
expiry does not erase training text or admitted paper evidence.

The application lifecycle hook configures only Bronze and Silver expiry and
preserves unrelated operator rules. Object expiry is asynchronous. It does not
apply age deletion to Gold, post-training or state buckets.

## Write and maintenance behavior

Iceberg commits use count- or time-bounded batches. The Foundry commits one
cross-job batch after 5,000 records or one hour, whichever comes first. Its
shared coordination database is a durable outbox, so an accepted package or
audit event survives a worker restart before the next Iceberg commit. Kubernetes
uses PostgreSQL, while local development can use SQLite. Deterministic duplicate
admissions and decisions do not create repeated logical rows. Snapshot
properties belong to snapshots, not an ever-growing table-property log.

The maintenance job expires snapshots older than 24 hours while retaining at
least ten snapshots. It then walks every retained snapshot and protects current
metadata JSON, metadata-log JSON, manifest lists, manifests, live Parquet files
and statistics files. Only unreferenced table objects older than 24 hours are
deleted, after one final catalog reload immediately before deletion. Scientific
evidence and signed packages are outside table roots and can never be selected.
Maintenance is not an age-based deletion policy for corpus rows.

Each DuckDB API Pod rebuilds a private serving index from Iceberg and then
consumes transactional Kafka deltas with its own consumer identity. The index
and aggregate cache use `emptyDir` derived state and are never the sole copy of
the corpus.

## Capacity

The DHBW k3s `local-path` provisioner stores data on node filesystems. A larger
PVC request does not supply new physical disk or impose a storage quota.
The pinned Ansible role also installs Longhorn as a non-default StorageClass,
with the iSCSI and NFS prerequisites required for `ReadWriteMany` volumes.

The horizontal Bytewax overlay explicitly selects Longhorn RWX checkpoint
claims. Kubernetes cannot change an existing PVC's access mode or StorageClass.
Both deployment paths therefore inspect retained claims before quiescing the
workers and fail on RWO, `local-path` or class drift. Migration requires a
verified checkpoint copy into the target claim. No deployment command deletes
or rewrites the retained source claim.

## Non-destructive migration prerequisites

Stateful topology changes require a recorded maintenance boundary and verified
copies before any source resource is retired. The deployment scripts do not
perform these migrations.

- MinIO: create the target distributed MinIO or external S3 service, copy every
  application bucket while the source remains intact, and compare per-bucket
  object counts, byte totals, and a cryptographic checksum manifest. Verify
  representative Iceberg metadata, scientific evidence, and signed package
  reads from the target before changing endpoints.
- PostgreSQL: quiesce catalog and application writers, take consistent
  `pg_dump` backups of the Polaris and coordination databases, restore them into
  the target cluster, and validate both schemas. Confirm table counts,
  Iceberg table resolution, curator duplicate and decision state, Foundry queue
  state, quota records, outbox rows, and artifact audits before reconnecting
  workers.
- Redpanda: change replication through broker-aware partition replica
  reassignment. Wait until every target replica is in sync, confirm there are no
  under-replicated partitions, and compare topic partition counts and consumer
  group offsets before removing an old broker or volume.
- Bytewax and Foundry recovery: stop the coordinated execution at a recorded
  boundary, snapshot the source claim, copy every recovery file to the
  pre-created RWX claim, compare cryptographic checksums, and verify the flow,
  recovery, and cutover identities before starting the new replica count.

Keep every source volume and backup until the restored services pass these
checks. Deletion and rollback cleanup require separate explicit approval.

### Legacy Foundry SQLite cutover

The retired `stream2pretrain-foundry` StatefulSet created the claim
`state-stream2pretrain-foundry-0`. Both its worker and API mounted that claim at
`/var/lib/s2p/foundry`, so it can contain `control.sqlite3` and `quota.sqlite3`.
The first file includes the queue, provider results, outbox state, artifacts,
and append-only reviewer audits. Copying only the Bytewax recovery directory
does not preserve either database.

Before removing the old StatefulSet, stop all Foundry writers and expose the
retained claim read-only to a maintenance environment that can reach the target
coordination database. Preserve the claim itself, then run the checked migration
against both files:

```bash
export S2P_COORDINATION_DATABASE_URL="$DATABASE_URL_FROM_COORDINATION_SECRET"
migration_result="$(
  uv run python scripts/migrate_foundry_sqlite_to_postgres.py \
    --state-dir /mnt/state-stream2pretrain-foundry-0 \
    --snapshot-dir /retained/foundry-sqlite-migration
)"
printf '%s\n' "$migration_result"
manifest_sha256="$(jq -r .manifest_sha256 <<< "$migration_result")"
```

The command uses SQLite's backup API for consistent copies of both databases,
normalizes private copies to the current schema, and initializes the target
tables under the Foundry schema advisory lock. It then takes PostgreSQL table
locks with `NOWAIT`. The import proceeds only when every target Foundry table is
empty apart from the zero-valued candidate sequence, or when every target table
already matches the source. Before commit it compares the row count and a
type-aware SHA-256 content fingerprint for every control and quota table. A
mismatch rolls back the import. The retained snapshot directory contains both
SQLite backups and `migration-manifest.json`.

Review the manifest, retain it with the backups, and mark the coordination
Secret only after the command reports `migrated` or `verified-existing`:

```bash
kubectl -n stream2pretrain annotate --overwrite \
  secret/stream2pretrain-coordination \
  stream2pretrain.io/foundry-control-migrated-from=state-stream2pretrain-foundry-0 \
  stream2pretrain.io/foundry-control-migration-verified=true \
  "stream2pretrain.io/foundry-control-migration-manifest-sha256=$manifest_sha256"
```

The deployment paths fail before topology mutation while the legacy claim
exists and these three independent control-migration annotations are absent or
invalid. The recovery-claim annotations remain a separate requirement. Keep the
legacy claim, both SQLite snapshots, and the manifest until PostgreSQL queries
confirm the expected queue, quota, outbox, artifact, and audit rows and the new
worker and API pass readiness.

Measure durable bytes per accepted document, all decision-row bytes, transient
bytes per admitted input, daily arrival rate and retention. Daily growth is
durable output plus metadata/state growth. One-day transient storage is a
rolling working set, not indefinitely accumulating daily growth.

For larger deployments, separate Redpanda, Prometheus, recovery and query state
onto expandable volumes. Place bulk corpus objects in a backed-up external or
distributed store. Monitor free capacity, projected exhaustion, DiskPressure,
maintenance failures and catalog backup age. Verify restore before claiming
production resilience. Capacity figures are `needs-measurement` for a new
deployment.
