# Storage ownership and scaling

The object store and compute workers scale independently. Object APIs allow an
external S3-compatible store or distributed MinIO deployment without changing
the training-data format.

## Retention

| Data | Owner | Lifecycle |
|---|---|---|
| Admitted source bytes | `s2p-bronze` | One-day audit retention |
| Transient extracted assets and scientific JSON | `s2p-silver` | One-day audit retention |
| Accepted paper evidence capsule | `s2p-gold/scientific-evidence/` | Durable, persisted before candidate publication |
| Licence decisions and curation outcomes | Iceberg tables in `s2p-gold` | Durable latest state and policy provenance |
| Training text | Iceberg Parquet in `s2p-gold` | Durable, never subject to raw-object expiry |
| Generated SFT/RL packages and audits | `s2p-posttrain` plus Foundry state | Durable artifacts and named audit history |
| In-flight events | Redpanda | Topic-specific bounded retention |
| Recovery, dedup and serving state | Component PVCs | Persistent operational state and backups |

Explicitly incompatible items stop before content fetch. Structured evidence
travels in normalized events, is materialized durably before candidate
publication, and is cached with the Foundry queue entry. Transient raw or figure
expiry does not erase training text or admitted paper evidence.

The application lifecycle hook configures only Bronze and Silver expiry and
preserves unrelated operator rules. Object expiry is asynchronous. It does not
apply age deletion to Gold, post-training or state buckets.

## Write and maintenance behavior

Iceberg commits use count- or time-bounded batches. Deterministic duplicate
admissions and decisions do not create repeated logical rows. Snapshot
properties belong to snapshots, not an ever-growing table-property log.

The maintenance job expires old snapshots and removes unreferenced metadata
only outside its configured safety windows. Current metadata, reachable
manifests and live data files remain protected. Use dry-run inspection before
any deletion. Maintenance is not an age-based deletion policy for corpus rows.

DuckDB performs a one-time authoritative bootstrap and then consumes
transactional deltas. Its persistent serving index and aggregate cache can be
rebuilt from the lakehouse; they are not the sole copy of the corpus.

## Capacity

The DHBW k3s `local-path` provisioner stores data on node filesystems. A larger
PVC request does not supply new physical disk or impose a storage quota.
Expansion requires an expandable CSI volume or additional backing storage.

Measure durable bytes per accepted document, all decision-row bytes, transient
bytes per admitted input, daily arrival rate and retention. Daily growth is
durable output plus metadata/state growth; one-day transient storage is a
rolling working set, not indefinitely accumulating daily growth.

For larger deployments, separate Redpanda, Prometheus, recovery and query state
onto expandable volumes. Place bulk corpus objects in a backed-up external or
distributed store. Monitor free capacity, projected exhaustion, DiskPressure,
maintenance failures and catalog backup age. Verify restore before claiming
production resilience. Capacity figures are `needs-measurement` for a new
deployment.
