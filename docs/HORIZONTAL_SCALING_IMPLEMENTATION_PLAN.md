# Horizontal Scaling Implementation Plan

The examination requires every application component to be designed for
horizontal scaling and requires that scaling to be demonstrated. Increasing a
replica field alone is not sufficient. Each change below must preserve
deterministic processing, durable state and replay safety.

## Invariants

- Kafka work is partitioned and one active worker owns each assigned partition.
- A completed record is acknowledged only after its durable output is visible.
- Exact and near-duplicate decisions remain global across curator replicas.
- Iceberg retries do not lose decisions or silently create divergent current
  corpus state.
- Every serving replica has an independent rebuildable read model.
- Foundry candidates are claimed atomically and a failed lease can be recovered.
- Stateful infrastructure uses replicated deployments rather than several Pods
  writing one `ReadWriteOnce` volume.

## Workstreams

1. Ingestion
   - Separate arXiv discovery work from the enriched Bronze topic.
   - Use source-specific Kafka lag for worker scaling.
   - Add atomic cursor ownership for scheduled pollers.
2. Fetcher and curator
   - Run Bytewax with explicit distributed process identity and peer addresses.
   - Use stable StatefulSet identities and a fixed recovery partition set on
     shared RWX storage.
   - Move global curator cache and duplicate coordination to shared transactional
     state before allowing more than one curator process.
3. Iceberg writer
   - Partition write ownership and retry optimistic commit conflicts.
   - Replace process-local idempotency assumptions with durable row identities.
4. Serving
   - Give every DuckDB API replica an independent index rebuilt from Iceberg and
     retained Kafka topics.
5. Foundry
   - Move queue coordination to a shared SQL backend.
   - Claim candidates with expiring leases and separate API from workers.
6. Platform state
   - Run Redpanda with replicated brokers and replicated topics.
   - Use distributed MinIO storage.
   - Use an operator-managed replicated PostgreSQL service for Polaris and
     application coordination.

## Verification sequence

1. Run deterministic unit tests for each new coordination primitive.
2. Render Helm templates with a two-replica application profile.
3. Run a sub-minute synthetic pipeline test without model or paid-provider calls.
4. Verify work distribution, fail one replica and verify recovery.
5. Compare durable document and artifact identities before and after replay.
6. Only then collect Kubernetes and UI evidence for the README.

Cluster throughput, storage capacity and safe maximum replica counts remain
`needs-measurement` until the deployment evidence workflow measures them.
