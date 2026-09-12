# Capacity measurement

Use the deployed cluster, not laptop estimates. A healthy Pod does not prove
that the pipeline keeps up.

## Read-only snapshot

```bash
gh workflow run deploy-main.yml --ref main -f mode=check-pipeline
```

Use `mode=capture-evidence` for matched measurement snapshots. The workflow
uploads counters, broker frontiers, object-store sizes and resource state as
a seven-day Actions artifact without changing offsets or queues. Compare two
captures from the same deployed configuration and report their actual interval.

The compact check records core readiness, serving totals, worker counters,
classifier decisions, queued evidence and recent Foundry events. It creates no
provider calls and does not mutate queues.

For resource sizing, `scripts/capacity_probe.py` collects node, Pod, PVC,
Redpanda and storage observations using an explicitly configured cluster
context. `scripts/benchmark_model_service.py` measures complete model requests
and therefore consumes inference compute. Run it only as an intentional test.

## Measurement protocol

1. Record start/end times, image digests, model manifest, replicas, CPU/RAM limits
   and policy generation.
2. Measure a representative fresh-input interval after rollouts. Include an
   arXiv announcement burst and distinguish weekday from weekend arrivals.
3. Count unique discovered content, licence-admitted content, normalized output,
   decided records and durable training exports separately for each source.
4. Separate replay from new intake. A worker counter increments per processing
   event. Latest-per-document corpus totals need not increase after replay.
5. Record queue age and backlog change alongside stage throughput. Increasing
   backlog proves that the measured configuration is not keeping up.
6. Measure classifier seconds, tokens and windows by head. Include all four
   models under the two-stage policy, not quality-only throughput.
7. Measure object bytes by bucket/prefix and current Iceberg references.
   Distinguish the one-day transient working set from durable daily growth.
8. Record peak memory, OOMs, CPU throttling, pending Pods and disk headroom.
   Request more capacity when measured demand exceeds resources.
9. For Foundry, report completed papers, accepted/rejected SFT trajectories and
   RL environments, calls, tokens and provider-capacity stops. Separate content
   rejection from parsing, transport and execution failures.
10. For every multi-replica application path, record per-replica work, stop one
    replica, and verify takeover without duplicate durable identities or lost
    offsets. Stateful tests must include Bytewax recovery, curator coordination,
    Iceberg commit conflict, and Foundry lease expiry.
11. For Redpanda, MinIO, and PostgreSQL, record replica placement, quorum health,
    recovery time after one member is stopped, and usable storage headroom.

Never remove quality checks, skip sections or substitute classifiers to make a
capacity benchmark pass. Sustained rate, daily storage growth and accepted
artifact yield remain `needs-measurement` until this protocol has a recorded
representative interval.

## Scaling boundary

The frozen cluster evidence demonstrates ordinary UI scaling and two Ready
quality-service replicas. It does not demonstrate the later full application
or stateful infrastructure topology.

The opt-in horizontal profile renders two replicas for every application
component. Deterministic tests cover Kubernetes cursor ownership, shared
curator duplicate and decision state, optimistic Iceberg conflicts, independent
DuckDB serving indexes, and Foundry candidate and quota fencing. These checks
establish the intended coordination contracts. They do not establish live
throughput, recovery time, or safe replica ceilings.

Stateless classifier replicas scale with demand within declared limits. Source
controllers and pollers use Kubernetes Leases, while the arXiv full-text worker
uses source-topic partition ownership. Bytewax fetcher, curator, Iceberg writer,
and Foundry worker replicas form coordinated executions and require reviewed
restarts with verified RWX recovery. DuckDB replicas rebuild private serving
indexes. Foundry API replicas are stateless and workers coordinate through
PostgreSQL leases.

The current manifests also request three Redpanda brokers, four MinIO members,
and three CloudNativePG instances. Their migration, failover, throughput, and
storage behavior remain `needs-measurement` until the protocol above is run on
the target cluster.
