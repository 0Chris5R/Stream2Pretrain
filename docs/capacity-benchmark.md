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
and therefore consumes inference compute; run it only as an intentional test.

## Measurement protocol

1. Record start/end times, image digests, model manifest, replicas, CPU/RAM limits
   and policy generation.
2. Measure a representative fresh-input interval after rollouts. Include an
   arXiv announcement burst and distinguish weekday from weekend arrivals.
3. Count unique discovered content, licence-admitted content, normalized output,
   decided records and durable training exports separately for each source.
4. Separate replay from new intake. A worker counter increments per processing
   event; latest-per-document corpus totals need not increase after replay.
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

Never remove quality checks, skip sections or substitute classifiers to make a
capacity benchmark pass. Sustained rate, daily storage growth and accepted
artifact yield remain `needs-measurement` until this protocol has a recorded
representative interval.

## Scaling boundary

Stateless classifier replicas scale with demand within declared limits.
Bytewax fetcher and curator each own coordinated recovery state; independent
replicas must not fork that state. Rescale through a reviewed coordinated
restart. Iceberg commits and the Foundry queue currently have single writers.
