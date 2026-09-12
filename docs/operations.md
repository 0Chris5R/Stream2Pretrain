# Stream2Pretrain - Operations Runbook

Routine procedures for Stream2Pretrain on a k3s cluster. The dev-stack equivalent
(docker compose) is documented in the [local guide](../local/README.md). This file
assumes you can `kubectl` against the cluster as a user with chart-admin
privileges in the `stream2pretrain` namespace.

## 1. First deploy

```bash
# 1.1 Validate locally and review the OpenStack plan.
./scripts/setup_dhbw_demo.sh validate
OPENRC_PATH=/absolute/path/to/openrc.sh ./scripts/setup_dhbw_demo.sh plan

# 1.2 Provision the reviewed VM plan and install k3s.
OPENRC_PATH=/absolute/path/to/openrc.sh ./scripts/setup_dhbw_demo.sh cluster

# 1.3 Create the required Secrets using README.md.
# No demo credentials are created or committed by the script.

# 1.4 Apply the measured ownership tiers.
./scripts/setup_dhbw_demo.sh platform
./scripts/setup_dhbw_demo.sh storage
./scripts/setup_dhbw_demo.sh catalog
./scripts/setup_dhbw_demo.sh topics
./scripts/setup_dhbw_demo.sh application

# 1.5 Seed sources and verify.
NAMESPACE=stream2pretrain bash scripts/load_seed_feeds.sh
./scripts/setup_dhbw_demo.sh verify
kubectl -n stream2pretrain port-forward svc/stream2pretrain-ui 3000:80
```

The chart-owned RSS and OAI-PMH CronJobs are suspended templates.
The SourceFeed controller creates the only active CronJob for each CRD. Do not
unsuspend a template job: doing so would duplicate the per-source schedules.

This sequence provisions a new installation. Existing deployments apply only
the changed ownership tier. Loki, Tempo and Alloy are not enabled in the DHBW
profile.

## 2. Scale application components

The fetcher, curator, and Iceberg writer are coordinated Bytewax executions.
Replicas within one stage are peers in one distributed execution, not
independent Kafka consumers. Each stage owns source progress in a recovery
database on its checkpoint claim. A crash before a recovery snapshot replays
the keyed output. An extraction, storage, model, or commit failure escapes the
operator so Bytewax cannot checkpoint past the record. The curator stores its
global near-duplicate and decision state in the shared coordination database.
The Iceberg writer uses deterministic row identity and retries optimistic
catalog commit conflicts.

`processor.fetcher.sourceBatchSize` and `processor.curate.sourceBatchSize`
default to one record per Kafka partition. This is intentional: extraction,
OCR, and classification are expensive, so inheriting Bytewax's 1,000-record
connector default can leave hours of computed results unpublished and
uncheckpointed. Do not increase either value without measuring end-to-end
durable append latency and restart replay on the cluster.

Ordinary Kafka-lag KEDA must not independently scale a core execution:
broker commits are not the authoritative Bytewax progress boundary. A core
rescale is a coordinated stop, worker-count change, and restart using the
pre-created recovery partitions. More than one process also requires a shared
`ReadWriteMany` checkpoint with an explicit non-`local-path` StorageClass.
The application deployment checks retained claims before stopping a worker. It
rejects RWO claims, `local-path`, missing external claims, and storage-class
mismatches. It does not copy or convert checkpoint data.

The opt-in
[`stream2pretrain.horizontal-scaling.yaml`](../infra/helmfile-values/stream2pretrain.horizontal-scaling.yaml)
profile is an offline render and migration target. Before applying equivalent
values to a deployment profile, snapshot each retained checkpoint, copy it to
the reviewed RWX claim, and verify the recovery identity. The application
setup then quiesces the coordinated workloads, waits for their Pods to stop,
applies the chart, and waits for the StatefulSets. A failed pre-apply or Helm
step restores the recorded replica counts where the old controller still
exists.

ModernBERT quality and KenLM services are
stateless deployments with separate resource budgets and demand-based scaling.
The four quality heads share each quality Pod's bounded CPU budget. The
curator leases asynchronous requests to ready replicas and preserves input
order and exact model provenance.
Their deployment strategy is `Recreate`: the DHBW nodes cannot hold two
generations of the multi-GiB model images at once. The release workflow removes
an HPA and scales its service to one Pod only when that service's immutable
image digest changed. An unchanged digest preserves the running Pod, loaded
model memory, HPA state, and readiness. Foundry has a separate application
image, so post-training edits do not replace pretraining workers and core edits
do not replace Foundry.

Source scheduling uses explicit ownership. SourceFeed controller replicas
serve the same read-only status API, while one replica holds the reconciliation
Lease. RSS and OAI-PMH jobs take a Lease per feed cursor. Hugging Face replicas
share MinIO cursor state and likewise take one Lease per model or dataset
cursor. The arXiv HTML workers consume the dedicated `arxiv.discovery` topic as
a Kafka consumer group, so KEDA can use that source-specific lag without
counting the enriched records published to `raw.fetched`. Each record is
committed only after durable handling.

The DuckDB API stores its serving index in per-Pod `emptyDir` state. Every
replica rebuilds from Iceberg and then consumes retained Kafka deltas with an
independent consumer identity. Replicas never write one DuckDB file.

Foundry workers share candidate, quota, call, and outbox coordination in
PostgreSQL. Expiring lease tokens fence stale candidate owners. The worker
StatefulSet is separate from the stateless API Deployment, and multiple worker
processes use one coordinated Bytewax execution with shared RWX recovery. The
`Qwen3.8-27B` model remains an external provider API. Stream2Pretrain controls
request concurrency, not provider replicas.

The repository verifies these paths with deterministic concurrency tests and a
two-replica Helm render. The frozen cluster evidence demonstrates UI and model
service replicas only. Live application failover, sustained throughput, and
safe maximum replicas remain `needs-measurement`.

### 2.1 Stateful migration prerequisites

Do not replace a retained service or checkpoint in place. Establish one
recorded maintenance boundary, keep every source volume intact, and complete
the relevant validation before changing application endpoints or replica
counts.

1. For MinIO, create the target distributed service, copy all five buckets, and
   compare object counts, byte totals, and a cryptographic checksum manifest.
   Read representative Iceberg metadata, scientific evidence, and signed
   packages from the target.
2. For PostgreSQL, quiesce catalog and application writers, take consistent
   `pg_dump` backups of the Polaris and coordination databases, restore them
   into the target cluster, and validate both schemas. Confirm table counts,
   Iceberg resolution, curator state, Foundry queue and quota state, outbox rows,
   and artifact audits.
3. For Redpanda, use broker-aware partition replica reassignment. Wait for all
   target replicas to become in sync, confirm there are no under-replicated
   partitions, and compare partition counts and consumer-group offsets.
4. For each Bytewax or Foundry recovery claim, stop the coordinated execution,
   snapshot the source claim, copy every recovery file into the pre-created RWX
   claim, compare cryptographic checksums, and verify its flow, recovery, and
   cutover identities before starting the new replica count.

The legacy `state-stream2pretrain-foundry-0` claim also held
`control.sqlite3` and `quota.sqlite3`. Those files contain state that is not in
the Bytewax recovery directory, including artifact audits. Follow the exact
SQLite-to-PostgreSQL migration and manifest procedure in
[Storage ownership and scaling](storage-scaling.md#legacy-foundry-sqlite-cutover).
The deployment remains fail-closed until the coordination Secret carries the
separate source, verification, and manifest-digest annotations.

These are operator prerequisites, not automated destructive steps. Keep the
source services, claims, dumps, and snapshots until the restored topology passes
readiness and data validation. Cleanup requires separate explicit approval.

The production fetcher consumes only `raw.fetched`. A separate fixed Bytewax
canary execution uses its own short-retention `raw.smoke` input,
`docs.normalized.smoke` output, flow name, and recovery PVC. The release
workflow also creates an isolated one-record curator canary with an ephemeral
recovery directory and dedicated `curation.decisions.smoke` and
`docs.curated.smoke` outputs. Its admission is written only to
`license.admissions.smoke`, and its temporary Bronze object is deleted on both
success and failure. The canary tails production topics before injection and
fails if its exact `doc_id` appears there. Synthetic records therefore cannot
advance production progress or mutate production state. Any deterministic
canary-only failure is separated under the state bucket's
`canary-processing-failures/` prefix instead of the production Gold ledger.

Normalization and curation each stop unfinished work once the original content
intake timestamp is 24 hours old. The check runs before expensive processing,
after a long normalization, before classifier retry, and before durable curation
output. An extraction retry preserves the original timestamp and never receives
a new window. Missing legacy timestamps also expire closed. These skips emit
`s2p_processor_work_expired_total` by stage, source and reason. They are neither
quality rejections nor unique-document counts. Completed decisions, Gold data
and post-training artifacts do not age out. The one-day Bronze/Silver retention
remains a separate audit-asset lifetime. The Foundry independently freezes and
expires daily cohorts.

When a core or source contract changes, `scripts/reconcile_topic_partitions.sh`
also reconciles
the seven-day core and 24-hour smoke retention already declared in
`schemas/topics.py`, the document-topic partition floor, delete cleanup policy,
and the maximum Kafka record size. It inventories topics once and applies
configuration by retention class instead of opening a Kubernetes exec session
for every property of every topic. Deployment stops if a required topic has no
partitions. Documentation-only pushes do not deploy. Python and Helm checks,
source reconciliation, the core canary, image builds, and rollout waits are
selected from the changed paths and immutable digests. An unchanged unhealthy
workload cannot delay an unrelated release. Application updates use a direct
Helm sync, then wait in parallel only for Deployments and StatefulSets whose
generation changed in that release. The normal readiness budget is 60 seconds.
Only an actual multi-GiB model-image change receives the extended model-load
budget. Source-only processor images rely on the locked repository suite and
the separately validated immutable dependency bases. Their Dockerfile stages
do not import the runtime again, so BuildKit can reuse those large bases
without materializing them for ordinary source edits.

```bash
uv run python scripts/capacity_probe.py
./scripts/setup_dhbw_demo.sh validate
```

## 3. Debug a stuck stage

Symptom: a core processor's throughput counters stop advancing or its Pod
restarts repeatedly on the same input.

```bash
# 3.1 Identify the bottleneck.
kubectl -n stream2pretrain logs statefulset/stream2pretrain-processor-curate --tail=200 | rg -i 'error|warn'
kubectl -n stream2pretrain top pod | rg 'processor-curate|processor-model-service-'

# 3.2 Pull a sample from each topic.
kubectl -n redpanda exec -it redpanda-0 -c redpanda -- \
    rpk topic consume docs.normalized --num 5 --offset latest

# 3.3 Trace a single doc end to end.
DOC_ID="sha256:..."
kubectl -n stream2pretrain logs statefulset/stream2pretrain-processor-curate | rg "$DOC_ID"
# If optional tracing is enabled, use the trace_id in the configured backend.
```

If the fetcher, curator, or Iceberg writer stalls, inspect its logs,
processing-failure metric, Pod restarts, and recovery PVC. Deterministic poison
records are queryable in the Gold bucket under `processing-failures/`, keyed by
stage, Kafka topic, partition, offset, and payload hash. Each JSON object
retains document and trace ids from the payload, message metadata, or a
deterministic unresolved fallback, plus retry classification, error revision,
and reason. A failed failure-object write stops the Bytewax execution before
its next recovery snapshot. Kafka consumer-group lag is useful backlog context
but is not the recovery checkpoint. Deleting a recovery database can replay
retained input. PostgreSQL duplicate and decision state survives deletion of the
curator checkpoint, but checkpoint deletion is not a routine restart step.

For a read-only aggregate that correlates these objects with their retained
Kafka coordinates, run [`audit_processing_failures.py`](../scripts/audit_processing_failures.py)
inside the fetcher container using the same `python -` pattern as the workflow
diagnostics.

## 4. Restart from checkpoint

The fetcher, curator, and Iceberg writer resume from their Bytewax recovery
databases. The Foundry worker uses the same recovery contract for its input
flow and PostgreSQL for queue coordination. During
the one-time native-consumer-to-Bytewax cutover, `startingOffset=stored`
bridges the last broker commit only when no Bytewax recovery snapshot exists.
The deployment writes and validates the identity-bound
`cutovers/native-consumer-to-bytewax-v2/<component>.json` marker on each
retained state volume. Legacy state without either a matching marker or a
readable recovery source fails closed, and an identity mismatch is never
overwritten. After the first Bytewax snapshot, the PVC is authoritative. The
only manual case is an explicitly approved recovery or audit replay.

```bash
# 4.1 Stop the curator.
kubectl -n stream2pretrain scale statefulset stream2pretrain-processor-curate --replicas=0

# 4.2 Back up the checkpoint and coordination database at one recorded boundary.

# 4.3 Destructive checkpoint replacement, only after verification and approval.
kubectl -n stream2pretrain delete pvc \
    stream2pretrain-processor-curate-checkpoint
# Recreate the claim and workload through the reviewed application release.

# 4.4 Bring the curator back.
./scripts/setup_dhbw_demo.sh application
```

## 5. Rotate the Foundry artifact-signing key

The Foundry uses a persistent in-cluster Ed25519 key in a Kubernetes Secret to
sign generated packages. Deployment bootstrap creates the identity once when
it is absent and never overwrites an existing Secret. Operators may instead
pre-provision that Secret with their managed key before the first deployment.

```bash
# 5.1 Generate a fresh key pair.
openssl genpkey -algorithm Ed25519 -out new.key
openssl pkey -in new.key -pubout -out new.pub

# 5.2 Wrap the public key in a self-signed certificate.
openssl req -new -x509 -key new.key -out new.crt -days 365 \
    -subj "/CN=stream2pretrain-foundry/O=Stream2Pretrain"

# 5.3 Update the Secret in-place.
kubectl -n stream2pretrain create secret generic stream2pretrain-foundry-signing \
    --from-file=ed25519.key=new.key \
    --from-file=ed25519.crt=new.crt \
    --dry-run=client -o yaml | kubectl apply -f -

# 5.4 Restart the Foundry workload to pick up the new key.
kubectl -n stream2pretrain rollout restart statefulset/stream2pretrain-foundry-worker
```

Existing packages retain the certificate stored with their signature. Newly
generated packages use the new key.

## 6. Observability cheat sheet

- **Grafana**: `kubectl -n monitoring port-forward svc/grafana 3001:80`
  -> dashboards "Stream2Pretrain - Pipeline" and "Stream2Pretrain - KEDA".
- **Optional Loki**: when enabled, filter on
  `{app="curator", source_feed="rss-arxiv-cs-cl"}`.
- **Optional Tempo / Jaeger**: when enabled, search by `trace_id` recovered
  from a Gold row or application log. These services are disabled in the DHBW
  profile.
- **Redpanda Console**: `kubectl port-forward svc/redpanda-console 8080`
  -> topic browser, consumer-group lag.

## 7. Backups

- Mirror `s2p-bronze`, `s2p-silver`, `s2p-gold`, `s2p-posttrain`, and
  `s2p-state` to a
  second failure domain on the reviewed schedule.
- Iceberg data and metadata live in `s2p-gold`. Snapshot expiry and orphan
  removal must run through the guarded Iceberg maintenance command, not a
  bucket-wide age deletion.
- Writers retain twenty previous metadata versions. The scheduled per-table
  Iceberg maintenance CronJobs are the sole physical cleanup owners: they
  retain 24 hours and at least ten snapshots, walk the complete retained
  snapshot graph, and remove
  only aged, unreachable metadata, manifests, data and statistics files. Inspect
  its most recent log before any manual maintenance run.
- Back up Redpanda if the configured replay horizon is operationally required,
  plus every Bytewax and Foundry recovery claim for in-flight recovery.
- Back up the CloudNativePG cluster and test catalog and coordination restoration
  alongside the MinIO backup. PostgreSQL retains table pointers, access
  metadata, curator coordination, and Foundry leases and audits. Iceberg data
  and metadata remain in MinIO.
- See [`storage-scaling.md`](./storage-scaling.md) for the complete ownership
  and lifecycle contract.

### Dashboard serving index

Normal monitoring routes never query complete Iceberg history. The DuckDB API
keeps a current-state read model from `curation.decisions` and
`license.admissions`, acknowledging Kafka records only after the local upsert.
Headline totals, Documents, Sources, and Dataset selection query this compact
index. Documents use cursor pagination and fetch further rows only on demand.
The `as-of` route applies source-validity intervals to the same current-state index,
which includes all scoring generations. It does not reconstruct processing-time
Iceberg snapshots. Explicit read-only SQL uses a separate executor against
Iceberg, so an expensive lakehouse query cannot block monitoring pages.

The serving index is derived per-Pod `emptyDir` state. A new Pod rebuilds a
complete baseline from Iceberg, creates a fresh instance identity, and replays
the still-retained Kafka deltas. No serving-index PVC is shared or restored.

## 8. Quotas and DHBWCloud caveats

- DHBWCloud OpenStack quota: vCPU / RAM / disk per VM is `needs-measurement`
  - confirm with the team before increasing replica caps.
- The current manifests request three Redpanda brokers, four MinIO members, and
  three PostgreSQL instances. The frozen screenshots predate that topology.
  Capacity, migration, and node-loss recovery remain `needs-measurement`.
- Wildcard TLS DNS zone (rfc2136 + tsig credentials) is required for
  cert-manager. Per the lecture, the team's zone is `needs-measurement`.

## 9. Decommission

```bash
# 9.1 Stop new ingestion.
kubectl -n stream2pretrain scale deploy --all --replicas=0

# 9.2 Tear the chart down.
helmfile -f helmfile.yaml destroy

# 9.3 Reclaim PVCs. Review first because this is destructive.
kubectl -n stream2pretrain delete pvc --all

# 9.4 Drop the namespace.
kubectl delete namespace stream2pretrain
```

Bucket contents survive the namespace delete. The operator must remove them
from MinIO separately if a clean slate is required.
