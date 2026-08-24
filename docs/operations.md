# Stream2Pretrain - Operations Runbook

Routine procedures for the curator on a k3s cluster. The dev-stack equivalent
(docker compose) is documented in the README's Quickstart section. This file
assumes you can `kubectl` against the cluster as a user with chart-admin
privileges in the `stream2pretrain` namespace.

## 1. First deploy

```bash
# 1.1 Validate locally and review the OpenStack plan.
./scripts/setup_dhbw_demo.sh validate
OPENRC_PATH=/absolute/path/to/openrc.sh ./scripts/setup_dhbw_demo.sh plan

# 1.2 Provision the reviewed VM plan and install k3s.
OPENRC_PATH=/absolute/path/to/openrc.sh ./scripts/setup_dhbw_demo.sh cluster

# 1.3 Provision MinIO, required buckets, Secrets, and benchmark ConfigMap.
# See infra/README.md. No demo credentials are created by the script.

# 1.4 Apply the measured ownership tiers.
./scripts/setup_dhbw_demo.sh platform
./scripts/setup_dhbw_demo.sh catalog
./scripts/setup_dhbw_demo.sh topics
./scripts/setup_dhbw_demo.sh application

# 1.5 Seed sources and verify.
NAMESPACE=stream2pretrain bash scripts/load_seed_feeds.sh
./scripts/setup_dhbw_demo.sh verify
kubectl -n stream2pretrain port-forward svc/stream2pretrain-ui 3000:3000
```

The chart-owned RSS, OAI-PMH, and sitemap CronJobs are suspended templates.
The SourceFeed controller creates the only active CronJob for each CRD. Do not
unsuspend a template job: doing so would duplicate the per-source schedules.

This sequence is for a clean install. Do not apply the application tier to the
current legacy release until the immutable-selector and curator StatefulSet
migration in `docs/infrastructure-reimplementation.md` is approved. Loki,
Tempo, and Alloy are not in the measured baseline.

## 2. Scale the processor

The fetcher and curator are coordinated Bytewax executions. Each owns source
progress in a recovery database on its checkpoint PVC. A crash before a
recovery snapshot replays the keyed output; an extraction, storage, or model
failure escapes the operator so Bytewax cannot checkpoint past the record.
The curator recovery boundary also covers its near-duplicate index and
deterministic decision cache.

Ordinary Kafka-lag KEDA must not independently scale either core execution:
broker commits are not the authoritative Bytewax progress boundary. A core
rescale is a coordinated stop, worker-count change, and restart using the
pre-created recovery partitions. Quality, KenLM, and E5 remain stateless
`processor-model-service-*` deployments with CPU HPA and cross-node spreading.
Their deployment strategy is `Recreate`: the DHBW nodes cannot hold two
generations of the multi-GiB model images at once. The release workflow removes
an HPA and scales its service to one Pod only when that service's immutable
image digest changed. An unchanged digest preserves the running Pod, loaded
model memory, HPA state, and readiness. Foundry has a separate application
image, so post-training edits do not replace pretraining workers and core edits
do not replace Foundry.

The single Iceberg writer retains one Bytewax recovery partition. That state
shard count is independent of the four Kafka topic partitions and deliberately
matches the existing cloud checkpoint.

KEDA remains appropriate for independently committing ingest consumers with a
dedicated input topic, such as the GitHub tarball fetcher. It is disabled for
the arXiv HTML fetcher because that worker consumes and republishes on the
shared `raw.fetched` topic, making Kafka lag a self-amplifying signal rather
than an arXiv backlog. Keep that worker at one replica until a source-specific
Prometheus backlog metric exists. The live arXiv worker processes and commits
one discovered paper at a time so a partially filled batch cannot remain idle
between announcement windows.

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

When a core or source contract changes, `scripts/reconcile_topic_partitions.sh`
also reconciles
the seven-day core and 24-hour smoke retention already declared in
`schemas/topics.py`, the document-topic partition floor, delete cleanup policy,
and the maximum Kafka record size. It inventories topics once and applies
configuration by retention class instead of opening a Kubernetes exec session
for every property of every topic. Deployment stops if a required topic has no
partitions. Documentation-only pushes do not deploy. Python and Helm checks,
source reconciliation, the core canary, image builds, and rollout waits are
selected from the changed paths and immutable digests; an unchanged unhealthy
workload cannot delay an unrelated release. Application updates use a direct
Helm sync, then wait in parallel only for Deployments and StatefulSets whose
generation changed in that release. The normal readiness budget is 60 seconds;
only an actual multi-GiB model-image change receives the extended model-load
budget.

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
# Then jump to Tempo using the trace_id field on the log line.
```

If the fetcher, curator, or Iceberg writer stalls, inspect its logs,
processing-failure metric, Pod restarts, and recovery PVC. Deterministic poison
records are queryable in the Gold bucket under `processing-failures/`, keyed by
stage, Kafka topic, partition, offset, and payload hash. Each JSON object
retains document and trace ids from the payload, message metadata, or a
deterministic unresolved fallback, plus retry classification, error revision,
and reason. A failed failure-object write stops the Bytewax execution before
its next recovery snapshot. Kafka consumer-group lag is useful backlog context
but is not the recovery checkpoint. Deleting either recovery database can
replay retained input; deleting the curator PVC also destroys the near-duplicate
and decision-cache boundary and is not a routine restart step.

## 4. Restart from checkpoint

The fetcher and curator resume from their Bytewax recovery databases. During
the one-time native-consumer-to-Bytewax cutover, `startingOffset=stored`
bridges the last broker commit only when no Bytewax recovery snapshot exists.
The deployment writes and validates the identity-bound
`cutovers/native-consumer-to-bytewax-v2/<component>.json` marker on each
retained state volume. Legacy state without either a matching marker or a
readable recovery source fails closed, and an identity mismatch is never
overwritten. After the first Bytewax snapshot, the PVC is authoritative. The
only manual case is a **deliberate replay** such as a contamination bisect.

```bash
# 4.1 Stop the curator.
kubectl -n stream2pretrain scale statefulset stream2pretrain-processor-curate --replicas=0

# 4.2 Back up the checkpoint PVC and record the intended replay boundary.
# Bytewax source progress and near-duplicate state form one boundary.

# 4.3 Destructive cold start, only after a snapshot and explicit approval.
kubectl -n stream2pretrain delete pvc \
    checkpoint-stream2pretrain-processor-curate-0
# (The StatefulSet recreates the PVC; the configured start offset is used.)

# 4.4 Bring the curator back.
kubectl -n stream2pretrain scale statefulset stream2pretrain-processor-curate --replicas=1
```

## 5. Rotate the Decon-Gate signing key

The prototype uses a single in-cluster Ed25519 key in a Kubernetes Secret.
A real deployment should use Sigstore Rekor; this runbook covers the
prototype path.

```bash
# 5.1 Generate a fresh key pair.
openssl genpkey -algorithm Ed25519 -out new.key
openssl pkey -in new.key -pubout -out new.pub

# 5.2 Wrap the public key in a self-signed cert (used as `signer_cert`).
openssl req -new -x509 -key new.key -out new.crt -days 365 \
    -subj "/CN=stream2pretrain-decon-gate/O=Stream2Pretrain"

# 5.3 Update the Secret in-place.
kubectl -n stream2pretrain create secret generic stream2pretrain-decon-signing \
    --from-file=ed25519.key=new.key \
    --from-file=ed25519.crt=new.crt \
    --dry-run=client -o yaml | kubectl apply -f -

# 5.4 Restart signing workloads to pick up the new key.
kubectl -n stream2pretrain rollout restart statefulset/stream2pretrain-processor-curate
kubectl -n stream2pretrain rollout restart deploy/stream2pretrain-processor-iceberg-writer
```

After rotation, old attestations remain verifiable using their embedded
`signer_cert` field; only new attestations carry the new cert.

## 6. Promote a shadow MixtureRecipe

The shadow A/B feature runs a candidate `MixtureRecipe` alongside the
production one. When the candidate's perplexity-delta gate passes, the
mixture controller flips the `branch` of the production recipe to the
candidate's Iceberg branch.

```bash
# 6.1 Inspect both recipes.
kubectl -n stream2pretrain get mixturerecipes -o wide

# 6.2 Trigger a manual promotion (bypassing the perplexity gate).
kubectl -n stream2pretrain patch mixturerecipe candidate \
    --type=merge -p '{"metadata":{"annotations":{"stream2pretrain.io/promote":"true"}}}'

# 6.3 Roll back if downstream metrics regress.
kubectl -n stream2pretrain patch mixturerecipe production \
    --type=merge -p '{"spec":{"branch":"main"}}'
```

## 7. Observability cheat sheet

- **Grafana**: `kubectl -n monitoring port-forward svc/grafana 3001:80`
  -> dashboards "Stream2Pretrain - Pipeline" and "Stream2Pretrain - KEDA".
- **Loki**: filter on `{app="curator", source_feed="rss-arxiv-cs-cl"}`.
- **Tempo / Jaeger**: search by `trace_id` (32 hex chars) recovered from a
  gold row or a Loki line.
- **Redpanda Console**: `kubectl port-forward svc/redpanda-console 8080`
  -> topic browser, consumer-group lag.

## 8. Backups

- Mirror `s2p-bronze`, `s2p-silver`, `s2p-gold`, `s2p-decon`, and
  `s2p-posttrain` to a second failure domain on the reviewed schedule.
- Iceberg data and metadata live in `s2p-gold`; snapshot expiry and orphan
  removal must run through the guarded Iceberg maintenance command, not a
  bucket-wide age deletion.
- Back up Redpanda if the configured replay horizon is operationally required,
  plus the curator and foundry state PVCs for in-flight recovery.
- Production Polaris requires its relational database backup and a tested
  catalog restore. The DHBW dev profile is in-memory and is not a recoverable
  production catalog.
- See [`storage-scaling.md`](./storage-scaling.md) for the complete ownership
  and lifecycle contract.

## 9. Quotas and DHBWCloud caveats

- DHBWCloud OpenStack quota: vCPU / RAM / disk per VM is `needs-measurement`
  - confirm with the team before increasing replica caps.
- 2-worker layout means Redpanda runs single-broker. Document the
  limitation explicitly in any release notes.
- Wildcard TLS DNS zone (rfc2136 + tsig credentials) is required for
  cert-manager. Per the lecture, the team's zone is `needs-measurement`.

## 10. Decommission

```bash
# 10.1 Stop new ingestion.
kubectl -n stream2pretrain scale deploy --all --replicas=0

# 10.2 Tear the chart down.
helmfile -f helmfile.yaml destroy

# 10.3 Reclaim PVCs (review first; this is destructive).
kubectl -n stream2pretrain delete pvc --all

# 10.4 Drop the namespace.
kubectl delete namespace stream2pretrain
```

Bucket contents survive the namespace delete; the operator must remove them
from MinIO separately if a clean slate is required.
