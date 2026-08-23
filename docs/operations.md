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

This sequence is for a clean install. Do not apply the application tier to the
current legacy release until the immutable-selector and curator StatefulSet
migration in `docs/infrastructure-reimplementation.md` is approved. Loki,
Tempo, and Alloy are not in the measured baseline.

## 2. Scale the processor

The fetcher is stateless across records and commits Redpanda consumer-group
offsets only after the corresponding `docs.normalized` batch is confirmed
delivered. The DHBW profile keeps one replica warm and uses KEDA lag scaling up
to the four `raw.fetched` partitions. A 24-hour `raw.smoke` topic shares the
same consumer group so deployment canaries exercise the real worker without
waiting behind a production replay backlog.

Curator and Iceberg writer still use Bytewax recovery databases and do not
commit broker offsets. Redpanda therefore reports those groups as `Dead` with
zero lag; independent KEDA replicas are unsafe because a rebalance can move a
partition away from the PVC that owns its latest state. Keep those stages as
one durable writer until their state is externalized and coordinated.

KEDA remains appropriate for independently committing ingest consumers such
as the arXiv HTML and GitHub tarball fetchers. Run the target-cluster procedure
in `docs/capacity-benchmark.md` and record its evidence before enabling those
scalers.

The production fetcher consumes only `raw.fetched` and scales on that group's
lag. A fixed canary worker consumes the short-retention `raw.smoke` traffic
class with the same image and normalization code. This keeps a multi-minute
production PDF from starving the bounded deployment check; the canary is not
production capacity and is never included in the KEDA replica count.

```bash
uv run python scripts/capacity_probe.py
./scripts/setup_dhbw_demo.sh validate
```

## 3. Debug a stuck stage

Symptom: fetcher KEDA replicas climb to the cap without reducing broker lag,
or a stateful processor's throughput counters stop advancing.

```bash
# 3.1 Identify the bottleneck.
kubectl -n stream2pretrain logs statefulset/stream2pretrain-processor-curate --tail=200 | rg -i 'error|warn'
kubectl -n stream2pretrain top pod | rg processor-curate

# 3.2 Pull a sample from each topic.
kubectl -n redpanda exec -it redpanda-0 -c redpanda -- \
    rpk topic consume docs.normalized --num 5 --offset latest

# 3.3 Trace a single doc end to end.
DOC_ID="sha256:..."
kubectl -n stream2pretrain logs statefulset/stream2pretrain-processor-curate | rg "$DOC_ID"
# Then jump to Tempo using the trace_id field on the log line.
```

If the fetcher stalls, inspect `rpk group describe s2p-fetcher`, its delivery
failure metric, and Pod restarts. A restart resumes from the broker commit. If
a Bytewax operator panics: check `kubectl -n stream2pretrain describe pod`
for the exit code, then the named operator in the processor module. Recovery
is automatic from the stage's SQLite recovery checkpoint. Bytewax deliberately
has no last committed Redpanda offset to fall back to, so deleting a checkpoint
is a destructive replay/cutover decision, not a routine restart step.

## 4. Restart from checkpoint

The fetcher resumes from broker-owned `s2p-fetcher` offsets; the deployment
workflow migrates a legacy fetcher PVC once and then removes it. Curator and
Iceberg writer persist Bytewax state and source offsets on PVCs. Their recovery
is automatic. The only manual case is a **deliberate replay** (for example a
contamination bisect).

```bash
# 4.1 Stop the curator.
kubectl -n stream2pretrain scale statefulset stream2pretrain-processor-curate --replicas=0

# 4.2 Back up the checkpoint PVC and record the intended replay offset.
# Bytewax does not use `rpk group seek`; set S2P_KAFKA_START_OFFSET for the
# cold-start release after the checkpoint is removed.

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
