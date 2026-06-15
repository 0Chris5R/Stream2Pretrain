# Stream2Pretrain - Operations Runbook

Routine procedures for the curator on a k3s cluster. The dev-stack equivalent
(docker compose) is documented in the README's Quickstart section. This file
assumes you can `kubectl` against the cluster as a user with chart-admin
privileges in the `stream2pretrain` namespace.

## 1. First deploy

```bash
# 1.1 Provision the cluster (one-time).
bash infra/k3s-install.sh

# 1.2 Apply the helmfile (chart + dependencies).
helmfile -f helmfile.yaml apply

# 1.3 Seed the Phase-1 SourceFeed CRDs.
NAMESPACE=stream2pretrain bash scripts/load_seed_feeds.sh

# 1.4 Bootstrap Redpanda topics from the dev profile.
kubectl -n stream2pretrain exec deploy/redpanda -- bash /scripts/seed_topics.sh

# 1.5 Smoke check.
kubectl -n stream2pretrain get pods
kubectl -n stream2pretrain port-forward svc/stream2pretrain-ui 3000:3000
```

Expected pods (18-22 depending on optional extras): redpanda + console,
minio + bootstrap, polaris, duckdb, ui, curator (1 replica that KEDA
scales), 5-7 ingest pollers, decon-gate sidecar, plus the
kube-prometheus-stack + Loki + Alloy + Tempo + Traefik + cert-manager +
Gatekeeper system pods.

## 2. Scale the curator

The curator is keyed off the `docs.normalized` topic lag. It scales
horizontally up to the cap in
`charts/stream2pretrain/templates/scaledobjects.yaml`.

```bash
# Inspect the current scale + lag.
kubectl -n stream2pretrain get scaledobject curator -o yaml | yq .status

# Force a temporary scale (overrides KEDA for one minute).
kubectl -n stream2pretrain scale deploy curator --replicas=4

# Raise the cap permanently:
helm upgrade stream2pretrain charts/stream2pretrain \
    --reuse-values \
    --set curator.maxReplicas=12
```

The other consumers (Iceberg writer, Decon-Gate) follow the same pattern;
their `ScaledObject` names are `iceberg-writer` and `decon-gate`.

## 3. Debug a stuck stage

Symptom: KEDA replica count climbs to the cap; lag keeps growing.

```bash
# 3.1 Identify the bottleneck.
kubectl -n stream2pretrain logs deploy/curator --tail=200 | rg -i 'error|warn'
kubectl -n stream2pretrain top pod | grep curator

# 3.2 Pull a sample from each topic.
kubectl -n stream2pretrain exec -it deploy/redpanda -- \
    rpk topic consume docs.normalized --num 5 --offset latest

# 3.3 Trace a single doc end to end.
DOC_ID="sha256:..."
kubectl -n stream2pretrain logs deploy/curator | grep "$DOC_ID"
# Then jump to Tempo using the trace_id field on the log line.
```

If a Bytewax operator panics: check `kubectl -n stream2pretrain describe pod
curator-<n>` for the exit code, then `processor/curate.py` for the
named operator that owned that key. Recovery is automatic from the RocksDB
checkpoint; if checkpoints are corrupt, drop the PVC and let Bytewax replay
from the last committed Redpanda offset.

## 4. Restart from checkpoint

The curator persists Bytewax operator state on a PVC and Kafka offsets in
the consumer group `s2p-curator`. Recovery is automatic; the only manual
case is **deliberate replay** (e.g. for a contamination bisect).

```bash
# 4.1 Stop the curator.
kubectl -n stream2pretrain scale deploy curator --replicas=0

# 4.2 Reset the consumer-group offsets to a known epoch.
kubectl -n stream2pretrain exec -it deploy/redpanda -- \
    rpk group seek s2p-curator --to-timestamp 2026-06-15T00:00:00Z \
        --topics raw.fetched,docs.normalized

# 4.3 Wipe the operator-state PVC if you want a cold start.
kubectl -n stream2pretrain delete pvc bytewax-state-curator
# (the StatefulSet will recreate the PVC.)

# 4.4 Bring the curator back.
kubectl -n stream2pretrain scale deploy curator --replicas=1
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
kubectl -n stream2pretrain create secret generic decon-gate-signing \
    --from-file=tls.key=new.key \
    --from-file=tls.crt=new.crt \
    --dry-run=client -o yaml | kubectl apply -f -

# 5.4 Restart the Decon-Gate sidecar to pick up the new key.
kubectl -n stream2pretrain rollout restart deploy/decon-gate

# 5.5 Re-attest the latest snapshot so verifiers can adopt the new key.
kubectl -n stream2pretrain exec -it deploy/decon-gate -- \
    python -m processor.decon_gate reattest --latest
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

- MinIO buckets `bronze`, `silver`, `gold`, `decon-attestations` are the
  only authoritative state. Snapshot them with `mc mirror` to a remote S3
  on a daily schedule.
- Iceberg metadata lives inside the buckets (`<warehouse>/_metadata/`).
- Polaris's Postgres holds a denormalised view; rebuilding from the buckets
  is supported but takes minutes.

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
