# Local pilot report

The Podman profile exercises the same source-specific normalization, CPU model
services, Bytewax curation, MinIO objects, Iceberg tables, DuckDB serving, and
Next.js monitoring UI as the cluster-oriented application configuration. It is
intended for a bounded functional pilot, not a throughput claim.

## Covered locally

- arXiv discovery and full-text extraction, including native HTML and bounded
  CPU PDF fallback;
- exact-revision Hugging Face model-card and dataset-card README projection;
- item-level licence routing;
- scientific and card-specific quality handling;
- language, PII, exact duplicate, MinHash/LSH near-duplicate, and KenLM audit
  signals;
- durable route decisions and trainable Gold rows;
- DuckDB-backed Dashboard, Documents, Sources, Datasets, Post-training, and
  Mixture pages;
- daily/manual Foundry triggering when a provider key is configured;
- named human review of generated SFT and RL artifacts.

## Local operating contract

Use `podman compose -f compose.local.yml up -d --build`. The profile has bounded
container memory and CPU settings and uses persistent named volumes. Re-running
the profile must not create duplicate effective Gold identities. A failure in
one deterministic document is recorded and the stream continues; transport,
model-service, and storage failures remain retryable.

## Interpretation

Local success establishes integration correctness only. Sustained items/hour,
peak RSS, raw-object retention volume, and daily Iceberg growth remain
`needs-measurement` on the Kubernetes cluster. The cloud deployment is also the
authoritative test for pod recovery, persistent-volume attachment, KEDA-managed
ingest scaling, and public ingress.
