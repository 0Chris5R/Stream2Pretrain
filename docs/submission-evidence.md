# Submission evidence

## Scope

The frozen 8 September 2026 evidence covers the live source adapters, Bytewax
processing, Iceberg persistence, DuckDB serving, monitoring UI, stateless
classifier services and experimental post-training Foundry. The screenshots in
the repository represent the latest verified live release and show their capture
time. The submission does not depend on external workflow pages to establish
that these components ran.

The repository has since added coordinated horizontal-scaling contracts and
distributed infrastructure manifests. Those later changes are supported by
offline Helm renders and deterministic concurrency tests. They are not shown
in the frozen screenshots and are not presented as live multi-replica evidence.

MinIO is a first-class release in the submitted Helmfile graph. The current
chart creates four distributed members with one retained PVC each, client and
peer Services, a ServiceMonitor, and five buckets before Polaris and the
application. On 8 September 2026 the live cluster completed the repository's
earlier guarded migration from a Deployment to a one-Pod Helm-managed
StatefulSet. That migration retained and adopted the existing `minio-data` PVC
and Service, started `minio-0`, compared all five buckets before and after the
handoff, and deleted the obsolete Deployment only after the comparison
succeeded. The pinned server and client images were also exercised
independently in Podman with a stop-and-replace test against the same volume.
This historical migration is not evidence for the current four-member layout.

## Deterministic verification

- The frozen local verification run completed with 641 passed tests and two
  container-dependent integration tests skipped when no local stack was running.
- Python lint and formatting checks pass.
- The application and MinIO Helm charts render and lint successfully.
- The opt-in horizontal profile renders two replicas for every application
  component. It checks stable Bytewax and Foundry peer identities, RWX recovery
  claims, independent DuckDB indexes, source leases, and stateless APIs.
- Deterministic concurrency tests cover cursor exclusion and takeover, shared
  curator duplicate and decision state, Iceberg optimistic commit conflicts,
  independent serving indexes, and Foundry candidate and quota fencing.
- The pinned MinIO server starts under Podman and the pinned client creates and
  lists `s2p-bronze`, `s2p-silver`, `s2p-gold`, `s2p-posttrain`, and `s2p-state`.
- The repository security scan passes.
- README and operating-guide relative links resolve.
- The tracked-files-only submission ZIP builds reproducibly and contains the
  required README, architecture diagram, manifests, and evidence images.
- Classifier evaluation statistics are stored in
  [`validation/classifier-evaluation.json`](../validation/classifier-evaluation.json),
  with training code in [`notebooks/`](../notebooks). Source corpora, optimizer
  checkpoints and credentials are intentionally excluded.

## Frozen live snapshot

At 18:00 UTC on 8 September 2026, the live system recorded:

| Source | Training export | Durable decisions |
|---|---:|---:|
| arXiv HTML | 5,385 | 6,642 |
| HF datasets | 1,840 | 16,163 |
| HF models | 3,112 | 16,938 |
| **Total** | **10,337** | **39,743** |

All captured application containers were Ready. The production fetcher had a
cumulative restart count of two. The remaining captured application containers
had zero. The five MinIO buckets occupied about 7.01 GiB at this snapshot.
These values are a point-in-time operational record, not a throughput or
capacity claim.

## Pipeline and scaling evidence

- A controlled record completed Bronze persistence, normalization, active
  quality scoring and curated publication without entering production topics.
- Polaris exposed the physical `gold.license_admissions`,
  `gold.curation_decisions`, and `gold.curated` tables.
- DuckDB returned the isolated smoke document and corpus overview with HTTP 200.
- Browser validation returned HTTP 200 for every submitted page and typed API
  probe.
- The UI Deployment scaled from one to three Ready replicas in 14 measured
  seconds and returned to its one-replica course setting. Its chart exposes an
  ordinary replica setting.
- A deliberate unschedulable replica request triggered the configured
  availability alert, which cleared after recovery.
- The submitted pod capture contains two independently Ready quality-service
  replicas. The stateless quality API has a measured safe range of two to three replicas
  on the course cluster. A fourth replica removed the memory reservation needed
  by the node-local DuckDB index, so it is not part of the submitted profile.
- The external `Qwen3.8-27B` endpoint is provider managed. Stream2Pretrain can
  adjust request concurrency, but does not claim Kubernetes autoscaling for a
  model it does not host.
- The frozen evidence does not include a multi-replica source-controller,
  ingest, Bytewax, Iceberg, DuckDB, or Foundry run. Their current scale-out
  contracts are offline render and concurrency-test evidence only.
- The curated platform-wide capture combines Ready/Running Pod rows from the
  successful read-only evidence workflow with Helm release records queried
  immediately afterward. It records Redpanda, the Helm-managed MinIO
  StatefulSet, Polaris/PostgreSQL, ingress, KEDA, monitoring, and representative
  application workloads from the earlier topology. It does not demonstrate the
  later three-broker Redpanda, four-member MinIO, three-instance CloudNativePG,
  or two-replica application profile.

The bounded 4 September measurement lasted 1,650.8 seconds and recorded 113
normalized events and 32 curation decisions. Normalized input grew faster than
completed decisions during that interval, so it does not demonstrate sustained
catch-up capacity. Event counts include replay and must not be interpreted as
fresh unique-document throughput.

## Content spot-check

The read-only audit inspected 21 document details and 12 generated artifacts.
It was purposive rather than random, so it is not an error-rate measurement.

Observed strengths:

- Sampled HTML papers retained coherent scientific prose, tables and equations
  without reference sections.
- Sampled Hugging Face cards retained substantive evaluation, data-layout and
  sampling information.
- Explicitly incompatible items produced admission-only quarantine records
  without body retrieval.
- Generated SFT and RL artifacts retained their evidence, verifier results and
  audit history.

Observed limitations:

- Historical PDF rows can retain pre-abstract author material.
- Numerical scientific text can trigger false-positive phone redaction.
- One English document was rejected by language confidence.
- The sampled experimental Foundry output included both an automatically
  accepted answer with insufficient visible support and a plausible rejected
  answer whose verifier failed.

No human-approved Foundry artifact is presented as final training output. The
post-training branch remains an inspectable experimental extension, while the
pretraining curation path is the primary submitted pipeline.
