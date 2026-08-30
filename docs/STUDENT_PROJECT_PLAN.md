# Student project execution plan

## Goal

Demonstrate a real Kubernetes big-data system that continuously ingests fresh
AI research material, performs non-trivial stateful processing, writes an
auditable Iceberg corpus, and serves live monitoring through a containerized
UI. Model training is an optional downstream demonstration, not a course
requirement.

## Required end-to-end path

1. Poll the configured arXiv and Hugging Face sources.
2. Resolve item-level rights before retaining a content body.
3. Persist immutable Bronze content and provenance.
4. Normalize each supported source with its dedicated projection.
5. Apply source-aware language, privacy, quality, and duplication policies.
6. Record every route decision and persist eligible training text in Iceberg.
7. Expose durable totals, live stage activity, sources, documents, exports, and
   post-training state through a read-only cockpit.
8. Generate daily ranked scientific-paper SFT and RL candidates, validate them,
   package accepted artifacts, and require a named human decision per artifact.

## Active corpus products

- Pretraining: permissively licensed arXiv main-body projections and substantive
  Hugging Face model/dataset documentation.
- Post-training input: the highest-ranked scientific papers that are either
  permissively licensed or allowed only for derived use.
- SFT: two independently generated solver trajectories for accepted tasks.
- RL: packaged environments whose deterministic positive, equivalence,
  adversarial, mutation, metamorphic, replay, and security checks pass.
- Post-training evaluation split: 20 percent of validated artifacts in each SFT
  and RL pool, assigned after generation and validation.

## UI contract

Dashboard, Documents, Sources, Datasets, and Mixture are monitoring and export
surfaces without runtime configuration controls. Document and Foundry details
open in dialogs so tables remain usable at large row counts. Post-training
artifact approval and rejection are the only ordinary user mutations.

## Deployment and validation

- Helm is the deployment source of truth.
- Redpanda, MinIO, Polaris, Prometheus, and the application workloads are
  deployed independently so unchanged infrastructure is not rebuilt.
- Bytewax owns durable processing progress; record-local deterministic failures
  are written to the failure ledger and transient failures replay.
- The release gate requires unit tests, schema generation, UI lint/typecheck and
  build, Helm rendering, an isolated cloud smoke record, and no production-lane
  leakage.
- Throughput, memory, and daily storage claims must be measured on the actual
  k3s cluster and marked `needs-measurement` until then.

## Deferred work

The N3 mixture experiment remains the only intentionally deferred product
feature. When GPU budget is available, two branches will train the same small LM
on rolling corpus mixtures and compare held-out per-domain loss before a recipe
is promoted.
