# Classifier activation and reliability, 2026-09-04

Owner-approved implementation contract:

- Gate the sanitized full-document projection by token-weighted mean source
  quality: arXiv >= 3.0, HF cards >= 3.5. No calibration campaign blocks this
  rollout. Confidence is not a gate. Do not filter individual sections by score.
- Run arXiv mathematical-reasoning and post-training-suitability classifiers
  only after the arXiv quality gate passes. Score every retained section then.
  Source licence eligibility remains independent: transform-only papers may
  qualify for post-training without being admitted to pretraining.
- Rank the daily post-training queue by token-weighted mean suitability.
  Maximum section scores only supply short optional sentences identifying
  especially relevant or mathematically promising sections in the existing
  generator prompt. Preserve the complete paper and existing task pipeline.
  No section-only input, forced task allocation, or class-5 admission rule.
- Repair the DuckDB serving write path, with incremental, idempotent ingestion,
  all-corpus counts, pagination, crash recovery and authoritative Iceberg parity.
  Do not repeatedly restart an invalid database or scan history per UI request.
- Persist accepted scientific evidence before publishing candidates, independent
  of transient raw-object retention. Clear stale unstarted queue work once;
  do not attempt historical paper repair. Preserve generated artifacts/audits
  and an in-progress job. Old Kafka deliveries must not refill the reset queue.
- Fix repeated expired-input work and any other observed runtime failures without
  weaker models, truncated sections, disabled checks or unapproved data deletion.
- Local validation is deterministic code correctness only. Live inference,
  workload validation and performance measurement take place in the cloud.
- Deploy coordinated changes, verify fresh scored/routed/durable documents,
  dashboard parity and SFT/RL progress, and report remaining capacity needs.
  No scheduled task.

Execution checklist:

- [x] Two-stage classifier protocol, quality gates, ranking and prompt hints.
- [x] Serving index repair and deterministic regression coverage.
- [x] Evidence retention and one-time stale queue reset.
- [x] Expired-input/retry/publication checks.
- [ ] Deterministic checks, cloud deployment and bounded live audit.
