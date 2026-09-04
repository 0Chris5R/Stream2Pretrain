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
- For legacy records with expired source evidence, retain an independently
  valid, permissively licensed pretraining projection and remove post-training
  eligibility. Do not turn missing post-training evidence into a pretraining
  quality rejection. New candidate evidence must still be persisted in Gold.
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
- [x] Deterministic checks, cloud deployment and bounded live audit.

Validation so far:

- 610 deterministic checks passed locally; no local pipeline/model execution.
  Two dependency-specific checks were skipped on the host.
- Cloud release `bf81dfb` completed and all four heads passed the production
  protocol check on each of four replicas. The missing fetcher image module
  found during the first rollout was corrected.
- The cloud browser audit returned HTTP 200 for all eight pages and eleven API
  probes with no browser page errors. DuckDB, curator, fetcher, writer and all
  classifier replicas had zero restarts in the post-rollout snapshot.
- The queue reset preserved the active job and its cached evidence. Fresh
  artifact production and steady-state throughput remain `needs-measurement`.
- The first active arXiv samples were legacy normalized records with expired
  Silver evidence. These are not representative measurements of fresh evidence
  preservation. The forward-only handling above preserves usable pretrain text
  without reconstructing historical scientific artifacts.
- `check-pipeline` is a short read-only Actions mode for current pod health,
  serving totals, processing counters, active classifier decisions and recent
  Foundry events. It does not launch model calls or mutate the queue.

Final bounded check, 2026-09-04 13:31 UTC:

- Application release `6ad3557` deployed successfully in run `33877530689`.
  The deployment step took 189 seconds, excluding validation and image builds.
- DuckDB, curator, fetcher, writer, Foundry and the four classifier replicas were
  Ready with zero restarts in the post-rollout check. The subsequent compact
  check also returned healthy serving and processing endpoints.
- A real arXiv document scored 3.4829 across 27 retained sections, received both
  reasoning heads, and entered both eligible routes. Its scientific evidence
  was materialized under `s2p-gold/scientific-evidence/` and the Foundry queue
  held its cached evidence. A real HF card at 2.9775 was quarantined at the 3.5
  cutoff without either arXiv reasoning head.
- New pretraining publication resumed. The cleared Foundry queue had one
  preserved processing paper and one newly admitted paper, both with evidence.
- The active solver's third turn was still streaming, with 28,995 response
  characters and a fresh checkpoint at 13:31:18 UTC. Attempt 47 is the job's
  call sequence, not 47 retries. No new completed SFT/RL artifact was claimed
  during this bounded check; generation remains running.
- Sustained throughput and catch-up capacity remain `needs-measurement`.
  Historical expired-record handling and rollouts contaminate these short
  windows, so neither readiness nor the sampled route counts establish that
  the cluster can sustain the full daily intake.
