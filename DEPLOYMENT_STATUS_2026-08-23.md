# Stream2Pretrain deployment status — 2026-08-23

## Executive status

The application release is deployed and Kubernetes reports the rollout as successful. The release workflow also materialized all configured source schedules and passed the isolated one-record pretraining canary. The deployment is **not yet fully functional**: the final rendered audit still fails because DuckDB/Iceberg-backed cockpit queries exceed the 30-second client deadline.

Current branch: `fix/deployment-pipeline`

Application hostname: `https://stream2pretrain-app.s241221-at-student-dhbw-mannheim-de.users.dhbw.site/`

## Verified green

- Deployment run [32639246763](https://github.com/0Chris5R/Stream2Pretrain/actions/runs/32639246763) completed successfully from commit `0d00db4`.
- Repository validation, Helm rendering, image resolution, cluster capacity preflight, workload rollout, source-schedule materialization, and the isolated pretraining canary passed.
- The final browser audit rendered every route with HTTP 200: `/`, `/dashboard`, `/documents`, `/sources`, `/decon`, `/datasets`, `/post-training`, `/mixture`, and the `/as-of` redirect.
- `/api/health`, `/api/activity`, `/api/decon`, `/api/decon/coverage`, and `/api/foundry/activity` returned valid JSON with HTTP 200 in the final audit.
- Benchmark-safety coverage now accepts the configured `synthetic_canary` corpus kind and reports all five configured benchmark families.
- Local repository verification completed with 544 tests passing and 6 environment-dependent skips; Ruff check and formatting checks pass.
- Unchanged container images are reused by digest. In the successful build, unchanged components resolved in roughly 14–29 seconds and the rebuilt processor completed in 57 seconds.

## Remaining release blocker

Final audit run [32639554691](https://github.com/0Chris5R/Stream2Pretrain/actions/runs/32639554691) failed.

The previous 1 GiB DuckDB pod was conclusively OOM-killed during audit run `32638807164`. The dev deployment now gives DuckDB a 1 GB engine budget and a 2 GiB pod limit, with one query thread and bounded disk spill. That allowed the replacement pod to deploy and pass readiness, but the following final-audit requests still exceeded 30 seconds:

- `/api/dashboard`
- `/api/documents`
- `/api/documents/facets`
- `/api/datasets/summary`
- `/api/as-of`

Later probes to sources and two Foundry endpoints also timed out or reset after those outstanding requests. Their services had returned HTTP 200 earlier, so the primary demonstrated blocker remains the serialized DuckDB/Iceberg query lane. A post-audit pod termination inspection was not run, so this document does not claim whether the 2 GiB pod restarted during the final audit.

The principled next fix is to stop recomputing `ROW_NUMBER` deduplication and dashboard aggregates over the complete append-only Iceberg scan on each request. Compaction should materialize a deduplicated current snapshot (and small dashboard aggregate tables), while API requests read those bounded tables and refresh them when the Iceberg snapshot ID changes. Merely increasing memory again would not solve the latency or scaling problem.

## Pipeline status

### Pretraining

- Synthetic end-to-end canary: passing.
- SourceFeed controller: all configured schedules materialized during deployment.
- License admission, curation route, and training-output path: exercised successfully by the canary.
- Complete live validation of every external source is not established by the deployment canary and must not be represented as complete.

### Post-training

- Foundry API and UI render successfully under normal access.
- Durable state observed in the audit: 4 accepted SFT trajectories, 0 accepted RL environments, and 13 rejected RL environments.
- RL acceptance is therefore still an unresolved requirement.
- The final audit also showed long-running/stalled Foundry jobs and a recent Hetzner provider HTTP 503. Post-training is not end-to-end complete.

## Monitoring and storage

- Monitoring recovery run [32637679416](https://github.com/0Chris5R/Stream2Pretrain/actions/runs/32637679416) completed and Prometheus passed its readiness endpoint with an automatic Go heap limit.
- A later diagnostic observed another Prometheus OOM restart at 12:03 UTC. Activity queries subsequently returned HTTP 200, but monitoring memory pressure is not fully resolved.
- Deployment preflight observed approximately 11 GB of root-filesystem space available on each worker and no active node pressure condition.
- MinIO volume metrics reported about 43 GB used on the underlying local-path filesystem despite the nominal 10 GiB claim. Storage accounting, retention, compaction, and expansion remain operational risks.

## Public reachability

The canonical hostname currently publishes IPv6 (`AAAA`) ingress addresses but no IPv4 (`A`) address. The Windows test client had no IPv6 default route, so public-browser reachability could not be verified from that client. The successful checks above used the authenticated VPN and an in-cluster service port-forward. This is an external ingress/DNS reachability limitation, separate from the successful Kubernetes rollout and the remaining DuckDB API defect.

## Changes included in this recovery branch

- Restored deterministic deployment validation and retry-safe Redpanda topic reconciliation across transient university-VPN Kubernetes API failures.
- Added bounded DuckDB memory, query-thread, metadata-refresh, and disk-spill configuration; raised the measured dev pod ceiling to 2 GiB after a confirmed OOM.
- Corrected benchmark-safety UI/schema handling for the configured synthetic canary reserve.
- Added an explicit monitoring-repair workflow with readiness checks and failure diagnostics.
- Added Prometheus automatic Go heap limiting.
- Kept the original `users.dhbw.site` hostname as the canonical deployment hostname.

## Bottom line

The cluster release is up and its pretraining deployment canary passes, but the project must not yet be described as completely working: lakehouse cockpit queries remain too slow, RL has no accepted environments, monitoring has recent OOM history, full live-source validation is incomplete, and the public hostname is IPv6-only.
