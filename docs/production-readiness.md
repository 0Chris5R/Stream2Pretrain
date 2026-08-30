# Production Readiness Backlog

This backlog is the execution catalog for moving Stream2Pretrain from prototype
to an operable Kubernetes system. Backward compatibility is not a constraint;
the target is a clean production-grade contract between code, manifests, tests,
and documentation.

Status values:

- `todo` - confirmed from current repo evidence, not fixed yet.
- `in-progress` - currently being changed.
- `done` - fixed and verified by the listed evidence.
- `needs-measurement` - cannot be decided without running against the target
  cluster or real workload.

## Release Gates

These must pass before calling the repo production ready:

- `uv run pytest` runs the default Python suite without workspace errors.
- `uv run ruff check schemas ingest processor tests` passes.
- `uv run python scripts/security_scan.py` passes.
- `uv run mypy schemas ingest processor tests` has either a clean pass or a
  documented, narrow production exception.
- `helm lint charts/stream2pretrain` passes.
- `helm template stream2pretrain charts/stream2pretrain --debug` renders.
- `npm ci --prefix ui` and `npm run build --prefix ui` pass from a committed
  lockfile.
- `uv run python scripts/capacity_probe.py` can generate a capacity report
  against the target kube context; generated reports must keep uncollected
  values as `needs-measurement`.
- Rendered Kubernetes workloads use the same environment variable names the
  Python and Next.js runtimes read.
- Every configured Kubernetes liveness/readiness probe targets an implemented
  endpoint.

## Catalog

| ID | Status | Area | Required fix | Verification |
|---|---|---|---|---|
| PRD-001 | done | Python workspace | Add every local Python component to the uv workspace, add a real `tests` package, and make the root dev group install all testable components. | `uv run pytest -q` - 280 passed, 4 skipped |
| PRD-003 | done | Runtime config | Align Helm env vars with `ingest.common.config` and `processor.common`. Remove silent localhost/default credential fallbacks from Kubernetes paths. | Rendered manifests plus config unit tests |
| PRD-004 | done | Kubernetes probes | Add real health/ready/metrics endpoints or remove invalid probes. Prefer explicit endpoints for long-running Deployments. | Rendered manifests plus probe route/module tests |
| PRD-005 | done | UI build | Commit `ui/package-lock.json` and make UI build reproducible with `npm ci`. | `npm ci --prefix ui`, `npm run build --prefix ui` |
| PRD-007 | done | Seed loader | Recompute real MinHash in curate when seed rows carry placeholders, so day-one seed data can reach clean Gold. | Curate unit test with seed placeholder input |
| PRD-008 | done | SPDX provenance | Propagate `source_format`, `extraction_pipeline`, `spdx_license`, and `spdx_license_source` from Silver to Gold and Iceberg. | Curate + Iceberg writer tests |
| PRD-010 | done | Metrics | Ensure every metric used by Grafana and UI is emitted by code with stable names. | Processor and ingest metric unit tests; `source_feed` propagated through Silver/Gold for per-source UI queries |
| PRD-012 | done | Security | Remove default wide-open ingress/admin CIDRs, require explicit prod secrets, and add secret-scan/lint gates. | `uv run python scripts/security_scan.py`, scanner unit tests, Terraform fmt/validate, Helm prod render |
| PRD-013 | needs-measurement | Capacity | Measure Redpanda partitions, worker CPU/RAM, MinIO throughput, recovery storage, and model-cache sizing on the target k3s cluster. | `docs/capacity-benchmark.md`, `scripts/capacity_probe.py`, and a generated target-cluster report with commands and outputs |
| PRD-014 | done | Documentation | Update README, chart README, and operations docs to match implemented deploy paths only. | Documentation drift check |
| PRD-015 | done | UI backend services | Implement the DuckDB server, read-only source monitor, and mixture comparison REST routes the UI proxies to. | DuckDB API + mixture controller API unit tests, UI typecheck, rendered prod Services |
| PRD-016 | done | Gold/training contract | Decide and enforce whether `docs.curated` / Gold contains only trainable rows or all scored rows. If all scored rows remain, every training/as-of query must filter `risk_tier = 1` and empty `reject_reasons`. | Curate dataflow tests plus defensive Iceberg writer test |
| PRD-017 | done | Typing gate | Make `uv run mypy schemas ingest processor tests` pass or document narrow, production-scoped exceptions for optional runtime dependencies. | `uv run mypy schemas ingest processor tests`; explicit overrides for untyped runtime integrations and non-production test fakes |

## Typing Exceptions

The mypy gate remains strict for first-party schemas and normal application
code. Narrow overrides in `pyproject.toml` cover third-party runtime packages
that do not ship usable stubs (`bytewax`, `pyiceberg`, `duckdb`, model-serving
libraries, Kubernetes clients, and similar boundary SDKs) plus test fakes whose
protocol completeness is already covered by unit tests. `warn_unused_ignores` is
disabled because several optional dependency imports flip between
`import-not-found` and `import-untyped` depending on which runtime extras are
installed locally; this is treated as a mypy environment issue, not as a reason
to block production deploys.

## Latest Local Verification

- `uv run pytest -q` - 307 passed, 4 skipped.
- `uv run pytest -q tests/test_security_scan.py` - 6 passed.
- `uv run pytest -q processor/tests/test_metrics.py ingest/common/tests/test_metrics.py processor/tests/test_fetcher.py processor/tests/test_curate.py processor/tests/test_iceberg_writer.py` - 21 passed.
- `uv run pytest -q processor/tests/test_duckdb_api.py processor/tests/test_mixture_controller_api.py ingest/common/tests/test_feeds.py` - 12 passed.
- `uv run pytest -q tests/test_capacity_probe.py` - 4 passed.
- `uv run ruff check schemas ingest processor tests` - passed.
- `uv run ruff check processor/duckdb_api.py processor/tests/test_duckdb_api.py processor/mixture_controller/controller.py processor/tests/test_mixture_controller_api.py schemas/sourcefeed.py ingest/common/tests/test_feeds.py` - passed.
- `uv run ruff check scripts/security_scan.py tests/test_security_scan.py infra/terraform` - passed.
- `uv run ruff check scripts/capacity_probe.py tests/test_capacity_probe.py` - passed.
- `uv run python scripts/security_scan.py` - passed.
- `uv run python scripts/capacity_probe.py --out /tmp/s2p-capacity.md --json-out /tmp/s2p-capacity.json` - passed locally and preserved missing cluster values as `needs-measurement`.
- `helm lint charts/stream2pretrain` - passed.
- `helm template stream2pretrain charts/stream2pretrain` - passed.
- `helm template stream2pretrain charts/stream2pretrain -f charts/stream2pretrain/values-dev.yaml` - passed.
- `helm template stream2pretrain charts/stream2pretrain -f charts/stream2pretrain/values-prod.yaml` - passed.
- `npm ci --prefix ui` - passed with an engine warning for transitive `eslint-visitor-keys` on local Node v22.12.0; no vulnerabilities.
- `npm run build --prefix ui` - passed.
- `npm run typecheck --prefix ui` - passed.
- `uv run mypy schemas ingest processor tests` - passed on 160 source files with the documented
  optional-runtime and test-fake exceptions above.
- `terraform fmt -check infra/terraform` - passed.
- `terraform -chdir=infra/terraform validate` - passed after `terraform init -backend=false`.
- Documentation drift check for stale service claims - passed.
