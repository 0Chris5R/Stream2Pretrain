# Contributing

Use `uv` for Python, Node 20+ with npm for the UI, and Helm 3 plus Helmfile
for deployment configuration. See [README.md](README.md) for the architecture
and [the operations guide](docs/operations.md) for cloud commands.

## Deterministic checks

```bash
uv sync --all-packages --all-groups
uv run pytest schemas ingest processor tests --ignore=tests/integration
uv run ruff check schemas ingest processor tests scripts
uv run ruff format --check schemas ingest processor tests scripts
uv run python scripts/security_scan.py
helm lint charts/stream2pretrain
cd ui
npm ci
npm run typecheck -- --incremental false
npm run lint
```

Integration tests in `tests/integration/` require an explicitly started container
stack. They are separate from unit checks. [The local guide](local/README.md)
describes the Podman profile; do not start it as a side effect of linting.

## Changes

- Keep changes focused and cover behavioral contracts with tests.
- Use direct comments explaining invariants and decisions, not repair history.
- Update the relevant guide when a runtime contract changes.
- Keep credentials, model checkpoints, corpus exports and local state out of Git.
- Review rendered Helm manifests before applying them to an explicit context.
- Mark unmeasured capacity or quality claims `needs-measurement`.
- Follow [the code of conduct](CODE_OF_CONDUCT.md).

Simple descriptive commit messages are sufficient. Releases use immutable
image digests and checksum-pinned model artifacts, with versions declared in
`pyproject.toml` and `charts/stream2pretrain/Chart.yaml`.
