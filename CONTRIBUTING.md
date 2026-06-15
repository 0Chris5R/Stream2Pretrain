# Contributing to Stream2Pretrain

Thanks for taking the time to contribute. This document covers what you
need to know to work on the project.

## Code of conduct

This project adheres to the [Contributor Covenant](./CODE_OF_CONDUCT.md).
Participation in any community space implies acceptance of those terms.

## Toolchain

- Python: managed via [`uv`](https://docs.astral.sh/uv/). Do **not** use a
  bare `pip` against the workspace.
- Helm 3.14+ and `helmfile` for chart work.
- `kubectl`, `rpk`, and `mc` for cluster operations (see
  `docs/operations.md`).
- Node 20+ and `pnpm` for the UI (`ui/`).

A first-time bootstrap looks like:

```bash
uv sync --all-extras
make seed-topics
make dev-up
```

## Repository layout

See `RESEARCH.md` section 8 and the README "Repo layout" section.

## Style

- No emojis, no em dashes (use hyphens or colons).
- All Python uses `ruff` formatting and `mypy --strict`. Run `make fmt`
  before committing.
- All shell scripts use `set -euo pipefail` at the top and quote variables.
- Never invent numerical values: mark unmeasured numbers `needs-measurement`.
- Helm templates must pass `helm lint` (run `make helm-lint`).

## Tests

Component-local unit tests live next to their components
(`ingest/<component>/tests`, `processor/tests`, etc.). Cross-component
integration tests live in `tests/integration/`. Run the full suite with:

```bash
uv run pytest
```

Integration tests skip cleanly when Docker or the dev stack is unavailable.

Load tests use [k6](https://k6.io):

```bash
k6 run -e SUBMIT_URL=http://localhost:8000/submit tests/load/k6_submit.js
```

## Branches and PRs

- Branch from `main`. Use a short topical prefix: `feat/`, `fix/`, `chore/`,
  `docs/`.
- One logical change per PR. Bundle scope-creep into follow-up PRs.
- PR description must answer: what changed, why, what tests cover it, any
  `needs-measurement` items added or resolved.

## Commit messages

Conventional Commits style:

```
feat(ingest): add Atom poller for github releases
fix(processor): drop docs whose minhash signature is empty
chore(charts): bump kube-prometheus-stack to v62
docs(architecture): clarify validity-interval precedence
```

## Releasing

Releases are tagged from `main`. Bump `pyproject.toml` and the chart's
`Chart.yaml`, update `CHANGELOG.md`, then tag `v<major>.<minor>.<patch>`.

## Getting help

Open a discussion on GitHub. For security-sensitive reports (a credential
in a log line, an exploit in the operator surface), email the maintainers
listed in the repository metadata privately rather than opening a public
issue.
