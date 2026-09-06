# Local Podman pilot

The local profile runs the same source projection, Bytewax curation, CPU model
services, MinIO objects, local Iceberg warehouse, DuckDB API, Prometheus, and
Next.js cockpit used by the deployable application.

## Start

```bash
make local-up
```

The UI is available at `http://localhost:3100`. The profile uses project-scoped
named volumes and explicit CPU/memory limits. If a port is occupied, change only
the host-side port and do not stop unrelated services.

## Pipeline

```text
scheduled source ingest
  -> raw.fetched
  -> Bytewax fetcher and source-specific extraction
  -> docs.normalized
  -> source-aware quality, language, privacy, and deduplication
  -> curation.decisions plus accepted docs.curated
  -> Iceberg decisions and training rows
  -> DuckDB and read-only monitoring UI
```

arXiv uses full scientific extraction and the four-model source-classifier
bundle. The arXiv quality mean gates at 3.0, HF at 3.5; both arXiv auxiliary
heads run after quality passes. Hugging Face uses exact-revision README
projection and its dedicated card-structure gate. KenLM and web heuristics are
not applied to papers or cards. Model bootstrap downloads the checksum-pinned
release archives into the model volume.

## Bounded fixtures

```bash
make local-ingest-fixtures
make local-status
```

Fixtures cover clean acceptance, exact/near duplicate handling, a web-heuristic
failure, and a PII failure. They are synthetic integration records, not reported
as source throughput.

## Monitoring contract

Dashboard totals and document tables come from durable Iceberg state. Live
stage charts come from Prometheus. Sources reports the file-configured source
workloads and scheduler status and has no mutation controls. Documents and
post-training job details open in dialogs. Only named SFT/RL artifact approval
or rejection is interactive.

## Stop

```bash
podman compose -f compose.local.yml down
```

Do not add `-v` unless the explicit goal is to delete the project-scoped local
data volumes.
