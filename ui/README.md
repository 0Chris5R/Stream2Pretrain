# Stream2Pretrain UI

The Next.js curation UI contains six workspaces:

- `/dashboard`: live corpus, route, source, acceptance, and score state;
- `/documents`: paginated curation browser with filters, section decisions,
  projection, figures, tables, OCR audit, and advanced provenance;
- `/sources`: read-only source topology, health, throughput, and licence outcomes;
- `/datasets`: point/range selection with JSONL or Parquet export and manifest;
- `/mixture`: the explicitly future N3 two-branch experiment.

The product design and upstream inspirations are recorded in
`docs/UI_DESIGN_PROVENANCE.md`. Score meanings are in
`docs/SCORING_AND_ROUTING.md`.

## Stack

- Next.js 16 App Router, React 19, TypeScript;
- Tailwind and local shadcn/ui primitives;
- TanStack Query and Zod wire validation;
- Recharts for compact live and distribution plots;
- DuckDB API for Iceberg-backed collection queries and exports;
- lucide-react icons.

## Development

```bash
npm ci
npm run dev
npm run lint
npm run typecheck
npm run build
```

The Podman profile builds and serves this UI on `http://localhost:3100`.

## Runtime services

| Variable | Purpose |
|---|---|
| `DUCKDB_URL` | documents, dashboard, facets, and dataset exports |
| `SOURCES_API_URL` | Kubernetes controller or persisted local source monitor |
| `PROMETHEUS_URL` | live activity stream |
| `S2P_LOCAL_MODE` | reports the active local runtime profile |

Every browser payload is validated with Zod in `ui/lib/schemas.ts`. Browser
code never talks directly to Kubernetes, object storage, or the Iceberg
catalog. Next.js routes under `ui/app/api` form the typed backend-for-frontend.

## Source monitoring

The Sources page is monitoring-only. In Kubernetes, source configuration is
managed as deployment configuration and observed through the controller. In
Podman, `processor/local_sources_api.py` reports the file-configured sources and
their scheduled ingestion status.

## Dataset exports

The export API enforces risk tier 1, no rejection reasons, fixture exclusion,
and training-route selection. The UI exposes date range, route, source, one
content tag, structured-surrogate inclusion, JSONL, and Parquet. Post-training
benchmark splits are allocated only after generated artifacts pass validation
and are never part of a pretraining export.

## Container image

```bash
podman build -t stream2pretrain-ui:local -f ui/Dockerfile ui
```

The image contains the standalone Next.js server. Classifier and extraction
models live in processor images/volumes, not in the UI.
