# Stream2Pretrain UI

The Next.js curation UI contains six workspaces:

- `/dashboard`: live corpus, route, source, acceptance, and score state;
- `/documents`: paginated curation browser with filters, section decisions,
  projection, figures, tables, OCR audit, and advanced provenance;
- `/sources`: read-only source topology, health, throughput, and licence outcomes;
- `/datasets`: export-only controls for pretraining JSONL/Parquet, SFT JSONL,
  and validated RL environment archives;
- `/as-of`: validity-interval corpus selection;
- `/post-training`: generation activity, artifact inspection and human audit.

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

The pretraining export enforces risk tier 1, no rejection reasons, fixture
exclusion, a permissive licence, and training-route selection. Its filters are
date range, source, structured-surrogate inclusion, and JSONL or Parquet.
Accepted SFT trajectories export as JSONL. Validated RL environments export as
a package archive. Both post-training pools can be filtered by train or
benchmark split.

## Container image

```bash
podman build -t stream2pretrain-ui:local -f ui/Dockerfile ui
```

The image contains the standalone Next.js server. Classifier and extraction
models live in processor images/volumes, not in the UI.
