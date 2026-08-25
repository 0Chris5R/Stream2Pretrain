# Stream2Pretrain UI

The Next.js curation UI contains six workspaces:

- `/dashboard`: live corpus, route, source, acceptance, and score state;
- `/documents`: paginated curation browser with filters, section decisions,
  projection, figures, tables, OCR audit, and advanced provenance;
- `/sources`: source topology and real add/edit/enable/run/delete actions;
- `/decon`: Benchmark Safety reserve coverage and automatically verified signed
  attestations;
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
| `SOURCES_API_URL` | Kubernetes CRD controller or persisted local SourceFeed API |
| `DECON_GATE_URL` | benchmark reserve coverage, attestations, verification |
| `PROMETHEUS_URL` | live activity stream |
| `S2P_LOCAL_MODE` | reports the active local runtime profile |

Every browser payload is validated with Zod in `ui/lib/schemas.ts`. Browser
code never talks directly to Kubernetes, object storage, or the Iceberg
catalog. Next.js routes under `ui/app/api` form the typed backend-for-frontend.

## Source controls

In Kubernetes, writes target real `SourceFeed` CRDs and `Run once` creates a
bounded Job from the appropriate poller template. In Podman, the compatible
local source service persists specs in its named volume and runs RSS/Atom,
OAI-PMH ingestion against the same local Redpanda and MinIO. The
default arXiv RSS source resolves current entry ids and fetches full native HTML
or the bounded PDF fallback.

## Dataset exports

The export API enforces risk tier 1, no rejection reasons, fixture exclusion,
and training-route selection. The UI supports source, source format, tags,
score floors, date range, structured-surrogate inclusion, JSONL, and Parquet.
The separately routed benchmark reserve is never part of a training export.

## Container image

```bash
podman build -t stream2pretrain-ui:local -f ui/Dockerfile ui
```

The image contains the standalone Next.js server. Classifier and extraction
models live in processor images/volumes, not in the UI.
