# Stream2Pretrain cockpit (UI)

Next.js 14 App Router front-end for Stream2Pretrain. Serves five workspaces:

- `/dashboard` - last-hour KPIs, live curated docs/min spark line (SSE),
  FineWeb-Edu quality histogram, rejected-by-reason breakdown, per-source
  acceptance table.
- `/sources` - SourceFeed CRD list with poll state, last-success time, 24 h
  error rate; create + delete via API routes.
- `/decon` - signed contamination attestations; one-click `cosign verify-blob`
  via the server-side `/api/decon/verify` route.
- `/as-of` - date picker + `iceberg_scan(snapshot_from_timestamp => ?)` against
  the gold table; token mixture by source for the chosen instant.
- `/mixture` - A/B comparison of two MixtureRecipe branches; per-step proxy-LM
  perplexity delta.

## Stack

- Next.js 14.2 (App Router) + React 18 + TypeScript 5.5
- Tailwind 3.4 + shadcn/ui primitives (inline, no `shadcn` CLI)
- TanStack Query 5 (data fetching, refetch intervals)
- Zod 3 schemas mirrored from `schemas/json_schema/*.schema.json`
- Recharts 2.12 for histograms, sparkline, area charts
- DuckDB-WASM 1.29 for in-browser Parquet queries (lazy-loaded);
  fallback `proxy` mode forwards to the in-cluster DuckDB-server pod
- lucide-react icons

## Package manager

We use **npm** (not pnpm). `package-lock.json` is committed; CI installs with
`npm ci`. The Dockerfile is also npm-only.

## Local development

```bash
npm install
npm run dev          # http://localhost:3000
npm run lint         # next lint
npm run typecheck    # tsc --noEmit
npm run build        # production build (also produces .next/standalone)
```

The `dev` server expects two backends to be reachable:

- A Next.js API surface in this same app (auto-served).
- An optional in-cluster Prometheus for the live throughput SSE stream.
  Without it, the spark line stays flat at zero and the SSE endpoint emits
  an `x-error` comment per tick (the EventSource auto-reconnects).

### Environment variables

| Var | Default | Notes |
|---|---|---|
| `PROMETHEUS_URL` | `http://kube-prometheus-stack-prometheus.monitoring.svc:9090` | base URL for the SSE throughput stream |
| `THROUGHPUT_TICK_MS` | `5000` | min `1000`; cadence of SSE frames |
| `THROUGHPUT_RANGE` | `1m` | PromQL range selector for `rate()` |
| `DECON_GATE_URL` | `http://decon-gate.stream2pretrain.svc:8081` | source for attestation payloads |
| `COSIGN_BIN` | `cosign` | absolute path to cosign binary in container |
| `DUCKDB_MODE` | unset | set to `proxy` to forward queries to in-cluster DuckDB-server |

## API routes (proxies)

The cockpit ships a thin proxy layer under `app/api/*`. Routes here are
deliberately small: business logic lives in the in-cluster services. Today
the implemented routes are:

- `POST /api/decon/verify` - shells out to `cosign verify-blob` against the
  attestation fetched from `DECON_GATE_URL`.
- `GET  /api/throughput/sse` - Server-Sent Events stream synthesised from
  Prometheus instant queries. Re-emits a synthetic zero-frame when Prometheus
  is unreachable so the chart stays connected.

The other routes referenced by pages (`/api/dashboard`, `/api/sources`,
`/api/decon`, `/api/as-of`, `/api/mixture/compare`) are served by the cluster
control plane (FastAPI + Kubernetes API) reachable through Traefik. Wiring
them through Next.js is intentionally deferred so we do not duplicate
authentication/authorisation logic in two places.

## DuckDB-WASM

`lib/duckdb-client.ts` lazily instantiates `@duckdb/duckdb-wasm` from the
JSDelivr CDN (the worker is created via a one-line `importScripts` shim so
Webpack does not try to bundle it). It loads `httpfs` + `iceberg` extensions
and reads short-lived S3 credentials from a `<meta name="x-s2p-s3-creds">`
tag rendered by the page when the user is authorised to query MinIO directly.

For environments without `SharedArrayBuffer` (some embedded browsers, some
proxies that strip COOP/COEP headers), set `DUCKDB_MODE=proxy` and route
queries through `/api/duckdb/query` to the in-cluster DuckDB-server.

## Layout

```
ui/
  app/
    api/
      decon/verify/route.ts        - server-side cosign verify
      throughput/sse/route.ts      - Prometheus-backed SSE stream
    dashboard/page.tsx
    sources/page.tsx
    decon/page.tsx
    as-of/page.tsx
    mixture/page.tsx
    layout.tsx
    page.tsx                       - landing page
    globals.css                    - tailwind base + shadcn tokens
  components/
    ui/                            - shadcn primitives (button/card/dialog/...)
    nav.tsx, providers.tsx
    quality-histogram.tsx
    timeline.tsx
    attestation-viewer.tsx
    source-card.tsx
    throughput-spark.tsx
  lib/
    api.ts                         - typed fetcher + zod validation
    schemas.ts                     - zod schemas mirrored from JSON Schema
    duckdb-client.ts               - DuckDB-WASM bootstrap
    query-keys.ts                  - TanStack Query key registry
    utils.ts                       - cn(), formatInt, formatTs, ...
  Dockerfile                       - multi-stage; node:20-alpine + cosign
  next.config.js                   - output: 'standalone', COOP/COEP headers
  package.json, tsconfig.json
  tailwind.config.ts, postcss.config.js
  components.json                  - shadcn config (manual primitives)
  .eslintrc.json, .prettierrc
```

## Container image

```bash
docker build -t stream2pretrain/ui:dev ui/
docker run --rm -p 3000:3000 \
  -e PROMETHEUS_URL=http://prom:9090 \
  -e DECON_GATE_URL=http://decon-gate:8081 \
  stream2pretrain/ui:dev
```

The runner stage is built on `node:20-alpine` and ships `cosign` + `tini`
alongside the Next.js standalone server. Final image size is
`needs-measurement` (depends on registry compression; estimate not yet
benchmarked on the cluster).

## Caveats / what is intentionally stubbed

- The `/api/decon/verify` route invokes `cosign verify-blob` with
  `--insecure-ignore-tlog`. We do not yet pin a Rekor instance; once the
  in-cluster Sigstore stack lands, drop that flag and pass `--rekor-url`.
- `lib/duckdb-client.ts` reads S3 credentials from a `<meta>` tag rendered
  server-side. In production, those credentials must be short-lived
  (e.g. STS via MinIO's AssumeRoleWithWebIdentity) - the cockpit must never
  embed long-lived keys.
- The dashboard page reads `/api/dashboard` and `/api/sources`, which are
  not implemented in this directory. They are served by the in-cluster
  control plane; in dev, you can stub them with a small fixture server.

## See also

- `RESEARCH.md` (sections 4-8) - full architectural spec.
- `CLAUDE.md` - decision log and locked stack.
- `SOURCES.md` - source feed catalog with rate limits.
