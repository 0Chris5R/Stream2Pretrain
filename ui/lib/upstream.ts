/**
 * Shared upstream URL config for Next.js API route handlers.
 *
 * The cockpit's `/api/*` routes proxy to in-cluster services. Each
 * upstream is overridable via env so the same UI image runs against the
 * dev (kind/k3d) and prod (DHBWCloud k3s) clusters without rebuilds.
 *
 * Defaults match the Helm chart's Service names; see
 * `charts/stream2pretrain/templates/processor-curate.yaml` and
 * `ingest-submit-api.yaml` for the canonical names.
 */

export const UPSTREAM = {
  /** FastAPI submit-api: SourceFeed CRUD, manual document submit. */
  submitApi: process.env.SUBMIT_API_URL ?? 'http://stream2pretrain-submit-api.stream2pretrain.svc:80',
  /** Decon-Gate REST: per-snapshot attestation lookup, decon stats. */
  deconGate: process.env.DECON_GATE_URL ?? 'http://decon-gate.stream2pretrain.svc:8081',
  /** Mixture controller REST: shadow-mode A/B comparison. */
  mixture: process.env.MIXTURE_API_URL ?? 'http://stream2pretrain-mixture-controller.stream2pretrain.svc:8080',
  /** DuckDB-server: lakehouse temporal queries (as_of, mixture). */
  duckdb: process.env.DUCKDB_URL ?? 'http://stream2pretrain-duckdb.stream2pretrain.svc:8090',
  /** Prometheus: metrics queries used by the dashboard. */
  prometheus: process.env.PROMETHEUS_URL ?? 'http://kube-prometheus-stack-prometheus.monitoring.svc:9090',
} as const;

/**
 * Map upstream errors to a sanitised JSON detail. Never echo URLs or
 * resolved hostnames - leaks the in-cluster topology to anonymous
 * browsers (see /api/throughput/sse).
 */
export function upstreamError(reason: string): { detail: string } {
  if (/^[a-z0-9_]+$/i.test(reason)) return { detail: reason };
  return { detail: 'upstream_error' };
}
