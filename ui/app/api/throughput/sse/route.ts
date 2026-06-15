/**
 * GET /api/throughput/sse
 *
 * Server-Sent Events stream of `ThroughputPoint` JSON frames, each emitted on
 * a fixed cadence (default 5 s). The values are pulled from the in-cluster
 * Prometheus instance via its `/api/v1/query` endpoint.
 *
 * Why SSE instead of WebSockets:
 *   - We only push one direction (server -> browser).
 *   - SSE survives Traefik's default proxying without sticky-session config.
 *   - The `EventSource` API auto-reconnects on transient failures.
 *
 * Failure mode: if Prometheus is unreachable, the route emits a synthetic
 * frame with zeros and an `x-error` SSE comment so the browser keeps the
 * connection alive without falsely registering throughput.
 *
 * Env:
 *   PROMETHEUS_URL    base URL of Prometheus (default: in-cluster service).
 *   THROUGHPUT_TICK_MS frame cadence in ms (default 5000, min 1000).
 *   THROUGHPUT_RANGE   PromQL range selector for rate() (default 1m).
 */
import type { NextRequest } from 'next/server';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

const PROMETHEUS_URL =
  process.env.PROMETHEUS_URL ?? 'http://kube-prometheus-stack-prometheus.monitoring.svc:9090';
const TICK_MS = Math.max(1_000, Number.parseInt(process.env.THROUGHPUT_TICK_MS ?? '5000', 10));
const RANGE = process.env.THROUGHPUT_RANGE ?? '1m';

// Counter names emitted by the processor (see processor/metrics.py).
const QUERIES = {
  ingested: `sum(rate(s2p_processor_ingested_total[${RANGE}]))`,
  curated: `sum(rate(s2p_processor_curated_total[${RANGE}]))`,
  rejected: `sum(rate(s2p_processor_dropped_total[${RANGE}]))`,
} as const;

interface PromInstantResp {
  status: 'success' | 'error';
  data?: {
    resultType: string;
    result: Array<{ metric: Record<string, string>; value: [number, string] }>;
  };
  error?: string;
}

/**
 * Throws an error with a sanitised message so the SSE error frame does
 * not leak the in-cluster Prometheus URL / hostname (`PROMETHEUS_URL`).
 * The original cause is preserved for server-side logs only.
 */
async function instantQuery(query: string, signal: AbortSignal): Promise<number> {
  const url = `${PROMETHEUS_URL}/api/v1/query?query=${encodeURIComponent(query)}`;
  let resp: Response;
  try {
    resp = await fetch(url, { signal, cache: 'no-store' });
  } catch (err) {
    console.warn('throughput.sse fetch failed', err);
    throw new Error('prometheus_unreachable');
  }
  if (!resp.ok) throw new Error(`prometheus_status_${resp.status}`);
  let json: PromInstantResp;
  try {
    json = (await resp.json()) as PromInstantResp;
  } catch {
    throw new Error('prometheus_invalid_json');
  }
  if (json.status !== 'success' || !json.data) {
    throw new Error('prometheus_error');
  }
  const first = json.data.result[0];
  if (!first) return 0;
  const v = Number.parseFloat(first.value[1]);
  return Number.isFinite(v) ? v : 0;
}

interface ThroughputFrame {
  ts: string;
  ingested: number;
  curated: number;
  rejected: number;
}

async function snapshot(signal: AbortSignal): Promise<ThroughputFrame> {
  const [ingested, curated, rejected] = await Promise.all([
    instantQuery(QUERIES.ingested, signal),
    instantQuery(QUERIES.curated, signal),
    instantQuery(QUERIES.rejected, signal),
  ]);
  // The processor exposes per-second rates; the UI displays per-minute counts
  // in the spark line, so we multiply by 60.
  return {
    ts: new Date().toISOString(),
    ingested: ingested * 60,
    curated: curated * 60,
    rejected: rejected * 60,
  };
}

function sseFrame(data: unknown): string {
  return `data: ${JSON.stringify(data)}\n\n`;
}

function sseComment(text: string): string {
  return `: ${text}\n\n`;
}

export async function GET(req: NextRequest): Promise<Response> {
  const encoder = new TextEncoder();
  const ac = new AbortController();
  // Forward the client's abort (close) to the in-flight Prometheus fetches.
  req.signal.addEventListener('abort', () => ac.abort(), { once: true });

  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      let closed = false;
      const close = () => {
        if (closed) return;
        closed = true;
        try {
          controller.close();
        } catch {
          // Already closed.
        }
      };

      // Initial hello so the browser knows the stream is alive and to flush
      // any intermediary proxy buffers (Traefik, kube-proxy).
      controller.enqueue(encoder.encode(sseComment(`stream2pretrain throughput; tick=${TICK_MS}ms`)));
      controller.enqueue(encoder.encode('retry: 5000\n\n'));

      const tick = async () => {
        if (closed) return;
        try {
          const frame = await snapshot(ac.signal);
          controller.enqueue(encoder.encode(sseFrame(frame)));
        } catch (err) {
          // Strip any URL/host that may have slipped into the message
          // before forwarding to the browser. Keep the categorical reason
          // (prometheus_unreachable, prometheus_status_502, etc.) only.
          const raw = (err as Error).message ?? 'unknown';
          const safe = /^[a-z0-9_]+$/i.test(raw) ? raw : 'prometheus_unavailable';
          controller.enqueue(encoder.encode(sseComment(`x-error: ${safe}`)));
          controller.enqueue(
            encoder.encode(
              sseFrame({
                ts: new Date().toISOString(),
                ingested: 0,
                curated: 0,
                rejected: 0,
              }),
            ),
          );
        }
      };

      // Emit the first frame immediately so the chart paints without a gap.
      await tick();
      const interval = setInterval(tick, TICK_MS);

      req.signal.addEventListener(
        'abort',
        () => {
          clearInterval(interval);
          close();
        },
        { once: true },
      );
    },
    cancel() {
      ac.abort();
    },
  });

  return new Response(stream, {
    status: 200,
    headers: {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    },
  });
}
