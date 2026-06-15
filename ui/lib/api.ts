/**
 * Typed fetch wrapper with zod validation.
 *
 * The cockpit talks to two backends:
 *   1) Next.js API routes (same origin) — these proxy to in-cluster services.
 *   2) Server-Sent Events for live throughput.
 *
 * Network errors and validation errors both throw `ApiError`, which the
 * TanStack Query hooks surface to the user via toast/banners.
 */
import { z, ZodSchema } from 'zod';

export class ApiError extends Error {
  status: number;
  detail?: unknown;

  constructor(message: string, status: number, detail?: unknown) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.detail = detail;
  }
}

export interface FetchOptions<TBody> {
  method?: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';
  body?: TBody;
  signal?: AbortSignal;
  headers?: Record<string, string>;
}

/**
 * Fetch JSON and validate it against `schema`.
 * Throws `ApiError` on non-2xx, network failure, or schema mismatch.
 */
export async function apiFetch<TOut, TBody = unknown>(
  url: string,
  schema: ZodSchema<TOut>,
  opts: FetchOptions<TBody> = {},
): Promise<TOut> {
  const headers: Record<string, string> = {
    Accept: 'application/json',
    ...(opts.headers ?? {}),
  };
  if (opts.body !== undefined) headers['Content-Type'] = 'application/json';

  let resp: Response;
  try {
    resp = await fetch(url, {
      method: opts.method ?? 'GET',
      headers,
      body: opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
      signal: opts.signal,
      cache: 'no-store',
    });
  } catch (err) {
    throw new ApiError(`network error: ${(err as Error).message}`, 0);
  }

  const text = await resp.text();
  let payload: unknown = null;
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      throw new ApiError(`non-json response: ${text.slice(0, 200)}`, resp.status);
    }
  }

  if (!resp.ok) {
    const msg =
      typeof payload === 'object' && payload && 'detail' in payload
        ? String((payload as { detail: unknown }).detail)
        : `request failed: ${resp.status}`;
    throw new ApiError(msg, resp.status, payload);
  }

  const parsed = schema.safeParse(payload);
  if (!parsed.success) {
    throw new ApiError(`response failed validation: ${parsed.error.message}`, resp.status, payload);
  }
  return parsed.data;
}

/** Build a query string from a record, skipping nullish values. */
export function qs(params: Record<string, string | number | boolean | undefined | null>): string {
  const search = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined || v === null) continue;
    search.append(k, String(v));
  }
  const s = search.toString();
  return s ? `?${s}` : '';
}

/** Helper for endpoints that simply echo back JSON without strict validation. */
export const PassthroughSchema = z.unknown();
