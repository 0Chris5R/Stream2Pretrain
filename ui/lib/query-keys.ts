/** Centralised TanStack Query keys so cache invalidation stays consistent. */
export const queryKeys = {
  dashboard: ['dashboard'] as const,
  sources: ['sources'] as const,
  source: (name: string) => ['sources', name] as const,
  decon: (limit: number) => ['decon', limit] as const,
  asOf: (ts: string) => ['as-of', ts] as const,
  mixture: (a: string, b: string) => ['mixture', a, b] as const,
} as const;
