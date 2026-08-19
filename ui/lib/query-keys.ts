/** Centralised TanStack Query keys so cache invalidation stays consistent. */
export const queryKeys = {
  dashboard: ['dashboard'] as const,
  sources: ['sources'] as const,
  source: (name: string) => ['sources', name] as const,
  decon: (limit: number) => ['decon', limit] as const,
  benchmarkCoverage: ['benchmark-coverage'] as const,
  asOf: (ts: string) => ['as-of', ts] as const,
  dataset: (query: string) => ['dataset', query] as const,
  documents: (query = '') => ['documents', query] as const,
  documentFacets: (includeFixtures: boolean) => ['document-facets', includeFixtures] as const,
  document: (docId: string) => ['documents', docId] as const,
  mixture: (a: string, b: string) => ['mixture', a, b] as const,
  foundry: ['foundry'] as const,
  foundryJob: (jobId: string) => ['foundry', 'job', jobId] as const,
  foundryArtifacts: ['foundry', 'artifacts'] as const,
  foundryArtifactInspection: (artifactId: string) =>
    ['foundry', 'artifacts', artifactId, 'inspection'] as const,
} as const;
