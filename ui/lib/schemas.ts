/**
 * Zod schemas mirrored from `schemas/json_schema/*.schema.json`.
 *
 * Treat these as the wire-contract: they validate every payload entering or
 * leaving the cockpit. Keep them in sync by hand — generation is overkill for
 * the small surface here, and the source schemas are also hand-curated.
 */
import { z } from 'zod';

export const SourceFeedAuthSchema = z.object({
  type: z.enum(['none', 'bearer', 'header', 'basic']).default('none'),
  secret_name: z.string().nullable().optional(),
  secret_key: z.string().nullable().optional(),
  header_name: z.string().nullable().optional(),
});

export const SourceFeedRateLimitSchema = z.object({
  requests_per_second: z.number().positive(),
  burst: z.number().int().positive(),
  respect_x_poll_interval: z.boolean().default(false),
});

/**
 * Protocol enum mirrors `schemas/sourcefeed.py::SourceFeedProtocol` and the
 * generated `schemas/json_schema/source_feed_spec.schema.json`. The wire
 * uses kebab-case (`oai-pmh`, `rest-json`); the legacy underscore variants
 * (`oai_pmh`, `rest_json`, `submit_api`) are NOT accepted by the FastAPI
 * submit API or by the Gatekeeper admission policy. Keep in sync.
 */
export const SourceFeedProtocols = [
  'rss',
  'atom',
  'oai-pmh',
  'rest-json',
  'sitemap',
  'manual',
] as const;

export const SourceFeedSpecSchema = z.object({
  name: z.string().min(1).max(63),
  protocol: z.enum(SourceFeedProtocols),
  endpoint: z.string().url(),
  enabled: z.boolean().default(true),
  poll_interval_seconds: z.number().int().positive(),
  rate_limit: SourceFeedRateLimitSchema,
  auth: SourceFeedAuthSchema.optional(),
  accept_content_types: z.array(z.string()).default([]),
  egress_allow: z.array(z.string()).default([]),
  license_default: z.string().nullable().optional(),
});

export type SourceFeedSpec = z.infer<typeof SourceFeedSpecSchema>;

export const SourceFeedStatusSchema = z.object({
  name: z.string(),
  spec: SourceFeedSpecSchema,
  last_success_at: z.string().nullable(),
  last_attempt_at: z.string().nullable(),
  last_error: z.string().nullable(),
  documents_24h: z.number().int().nonnegative().default(0),
  error_rate_24h: z.number().min(0).max(1).default(0),
  poll_state: z.enum(['idle', 'polling', 'cooldown', 'error']),
});

export type SourceFeedStatus = z.infer<typeof SourceFeedStatusSchema>;

export const DeconAttestationSchema = z.object({
  snapshot_id: z.number().int().nonnegative(),
  committed_at: z.string(),
  benchmark_set_version: z.string(),
  benchmarks: z.array(z.enum(['MMLU', 'GSM8K', 'HumanEval', 'MATH', 'GPQA'])),
  per_benchmark_hits: z.record(z.string(), z.number().int().nonnegative()),
  rejected_doc_hashes: z.array(z.string()),
  tokens_scanned: z.number().int().nonnegative(),
  tokens_flagged: z.number().int().nonnegative(),
  signature: z.string(),
  signer_cert: z.string(),
});

export type DeconAttestation = z.infer<typeof DeconAttestationSchema>;

export const MixtureSourceWeightSchema = z.object({
  // Field name mirrors Pydantic `MixtureSourceWeight.source_feed`. The
  // FastAPI submit API has `extra='forbid'` so the legacy `source` key is
  // rejected on the wire. Always send `source_feed`.
  source_feed: z.string(),
  weight: z.number().gt(0).max(1),
});

export const MixtureRecipeSpecSchema = z.object({
  name: z.string().min(1).max(63),
  branch: z.string().min(1).max(63),
  sources: z.array(MixtureSourceWeightSchema).min(1),
  min_quality_score: z.number().min(0).max(5).default(2),
  min_edu_score: z.number().min(0).max(5).default(2),
  max_risk_tier: z.union([z.literal(1), z.literal(2), z.literal(3)]).default(2),
  languages: z.array(z.string()).default([]),
  target_tokens_per_hour: z.number().int().nonnegative().nullable().optional(),
});

export type MixtureRecipeSpec = z.infer<typeof MixtureRecipeSpecSchema>;

export const ThroughputPointSchema = z.object({
  ts: z.string(),
  ingested: z.number().nonnegative(),
  curated: z.number().nonnegative(),
  rejected: z.number().nonnegative(),
});

export type ThroughputPoint = z.infer<typeof ThroughputPointSchema>;

export const QualityHistogramSchema = z.object({
  buckets: z.array(
    z.object({
      score: z.number(),
      count: z.number().int().nonnegative(),
    }),
  ),
});

export type QualityHistogram = z.infer<typeof QualityHistogramSchema>;

export const DashboardSummarySchema = z.object({
  ingested_last_hour: z.number().int().nonnegative(),
  curated_last_hour: z.number().int().nonnegative(),
  rejected_by_reason: z.record(z.string(), z.number().int().nonnegative()),
  per_source_acceptance: z.array(
    z.object({
      source: z.string(),
      accepted: z.number().int().nonnegative(),
      total: z.number().int().nonnegative(),
    }),
  ),
  quality_histogram: QualityHistogramSchema,
});

export type DashboardSummary = z.infer<typeof DashboardSummarySchema>;

export const VerifyResultSchema = z.object({
  ok: z.boolean(),
  snapshot_id: z.number().int().nonnegative(),
  message: z.string(),
  signer_subject: z.string().nullable().optional(),
  verified_at: z.string(),
});

export type VerifyResult = z.infer<typeof VerifyResultSchema>;

export const AsOfMixtureRowSchema = z.object({
  source_feed: z.string(),
  tokens: z.number().int().nonnegative(),
  documents: z.number().int().nonnegative(),
});

export type AsOfMixtureRow = z.infer<typeof AsOfMixtureRowSchema>;

export const MixtureCompareSchema = z.object({
  recipe_a: z.string(),
  recipe_b: z.string(),
  perplexity_delta: z.array(
    z.object({
      step: z.number().int().nonnegative(),
      delta: z.number(),
    }),
  ),
  tokens_per_hour_a: z.number().nonnegative(),
  tokens_per_hour_b: z.number().nonnegative(),
});

export type MixtureCompare = z.infer<typeof MixtureCompareSchema>;
