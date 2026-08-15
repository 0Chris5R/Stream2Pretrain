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
 * (`oai_pmh`, `rest_json`) are NOT accepted by the Gatekeeper admission
 * policy. Keep in sync.
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

export const RuntimeProfileSchema = z.object({
  status: z.literal('ok'),
  local_mode: z.boolean(),
  source_control_plane: z.enum(['kubernetes', 'local-sourcefeed-scheduler']),
  mixture_backend: z.enum(['controller', 'future-work']),
});

export type RuntimeProfile = z.infer<typeof RuntimeProfileSchema>;

export const DeconAttestationSchema = z.object({
  snapshot_id: z.string().regex(/^\d+$/),
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

export const BenchmarkCoverageSchema = z.object({
  benchmark_set_version: z.string(),
  manifest_sha256: z.string(),
  corpus_kind: z.enum(['demo_canaries', 'restricted_reserve']),
  item_count: z.number().int().nonnegative(),
  per_benchmark_items: z.record(z.string(), z.number().int().nonnegative()),
  non_empty_benchmarks: z.array(z.string()),
  latest_snapshot_id: z.string().nullable(),
  last_successful_scan: z.string().nullable(),
  tokens_scanned: z.number().int().nonnegative(),
  tokens_flagged: z.number().int().nonnegative(),
});

export type BenchmarkCoverage = z.infer<typeof BenchmarkCoverageSchema>;

export const MixtureSourceWeightSchema = z.object({
  // Field name mirrors Pydantic `MixtureSourceWeight.source_feed`. The
  // SourceFeed REST API has `extra='forbid'` so the legacy `source` key is
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

export const ActivityWindowSchema = z.enum(['5m', '1h', '24h']);
export type ActivityWindow = z.infer<typeof ActivityWindowSchema>;

export const ActivityPointSchema = z.object({
  ts: z.string(),
  fetched: z.number().nonnegative(),
  extracted: z.number().nonnegative(),
  decided: z.number().nonnegative(),
  training: z.number().nonnegative(),
});

export const ActivitySummarySchema = z.object({
  window: ActivityWindowSchema,
  start: z.string(),
  end: z.string(),
  bucket_seconds: z.number().int().positive(),
  totals: z.object({
    fetched: z.number().nonnegative(),
    extracted: z.number().nonnegative(),
    decided: z.number().nonnegative(),
    training: z.number().nonnegative(),
  }),
  points: z.array(ActivityPointSchema),
});

export type ActivityPoint = z.infer<typeof ActivityPointSchema>;
export type ActivitySummary = z.infer<typeof ActivitySummarySchema>;

export const QualityHistogramSchema = z.object({
  buckets: z.array(
    z.object({
      score: z.number(),
      count: z.number().int().nonnegative(),
    }),
  ),
  edu_buckets: z.array(
    z.object({
      score: z.number(),
      count: z.number().int().nonnegative(),
    }),
  ),
});

export type QualityHistogram = z.infer<typeof QualityHistogramSchema>;

export const DashboardSummarySchema = z.object({
  durable_decisions: z.number().int().nonnegative(),
  training_export_documents: z.number().int().nonnegative(),
  rejected_by_reason: z.record(z.string(), z.number().int().nonnegative()),
  per_source_acceptance: z.array(
    z.object({
      source: z.string(),
      accepted: z.number().int().nonnegative(),
      total: z.number().int().nonnegative(),
    }),
  ),
  quality_histogram: QualityHistogramSchema,
  route_distribution: z.array(
    z.object({
      route: z.string(),
      documents: z.number().int().nonnegative(),
      source_words: z.number().int().nonnegative(),
      training_words: z.number().int().nonnegative(),
      mean_quality: z.number(),
      mean_edu: z.number(),
    }),
  ),
});

export type DashboardSummary = z.infer<typeof DashboardSummarySchema>;

export const VerifyResultSchema = z.object({
  ok: z.boolean(),
  snapshot_id: z.string().regex(/^\d+$/),
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

export const DatasetSummarySchema = z.object({
  documents: z.number().int().nonnegative(),
  tokens: z.number().int().nonnegative(),
  source_words: z.number().int().nonnegative(),
  projection_words: z.number().int().nonnegative(),
  source_count: z.number().int().nonnegative(),
  selection: z.object({
    date_from: z.string(),
    date_to: z.string(),
    routes: z.array(z.string()),
    sources: z.array(z.string()),
    source_formats: z.array(z.string()),
    content_tags: z.array(z.string()),
    min_edu: z.number().nullable(),
    min_quality: z.number().nullable(),
    include_structured: z.boolean(),
    fixtures_included: z.literal(false),
  }),
  manifest: z.object({
    revisions: z.record(z.string(), z.array(z.string())),
    decision_table: z.object({
      namespace: z.string(),
      table: z.string(),
      snapshot_id: z.string().regex(/^\d+$/).nullable(),
      metadata_location: z.string().nullable(),
    }),
    export_limit: z.number().int().positive(),
  }),
});

export type DatasetSummary = z.infer<typeof DatasetSummarySchema>;

export const CorpusRouteSchema = z.enum([
  'broad_pretraining',
  'reasoning_candidate',
  'benchmark_candidate',
  'quarantine',
  'retry',
]);

export type CorpusRoute = z.infer<typeof CorpusRouteSchema>;

export const ScientificFigureSchema = z.object({
  figure_id: z.string(),
  source_element_id: z.string().nullable(),
  source_url: z.string(),
  asset_s3_uri: z.string().nullable(),
  image_sha256: z.string().nullable(),
  mime_type: z.string().nullable(),
  width: z.number().int().positive().nullable(),
  height: z.number().int().positive().nullable(),
  caption: z.string().nullable(),
  alt_text: z.string().nullable(),
  nearby_text: z.string().nullable(),
  page_number: z.number().int().positive().nullable(),
  bbox: z.tuple([z.number(), z.number(), z.number(), z.number()]).nullable(),
  figure_type: z.string(),
  figure_type_confidence: z.number().min(0).max(1).nullable(),
  classifier_revision: z.string().nullable(),
  ocr_text: z.string().nullable(),
  ocr_revision: z.string().nullable(),
  ocr_training_eligible: z.boolean().default(false),
  ocr_quality_score: z.number().min(0).max(1).nullable().default(null),
  ocr_policy_revision: z.string().nullable().default(null),
  warnings: z.array(z.string()),
});

export const ScientificDocumentSchema = z.object({
  schema_version: z.string(),
  doc_id: z.string(),
  source_url: z.string(),
  title: z.string().nullable(),
  authors: z.array(z.string()),
  author_metadata: z.array(z.string()),
  abstract: z.string().nullable(),
  source_identifier: z.string().nullable(),
  publication_date: z.string().nullable(),
  license: z.string().nullable(),
  text_sha256: z.string(),
  extraction_pipeline: z.string(),
  projection_version: z.string(),
  source_word_count: z.number().int().nonnegative(),
  training_word_count: z.number().int().nonnegative(),
  included_section_count: z.number().int().nonnegative(),
  excluded_section_count: z.number().int().nonnegative(),
  excluded_sections: z.array(z.string()),
  raw_extractor_s3_uri: z.string().nullable(),
  sections: z.array(
    z.object({
      section_id: z.string(),
      level: z.number().int(),
      title: z.string(),
      text: z.string(),
      role: z.string(),
      include_in_training: z.boolean(),
      exclusion_reason: z.string().nullable(),
      word_count: z.number().int().nonnegative(),
      paragraphs: z.array(
        z.object({
          paragraph_id: z.string(),
          text: z.string(),
          include_in_training: z.boolean(),
          exclusion_reason: z.string().nullable(),
        }),
      ),
    }),
  ),
  equations: z.array(
    z.object({
      equation_id: z.string(),
      latex: z.string().nullable(),
      mathml: z.string().nullable(),
      display: z.boolean(),
      page_number: z.number().int().positive().nullable(),
      bbox: z.tuple([z.number(), z.number(), z.number(), z.number()]).nullable(),
    }),
  ),
  tables: z.array(
    z.object({
      table_id: z.string(),
      caption: z.string().nullable(),
      rows: z.array(z.array(z.string())),
      footnotes: z.array(z.string()),
      page_number: z.number().int().positive().nullable(),
      bbox: z.tuple([z.number(), z.number(), z.number(), z.number()]).nullable(),
    }),
  ),
  figures: z.array(ScientificFigureSchema),
  citations: z.array(
    z.object({
      citation_id: z.string(),
      text: z.string(),
      target: z.string().nullable(),
    }),
  ),
  warnings: z.array(z.string()),
});

export const SegmentScoreSchema = z.object({
  segment_id: z.string(),
  title: z.string(),
  role: z.string(),
  word_count: z.number().int().nonnegative(),
  edu_score: z.number().min(0).max(5).nullable(),
  finepdfs_edu_score: z.number().min(0).max(5).nullable().default(null),
  fineweb_edu_score: z.number().min(0).max(5).nullable().default(null),
  quality_classifier_revision: z.string().nullable().default(null),
  comparison_classifier_revision: z.string().nullable().default(null),
  perplexity: z.number().nonnegative().nullable(),
  perplexity_bucket: z.enum(['head', 'middle', 'tail']).nullable(),
  c4_pass: z.boolean(),
  pii_flags: z.array(z.string()),
  decision: z.enum(['included', 'excluded']),
  exclusion_reasons: z.array(z.string()),
});

export const DocumentSummarySchema = z.object({
  doc_id: z.string(),
  title: z.string(),
  source_feed: z.string(),
  source_format: z.string(),
  lang: z.string(),
  valid_from: z.string(),
  quality_score: z.number(),
  edu_score: z.number(),
  structural_quality_score: z.number(),
  reasoning_score: z.number(),
  benchmark_score: z.number(),
  perplexity: z.number(),
  risk_tier: z.number().int(),
  route: CorpusRouteSchema,
  content_tags: z.array(z.string()),
  reject_reasons: z.array(z.string()),
  source_word_count: z.number().int().nonnegative(),
  training_word_count: z.number().int().nonnegative(),
  included_section_count: z.number().int().nonnegative(),
  excluded_section_count: z.number().int().nonnegative(),
  figure_count: z.number().int().nonnegative(),
  table_count: z.number().int().nonnegative(),
  equation_count: z.number().int().nonnegative(),
  citation_count: z.number().int().nonnegative(),
  scientific_artifact_s3_uri: z.string().nullable(),
  text_preview: z.string(),
});

export const DocumentPageSchema = z.object({
  items: z.array(DocumentSummarySchema),
  total: z.number().int().nonnegative(),
  page: z.number().int().positive(),
  page_size: z.number().int().positive(),
  pages: z.number().int().nonnegative(),
});

export const DocumentFacetsSchema = z.object({
  sources: z.array(z.string()),
  source_formats: z.array(z.string()),
  content_tags: z.array(z.string()),
  rejection_reasons: z.array(z.string()),
});

export const DocumentDetailSchema = DocumentSummarySchema.omit({ text_preview: true }).extend({
  text: z.string(),
  pii_flags: z.array(z.string()),
  metadata_pii_flags: z.array(z.string()),
  removed_body_pii_flags: z.array(z.string()),
  pii_action: z.string(),
  pii_scanner_revision: z.string(),
  contaminated_with: z.array(z.string()),
  extraction_pipeline: z.string(),
  classifier_revision: z.string(),
  classifier_backend: z.string(),
  scoring_version: z.string(),
  policy_revision: z.string(),
  license: z.string(),
  license_source: z.string(),
  spdx_license: z.string().nullable(),
  spdx_license_source: z.string(),
  extraction_warnings: z.array(z.string()),
  lang_score: z.number(),
  lang_detector_revision: z.string(),
  tokenizer_revision: z.string(),
  gopher_pass: z.boolean(),
  gopher_word_count: z.number().int().nonnegative(),
  gopher_mean_word_len: z.number(),
  gopher_stopword_ratio: z.number(),
  gopher_bullet_line_ratio: z.number(),
  gopher_ellipsis_line_ratio: z.number(),
  gopher_symbol_word_ratio: z.number(),
  gopher_alpha_word_ratio: z.number(),
  c4_nopunc_pass: z.boolean(),
  c4_curly_brace_pass: z.boolean(),
  c4_lorem_ipsum_pass: z.boolean(),
  c4_fraction_lines_with_punct: z.number(),
  perplexity_bucket: z.string(),
  perplexity_scorer: z.string(),
  near_duplicate: z.boolean(),
  near_dup_cluster_id: z.string().nullable(),
  minhash_backend: z.string(),
  minhash_num_perms: z.number().int(),
  lsh_backend: z.string(),
  extraction_completeness: z.number(),
  eligible_routes: z.array(CorpusRouteSchema),
  route_reasons: z.array(z.string()),
  segment_scores: z.array(SegmentScoreSchema),
  projection_version: z.string(),
  excluded_sections: z.array(z.string()),
  decon_exact_matches: z.array(z.string()),
  decon_semantic_matches: z.array(z.string()),
  decon_max_similarity: z.number(),
  decon_ngram_size: z.number().int().positive(),
  decon_embedding_revision: z.string(),
  benchmark_set_version: z.string(),
  scientific_artifact: ScientificDocumentSchema.nullable(),
});

export type DocumentSummary = z.infer<typeof DocumentSummarySchema>;
export type DocumentDetail = z.infer<typeof DocumentDetailSchema>;
export type DocumentPage = z.infer<typeof DocumentPageSchema>;
export type DocumentFacets = z.infer<typeof DocumentFacetsSchema>;

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
