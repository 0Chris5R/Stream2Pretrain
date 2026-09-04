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
export const SourceFeedProtocols = ['rss', 'atom', 'oai-pmh', 'rest-json', 'manual'] as const;

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
  license_default: z.literal('per-record').default('per-record'),
});

export type SourceFeedSpec = z.infer<typeof SourceFeedSpecSchema>;

export const SourceFeedStatusSchema = z.object({
  name: z.string(),
  spec: SourceFeedSpecSchema,
  last_success_at: z.string().nullable(),
  last_attempt_at: z.string().nullable(),
  last_error: z.string().nullable(),
  documents_24h: z.number().int().nonnegative().default(0),
  pretrain_documents_24h: z.number().int().nonnegative().default(0),
  posttrain_only_documents_24h: z.number().int().nonnegative().default(0),
  quarantined_documents_24h: z.number().int().nonnegative().default(0),
  license_distribution: z
    .array(
      z.object({
        license_id: z.string(),
        status: z.enum(['admitted', 'posttrain_transform_only', 'quarantined']),
        count: z.number().int().nonnegative(),
      }),
    )
    .default([]),
  license_provenance: z
    .array(
      z.object({
        license_source: z.string(),
        count: z.number().int().nonnegative(),
      }),
    )
    .default([]),
  error_rate_24h: z.number().min(0).max(1).default(0),
  poll_state: z.enum(['idle', 'polling', 'cooldown', 'error']),
  management: z.enum(['sourcefeed', 'builtin']).default('sourcefeed'),
  quality_policy: z.string().default('Source-aware after extraction'),
  license_resolver: z.string().default('Item evidence required'),
  stages: z.array(z.string()).default([]),
  supports_run: z.boolean().default(false),
});

export type SourceFeedStatus = z.infer<typeof SourceFeedStatusSchema>;

export const RuntimeProfileSchema = z.object({
  status: z.literal('ok'),
  local_mode: z.boolean(),
  source_control_plane: z.enum(['kubernetes', 'local-sourcefeed-scheduler']),
  mixture_backend: z.enum(['controller', 'future-work']),
});

export type RuntimeProfile = z.infer<typeof RuntimeProfileSchema>;

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
  available_content_tags: z.array(z.string()),
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
    license_policy: z.literal('strict_allowlist'),
    allowed_licenses: z.array(z.string()),
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
  'pretrain',
  'broad_pretraining',
  'posttrain_candidate',
  'reasoning_candidate',
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
  quality_classifier_revision: z.string().nullable().default(null),
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
  perplexity: z.number(),
  risk_tier: z.number().int(),
  route: CorpusRouteSchema,
  training_usage: z.enum(['pretrain_and_posttrain', 'posttrain_transform_only', 'quarantined']),
  admission_only: z.boolean().default(false),
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
  next_cursor: z.string().nullable().optional(),
  has_more: z.boolean().optional(),
});

export const DocumentFacetsSchema = z.object({
  sources: z.array(z.string()),
  source_formats: z.array(z.string()),
  content_tags: z.array(z.string()),
  rejection_reasons: z.array(z.string()),
});

const LicenseAdmissionAuditSchema = z.object({
  status: z.enum(['admitted', 'posttrain_transform_only', 'quarantined']),
  license_id: z.string(),
  license_source: z.string(),
  reason: z.string(),
  raw_license: z.string().nullable(),
  normalized_license: z.string().nullable(),
  resolver: z.string().nullable(),
  evidence_url: z.string().nullable(),
  evidence_revision: z.string().nullable(),
  evidence_scope: z.string().nullable(),
  policy_revision: z.string().nullable(),
  resolved_at: z.string().nullable(),
});

export const DocumentDetailSchema = DocumentSummarySchema.omit({ text_preview: true }).extend({
  admission_only: z.literal(false).default(false),
  text: z.string(),
  pii_flags: z.array(z.string()),
  metadata_pii_flags: z.array(z.string()),
  removed_body_pii_flags: z.array(z.string()),
  pii_action: z.string(),
  pii_scanner_revision: z.string(),
  extraction_pipeline: z.string(),
  classifier_revision: z.string(),
  classifier_backend: z.string(),
  scoring_version: z.string(),
  policy_revision: z.string(),
  license: z.string(),
  license_source: z.string(),
  spdx_license: z.string().nullable(),
  spdx_license_source: z.string(),
  license_admission: LicenseAdmissionAuditSchema.nullable(),
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
  quality_diagnostics: z.object({
    mode: z.enum(['diagnostic', 'active']),
    cutoff: z.number().optional(),
    passed: z.boolean().optional(),
    score: z.number(),
    confidence: z.number().nullable(),
    class: z.number(),
    model_revision: z.string(),
    aggregation: z.string(),
    bundle_revision: z.string().optional(),
    classifiers: z.record(z.string(), z.object({
      mode: z.enum(['diagnostic', 'active']), score: z.number(), class: z.number(),
      confidence: z.number().nullable(), aggregation: z.string(),
      weighted_mean: z.number(), mean: z.number(), best_section_id: z.string(),
      model_revision: z.string(), sections: z.number(), class_5_sections: z.number(),
    })).default({}),
    sections: z.array(z.object({
      section_id: z.string(), title: z.string(), section_type: z.string(),
      score: z.number(), confidence: z.number().nullable(),
      class: z.number().nullable(), probabilities: z.array(z.number()),
      tokens: z.number(), chunks: z.number(), model_revision: z.string(),
      text: z.string().optional(),
      classifiers: z.record(z.string(), z.object({
        edu_score: z.number(), confidence: z.number().nullable(),
        score_class: z.number().nullable(), probabilities: z.array(z.number()),
        tokens: z.number(), chunks: z.number(), model_revision: z.string(),
      })).default({}),
    })),
  }).nullable().default(null),
  projection_version: z.string(),
  excluded_sections: z.array(z.string()),
  scientific_artifact: ScientificDocumentSchema.nullable(),
});

export const AdmissionOnlyDocumentSchema = z.object({
  admission_only: z.literal(true),
  doc_id: z.string(),
  title: z.string(),
  source_url: z.string(),
  source_feed: z.string(),
  source_format: z.string(),
  valid_from: z.string(),
  route: z.literal('quarantine'),
  training_usage: z.literal('quarantined'),
  content_tags: z.array(z.string()),
  reject_reasons: z.array(z.string()),
  license_admission: LicenseAdmissionAuditSchema,
});

export const DocumentDetailResponseSchema = z.union([
  DocumentDetailSchema,
  AdmissionOnlyDocumentSchema,
]);

export type DocumentSummary = z.infer<typeof DocumentSummarySchema>;
export type CuratedDocumentDetail = z.infer<typeof DocumentDetailSchema>;
export type DocumentDetail = z.infer<typeof DocumentDetailResponseSchema>;
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

export const FoundryQuotaSchema = z.object({
  provider: z.literal('hetzner'),
  window: z.enum(['minute', 'day']),
  observed_requests_used: z.number().int().nonnegative(),
  observed_input_used: z.number().int().nonnegative(),
  observed_output_used: z.number().int().nonnegative(),
  locally_reserved_requests: z.number().int().nonnegative(),
  locally_reserved_input: z.number().int().nonnegative(),
  locally_reserved_output: z.number().int().nonnegative(),
  estimated_remaining_requests: z.number().int().nonnegative().nullable(),
  estimated_remaining_input: z.number().int().nonnegative().nullable(),
  estimated_remaining_output: z.number().int().nonnegative().nullable(),
  reset_at: z.string(),
  confidence: z.enum(['provider_reported', 'local_exact', 'local_estimate']),
});

export const FoundryModelSchema = z.object({
  provider: z.literal('hetzner'),
  discovered_at: z.string(),
  response_hash: z.string(),
  models: z.array(z.record(z.string(), z.unknown())),
  configured_model_ids: z.array(z.string()),
  drifted: z.boolean(),
  previous_response_hash: z.string().nullable(),
});

export const FoundryJobSummarySchema = z.object({
  job_id: z.string(),
  paper_id: z.string(),
  doc_id: z.string(),
  state: z.string(),
  reason: z.string().nullable(),
  received_at: z.string(),
  updated_at: z.string(),
});

export const FoundryValidationSchema = z.object({
  task_id: z.string(),
  positive_pass: z.boolean(),
  equivalent_pass: z.boolean(),
  adversarial_pass: z.boolean(),
  mutation_killed: z.number().int().nonnegative(),
  mutation_total: z.number().int().nonnegative(),
  metamorphic_pass: z.boolean(),
  replay_pass: z.boolean(),
  security_pass: z.boolean(),
  false_positive_count: z.number().int().nonnegative(),
  false_negative_count: z.number().int().nonnegative(),
  details: z.record(z.string(), z.unknown()),
});

export const ArtifactAuditSchema = z.object({
  audit_id: z.string(),
  artifact_id: z.string(),
  job_id: z.string(),
  decision: z.enum(['approved', 'rejected']),
  reviewer: z.string(),
  reason: z.string().nullable(),
  created_at: z.string(),
});

export const FoundryArtifactSchema = z.object({
  artifact_id: z.string(),
  job_id: z.string(),
  paper_id: z.string(),
  task_id: z.string(),
  family: z.string(),
  kind: z.enum(['sft_trajectory', 'rl_environment']),
  status: z.enum(['accepted', 'rejected', 'deprecated']),
  quality_label: z.string(),
  pool: z.enum(['sft', 'rl']),
  dataset_split: z.enum(['train', 'benchmark', 'none']),
  package_uri: z.string().nullable(),
  signature_uri: z.string().nullable(),
  signature_backend: z.string().nullable(),
  signer_cert_hash: z.string().nullable(),
  package_hash: z.string(),
  environment_hash: z.string(),
  paper_hash: z.string(),
  provider_trace_ids: z.array(z.string()),
  constructor_family: z.string(),
  critic_family: z.string(),
  validation: FoundryValidationSchema,
  created_at: z.string(),
  deprecated_at: z.string().nullable(),
  deprecation_reason: z.string().nullable(),
  human_audit: ArtifactAuditSchema.nullable(),
  human_audit_history: z.array(ArtifactAuditSchema),
});

const FoundryTaskSpecSchema = z
  .object({
    task_id: z.string(),
    paper_id: z.string(),
    family: z.string(),
    public_instruction: z.string(),
    public_context_policy: z.object({
      included_spans: z.array(z.string()),
      same_paper_distractors: z.array(z.string()),
      tool_access: z.array(z.string()),
    }),
    hidden_targets: z.record(z.string(), z.unknown()),
    answer_contract: z.string(),
    verifier_class: z.string(),
    difficulty: z.object({
      estimated: z.number().int(),
      sources: z.array(z.string()),
    }),
    reasoning_operations: z.array(z.string()),
    ambiguity_risks: z.array(z.string()),
    construction_provenance: z.array(z.string()),
    route: z.enum(['sft', 'rl', 'reject']),
  })
  .passthrough();

const FoundryAnswerSchema = z
  .object({
    report: z.string(),
    answer_manifest: z.record(z.string(), z.unknown()),
  })
  .passthrough();

const FoundryTrajectorySchema = z
  .object({
    trajectory_id: z.string(),
    task_id: z.string(),
    provider_trace_id: z.string(),
    provider_trace_ids: z.array(z.string()),
    answer: FoundryAnswerSchema,
    tool_calls: z.array(z.unknown()),
    turns: z.array(z.unknown()),
    accepted: z.boolean(),
    reward: z.number(),
    validation: z.record(z.string(), z.unknown()),
    loss_masked_turns: z.array(z.number().int()),
  })
  .passthrough();

export const FoundryArtifactInspectionSchema = z.object({
  artifact: FoundryArtifactSchema,
  source: z.enum(['package', 'durable_cache']),
  package_available: z.boolean(),
  package_error: z.string().nullable(),
  task: FoundryTaskSpecSchema.nullable(),
  prompt: z.record(z.string(), z.unknown()).nullable(),
  public_context: z.object({
    paper_text: z.string(),
    spans: z.array(z.record(z.string(), z.unknown())),
    equations: z.array(z.unknown()),
    tables: z.array(z.unknown()),
    figures: z.array(z.unknown()),
  }),
  evidence_graph: z.record(z.string(), z.unknown()).nullable(),
  trajectories: z.array(FoundryTrajectorySchema),
  verifier: z.record(z.string(), z.unknown()).nullable(),
  validation: z.object({
    report: FoundryValidationSchema.nullable(),
    valid: z.array(z.unknown()),
    equivalent: z.array(z.unknown()),
    adversarial: z.array(z.unknown()),
    mutations: z.array(z.unknown()),
    metamorphic: z.array(z.unknown()),
  }),
  manifest: z.record(z.string(), z.unknown()).nullable(),
  provenance: z.array(z.record(z.string(), z.unknown())),
  files: z.array(
    z.object({
      path: z.string(),
      size: z.number().int().nonnegative(),
      category: z.string(),
    }),
  ),
  generation_attempts: z.array(
    z.object({
      call_key: z.string(),
      stage: z.enum(['solution', 'grounding_review', 'verifier', 'repair']),
      response: z.unknown(),
      trace: z.record(z.string(), z.unknown()),
      created_at: z.string(),
    }),
  ),
});

export const FoundryEventSchema = z.object({
  event_id: z.string(),
  job_id: z.string(),
  paper_id: z.string(),
  sequence: z.number().int().nonnegative(),
  state: z.string(),
  occurred_at: z.string(),
  attempt: z.number().int().positive(),
  idempotency_key: z.string(),
  provider_trace_id: z.string().nullable(),
  artifact_hash: z.string().nullable(),
  reason: z.string().nullable(),
  metadata: z.record(z.string(), z.unknown()),
});

export const FoundryProviderTraceSchema = z
  .object({
    trace_id: z.string(),
    provider: z.enum(['hetzner', 'replay']),
    role: z.string(),
    requested_model: z.string(),
    returned_model: z.string(),
    model_family: z.string(),
    input_tokens: z.number().int().nonnegative(),
    output_tokens: z.number().int().nonnegative(),
    request_attempts: z.number().int().positive(),
    latency_ms: z.number().int().nonnegative(),
    completed_at: z.string(),
  })
  .passthrough();

export const FoundryDashboardSchema = z.object({
  jobs: z.record(z.string(), z.number().int().nonnegative()),
  artifacts: z.record(z.string(), z.number().int().nonnegative()),
  families: z.record(z.string(), z.number().int().nonnegative()),
  splits: z.record(z.string(), z.number().int().nonnegative()),
  providers: z.record(
    z.string(),
    z.object({
      calls: z.number().int().nonnegative(),
      input_tokens: z.number().int().nonnegative(),
      output_tokens: z.number().int().nonnegative(),
    }),
  ),
  provider_statuses: z.record(
    z.string(),
    z.object({
      state: z.string(),
      reason: z.string().nullable(),
      occurred_at: z.string(),
    }),
  ),
  stages: z.record(z.string(), z.number().int().nonnegative()),
  recent_jobs: z.array(FoundryJobSummarySchema),
  quotas: z.array(FoundryQuotaSchema),
  models: z.array(FoundryModelSchema),
  human_audits: z.record(z.string(), z.number().int().nonnegative()),
  daily_run_hour_utc: z.number().int().min(0).max(23),
  daily_run_minute_utc: z.number().int().min(0).max(59),
  queued_candidates: z.number().int().nonnegative(),
  daily_runs: z.array(
    z.object({
      run_date: z.string(),
      state: z.string(),
      cutoff_at: z.string(),
      started_at: z.string(),
      completed_at: z.string().nullable(),
      candidate_count: z.number().int().nonnegative(),
      processed_count: z.number().int().nonnegative(),
      stop_reason: z.string().nullable(),
    }),
  ),
  manual_runs: z.array(
    z.object({
      run_id: z.string(),
      state: z.string(),
      cutoff_at: z.string(),
      requested_at: z.string(),
      started_at: z.string().nullable(),
      completed_at: z.string().nullable(),
      candidate_count: z.number().int().nonnegative(),
      max_candidates: z.number().int().positive().nullable().optional(),
      processed_count: z.number().int().nonnegative(),
      stop_reason: z.string().nullable(),
    }),
  ),
});

const FoundryCallCountsSchema = z.object({
  started: z.number().int().nonnegative(),
  succeeded: z.number().int().nonnegative(),
  failed: z.number().int().nonnegative(),
  rate_limited: z.number().int().nonnegative(),
});

const FoundryTokenCountsSchema = z.object({
  input: z.number().int().nonnegative(),
  output: z.number().int().nonnegative(),
});

const FoundryStageCountsSchema = z.object({
  received: z.number().int().nonnegative(),
  graph_compiled: z.number().int().nonnegative(),
  graph_critiqued: z.number().int().nonnegative(),
  tasks_proposed: z.number().int().nonnegative(),
  solutions_generated: z.number().int().nonnegative(),
  verifiers_compiled: z.number().int().nonnegative(),
  adversarial_validated: z.number().int().nonnegative(),
});

export const FoundryActivitySchema = z.object({
  window: ActivityWindowSchema,
  start: z.string(),
  end: z.string(),
  bucket_seconds: z.number().int().positive(),
  totals: z.object({
    calls: FoundryCallCountsSchema,
    tokens: FoundryTokenCountsSchema,
    stages: FoundryStageCountsSchema,
  }),
  points: z.array(
    z.object({
      ts: z.string(),
      calls: FoundryCallCountsSchema,
      tokens: FoundryTokenCountsSchema,
      stages: FoundryStageCountsSchema,
    }),
  ),
  active_calls: z.array(
    z.object({
      job_id: z.string(),
      paper_id: z.string(),
      call_key: z.string(),
      role: z.string(),
      provider: z.string(),
      attempt: z.number().int().positive(),
      started_at: z.string(),
      checkpoint_at: z.string().nullable(),
      partial_characters: z.number().int().nonnegative(),
      estimated_input_tokens: z.number().int().nonnegative(),
      max_output_tokens: z.number().int().nonnegative(),
    }),
  ),
});

export const FoundryManualRunResponseSchema = z.object({
  run: FoundryDashboardSchema.shape.manual_runs.element,
  created: z.boolean(),
});

export const FoundryArtifactAuditResponseSchema = z.object({
  audit: ArtifactAuditSchema,
});

export const FoundryArtifactListSchema = z.object({
  items: z.array(FoundryArtifactSchema),
});

export const FoundryJobDetailSchema = z.object({
  job_id: z.string(),
  idempotency_key: z.string(),
  paper_id: z.string(),
  paper_hash: z.string(),
  doc_id: z.string(),
  state: z.string(),
  reason: z.string().nullable(),
  received_at: z.string(),
  updated_at: z.string(),
  events: z.array(FoundryEventSchema),
  artifacts: z.array(FoundryArtifactSchema),
  provider_traces: z.array(FoundryProviderTraceSchema),
});

export type FoundryDashboard = z.infer<typeof FoundryDashboardSchema>;
export type FoundryActivity = z.infer<typeof FoundryActivitySchema>;
export type FoundryArtifact = z.infer<typeof FoundryArtifactSchema>;
export type FoundryArtifactInspection = z.infer<typeof FoundryArtifactInspectionSchema>;
export type FoundryJobDetail = z.infer<typeof FoundryJobDetailSchema>;
export type FoundryJobSummary = z.infer<typeof FoundryJobSummarySchema>;
