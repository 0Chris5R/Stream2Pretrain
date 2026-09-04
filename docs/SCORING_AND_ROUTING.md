# Scoring and routing contract

Status: implementation contract
Policy implementation: `processor/scientific_policy.py` and `processor/curate.py`
Current policy id: `S2P_POLICY_REVISION`
Current scoring id: `S2P_SCORING_VERSION`, default `pretrain-content-v3`

This document defines every score shown in the Stream2Pretrain UI. A model
score, a deterministic evidence score, and a composite policy score are
different signals and must not share a label.

## 1. Evaluation unit

Scientific sources are not scored as one flattened paper.

1. The extractor removes author and affiliation blocks, acknowledgements,
   declarations, and references from the trainable projection.
2. Every retained heading-delimited section becomes a `SilverSegment`.
3. PII checks run on every retained section. C4 section isolation runs only on
   ordinary web prose.
4. Existing deterministic rejection checks run before model inference.
   The source-specific ModernBERT quality head then runs on every retained section of eligible papers and
   Hugging Face cards. KenLM runs only where its source policy enables it.
5. The final model text is rebuilt from sections that survived section-level
   safety checks.
6. Source-authored captions, alt text, structured tables, and display
   equations may be appended. Raw OCR is audit-only unless an OCR policy
   explicitly sets `ocr_training_eligible=true`.
7. Deduplication and final PII scanning run on the
   exact final projection that could be exported.

## 2. Section coverage

Every retained section is scored in full. `quality_diagnostics_json` retains
section-level predictions, input identities, model digests and aggregation.
The complete contract is in [Classifiers](CLASSIFIERS.md).

## 3. Model signals

### 3.1 Learned quality and suitability

Independent arXiv and HF ModernBERT-base quality models score every retained
section. Overlength sections use all 8,192-token windows with 512-token
overlap. Window logits are averaged before softmax; six ordinal probabilities
produce a continuous 0-5 score and entropy-based confidence.

The document quality is the token-weighted mean of its retained sections.
arXiv requires >=3.0; HF requires >=3.5. Confidence is not an admission gate.
A failed quality gate prevents arXiv auxiliary inference and Foundry admission.

Quality-passing arXiv papers receive mathematical-reasoning and post-training
suitability scores on every retained section. Mean suitability ranks the daily
queue. High-scoring section titles can add optional task-designer hints;
the paper input remains intact. Maxima do not gate admission.

### 3.2 KenLM typicality

The pinned English Wikipedia `edugp/kenlm` binary uses its paired SentencePiece
model. It applies only to ordinary web prose. Per-section perplexity is
aggregated by weighted median with the same bounded word weights.

Buckets are:

- head: perplexity at most 200;
- middle: above 200 and at most 1,000;
- tail: above 1,000.

A document is blocked for high perplexity only when at least 75 percent of its
measured retained sections are in the tail and the weighted median is above
2,000. KenLM measures language-model typicality. It is not a correctness or
scientific-value score.

Scientific papers and cards, along with
discovery metadata do not receive a KenLM value. Their persisted scorer is
`not-applicable`, and typicality is removed from the composite instead of
substituting an artificial value. Gopher and C4 are hard gates only for
ordinary web prose. Every trainable profile
retains privacy, licence, exact/near-duplicate, and validity checks.

### 3.3 Language, privacy, and deduplication

- FastLangID must select English with confidence at least 0.5.
- Presidio plus the regex/Luhn pack redacts matched contact data in place,
  preserving surrounding section text. High-risk identifiers are also redacted
  from the audit projection and quarantine the record even after redaction.
  Structured text and the rebuilt projection are checked as well.
- MinHash uses 112 permutations. The LSH/Bloom backend treats any matching LSH
  band as a candidate, then confirms at least 0.80 estimated MinHash similarity
  against the durable anchor signature. State is retained across worker
  restarts and namespaced by scoring generation.
Strict laptop and Kubernetes profiles set `S2P_REQUIRE_REAL_MODELS=1` and fail
startup if ModernBERT, KenLM/SentencePiece, Presidio, Rensa,
Plyvel, or the required tokenizer artifacts cannot load.

## 4. Deterministic evidence scores

### 4.1 Extraction completeness, 0 to 1

The current baseline adds:

- 0.12 for a title;
- 0.18 for an abstract;
- 0.20 for at least three retained sections, otherwise 0.10 for any section;
- 0.20 for at least 500 retained words, otherwise 0.10 for at least 100;
- 0.12 for at least one evidence kind among equation, table, and figure;
- 0.08 for at least one citation;
- 0.10 when extraction has no warnings, otherwise 0.03.

The sum is clipped to 1.

### 4.2 Scientific structure, 0 to 5

`5 * (0.55 * completeness + 0.25 * role_coverage + 0.20 * evidence_coverage)`

`role_coverage` is the fraction present among abstract, methods, and
results/discussion/conclusion. `evidence_coverage` is the fraction present
among equations, tables, and figures.

### 4.3 Reasoning evidence, 0 to 1

`0.22 * methods + 0.24 * results + 0.14 * equations + 0.12 * tables + 0.08 *
figures + 0.10 * structure/5 + 0.10 * educational_quality/5`

Binary terms are 1 when present and 0 otherwise. This score indicates visible
reasoning-supporting anatomy. It does not claim that a model has judged the
reasoning correct.

### 4.4 Post-training dataset allocation

Pretraining does not score or reserve papers for an evaluation split. After a
paper has produced accepted SFT or RL artifacts, the foundry assigns its paper
family within that output pool: four families to `train`, then one to the
post-training evaluation split.

### 4.5 Non-scientific structure

HF cards do not receive paper-anatomy credit. Their completeness is
`0.15*has_title + 0.55*min(1, words/500) + 0.20*clean_extraction +
0.10*nonempty_text`, clipped to 1. Structure is five times completeness.
Their content tags identify model or dataset documentation and the
deterministic card assessment.

## 5. Composite quality, 0 to 5

The base weights are educational quality 0.35 and source-appropriate structure
0.25. Natural-language profiles add language confidence 0.15. Ordinary web additionally contributes
heuristic pass rate 0.15 and KenLM typicality 0.10. The active weights are
renormalized to sum to one before multiplication by 5.

`heuristic_pass_rate` is the mean of Gopher pass and C4 pass. Typicality maps
head to 1.0, middle to 0.72, and tail to 0.25. Non-applicable signals contribute
neither a value nor weight. This composite supports sorting and dashboards. It
is never presented as an individual model output.

## 6. Blocking rules and route precedence

Blocking reasons are applied before corpus-use routes:

1. Any blocking reason other than insufficient body routes to `quarantine`.
2. Insufficient retained scientific body routes to `retry` when it is the only
   problem.
3. Otherwise the record is eligible for `pretrain`.
4. Reasoning evidence at least 0.55 adds `posttrain_candidate` eligibility and
   makes it the primary inspection route; the record remains pretraining-eligible.
5. All other clean records keep `pretrain` as their primary route.

Current curation emits only the route values listed above.

Source quality additionally gates arXiv at 3.0 and HF at 3.5. Licence handling
then removes verbatim pretraining eligibility from transform-only records.
Candidate publication requires durable scientific evidence; missing evidence
cannot turn a valid permissive pretraining projection into post-training data.

## 7. Content taxonomy

All tags are currently deterministic and multi-label:

- `mathematical_reasoning`: at least three equations;
- `empirical_evidence`: results/discussion, a table, or a figure;
- `methods_and_procedures`: a methods section;
- `benchmark_or_dataset`: title or headings mention benchmark, dataset, corpus,
  or evaluation;
- `survey_synthesis`: title or headings mention survey, review, or overview;
- `systems_implementation`: title or headings mention system, pipeline,
  architecture, or infrastructure;
- `visual_evidence`: at least one figure;
- `general_scientific`: fallback when no other tag fires.

The dataset UI shows nonzero tags from the actual corpus and uses a single
content-tag filter. A trained topic tagger is separate future work, not one of
the four scalar classifiers.

## 8. Evaluation

Review held-out sections and documents across each source's quality range.
Report MAE, weighted kappa, rank correlation, threshold precision/recall,
and confusion matrices. Evaluate OCR character/word error and numerical
exact match separately from text usefulness. Split and bootstrap by document.

Threshold or weight changes require a policy/scoring revision. Runtime
throughput, memory and daily storage claims need deployed measurements.
