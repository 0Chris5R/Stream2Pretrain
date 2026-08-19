# Scoring and routing contract

Status: implementation contract
Policy implementation: `processor/scientific_policy.py` and `processor/curate.py`
Current policy id: `S2P_POLICY_REVISION`
Current scoring id: `S2P_SCORING_VERSION`

This document defines every score shown in the Stream2Pretrain UI. A model
score, a deterministic evidence score, and a composite policy score are
different signals and must not share a label.

## 1. Evaluation unit

Scientific sources are not scored as one flattened paper.

1. The extractor removes author and affiliation blocks, acknowledgements,
   declarations, and references from the trainable projection.
2. Every retained heading-delimited section becomes a `SilverSegment`.
3. Cheap C4 and PII checks run on every retained section.
4. FinePDFs Edu v2, FineWeb-Edu comparison, and KenLM run on a bounded sample
   selected from retained sections only.
5. The final model text is rebuilt from sections that survived section-level
   safety checks.
6. Source-authored captions, alt text, structured tables, and display
   equations may be appended. Raw OCR is audit-only unless an OCR policy
   explicitly sets `ocr_training_eligible=true`.
7. Deduplication, final PII scanning, and benchmark decontamination run on the
   exact final projection that could be exported.

## 2. Role-stratified sampling

`S2P_MAX_SCORED_SEGMENTS` sets the bound and defaults to 10.

The sampler reserves at most one longest section from each role family in this
order:

1. abstract;
2. introduction or background;
3. methods;
4. results or discussion;
5. conclusion or limitations;
6. other or appendix.

Unused capacity is filled by the longest remaining sections, with source order
as the deterministic tie breaker. Returned segments preserve source order.
The sampled ids and both educational-model results remain in
`segment_scores_json`.

## 3. Model signals

### 3.1 Scientific quality

Scientific HTML, PDF, and LaTeX records use
`HuggingFaceFW/finepdfs_edu_classifier_v2_eng_Latn` at revision
`90ddef285f67230389057c14b2f6bbfeb70d40ea` as the primary 0 to 5 regression
score. The implementation follows the model card's 10,000-character and
2,046-token top/bottom chunk rule and takes the maximum chunk score.

During calibration, the same sampled sections also run
`HuggingFaceFW/fineweb-edu-classifier` at revision
`284663cbb2dabf9bda30d8f8cc49601251ee1631`. This is an A/B measurement, not a
second headline score. General web/blog content uses FineWeb-Edu as its primary
educational score.

Code records do not run either prose classifier. They use
`code-quality-rules-v1`, a visible 0 to 5 sum of non-trivial length, source
length, bounded line length, comments/documentation, and syntax/balanced-token
checks. Generated/vendor/minified paths score zero. This is recorded as a
rule-based source-quality signal, never labelled FinePDFs or FineWeb-Edu.

The document educational score is the word-weighted mean of measured, retained
sections. Each section weight is `max(1, min(word_count, 512))`.

No threshold taken from either model card is treated as scientifically valid
for this corpus until the labelled validation set is reviewed.

### 3.2 KenLM typicality

The pinned English Wikipedia `edugp/kenlm` binary uses its paired SentencePiece
model. Per-section perplexity is aggregated by weighted median with the same
bounded word weights.

Buckets are:

- head: perplexity at most 200;
- middle: above 200 and at most 1,000;
- tail: above 1,000.

A document is blocked for high perplexity only when at least 75 percent of its
measured retained sections are in the tail and the weighted median is above
2,000. KenLM measures language-model typicality. It is not a correctness or
scientific-value score.

KenLM and prose-specific Gopher checks do not gate code records. Code retains
privacy, license, exact/near-duplicate, validity, and benchmark checks.

### 3.3 Language, privacy, deduplication, and decontamination

- FastLangID must select English with confidence at least 0.5.
- Presidio plus the regex/Luhn pack removes high-confidence affected sections,
  then scans the rebuilt projection again. A remaining blocking match
  quarantines the record.
- MinHash uses 112 permutations. The LSH/Bloom backend observes the final
  projection and blocks near duplicates.
- Decon-Gate checks exact 13-token shingles and E5-small-v2 semantic similarity
  at 0.92 against the versioned restricted benchmark reserve. A hit quarantines
  the record and is included in the signed snapshot attestation.

Strict laptop and Kubernetes profiles set `S2P_REQUIRE_REAL_MODELS=1` and fail
startup if FinePDFs, FineWeb-Edu, KenLM/SentencePiece, E5, Presidio, Rensa,
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

### 4.4 Post-training benchmark allocation

Pretraining does not score or reserve papers for a benchmark. The persisted
`benchmark_score` field is zero and retained only so historical snapshots remain
readable. After a paper has produced accepted SFT or RL artifacts, the foundry
assigns its paper family within that output pool: four families to `train`, then
one to `benchmark`. This is the only active generated-data benchmark split.

### 4.5 Non-scientific structure

Web and code do not receive paper-anatomy credit. Their completeness is a
bounded combination of title presence, usable length up to 500 words, clean
extraction, and non-empty content. Code reasoning evidence uses visible
functions, classes, tests, and code-quality results; its content tags are
`systems_implementation` and `methods_procedures`. Web reasoning evidence is a
  low-weight completeness/quality baseline.

## 5. Composite quality, 0 to 5

The convenience score is:

`5 * (0.35 * educational_quality/5 + 0.25 * scientific_structure/5 + 0.15 *
language_confidence + 0.15 * heuristic_pass_rate + 0.10 * typicality)`

`heuristic_pass_rate` is the mean of Gopher pass and C4 pass. Typicality maps
head to 1.0, middle to 0.72, and tail to 0.25. This composite supports sorting
and dashboards. It is never displayed as FinePDFs, FineWeb-Edu, or KenLM.

## 6. Blocking rules and route precedence

Blocking reasons are applied before corpus-use routes:

1. Any blocking reason other than insufficient body routes to `quarantine`.
2. Insufficient retained scientific body routes to `retry` when it is the only
   problem.
3. Otherwise the record is eligible for `pretrain`.
4. Reasoning evidence at least 0.55 adds `posttrain_candidate` eligibility and
   makes it the primary inspection route; the record remains pretraining-eligible.
5. All other clean records keep `pretrain` as their primary route.

`broad_pretraining`, `reasoning_candidate`, and `benchmark_candidate` are
read-only compatibility labels for historical snapshots. Current curation
never creates them.

The current low-quality blocking rule fires only when the primary educational
score is below 0.75 and scientific structure is below 2.0. It is deliberately
conjunctive so one weak model signal cannot discard a structurally complete
paper before calibration.

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

The UI shows every tag and allows multi-tag filtering. A trained tagger may
replace this baseline only after a held-out comparison.

## 8. Measured scientific pilot

The clean replay used FinePDFs Edu v2 over ten role-stratified, retained
sections per paper. These are measured outputs, not expected human labels:

| Paper | FinePDFs v2 | Structure | Composite | KenLM perplexity | Route |
|---|---:|---:|---:|---:|---|
| HTML papers on arXiv | 2.960 | 3.755 | 3.804 | 372.2 | reasoning candidate |
| The FineWeb Datasets | 4.385 | 5.000 | 4.595 | 437.8 | reasoning candidate |
| Dolma | 3.873 | 5.000 | 4.398 | 649.4 | reasoning candidate |

The values explain the original UI issue: FineWeb-Edu's web rubric produced
low values for scientific prose, while FinePDFs v2 produces a useful spread
without being copied into the composite score. All values describe the clean
training projection rather than authors, acknowledgements, or references.

The pinned v1/v2 comparison in
`validation/finepdfs-v1-v2-pilot.json` ran both checkpoints on the same 30
role-stratified sections. V1 averaged 0.816 (median 0.741); v2 averaged 3.646
(median 3.965), with v2 higher on all 30. This supports v2 as the working
scientific rubric. It does not establish classifier accuracy because those 30
sections do not yet have reviewed usefulness labels.

## 9. Calibration and change control

The checked-in validation manifest covers 30 to 60 papers across theory,
empirical work, surveys, short notes, datasets, specialized prose, native HTML,
clean/scanned/malformed PDF, tables, figures, equations, and references.
Reviewed labels are recorded by paper and section.

Report:

- MAE and rank correlation for model and composite scores;
- precision, recall, and confusion matrices for routes;
- OCR character error rate, word error rate, and numeric exact match;
- score distributions by source format and content category;
- FinePDFs v2 versus FineWeb-Edu on identical sampled sections.

Any threshold or weight change requires a new scoring version, the evaluation
report, and a policy revision. Unknown deployment capacity or performance is
recorded as `needs-measurement`, never guessed.
