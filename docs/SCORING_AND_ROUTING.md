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
   FinePDFs Edu v2 then runs on every retained section of eligible papers and
   Hugging Face cards. KenLM runs only where its source policy enables it.
5. The final model text is rebuilt from sections that survived section-level
   safety checks.
6. Source-authored captions, alt text, structured tables, and display
   equations may be appended. Raw OCR is audit-only unless an OCR policy
   explicitly sets `ocr_training_eligible=true`.
7. Deduplication and final PII scanning run on the
   exact final projection that could be exported.

## 2. Section coverage

Every retained section is scored. Earlier pilots used a role-stratified sample,
but the deployed policy no longer makes a document decision from only part of
the exportable training projection. `segment_scores_json` retains the exact
source-quality results and applicability metadata for each section.

## 3. Model signals

### 3.1 Scientific quality

Scientific HTML, PDF, and LaTeX records use
`HuggingFaceFW/finepdfs_edu_classifier_v2_eng_Latn` at revision
`90ddef285f67230389057c14b2f6bbfeb70d40ea` as the primary 0 to 5 regression
score. The implementation follows the model card's 10,000-character and
2,046-token top/bottom chunk rule and takes the maximum chunk score.

Hugging Face cards remove front matter, fenced code, HTML comments, asset-only
sections, templates, mirrors, and other deterministic slop before FinePDFs
scoring. FinePDFs remains a continuous audit and ranking signal for cards until
a reviewed same-source calibration establishes a safe threshold. Discovery
metadata has no educational classifier and cannot reach Gold.

The document educational score is the word-weighted mean of measured, retained
sections. Each section weight is `max(1, min(word_count, 512))`.

No threshold taken from either model card is treated as scientifically valid
for scientific papers, cards, code, or reviews until a source-specific labelled
validation set is reviewed.

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
- Presidio plus the regex/Luhn pack removes high-confidence affected sections,
  then scans the rebuilt projection again. A remaining blocking match
  quarantines the record.
- MinHash uses 112 permutations. The LSH/Bloom backend treats any matching LSH
  band as a candidate, then confirms at least 0.80 estimated MinHash similarity
  against the durable anchor signature. State is retained across worker
  restarts and namespaced by scoring generation.
Strict laptop and Kubernetes profiles set `S2P_REQUIRE_REAL_MODELS=1` and fail
startup if FinePDFs, KenLM/SentencePiece, Presidio, Rensa,
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

Web and code do not receive paper-anatomy credit. Their completeness is a
bounded combination of title presence, usable length up to 500 words, clean
extraction, and non-empty content. Code reasoning evidence uses visible
functions, classes, tests, and code-quality results; its content tags are
`systems_implementation` and `methods_procedures`. Web reasoning evidence is a
  low-weight completeness/quality baseline.

## 5. Composite quality, 0 to 5

The base weights are educational quality 0.35 and source-appropriate structure
0.25. Natural-language profiles add language confidence 0.15. Ordinary web additionally contributes
heuristic pass rate 0.15 and KenLM typicality 0.10. The active weights are
renormalized to sum to one before multiplication by 5.

`heuristic_pass_rate` is the mean of Gopher pass and C4 pass. Typicality maps
head to 1.0, middle to 0.72, and tail to 0.25. Non-applicable signals contribute
neither a value nor weight. This composite supports sorting and dashboards. It
is never displayed as FinePDFs or KenLM.

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

FinePDFs has no transferable hard threshold for both papers and cards. Its
score contributes to ranking and the composite, while source-specific
deterministic blockers make admission decisions.

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

## 8. Historical measured scientific pilot

The earlier local replay used FinePDFs Edu v2 over ten role-stratified,
retained sections per paper and recorded KenLM for comparison. These are
historical measured outputs, not the current source policy or expected human
labels:

| Paper | FinePDFs v2 | Structure | Composite | KenLM perplexity | Route |
|---|---:|---:|---:|---:|---|
| HTML papers on arXiv | 2.960 | 3.755 | 3.804 | 372.2 | reasoning candidate |
| The FineWeb Datasets | 4.385 | 5.000 | 4.595 | 437.8 | reasoning candidate |
| Dolma | 3.873 | 5.000 | 4.398 | 649.4 | reasoning candidate |

The values show that FinePDFs v2 produces a useful scientific-quality spread.
All values describe the clean
training projection rather than authors, acknowledgements, or references.
The current policy scores every retained scientific section and disables
KenLM for scientific text, so a new cloud validation report must not compare
its resulting composite directly with this historical table.

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
- FinePDFs v2 distributions on independently reviewed paper and card samples.

Any threshold or weight change requires a new scoring version, the evaluation
report, and a policy revision. Unknown deployment capacity or performance is
recorded as `needs-measurement`, never guessed.
