# Hugging Face card quality audit

Status: implemented policy. Two bounded live content samples have been
inspected; a labelled learned-model calibration still remains
`needs-measurement`.

## What the card source contributes

The model and dataset feeds retain only exact-revision README prose. YAML front
matter, fenced code, HTML scaffolding, and repository assets are not training
text. Cards are pretraining documentation only. They do not enter the
scientific-paper SFT/RL Foundry.

## Observed content classes

The live inspection found these materially different card classes. They are
now durable content tags and form the label set for a future card-specific
classifier:

- `dense_scientific_card`: architecture, training, evaluation, data, or
  limitation evidence across multiple technical dimensions.
- `substantive_technical_card`: useful documentation with a narrower scope.
- `template_boilerplate`: unfilled generated card templates.
- `stub_checkpoint_upload`: upload notices and checkpoint-only READMEs.
- `synthetic_script_card`: generated `inference.py`, `pipeline.py`, or trainer
  inventories whose apparent technical fields do not document a real model.
- `minimal_artifact_listing`: short checkpoint or file inventories without
  measured, source, revision, or evaluation evidence.
- `marketing`: promotional prose without technical grounding.
- `generic_marketing_benchmark`: polished claims built from unidentified
  models, benchmarks, or results that provide no checkable provenance.
- `wrong_repository_type`: model documentation in a dataset feed or the
  converse, without the expected source-specific sections.
- `insufficient_card_documentation`: prose that does not establish either the
  official card structure or multiple technical dimensions.
- `language_filter` and PII flags remain shared pipeline outcomes rather than
  card-content labels.

Manual review found useful dense technical reports with metrics and
hyperparameters, compact format/runtime documentation, empty trainer or card
templates, quantization mirrors, generic marketing with unidentified benchmark
claims, and mixed cards that combine useful measured results with unfinished
template sections. Those patterns, rather than a generic educational score,
define the future card-classifier labels.

## Current gate

The deterministic gate follows the official Hugging Face card structure:

- A model card must provide an overview plus substantive material about use,
  training, evaluation, architecture, limitations, or inference.
- A dataset card must provide an overview plus substantive material about data
  composition, fields, collection, use, or limitations.
- Older non-template cards without current headings may pass when their prose
  independently establishes technical detail plus concrete measurements,
  named sources, immutable revisions, metrics, repository references, or
  executable examples.
- Placeholder templates, upload stubs, wrong-type cards, and ungrounded
  marketing are rejected.
- Unfilled template sections are removed independently. A card with measured
  results and hyperparameters is therefore not discarded merely because its
  generated README still contains an unfinished limitations section.
- Compact format and runtime documentation can pass without template headings
  when it establishes independent artifact-format and runtime evidence.

This gate is independent of the learned educational-quality scores. Every
retained section receives both FineWeb-Edu and FinePDFs Edu v2. FineWeb-Edu is
the primary reported card signal because its official training set is web
content, which is closer to a Markdown repository README. Its model card still
describes a school-focused web rubric and warns about specialized higher
education, so it is audit-only for cards. FinePDFs Edu v2 was trained on PDF
samples and explicitly documents out-of-domain limitations, so it is retained
only as a labelled comparison for cards. Neither classifier gates card
admission and a higher-looking out-of-domain FinePDFs score must not replace the
FineWeb score.

## Bounded live sample, 2026-08-27

The audit replayed the exact README revisions in the most recent 100 deployed
licence-admission records. On the final audit pass 87 revisions remained
reachable: 54 dataset cards and 33 model cards. The revised deterministic
projection retained 21 dataset cards and 15 model cards, and removed 81
unfilled template sections.
The sample exposed four concrete errors that are now covered by regression
tests: multi-line HTML comments leaking asset URLs, duplicated first headings,
quantization-mirror and generated-trainer shells passing as documentation, and
a substantive trainer report being rejected because other sections still held
placeholders. The sample is operational evidence, not an estimate of classifier
precision or recall.

## Bounded deployed-corpus sample, 2026-08-30

The audit inspected 100 durable accepted model-card projections, 100 durable
accepted dataset-card projections, 100 model-card rejections, 100 dataset-card
rejections, and 20 accepted arXiv projections. It found generated script cards,
trainer shells, minimal checkpoint inventories, placeholder paper-title cards,
and repeated lightly edited repository cards among the historical acceptances.
It also found useful compact and legacy cards rejected because they did not use
the expected heading vocabulary. The revised deterministic policy retained 94
of the 100 previously accepted model cards and 92 of the 100 previously accepted
dataset cards. The removed sample rows were the observed generated scripts,
trainer/quantization shells, minimal inventories, access-only or generic cards,
and placeholder-title cards.
The revised policy also accepts the audited compact measured card and the four
useful no-template-heading rejection examples. These are bounded observed
sample counts, not population precision or recall estimates.

The arXiv sample contained 19 substantive scientific papers and one published
IEEEtran starter file. A narrow literal template detector removes that starter
without imposing a minimum paper length or requiring conventional headings.
Equations, tables, figures, and captions remain in the scientific projection.

The `pretrain-content-v2` scoring generation is the clean-output boundary.
Normal documents, aggregates, as-of views, and dataset exports expose only that
generation. Historical rows remain durable audit history but do not masquerade
as current clean output. Deployment must replay eligible live input through the
new generation before expecting current exports to repopulate.

## Classifier follow-up

The next classifier step is a small card-specific model trained on manually
audited live cards using the listed labels. Sampling must be stratified by
model versus dataset card and by deterministic outcome. The comparison report
must record precision and recall per class, CPU latency, peak RSS, exact model
revision, and the same-sample FineWeb-Edu versus FinePDFs results. All values
remain `needs-measurement` until the deployed evaluation is complete.

Primary references:

- <https://huggingface.co/docs/hub/model-cards>
- <https://huggingface.co/docs/hub/model-card-annotated>
- <https://huggingface.co/docs/hub/datasets-cards>
- <https://huggingface.co/HuggingFaceFW/fineweb-edu-classifier>
- <https://huggingface.co/HuggingFaceFW/finepdfs_edu_classifier_v2_eng_Latn>
