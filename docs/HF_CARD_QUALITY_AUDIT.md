# Hugging Face card quality audit

Status: implemented policy. A bounded live content sample has been inspected;
the same-sample learned-model comparison still remains `needs-measurement`.

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
  independently establishes multiple technical dimensions.
- Placeholder templates, upload stubs, wrong-type cards, and ungrounded
  marketing are rejected.
- Unfilled template sections are removed independently. A card with measured
  results and hyperparameters is therefore not discarded merely because its
  generated README still contains an unfinished limitations section.
- Compact format and runtime documentation can pass without template headings
  when it establishes independent artifact-format and runtime evidence.

This gate is independent of the learned educational-quality scores. Every
retained section receives both FineWeb-Edu and FinePDFs Edu v2. FineWeb-Edu is
currently the primary reported card score and FinePDFs is the comparison score.
Neither score alone admits or rejects a card until a same-sample evaluation has
been measured on the labels above.

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
- <https://huggingface.co/HuggingFaceFW/finepdfs_edu_classifier_v2_eng_Latn>
