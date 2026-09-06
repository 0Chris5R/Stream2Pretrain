# Hugging Face card quality audit

This guide records the content patterns used by the deterministic card gate
and supplies a rubric for future categorical annotations. The current learned
usefulness score is the independent HF ModernBERT head.

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

This gate runs before learned inference. Every retained section receives the
HF ModernBERT score; the token-weighted document mean must be at least 3.5.
Quality labels are scalar usefulness judgments, separate from the categorical
rubric above.

## Evaluation

Review model and dataset cards separately across the predicted score range.
Inspect compact technical cards, dense reports, mirrors, generated templates,
marketing, wrong-type cards and mixed useful/template sections. Measure false
admission and false rejection, not just average scores.

The current usefulness classifier is already source-specific. A categorical
tagger using the rubric above would be a separate optional model. Provider
errors and extraction defects must not become negative usefulness labels.

Primary references:

- <https://huggingface.co/docs/hub/model-cards>
- <https://huggingface.co/docs/hub/model-card-annotated>
- <https://huggingface.co/docs/hub/datasets-cards>
