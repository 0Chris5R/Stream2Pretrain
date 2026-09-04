# Source classifiers

Four independent fine-tunes of `answerdotai/ModernBERT-base` provide section
scores. The base revision is
`8949b909ec900327062f0ebf497f51aef5e6f0c8`, licensed Apache-2.0.
[The release manifest](../processor/source-classifiers.json) pins every archive,
final weight digest and model revision. Training labels and optimizer state are
not distributed with runtime images.

| Model | Input | Pipeline use |
|---|---|---|
| arxiv-pretrain-quality | Every retained paper section | Token-weighted mean >=3.0 |
| hf-pretrain-quality | Every retained model/dataset-card section | Token-weighted mean >=3.5 |
| arxiv-math-reasoning | Every retained section of quality-passing papers | Optional mathematical-section hints to the task designer |
| arxiv-posttrain-suitability | Every retained section of quality-passing papers | Token-weighted mean ranks the daily queue; high sections add optional hints |

Licence eligibility is separate. A paper restricted to derived use still has
to pass source quality before either post-training head runs. HF cards never
enter the paper Foundry. Confidence does not gate admission.

## Input and aggregation

Sections use the labeling/training parser and this exact prefix:

```text
[SOURCE=arxiv|hf] [SECTION_TYPE=...] [SECTION_TITLE=...]
<complete sanitized section text>
```

Each section longer than 8,192 tokens uses all windows with 512-token overlap.
Windows run individually to bound memory; their logits are averaged before
softmax. No middle content is discarded.

For six ordinal bins, the score is `sum(i * p[i])` for `i=0..5`.
The displayed class is the rounded expectation. Confidence is
`1 - entropy(p) / log(6)`, a concentration measure, not a calibrated probability
of correctness. Document scores and confidence use section token weights:
summed encoded window lengths minus overlap.

The arXiv heads also retain unweighted means, maxima, best section identity and
class-5 section counts. Maxima are not admission gates. Section hints do not
replace or shorten the paper supplied to generation.

## Runtime

CPU FP32 inference uses Transformers 4.57.6 with SDPA and local Safetensors.
Strict profiles fail if pinned artifacts or protocol revisions cannot load.
Deterministic extraction, card, language, privacy and duplicate rejection runs
before inference. Two-stage scoring avoids running either arXiv auxiliary head
on a quality-rejected paper.

The cloud uses stateless inference replicas with asynchronous, idempotent
requests and bounded polling. The source-quality service exposes per-head time,
token, window and score metrics. KEDA scales within the configured CPU capacity.
Model weights are installed and verified during image construction, not Pod
startup.

The retained `quality_diagnostics_json` contains model provenance, mode,
cutoff, pass/fail, section titles and types, scores, probabilities, confidence,
token counts and window counts. The document inspector joins these with the
actual section text. `edu_score` is the durable document quality field;
the separate composite score is described in
[scoring and routing](SCORING_AND_ROUTING.md).

## Evaluation

The released aggregate [evaluation results](../validation/classifier-evaluation.json)
come from the saved Kaggle reports. No teacher labels or document text are
included. The split is 90/10 by document, stratified by source and document
quality, with 301 held-out papers and 500 held-out cards.

| Head | Section MAE | Section Spearman | Document MAE | Document Spearman |
|---|---|---|---|---|
| arXiv quality | 0.417 | 0.711 | 0.322 | 0.505 |
| HF quality | 0.311 | 0.913 | 0.318 | 0.914 |
| arXiv math | 0.553 | 0.875 | 0.701 | 0.773 |
| arXiv posttrain | 0.497 | 0.824 | 0.351 | 0.529 |

Quality document metrics use token-weighted means. The auxiliary reports use
maximum section scores, not the deployed mean queue ranker. In particular,
291/301 posttrain predictions round to 5 under the maximum aggregation. This
supports using section scores as hints and means for ranking, not a class-5
admission rule. Mean-ranker evaluation remains a separate measurement.

The arXiv quality test set is concentrated at document label 4. Its constant
majority baseline has document MAE 0.296, lower than the model's 0.322, but zero
rank correlation. HF improves strongly over its document baseline MAE 1.164.
These are agreement measurements with a teacher, not proof of downstream model
improvement. Confidence is entropy-derived and is not empirically calibrated.

## Training procedure

[The data builder](../scripts/build_classifier_training_data.py) joins complete
prepared source projections to validated teacher responses and assigns the
document split before emitting section rows. Evaluation date and section roles
are retained. Teacher request construction is in
[pretrain_judge_batch.py](../scripts/pretrain_judge_batch.py).

[The Kaggle launcher](../notebooks/train_source_classifiers_kaggle.py) trains
one independent model on each of two T4s, then runs the next model on that GPU.
It uses six bins, effective-class-weighted cross entropy plus normalized
expected-score MSE, four epochs, AdamW, cosine decay and gradient accumulation.
Complete sections use 8,192-token windows and 512-token overlap. Prediction
uses sequential ordering before joining logits back to section/document IDs.
The [Molab notebook](../notebooks/train_source_classifiers.py) provides the
single-GPU variant. Training requires a separately supplied labeled dataset;
runtime images include only final weights, tokenizer and pinned manifest.

For Kaggle, attach the script and labeled JSONL as notebook inputs, select two
T4s, then launch the script from a saved notebook version. Checkpoints and final
artifacts are written under `/kaggle/working/stream2pretrain-modernbert-classifiers`.
The launcher resumes its own last checkpoint when that output directory is
restored. Do not publish optimizer state or the labeled corpus in this repository.

Training and evaluation separate documents, not individual sections. Assess
section and document MAE, weighted kappa, rank correlation, confusion matrices
and threshold precision/recall on held-out documents. Bootstrap uncertainty by
document. Review both high and low predictions from each source, including
introductions, limitations, mathematical sections and card templates.

An inference health check establishes availability, not usefulness or sustained
throughput. Measure a representative live interval after rollouts, separate
fresh records from replay, and compare normalized input against durable output.
