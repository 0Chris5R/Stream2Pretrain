# Source-specific quality classifier pilot

2026-09-03: the owner requested deployment of the two completed Kaggle models
in place of the public FinePDFs quality scorer, initially diagnostic only.
Weights may be published through GitHub. The math and post-training classifiers
are separate unfinished training jobs and are not part of this deployment.

## Model and input contract

- `arxiv-pretrain-quality`: independent ModernBERT-base fine-tune for arXiv.
- `hf-pretrain-quality`: independent ModernBERT-base fine-tune for model and
  dataset cards. No weights or dataset rows are fetched from those sources.
- Base: `answerdotai/ModernBERT-base`, revision
  `8949b909ec900327062f0ebf497f51aef5e6f0c8`, Apache-2.0.
- Exact final weight and archive hashes are in
  `processor/source-classifiers.json`. Final weights, not intermediate optimizer
  checkpoints, are published in the `source-classifiers-2026-09-03` release.
- CPU, FP32, Transformers 4.57.6, SDPA; no quantization, truncation-only
  shortcut, secondary public quality model, or production heuristic fallback.
- Existing license, privacy, extraction, HF-card and duplicate rejection runs
  first. Only eligible retained projections consume classifier inference.
- Input is the actual sanitized training projection, including retained
  table/equation/figure-text surrogates. Its Markdown sections use the same
  parser and section-type rules as the Luna labeling set.
- Every section has the exact training prefix:
  `[SOURCE=arxiv|hf] [SECTION_TYPE=...] [SECTION_TITLE=...]` followed by a newline
  and the full section text.
- An overlength section uses all 8,192-token windows with 512-token overlap.
  Windows execute individually to bound memory. Their logits are averaged
  before softmax, exactly as in the training evaluation.
- Six internal ordinal-bin probabilities produce score `sum(p[i] * i)`.
  Confidence is `1 - entropy(p) / log(6)`, not a calibrated correctness
  probability and not the teacher's confidence. Class is the rounded score.
- Document score and confidence are token-weighted section means. Weights are
  summed encoded lengths minus overlap, matching the reported evaluation.

## Diagnostic isolation

The learned score cannot reject or admit a record, remove a section, affect
its reasoning-route score, or rank the Foundry queue. During this pilot the
composite excludes the learned term and renormalizes the remaining applicable
terms; reasoning uses structural/evidence signals. Foundry ranking omits the
learned score for records marked `diagnostic`. These restrictions are tested.
No automatic promotion or new threshold is configured.

The existing durable `edu_score` column holds the learned quality value for
wire/schema compatibility. `quality_diagnostics_json` records diagnostic mode,
exact model digest, aggregation, and each section's title, type, text hash,
score, confidence, probabilities, tokens and window count. This is an optional
Iceberg column, so historical records remain readable and are never relabeled
as predictions from the new model. Original training text stays in its existing
column; the document API joins section text to the score report on demand.

Documents displays source quality, confidence, and expandable scored sections.
Advanced audit retains exact model provenance. Aggregate dashboard counts stay
all-corpus; this rollout does not replay, delete or rescore historical records.

## Deployment and validation

GitHub release archives are checksum-verified while building the dedicated
quality image. Its final filesystem contains the two trained heads, not the
old FinePDFs checkpoint. Only the pinned Python environment is copied from
the established dependency image. The unrelated fetcher and KenLM model bases
remain unchanged. Pods never redownload weights at startup.

Stateless quality replicas retain KEDA demand scaling, two baseline
replicas and a cloud maximum of four. Each requests 1 CPU and 2 GiB RAM with limits of
2 CPU and 6 GiB RAM to accommodate both heads and full-length sections. This is
capacity configuration, not a measured memory claim. Actual peak RSS, latency
and sustained throughput: needs-measurement on the deployed workload.

Acceptance checks: unit/schema/API tests; Helm and UI checks; CPU model startup;
both source heads returning finite values; replica distribution and batch
parity; fresh durable records with matching API and UI diagnostics. Quality
review after a representative live interval remains a separate team decision.

## 2026-09-04 runtime recovery

Cloud diagnostics confirmed curator exit-code-1 failures after the fixed
180-second quality HTTP deadline. These were not curator OOM kills. Four
quality Pods fit the observed reservations; the fifth and sixth stayed Pending.
The cloud ceiling is therefore four, not a reduction in sections or input length.

- Quality jobs use bounded 20-second HTTP polling while full inference continues
  on the same leased Pod. Exact requests are idempotent within a Pod. No scoring
  timeout truncates a section. Legacy synchronous clients remain compatible.
- Completed section scores are cached on the curator PVC by exact input hash and
  model revision. The cache contains scores, not text, and retains at most about
  100,000 entries. Transient model failures retry without advancing input or
  terminating Bytewax. Model demand metrics are exposed to KEDA.
- Foundry event/artifact appends are serialized across admission and generation
  threads. Iceberg conflicts reload the snapshot and deduplicate stable IDs
  before retrying. Failed writes retain the SQLite outbox.
- Losslessly compressed scientific JSON travels with normalized records until
  admission. Accepted structured evidence is persisted in Gold before its
  decision is emitted. If the capsule exceeds Kafka's message budget, exact JSON
  is externalized to Gold and the record carries its pointer instead. This rare
  overflow path also preserves evidence for later-rejected documents; its size
  is needs-measurement. No PDFs or image binaries are embedded or retained there.
  Existing transient bodies and images keep their configured 24-hour lifecycle.
  Already-expired historical evidence cannot be reconstructed; Foundry records
  that explicit preflight outcome instead of inventing input.
- Diagnostic document date windows are labeled as publication-date windows,
  not processing throughput. Use stage counter increases for work in an hour,
  and the durable all-corpus overview for unique corpus totals.

The bundled release must pass remote regression checks, then receive a live
one-hour comparison of restarts, committed decisions, ingestion, model load,
and Foundry progress. Sustained post-fix throughput remains needs-measurement
until that comparison. Model weights, scoring aggregation, and all admission
gates are unchanged.

Held-out document agreement with Luna labels from the downloaded run:

| Model | Documents | Spearman | QWK | MAE |
| --- | ---: | ---: | ---: | ---: |
| arXiv pretrain | 301 | 0.505 | 0.636 | 0.322 |
| HF pretrain | 500 | 0.914 | 0.904 | 0.318 |

These are teacher-agreement metrics on document-disjoint 90/10 splits, not
independent human quality measurements. arXiv document labels are concentrated
at class 4, so accuracy alone is not an appropriate promotion criterion.

Runtime reference: [Transformers ModernBERT documentation](https://huggingface.co/docs/transformers/v4.57.6/model_doc/modernbert).
