# Source-specific quality classifier pilot

2026-09-03: the owner requested deployment of the two completed Kaggle models
in place of the public FinePDFs quality scorer, initially diagnostic only.
Weights may be published through GitHub. On 2026-09-04 the owner also approved
the completed math and post-training models for full live diagnostic scoring.

## Model and input contract

- `arxiv-pretrain-quality`: independent ModernBERT-base fine-tune for arXiv.
- `hf-pretrain-quality`: independent ModernBERT-base fine-tune for model and
  dataset cards. No weights or dataset rows are fetched from those sources.
- `arxiv-math-reasoning`: independent fine-tune for mathematical reasoning.
- `arxiv-posttrain-suitability`: independent fine-tune for grounded post-training
  task potential. Neither new head runs on HF cards.
- Base: `answerdotai/ModernBERT-base`, revision
  `8949b909ec900327062f0ebf497f51aef5e6f0c8`, Apache-2.0.
- Exact final weight and archive hashes are in
  `processor/source-classifiers.json`. Final weights, not intermediate optimizer
  checkpoints, are published in the `source-classifiers-2026-09-03` and
  `source-classifiers-2026-09-04` releases.
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
- The two arXiv reasoning heads score every eligible retained section, including
  non-mathematical sections. Their original evaluation aggregation is maximum;
  retain that plus the unweighted and token-weighted means, best section ID,
  and count of rounded class-5 sections. None ranks, filters, or changes prompts.
  The team has not yet selected an operational aggregation or cutoff.

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
quality image. Its final filesystem contains the four trained heads, not the
old FinePDFs checkpoint. Only the pinned Python environment is copied from
the established dependency image. The unrelated fetcher and KenLM model bases
remain unchanged. Pods never redownload weights at startup.

Stateless quality replicas retain KEDA demand scaling, two baseline
replicas and a cloud maximum of four. Each requests 1 CPU and 2 GiB RAM with limits of
2 CPU and 6 GiB RAM for the heads and full-length sections. This is
capacity configuration, not a measured memory claim. Actual peak RSS, latency
and sustained throughput: needs-measurement on the deployed workload.

Acceptance checks: unit/schema/API tests; Helm and UI checks; CPU model startup;
all four heads returning finite values; replica distribution and batch
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

### Live baseline for the one-hour follow-up

Release `24245f9491f3b35f95daeaf34ebe1004d4bfbb17`, Helm revision 138, became
ready at 2026-09-04 07:45:38 UTC. The legacy distribution stress test then
timed out; the corrected read-only production-protocol check passed on all
four quality Pods in diagnostic run `33850823680`. No second application
rollout was performed. Remote CI passed 606 tests, with one environment-based
integration skip, plus Helm validation.

At 07:55:31-37 UTC, process counters were 58 normalized records, 11 trainable
curator outputs (10 arXiv, one HF dataset), 17 decision writes and nine Gold
writes. These are stage-work counters, not unique-document counts. At
07:56:36 UTC the curator had four startup restarts, with its last restart
during model-service replacement; Foundry and all four quality Pods had zero.
No quality Pods were Pending.

The all-corpus API baseline at 07:55:41 UTC was 18,303 durable decisions and
6,643 training-export documents. Per-source accepted/total: arXiv 4,038/5,857;
HF dataset cards 849/5,871; HF model cards 1,756/6,575.

Foundry at 07:57 UTC had 63 queued candidates. Its sampled artifact inventory
contained 27 accepted and 91 rejected RL environments, and 44 accepted and 59
rejected SFT trajectories. The diagnostic reads at most 500 artifacts and 100
jobs, so those are not guarantees of complete historical totals. Recent jobs
were explicit preflight outcomes for already-expired historical Silver
artifacts; the latest recorded model stream was still from September 3 at
22:37 UTC. A new model-generation success after rollout is not yet established.
Keep the configured 08:30 UTC daily cohort boundary for the follow-up.

The one-time thread follow-up is scheduled for approximately 09:01 UTC
(11:01 Europe/Berlin). Compare counter deltas and unique corpus growth, verify
no steady-state restarts, examine the new daily Foundry cohort and durable
scientific-evidence pointers, then stop the follow-up automation.

Held-out document agreement with Luna labels from the downloaded run:

| Model | Documents | Spearman | QWK | MAE |
| --- | ---: | ---: | ---: | ---: |
| arXiv pretrain | 301 | 0.505 | 0.636 | 0.322 |
| HF pretrain | 500 | 0.914 | 0.904 | 0.318 |

These are teacher-agreement metrics on document-disjoint 90/10 splits, not
independent human quality measurements. arXiv document labels are concentrated
at class 4, so accuracy alone is not an appropriate promotion criterion.

Runtime reference: [Transformers ModernBERT documentation](https://huggingface.co/docs/transformers/v4.57.6/model_doc/modernbert).

## Four-head live measurement, 2026-09-04

The pre-expansion check (`33852461980`, around 08:16 UTC) found no steady-state
curator, Foundry, or model-service restarts. Unique durable decisions grew from
18,303 to 18,346 and strict training exports from 6,643 to 6,667 in about twenty
minutes. Process counters reached 155 normalized, 91 trainable outputs, 103
decision writes and 78 Gold writes. These count replay work separately from
unique corpus growth. Historical missing raw/scientific objects still produce
explicit failures; the new handoff does not reconstruct expired evidence.

All four models run inline in the production inference path so the pilot pays
the actual full classifier cost. Each arXiv section executes three independent
encoders sequentially on its leased Pod; an HF section executes one. Per-head
Prometheus histograms expose full-section wall seconds and score distributions,
with token/window counters. Do not sum overlapping model-service timing and
per-head timing as though they were separate work. Compare a stable live
interval, including cache hits, with ingestion, durable decisions and restart
deltas. Four-head sustained throughput and live quality remain
needs-measurement until this rollout has produced new scored corpus records.

The remaining rollout-only curator crash was a headless-DNS discovery failure
while the model Deployment was being replaced. Strict startup now waits for
ready, revision-matching backends for up to ten minutes before starting
Bytewax, within the existing fifteen-minute startup-probe allowance. Runtime
inference retries and recovery state remain unchanged.

### Transfer pending, 2026-09-04 08:41 UTC checkpoint

Implementation passed 611 remote tests and Helm validation at `6e6d2bb`.
The new model archives are not deployed yet. The initial Mac upload was slow;
the archive's original Kaggle signed URL expired at 07:47 UTC. A fresh download
link is required for the faster cloud-to-cloud transfer. The direct upload
continues as a fallback. `publish-classifiers` is a separate no-cluster workflow
mode that verifies the entire Kaggle archive and packages only final inference
files. Packaging is reproducible across Mac and Linux. No corpus is uploaded.

The second live check (`33854519584`) still audited application `24245f9`:
18,424 unique durable decisions, 6,687 strict training exports, normalized 222,
curator trainable 181, decision writes 185, Gold writes 136. No new curator
restarts since 07:44:08. Today's Foundry cohort started at 08:30:01 UTC with 64
papers: one processing, 63 queued, all with cached scientific evidence. A new
`graph_critic` model stream was recorded at 08:41:57 UTC. The historical raw
object-missing counter reached 154; do not count failed bodies as normalized
documents or claim these old objects were recovered.

After both release assets are available, deploy the prepared four-head bundle,
record its own baseline, and measure again after one stable hour. The thread
follow-up handles that continuation; it must stay quiet while an upload is
unchanged and stop after the final measurement.

### Public HF transfer, 2026-09-04 09:25 UTC

At the owner's request, the direct Mac-to-GitHub upload was stopped and the
same two checksum-pinned inference archives are uploading to the temporary
public model repository `ChrisR05/transfer-20260904-092445`. No model card,
description, labels, source texts, training checkpoints or credentials are
included. The HF write token is used only by the local transfer process.

The publication workflow now downloads these public archives without HF
credentials, verifies their existing manifest checksums and copies them to
the GitHub release. Its public base URL is configured by repository variable
`S2P_CLASSIFIER_TRANSFER_BASE_URL`. Deployment still uses the permanent GitHub
URLs, so the temporary HF repository may be deleted after that copy succeeds.
Do not retry the cancelled GitHub upload or the expired Kaggle URL. Wait for
the HF commit before running `publish-classifiers`, then deploy and take the
one-hour production measurement as above.

The first HF uploader subsequently stalled after repeated 300-second Xet
network timeouts; its public commit never completed. It was restarted against
the same repository with `HF_XET_CLIENT_READ_TIMEOUT=1800s` and one concurrent
upload stream. Current transfer process is Python PID 59521 (uv parent 59520),
exec session 82680. The former PID 40443/session 1799 was intentionally stopped.
No application deployment or classifier change occurred during this retry.

HF publication completed at 2026-09-04 10:07:49 UTC, commit
`307db3ffde06e3189bc7567c50debdbf2fd01def`. Both public LFS SHA256 values match
the manifest exactly. The cloud transfer variable now pins that immutable HF
commit, and publication run `33861892180` was dispatched at 10:09:49 UTC to copy
the two archives to their permanent GitHub release URLs. Do not require the HF
write token for this or any deployment step.

The permanent GitHub publication succeeded in 56 seconds. At 10:11 UTC both
release assets report `uploaded` and their GitHub SHA256 digests match the
manifest. The owner can now delete the temporary HF repository; production
builds no longer depend on it. A single four-head deployment was dispatched
at 10:11 UTC with `verify_classifiers=true` on branch commit `96f3397`.

Deployment run `33862002504` stopped before rollout: the original interrupted
GitHub release creation had left the release as a draft, making its assets
invisible to anonymous image builds despite successful authenticated asset
checks. The release is now explicitly published, and the publication workflow
does that after each completed transfer. Public download URLs include
`?download=1` to bypass the negative 404 cache created before publication.
Anonymous checks of those URLs succeed. Weight bytes and checksums are unchanged.

The four-model base image built successfully in run `33862519184`. That run
then stopped before rollout on a UI type error: the two new diagnostic maps
used Zod's old one-argument `record` form. Both now provide explicit string
key schemas, consistent with the existing Zod 4 schemas. Full UI TypeScript
checking passes after the correction; no classifier or routing logic changed.
