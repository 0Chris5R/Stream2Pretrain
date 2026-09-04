# Submission evidence

## Scope

The September 2026 release includes the source adapters, Bytewax processing,
Iceberg persistence, monitoring UI and experimental post-training Foundry.
The four independent ModernBERT classifiers are active. No model, extraction
stage or quality gate was reduced for this check.

The application baseline is `f58257269b234493f67c57d4a226ba0e631be224`.
The release-check and indexed source-validity changes are in `b2483dc`, with
the formatted release at `9697e27`.
Cloud verification uses the repository's GitHub Actions VPN workflow; no
workstation Kubernetes context or local pipeline was used.

## Deterministic verification

- Cloud Python validation: 610 passed and one integration test skipped because
  the CI environment has no running curation topic.
- Python lint, workflow syntax, Helm/Helmfile contracts and the credential scan
  passed.
- README and operating-guide relative links resolve.
- Saved classifier evaluation reports are included as aggregate statistics in
  `validation/classifier-evaluation.json`, with training code in `notebooks/`.
  Labels, source corpus, optimizer checkpoints and credentials are excluded.

## Deployment observations

The full dependency/image replacement briefly caused disk-pressure scheduling
taints. The initial release job exceeded its Foundry readiness deadline.
Kubernetes cleared the taints and the Foundry became Ready at approximately
14:51 UTC without a code, quality or resource-limit change. The subsequent
read-only check found all core services Ready with zero container restarts.

The final release workflow `33889908880` passed. Its canary captures the smoke
topic's numeric partition frontiers before starting its isolated worker and
waits for actual runtime readiness before injection. At 15:41 UTC, one controlled
record completed Bronze, normalization, active quality scoring and curated
publication in 9.089 seconds. Its quality score was 3.885, route `pretrain`, with
no rejection reasons. Production-topic isolation passed and the synthetic
Bronze object was removed. This is a functional test, not a throughput benchmark.

The typed `as-of` endpoint reads retained source-validity intervals from the
serving index. Explicit SQL remains isolated on the Iceberg executor. The
regression tests cover all-policy generations, the half-open end boundary and
a newer rejection replacing an older acceptance. Acceptance filtering happens
after selecting the latest valid decision, not before it.

Browser audit `33890545842` returned HTTP 200 for all eight pages and eleven
typed API probes, including `as-of`, with no page exceptions. It also retrieved
21 document details and 12 artifact inspections without an error. The README
dashboard screenshot comes from this run. Numerical inspection additionally
identified the old-acceptance selection-order issue covered by the regression
above.

At 14:52 UTC the master exposed 4 vCPUs and about 32 GiB RAM; each worker exposed
6 vCPUs and about 8 GiB RAM. Node working-memory usage was 24%, 72% and 53%.
Root filesystem free space was about 33.5, 18.7 and 14.9 GiB respectively.
These are observed resources, not a guarantee of spare capacity under every
future workload or full-image replacement.

## Measurement method

`capture-evidence` records the same service counters, broker frontiers, durable
corpus totals, node usage and object-store inventory at two times. Processing
events can include replay; broker message deltas and latest-per-document corpus
deltas answer different questions. They must not be labeled as interchangeable
fresh-document throughput.

Object-store inventories are sequential bucket scans, not atomic filesystem
snapshots. Net retained-byte changes are approximate. The last-24-hour size of
currently retained objects includes rewritten lakehouse files and must not be
presented as permanent daily corpus growth.

The matched snapshots cover 14:57:10 to 15:24:41 UTC, or 1,650.8 seconds.
The interval includes release-canary and classifier-distribution verification,
plus browser/content reads. It is not an unloaded maximum-capacity benchmark.
The core production Pods did not restart between these observations.

| Observation | Interval change | Rate per hour |
|---|---:|---:|
| Raw topic events | 405 | 883 |
| Normalized topic events | 113 | 246 |
| Curation decision events | 32 | 70 |
| Curated topic events | 16 | 35 |
| Latest-per-document corpus decisions | 14 | Not a processing-capacity rate |
| Latest training-export documents | 1 | Includes changed outcomes of existing documents |

Normalized inputs exceeded completed decisions by 81 during this interval.
The evidence therefore does not establish that the current processing capacity
keeps up. Production fetcher counters separately recorded 83 arXiv and 19 HF
model-card normalizations. One additional document-local `ValueError` occurred.
The source ledger's preceding-24-hour observations were 2,172 arXiv items,
4,098 HF model cards and 4,610 HF dataset cards. For arXiv, 1,026 were permissive,
1,011 derived-posttrain-only and 135 pre-fetch quarantines. These ledger counts
are admission observations, not proven fresh unique papers or quality acceptances.

End-state corpus acceptance was 4,077/5,088 arXiv records, 1,795/6,721 HF model
cards and 858/6,040 HF dataset cards. Corpus totals include older policy
generations and select the latest visible decision per document.

| Bucket | Retained size at end | Net interval change |
|---|---:|---:|
| Bronze | 1.78 GiB | +69.7 MiB |
| Silver | 9.66 GiB | +101.0 MiB |
| Gold | 40.19 GiB | +8.53 MiB |
| Post-training packages | 37.1 MiB | 0 |
| State objects | 293.7 MiB | +43.1 KiB |

At this interval's rate, Gold's net increase extrapolates to about 0.44 GiB/day.
Bronze and Silver extrapolate to about 8.73 GiB/day combined before expiry;
their one-day retention means this is turnover, not permanent daily growth.
The small window and sequential scans cannot establish a long-term storage
budget. It is incorrect to extrapolate all rewritten Gold object bytes as
new corpus growth. The compact measurements are preserved in
[`validation/submission-2026-09-04.json`](../validation/submission-2026-09-04.json).

## Post-training schedule

The 4 September cohort started at 08:30 UTC and completed at 13:37 UTC. At the
14:55 UTC check, two newer candidates were queued for the next cutoff, both
with cached scientific evidence. The ready worker was therefore idle by
schedule, not stuck. Cumulative generated outputs were 46 accepted SFT
trajectories and 27 accepted RL environments. Acceptance is automated validation,
not a substitute for a named human audit.

## Content spot-check

The read-only browser audit retrieved 21 document details and 12 generated
artifact inspections without a retrieval error. The sample spans arXiv, HF
model cards, HF dataset cards, accepted routes and quarantine. It is a
purposive spot-check, not a random sample or a measured error rate. Full bodies
were retrieved; reading covered the short cards, one complete short paper,
selected passages of longer papers, task instructions, answers and validation
reports. Source text and generated packages are not redistributed in Git.

### Pretraining

- The HTML paper *From Producing to Validating* retains a coherent abstract,
  argument and discussion without a references section. The trace-integrity
  paper retains structured tables and LaTeX equations in the sampled output.
- The PDF-derived SCROLL paper retains useful scientific content but still
  includes an author/affiliation heading before its abstract. Its contact email
  is redacted. Figure labels are not a reliable semantic classification:
  plotted results in this sample were labeled `table`.
- The HF `kernel-code-embed`, `B1k_Recovery` and `base-mixedp-ic16` cards contain
  substantive evaluation, data-layout and sampling information. Scores are
  approximately 4.22, 4.08 and 3.73. Some empty citation headings remain.
- Older accepted HF cards include a 1.50-scoring quantization/repackaging card.
  That diagnostic-era admission predates the active 3.5 gate. A current
  2.21-scoring Gemma derivative card is correctly quarantined by that gate.
  All-policy corpus totals intentionally retain historical admissions; they
  do not imply that every stored document passes today's classifier threshold.
- Numerical PII false positives remain: a correlation vector in `B1k_Recovery`
  and several decimal benchmark rows in VBVR-Pro contain `[PHONE]` replacements.
  VBVR-Pro is English but was rejected at language confidence 0.48. These are
  projection/decision errors, not evidence that the source is unusable.
- Two sampled CC-BY-NC-ND papers have admission-only quarantine records and no
  body or classifier result. A missing-license paper is posttrain-only.
  The old `assumed-1991-2003` arXiv grant is also quarantined by the current
  policy; its treatment needs a targeted rights-policy review before calling
  every license rejection correct.

### Post-training

The sample contains six SFT inspections and six RL inspections, split evenly
between automated acceptance and rejection within each kind.

| Example | Observed content | Audit judgment |
|---|---|---|
| SFT, `2608.26086`, assumption consequence | Calculates expected pivot effects as rate times effect, then compares scaffolds. | Arithmetic is correct under the stated hypothetical approximation, but it is a simple task. |
| SFT, `2608.26086`, table reasoning | Requests per-competition differences; the accepted answer says Table 18 is absent, yet supplies a mean improvement of 0.214 without deriving it. | Automated acceptance is not defensible from the visible solution. Needs task-completeness and evidence-grounding review. |
| Rejected SFT, `2608.24024`, RGV | Computes maximum document recall 0.45 and compares votes 0.45 versus 0.80. | The readable arithmetic is correct and the grounding critic agrees, but positive/equivalent verifier checks fail. The rejection cannot be explained as a scientific-answer error alone. |
| Accepted RL, `2608.15465`, `2608.21743`, `2608.22312` | Corrects one reversed dependency or precedence relation using paper evidence. | Inspectable and consistent with the frozen relation verifiers, but narrow and simple, not demonstrated frontier-level reasoning environments. |
| Rejected RL, `2603.01113` | Stochastic question-answer cascade. | The answer confuses per-question expected calls with calls over the full residual question set and makes inconsistent deterministic-case claims. Rejection is reasonable. |
| Rejected RL, `2608.24024` and inverse rendering | Counterfactual assumption propagation. | Some useful reasoning, but claims exceed what the changed assumption alone establishes. Verifier failures also need interpretation, not blanket relabeling as bad science. |

No human approval records were written during this inspection. The Foundry
remains an experimental side feature: inspectability and verifier execution
are demonstrated, but neither automatic acceptance nor this sample establishes
that its full historical pool is ready for model training.
