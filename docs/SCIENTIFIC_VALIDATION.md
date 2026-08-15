# Scientific Curation Validation

The fixed paper list is `validation/scientific_papers.yaml`. It contains 37
verified arXiv identifiers split by paper into calibration and holdout sets.
The list deliberately covers theory, empirical work, systems, datasets,
benchmarks, visual evidence, technical reports, short papers, tables,
equations, and recent native-HTML candidates.

`review_status: pending` is intentional until a reviewer inspects the extracted
artifact. Predicted routes or LLM-generated labels must never be written into
human-label fields and called ground truth.

## Label file

Reviewers create `validation/scientific_labels.jsonl`, one row per paper:

```json
{"arxiv_id":"1706.03762","extraction_correct":true,"training_usefulness":4.5,"reasoning_evidence":0.9,"benchmark_evidence":0.1,"expected_route":"reasoning_candidate","sections":{"abstract":"keep","introduction":"keep","references":"remove"},"figure_ocr":[{"figure_id":"figure-1","cer":0.04,"wer":0.08,"numeric_exact_match":1.0}]}
```

Required review fields are extraction correctness, a 0..5 training-usefulness
score, 0..1 reasoning evidence, 0..1 benchmark evidence, expected route, and a
keep/remove decision for every extracted section. Papers with figures also get
OCR character error rate, word error rate, and numeric exact match for manually
transcribed crops.

## Evaluation protocol

1. Ingest the calibration split with the exact model and policy revisions that
   will be evaluated.
2. Export document decisions and section scores from the Documents API.
3. Review without seeing the predicted route or aggregate score.
4. Tune thresholds only on `calibration`.
5. Freeze the policy revision.
6. Evaluate once on `holdout` and report score MAE/rank correlation plus route
   precision, recall, and confusion matrix.
7. Record extraction error counts by native HTML, clean PDF, degraded PDF,
   tables, equations, and figures.

The current three-paper laptop run is a pipeline acceptance test, not this
scientific evaluation. A 37-paper CPU run is intentionally separate so a demo
restart never silently changes the labelled benchmark.

## FinePDFs v1 versus v2

Export the reviewed sample as JSONL and run:

```bash
uv run python scripts/compare_finepdfs_classifiers.py \
  validation/scientific_scoring_sample.jsonl \
  --sample-unit section \
  --output validation/finepdfs-v1-v2-report.json
```

The command pins v1 at `d1d20d432b6588831bfec203e11aeb9195ef32fd`
and v2 at `90ddef285f67230389057c14b2f6bbfeb70d40ea`, loads them sequentially,
and reports per-section scores plus MAE when reviewed `expected_score` values
are present. The unlabelled three-paper pilot report is checked in at
`validation/finepdfs-v1-v2-pilot.json`: across the same 30 role-stratified
sections, v1 averaged 0.816 and v2 averaged 3.646, with v2 higher on all 30.
That validates the runnable comparison and supports v2 as the working default;
only reviewed holdout labels can establish accuracy or reverse the selection.
