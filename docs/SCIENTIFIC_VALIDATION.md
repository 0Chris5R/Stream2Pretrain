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
{"arxiv_id":"1706.03762","extraction_correct":true,"training_usefulness":4.5,"reasoning_evidence":0.9,"expected_route":"posttrain_candidate","sections":{"abstract":"keep","introduction":"keep","references":"remove"},"figure_ocr":[{"figure_id":"figure-1","cer":0.04,"wer":0.08,"numeric_exact_match":1.0}]}
```

Required review fields are extraction correctness, a 0..5 training-usefulness
score, 0..1 reasoning evidence, expected route, and a
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

The extraction review is separate from classifier training/evaluation.
[The four classifier contract](CLASSIFIERS.md) defines document-grouped splits,
ordinal metrics and threshold evaluation. A successful runtime test is not
a human quality label.
