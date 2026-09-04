# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "accelerate>=1.10,<2",
#   "datasets>=4.0,<5",
#   "marimo>=0.18,<1",
#   "scikit-learn>=1.7,<2",
#   "scipy>=1.16,<2",
#   "torch>=2.7,<3",
#   "transformers>=4.57,<5",
# ]
# ///

"""One-cell Molab notebook for four Stream2Pretrain ModernBERT classifiers."""

import marimo

__generated_with = "0.18.1"
app = marimo.App(width="full", app_title="Stream2Pretrain classifier training")


@app.cell(hide_code=False)
def _():
    import hashlib
    import json
    import math
    import shutil
    import time
    from collections import defaultdict
    from pathlib import Path

    import numpy as np
    import torch
    import torch.nn.functional as functional
    from datasets import DatasetDict, load_dataset
    from scipy.stats import spearmanr
    from sklearn.metrics import cohen_kappa_score, confusion_matrix
    from transformers import (
        AutoModelForSequenceClassification,
        AutoTokenizer,
        DataCollatorWithPadding,
        Trainer,
        TrainingArguments,
        set_seed,
    )

    # Upload this file beside the notebook in Molab, then attach the RTX Pro
    # 6000 Blackwell before running this single cell.
    data_filename = "stream2pretrain-classifier-labels.jsonl.gz"
    base_model = "answerdotai/ModernBERT-base"
    base_revision = "8949b909ec900327062f0ebf497f51aef5e6f0c8"
    max_length = 8192
    stride = 512
    seed = 20260901
    output_root = Path("stream2pretrain-modernbert-classifiers")
    tasks = (
        {
            "name": "arxiv-pretrain-quality",
            "source": "arxiv",
            "label": "label_pretrain",
            "document_label": "document_pretrain",
            "aggregation": "weighted_mean",
        },
        {
            "name": "hf-pretrain-quality",
            "source": "hf",
            "label": "label_pretrain",
            "document_label": "document_pretrain",
            "aggregation": "weighted_mean",
        },
        {
            "name": "arxiv-math-reasoning",
            "source": "arxiv",
            "label": "label_math",
            "document_label": "document_math",
            "aggregation": "maximum",
        },
        {
            "name": "arxiv-posttrain-suitability",
            "source": "arxiv",
            "label": "label_posttrain",
            "document_label": "document_posttrain",
            "aggregation": "maximum",
        },
    )

    _matches = list(Path(".").rglob(data_filename))
    if len(_matches) != 1:
        raise FileNotFoundError(f"Expected exactly one uploaded {data_filename}; found {_matches}")
    _data_path = _matches[0]
    if not torch.cuda.is_available():
        raise RuntimeError(
            "No CUDA GPU is attached. In Molab attach the RTX Pro 6000 Blackwell, "
            "restart the runtime, and run this cell again."
        )
    _gpu_name = torch.cuda.get_device_name(0)
    _gpu_memory_gib = torch.cuda.get_device_properties(0).total_memory / 2**30
    print(f"GPU: {_gpu_name} ({_gpu_memory_gib:.1f} GiB)")
    print(f"Data: {_data_path} ({_data_path.stat().st_size / 2**20:.1f} MiB)")

    set_seed(seed)
    torch.backends.cuda.matmul.allow_tf32 = True
    torch.backends.cudnn.allow_tf32 = True
    output_root.mkdir(parents=True, exist_ok=True)

    _raw = load_dataset("json", data_files=str(_data_path), split="train")
    _required = {
        "document_id",
        "source_family",
        "model_input",
        "split",
        "label_pretrain",
        "label_math",
        "label_posttrain",
        "document_pretrain",
        "document_math",
        "document_posttrain",
        "section_id",
        "section_type",
        "source_feed",
    }
    _missing_columns = _required - set(_raw.column_names)
    if _missing_columns:
        raise ValueError(f"Training data is missing columns: {sorted(_missing_columns)}")
    if set(_raw.unique("split")) != {"train", "test"}:
        raise ValueError("Training data must contain document-level train and test splits")

    _tokenizer = AutoTokenizer.from_pretrained(
        base_model,
        revision=base_revision,
        use_fast=True,
    )

    class _WeightedTrainer(Trainer):
        def __init__(self, *args, class_weights, **kwargs):
            super().__init__(*args, **kwargs)
            self._class_weights = class_weights

        def compute_loss(
            self,
            model,
            inputs,
            return_outputs=False,
            num_items_in_batch=None,
        ):
            labels = inputs.pop("labels")
            outputs = model(**inputs)
            classification_loss = functional.cross_entropy(
                outputs.logits,
                labels,
                weight=self._class_weights.to(outputs.logits.device),
                reduction="sum",
            )
            probabilities = torch.softmax(outputs.logits.float(), dim=-1)
            bins = torch.arange(6, device=outputs.logits.device, dtype=torch.float32)
            expected_scores = (probabilities * bins).sum(dim=-1)
            ordinal_loss = functional.mse_loss(
                expected_scores / 5.0,
                labels.float() / 5.0,
                reduction="sum",
            )
            # Both terms are dimensionless. Cross entropy learns the labelled
            # bin, while normalized ordinal MSE makes a five-bin miss cost more
            # than a one-bin miss without introducing a tuned loss weight.
            accumulated_items = torch.as_tensor(
                num_items_in_batch if num_items_in_batch is not None else labels.numel(),
                device=outputs.logits.device,
                dtype=torch.float32,
            ).clamp_min(1.0)
            loss = (classification_loss + ordinal_loss) / accumulated_items
            return (loss, outputs) if return_outputs else loss

    def _tokenize_split(dataset, label_column, document_label_column):
        def _tokenize(batch):
            encoded = _tokenizer(
                batch["model_input"],
                truncation=True,
                max_length=max_length,
                stride=stride,
                return_overflowing_tokens=True,
                return_length=True,
            )
            mapping = encoded.pop("overflow_to_sample_mapping")
            encoded["labels"] = [int(batch[label_column][index]) for index in mapping]
            encoded["document_id"] = [batch["document_id"][index] for index in mapping]
            encoded["section_id"] = [batch["section_id"][index] for index in mapping]
            encoded["section_type"] = [batch["section_type"][index] for index in mapping]
            encoded["source_feed"] = [batch["source_feed"][index] for index in mapping]
            encoded["section_characters"] = [len(batch["text"][index]) for index in mapping]
            encoded["document_labels"] = [
                int(batch[document_label_column][index]) for index in mapping
            ]
            return encoded

        return dataset.map(
            _tokenize,
            batched=True,
            batch_size=64,
            remove_columns=dataset.column_names,
            desc=f"Tokenize {label_column}",
        )

    def _predictions(logits, temperature=1.0):
        probabilities = torch.softmax(
            torch.tensor(logits, dtype=torch.float32) / temperature,
            dim=-1,
        )
        expected = (probabilities * torch.arange(6, dtype=torch.float32)).sum(dim=-1).numpy()
        predictions = np.clip(np.rint(expected), 0, 5).astype(np.int64)
        entropy = -(probabilities * probabilities.clamp_min(1e-12).log()).sum(dim=-1)
        confidence = (1.0 - entropy / math.log(6.0)).clamp(0.0, 1.0).numpy()
        return expected, confidence, predictions

    def _classification_metrics(labels, expected, confidence, predictions):
        labels = np.asarray(labels, dtype=np.int64)
        expected = np.asarray(expected, dtype=np.float64)
        confidence = np.asarray(confidence, dtype=np.float64)
        predictions = np.asarray(predictions, dtype=np.int64)
        correlation = spearmanr(labels, expected).statistic
        qwk = cohen_kappa_score(labels, predictions, weights="quadratic")
        exact = predictions == labels
        return {
            "examples": len(labels),
            "qwk": float(0.0 if math.isnan(qwk) else qwk),
            "mae": float(np.mean(np.abs(expected - labels))),
            "exact_accuracy": float(np.mean(exact)),
            "within_one_accuracy": float(np.mean(np.abs(predictions - labels) <= 1)),
            "spearman": float(0.0 if math.isnan(correlation) else correlation),
            "mean_confidence": float(np.mean(confidence)),
            "mean_confidence_exact": (float(np.mean(confidence[exact])) if np.any(exact) else None),
            "mean_confidence_inexact": (
                float(np.mean(confidence[~exact])) if np.any(~exact) else None
            ),
            "label_distribution": np.bincount(labels, minlength=6).tolist(),
            "prediction_distribution": np.bincount(predictions, minlength=6).tolist(),
            "confusion_matrix": confusion_matrix(
                labels, predictions, labels=list(range(6))
            ).tolist(),
        }

    def _metrics(eval_prediction):
        logits, labels = eval_prediction
        expected, confidence, predictions = _predictions(logits)
        metrics = _classification_metrics(labels, expected, confidence, predictions)
        metrics.pop("confusion_matrix")
        metrics.pop("label_distribution")
        metrics.pop("prediction_distribution")
        metrics.pop("examples")
        return metrics

    def _effective_class_weights(labels):
        counts = np.bincount(np.asarray(labels, dtype=np.int64), minlength=6)
        beta = 0.999
        effective = 1.0 - np.power(beta, counts)
        weights = np.where(counts > 0, (1.0 - beta) / effective, 0.0)
        weights = weights / weights[counts > 0].mean()
        return torch.tensor(weights, dtype=torch.float32), counts.tolist()

    def _bootstrap_document_intervals(labels, expected, predictions):
        labels = np.asarray(labels, dtype=np.int64)
        expected = np.asarray(expected, dtype=np.float64)
        predictions = np.asarray(predictions, dtype=np.int64)
        rng = np.random.default_rng(seed)
        maes = []
        qwks = []
        for _ in range(500):
            indices = rng.integers(0, len(labels), size=len(labels))
            sample_labels = labels[indices]
            sample_predictions = predictions[indices]
            maes.append(float(np.mean(np.abs(expected[indices] - sample_labels))))
            qwk = cohen_kappa_score(
                sample_labels,
                sample_predictions,
                weights="quadratic",
            )
            if not math.isnan(qwk):
                qwks.append(float(qwk))
        return {
            "replicates": 500,
            "mae_95_ci": np.quantile(maes, [0.025, 0.975]).tolist(),
            "qwk_95_ci": np.quantile(qwks, [0.025, 0.975]).tolist(),
        }

    def _production_evaluation(
        tokenized_test,
        logits,
        temperature,
        aggregation,
        train_section_labels,
        train_document_labels,
    ):
        # Average logits for overflow chunks so every original section is one
        # evaluation unit, then apply the same document aggregation intended
        # for production. Document IDs never cross dataset splits.
        chunk_logits = np.asarray(logits, dtype=np.float64)
        sections = {}
        for index in range(len(tokenized_test)):
            key = (
                tokenized_test[index]["document_id"],
                tokenized_test[index]["section_id"],
            )
            entry = sections.setdefault(
                key,
                {
                    "logits": [],
                    "token_lengths": [],
                    "label": int(tokenized_test[index]["labels"]),
                    "document_label": int(tokenized_test[index]["document_labels"]),
                    "section_type": tokenized_test[index]["section_type"],
                    "source_feed": tokenized_test[index]["source_feed"],
                    "characters": int(tokenized_test[index]["section_characters"]),
                },
            )
            entry["logits"].append(chunk_logits[index])
            entry["token_lengths"].append(int(tokenized_test[index]["length"]))

        section_rows = []
        for (document_id, section_id), entry in sections.items():
            section_logits = np.mean(entry["logits"], axis=0, keepdims=True)
            expected, confidence, predicted = _predictions(section_logits, temperature)
            section_rows.append(
                {
                    **entry,
                    "document_id": document_id,
                    "section_id": section_id,
                    "expected": float(expected[0]),
                    "confidence": float(confidence[0]),
                    "predicted": int(predicted[0]),
                    "tokens": max(
                        1,
                        sum(entry["token_lengths"])
                        - stride * max(0, len(entry["token_lengths"]) - 1),
                    ),
                }
            )

        section_labels = np.asarray([row["label"] for row in section_rows])
        section_expected = np.asarray([row["expected"] for row in section_rows])
        section_confidence = np.asarray([row["confidence"] for row in section_rows])
        section_predictions = np.asarray([row["predicted"] for row in section_rows])
        section_metrics = _classification_metrics(
            section_labels,
            section_expected,
            section_confidence,
            section_predictions,
        )
        section_majority = int(
            np.bincount(np.asarray(train_section_labels, dtype=np.int64), minlength=6).argmax()
        )
        section_baseline = _classification_metrics(
            section_labels,
            np.full(len(section_labels), section_majority, dtype=np.float64),
            np.zeros(len(section_labels), dtype=np.float64),
            np.full(len(section_labels), section_majority, dtype=np.int64),
        )

        slices = {}
        for field in ("section_type", "source_feed"):
            for value in sorted({row[field] for row in section_rows}):
                indices = [i for i, row in enumerate(section_rows) if row[field] == value]
                if len(indices) < 20:
                    continue
                slices[f"{field}:{value}"] = _classification_metrics(
                    section_labels[indices],
                    section_expected[indices],
                    section_confidence[indices],
                    section_predictions[indices],
                )

        documents = defaultdict(list)
        for row in section_rows:
            documents[row["document_id"]].append(row)
        document_labels = []
        document_expected = []
        document_confidence = []
        for rows in documents.values():
            labels = {row["document_label"] for row in rows}
            if len(labels) != 1:
                raise ValueError("inconsistent document labels across sections")
            document_labels.append(labels.pop())
            if aggregation == "weighted_mean":
                weights = np.asarray([row["tokens"] for row in rows])
                document_expected.append(
                    float(np.average([row["expected"] for row in rows], weights=weights))
                )
                document_confidence.append(
                    float(np.average([row["confidence"] for row in rows], weights=weights))
                )
            else:
                highest_scoring_row = max(rows, key=lambda row: row["expected"])
                document_expected.append(highest_scoring_row["expected"])
                document_confidence.append(highest_scoring_row["confidence"])
        document_labels = np.asarray(document_labels, dtype=np.int64)
        document_expected = np.asarray(document_expected, dtype=np.float64)
        document_confidence = np.asarray(document_confidence, dtype=np.float64)
        document_predictions = np.clip(np.rint(document_expected), 0, 5).astype(np.int64)
        document_metrics = _classification_metrics(
            document_labels,
            document_expected,
            document_confidence,
            document_predictions,
        )
        document_metrics["bootstrap"] = _bootstrap_document_intervals(
            document_labels,
            document_expected,
            document_predictions,
        )
        document_majority = int(
            np.bincount(np.asarray(train_document_labels, dtype=np.int64), minlength=6).argmax()
        )
        document_baseline = _classification_metrics(
            document_labels,
            np.full(len(document_labels), document_majority, dtype=np.float64),
            np.zeros(len(document_labels), dtype=np.float64),
            np.full(len(document_labels), document_majority, dtype=np.int64),
        )
        return {
            "temperature": temperature,
            "section": section_metrics,
            "document": document_metrics,
            "section_majority_baseline": section_baseline,
            "document_majority_baseline": document_baseline,
            "slices": slices,
        }

    _data_sha256 = hashlib.sha256(_data_path.read_bytes()).hexdigest()
    _all_results = {}
    for _task in tasks:
        _started = time.time()
        _task_name = _task["name"]
        _task_dir = output_root / _task_name
        print(f"\n=== Training {_task_name} ===")
        _task_source = _task["source"]
        _source_data = _raw.filter(
            lambda row, expected=_task_source: row["source_family"] == expected,
            desc=f"Select {_task_source}",
        )
        _split_data = DatasetDict(
            {
                split_name: _source_data.filter(
                    lambda row, expected=split_name: row["split"] == expected,
                    desc=f"Select {split_name}",
                )
                for split_name in ("train", "test")
            }
        )
        _tokenized = DatasetDict(
            {
                split_name: _tokenize_split(
                    dataset,
                    _task["label"],
                    _task["document_label"],
                )
                for split_name, dataset in _split_data.items()
            }
        )
        _weights, _class_counts = _effective_class_weights(_tokenized["train"]["labels"])
        _model = AutoModelForSequenceClassification.from_pretrained(
            base_model,
            revision=base_revision,
            num_labels=6,
            id2label={index: str(index) for index in range(6)},
            label2id={str(index): index for index in range(6)},
            problem_type="single_label_classification",
            dtype=torch.bfloat16,
            attn_implementation="sdpa",
        )
        _model.config.stream2pretrain_task = _task_name
        _model.config.stream2pretrain_score_range = [0, 5]
        _model.config.stream2pretrain_input = "one complete extracted section"
        _model.config.stream2pretrain_output = {
            "score": "expected value of the six internal ordinal bins",
            "confidence": "one minus categorical entropy normalized by log(6)",
            "class": "score rounded to the nearest integer and clipped to 0-5",
        }

        _arguments = TrainingArguments(
            output_dir=str(_task_dir / "checkpoints"),
            num_train_epochs=4,
            learning_rate=2e-5,
            weight_decay=0.01,
            warmup_ratio=0.06,
            lr_scheduler_type="cosine",
            per_device_train_batch_size=8,
            per_device_eval_batch_size=16,
            gradient_accumulation_steps=4,
            bf16=True,
            tf32=True,
            optim="adamw_torch_fused",
            eval_strategy="no",
            save_strategy="epoch",
            logging_strategy="steps",
            logging_steps=50,
            load_best_model_at_end=False,
            save_total_limit=1,
            dataloader_num_workers=4,
            dataloader_pin_memory=True,
            group_by_length=True,
            length_column_name="length",
            report_to="none",
            seed=seed,
            data_seed=seed,
            remove_unused_columns=True,
        )
        _trainer = _WeightedTrainer(
            model=_model,
            args=_arguments,
            train_dataset=_tokenized["train"],
            data_collator=DataCollatorWithPadding(
                tokenizer=_tokenizer,
                pad_to_multiple_of=8,
            ),
            processing_class=_tokenizer,
            compute_metrics=_metrics,
            class_weights=_weights,
        )
        _trainer.train()
        _temperature = 1.0
        # Preserve length grouping for training speed, but restore sequential
        # ordering before matching prediction logits to held-out dataset rows.
        _trainer.args.group_by_length = False
        _test_prediction = _trainer.predict(_tokenized["test"])
        _train_document_labels = []
        _seen_train_documents = set()
        for _row in _split_data["train"]:
            if _row["document_id"] in _seen_train_documents:
                continue
            _seen_train_documents.add(_row["document_id"])
            _train_document_labels.append(int(_row[_task["document_label"]]))
        _test_metrics = _production_evaluation(
            _tokenized["test"],
            _test_prediction.predictions,
            _temperature,
            _task["aggregation"],
            _split_data["train"][_task["label"]],
            _train_document_labels,
        )
        _final_dir = _task_dir / "model"
        _trainer.save_model(str(_final_dir))
        _tokenizer.save_pretrained(str(_final_dir))
        (_final_dir / "stream2pretrain_calibration.json").write_text(
            json.dumps(
                {
                    "temperature": _temperature,
                    "calibration": "not_fit_to_keep_the_test_set_untouched",
                    "external_output": {
                        "score": "sum(bin_probability * bin), in [0, 5]",
                        "confidence": "1 - entropy(bin_probabilities) / log(6), in [0, 1]",
                        "class": "round(score), clipped to [0, 5]",
                    },
                    "training_objective": (
                        "effective-class-weighted cross entropy plus normalized "
                        "expected-score mean squared error"
                    ),
                    "aggregation": {
                        "pretrain_quality": (
                            "token-count-weighted section score and confidence means"
                        ),
                        "math_reasoning": (
                            "document score and confidence from the maximum-score section"
                        ),
                        "posttrain_suitability": (
                            "document score and confidence from the maximum-score section"
                        ),
                    },
                },
                indent=2,
            )
            + "\n"
        )
        _task_result = {
            "source": _task_source,
            "label_column": _task["label"],
            "base_model": base_model,
            "base_revision": base_revision,
            "data_sha256": _data_sha256,
            "max_length": max_length,
            "stride": stride,
            "seed": seed,
            "section_rows": {name: len(value) for name, value in _split_data.items()},
            "token_chunks": {name: len(value) for name, value in _tokenized.items()},
            "train_class_counts": _class_counts,
            "test_metrics": _test_metrics,
            "elapsed_seconds": round(time.time() - _started, 3),
        }
        (_task_dir / "training-result.json").write_text(
            json.dumps(_task_result, indent=2, sort_keys=True) + "\n"
        )
        _all_results[_task_name] = _task_result
        print(json.dumps(_task_result, indent=2))
        del _trainer, _model, _tokenized, _split_data, _source_data
        torch.cuda.empty_cache()

    _summary = {
        "completed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "gpu": _gpu_name,
        "gpu_memory_gib": round(_gpu_memory_gib, 3),
        "data_file": str(_data_path),
        "data_sha256": _data_sha256,
        "tasks": _all_results,
    }
    (output_root / "training-summary.json").write_text(
        json.dumps(_summary, indent=2, sort_keys=True) + "\n"
    )
    _archive = Path(
        shutil.make_archive(
            str(output_root),
            "gztar",
            root_dir=output_root.parent,
            base_dir=output_root.name,
        )
    )
    print(f"\nComplete. Download {_archive} and {output_root / 'training-summary.json'}")
    return


if __name__ == "__main__":
    app.run()
