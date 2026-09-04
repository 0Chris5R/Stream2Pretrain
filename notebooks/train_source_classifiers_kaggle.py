"""Train the four Stream2Pretrain classifiers on a Kaggle 2x T4 notebook.

Upload this script and ``stream2pretrain-classifier-labels.jsonl.gz`` as a
Kaggle notebook input, select the 2x T4 accelerator, and run the launcher cell
shown in the repository handoff. The parent process assigns two independent
classifier jobs to each GPU so both T4s remain useful without distributed
training overhead.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import subprocess
import sys
import tarfile
import time
from collections import defaultdict
from pathlib import Path

BASE_MODEL = "answerdotai/ModernBERT-base"
BASE_REVISION = "8949b909ec900327062f0ebf497f51aef5e6f0c8"
DATA_FILENAMES = (
    "stream2pretrain-classifier-labels.jsonl.gz",
    "stream2pretrain-classifier-labels.jsonl",
)
MAX_LENGTH = 8192
STRIDE = 512
SEED = 20260901
OUTPUT_ROOT = Path("/kaggle/working/stream2pretrain-modernbert-classifiers")
MODEL_ROOT = Path("/kaggle/working/modernbert-base")

TASKS = {
    "arxiv-pretrain-quality": {
        "source": "arxiv",
        "label": "label_pretrain",
        "document_label": "document_pretrain",
        "aggregation": "weighted_mean",
    },
    "hf-pretrain-quality": {
        "source": "hf",
        "label": "label_pretrain",
        "document_label": "document_pretrain",
        "aggregation": "weighted_mean",
    },
    "arxiv-math-reasoning": {
        "source": "arxiv",
        "label": "label_math",
        "document_label": "document_math",
        "aggregation": "maximum",
    },
    "arxiv-posttrain-suitability": {
        "source": "arxiv",
        "label": "label_posttrain",
        "document_label": "document_posttrain",
        "aggregation": "maximum",
    },
}

GPU_ASSIGNMENTS = {
    "0": ("arxiv-pretrain-quality", "arxiv-posttrain-suitability"),
    "1": ("hf-pretrain-quality", "arxiv-math-reasoning"),
}


def find_data() -> Path:
    search_roots = (Path("/kaggle/input"), Path("/kaggle/working/uploads"))
    matches = [
        match
        for root in search_roots
        if root.exists()
        for filename in DATA_FILENAMES
        for match in root.rglob(filename)
    ]
    if len(matches) != 1:
        raise FileNotFoundError(f"Expected exactly one uploaded labels JSONL; found {matches}.")
    return matches[0]


def prepare_model() -> None:
    from huggingface_hub import snapshot_download

    if (MODEL_ROOT / "config.json").exists():
        return
    snapshot_download(
        repo_id=BASE_MODEL,
        revision=BASE_REVISION,
        local_dir=MODEL_ROOT,
    )


def class_weights(labels):
    import numpy as np
    import torch

    counts = np.bincount(np.asarray(labels, dtype=np.int64), minlength=6)
    beta = 0.999
    effective = 1.0 - np.power(beta, counts)
    weights = np.where(counts > 0, (1.0 - beta) / effective, 0.0)
    weights = weights / weights[counts > 0].mean()
    return torch.tensor(weights, dtype=torch.float32), counts.tolist()


def predictions(logits, temperature: float = 1.0):
    import numpy as np
    import torch

    probabilities = torch.softmax(
        torch.as_tensor(logits, dtype=torch.float32) / temperature,
        dim=-1,
    )
    bins = torch.arange(6, dtype=torch.float32)
    scores = (probabilities * bins).sum(dim=-1).numpy()
    classes = np.clip(np.rint(scores), 0, 5).astype(np.int64)
    entropy = -(probabilities * probabilities.clamp_min(1e-12).log()).sum(dim=-1)
    confidence = (1.0 - entropy / math.log(6.0)).clamp(0.0, 1.0).numpy()
    return scores, confidence, classes


def classification_metrics(labels, scores, confidence, classes):
    import numpy as np
    from scipy.stats import spearmanr
    from sklearn.metrics import cohen_kappa_score, confusion_matrix

    labels = np.asarray(labels, dtype=np.int64)
    scores = np.asarray(scores, dtype=np.float64)
    confidence = np.asarray(confidence, dtype=np.float64)
    classes = np.asarray(classes, dtype=np.int64)
    correlation = spearmanr(labels, scores).statistic
    qwk = cohen_kappa_score(labels, classes, weights="quadratic")
    exact = classes == labels
    return {
        "examples": len(labels),
        "qwk": float(0.0 if math.isnan(qwk) else qwk),
        "mae": float(np.mean(np.abs(scores - labels))),
        "exact_accuracy": float(np.mean(exact)),
        "within_one_accuracy": float(np.mean(np.abs(classes - labels) <= 1)),
        "spearman": float(0.0 if math.isnan(correlation) else correlation),
        "mean_confidence": float(np.mean(confidence)),
        "mean_confidence_exact": (float(np.mean(confidence[exact])) if np.any(exact) else None),
        "mean_confidence_inexact": (float(np.mean(confidence[~exact])) if np.any(~exact) else None),
        "label_distribution": np.bincount(labels, minlength=6).tolist(),
        "prediction_distribution": np.bincount(classes, minlength=6).tolist(),
        "confusion_matrix": confusion_matrix(labels, classes, labels=list(range(6))).tolist(),
    }


def bootstrap_intervals(labels, scores, classes):
    import numpy as np
    from sklearn.metrics import cohen_kappa_score

    labels = np.asarray(labels, dtype=np.int64)
    scores = np.asarray(scores, dtype=np.float64)
    classes = np.asarray(classes, dtype=np.int64)
    rng = np.random.default_rng(SEED)
    maes = []
    qwks = []
    for _ in range(500):
        indices = rng.integers(0, len(labels), size=len(labels))
        sample_labels = labels[indices]
        sample_classes = classes[indices]
        maes.append(float(np.mean(np.abs(scores[indices] - sample_labels))))
        qwk = cohen_kappa_score(sample_labels, sample_classes, weights="quadratic")
        if not math.isnan(qwk):
            qwks.append(float(qwk))
    return {
        "replicates": 500,
        "mae_95_ci": np.quantile(maes, [0.025, 0.975]).tolist(),
        "qwk_95_ci": np.quantile(qwks, [0.025, 0.975]).tolist(),
    }


def production_evaluation(
    tokenized_test,
    logits,
    aggregation,
    train_section_labels,
    train_document_labels,
):
    import numpy as np

    chunk_logits = np.asarray(logits, dtype=np.float64)
    sections = {}
    for index in range(len(tokenized_test)):
        row = tokenized_test[index]
        key = (row["document_id"], row["section_id"])
        entry = sections.setdefault(
            key,
            {
                "logits": [],
                "token_lengths": [],
                "label": int(row["labels"]),
                "document_label": int(row["document_labels"]),
                "section_type": row["section_type"],
                "source_feed": row["source_feed"],
            },
        )
        entry["logits"].append(chunk_logits[index])
        entry["token_lengths"].append(int(row["length"]))

    section_rows = []
    for (document_id, section_id), entry in sections.items():
        score, confidence, predicted_class = predictions(
            np.mean(entry["logits"], axis=0, keepdims=True)
        )
        section_rows.append(
            {
                **entry,
                "document_id": document_id,
                "section_id": section_id,
                "score": float(score[0]),
                "confidence": float(confidence[0]),
                "class": int(predicted_class[0]),
                "tokens": max(
                    1,
                    sum(entry["token_lengths"]) - STRIDE * max(0, len(entry["token_lengths"]) - 1),
                ),
            }
        )

    section_labels = np.asarray([row["label"] for row in section_rows])
    section_scores = np.asarray([row["score"] for row in section_rows])
    section_confidence = np.asarray([row["confidence"] for row in section_rows])
    section_classes = np.asarray([row["class"] for row in section_rows])
    section_metrics = classification_metrics(
        section_labels,
        section_scores,
        section_confidence,
        section_classes,
    )
    section_majority = int(
        np.bincount(np.asarray(train_section_labels, dtype=np.int64), minlength=6).argmax()
    )
    section_baseline = classification_metrics(
        section_labels,
        np.full(len(section_labels), section_majority, dtype=np.float64),
        np.zeros(len(section_labels), dtype=np.float64),
        np.full(len(section_labels), section_majority, dtype=np.int64),
    )

    slices = {}
    for field in ("section_type", "source_feed"):
        for value in sorted({row[field] for row in section_rows}):
            indices = [i for i, row in enumerate(section_rows) if row[field] == value]
            if len(indices) >= 20:
                slices[f"{field}:{value}"] = classification_metrics(
                    section_labels[indices],
                    section_scores[indices],
                    section_confidence[indices],
                    section_classes[indices],
                )

    documents = defaultdict(list)
    for row in section_rows:
        documents[row["document_id"]].append(row)
    document_labels = []
    document_scores = []
    document_confidence = []
    for rows in documents.values():
        labels = {row["document_label"] for row in rows}
        if len(labels) != 1:
            raise ValueError("Inconsistent document labels across sections")
        document_labels.append(labels.pop())
        if aggregation == "weighted_mean":
            weights = np.asarray([row["tokens"] for row in rows])
            document_scores.append(
                float(np.average([row["score"] for row in rows], weights=weights))
            )
            document_confidence.append(
                float(np.average([row["confidence"] for row in rows], weights=weights))
            )
        else:
            best = max(rows, key=lambda row: row["score"])
            document_scores.append(best["score"])
            document_confidence.append(best["confidence"])

    document_labels = np.asarray(document_labels, dtype=np.int64)
    document_scores = np.asarray(document_scores, dtype=np.float64)
    document_confidence = np.asarray(document_confidence, dtype=np.float64)
    document_classes = np.clip(np.rint(document_scores), 0, 5).astype(np.int64)
    document_metrics = classification_metrics(
        document_labels,
        document_scores,
        document_confidence,
        document_classes,
    )
    document_metrics["bootstrap"] = bootstrap_intervals(
        document_labels,
        document_scores,
        document_classes,
    )
    document_majority = int(
        np.bincount(np.asarray(train_document_labels, dtype=np.int64), minlength=6).argmax()
    )
    document_baseline = classification_metrics(
        document_labels,
        np.full(len(document_labels), document_majority, dtype=np.float64),
        np.zeros(len(document_labels), dtype=np.float64),
        np.full(len(document_labels), document_majority, dtype=np.int64),
    )
    return {
        "section": section_metrics,
        "document": document_metrics,
        "section_majority_baseline": section_baseline,
        "document_majority_baseline": document_baseline,
        "slices": slices,
    }


def train_task(task_name: str, data_path: Path) -> None:
    import torch
    import torch.nn.functional as functional
    from datasets import DatasetDict, load_dataset
    from transformers import (
        AutoModelForSequenceClassification,
        AutoTokenizer,
        DataCollatorWithPadding,
        Trainer,
        TrainingArguments,
        set_seed,
    )
    from transformers.trainer_utils import get_last_checkpoint

    if not torch.cuda.is_available():
        raise RuntimeError("This worker cannot see its assigned T4 GPU")
    task = TASKS[task_name]
    started = time.time()
    set_seed(SEED)
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    task_dir = OUTPUT_ROOT / task_name
    task_dir.mkdir(parents=True, exist_ok=True)
    result_path = task_dir / "training-result.json"
    final_dir = task_dir / "model"
    if result_path.exists() and (final_dir / "config.json").exists():
        print(f"[{task_name}] already complete; preserving existing result", flush=True)
        return
    print(f"[{task_name}] GPU: {torch.cuda.get_device_name(0)}", flush=True)

    raw = load_dataset("json", data_files=str(data_path), split="train")
    source_data = raw.filter(
        lambda row: row["source_family"] == task["source"],
        desc=f"[{task_name}] select source",
    )
    split_data = DatasetDict(
        {
            split: source_data.filter(
                lambda row, expected=split: row["split"] == expected,
                desc=f"[{task_name}] select {split}",
            )
            for split in ("train", "test")
        }
    )
    tokenizer = AutoTokenizer.from_pretrained(MODEL_ROOT, local_files_only=True)

    def tokenize_split(dataset):
        def tokenize(batch):
            encoded = tokenizer(
                batch["model_input"],
                truncation=True,
                max_length=MAX_LENGTH,
                stride=STRIDE,
                return_overflowing_tokens=True,
                return_length=True,
            )
            mapping = encoded.pop("overflow_to_sample_mapping")
            encoded["labels"] = [int(batch[task["label"]][i]) for i in mapping]
            encoded["document_id"] = [batch["document_id"][i] for i in mapping]
            encoded["section_id"] = [batch["section_id"][i] for i in mapping]
            encoded["section_type"] = [batch["section_type"][i] for i in mapping]
            encoded["source_feed"] = [batch["source_feed"][i] for i in mapping]
            encoded["document_labels"] = [int(batch[task["document_label"]][i]) for i in mapping]
            return encoded

        return dataset.map(
            tokenize,
            batched=True,
            batch_size=32,
            remove_columns=dataset.column_names,
            desc=f"[{task_name}] tokenize",
        )

    tokenized = DatasetDict(
        {split: tokenize_split(dataset) for split, dataset in split_data.items()}
    )
    weights, counts = class_weights(tokenized["train"]["labels"])

    class OrdinalTrainer(Trainer):
        def __init__(self, *args, class_weights_tensor, **kwargs):
            super().__init__(*args, **kwargs)
            self.class_weights_tensor = class_weights_tensor

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
                weight=self.class_weights_tensor.to(outputs.logits.device),
                reduction="sum",
            )
            probabilities = torch.softmax(outputs.logits.float(), dim=-1)
            bins = torch.arange(6, device=outputs.logits.device, dtype=torch.float32)
            scores = (probabilities * bins).sum(dim=-1)
            ordinal_loss = functional.mse_loss(
                scores / 5.0,
                labels.float() / 5.0,
                reduction="sum",
            )
            accumulated_items = torch.as_tensor(
                num_items_in_batch if num_items_in_batch is not None else labels.numel(),
                device=outputs.logits.device,
                dtype=torch.float32,
            ).clamp_min(1.0)
            loss = (classification_loss + ordinal_loss) / accumulated_items
            return (loss, outputs) if return_outputs else loss

    model = AutoModelForSequenceClassification.from_pretrained(
        MODEL_ROOT,
        local_files_only=True,
        num_labels=6,
        id2label={index: str(index) for index in range(6)},
        label2id={str(index): index for index in range(6)},
        problem_type="single_label_classification",
        attn_implementation="sdpa",
    )
    model.config.stream2pretrain_task = task_name
    model.config.stream2pretrain_score_range = [0, 5]
    model.config.stream2pretrain_input = "one complete extracted section"
    model.config.stream2pretrain_output = {
        "score": "expected value of the six internal ordinal bins",
        "confidence": "one minus categorical entropy normalized by log(6)",
        "class": "score rounded to the nearest integer and clipped to 0-5",
    }
    model.config.use_cache = False
    model.gradient_checkpointing_enable(gradient_checkpointing_kwargs={"use_reentrant": False})

    arguments = TrainingArguments(
        output_dir=str(task_dir / "checkpoints"),
        num_train_epochs=4,
        learning_rate=2e-5,
        weight_decay=0.01,
        warmup_ratio=0.06,
        lr_scheduler_type="cosine",
        per_device_train_batch_size=4,
        per_device_eval_batch_size=4,
        gradient_accumulation_steps=8,
        auto_find_batch_size=True,
        gradient_checkpointing=True,
        gradient_checkpointing_kwargs={"use_reentrant": False},
        fp16=True,
        optim="adamw_torch_fused",
        eval_strategy="no",
        save_strategy="epoch",
        save_total_limit=1,
        save_safetensors=True,
        logging_strategy="steps",
        logging_steps=50,
        dataloader_num_workers=2,
        dataloader_pin_memory=True,
        group_by_length=True,
        length_column_name="length",
        eval_accumulation_steps=16,
        report_to="none",
        seed=SEED,
        data_seed=SEED,
        remove_unused_columns=True,
    )
    trainer = OrdinalTrainer(
        model=model,
        args=arguments,
        train_dataset=tokenized["train"],
        data_collator=DataCollatorWithPadding(
            tokenizer=tokenizer,
            pad_to_multiple_of=8,
        ),
        processing_class=tokenizer,
        class_weights_tensor=weights,
    )
    checkpoint_dir = task_dir / "checkpoints"
    last_checkpoint = get_last_checkpoint(str(checkpoint_dir)) if checkpoint_dir.exists() else None
    if last_checkpoint:
        print(f"[{task_name}] resuming from {last_checkpoint}", flush=True)
    trainer.train(resume_from_checkpoint=last_checkpoint)
    # Length grouping is useful for training throughput, but Transformers also
    # applies it to prediction. Production evaluation pairs logits with rows by
    # index, so prediction must use the sequential sampler.
    trainer.args.group_by_length = False
    test_prediction = trainer.predict(tokenized["test"])
    train_document_labels = []
    seen_documents = set()
    for row in split_data["train"]:
        if row["document_id"] not in seen_documents:
            seen_documents.add(row["document_id"])
            train_document_labels.append(int(row[task["document_label"]]))
    metrics = production_evaluation(
        tokenized["test"],
        test_prediction.predictions,
        task["aggregation"],
        split_data["train"][task["label"]],
        train_document_labels,
    )

    trainer.save_model(str(final_dir))
    tokenizer.save_pretrained(str(final_dir))
    elapsed = time.time() - started
    result = {
        "task": task_name,
        "source": task["source"],
        "base_model": BASE_MODEL,
        "base_revision": BASE_REVISION,
        "data_sha256": hashlib.sha256(data_path.read_bytes()).hexdigest(),
        "max_length": MAX_LENGTH,
        "stride": STRIDE,
        "seed": SEED,
        "section_rows": {name: len(value) for name, value in split_data.items()},
        "token_chunks": {name: len(value) for name, value in tokenized.items()},
        "train_class_counts": counts,
        "test_metrics": metrics,
        "elapsed_seconds": round(elapsed, 3),
        "train_chunks_per_second": round(len(tokenized["train"]) * 4 / elapsed, 6),
    }
    result_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    (final_dir / "stream2pretrain_inference.json").write_text(
        json.dumps(
            {
                "external_output": {
                    "score": "sum(bin_probability * bin), in [0, 5]",
                    "confidence": "1 - entropy(bin_probabilities) / log(6), in [0, 1]",
                    "class": "round(score), clipped to [0, 5]",
                },
                "training_objective": (
                    "effective-class-weighted cross entropy plus normalized "
                    "expected-score mean squared error"
                ),
                "aggregation": task["aggregation"],
            },
            indent=2,
        )
        + "\n"
    )
    print(f"[{task_name}] complete in {elapsed / 60:.1f} minutes", flush=True)
    print(json.dumps(result["test_metrics"]["document"], indent=2), flush=True)


def run_worker(task_names: list[str], data_path: Path) -> None:
    for task_name in task_names:
        train_task(task_name, data_path)


def launch() -> None:
    data_path = find_data()
    script_path = Path(__file__).resolve()
    prepare_model()
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    print(f"Data: {data_path} ({data_path.stat().st_size / 2**20:.1f} MiB)")
    print("Launching one independent classifier per T4", flush=True)
    workers = []
    for gpu, task_names in GPU_ASSIGNMENTS.items():
        env = os.environ.copy()
        env["CUDA_VISIBLE_DEVICES"] = gpu
        env["PYTHONUNBUFFERED"] = "1"
        env["HF_DATASETS_CACHE"] = f"/kaggle/working/hf-datasets-cache-gpu-{gpu}"
        command = [
            sys.executable,
            str(script_path),
            "--worker",
            "--data",
            str(data_path),
            "--tasks",
            *task_names,
        ]
        workers.append((gpu, subprocess.Popen(command, env=env)))
    failures = []
    for gpu, process in workers:
        return_code = process.wait()
        if return_code:
            failures.append((gpu, return_code))
    if failures:
        raise RuntimeError(f"Training workers failed: {failures}")

    summary = {}
    for task_name in TASKS:
        result_path = OUTPUT_ROOT / task_name / "training-result.json"
        summary[task_name] = json.loads(result_path.read_text())
    (OUTPUT_ROOT / "training-summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    # Retain each task's last epoch checkpoint as a Kaggle version output, but
    # keep optimizer states out of the final-model archive to avoid duplicating
    # several gigabytes.
    archive = OUTPUT_ROOT.parent / f"{OUTPUT_ROOT.name}.tar.gz"
    with tarfile.open(archive, "w:gz") as bundle:
        bundle.add(
            OUTPUT_ROOT / "training-summary.json",
            arcname=f"{OUTPUT_ROOT.name}/training-summary.json",
        )
        for task_name in TASKS:
            task_dir = OUTPUT_ROOT / task_name
            bundle.add(
                task_dir / "model",
                arcname=f"{OUTPUT_ROOT.name}/{task_name}/model",
            )
            bundle.add(
                task_dir / "training-result.json",
                arcname=f"{OUTPUT_ROOT.name}/{task_name}/training-result.json",
            )
    print(f"All four classifiers complete. Download: {archive}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--data", type=Path)
    parser.add_argument("--tasks", nargs="*", choices=tuple(TASKS))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.worker:
        if not args.data or not args.tasks:
            raise ValueError("Workers require --data and --tasks")
        run_worker(args.tasks, args.data)
    else:
        launch()
