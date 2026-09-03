"""Package only the two completed deployment models from extracted Kaggle output."""

from __future__ import annotations

import argparse
import hashlib
import json
import tarfile
from pathlib import Path

TASKS = ("arxiv-pretrain-quality", "hf-pretrain-quality")
FILES = (
    "config.json",
    "model.safetensors",
    "special_tokens_map.json",
    "tokenizer.json",
    "tokenizer_config.json",
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    models = {}
    for task in TASKS:
        model = args.root / task / "model"
        config = json.loads((model / "config.json").read_text())
        if config.get("stream2pretrain_task") != task or len(config["id2label"]) != 6:
            raise ValueError(f"Wrong classifier checkpoint: {task}")
        result = json.loads((args.root / task / "training-result.json").read_text())
        archive = args.output / f"{task}.tar"
        weights_sha = hashlib.file_digest(
            (model / "model.safetensors").open("rb"), "sha256"
        ).hexdigest()
        with tarfile.open(archive, "w") as bundle:
            for name in FILES:
                bundle.add(model / name, arcname=f"{task}/{name}")
        archive_sha = hashlib.file_digest(archive.open("rb"), "sha256").hexdigest()
        models[task] = {
            "url": f"https://github.com/0Chris5R/Stream2Pretrain/releases/download/source-classifiers-2026-09-03/{archive.name}",
            "archive_sha256": archive_sha,
            "weights_sha256": weights_sha,
            "revision": f"{task}@sha256:{weights_sha}",
            "base_model": result["base_model"],
            "base_revision": result["base_revision"],
        }
    print(json.dumps({"version": "source-modernbert-2026-09-03", "models": models}, indent=2))


if __name__ == "__main__":
    main()
