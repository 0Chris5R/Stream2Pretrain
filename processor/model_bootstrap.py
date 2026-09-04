"""Download and validate the pinned CPU model bundle for the local profile.

The bundle lives in its own Podman volume instead of an image layer. This
keeps the laptop build from holding duplicate copies of the 4.4 GB KenLM
binary while preserving the exact same strict-mode runtime artifacts.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from huggingface_hub import snapshot_download

MODELS_DIR = Path(os.environ.get("S2P_MODELS_DIR", "/opt/models"))
REVISIONS = {
    "kenlm": os.environ.get("KENLM_REVISION", "3fbe35c83b1a39f420a345b7c96a186c8030d834"),
    "figure_classifier": os.environ.get(
        "FIGURE_CLASSIFIER_REVISION", "f859dfbff5c9916cd996942d4b0db7fa25808220"
    ),
}


def _snapshot(*, repo_id: str, revision: str, destination: Path, patterns: list[str]) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    snapshot_download(
        repo_id=repo_id,
        revision=revision,
        local_dir=destination,
        allow_patterns=patterns,
    )


def _bootstrap_kenlm() -> None:
    destination = MODELS_DIR / "kenlm"
    _snapshot(
        repo_id="edugp/kenlm",
        revision=REVISIONS["kenlm"],
        destination=destination,
        patterns=["wikipedia/en.arpa.bin", "wikipedia/en.sp.model"],
    )
    nested = destination / "wikipedia"
    for source_name, target_name in (
        ("en.arpa.bin", "en.arpa.bin"),
        ("en.sp.model", "en.sp.model"),
    ):
        source = nested / source_name
        target = destination / target_name
        if source.is_file() and not target.is_file():
            source.replace(target)
    shutil.rmtree(nested, ignore_errors=True)


def _bootstrap_docling() -> None:
    destination = MODELS_DIR / "docling"
    destination.mkdir(parents=True, exist_ok=True)
    if any(path.is_file() for path in destination.rglob("*")):
        return
    subprocess.run(
        [
            "docling-tools",
            "models",
            "download",
            "layout",
            "tableformer",
            "code_formula",
            "--output-dir",
            str(destination),
        ],
        check=True,
    )


def _bootstrap_tiktoken() -> None:
    destination = MODELS_DIR / "tiktoken"
    destination.mkdir(parents=True, exist_ok=True)
    os.environ["TIKTOKEN_CACHE_DIR"] = str(destination)
    import tiktoken

    tiktoken.get_encoding("cl100k_base")


def _validate() -> None:
    required = {
        "figure ONNX model": MODELS_DIR / "figure-classifier" / "model.onnx",
        "KenLM binary": MODELS_DIR / "kenlm" / "en.arpa.bin",
        "KenLM tokenizer": MODELS_DIR / "kenlm" / "en.sp.model",
    }
    manifest = json.loads((MODELS_DIR / "source-classifiers.json").read_text())
    for task in manifest["models"]:
        required[f"{task} weights"] = MODELS_DIR / task / "model.safetensors"
        required[f"{task} tokenizer"] = MODELS_DIR / task / "tokenizer.json"
    missing = [
        label for label, path in required.items() if not path.is_file() or path.stat().st_size == 0
    ]
    if missing:
        raise RuntimeError(f"model bootstrap incomplete: {', '.join(missing)}")
    docling = MODELS_DIR / "docling"
    if not any(path.is_file() for path in docling.rglob("*")):
        raise RuntimeError("model bootstrap incomplete: Docling artifacts")
    if not any(path.is_file() for path in (MODELS_DIR / "tiktoken").iterdir()):
        raise RuntimeError("model bootstrap incomplete: tiktoken vocabulary")


def main() -> None:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    manifest = Path(__file__).with_name("source-classifiers.json")
    subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve().parents[1] / "scripts/download_source_classifiers.py"),
            str(manifest),
            str(MODELS_DIR),
        ],
        check=True,
    )
    shutil.copyfile(manifest, MODELS_DIR / manifest.name)
    print("Downloading pinned document-figure ONNX classifier", flush=True)
    _snapshot(
        repo_id="docling-project/DocumentFigureClassifier-v2.5",
        revision=REVISIONS["figure_classifier"],
        destination=MODELS_DIR / "figure-classifier",
        patterns=["model.onnx", "*.json"],
    )
    print("Downloading pinned KenLM model", flush=True)
    _bootstrap_kenlm()
    print("Downloading pinned Docling CPU artifacts", flush=True)
    _bootstrap_docling()
    print("Caching pinned tokenizer vocabulary", flush=True)
    _bootstrap_tiktoken()
    _validate()
    (MODELS_DIR / "revisions.json").write_text(
        json.dumps(REVISIONS, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    (MODELS_DIR / ".ready").write_text("validated\n", encoding="utf-8")
    print("Pinned CPU model bundle is complete and validated", flush=True)


if __name__ == "__main__":
    main()
