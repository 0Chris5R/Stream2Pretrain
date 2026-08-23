"""Seed loader for ``HuggingFaceTB/stack-edu`` filtered to Python+ML.

Stack-Edu is the educational-quality code subset Dolma 3 Mix uses for the
0.41T-token code component. Its dataset wrapper is not inherited; an explicit
allowlisted per-file SPDX licence is required.

Filter strategy:

- Keep rows whose ``language`` (case-insensitive) is in ``LANGUAGES`` (Python).
- Within Python rows, keep those whose ``repository_name`` matches one of
  the curated AI/ML repos in ``ML_REPOS`` *or* whose ``path`` references
  any of the AI/ML keyword tokens in ``ML_KEYWORDS``.

The keyword fallback exists because ``repository_name`` may be absent on a
shard; the keyword list is conservative on purpose.
"""

from __future__ import annotations

import gzip
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from typing import Any

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from processor.seed.cursor import SeedCursor
from processor.seed.types import SeedDocument

REPO_ID: str = "HuggingFaceTB/stack-edu"
DATASET_REVISION: str = "eeec5caac5cc3758a18f1d3ba4416837a9ba814c"
SOFTWARE_HERITAGE_BUCKET: str = "softwareheritage"
LANGUAGES: frozenset[str] = frozenset({"python"})

# Curated ML / inference / training repositories. Subset of SOURCES.md
# Phase-1 GitHub Releases allowlist plus a few additional ones common in
# Stack-Edu shards.
ML_REPOS: frozenset[str] = frozenset(
    {
        "huggingface/transformers",
        "huggingface/datasets",
        "huggingface/diffusers",
        "huggingface/accelerate",
        "huggingface/peft",
        "huggingface/tokenizers",
        "huggingface/datatrove",
        "vllm-project/vllm",
        "pytorch/pytorch",
        "pytorch/audio",
        "pytorch/vision",
        "pytorch/torchtitan",
        "tensorflow/tensorflow",
        "google/jax",
        "openai/whisper",
        "openai/triton",
        "triton-lang/triton",
        "openai/tiktoken",
        "google-deepmind/penzai",
        "google-deepmind/gemma",
        "meta-llama/llama",
        "ggml-org/llama.cpp",
        "facebookresearch/llama-stack",
        "ogx-ai/ogx",
        "mistralai/mistral-src",
        "mistralai/mistral-inference",
        "allenai/dolma",
        "anthropics/courses",
        "apple/ml-tic-lm",
        "apache/iceberg",
        "allenai/OLMo",
        "mlfoundations/dclm",
        "mlfoundations/open_clip",
        "NVIDIA-NeMo/Curator",
        "karpathy/llm.c",
        "karpathy/nanoGPT",
        "karpathy/minGPT",
        "bytewax/bytewax",
        "firecrawl/firecrawl",
        "MaterializeInc/materialize",
        "microsoft/onnxruntime",
        "pathwaycom/pathway",
        "redpanda-data/redpanda",
        "risingwavelabs/risingwave",
        "tinygrad/tinygrad",
        "unclecode/crawl4ai",
        "rwightman/pytorch-image-models",
        "huggingface/pytorch-image-models",
        "unslothai/unsloth",
        "Lightning-AI/pytorch-lightning",
        "huggingface/lerobot",
    }
)

# Lowercase tokens we look for in ``path`` when ``repository_name`` is
# missing. Matched as substrings.
ML_KEYWORDS: tuple[str, ...] = (
    "/transformer",
    "/attention",
    "/tokenizer",
    "/pretrain",
    "/pretraining",
    "/finetune",
    "/finetuning",
    "/lora",
    "/peft",
    "/quantize",
    "/inference",
    "/dataloader",
    "/dataset",
    "/embedding",
    "/training",
    "/llm",
    "/nlp",
    "/torchrun",
    "/megatron",
)


def is_python(row: dict[str, Any]) -> bool:
    """Case-insensitive language check."""
    lang = row.get("language") or row.get("lang") or row.get("file_language")
    if not isinstance(lang, str):
        return False
    return lang.strip().lower() in LANGUAGES


def is_ml_relevant(row: dict[str, Any]) -> bool:
    """True if the row's repo or path looks AI/ML on-topic."""
    repo = row.get("repository_name") or row.get("repo_name") or row.get("repo")
    if isinstance(repo, str) and repo in ML_REPOS:
        return True
    path = row.get("path") or row.get("file_path") or ""
    if isinstance(path, str):
        lower = path.lower()
        if any(tok in lower for tok in ML_KEYWORDS):
            return True
    return False


def derive_valid_from(row: dict[str, Any]) -> datetime:
    """Pick a per-row commit timestamp; fall back to Stack-Edu cutoff.

    Stack-Edu inherits Stack-v2 commit metadata. Field name is one of
    ``commit_date`` / ``last_commit_date`` / ``date`` depending on the
    shard.
    """
    for key in ("commit_date", "last_commit_date", "date"):
        raw = row.get(key)
        if isinstance(raw, str) and raw:
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                return dt.astimezone(UTC)
            except ValueError:
                continue
        if isinstance(raw, datetime):
            dt = raw
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)
    return datetime(2024, 6, 1, tzinfo=UTC)


def native_id_for(row: dict[str, Any]) -> str:
    """Prefer the blob id (content hash) so reruns on the same shard
    produce identical native ids."""
    for key in ("blob_id", "hexsha", "id", "path"):
        raw = row.get(key)
        if isinstance(raw, str) and raw:
            return raw
    return ""


def license_for(row: dict[str, Any]) -> tuple[str | None, str]:
    """Pick the per-file SPDX licence; a dataset wrapper cannot license code."""
    raw = row.get("license") or row.get("license_spdx")
    if isinstance(raw, str) and raw.strip():
        return raw.strip(), "dataset_metadata"
    for key in ("detected_licenses", "detected_licenses_right"):
        detected = row.get(key)
        if not isinstance(detected, list):
            continue
        unique = sorted(
            {value.strip() for value in detected if isinstance(value, str) and value.strip()}
        )
        # Multiple distinct detectors are ambiguous without an SPDX AND/OR
        # expression in the source row. Fail closed instead of guessing.
        if len(unique) == 1:
            return unique[0], "dataset_metadata"
    return None, "unknown"


def fetch_software_heritage_blob(blob_id: str) -> str:
    """Fetch one exact Stack-Edu blob from the public SWH S3 bucket.

    Stack-Edu intentionally contains identifiers and licence metadata only.
    This callable is attached lazily to :class:`SeedDocument` so the S3 body
    request cannot start until the item admission has been acknowledged.
    """
    client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    response = client.get_object(
        Bucket=SOFTWARE_HERITAGE_BUCKET,
        Key=f"content/{blob_id}",
    )
    with gzip.GzipFile(fileobj=response["Body"]) as stream:
        return stream.read().decode("utf-8", errors="replace")


def to_seed_document(row: dict[str, Any]) -> SeedDocument | None:
    """Convert one Stack-Edu row to a :class:`SeedDocument` if on-domain."""
    if not is_python(row):
        return None
    if not is_ml_relevant(row):
        return None
    nid = native_id_for(row)
    if not nid:
        return None
    raw_text = row.get("content") or row.get("text") or row.get("source")
    text = raw_text if isinstance(raw_text, str) and raw_text.strip() else ""
    repo = row.get("repository_name") or row.get("repo_name") or ""
    path = row.get("path") or ""
    valid_from = derive_valid_from(row)
    spdx, spdx_source = license_for(row)
    revision_raw = (
        row.get("commit_hash")
        or row.get("commit_sha")
        or row.get("hexsha")
        or row.get("blob_id")
        or nid
    )
    revision = str(revision_raw)
    if isinstance(repo, str) and isinstance(path, str) and repo and path:
        url = f"https://github.com/{repo}/blob/{revision}/{path}"
    else:
        url = f"hf://{REPO_ID}/{nid}"
    extra: dict[str, str] = {"language": "Python"}
    if isinstance(repo, str) and repo:
        extra["repository_name"] = repo
    return SeedDocument(
        repo_id=REPO_ID,
        native_id=nid,
        url=url,
        title=path if isinstance(path, str) and path else None,
        text=text,
        lang="en",  # code is treated as ``en`` for the SilverRecord lang tag
        valid_from=valid_from,
        source_format="code",
        extraction_pipeline="stack-edu-2024",
        spdx_license=spdx,
        spdx_license_source=spdx_source,  # type: ignore[arg-type]
        license_resolver="stack-edu-file-item-field",
        license_evidence_url=(
            f"https://huggingface.co/datasets/{REPO_ID}/tree/{DATASET_REVISION}/Python"
        ),
        license_evidence_revision=f"{DATASET_REVISION}:{revision}",
        license_evidence_scope="file" if spdx else "unknown",
        body_loader=(None if text else lambda blob_id=nid: fetch_software_heritage_blob(blob_id)),
        extra=extra,
    )


def iter_documents(
    cursor: SeedCursor,
    *,
    rows: Iterable[dict[str, Any]] | None = None,
    max_docs: int | None = None,
) -> Iterator[SeedDocument]:
    """Stream Stack-Edu rows passing the Python+ML filter."""
    if rows is None:
        rows = load_hf_stream()
    emitted = 0
    for row in rows:
        if max_docs is not None and emitted >= max_docs:
            return
        doc = to_seed_document(row)
        if doc is None:
            continue
        if cursor.should_skip(doc.native_id):
            continue
        yield doc
        emitted += 1


def load_hf_stream() -> Iterable[dict[str, Any]]:
    """Construct the streaming iterator. Stack-Edu is sharded by language."""
    from datasets import load_dataset

    # The pinned repository exposes a capitalized ``Python`` config and no
    # content column. Retained bodies are resolved lazily from SWH by blob id.
    return load_dataset(  # type: ignore[return-value]
        REPO_ID,
        name="Python",
        split="train",
        streaming=True,
        revision=DATASET_REVISION,
    )


__all__ = [
    "DATASET_REVISION",
    "LANGUAGES",
    "ML_KEYWORDS",
    "ML_REPOS",
    "REPO_ID",
    "derive_valid_from",
    "fetch_software_heritage_blob",
    "is_ml_relevant",
    "is_python",
    "iter_documents",
    "license_for",
    "load_hf_stream",
    "native_id_for",
    "to_seed_document",
]
