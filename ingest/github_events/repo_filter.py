"""AI-repo filter list for the GitHub Events poller.

Matches the curated list from SOURCES.md plus a small set of well-known AI
orgs. The filter is intentionally narrow to keep the demo's signal-to-noise
high; expansion happens in Phase 2 via SourceFeed CRD updates.
"""

from __future__ import annotations

CURATED_REPOS: frozenset[str] = frozenset(
    {
        "huggingface/transformers",
        "vllm-project/vllm",
        "pytorch/pytorch",
        "ggml-org/llama.cpp",
        "karpathy/llm.c",
        "unslothai/unsloth",
        "meta-llama/llama",
        "openai/whisper",
        "anthropics/courses",
        "apple/ml-tic-lm",
        "mlfoundations/dclm",
        "huggingface/datatrove",
        "NVIDIA-NeMo/Curator",
        "allenai/dolma",
        "bytewax/bytewax",
        "redpanda-data/redpanda",
        "apache/iceberg",
        "MaterializeInc/materialize",
        "risingwavelabs/risingwave",
        "pathwaycom/pathway",
        "unclecode/crawl4ai",
        "firecrawl/firecrawl",
        "microsoft/onnxruntime",
        "ogx-ai/ogx",
        "triton-lang/triton",
        "google-deepmind/gemma",
        "mistralai/mistral-inference",
        "tinygrad/tinygrad",
        "huggingface/pytorch-image-models",
        "pytorch/torchtitan",
    }
)

# Org-level wildcard (any repo under these orgs counts).
CURATED_ORGS: frozenset[str] = frozenset(
    {
        "huggingface",
        "openai",
        "anthropics",
        "deepmind",
        "google-deepmind",
        "meta-llama",
        "facebookresearch",
        "EleutherAI",
    }
)


def is_relevant_repo(full_name: str) -> bool:
    """Return True if ``owner/repo`` is in the AI allow-list."""
    if not full_name or "/" not in full_name:
        return False
    if full_name in CURATED_REPOS:
        return True
    org = full_name.split("/", 1)[0]
    return org in CURATED_ORGS
