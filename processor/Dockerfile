# Multi-stage Dockerfile for the Stream2Pretrain processor.
#
# Stage 1 (builder):  install uv + Python deps from the workspace root.
# Stage 2 (runtime):  install only the selected immutable CPU model profile.
ARG PYTHON_VERSION=3.11
ARG S2P_MODEL_PROFILE=none
ARG S2P_INSTALL_EXTRA=runtime
ARG FINEPDFS_EDU_V2_REVISION=90ddef285f67230389057c14b2f6bbfeb70d40ea
ARG FINEWEB_EDU_REVISION=284663cbb2dabf9bda30d8f8cc49601251ee1631
ARG E5_SMALL_REVISION=ffb93f3bd4047442299a41ebb6fa998a38507c52
ARG KENLM_REVISION=3fbe35c83b1a39f420a345b7c96a186c8030d834
ARG FIGURE_CLASSIFIER_REVISION=f859dfbff5c9916cd996942d4b0db7fa25808220

# ---------------------------------------------------------------------------
# Stage 1: builder
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS builder
ARG S2P_INSTALL_EXTRA

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/src/.venv \
    PATH="/src/.venv/bin:${PATH}"

# Build deps for rensa (Rust), plyvel (LevelDB), kenlm (C++), onnxruntime wheels.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        cmake \
        curl \
        git \
        libleveldb-dev \
        libssl-dev \
        pkg-config \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv==0.8.17

WORKDIR /src

# Keep the large, locked ML dependency environment independent of application
# source. Workspace packages are imported from the separate /app source layer
# at runtime, so an ordinary edit never changes or re-exports this venv layer.
COPY pyproject.toml uv.lock README.md /src/
COPY ingest/common/pyproject.toml /src/ingest/common/pyproject.toml
COPY ingest/rss_poller/pyproject.toml /src/ingest/rss_poller/pyproject.toml
COPY ingest/github_releases/pyproject.toml /src/ingest/github_releases/pyproject.toml
COPY ingest/hf_poller/pyproject.toml /src/ingest/hf_poller/pyproject.toml
COPY ingest/oaipmh_poller/pyproject.toml /src/ingest/oaipmh_poller/pyproject.toml
COPY ingest/arxiv_html_fetcher/pyproject.toml /src/ingest/arxiv_html_fetcher/pyproject.toml
COPY ingest/github_release_tarball_fetcher/pyproject.toml /src/ingest/github_release_tarball_fetcher/pyproject.toml
COPY processor/pyproject.toml /src/processor/pyproject.toml
COPY tests/pyproject.toml /src/tests/pyproject.toml

RUN uv sync \
        --frozen \
        --package stream2pretrain-processor \
        --extra "${S2P_INSTALL_EXTRA}" \
        --no-dev \
        --no-install-workspace

# ---------------------------------------------------------------------------
# Stage 2: immutable runtime dependency and model base
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS runtime-base
ARG S2P_MODEL_PROFILE
ARG FINEPDFS_EDU_V2_REVISION
ARG FINEWEB_EDU_REVISION
ARG E5_SMALL_REVISION
ARG KENLM_REVISION
ARG FIGURE_CLASSIFIER_REVISION

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    PATH="/src/.venv/bin:${PATH}" \
    HF_HUB_DISABLE_XET=1 \
    HF_HUB_DISABLE_TELEMETRY=1 \
    S2P_MODELS_DIR=/opt/models \
    TIKTOKEN_CACHE_DIR=/opt/models/tiktoken \
    S2P_STATE_DIR=/var/lib/s2p

RUN groupadd --system --gid 65532 nonroot \
 && useradd  --system --uid 65532 --gid 65532 --home /home/nonroot --create-home nonroot \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates \
        libgl1 \
        libglib2.0-0 \
        libgomp1 \
        libleveldb1d \
        tesseract-ocr \
        tesseract-ocr-eng \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p ${S2P_STATE_DIR} \
 && chown -R nonroot:nonroot ${S2P_STATE_DIR}

COPY --from=builder /src/.venv                         /src/.venv

WORKDIR /opt/models

# Official FinePDFs Edu v2 scientific-quality checkpoint.
RUN case "${S2P_MODEL_PROFILE}" in quality|all) \
      python -c "from huggingface_hub import snapshot_download; \
        snapshot_download(repo_id='HuggingFaceFW/finepdfs_edu_classifier_v2_eng_Latn', \
          revision='${FINEPDFS_EDU_V2_REVISION}', \
          local_dir='/opt/models/finepdfs-edu-v2', \
          allow_patterns=['*.json', '*.txt', '*.safetensors'])"; \
      rm -rf /opt/models/finepdfs-edu-v2/.cache /root/.cache/huggingface ;; \
    none|embedding|kenlm|fetcher) mkdir -p /opt/models/finepdfs-edu-v2 ;; \
    *) echo "Unsupported S2P_MODEL_PROFILE=${S2P_MODEL_PROFILE}" >&2; exit 2 ;; \
    esac

# Official FineWeb-Edu Safetensors checkpoint. It runs through Transformers on
# CPU unless a separately verified model.onnx is mounted in this directory.
RUN case "${S2P_MODEL_PROFILE}" in quality|all) \
      python -c "from huggingface_hub import snapshot_download; \
        snapshot_download(repo_id='HuggingFaceFW/fineweb-edu-classifier', \
          revision='${FINEWEB_EDU_REVISION}', \
          local_dir='/opt/models/fineweb-edu', \
          allow_patterns=['*.json', '*.txt', '*.safetensors'])"; \
      rm -rf /opt/models/fineweb-edu/.cache /root/.cache/huggingface ;; \
    none|embedding|kenlm|fetcher) mkdir -p /opt/models/fineweb-edu ;; \
    *) echo "Unsupported S2P_MODEL_PROFILE=${S2P_MODEL_PROFILE}" >&2; exit 2 ;; \
    esac

# Small CPU ONNX router for 26 document-figure categories.
RUN case "${S2P_MODEL_PROFILE}" in fetcher|all) \
      python -c "from huggingface_hub import snapshot_download; \
        snapshot_download(repo_id='docling-project/DocumentFigureClassifier-v2.5', \
          revision='${FIGURE_CLASSIFIER_REVISION}', \
          local_dir='/opt/models/figure-classifier', \
          allow_patterns=['model.onnx', '*.json'])"; \
      rm -rf /opt/models/figure-classifier/.cache /root/.cache/huggingface ;; \
    none|quality|embedding|kenlm) mkdir -p /opt/models/figure-classifier ;; \
    *) echo "Unsupported S2P_MODEL_PROFILE=${S2P_MODEL_PROFILE}" >&2; exit 2 ;; \
    esac

# E5-small ONNX embedding model for the Decon-Gate semantic sketch.
RUN case "${S2P_MODEL_PROFILE}" in embedding|all) \
      python -c "from huggingface_hub import snapshot_download; \
        snapshot_download(repo_id='intfloat/e5-small-v2', \
          revision='${E5_SMALL_REVISION}', \
          local_dir='/opt/models/e5-small', \
          allow_patterns=['model.onnx', '*.json', '*.txt'])"; \
      rm -rf /opt/models/e5-small/.cache /root/.cache/huggingface ;; \
    none|quality|kenlm|fetcher) mkdir -p /opt/models/e5-small ;; \
    *) echo "Unsupported S2P_MODEL_PROFILE=${S2P_MODEL_PROFILE}" >&2; exit 2 ;; \
    esac

# KenLM English Wikipedia binary plus its exact SentencePiece tokenizer. Delete
# the Hub cache in the same layer so the 4.44 GB binary is stored only once.
RUN mkdir -p /opt/models/kenlm \
 && case "${S2P_MODEL_PROFILE}" in kenlm|all) \
      python -c "from huggingface_hub import hf_hub_download; import os, shutil; \
        repo='edugp/kenlm'; rev='${KENLM_REVISION}'; \
        binary=hf_hub_download(repo_id=repo, filename='wikipedia/en.arpa.bin', revision=rev, local_dir='/opt/models/kenlm'); \
        tokenizer=hf_hub_download(repo_id=repo, filename='wikipedia/en.sp.model', revision=rev, local_dir='/opt/models/kenlm'); \
        os.replace(binary, '/opt/models/kenlm/en.arpa.bin'); \
        os.replace(tokenizer, '/opt/models/kenlm/en.sp.model'); \
        shutil.rmtree('/opt/models/kenlm/.cache', ignore_errors=True); \
        shutil.rmtree('/opt/models/kenlm/wikipedia', ignore_errors=True)"; \
      rm -rf /root/.cache/huggingface /opt/models/kenlm/.cache ;; \
    none|quality|embedding|fetcher) ;; \
    *) echo "Unsupported S2P_MODEL_PROFILE=${S2P_MODEL_PROFILE}" >&2; exit 2 ;; \
    esac

# Pre-cache the exact BPE vocabulary in worker profiles. Token counting remains
# local to the lightweight curator and this vocabulary is only a few MiB.
RUN mkdir -p /opt/models/tiktoken \
 && case "${S2P_MODEL_PROFILE}" in none|fetcher|all) \
      TIKTOKEN_CACHE_DIR=/opt/models/tiktoken \
        python -c "import tiktoken; tiktoken.get_encoding('cl100k_base')" ;; \
    quality|embedding|kenlm) ;; \
    *) echo "Unsupported S2P_MODEL_PROFILE=${S2P_MODEL_PROFILE}" >&2; exit 2 ;; \
    esac

# Docling's CPU pipeline needs layout, TableFormer, and formula artifacts.
RUN mkdir -p /opt/models/docling \
 && case "${S2P_MODEL_PROFILE}" in fetcher|all) \
      HF_HOME=/tmp/huggingface \
        docling-tools models download layout tableformer code_formula \
          --output-dir /opt/models/docling; \
      rm -rf /tmp/huggingface ;; \
    none|quality|embedding|kenlm) ;; \
    *) echo "Unsupported S2P_MODEL_PROFILE=${S2P_MODEL_PROFILE}" >&2; exit 2 ;; \
    esac

RUN python -c "import json; json.dump({ \
      'profile': '${S2P_MODEL_PROFILE}', \
      'finepdfs_edu_v2': '${FINEPDFS_EDU_V2_REVISION}', \
      'fineweb_edu': '${FINEWEB_EDU_REVISION}', \
      'e5_small_v2': '${E5_SMALL_REVISION}', \
      'kenlm': '${KENLM_REVISION}', \
      'figure_classifier': '${FIGURE_CLASSIFIER_REVISION}'}, \
      open('/opt/models/revisions.json', 'w'), sort_keys=True)"

# Make an immutable model image fail during its one-time build, not when a
# scaled worker first lands on a new node. The profile-specific images share
# the dependency layers, while containerd stores only the model blobs each
# component actually needs.
RUN case "${S2P_MODEL_PROFILE}" in \
      none) find /opt/models/tiktoken -type f -size +0c -print -quit | grep -q . ;; \
      quality) \
        test -s /opt/models/finepdfs-edu-v2/model.safetensors; \
        test -s /opt/models/finepdfs-edu-v2/tokenizer.json; \
        test -s /opt/models/fineweb-edu/model.safetensors; \
        test -s /opt/models/fineweb-edu/tokenizer.json ;; \
      embedding) \
        test -s /opt/models/e5-small/model.onnx; \
        test -s /opt/models/e5-small/tokenizer.json ;; \
      kenlm) \
        test -s /opt/models/kenlm/en.arpa.bin; \
        test -s /opt/models/kenlm/en.sp.model ;; \
      fetcher) \
        test -s /opt/models/figure-classifier/model.onnx; \
        find /opt/models/docling -type f -size +0c -print -quit | grep -q .; \
        find /opt/models/tiktoken -type f -size +0c -print -quit | grep -q .; \
        python -c "import torch, torchvision; from docling.document_converter import DocumentConverter; from transformers import AutoProcessor; assert hasattr(torch.ops.torchvision, 'nms'); assert DocumentConverter and AutoProcessor" ;; \
      all) \
        test -s /opt/models/finepdfs-edu-v2/model.safetensors; \
        test -s /opt/models/fineweb-edu/model.safetensors; \
        test -s /opt/models/e5-small/model.onnx; \
        test -s /opt/models/kenlm/en.arpa.bin; \
        test -s /opt/models/figure-classifier/model.onnx; \
        find /opt/models/docling -type f -size +0c -print -quit | grep -q . ;; \
      *) echo "Unsupported S2P_MODEL_PROFILE=${S2P_MODEL_PROFILE}" >&2; exit 2 ;; \
    esac

# ---------------------------------------------------------------------------
# Stage 3: application source
#
# CI also builds runtime-base as an immutable, lockfile-keyed image and uses
# processor/Dockerfile.app for ordinary source revisions. Keeping this final
# stage here preserves a self-contained Dockerfile for local development.
# ---------------------------------------------------------------------------
FROM runtime-base AS runtime

COPY schemas                                            /app/schemas
COPY ingest/__init__.py                                 /app/ingest/__init__.py
COPY ingest/common                                      /app/ingest/common
COPY ingest/arxiv_html_fetcher                          /app/ingest/arxiv_html_fetcher
COPY ingest/oaipmh_poller                               /app/ingest/oaipmh_poller
COPY ingest/rss_poller                                  /app/ingest/rss_poller
COPY processor                                          /app/processor
# The foundry records the reviewed provider-terms document path in its policy
# audit. Keep only that required documentation subtree in the runtime image.
COPY docs/provider-terms                                /app/docs/provider-terms
COPY --chmod=0555 processor/container_entrypoint.sh     /usr/local/bin/s2p-entrypoint

RUN for command in \
      s2p-fetcher \
      s2p-curate \
      s2p-curator-model-service \
      s2p-iceberg-writer \
      s2p-iceberg-maintenance \
      s2p-decon-api \
      s2p-duckdb-api \
      s2p-local-sources-api \
      s2p-mixture-controller \
      s2p-foundry \
      s2p-foundry-api \
      s2p-foundry-export-replay \
      s2p-foundry-build-oracle; do \
        ln -s /usr/local/bin/s2p-entrypoint "/usr/local/bin/${command}"; \
    done

# Import every module reached by a container command while the image is still
# being built. This catches omitted workspace source or runtime dependencies
# before Helm can roll a healthy Pod onto a broken image.
RUN python -c "from processor.curate import main as curate; from processor.model_service import main as model_service; from processor.decon_api import main as decon; from processor.duckdb_api import main as duckdb; from processor.fetcher import main as fetcher; from processor.iceberg_maintenance import main as maintenance; from processor.iceberg_writer import main as writer; from processor.local_sources_api import main as sources; from processor.mixture_controller.controller import main as mixture; from processor.foundry.api import main as foundry_api; from processor.foundry.export_replay import main as replay; from processor.foundry.oracle_build import main as oracle; from processor.foundry.worker import main as foundry; assert all(callable(value) for value in (curate, model_service, decon, duckdb, fetcher, maintenance, writer, sources, mixture, foundry_api, replay, oracle, foundry))"

WORKDIR /app
USER nonroot

ENTRYPOINT ["s2p-curate"]
