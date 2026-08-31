# Multi-stage Dockerfile for the Stream2Pretrain processor.
#
# Stage 1 (builder):  install uv + Python deps from the workspace root.
# Stage 2 (runtime):  install only the selected immutable CPU model profile.
ARG PYTHON_VERSION=3.11
ARG S2P_MODEL_PROFILE=none
ARG S2P_INSTALL_EXTRA=runtime
ARG FINEPDFS_EDU_V2_REVISION=90ddef285f67230389057c14b2f6bbfeb70d40ea
ARG KENLM_REVISION=3fbe35c83b1a39f420a345b7c96a186c8030d834
ARG FIGURE_CLASSIFIER_REVISION=f859dfbff5c9916cd996942d4b0db7fa25808220
ARG META_RATER_REVISION=0072a9a83971eb4af6d689dfc64f8f203c45b398
ARG FINEMATH_REVISION=bd0b0e330750ccaafb16c47066f875b3fcb707c3

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
COPY ingest/hf_poller/pyproject.toml /src/ingest/hf_poller/pyproject.toml
COPY ingest/oaipmh_poller/pyproject.toml /src/ingest/oaipmh_poller/pyproject.toml
COPY ingest/arxiv_html_fetcher/pyproject.toml /src/ingest/arxiv_html_fetcher/pyproject.toml
COPY processor/pyproject.toml /src/processor/pyproject.toml
COPY tests/pyproject.toml /src/tests/pyproject.toml

RUN uv sync \
        --frozen \
        --package stream2pretrain-processor \
        --extra "${S2P_INSTALL_EXTRA}" \
        --no-dev \
        --no-install-workspace

# CSO 4.0.1's wheel pins an obsolete requests release even though its API use
# is compatible with the secured workspace version. Install only that wheel;
# the shadow-service extra above pins every imported runtime dependency.
RUN if [ "${S2P_INSTALL_EXTRA}" = "shadow-service" ]; then \
      uv pip install --python /src/.venv/bin/python --no-deps cso-classifier==4.0.1; \
    fi

# ---------------------------------------------------------------------------
# Stage 2: immutable runtime dependency and model base
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS runtime-base
ARG S2P_MODEL_PROFILE
ARG FINEPDFS_EDU_V2_REVISION
ARG KENLM_REVISION
ARG FIGURE_CLASSIFIER_REVISION
ARG META_RATER_REVISION
ARG FINEMATH_REVISION

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
    none|kenlm|fetcher|shadow) mkdir -p /opt/models/finepdfs-edu-v2 ;; \
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
    none|quality|kenlm|shadow) mkdir -p /opt/models/figure-classifier ;; \
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
    none|quality|fetcher|shadow) ;; \
    *) echo "Unsupported S2P_MODEL_PROFILE=${S2P_MODEL_PROFILE}" >&2; exit 2 ;; \
    esac

# Pre-cache the exact BPE vocabulary in worker profiles. Token counting remains
# local to the lightweight curator and this vocabulary is only a few MiB.
RUN mkdir -p /opt/models/tiktoken \
 && case "${S2P_MODEL_PROFILE}" in none|fetcher|all) \
      TIKTOKEN_CACHE_DIR=/opt/models/tiktoken \
        python -c "import tiktoken; tiktoken.get_encoding('cl100k_base')" ;; \
    quality|kenlm|shadow) ;; \
    *) echo "Unsupported S2P_MODEL_PROFILE=${S2P_MODEL_PROFILE}" >&2; exit 2 ;; \
    esac

# Docling's CPU pipeline needs layout, TableFormer, and formula artifacts.
RUN mkdir -p /opt/models/docling \
 && case "${S2P_MODEL_PROFILE}" in fetcher|all) \
      HF_HOME=/tmp/huggingface \
        docling-tools models download layout tableformer code_formula \
          --output-dir /opt/models/docling; \
      rm -rf /tmp/huggingface ;; \
    none|quality|kenlm|shadow) ;; \
    *) echo "Unsupported S2P_MODEL_PROFILE=${S2P_MODEL_PROFILE}" >&2; exit 2 ;; \
    esac

# Public non-gating shadow models. The two scalar models are pinned Hugging
# Face snapshots. CSO is pinned to package 4.0.1 and ontology 3.5; its 72 MB
# cached semantic map is the documented v2 artifact used by fast mode.
RUN case "${S2P_MODEL_PROFILE}" in shadow|all) \
      python -c "from huggingface_hub import snapshot_download; \
        snapshot_download(repo_id='opendatalab/meta-rater-reasoning-rating', \
          revision='${META_RATER_REVISION}', \
          local_dir='/opt/models/meta-rater-reasoning', \
          allow_patterns=['*.json', '*.txt', '*.safetensors']); \
        snapshot_download(repo_id='HuggingFaceTB/finemath-classifier', \
          revision='${FINEMATH_REVISION}', \
          local_dir='/opt/models/finemath-classifier', \
          allow_patterns=['*.json', '*.txt', '*.model', '*.safetensors'])"; \
      python -c "from cso_classifier import Config, Ontology; \
        from cso_classifier.misc import download_file, download_language_model; \
        cfg=Config(); \
        assert download_file('https://cso.kmi.open.ac.uk/download/version-3.5/cso_v3.5.csv', cfg.get_cso_path()); \
        cfg.set_cso_version('3.5'); \
        Ontology(load_ontology=False, silent=True).setup(); \
        assert download_file('https://cso.kmi.open.ac.uk/download/resources/token-to-cso-combined.v2.json', cfg.get_cached_model()); \
        download_language_model(notification=False)"; \
      rm -rf /root/.cache/huggingface /opt/models/meta-rater-reasoning/.cache /opt/models/finemath-classifier/.cache ;; \
    none|quality|kenlm|fetcher) mkdir -p /opt/models/meta-rater-reasoning /opt/models/finemath-classifier ;; \
    *) echo "Unsupported S2P_MODEL_PROFILE=${S2P_MODEL_PROFILE}" >&2; exit 2 ;; \
    esac

RUN python -c "import json; json.dump({ \
      'profile': '${S2P_MODEL_PROFILE}', \
      'finepdfs_edu_v2': '${FINEPDFS_EDU_V2_REVISION}', \
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
        test -s /opt/models/finepdfs-edu-v2/tokenizer.json ;; \
      kenlm) \
        test -s /opt/models/kenlm/en.arpa.bin; \
        test -s /opt/models/kenlm/en.sp.model ;; \
      fetcher) \
        test -s /opt/models/figure-classifier/model.onnx; \
        find /opt/models/docling -type f -size +0c -print -quit | grep -q .; \
        find /opt/models/tiktoken -type f -size +0c -print -quit | grep -q .; \
        python -c "import torch, torchvision; from docling.document_converter import DocumentConverter; from transformers import AutoProcessor; assert hasattr(torch.ops.torchvision, 'nms'); assert DocumentConverter and AutoProcessor" ;; \
      shadow) \
        test -s /opt/models/meta-rater-reasoning/model.safetensors; \
        test -s /opt/models/finemath-classifier/model.safetensors; \
        python -c "from cso_classifier import CSOClassifier; result=CSOClassifier(delete_outliers=False, fast_classification=True, silent=True).run('distributed machine learning systems'); assert result.get('union')" ;; \
      all) \
        test -s /opt/models/finepdfs-edu-v2/model.safetensors; \
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
      s2p-shadow-classifier \
      s2p-iceberg-writer \
      s2p-iceberg-maintenance \
      s2p-object-lifecycle \
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
RUN python -c "from processor.curate import main as curate; from processor.model_service import main as model_service; from processor.shadow_classifier import main as shadow; from processor.duckdb_api import main as duckdb; from processor.fetcher import main as fetcher; from processor.iceberg_maintenance import main as maintenance; from processor.object_lifecycle import main as object_lifecycle; from processor.iceberg_writer import main as writer; from processor.local_sources_api import main as sources; from processor.mixture_controller.controller import main as mixture; from processor.foundry.api import main as foundry_api; from processor.foundry.export_replay import main as replay; from processor.foundry.oracle_build import main as oracle; from processor.foundry.worker import main as foundry; assert all(callable(value) for value in (curate, model_service, shadow, duckdb, fetcher, maintenance, object_lifecycle, writer, sources, mixture, foundry_api, replay, oracle, foundry))"

WORKDIR /app
USER nonroot

ENTRYPOINT ["s2p-curate"]
