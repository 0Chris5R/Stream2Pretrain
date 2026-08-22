# Multi-stage Dockerfile for the Stream2Pretrain processor.
#
# Stage 1 (builder):  install uv + Python deps from the workspace root.
# Stage 2 (models):   download pinned CPU classifier artifacts into /opt/models.
# Stage 3 (runtime):  python:3.11-slim, non-root, no compilers, models copied in.
ARG PYTHON_VERSION=3.11
ARG S2P_BAKE_MODELS=0
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
ARG S2P_BAKE_MODELS

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
COPY ingest/sitemap_poller/pyproject.toml /src/ingest/sitemap_poller/pyproject.toml
COPY ingest/github_events/pyproject.toml /src/ingest/github_events/pyproject.toml
COPY ingest/github_releases/pyproject.toml /src/ingest/github_releases/pyproject.toml
COPY ingest/hf_poller/pyproject.toml /src/ingest/hf_poller/pyproject.toml
COPY ingest/oaipmh_poller/pyproject.toml /src/ingest/oaipmh_poller/pyproject.toml
COPY ingest/arxiv_html_fetcher/pyproject.toml /src/ingest/arxiv_html_fetcher/pyproject.toml
COPY ingest/github_release_tarball_fetcher/pyproject.toml /src/ingest/github_release_tarball_fetcher/pyproject.toml
COPY ingest/openreview_poller/pyproject.toml /src/ingest/openreview_poller/pyproject.toml
COPY processor/pyproject.toml /src/processor/pyproject.toml
COPY tests/pyproject.toml /src/tests/pyproject.toml

RUN uv sync \
        --frozen \
        --package stream2pretrain-processor \
        --extra "${S2P_INSTALL_EXTRA}" \
        --no-dev \
        --no-install-workspace

# Docling's standard CPU pipeline needs layout, TableFormer, and formula
# artifacts. Download them with the exact runtime version and keep the model
# cache in the final image so PDF fallback never needs runtime egress.
RUN mkdir -p /opt/models/docling \
 && if [ "${S2P_BAKE_MODELS}" = "1" ]; then \
      docling-tools models download layout tableformer code_formula \
        --output-dir /opt/models/docling; \
    fi

# ---------------------------------------------------------------------------
# Stage 2: models
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS models
ARG S2P_BAKE_MODELS
ARG FINEPDFS_EDU_V2_REVISION
ARG FINEWEB_EDU_REVISION
ARG E5_SMALL_REVISION
ARG KENLM_REVISION
ARG FIGURE_CLASSIFIER_REVISION

ENV PIP_NO_CACHE_DIR=1
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && pip install --no-cache-dir huggingface_hub==0.24.6 "tiktoken>=0.7,<1"

WORKDIR /opt/models
# Official FinePDFs Edu v2 scientific-quality checkpoint.
RUN if [ "${S2P_BAKE_MODELS}" = "1" ]; then \
      python -c "from huggingface_hub import snapshot_download; \
        snapshot_download(repo_id='HuggingFaceFW/finepdfs_edu_classifier_v2_eng_Latn', \
          revision='${FINEPDFS_EDU_V2_REVISION}', \
          local_dir='/opt/models/finepdfs-edu-v2', \
          allow_patterns=['*.json', '*.txt', '*.safetensors'])"; \
    else \
      mkdir -p /opt/models/finepdfs-edu-v2; \
    fi

# Official FineWeb-Edu Safetensors checkpoint. It runs through Transformers on
# CPU unless a separately verified model.onnx is mounted in this directory.
RUN if [ "${S2P_BAKE_MODELS}" = "1" ]; then \
      python -c "from huggingface_hub import snapshot_download; \
        snapshot_download(repo_id='HuggingFaceFW/fineweb-edu-classifier', \
          revision='${FINEWEB_EDU_REVISION}', \
          local_dir='/opt/models/fineweb-edu', \
          allow_patterns=['*.json', '*.txt', '*.safetensors'])"; \
    else \
      mkdir -p /opt/models/fineweb-edu; \
    fi

# Small CPU ONNX router for 26 document-figure categories.
RUN if [ "${S2P_BAKE_MODELS}" = "1" ]; then \
      python -c "from huggingface_hub import snapshot_download; \
        snapshot_download(repo_id='docling-project/DocumentFigureClassifier-v2.5', \
          revision='${FIGURE_CLASSIFIER_REVISION}', \
          local_dir='/opt/models/figure-classifier', \
          allow_patterns=['model.onnx', '*.json'])"; \
    else \
      mkdir -p /opt/models/figure-classifier; \
    fi

# E5-small for the Decon-Gate embedding sketch
RUN if [ "${S2P_BAKE_MODELS}" = "1" ]; then \
      python -c "from huggingface_hub import snapshot_download; \
        snapshot_download(repo_id='intfloat/e5-small-v2', \
          revision='${E5_SMALL_REVISION}', \
          local_dir='/opt/models/e5-small', \
          allow_patterns=['model.onnx', '*.json', '*.txt'])"; \
    else \
      mkdir -p /opt/models/e5-small; \
    fi

# KenLM English Wikipedia binary plus the exact SentencePiece tokenizer used by
# the publisher's scorer. The binary is roughly 4.44 GB and is mmap-friendly.
RUN mkdir -p /opt/models/kenlm \
 && if [ "${S2P_BAKE_MODELS}" = "1" ]; then \
      python -c "from huggingface_hub import hf_hub_download; import shutil; \
        repo='edugp/kenlm'; rev='${KENLM_REVISION}'; \
        binary=hf_hub_download(repo_id=repo, filename='wikipedia/en.arpa.bin', revision=rev); \
        tokenizer=hf_hub_download(repo_id=repo, filename='wikipedia/en.sp.model', revision=rev); \
        shutil.copy2(binary, '/opt/models/kenlm/en.arpa.bin'); \
        shutil.copy2(tokenizer, '/opt/models/kenlm/en.sp.model')"; \
    fi

# Pre-cache the exact BPE vocabulary so strict mode never needs runtime egress.
RUN mkdir -p /opt/models/tiktoken \
 && if [ "${S2P_BAKE_MODELS}" = "1" ]; then \
      TIKTOKEN_CACHE_DIR=/opt/models/tiktoken \
        python -c "import tiktoken; tiktoken.get_encoding('cl100k_base')"; \
    fi

RUN python -c "import json; json.dump({ \
      'finepdfs_edu_v2': '${FINEPDFS_EDU_V2_REVISION}', \
      'fineweb_edu': '${FINEWEB_EDU_REVISION}', \
      'e5_small_v2': '${E5_SMALL_REVISION}', \
      'kenlm': '${KENLM_REVISION}', \
      'figure_classifier': '${FIGURE_CLASSIFIER_REVISION}'}, \
      open('/opt/models/revisions.json', 'w'), sort_keys=True)"

# ---------------------------------------------------------------------------
# Stage 3: immutable runtime dependency base
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS runtime-base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    PATH="/src/.venv/bin:${PATH}" \
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
COPY --from=models  /opt/models                         /opt/models
COPY --from=builder /opt/models/docling                 /opt/models/docling

# ---------------------------------------------------------------------------
# Stage 4: application source
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
COPY ingest/sitemap_poller                              /app/ingest/sitemap_poller
COPY processor                                          /app/processor
# The foundry records the reviewed provider-terms document path in its policy
# audit. Keep only that required documentation subtree in the runtime image.
COPY docs/provider-terms                                /app/docs/provider-terms
COPY --chmod=0555 processor/container_entrypoint.sh     /usr/local/bin/s2p-entrypoint

RUN for command in \
      s2p-fetcher \
      s2p-curate \
      s2p-iceberg-writer \
      s2p-iceberg-maintenance \
      s2p-decon-api \
      s2p-duckdb-api \
      s2p-local-sources-api \
      s2p-mixture-controller \
      s2p-seed-loader \
      s2p-foundry \
      s2p-foundry-api \
      s2p-foundry-export-replay \
      s2p-foundry-build-oracle; do \
        ln -s /usr/local/bin/s2p-entrypoint "/usr/local/bin/${command}"; \
    done

# Import every module reached by a container command while the image is still
# being built. This catches omitted workspace source or runtime dependencies
# before Helm can roll a healthy Pod onto a broken image.
RUN python -c "from processor.curate import main as curate; from processor.decon_api import main as decon; from processor.duckdb_api import main as duckdb; from processor.fetcher import main as fetcher; from processor.iceberg_maintenance import main as maintenance; from processor.iceberg_writer import main as writer; from processor.local_sources_api import main as sources; from processor.mixture_controller.controller import main as mixture; from processor.seed_loader import main as seed; from processor.foundry.api import main as foundry_api; from processor.foundry.export_replay import main as replay; from processor.foundry.oracle_build import main as oracle; from processor.foundry.worker import main as foundry; assert all(callable(value) for value in (curate, decon, duckdb, fetcher, maintenance, writer, sources, mixture, seed, foundry_api, replay, oracle, foundry))"

WORKDIR /app
USER nonroot

ENTRYPOINT ["s2p-curate"]
