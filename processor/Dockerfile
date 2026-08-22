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
    UV_LINK_MODE=copy

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

RUN pip install --no-cache-dir uv==0.4.27

WORKDIR /src
COPY pyproject.toml /src/pyproject.toml
COPY README.md /src/README.md
COPY docs /src/docs
COPY schemas /src/schemas
COPY ingest /src/ingest
COPY processor /src/processor
COPY tests /src/tests

RUN uv pip install --system --no-cache \
        --extra-index-url https://download.pytorch.org/whl/cpu \
        --index-strategy unsafe-best-match \
        ./schemas/.. \
        "./processor[${S2P_INSTALL_EXTRA}]" \
    || (uv pip install --system --no-cache \
            --extra-index-url https://download.pytorch.org/whl/cpu \
            --index-strategy unsafe-best-match \
            -e . \
            -e "./processor[${S2P_INSTALL_EXTRA}]")

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
# Stage 3: runtime
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
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

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/s2p-fetcher          /usr/local/bin/s2p-fetcher
COPY --from=builder /usr/local/bin/s2p-curate           /usr/local/bin/s2p-curate
COPY --from=builder /usr/local/bin/s2p-iceberg-writer   /usr/local/bin/s2p-iceberg-writer
COPY --from=builder /usr/local/bin/s2p-iceberg-maintenance /usr/local/bin/s2p-iceberg-maintenance
COPY --from=builder /usr/local/bin/s2p-decon-api        /usr/local/bin/s2p-decon-api
COPY --from=builder /usr/local/bin/s2p-duckdb-api       /usr/local/bin/s2p-duckdb-api
COPY --from=builder /usr/local/bin/s2p-mixture-controller /usr/local/bin/s2p-mixture-controller
COPY --from=builder /usr/local/bin/s2p-foundry          /usr/local/bin/s2p-foundry
COPY --from=builder /usr/local/bin/s2p-foundry-api      /usr/local/bin/s2p-foundry-api
COPY --from=builder /usr/local/bin/s2p-foundry-export-replay /usr/local/bin/s2p-foundry-export-replay
COPY --from=builder /usr/local/bin/s2p-foundry-build-oracle /usr/local/bin/s2p-foundry-build-oracle
COPY --from=builder /src                                /app
COPY --from=models  /opt/models                         /opt/models
COPY --from=builder /opt/models/docling                 /opt/models/docling

WORKDIR /app
USER nonroot

ENTRYPOINT ["s2p-curate"]
