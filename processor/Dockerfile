# Multi-stage Dockerfile for the Stream2Pretrain processor.
#
# Stage 1 (builder):  install uv + Python deps from the workspace root.
# Stage 2 (models):   download ONNX classifier + KenLM binary into /opt/models.
# Stage 3 (runtime):  python:3.11-slim, non-root, no compilers, models copied in.
ARG PYTHON_VERSION=3.11

# ---------------------------------------------------------------------------
# Stage 1: builder
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS builder

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
COPY schemas /src/schemas
COPY processor /src/processor

RUN uv pip install --system --no-cache \
        ./schemas/.. \
        "./processor[runtime]" \
    || (uv pip install --system --no-cache \
            -e . \
            -e "./processor[runtime]")

# ---------------------------------------------------------------------------
# Stage 2: models
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS models

ENV PIP_NO_CACHE_DIR=1
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && pip install --no-cache-dir huggingface_hub==0.24.6

WORKDIR /opt/models
# FineWeb-Edu classifier (ONNX INT8)
RUN python -c "from huggingface_hub import snapshot_download; \
    snapshot_download(repo_id='HuggingFaceFW/fineweb-edu-classifier', \
        local_dir='/opt/models/fineweb-edu', \
        allow_patterns=['*.json', '*.txt', '*.onnx'])" \
 || mkdir -p /opt/models/fineweb-edu

# E5-small for the Decon-Gate embedding sketch
RUN python -c "from huggingface_hub import snapshot_download; \
    snapshot_download(repo_id='intfloat/e5-small-v2', \
        local_dir='/opt/models/e5-small', \
        allow_patterns=['*.json', '*.txt', '*.onnx'])" \
 || mkdir -p /opt/models/e5-small

# KenLM English binary (pretrained edugp/kenlm)
RUN mkdir -p /opt/models/kenlm \
 && (curl -fsSL https://huggingface.co/edugp/kenlm/resolve/main/wikipedia/en.arpa.bin \
        -o /opt/models/kenlm/en.bin || true)

# ---------------------------------------------------------------------------
# Stage 3: runtime
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    S2P_MODELS_DIR=/opt/models \
    S2P_STATE_DIR=/var/lib/s2p

RUN groupadd --system --gid 65532 nonroot \
 && useradd  --system --uid 65532 --gid 65532 --home /home/nonroot --create-home nonroot \
 && apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libleveldb1d \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p ${S2P_STATE_DIR} \
 && chown -R nonroot:nonroot ${S2P_STATE_DIR}

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/s2p-fetcher          /usr/local/bin/s2p-fetcher
COPY --from=builder /usr/local/bin/s2p-curate           /usr/local/bin/s2p-curate
COPY --from=builder /usr/local/bin/s2p-iceberg-writer   /usr/local/bin/s2p-iceberg-writer
COPY --from=builder /usr/local/bin/s2p-mixture-controller /usr/local/bin/s2p-mixture-controller
COPY --from=builder /src                                /app
COPY --from=models  /opt/models                         /opt/models

WORKDIR /app
USER nonroot

ENTRYPOINT ["s2p-curate"]
