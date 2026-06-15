# Multi-stage Dockerfile for the HF Hub CronJob poller.
ARG PYTHON_VERSION=3.11

FROM python:${PYTHON_VERSION}-slim AS builder
ENV PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1
RUN apt-get update && apt-get install -y --no-install-recommends build-essential ca-certificates \
 && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir uv==0.4.27
WORKDIR /src
COPY pyproject.toml /src/pyproject.toml
COPY schemas /src/schemas
COPY ingest/common /src/ingest/common
COPY ingest/hf_poller /src/ingest/hf_poller
RUN uv pip install --system --no-cache \
        -e . \
        -e ./ingest/common \
        -e ./ingest/hf_poller

FROM python:${PYTHON_VERSION}-slim AS runtime
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
RUN groupadd --system --gid 65532 nonroot \
 && useradd  --system --uid 65532 --gid 65532 --home /home/nonroot --create-home nonroot \
 && apt-get update && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/s2p-hf-poller /usr/local/bin/s2p-hf-poller
COPY --from=builder /src /app
WORKDIR /app
USER nonroot
ENTRYPOINT ["s2p-hf-poller"]
