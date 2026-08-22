# Multi-stage Dockerfile for the arXiv full-paper-HTML fetcher.
#
# Resiliparse pulls precompiled wheels for linux/amd64 and linux/arm64 on
# Python 3.11; we install via uv (project-wide convention from CLAUDE.md).
ARG PYTHON_VERSION=3.11

FROM python:${PYTHON_VERSION}-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv==0.4.27

WORKDIR /src
COPY pyproject.toml /src/pyproject.toml
COPY README.md /src/README.md
COPY schemas /src/schemas
# The install command ignores root workspace source mappings and receives each
# required local package explicitly, so unrelated components stay out of this
# image context and cannot invalidate its dependency layer.
COPY ingest/__init__.py /src/ingest/__init__.py
COPY ingest/common /src/ingest/common
COPY ingest/arxiv_html_fetcher /src/ingest/arxiv_html_fetcher

RUN uv pip install --system --no-cache --no-config --no-sources \
        -e . \
        -e ./ingest/common \
        -e ./ingest/arxiv_html_fetcher

FROM python:${PYTHON_VERSION}-slim AS runtime
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
RUN groupadd --system --gid 65532 nonroot \
 && useradd  --system --uid 65532 --gid 65532 --home /home/nonroot --create-home nonroot \
 && apt-get update && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/s2p-arxiv-html-fetcher /usr/local/bin/s2p-arxiv-html-fetcher
COPY --from=builder /src /app
ENV PYTHONPATH=/app
WORKDIR /app
USER nonroot
ENTRYPOINT ["s2p-arxiv-html-fetcher"]
