# Multi-stage Dockerfile for the OAI-PMH CronJob poller.
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
COPY ingest/__init__.py /src/ingest/__init__.py
COPY ingest/common /src/ingest/common
COPY ingest/oaipmh_poller /src/ingest/oaipmh_poller

RUN uv pip install --system --no-cache --no-config --no-sources \
        -e . \
        -e ./ingest/common \
        -e ./ingest/oaipmh_poller

FROM python:${PYTHON_VERSION}-slim AS runtime
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
RUN groupadd --system --gid 65532 nonroot \
 && useradd  --system --uid 65532 --gid 65532 --home /home/nonroot --create-home nonroot \
 && apt-get update && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/s2p-oaipmh-poller /usr/local/bin/s2p-oaipmh-poller
COPY --from=builder /src /app
ENV PYTHONPATH=/app
WORKDIR /app
USER nonroot
ENTRYPOINT ["s2p-oaipmh-poller"]
