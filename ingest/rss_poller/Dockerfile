# Multi-stage Dockerfile for the RSS/Atom CronJob poller.
#
# Stage 1: build a wheelhouse with `uv` from the workspace root so the shared
#          `schemas` and `ingest.common` packages are bundled.
# Stage 2: minimal python:3.11-slim runtime, non-root user, no compilers.
ARG PYTHON_VERSION=3.11

FROM python:${PYTHON_VERSION}-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv==0.4.27

WORKDIR /src
COPY pyproject.toml /src/pyproject.toml
COPY README.md /src/README.md
COPY schemas /src/schemas
COPY ingest /src/ingest
COPY processor /src/processor
COPY tests /src/tests

RUN uv pip install --system --no-cache \
        ./schemas/.. \
        ./ingest/common \
        ./ingest/rss_poller \
    || (uv pip install --system --no-cache \
            -e . \
            -e ./ingest/common \
            -e ./ingest/rss_poller)

FROM python:${PYTHON_VERSION}-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN groupadd --system --gid 65532 nonroot \
 && useradd  --system --uid 65532 --gid 65532 --home /home/nonroot --create-home nonroot \
 && apt-get update && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/s2p-rss-poller /usr/local/bin/s2p-rss-poller
COPY --from=builder /src /app
ENV PYTHONPATH=/app
WORKDIR /app

USER nonroot

ENTRYPOINT ["s2p-rss-poller"]
