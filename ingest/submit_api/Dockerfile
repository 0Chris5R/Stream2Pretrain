# Multi-stage Dockerfile for the FastAPI submit API.
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
COPY ingest/submit_api /src/ingest/submit_api
RUN uv pip install --system --no-cache \
        -e . \
        -e ./ingest/common \
        -e ./ingest/submit_api

FROM python:${PYTHON_VERSION}-slim AS runtime
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    S2P_BIND_HOST=0.0.0.0 S2P_BIND_PORT=8000
RUN groupadd --system --gid 65532 nonroot \
 && useradd  --system --uid 65532 --gid 65532 --home /home/nonroot --create-home nonroot \
 && apt-get update && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/s2p-submit-api /usr/local/bin/s2p-submit-api
COPY --from=builder /src /app
WORKDIR /app
EXPOSE 8000
USER nonroot
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz').read()"]
ENTRYPOINT ["s2p-submit-api"]
