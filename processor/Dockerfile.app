# Fast application-only processor image used by CI.
#
# PROCESSOR_BASE_IMAGE is an immutable runtime-base image keyed by the exact
# Dockerfile base section plus uv.lock and every workspace pyproject.toml.
ARG PROCESSOR_BASE_IMAGE=stream2pretrain-processor-base:local
FROM ${PROCESSOR_BASE_IMAGE} AS runtime

COPY schemas                                            /app/schemas
COPY ingest/__init__.py                                 /app/ingest/__init__.py
COPY ingest/common                                      /app/ingest/common
COPY ingest/arxiv_html_fetcher                          /app/ingest/arxiv_html_fetcher
COPY ingest/oaipmh_poller                               /app/ingest/oaipmh_poller
COPY ingest/rss_poller                                  /app/ingest/rss_poller
COPY processor                                          /app/processor
COPY docs/provider-terms                                /app/docs/provider-terms
COPY --chmod=0555 processor/container_entrypoint.sh     /usr/local/bin/s2p-entrypoint

# Dependencies and model artifacts are validated in the immutable base build;
# the locked repository suite validates this source-only application layer.
# Keeping this stage free of imports lets BuildKit reuse the remote base lazily.
WORKDIR /app
USER nonroot

ENTRYPOINT ["s2p-entrypoint", "s2p-curate"]
