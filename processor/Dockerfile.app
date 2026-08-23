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
COPY ingest/sitemap_poller                              /app/ingest/sitemap_poller
COPY processor                                          /app/processor
COPY docs/provider-terms                                /app/docs/provider-terms
COPY --chmod=0555 processor/container_entrypoint.sh     /usr/local/bin/s2p-entrypoint

RUN for command in \
      s2p-fetcher \
      s2p-curate \
      s2p-curator-model-service \
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

# Catch missing source or runtime dependencies before Helm sees the image.
RUN python -c "from processor.curate import main as curate; from processor.model_service import main as model_service; from processor.decon_api import main as decon; from processor.duckdb_api import main as duckdb; from processor.fetcher import main as fetcher; from processor.iceberg_maintenance import main as maintenance; from processor.iceberg_writer import main as writer; from processor.local_sources_api import main as sources; from processor.mixture_controller.controller import main as mixture; from processor.seed_loader import main as seed; from processor.foundry.api import main as foundry_api; from processor.foundry.export_replay import main as replay; from processor.foundry.oracle_build import main as oracle; from processor.foundry.worker import main as foundry; assert all(callable(value) for value in (curate, model_service, decon, duckdb, fetcher, maintenance, writer, sources, mixture, seed, foundry_api, replay, oracle, foundry))"

WORKDIR /app
USER nonroot

ENTRYPOINT ["s2p-curate"]
