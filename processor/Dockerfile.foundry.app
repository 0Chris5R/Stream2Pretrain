# Foundry-only application layer. Dependencies remain in the immutable shared
# processor base; post-training edits must not restart the pretraining core.
ARG PROCESSOR_BASE_IMAGE=stream2pretrain-processor-base:local
FROM ${PROCESSOR_BASE_IMAGE} AS runtime

COPY schemas                         /app/schemas
COPY processor/__init__.py           /app/processor/__init__.py
COPY processor/common.py             /app/processor/common.py
COPY processor/iceberg_catalog.py    /app/processor/iceberg_catalog.py
COPY processor/probes.py             /app/processor/probes.py
COPY processor/sign.py               /app/processor/sign.py
COPY processor/foundry               /app/processor/foundry
COPY --chmod=0555 processor/container_entrypoint.sh /usr/local/bin/s2p-entrypoint

RUN ln -s /usr/local/bin/s2p-entrypoint /usr/local/bin/s2p-foundry \
 && ln -s /usr/local/bin/s2p-entrypoint /usr/local/bin/s2p-foundry-api \
 && ln -s /usr/local/bin/s2p-entrypoint /usr/local/bin/s2p-foundry-export-replay \
 && ln -s /usr/local/bin/s2p-entrypoint /usr/local/bin/s2p-foundry-build-oracle \
 && python -c "from processor.foundry.api import main as api; from processor.foundry.worker import main as worker; assert callable(api) and callable(worker)"

WORKDIR /app
USER nonroot

ENTRYPOINT ["s2p-foundry"]
