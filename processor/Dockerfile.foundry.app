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

WORKDIR /app
USER nonroot

ENTRYPOINT ["s2p-entrypoint", "s2p-foundry"]
