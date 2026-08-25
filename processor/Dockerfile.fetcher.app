# Fetcher-only application image.
#
# Its base is built with the ``fetcher-service`` extra. Do not copy or import
# unrelated processor services here: that would either force their
# dependencies into this image or make its build contract misleading.
ARG PROCESSOR_BASE_IMAGE=stream2pretrain-processor-base-fetcher:local
FROM ${PROCESSOR_BASE_IMAGE} AS runtime

COPY schemas                                            /app/schemas
COPY ingest/__init__.py                                 /app/ingest/__init__.py
COPY ingest/common                                      /app/ingest/common
COPY processor/__init__.py                              /app/processor/__init__.py
COPY processor/common.py                                /app/processor/common.py
COPY processor/fetcher.py                               /app/processor/fetcher.py
COPY processor/metrics.py                               /app/processor/metrics.py
COPY processor/operators/__init__.py                    /app/processor/operators/__init__.py
COPY processor/operators/extract.py                     /app/processor/operators/extract.py
COPY processor/operators/langid.py                      /app/processor/operators/langid.py
COPY processor/operators/minhash.py                     /app/processor/operators/minhash.py
COPY processor/operators/validity.py                    /app/processor/operators/validity.py
COPY processor/probes.py                                /app/processor/probes.py
COPY processor/scientific.py                             /app/processor/scientific.py
COPY processor/source_policy.py                          /app/processor/source_policy.py
COPY --chmod=0555 processor/container_entrypoint.sh     /usr/local/bin/s2p-entrypoint

WORKDIR /app
USER nonroot

ENTRYPOINT ["s2p-entrypoint", "s2p-fetcher"]
