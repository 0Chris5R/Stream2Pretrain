# Minimal source layer for the stateless curator inference service.
ARG PROCESSOR_BASE_IMAGE=stream2pretrain-processor-base-quality:local
ARG S2P_MODEL_SERVICE_PROFILE=quality
FROM ${PROCESSOR_BASE_IMAGE} AS runtime
ARG S2P_MODEL_SERVICE_PROFILE

ENV S2P_MODEL_SERVICE_PROFILE=${S2P_MODEL_SERVICE_PROFILE}

COPY schemas                    /app/schemas
COPY processor/__init__.py      \
     processor/common.py        \
     processor/decon_gate.py    \
     processor/model_service.py \
     processor/sign.py          /app/processor/
COPY processor/operators/__init__.py     \
     processor/operators/kenlm_score.py  \
     processor/operators/quality.py      \
     processor/operators/pii.py           /app/processor/operators/

# Each immutable profile base validates its exact native/model dependencies.
# Repository tests validate this source-only layer without reloading that base.
WORKDIR /app
USER nonroot

ENTRYPOINT ["python", "-m", "processor.model_service"]
