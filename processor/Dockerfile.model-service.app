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

# This image intentionally lacks unrelated worker dependencies. Import the
# selected native/model backend so CI proves the minimal environment is
# complete before Kubernetes pulls it.
RUN case "${S2P_MODEL_SERVICE_PROFILE}" in \
      quality) python -c "import safetensors, tokenizers, torch, transformers; from processor.model_service import main; assert callable(main)" ;; \
      embedding) python -c "import onnxruntime, tokenizers, transformers; from processor.model_service import main; assert callable(main)" ;; \
      kenlm) python -c "import kenlm, sentencepiece; from processor.model_service import main; assert callable(main)" ;; \
      *) echo "Unsupported S2P_MODEL_SERVICE_PROFILE=${S2P_MODEL_SERVICE_PROFILE}" >&2; exit 2 ;; \
    esac

WORKDIR /app
USER nonroot

ENTRYPOINT ["python", "-m", "processor.model_service"]
