# Minimal source layer for the stateless curator inference service.
ARG PROCESSOR_BASE_IMAGE=stream2pretrain-processor-base-curate:local
FROM ${PROCESSOR_BASE_IMAGE} AS runtime

COPY schemas                    /app/schemas
COPY processor                  /app/processor

# This image intentionally lacks unrelated worker dependencies. Import both
# its command and every native/model backend so CI proves the minimal
# environment is complete before Kubernetes pulls it.
RUN python -c "import kenlm, onnxruntime, safetensors, sentencepiece, tokenizers, torch, transformers; from processor.model_service import main; assert callable(main)"

WORKDIR /app
USER nonroot

ENTRYPOINT ["python", "-m", "processor.model_service"]
