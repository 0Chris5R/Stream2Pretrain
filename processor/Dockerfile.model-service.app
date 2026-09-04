# Minimal source layer for the stateless curator inference service.
ARG PROCESSOR_BASE_IMAGE=stream2pretrain-processor-base-quality:local
ARG S2P_MODEL_SERVICE_PROFILE=quality
FROM ${PROCESSOR_BASE_IMAGE} AS runtime
ARG S2P_MODEL_SERVICE_PROFILE

ENV S2P_MODEL_SERVICE_PROFILE=${S2P_MODEL_SERVICE_PROFILE} \
    NLTK_DATA=/opt/models/nltk_data

# CSO's package downloads stopwords into the image builder's home by default,
# which is not visible to the nonroot runtime user. Keep the corpus in the
# immutable shadow application layer where that runtime can always resolve it.
RUN if [ "${S2P_MODEL_SERVICE_PROFILE}" = "shadow" ]; then \
      python -c "import nltk; assert nltk.download('stopwords', download_dir='/opt/models/nltk_data', quiet=True)"; \
      chown -R nonroot:nonroot /opt/models/nltk_data; \
    fi

COPY schemas                    /app/schemas
COPY processor/__init__.py      \
     processor/common.py        \
     processor/model_jobs.py     \
     processor/model_service.py /app/processor/
COPY processor/operators/__init__.py     \
     processor/operators/kenlm_score.py  \
     processor/operators/quality.py      \
     processor/operators/source_classifiers.py \
     processor/operators/shadow_models.py /app/processor/operators/

# Each immutable profile base validates its exact native/model dependencies.
# Repository tests validate this source-only layer without reloading that base.
WORKDIR /app
USER nonroot

ENTRYPOINT ["python", "-m", "processor.model_service"]
