# Docker Hub independent local image built from Redpanda's pinned Debian packages.
FROM python:3.11-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && architecture="$(dpkg --print-architecture)" \
 && case "${architecture}" in \
      arm64) \
        redpanda_file=redpanda_24.2.7-1_arm64_c3d2cde48b74d8e9b5bed8de7dcbc8ae.deb; \
        redpanda_sha=fe513efb90a4ab448aa9478e47608a2cab4727adf7858a87c52d668bfb749773; \
        rpk_file=redpanda-rpk_24.2.7-1_arm64_bf41527a2832effa695373fb2d4d22e1.deb; \
        rpk_sha=7f0bcc41d97f4a67b63a11c5e9f16f5250d8b3ed9833bdf1cdddc68fe0a7993d; \
        tuner_file=redpanda-tuner_24.2.7-1_arm64_c95c05024c810336b44e6ced728f8436.deb; \
        tuner_sha=a403c872cc279dc8a3063f6005cf291d76f3669145a927414bede60a4231b5b0 ;; \
      amd64) \
        redpanda_file=redpanda_24.2.7-1_amd64_c6cbc60dba0566a20e5e0a7686e42287.deb; \
        redpanda_sha=efb7b54ee2b4502f29e70670acb82bce3862d8a074b22b66d24493d18f55b391; \
        rpk_file=redpanda-rpk_24.2.7-1_amd64_2f5c5fa32704efe34d9889f0b70b67f9.deb; \
        rpk_sha=05d88ba1d8c5a93108dc9b78e2e1c108167f44daad801701aece9656b11b668c; \
        tuner_file=redpanda-tuner_24.2.7-1_amd64_a884d8deb538dd7b83105e1cb9543885.deb; \
        tuner_sha=e9d9f6922c607b39f0fe9343ebe594d6e03b56753c4d1d022a44b023eb45aa19 ;; \
      *) echo "unsupported architecture: ${architecture}" >&2; exit 1 ;; \
    esac \
 && base=https://linux.pkg.redpanda.com/apt/pool/redpanda-apt \
 && curl -fsSL --retry 5 -o "/tmp/${redpanda_file}" "${base}/${redpanda_file}" \
 && curl -fsSL --retry 5 -o "/tmp/${rpk_file}" "${base}/${rpk_file}" \
 && curl -fsSL --retry 5 -o "/tmp/${tuner_file}" "${base}/${tuner_file}" \
 && echo "${redpanda_sha}  /tmp/${redpanda_file}" | sha256sum -c - \
 && echo "${rpk_sha}  /tmp/${rpk_file}" | sha256sum -c - \
 && echo "${tuner_sha}  /tmp/${tuner_file}" | sha256sum -c - \
 && apt-get install -y "/tmp/${rpk_file}" "/tmp/${tuner_file}" "/tmp/${redpanda_file}" \
 && apt-get purge -y --auto-remove curl \
 && rm -rf /tmp/*.deb /var/lib/apt/lists/*

ENTRYPOINT []
