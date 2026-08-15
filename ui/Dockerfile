# syntax=docker/dockerfile:1.7
#
# Stream2Pretrain cockpit (Next.js 16, App Router).
#
# Multi-stage build:
#   1) deps    - install npm dependencies under a clean cache
#   2) build   - compile the Next.js standalone bundle (output: 'standalone')
#   3) runner  - minimal alpine image with only the standalone server,
#                static assets, and the cosign binary (used by /api/decon/verify)
#
# The runner image runs as a non-root user and exposes port 3000.

ARG NODE_VERSION=20.19.0
ARG ALPINE_VERSION=3.20
ARG COSIGN_VERSION=2.4.0

# ---- 1) deps ---------------------------------------------------------------
FROM node:${NODE_VERSION}-alpine${ALPINE_VERSION} AS deps
WORKDIR /app

# `libc6-compat` keeps native modules (e.g. swc) loadable on alpine.
RUN apk add --no-cache libc6-compat

COPY package.json package-lock.json* ./
RUN --mount=type=cache,target=/root/.npm \
    if [ -f package-lock.json ]; then \
      npm ci --no-audit --no-fund --prefer-offline; \
    else \
      npm install --no-audit --no-fund; \
    fi

# ---- 2) build --------------------------------------------------------------
FROM node:${NODE_VERSION}-alpine${ALPINE_VERSION} AS build
WORKDIR /app

ENV NEXT_TELEMETRY_DISABLED=1 \
    NODE_ENV=production

COPY --from=deps /app/node_modules ./node_modules
COPY . .

RUN npm run build

# ---- cosign fetch ----------------------------------------------------------
# Pulled in a separate stage so its download doesn't bloat the final layer.
FROM alpine:${ALPINE_VERSION} AS cosign
ARG COSIGN_VERSION
ARG TARGETARCH
RUN apk add --no-cache curl ca-certificates \
 && BUILD_ARCH="${TARGETARCH:-$(apk --print-arch)}" \
 && case "${BUILD_ARCH}" in \
      amd64|x86_64) ARCH=amd64 ;; \
      arm64|aarch64) ARCH=arm64 ;; \
      *) echo "unsupported arch ${BUILD_ARCH}" && exit 1 ;; \
    esac \
 && curl -fsSL -o /usr/local/bin/cosign \
      "https://github.com/sigstore/cosign/releases/download/v${COSIGN_VERSION}/cosign-linux-${ARCH}" \
 && chmod 0755 /usr/local/bin/cosign

# ---- 3) runner -------------------------------------------------------------
FROM node:${NODE_VERSION}-alpine${ALPINE_VERSION} AS runner
WORKDIR /app

ENV NODE_ENV=production \
    NEXT_TELEMETRY_DISABLED=1 \
    PORT=3000 \
    HOSTNAME=0.0.0.0 \
    COSIGN_BIN=/usr/local/bin/cosign

# tini for proper PID 1 signal handling; ca-certificates for outbound HTTPS.
RUN apk add --no-cache tini ca-certificates \
 && addgroup -S -g 1001 nodejs \
 && adduser  -S -u 1001 -G nodejs nextjs

COPY --from=cosign /usr/local/bin/cosign /usr/local/bin/cosign

# next.config.js sets `output: 'standalone'`, which produces a self-contained
# server under .next/standalone with only the runtime deps it actually needs.
COPY --from=build --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=build --chown=nextjs:nodejs /app/.next/static    ./.next/static
COPY --from=build --chown=nextjs:nodejs /app/public          ./public

USER nextjs
EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- "http://127.0.0.1:${PORT}/" >/dev/null 2>&1 || exit 1

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "server.js"]
