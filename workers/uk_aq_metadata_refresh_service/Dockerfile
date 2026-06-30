FROM node:22-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates unzip && rm -rf /var/lib/apt/lists/* \
  && curl -fsSL https://deno.land/install.sh | DENO_INSTALL=/usr/local sh \
  && ln -s /usr/local/bin/deno /usr/bin/deno
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY workers/uk_aq_metadata_refresh_service ./workers/uk_aq_metadata_refresh_service
COPY scripts/backup_r2 ./scripts/backup_r2
COPY workers/uk_aq_latest_snapshot_cloud_run ./workers/uk_aq_latest_snapshot_cloud_run
COPY workers/shared/r2_sigv4.mjs ./workers/shared/r2_sigv4.mjs
CMD ["node", "workers/uk_aq_metadata_refresh_service/server.mjs"]
