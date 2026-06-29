import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { Client as PgClient } from "pg";

export const SERVICE_NAME = "uk_aq_metadata_refresh_service";
export const DEFAULT_QUIET_PERIOD_SECONDS = 120;

export function logStructured(severity, event, details = {}) {
  const entry = { severity, event, timestamp: new Date().toISOString(), service: SERVICE_NAME, ...details };
  const line = JSON.stringify(entry);
  if (severity === "ERROR") console.error(line);
  else if (severity === "WARNING") console.warn(line);
  else console.log(line);
}

export function parsePositiveInt(raw, fallback, min = 1, max = 86400) {
  const n = Number(raw);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(n)));
}

function dbUrlFromEnv(env = process.env) {
  return env.OBS_AQIDB_DB_URL || env.OBS_AQIDB_DATABASE_URL || env.SUPABASE_DB_URL || env.DATABASE_URL || "";
}

export function buildCommands(env = process.env) {
  const coreCommand = env.UK_AQ_METADATA_REFRESH_CORE_SNAPSHOT_COMMAND || "node scripts/backup_r2/uk_aq_core_snapshot_to_r2.mjs";
  const latestCommand = env.UK_AQ_METADATA_REFRESH_LATEST_SNAPSHOT_COMMAND || "deno run --allow-env --allow-net --allow-read --allow-write --allow-run workers/uk_aq_latest_snapshot_cloud_run/run_job.ts";
  return { coreCommand, latestCommand };
}

function shellCommand(command, extraEnv = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, { shell: true, stdio: "inherit", env: { ...process.env, ...extraEnv } });
    child.on("error", reject);
    child.on("close", (code, signal) => {
      if (code === 0) resolve({ code, signal });
      else reject(new Error(`Command failed (${code ?? signal}): ${command}`));
    });
  });
}

export async function getOldestPending(client) {
  const result = await client.query(`
    select id, created_at
    from uk_aq_ops.metadata_refresh_requests
    where request_type = 'core_metadata_refresh'
      and status in ('pending', 'retryable')
    order by created_at asc, id asc
    limit 1
  `);
  return result.rows[0] || null;
}

export function isEligible(oldest, nowMs = Date.now(), quietPeriodSeconds = DEFAULT_QUIET_PERIOD_SECONDS) {
  if (!oldest) return false;
  return nowMs - new Date(oldest.created_at).getTime() >= quietPeriodSeconds * 1000;
}

export async function claimEligibleBatch(client, { batchId = randomUUID(), quietPeriodSeconds = DEFAULT_QUIET_PERIOD_SECONDS } = {}) {
  await client.query("begin");
  try {
    const result = await client.query(`
      with eligible as (
        select id
        from uk_aq_ops.metadata_refresh_requests
        where request_type = 'core_metadata_refresh'
          and status in ('pending', 'retryable')
          and created_at <= now() - ($1::int * interval '1 second')
        order by created_at asc, id asc
        for update skip locked
      )
      update uk_aq_ops.metadata_refresh_requests r
      set status = 'processing',
          batch_id = $2::uuid,
          claimed_at = now(),
          attempt_count = attempt_count + 1,
          failed_at = null,
          last_error = null
      from eligible
      where r.id = eligible.id
      returning r.id
    `, [quietPeriodSeconds, batchId]);
    await client.query("commit");
    return { batchId, ids: result.rows.map((row) => Number(row.id)) };
  } catch (error) {
    await client.query("rollback");
    throw error;
  }
}

export async function markBatchComplete(client, batchId) {
  await client.query(`
    update uk_aq_ops.metadata_refresh_requests
    set status = 'completed', completed_at = now(), failed_at = null, last_error = null
    where batch_id = $1::uuid and status = 'processing'
  `, [batchId]);
}

export async function markBatchFailed(client, batchId, error) {
  const message = error instanceof Error ? error.message : String(error);
  await client.query(`
    update uk_aq_ops.metadata_refresh_requests
    set status = case when attempt_count >= 5 then 'failed' else 'retryable' end,
        failed_at = now(),
        last_error = left($2, 4000)
    where batch_id = $1::uuid and status = 'processing'
  `, [batchId, message]);
}

export async function runRefreshBatch({ client, env = process.env, runCommand = shellCommand, logger = logStructured } = {}) {
  const quietPeriodSeconds = parsePositiveInt(env.UK_AQ_METADATA_REFRESH_QUIET_PERIOD_SECONDS, DEFAULT_QUIET_PERIOD_SECONDS, 1, 3600);
  const oldest = await getOldestPending(client);
  if (!oldest) {
    logger("INFO", "metadata_refresh_no_pending");
    return { processed: false, reason: "no_pending" };
  }
  if (!isEligible(oldest, Date.now(), quietPeriodSeconds)) {
    logger("INFO", "metadata_refresh_quiet_period", { oldest_id: Number(oldest.id), oldest_created_at: oldest.created_at });
    return { processed: false, reason: "quiet_period" };
  }

  const batch = await claimEligibleBatch(client, { quietPeriodSeconds });
  if (batch.ids.length === 0) {
    logger("INFO", "metadata_refresh_no_claimed");
    return { processed: false, reason: "no_claimed" };
  }

  logger("INFO", "metadata_refresh_batch_claimed", { batch_id: batch.batchId, request_count: batch.ids.length });
  try {
    const { coreCommand, latestCommand } = buildCommands(env);
    await runCommand(coreCommand, {});
    await runCommand(latestCommand, { UK_AQ_LATEST_SNAPSHOT_FORCE_METADATA_REFRESH: "1", UK_AQ_LATEST_SNAPSHOT_TRIGGER_MODE: "metadata_refresh" });
    logger("INFO", "metadata_refresh_cache_purge_skipped", { reason: "latest_snapshot_realtime_profile_short_ttl" });
    await markBatchComplete(client, batch.batchId);
    logger("INFO", "metadata_refresh_batch_completed", { batch_id: batch.batchId });
    return { processed: true, batchId: batch.batchId, ids: batch.ids };
  } catch (error) {
    await markBatchFailed(client, batch.batchId, error);
    logger("ERROR", "metadata_refresh_batch_failed", { batch_id: batch.batchId, error: error instanceof Error ? error.message : String(error) });
    throw error;
  }
}

export async function main(env = process.env) {
  const connectionString = dbUrlFromEnv(env);
  if (!connectionString) throw new Error("Missing database URL: set OBS_AQIDB_DB_URL, OBS_AQIDB_DATABASE_URL, SUPABASE_DB_URL, or DATABASE_URL");
  const client = new PgClient({ connectionString, ssl: env.PGSSLMODE === "disable" ? false : { rejectUnauthorized: false } });
  await client.connect();
  try {
    return await runRefreshBatch({ client, env });
  } finally {
    await client.end();
  }
}
