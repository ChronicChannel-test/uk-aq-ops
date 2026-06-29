import test from "node:test";
import assert from "node:assert/strict";
import {
  isEligible,
  claimEligibleBatch,
  runRefreshBatch,
} from "../workers/uk_aq_metadata_refresh_service/core.mjs";

class FakeClient {
  constructor({ oldest = null, claimIds = [] } = {}) {
    this.oldest = oldest;
    this.claimIds = claimIds;
    this.queries = [];
    this.completed = false;
    this.failed = false;
  }
  async query(sql, params = []) {
    this.queries.push({ sql, params });
    if (/select id, created_at/i.test(sql)) return { rows: this.oldest ? [this.oldest] : [] };
    if (/returning r\.id/i.test(sql)) return { rows: this.claimIds.map((id) => ({ id })) };
    if (/set status = 'completed'/i.test(sql)) { this.completed = true; return { rows: [] }; }
    if (/set status = case when attempt_count >= 5/i.test(sql)) { this.failed = true; return { rows: [] }; }
    return { rows: [] };
  }
}

test("worker exits quickly when no pending requests exist", async () => {
  const client = new FakeClient();
  const result = await runRefreshBatch({ client, runCommand: async () => assert.fail("should not run"), logger: () => {} });
  assert.deepEqual(result, { processed: false, reason: "no_pending" });
});

test("worker exits during two minute quiet period", async () => {
  const client = new FakeClient({ oldest: { id: 1, created_at: new Date().toISOString() } });
  const result = await runRefreshBatch({ client, env: { UK_AQ_METADATA_REFRESH_QUIET_PERIOD_SECONDS: "120" }, runCommand: async () => assert.fail("should not run"), logger: () => {} });
  assert.equal(result.processed, false);
  assert.equal(result.reason, "quiet_period");
});

test("eligibility starts after quiet period", () => {
  const now = Date.parse("2026-06-29T12:02:00.000Z");
  assert.equal(isEligible({ created_at: "2026-06-29T12:00:00.000Z" }, now, 120), true);
  assert.equal(isEligible({ created_at: "2026-06-29T12:00:01.000Z" }, now, 120), false);
});

test("worker claims and coalesces eligible requests into one batch", async () => {
  const client = new FakeClient({ oldest: { id: 1, created_at: "2026-06-29T11:00:00.000Z" }, claimIds: [1, 2, 3] });
  const commands = [];
  const result = await runRefreshBatch({
    client,
    env: { UK_AQ_METADATA_REFRESH_QUIET_PERIOD_SECONDS: "1", UK_AQ_METADATA_REFRESH_CORE_SNAPSHOT_COMMAND: "core", UK_AQ_METADATA_REFRESH_LATEST_SNAPSHOT_COMMAND: "latest" },
    runCommand: async (command, env) => commands.push({ command, env }),
    logger: () => {},
  });
  assert.equal(result.processed, true);
  assert.deepEqual(result.ids, [1, 2, 3]);
  assert.deepEqual(commands.map((entry) => entry.command), ["core", "latest"]);
  assert.equal(commands[1].env.UK_AQ_LATEST_SNAPSHOT_FORCE_METADATA_REFRESH, "1");
  assert.equal(client.completed, true);
});

test("claim query uses FOR UPDATE SKIP LOCKED to prevent double processing", async () => {
  const client = new FakeClient({ claimIds: [42] });
  await claimEligibleBatch(client, { batchId: "00000000-0000-4000-8000-000000000001", quietPeriodSeconds: 120 });
  const claimSql = client.queries.map((q) => q.sql).join("\n");
  assert.match(claimSql, /for update skip locked/i);
  assert.match(claimSql, /status = 'processing'/i);
});

test("worker marks retryable failure when a command fails", async () => {
  const client = new FakeClient({ oldest: { id: 1, created_at: "2026-06-29T11:00:00.000Z" }, claimIds: [1] });
  await assert.rejects(() => runRefreshBatch({
    client,
    env: { UK_AQ_METADATA_REFRESH_QUIET_PERIOD_SECONDS: "1", UK_AQ_METADATA_REFRESH_CORE_SNAPSHOT_COMMAND: "core" },
    runCommand: async () => { throw new Error("boom"); },
    logger: () => {},
  }), /boom/);
  assert.equal(client.failed, true);
});
