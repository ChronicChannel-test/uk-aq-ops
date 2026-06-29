import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const source = readFileSync("workers/uk_aq_latest_snapshot_cloud_run/run_job.ts", "utf8");

test("latest-snapshot force flag bypasses fresh metadata cache path", () => {
  assert.match(source, /UK_AQ_LATEST_SNAPSHOT_FORCE_METADATA_REFRESH/);
  assert.match(source, /if \(!UK_AQ_LATEST_SNAPSHOT_FORCE_METADATA_REFRESH\) \{/);
  assert.match(source, /r2PutObject\(\{[\s\S]*UK_AQ_LATEST_SNAPSHOT_CORE_METADATA_CACHE_KEY/);
});

test("latest-snapshot default still permits fresh metadata cache reuse", () => {
  assert.match(source, /Deno\.env\.get\("UK_AQ_LATEST_SNAPSHOT_FORCE_METADATA_REFRESH"\),\s*false/s);
  assert.match(source, /isMetadataCacheFresh\(parsed\)[\s\S]*refreshed: false/);
});
