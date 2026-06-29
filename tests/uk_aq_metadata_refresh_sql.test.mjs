import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const sql = readFileSync("db/migrations/20260629_metadata_refresh_requests.sql", "utf8");

function hasTrigger(table, ...columns) {
  assert.match(sql, new RegExp(`create trigger uk_aq_core_${table}_metadata_refresh_outbox[\\s\\S]*after update on uk_aq_core\\.${table}`, "i"));
  for (const column of columns) {
    assert.match(sql, new RegExp(`'${column}'`, "i"), `${table} should watch ${column}`);
  }
}

test("metadata refresh outbox table has required status, batch, pending, and error indexes", () => {
  assert.match(sql, /create table if not exists uk_aq_ops\.metadata_refresh_requests/i);
  assert.match(sql, /status text not null default 'pending'/i);
  assert.match(sql, /batch_id uuid null/i);
  assert.match(sql, /attempt_count integer not null default 0/i);
  assert.match(sql, /metadata_refresh_requests_pending_idx/i);
  assert.match(sql, /metadata_refresh_requests_batch_idx/i);
  assert.match(sql, /metadata_refresh_requests_recent_errors_idx/i);
});

test("trigger function inserts only when watched columns actually change", () => {
  assert.match(sql, /to_jsonb\(OLD\) -> column_name is distinct from to_jsonb\(NEW\) -> column_name/i);
  assert.match(sql, /if not changed then\s+return NEW;/i);
  assert.match(sql, /insert into uk_aq_ops\.metadata_refresh_requests/i);
});

test("metadata refresh triggers cover watched core metadata tables and columns", () => {
  hasTrigger("networks", "public_display_enabled", "display_name", "network_code", "network_type");
  hasTrigger("stations", "network_id", "connector_id", "station_name", "label", "station_ref", "pcon_code", "la_code", "removed_at");
  hasTrigger("connectors", "connector_code", "display_name", "label", "station_display_name_template", "default_network_id");
  hasTrigger("timeseries", "station_id", "connector_id", "phenomenon_id", "label", "uom");
  hasTrigger("phenomena", "observed_property_id", "pollutant_label", "label", "notation", "source_label");
  hasTrigger("observed_properties", "code", "display_name");
});
