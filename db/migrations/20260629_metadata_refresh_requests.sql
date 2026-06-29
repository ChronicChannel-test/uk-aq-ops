create schema if not exists uk_aq_ops;

create table if not exists uk_aq_ops.metadata_refresh_requests (
  id bigint generated always as identity primary key,
  request_type text not null default 'core_metadata_refresh',
  reason text not null,
  source_schema text null,
  source_table text null,
  source_pk text null,
  old_values jsonb null,
  new_values jsonb null,
  status text not null default 'pending',
  batch_id uuid null,
  attempt_count integer not null default 0,
  created_at timestamptz not null default now(),
  claimed_at timestamptz null,
  completed_at timestamptz null,
  failed_at timestamptz null,
  last_error text null,
  constraint metadata_refresh_requests_status_check check (status in ('pending', 'processing', 'completed', 'retryable', 'failed')),
  constraint metadata_refresh_requests_type_check check (request_type = 'core_metadata_refresh')
);

create index if not exists metadata_refresh_requests_pending_idx
  on uk_aq_ops.metadata_refresh_requests (created_at, id)
  where status in ('pending', 'retryable') and request_type = 'core_metadata_refresh';

create index if not exists metadata_refresh_requests_batch_idx
  on uk_aq_ops.metadata_refresh_requests (batch_id, id)
  where batch_id is not null;

create index if not exists metadata_refresh_requests_recent_errors_idx
  on uk_aq_ops.metadata_refresh_requests (failed_at desc, created_at desc)
  where status in ('retryable', 'failed');

create or replace function uk_aq_ops.enqueue_metadata_refresh_if_watched_changed()
returns trigger
language plpgsql
security definer
set search_path = pg_catalog, public, uk_aq_core, uk_aq_ops
as $$
declare
  watched text[];
  changed boolean := false;
  column_name text;
  source_pk_value text;
begin
  if TG_OP <> 'UPDATE' then
    return NEW;
  end if;

  watched := TG_ARGV::text[];
  foreach column_name in array watched loop
    if to_jsonb(OLD) -> column_name is distinct from to_jsonb(NEW) -> column_name then
      changed := true;
      exit;
    end if;
  end loop;

  if not changed then
    return NEW;
  end if;

  source_pk_value := coalesce(to_jsonb(NEW) ->> 'id', to_jsonb(OLD) ->> 'id');

  insert into uk_aq_ops.metadata_refresh_requests (
    reason,
    source_schema,
    source_table,
    source_pk,
    old_values,
    new_values
  ) values (
    format('watched metadata changed on %I.%I', TG_TABLE_SCHEMA, TG_TABLE_NAME),
    TG_TABLE_SCHEMA,
    TG_TABLE_NAME,
    source_pk_value,
    to_jsonb(OLD),
    to_jsonb(NEW)
  );

  return NEW;
end;
$$;

drop trigger if exists uk_aq_core_networks_metadata_refresh_outbox on uk_aq_core.networks;

create trigger uk_aq_core_networks_metadata_refresh_outbox
  after update on uk_aq_core.networks
  for each row execute function uk_aq_ops.enqueue_metadata_refresh_if_watched_changed(
    'public_display_enabled', 'display_name', 'network_code', 'network_type'
  );

drop trigger if exists uk_aq_core_stations_metadata_refresh_outbox on uk_aq_core.stations;

create trigger uk_aq_core_stations_metadata_refresh_outbox
  after update on uk_aq_core.stations
  for each row execute function uk_aq_ops.enqueue_metadata_refresh_if_watched_changed(
    'network_id', 'connector_id', 'station_name', 'label', 'station_ref', 'pcon_code', 'la_code', 'removed_at'
  );

drop trigger if exists uk_aq_core_connectors_metadata_refresh_outbox on uk_aq_core.connectors;

create trigger uk_aq_core_connectors_metadata_refresh_outbox
  after update on uk_aq_core.connectors
  for each row execute function uk_aq_ops.enqueue_metadata_refresh_if_watched_changed(
    'connector_code', 'display_name', 'label', 'station_display_name_template', 'default_network_id'
  );

drop trigger if exists uk_aq_core_timeseries_metadata_refresh_outbox on uk_aq_core.timeseries;

create trigger uk_aq_core_timeseries_metadata_refresh_outbox
  after update on uk_aq_core.timeseries
  for each row execute function uk_aq_ops.enqueue_metadata_refresh_if_watched_changed(
    'station_id', 'connector_id', 'phenomenon_id', 'label', 'uom'
  );

drop trigger if exists uk_aq_core_phenomena_metadata_refresh_outbox on uk_aq_core.phenomena;

create trigger uk_aq_core_phenomena_metadata_refresh_outbox
  after update on uk_aq_core.phenomena
  for each row execute function uk_aq_ops.enqueue_metadata_refresh_if_watched_changed(
    'observed_property_id', 'pollutant_label', 'label', 'notation', 'source_label'
  );

drop trigger if exists uk_aq_core_observed_properties_metadata_refresh_outbox on uk_aq_core.observed_properties;

create trigger uk_aq_core_observed_properties_metadata_refresh_outbox
  after update on uk_aq_core.observed_properties
  for each row execute function uk_aq_ops.enqueue_metadata_refresh_if_watched_changed(
    'code', 'display_name'
  );
