-- ObsAQIDB diagnostics for AQI hourly station FK failures.
-- Run on ObsAQIDB before/after applying obs_aqidb_generic_timeseries_aqi_hourly_rpcs.sql.

-- 1) AQI rows whose stored station_id no longer matches ObsAQIDB core timeseries.
select
  count(*) as rows_with_mismatched_station_id,
  count(distinct h.timeseries_id) as timeseries_with_mismatched_station_id,
  (array_agg(distinct h.timeseries_id order by h.timeseries_id) filter (where h.timeseries_id is not null))[1:25]
    as sample_timeseries_ids
from uk_aq_aqilevels.timeseries_aqi_hourly h
join uk_aq_core.timeseries ts
  on ts.id = h.timeseries_id
 and ts.connector_id = h.connector_id
where h.station_id is distinct from ts.station_id;

-- 2) Core timeseries that would produce nullable AQI station links.
select
  count(*) as core_timeseries_with_null_station_id,
  (array_agg(ts.id order by ts.id))[1:25] as sample_timeseries_ids
from uk_aq_core.timeseries ts
where ts.station_id is null;

-- 3) Core timeseries with station_id values that do not resolve in ObsAQIDB stations.
select
  count(*) as core_timeseries_with_missing_station_fk,
  (array_agg(ts.id order by ts.id))[1:25] as sample_timeseries_ids
from uk_aq_core.timeseries ts
left join uk_aq_core.stations s
  on s.id = ts.station_id
where ts.station_id is not null
  and s.id is null;

-- 4) Connector-scoped timeseries/station mismatches in ObsAQIDB core reference data.
select
  count(*) as core_timeseries_station_connector_mismatches,
  (array_agg(ts.id order by ts.id))[1:25] as sample_timeseries_ids
from uk_aq_core.timeseries ts
join uk_aq_core.stations s
  on s.id = ts.station_id
where ts.connector_id is distinct from s.connector_id;
