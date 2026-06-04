-- Unity Catalog / Delta objects for the ESP32 + Meteostat forecasting pipeline.
-- Default namespace expected by the backend: dtdm.metrics_app_streaming.
--
-- IMPORTANT:
-- Hive Metastore can be disabled when legacy access is turned off. In that
-- case, do not use hive_metastore.*. Use a Unity Catalog catalog instead.
--
-- Recommended for a fresh workspace:
-- 1. Use `dtdm.metrics_app_streaming` to match this project's .env.
-- 2. Set DATABRICKS_CATALOG=dtdm for the Python jobs.
-- 3. If your Databricks workspace uses another Unity Catalog catalog, replace
--    `dtdm` below with that catalog name.
-- 4. Only create a new Unity Catalog catalog with an explicit managed location, for example:
--    CREATE CATALOG my_catalog MANAGED LOCATION 'abfss://container@account.dfs.core.windows.net/path';

CREATE SCHEMA IF NOT EXISTS dtdm.metrics_app_streaming;

USE CATALOG dtdm;
USE SCHEMA metrics_app_streaming;

CREATE TABLE IF NOT EXISTS dtdm.metrics_app_streaming.dim_province (
  province_id STRING NOT NULL,
  province_name STRING,
  region STRING,
  is_active BOOLEAN,
  created_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS dtdm.metrics_app_streaming.dim_location (
  location_id STRING NOT NULL,
  province_id STRING,
  location_name STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  altitude DOUBLE,
  description STRING,
  created_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS dtdm.metrics_app_streaming.dim_device (
  device_id STRING NOT NULL,
  location_id STRING,
  device_name STRING,
  device_type STRING,
  installed_at TIMESTAMP,
  status STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS dtdm.metrics_app_streaming.bronze_esp32_raw (
  device_id STRING,
  location_id STRING,
  province_id STRING,
  event_ts TIMESTAMP,
  temperature DOUBLE,
  humidity DOUBLE,
  ingest_time TIMESTAMP,
  source STRING
)
USING DELTA
PARTITIONED BY (province_id);

CREATE TABLE IF NOT EXISTS dtdm.metrics_app_streaming.bronze_meteostat_hourly (
  location_id STRING,
  province_id STRING,
  event_ts TIMESTAMP,
  temperature_c DOUBLE,
  relative_humidity DOUBLE,
  year INT,
  month INT,
  ingest_time TIMESTAMP,
  source STRING,
  fetch_method STRING,
  station_id STRING,
  station_name STRING,
  station_distance_km DOUBLE
)
USING DELTA
PARTITIONED BY (year, month);

CREATE TABLE IF NOT EXISTS dtdm.metrics_app_streaming.meteostat_sync_status (
  location_id STRING,
  province_id STRING,
  year INT,
  fetch_method STRING,
  station_id STRING,
  station_name STRING,
  station_distance_km DOUBLE,
  expected_hours INT,
  coverage_ratio DOUBLE,
  status STRING,
  row_count INT,
  error_message STRING,
  started_at TIMESTAMP,
  finished_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS dtdm.metrics_app_streaming.dim_weather_station (
  station_id STRING,
  station_name STRING,
  location_id STRING,
  province_id STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  elevation DOUBLE,
  distance_km DOUBLE,
  is_primary BOOLEAN,
  data_source STRING,
  hourly_start TIMESTAMP,
  hourly_end TIMESTAMP,
  created_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS dtdm.metrics_app_streaming.meteostat_station_mapping (
  location_id STRING,
  province_id STRING,
  year INT,
  station_id STRING,
  station_name STRING,
  fetch_method STRING,
  row_count INT,
  expected_hours INT,
  coverage_ratio DOUBLE,
  status STRING,
  selected_at TIMESTAMP
)
USING DELTA;

ALTER TABLE dtdm.metrics_app_streaming.bronze_meteostat_hourly ADD COLUMNS IF NOT EXISTS (
  fetch_method STRING,
  station_id STRING,
  station_name STRING,
  station_distance_km DOUBLE
);

ALTER TABLE dtdm.metrics_app_streaming.meteostat_sync_status ADD COLUMNS IF NOT EXISTS (
  fetch_method STRING,
  station_id STRING,
  station_name STRING,
  station_distance_km DOUBLE,
  expected_hours INT,
  coverage_ratio DOUBLE
);

ALTER TABLE dtdm.metrics_app_streaming.meteostat_station_mapping ADD COLUMNS IF NOT EXISTS (
  expected_hours INT,
  coverage_ratio DOUBLE
);

CREATE TABLE IF NOT EXISTS dtdm.metrics_app_streaming.silver_esp32_cleaned (
  device_id STRING,
  location_id STRING,
  province_id STRING,
  event_ts TIMESTAMP,
  temperature DOUBLE,
  humidity DOUBLE,
  ingest_time TIMESTAMP,
  quality_flag STRING
)
USING DELTA
PARTITIONED BY (province_id);

CREATE TABLE IF NOT EXISTS dtdm.metrics_app_streaming.silver_meteostat_cleaned (
  location_id STRING,
  province_id STRING,
  event_ts TIMESTAMP,
  temperature_c DOUBLE,
  relative_humidity DOUBLE,
  quality_flag STRING
)
USING DELTA
PARTITIONED BY (province_id);

CREATE TABLE IF NOT EXISTS dtdm.metrics_app_streaming.silver_weather_joined (
  location_id STRING,
  province_id STRING,
  device_id STRING,
  hour_ts TIMESTAMP,
  esp32_temperature_avg DOUBLE,
  esp32_humidity_avg DOUBLE,
  meteostat_temperature DOUBLE,
  meteostat_humidity DOUBLE
)
USING DELTA
PARTITIONED BY (province_id);

CREATE TABLE IF NOT EXISTS dtdm.metrics_app_streaming.gold_esp32_hourly (
  device_id STRING,
  location_id STRING,
  province_id STRING,
  hour_ts TIMESTAMP,
  temperature_avg DOUBLE,
  temperature_min DOUBLE,
  temperature_max DOUBLE,
  humidity_avg DOUBLE,
  humidity_min DOUBLE,
  humidity_max DOUBLE,
  record_count INT,
  missing_ratio DOUBLE,
  created_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (province_id);

CREATE TABLE IF NOT EXISTS dtdm.metrics_app_streaming.gold_training_dataset (
  location_id STRING,
  device_id STRING,
  hour_ts TIMESTAMP,
  temperature DOUBLE,
  humidity DOUBLE,
  meteostat_temperature DOUBLE,
  meteostat_humidity DOUBLE,
  hour INT,
  day_of_week INT,
  month INT,
  temp_lag_1 DOUBLE,
  temp_lag_6 DOUBLE,
  temp_lag_12 DOUBLE,
  temp_lag_24 DOUBLE,
  humidity_lag_1 DOUBLE,
  humidity_lag_6 DOUBLE,
  humidity_lag_12 DOUBLE,
  humidity_lag_24 DOUBLE,
  temp_rolling_24 DOUBLE,
  humidity_rolling_24 DOUBLE,
  temp_rolling_168 DOUBLE,
  humidity_rolling_168 DOUBLE,
  temp_target_168 DOUBLE,
  humidity_target_168 DOUBLE,
  created_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS dtdm.metrics_app_streaming.gold_forecast_result (
  device_id STRING,
  location_id STRING,
  province_id STRING,
  forecast_timestamp TIMESTAMP,
  metric_type STRING,
  forecast_value DOUBLE,
  base_forecast_value DOUBLE,
  bias_value DOUBLE,
  calibrated_forecast_value DOUBLE,
  horizon_step INT,
  horizon_days INT,
  model_name STRING,
  model_type STRING,
  training_mode STRING,
  model_scope STRING,
  model_status STRING,
  model_uri STRING,
  mlflow_run_id STRING,
  generated_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (metric_type);

CREATE TABLE IF NOT EXISTS dtdm.metrics_app_streaming.gold_model_metrics (
  model_name STRING,
  model_type STRING,
  training_mode STRING,
  model_scope STRING,
  location_id STRING,
  province_id STRING,
  location_count INT,
  target_variable STRING,
  horizon INT,
  forecast_horizon_hours INT,
  input_window_hours INT,
  mae DOUBLE,
  rmse DOUBLE,
  rse DOUBLE,
  mape DOUBLE,
  r2 DOUBLE,
  training_points INT,
  test_points INT,
  quality_label STRING,
  confidence_score DOUBLE,
  model_uri STRING,
  mlflow_run_id STRING,
  is_best BOOLEAN,
  created_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (target_variable);

ALTER TABLE dtdm.metrics_app_streaming.gold_forecast_result ADD COLUMNS IF NOT EXISTS (
  base_forecast_value DOUBLE,
  bias_value DOUBLE,
  calibrated_forecast_value DOUBLE,
  training_mode STRING,
  model_scope STRING,
  model_uri STRING,
  mlflow_run_id STRING
);

ALTER TABLE dtdm.metrics_app_streaming.gold_model_metrics ADD COLUMNS IF NOT EXISTS (
  training_mode STRING,
  model_scope STRING,
  location_id STRING,
  province_id STRING,
  location_count INT,
  forecast_horizon_hours INT,
  input_window_hours INT,
  model_uri STRING,
  mlflow_run_id STRING,
  is_best BOOLEAN
);

CREATE TABLE IF NOT EXISTS dtdm.metrics_app_streaming.device_calibration_profile (
  device_id STRING,
  location_id STRING,
  metric_type STRING,
  bias_value DOUBLE,
  sample_count INT,
  window_days INT,
  updated_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (metric_type);

CREATE OR REPLACE VIEW dtdm.metrics_app_streaming.model_leaderboard AS
WITH ranked AS (
  SELECT
    model_name,
    model_type,
    training_mode,
    model_scope,
    location_id,
    province_id,
    location_count,
    target_variable,
    mae,
    rmse,
    rse,
    mape,
    r2,
    forecast_horizon_hours,
    input_window_hours,
    model_uri,
    mlflow_run_id,
    training_points AS train_rows,
    test_points AS test_rows,
    is_best,
    created_at,
    ROW_NUMBER() OVER (
      PARTITION BY target_variable, COALESCE(training_mode, 'global'), COALESCE(location_id, 'global')
      ORDER BY rmse ASC NULLS LAST, mae ASC NULLS LAST, r2 DESC NULLS LAST, created_at DESC
    ) AS rank_order
  FROM dtdm.metrics_app_streaming.gold_model_metrics
)
SELECT
  model_name,
  model_type,
  training_mode,
  model_scope,
  location_id,
  province_id,
  location_count,
  target_variable,
  mae,
  rmse,
  rse,
  mape,
  r2,
  forecast_horizon_hours,
  input_window_hours,
  model_uri,
  mlflow_run_id,
  train_rows,
  test_rows,
  COALESCE(is_best, rank_order = 1) AS is_best,
  created_at
FROM ranked;

-- Compatibility views for the current backend .env:
-- DATABRICKS_BRONZE_TABLE=bronze_sensor_readings
-- DATABRICKS_FORECAST_TABLE=forecast_results
-- DATABRICKS_EVALUATION_TABLE=model_evaluation_results
--
-- These views keep the existing backend SQL working while the new pipeline
-- stores canonical data in bronze_esp32_raw, gold_forecast_result, and
-- gold_model_metrics.

CREATE OR REPLACE VIEW dtdm.metrics_app_streaming.bronze_sensor_readings AS
SELECT
  event_ts,
  device_id AS sensor_id,
  CAST(NULL AS STRING) AS station_id,
  'temperature' AS metric_type,
  temperature AS metric_value,
  'C' AS unit,
  'esp32' AS source_type,
  source AS ingestion_source,
  source AS provider,
  CAST(NULL AS STRING) AS environment_type,
  location_id AS location,
  province_id AS location_province,
  CAST(NULL AS DOUBLE) AS latitude,
  CAST(NULL AS DOUBLE) AS longitude,
  ingest_time AS ingested_at
FROM dtdm.metrics_app_streaming.bronze_esp32_raw
WHERE temperature IS NOT NULL
UNION ALL
SELECT
  event_ts,
  device_id AS sensor_id,
  CAST(NULL AS STRING) AS station_id,
  'humidity' AS metric_type,
  humidity AS metric_value,
  '%' AS unit,
  'esp32' AS source_type,
  source AS ingestion_source,
  source AS provider,
  CAST(NULL AS STRING) AS environment_type,
  location_id AS location,
  province_id AS location_province,
  CAST(NULL AS DOUBLE) AS latitude,
  CAST(NULL AS DOUBLE) AS longitude,
  ingest_time AS ingested_at
FROM dtdm.metrics_app_streaming.bronze_esp32_raw
WHERE humidity IS NOT NULL;

CREATE OR REPLACE VIEW dtdm.metrics_app_streaming.forecast_results AS
SELECT
  forecast_timestamp AS forecast_ts,
  device_id AS sensor_id,
  metric_type,
  forecast_value AS y_pred,
  model_name,
  horizon_step AS horizon_hours,
  generated_at AS created_at
FROM dtdm.metrics_app_streaming.gold_forecast_result;

CREATE OR REPLACE VIEW dtdm.metrics_app_streaming.model_evaluation_results AS
WITH ranked AS (
  SELECT
    model_name,
    model_type,
    training_mode,
    model_scope,
    location_id,
    target_variable,
    horizon,
    mae,
    rmse,
    rse,
    mape,
    r2,
    training_points,
    test_points,
    mlflow_run_id,
    model_uri,
    is_best AS stored_is_best,
    created_at,
    ROW_NUMBER() OVER (
      PARTITION BY target_variable, COALESCE(training_mode, 'global'), COALESCE(location_id, 'global')
      ORDER BY rmse ASC NULLS LAST, mae ASC NULLS LAST, r2 DESC NULLS LAST, created_at DESC
    ) AS rank_order
  FROM dtdm.metrics_app_streaming.gold_model_metrics
),
devices AS (
  SELECT device_id, location_id AS device_location_id FROM dtdm.metrics_app_streaming.dim_device
  UNION ALL
  SELECT 'global' AS device_id, CAST(NULL AS STRING) AS device_location_id
)
SELECT
  COALESCE(mlflow_run_id, concat(model_name, '_', target_variable, '_', date_format(created_at, 'yyyyMMddHHmmss'))) AS run_id,
  model_name,
  model_type,
  training_mode,
  model_scope,
  location_id,
  devices.device_id AS sensor_id,
  target_variable AS metric_type,
  target_variable AS target,
  mae,
  rmse * rmse AS mse,
  rmse,
  mape,
  r2,
  rse,
  training_points AS training_rows,
  test_points AS test_rows,
  CAST(NULL AS DOUBLE) AS training_time_seconds,
  COALESCE(stored_is_best, rank_order = 1) AS is_best,
  created_at
FROM ranked
INNER JOIN devices
  ON ranked.training_mode = 'global'
  OR ranked.location_id = devices.device_location_id
  OR devices.device_id = 'global';

OPTIMIZE dtdm.metrics_app_streaming.bronze_meteostat_hourly ZORDER BY (location_id, event_ts);
OPTIMIZE dtdm.metrics_app_streaming.bronze_esp32_raw ZORDER BY (device_id, event_ts);
