-- Create only the Silver/Gold tables that are required after Bronze ingest.
-- This file does not create a catalog or schema.
-- Run it when dtdm.metrics_app_streaming exists but file 05 reports a missing table.

USE CATALOG dtdm;
USE SCHEMA metrics_app_streaming;

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
      PARTITION BY target_variable
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

