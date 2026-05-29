# Databricks notebook source
# 01_create_lakehouse_tables.py

try:
    dbutils.widgets.text("catalog", "iot_cloud")
    dbutils.widgets.text("schema", "sensor_analytics")
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
except Exception:
    catalog = "iot_cloud"
    schema = "sensor_analytics"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.bronze_sensor_readings (
  sensor_id STRING,
  event_ts TIMESTAMP,
  temperature DOUBLE,
  humidity DOUBLE,
  temperature_unit STRING,
  humidity_unit STRING,
  source_type STRING,
  provider STRING,
  environment_type STRING,
  location STRING,
  location_province STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  ingested_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.silver_sensor_readings (
  sensor_id STRING,
  event_ts TIMESTAMP,
  temperature DOUBLE,
  humidity DOUBLE,
  source_type STRING,
  provider STRING,
  environment_type STRING,
  location_province STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  processed_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.gold_sensor_features (
  sensor_id STRING,
  event_ts TIMESTAMP,
  target STRING,
  value DOUBLE,
  temperature DOUBLE,
  humidity DOUBLE,
  hour_of_day INT,
  day_of_week INT,
  month INT,
  lag_1 DOUBLE,
  lag_3 DOUBLE,
  lag_6 DOUBLE,
  rolling_mean_6 DOUBLE,
  rolling_mean_24 DOUBLE,
  source_type STRING,
  location_province STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.model_evaluation_results (
  sensor_id STRING,
  target STRING,
  model_name STRING,
  mae DOUBLE,
  rmse DOUBLE,
  training_time_seconds DOUBLE,
  is_best BOOLEAN,
  trained_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.forecast_results (
  sensor_id STRING,
  target STRING,
  model_name STRING,
  forecast_ts TIMESTAMP,
  predicted_value DOUBLE,
  temperature DOUBLE,
  humidity DOUBLE,
  generated_at TIMESTAMP
) USING DELTA
""")
