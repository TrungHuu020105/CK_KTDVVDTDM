# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 4 - Sinh forecast 60 phút tiếp theo
# MAGIC
# MAGIC Notebook này:
# MAGIC - lấy model mới nhất cho từng metric_type
# MAGIC - lấy feature mới nhất từ bảng train
# MAGIC - tạo 60 dự báo cho 60 phút tiếp theo
# MAGIC - ghi vào bảng forecast output
# MAGIC
# MAGIC Bảng output:
# MAGIC - `iot_analytics.sensor_forecast_1h`

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import timedelta
import mlflow.pyfunc
import pandas as pd

spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")

DB_NAME = "iot_analytics"
TRAIN_TABLE = f"{DB_NAME}.sensor_forecast_train"
MODEL_RUNS_TABLE = f"{DB_NAME}.sensor_forecast_model_runs"
FORECAST_TABLE = f"{DB_NAME}.sensor_forecast_1h"

FEATURE_COLS = [
    "value_avg",
    "value_min",
    "value_max",
    "value_std",
    "obs_count",
    "lag_1",
    "lag_2",
    "lag_3",
    "lag_5",
    "lag_10",
    "lag_15",
    "lag_30",
    "lag_60",
    "roll_mean_3",
    "roll_mean_5",
    "roll_mean_10",
    "roll_mean_15",
    "roll_mean_30",
    "roll_mean_60",
    "roll_std_5",
    "roll_std_15",
    "roll_std_30",
    "roll_std_60",
    "minute_of_hour",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "sin_hour",
    "cos_hour",
    "sin_minute",
    "cos_minute",
    "horizon"
]

spark.sql(f'''
CREATE TABLE IF NOT EXISTS {FORECAST_TABLE} (
  forecast_created_at TIMESTAMP,
  base_ts TIMESTAMP,
  forecast_ts TIMESTAMP,
  horizon INT,
  sensor_id STRING,
  location STRING,
  metric_type STRING,
  unit STRING,
  predicted_value DOUBLE,
  model_uri STRING
) USING DELTA
PARTITIONED BY (metric_type)
''')

# COMMAND ----------

latest_model_df = spark.sql(f'''
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY metric_type ORDER BY run_time DESC) AS rn
    FROM {MODEL_RUNS_TABLE}
)
SELECT metric_type, model_uri
FROM ranked
WHERE rn = 1
''')

display(latest_model_df)

# COMMAND ----------

latest_feature_df = spark.sql(f'''
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY sensor_id, metric_type
               ORDER BY minute_ts DESC
           ) AS rn
    FROM {TRAIN_TABLE}
)
SELECT *
FROM ranked
WHERE rn = 1
''')

display(latest_feature_df.select("minute_ts", "sensor_id", "location", "metric_type", "unit").orderBy("sensor_id"))

# COMMAND ----------

model_map = {r["metric_type"]: r["model_uri"] for r in latest_model_df.collect()}
base_pdf = latest_feature_df.toPandas()

forecast_rows = []

for _, row in base_pdf.iterrows():
    metric = row["metric_type"]
    model_uri = model_map.get(metric)

    if model_uri is None:
        continue

    model = mlflow.pyfunc.load_model(model_uri)
    base_ts = row["minute_ts"]

    for h in range(1, 61):
        feature_row = {col: row[col] for col in FEATURE_COLS if col != "horizon"}
        feature_row["horizon"] = h

        pred = float(model.predict(pd.DataFrame([feature_row]))[0])
        forecast_ts = base_ts + timedelta(minutes=h)

        forecast_rows.append((
            base_ts,
            forecast_ts,
            h,
            row["sensor_id"],
            row["location"],
            metric,
            row["unit"],
            pred,
            model_uri
        ))

forecast_rows[:5], len(forecast_rows)

# COMMAND ----------

forecast_df = spark.createDataFrame(
    forecast_rows,
    schema='''
        base_ts timestamp,
        forecast_ts timestamp,
        horizon int,
        sensor_id string,
        location string,
        metric_type string,
        unit string,
        predicted_value double,
        model_uri string
    '''
).withColumn("forecast_created_at", F.current_timestamp()) \
 .select(
     "forecast_created_at",
     "base_ts",
     "forecast_ts",
     "horizon",
     "sensor_id",
     "location",
     "metric_type",
     "unit",
     "predicted_value",
     "model_uri"
 )

(forecast_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(FORECAST_TABLE)
)

display(forecast_df.orderBy("sensor_id", "horizon"))

# COMMAND ----------

display(
    spark.sql(f'''
        SELECT *
        FROM {FORECAST_TABLE}
        ORDER BY forecast_created_at DESC, sensor_id, horizon
        LIMIT 300
    ''')
)