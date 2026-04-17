# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 3 - Train XGBoost Direct + Shared Model
# MAGIC
# MAGIC Notebook này train:
# MAGIC - 1 model cho mỗi metric_type
# MAGIC - shared cho horizon 1..60
# MAGIC - direct forecasting với cột `horizon`
# MAGIC
# MAGIC Kết quả:
# MAGIC - log model vào MLflow
# MAGIC - lưu thông tin run vào bảng metadata

# COMMAND ----------

from pyspark.sql import functions as F
import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")

DB_NAME = "iot_analytics"
TRAIN_TABLE = f"{DB_NAME}.sensor_forecast_train"
METRICS_TABLE = f"{DB_NAME}.sensor_forecast_model_runs"

EXPERIMENT_NAME = "/Shared/iot_forecast_direct_shared_xgboost"

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
CREATE TABLE IF NOT EXISTS {METRICS_TABLE} (
  run_time TIMESTAMP,
  metric_type STRING,
  model_uri STRING,
  mae DOUBLE,
  rmse DOUBLE,
  train_rows BIGINT,
  valid_rows BIGINT
) USING DELTA
''')

mlflow.set_experiment(EXPERIMENT_NAME)

# COMMAND ----------

TRAIN_DAYS = 14

train_df = (
    spark.table(TRAIN_TABLE)
    .filter(F.col("minute_ts") >= F.expr(f"current_timestamp() - INTERVAL {TRAIN_DAYS} DAYS"))
)
metric_types = [r["metric_type"] for r in train_df.select("metric_type").distinct().collect()]
print("Metric types:", metric_types)

# COMMAND ----------

results = []

for metric in metric_types:
    print(f"Training metric_type = {metric}")

    sdf = train_df.filter(F.col("metric_type") == metric).orderBy("minute_ts")
    pdf = sdf.select(["minute_ts", "target"] + FEATURE_COLS).toPandas()
    pdf = pdf.sort_values("minute_ts").reset_index(drop=True)

    split_idx = int(len(pdf) * 0.8)
    train_pdf = pdf.iloc[:split_idx].copy()
    valid_pdf = pdf.iloc[split_idx:].copy()

    X_train = train_pdf[FEATURE_COLS]
    y_train = train_pdf["target"]
    X_valid = valid_pdf[FEATURE_COLS]
    y_valid = valid_pdf["target"]

    model = XGBRegressor(
        objective="reg:squarederror",
        n_estimators=400,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        reg_alpha=0.0,
        reg_lambda=1.0,
        random_state=42
    )

    with mlflow.start_run(run_name=f"xgb_direct_shared_{metric}") as run:
        mlflow.log_param("metric_type", metric)
        mlflow.log_param("strategy", "direct_shared")
        mlflow.log_param("horizon_max", 60)
        mlflow.log_param("feature_count", len(FEATURE_COLS))

        model.fit(X_train, y_train)

        pred_valid = model.predict(X_valid)
        mae = float(mean_absolute_error(y_valid, pred_valid))
        rmse = float(np.sqrt(mean_squared_error(y_valid, pred_valid)))

        mlflow.log_metric("mae", mae)
        mlflow.log_metric("rmse", rmse)
        mlflow.sklearn.log_model(model, artifact_path="model")

        model_uri = f"runs:/{run.info.run_id}/model"

        results.append((
            metric,
            model_uri,
            mae,
            rmse,
            int(len(train_pdf)),
            int(len(valid_pdf))
        ))

results

# COMMAND ----------

result_df = spark.createDataFrame(
    [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in results],
    ["metric_type", "model_uri", "mae", "rmse", "train_rows", "valid_rows"]
).withColumn("run_time", F.current_timestamp()) \
 .select("run_time", "metric_type", "model_uri", "mae", "rmse", "train_rows", "valid_rows")

(result_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(METRICS_TABLE)
)

display(result_df)

# COMMAND ----------

display(
    spark.sql(f'''
        SELECT *
        FROM {METRICS_TABLE}
        ORDER BY run_time DESC
    ''')
)