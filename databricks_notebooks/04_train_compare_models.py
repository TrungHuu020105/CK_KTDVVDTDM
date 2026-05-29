# Databricks notebook source
# 04_train_compare_models.py

import time
import math
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

try:
    from xgboost import XGBRegressor
    HAS_XGB = True
except Exception:
    HAS_XGB = False

catalog = "iot_cloud"
schema = "sensor_analytics"
gold = f"{catalog}.{schema}.gold_sensor_features"
eval_table = f"{catalog}.{schema}.model_evaluation_results"
forecast_table = f"{catalog}.{schema}.forecast_results"

pdf = spark.table(gold).dropna(subset=["value", "lag_1", "rolling_mean_6"]).toPandas()
feature_cols = ["temperature", "humidity", "hour_of_day", "day_of_week", "month", "lag_1", "lag_3", "lag_6", "rolling_mean_6", "rolling_mean_24"]

all_eval = []
all_forecasts = []

for (sensor_id, target), group in pdf.groupby(["sensor_id", "target"]):
    group = group.sort_values("event_ts").dropna(subset=feature_cols + ["value"])
    if len(group) < 48:
        continue

    split = max(1, int(len(group) * 0.8))
    train = group.iloc[:split]
    test = group.iloc[split:]
    if test.empty:
        continue

    models = {
        "ridge": Ridge(alpha=1.0),
        "random_forest": RandomForestRegressor(n_estimators=120, random_state=42, n_jobs=-1),
    }
    if HAS_XGB:
        models["xgboost"] = XGBRegressor(n_estimators=180, max_depth=3, learning_rate=0.05, subsample=0.9, random_state=42)

    results = []
    for name, model in models.items():
        start = time.time()
        model.fit(train[feature_cols], train["value"])
        pred = model.predict(test[feature_cols])
        elapsed = time.time() - start
        mae = float(mean_absolute_error(test["value"], pred))
        rmse = float(math.sqrt(mean_squared_error(test["value"], pred)))
        results.append((name, model, mae, rmse, elapsed))

    best_name, best_model, best_mae, best_rmse, best_elapsed = sorted(results, key=lambda row: row[3])[0]
    trained_at = pd.Timestamp.utcnow()
    for name, model, mae, rmse, elapsed in results:
        all_eval.append({
            "sensor_id": sensor_id,
            "target": target,
            "model_name": name,
            "mae": mae,
            "rmse": rmse,
            "training_time_seconds": float(elapsed),
            "is_best": name == best_name,
            "trained_at": trained_at,
        })

    future = test.tail(min(24, len(test))).copy()
    future_pred = best_model.predict(future[feature_cols])
    for ts, value in zip(future["event_ts"], future_pred):
        row = {
            "sensor_id": sensor_id,
            "target": target,
            "model_name": best_name,
            "forecast_ts": pd.to_datetime(ts),
            "predicted_value": float(value),
            "temperature": float(value) if target == "temperature" else None,
            "humidity": float(value) if target == "humidity" else None,
            "generated_at": trained_at,
        }
        all_forecasts.append(row)

if all_eval:
    spark.createDataFrame(pd.DataFrame(all_eval)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(eval_table)
if all_forecasts:
    spark.createDataFrame(pd.DataFrame(all_forecasts)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(forecast_table)
