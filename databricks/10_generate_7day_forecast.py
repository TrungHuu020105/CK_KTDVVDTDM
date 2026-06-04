# Databricks notebook source
"""Generate 168-hour forecasts for locations using best models selected by 09.

Flow:
  - 09_select_best_models.py marks best rows in gold_model_metrics.
  - This notebook defaults to FORECAST_MODEL_STRATEGY=global_best_type.
  - For each target, it uses the best global model type from 09.
  - Forecast rows are still generated separately for each selected location_id.
  - Set FORECAST_MODEL_STRATEGY=per_location_first to prefer per-location best rows.
"""

# COMMAND ----------

# Imports

import os
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import mlflow
import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient
from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor, GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import functions as F


# COMMAND ----------

# Constants and configuration

DEFAULTS = {
    "DATABRICKS_CATALOG": "dtdm",
    "DATABRICKS_SCHEMA": "metrics_app_streaming",
    "MLFLOW_EXPERIMENT_NAME": "/Shared/metrics_app_streaming_weather_forecast",
    "FORECAST_HORIZON_HOURS": "168",
    "forecast_horizon_hours": "168",
    "INPUT_WINDOW_HOURS": "168",
    "input_window_hours": "168",
    "CALIBRATION_WINDOW_DAYS": "7",
    "FORECAST_LOCATION_ID": "",
    "forecast_location_id": "",
    "FORECAST_MODEL_STRATEGY": "global_best_type",
    "forecast_model_strategy": "global_best_type",
    "LOCATION_SET": "current_34",
    "location_set": "current_34",
    "INCLUDE_INACTIVE_LOCATIONS": "false",
    "include_inactive_locations": "false",
    "MLFLOW_LOCAL_TMP_DIR": "/local_disk0/tmp/mlflow_model_downloads",
}

STALE_CATALOG_WARNING_SHOWN = False

BASE_NUMERIC_FEATURES = [
    "temperature",
    "humidity",
    "meteostat_temperature",
    "meteostat_humidity",
    "hour",
    "day_of_week",
    "month",
    "temp_lag_1",
    "temp_lag_6",
    "temp_lag_12",
    "temp_lag_24",
    "humidity_lag_1",
    "humidity_lag_6",
    "humidity_lag_12",
    "humidity_lag_24",
    "temp_rolling_24",
    "humidity_rolling_24",
    "temp_rolling_168",
    "humidity_rolling_168",
]
BASE_CATEGORICAL_FEATURES = ["location_id", "province_id", "device_id"]
SEQUENCE_COLS = ["temperature", "humidity", "meteostat_temperature", "meteostat_humidity"]
TARGET_LABELS = {
    "temperature": "temp_target_168",
    "humidity": "humidity_target_168",
}


# COMMAND ----------

# Environment and widget helpers

def load_local_env():
    candidates = []
    try:
        candidates.append(Path(__file__).resolve().parents[1] / ".env")
    except Exception:
        pass
    candidates.append(Path.cwd() / ".env")
    for env_path in candidates:
        if not env_path.exists():
            continue
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
        return


load_local_env()


def widget_value(name):
    try:
        value = dbutils.widgets.get(name).strip()  # type: ignore[name-defined]
        return value if value else None
    except Exception:
        return None


def setting(name):
    value = os.getenv(name) or widget_value(name) or DEFAULTS.get(name, "")
    if name == "DATABRICKS_CATALOG" and value in ("workspace", "hive_metastore"):
        return "dtdm"
    return value


def warn_stale_catalog_once():
    global STALE_CATALOG_WARNING_SHOWN
    value = os.getenv("DATABRICKS_CATALOG") or widget_value("DATABRICKS_CATALOG") or DEFAULTS["DATABRICKS_CATALOG"]
    if value in ("workspace", "hive_metastore") and not STALE_CATALOG_WARNING_SHOWN:
        print("WARNING: ignoring stale DATABRICKS_CATALOG=" + value + "; using dtdm.")
        STALE_CATALOG_WARNING_SHOWN = True


def setting_any(*names):
    for name in names:
        value = setting(name)
        if value not in ("", None):
            return value
    return ""


def int_setting(default_value, *names):
    raw = setting_any(*names)
    try:
        return int(raw)
    except Exception:
        return int(default_value)


def bool_setting(*names):
    return str(setting_any(*names)).strip().lower() in ("1", "true", "yes", "on")


def requested_location_ids():
    raw = setting_any("FORECAST_LOCATION_ID", "forecast_location_id", "LOCATION_ID", "location_id").strip()
    if raw.lower() in ("", "all", "*"):
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def create_widgets():
    try:
        for name, default in DEFAULTS.items():
            dbutils.widgets.text(name, os.getenv(name, default))  # type: ignore[name-defined]
    except Exception:
        pass


def fq_table(name):
    return setting("DATABRICKS_CATALOG") + "." + setting("DATABRICKS_SCHEMA") + "." + name


def table_exists(name):
    rows = spark.sql(  # type: ignore[name-defined]
        "SHOW TABLES IN "
        + setting("DATABRICKS_CATALOG")
        + "."
        + setting("DATABRICKS_SCHEMA")
        + " LIKE '"
        + name
        + "'"
    ).collect()
    return len(rows) > 0


def prepare_mlflow_tmp_dir():
    candidates = [
        setting("MLFLOW_LOCAL_TMP_DIR"),
        "/local_disk0/tmp/mlflow_model_downloads",
        "/tmp/mlflow_model_downloads",
    ]
    for candidate in candidates:
        if not candidate:
            continue
        try:
            tmp_dir = Path(candidate)
            tmp_dir.mkdir(parents=True, exist_ok=True)
            for env_name in ("TMPDIR", "TEMP", "TMP", "MLFLOW_TMP_DIR"):
                os.environ[env_name] = str(tmp_dir)
            tempfile.tempdir = str(tmp_dir)
            return str(tmp_dir)
        except Exception as exc:
            print("WARNING: could not use MLflow temp dir " + str(candidate) + ": " + str(exc))
    return None


# COMMAND ----------

# Location and model status helpers

def filter_locations_df(locations_df):
    location_set = setting_any("LOCATION_SET", "location_set").strip().lower()
    if location_set in ("current_34", "34", "current"):
        locations_df = locations_df.where(F.col("location_id").startswith("loc34_"))
    elif location_set in ("legacy_63", "63", "legacy"):
        locations_df = locations_df.where(~F.col("location_id").startswith("loc34_"))

    requested = requested_location_ids()
    if requested:
        locations_df = locations_df.where(F.col("location_id").isin(requested))

    province_table = fq_table("dim_province")
    if bool_setting("INCLUDE_INACTIVE_LOCATIONS", "include_inactive_locations") or not table_exists("dim_province"):
        return locations_df

    active_provinces = (
        spark.table(province_table)  # type: ignore[name-defined]
        .where(F.coalesce(F.col("is_active"), F.lit(True)) == F.lit(True))
        .select(F.col("province_id").alias("active_province_id"))
        .dropDuplicates(["active_province_id"])
    )
    return locations_df.join(
        active_provinces,
        locations_df.province_id == active_provinces.active_province_id,
        "inner",
    ).drop("active_province_id")


def status_for(metric_row, fallback):
    if fallback:
        return fallback
    if not metric_row:
        return "insufficient_data"
    name = str(metric_row.get("model_name") or "").lower()
    model_type = metric_row.get("model_type")
    if "gbt" in name:
        return "trained_spark_gbt"
    if "hist gradient" in name:
        return "trained_hist_gradient_boosting"
    if "random forest" in name:
        return "trained_random_forest"
    if "extra trees" in name:
        return "trained_extra_trees"
    if "lstm" in name and "cnn" not in name:
        return "trained_lstm"
    if "gru" in name:
        return "trained_gru"
    if "tft" in name:
        return "trained_tft"
    if model_type == "baseline":
        return "fallback_naive"
    return "trained_" + str(model_type or "model")


# COMMAND ----------

# Data loading

def load_history():
    table_name = fq_table("gold_training_dataset")
    if not table_exists("gold_training_dataset"):
        raise RuntimeError(table_name + " does not exist. Run 06_gold_feature_engineering.py first.")
    sdf = spark.table(table_name).orderBy("hour_ts")  # type: ignore[name-defined]
    if table_exists("dim_location"):
        loc = spark.table(fq_table("dim_location")).select("location_id", "province_id").dropDuplicates(["location_id"])  # type: ignore[name-defined]
        sdf = sdf.drop("province_id").join(loc, "location_id", "left")
    requested = requested_location_ids()
    if requested:
        sdf = sdf.where(F.col("location_id").isin(requested))
    pdf = sdf.toPandas()
    if pdf.empty:
        raise RuntimeError("gold_training_dataset has no rows for the requested forecast location(s); cannot forecast.")
    pdf["hour_ts"] = pd.to_datetime(pdf["hour_ts"])
    pdf["device_id"] = pdf.get("device_id", pd.Series(["meteostat_only"] * len(pdf))).fillna("meteostat_only").astype(str)
    pdf["location_id"] = pdf.get("location_id", pd.Series(["unknown_location"] * len(pdf))).fillna("unknown_location").astype(str)
    pdf["province_id"] = pdf.get("province_id", pd.Series(["unknown"] * len(pdf))).fillna("unknown").astype(str)
    return pdf.sort_values(["location_id", "device_id", "hour_ts"]).reset_index(drop=True)


def load_devices():
    device_table = fq_table("dim_device")
    location_table = fq_table("dim_location")
    if table_exists("dim_device") and table_exists("dim_location"):
        locations = filter_locations_df(spark.table(location_table))  # type: ignore[name-defined]
        devices = (
            spark.table(device_table)  # type: ignore[name-defined]
            .join(locations, "location_id", "inner")
            .select("device_id", "location_id", "province_id")
            .where(F.col("location_id").isNotNull())
            .toPandas()
        )
        if not devices.empty:
            devices["device_id"] = devices["device_id"].astype(str)
            devices["location_id"] = devices["location_id"].astype(str)
            return devices
    locations = filter_locations_df(spark.table(location_table)).select("location_id", "province_id").toPandas()  # type: ignore[name-defined]
    locations["device_id"] = "meteostat_only"
    devices = locations[["device_id", "location_id", "province_id"]]
    if devices.empty:
        requested = requested_location_ids()
        suffix = " for FORECAST_LOCATION_ID=" + ",".join(requested) if requested else ""
        raise RuntimeError("No forecast devices/locations found" + suffix + ".")
    return devices


def load_metrics():
    table_name = fq_table("gold_model_metrics")
    if not table_exists("gold_model_metrics"):
        return pd.DataFrame()
    pdf = spark.table(table_name).toPandas()  # type: ignore[name-defined]
    if pdf.empty:
        return pdf
    if "model_type" in pdf.columns:
        pdf = pdf[pdf["model_type"].isin(["spark_mllib", "deep_learning"])].copy()
    if pdf.empty:
        return pdf
    pdf["location_id"] = pdf.get("location_id", pd.Series([None] * len(pdf)))
    pdf["training_mode"] = pdf.get("training_mode", pd.Series(["global"] * len(pdf))).fillna("global")
    pdf["is_best"] = pdf.get("is_best", pd.Series([False] * len(pdf))).fillna(False)
    return pdf


def select_best_model(metrics_pdf, target_variable, location_id):
    if metrics_pdf is None or metrics_pdf.empty:
        return None
    base = metrics_pdf[metrics_pdf["target_variable"] == target_variable].copy()
    if base.empty:
        return None

    strategy = setting_any("FORECAST_MODEL_STRATEGY", "forecast_model_strategy").strip().lower()
    global_rows = base[(base["training_mode"] == "global") & (base["is_best"] == True)]
    if global_rows.empty:
        global_rows = base[base["training_mode"] == "global"]
    if strategy in ("global_best_type", "global", "global_first"):
        if not global_rows.empty:
            return global_rows.sort_values(["rmse", "mae", "r2"], ascending=[True, True, False]).iloc[0].to_dict()

    per_location = base[
        (base["training_mode"] == "per_location")
        & (base["location_id"].astype(str) == str(location_id))
        & (base["is_best"] == True)
    ]
    if per_location.empty:
        per_location = base[(base["training_mode"] == "per_location") & (base["location_id"].astype(str) == str(location_id))]
    if not per_location.empty:
        return per_location.sort_values(["rmse", "mae", "r2"], ascending=[True, True, False]).iloc[0].to_dict()

    if not global_rows.empty:
        return global_rows.sort_values(["rmse", "mae", "r2"], ascending=[True, True, False]).iloc[0].to_dict()
    return None


def model_scope_for(metric_row, location_id):
    if not metric_row:
        return "fallback"
    training_mode = str(metric_row.get("training_mode") or "global")
    if training_mode == "per_location":
        return str(metric_row.get("location_id") or location_id)
    return "global"


def print_forecast_plan(devices, metrics_pdf):
    requested = requested_location_ids()
    if requested:
        print("Forecast location filter: " + ", ".join(requested))
    else:
        print("Forecast location filter: all locations selected by LOCATION_SET=" + setting_any("LOCATION_SET", "location_set"))
    print("Forecast model strategy: " + setting_any("FORECAST_MODEL_STRATEGY", "forecast_model_strategy"))

    locations = sorted(devices["location_id"].astype(str).dropna().unique().tolist())
    for location_id in locations:
        for target_variable in ["temperature", "humidity"]:
            metric_row = select_best_model(metrics_pdf, target_variable, location_id)
            if metric_row:
                print(
                    "Forecast plan: location="
                    + str(location_id)
                    + ", target="
                    + target_variable
                    + ", model="
                    + str(metric_row.get("model_name"))
                    + ", type="
                    + str(metric_row.get("model_type"))
                    + ", training_mode="
                    + str(metric_row.get("training_mode"))
                    + ", scope="
                    + model_scope_for(metric_row, location_id)
                )
            else:
                print(
                    "Forecast plan: location="
                    + str(location_id)
                    + ", target="
                    + target_variable
                    + ", model=fallback_naive"
                )


# COMMAND ----------

# MLflow model loading

def candidate_model_uris(metric_row):
    if not metric_row:
        return []
    run_id = metric_row.get("mlflow_run_id")
    raw_uri = metric_row.get("model_uri")
    candidates = []
    if raw_uri and str(raw_uri) != "nan":
        candidates.append(str(raw_uri))
        if not run_id or str(run_id) == "nan":
            run_id = run_id_from_model_uri(str(raw_uri))
    if run_id and str(run_id) != "nan":
        run_id = str(run_id)
        candidates.extend(model_uris_from_run_metadata(run_id))
        candidates.extend(model_uris_from_logged_models(run_id))
        candidates.extend(model_uris_from_artifacts(run_id))
        candidates.extend(
            [
                "runs:/" + run_id + "/model",
            ]
        )
    return dedupe_model_uris(candidates)


def run_id_from_model_uri(model_uri):
    uri = str(model_uri).strip()
    if not uri.startswith("runs:/"):
        return None
    remainder = uri[len("runs:/") :].lstrip("/")
    if not remainder:
        return None
    return remainder.split("/", 1)[0]


def dedupe_model_uris(candidates):
    deduped = []
    for uri in candidates:
        clean_uri = str(uri).strip()
        if clean_uri.startswith("runs:/"):
            clean_uri = clean_uri.rstrip("/")
        if clean_uri and clean_uri not in deduped:
            deduped.append(clean_uri)
    return deduped


def model_uris_from_run_metadata(run_id):
    uris = []
    try:
        run = mlflow.get_run(run_id)
        history = run.data.tags.get("mlflow.log-model.history")
        if history:
            for item in json.loads(history):
                artifact_path = item.get("artifact_path") or item.get("name")
                model_id = item.get("model_id") or item.get("model_uuid")
                if artifact_path:
                    uris.append("runs:/" + run_id + "/" + str(artifact_path).strip("/"))
                if model_id:
                    uris.append("models:/" + str(model_id))
    except Exception:
        pass
    return uris


def model_uris_from_logged_models(run_id):
    uris = []
    try:
        run = mlflow.get_run(run_id)
        experiment_ids = [run.info.experiment_id]
    except Exception:
        experiment_ids = None

    search_fn = getattr(mlflow, "search_logged_models", None)
    if search_fn is None:
        return uris

    search_attempts = []
    if experiment_ids:
        search_attempts.append({"experiment_ids": experiment_ids, "filter_string": "source_run_id = '" + run_id + "'"})
        search_attempts.append({"experiment_ids": experiment_ids})
    search_attempts.append({"filter_string": "source_run_id = '" + run_id + "'"})
    search_attempts.append({})

    for kwargs in search_attempts:
        try:
            results = search_fn(**kwargs)
            for item in results:
                source_run_id = getattr(item, "source_run_id", None)
                if source_run_id is None and hasattr(item, "info"):
                    source_run_id = getattr(item.info, "source_run_id", None)
                if source_run_id and str(source_run_id) != run_id:
                    continue

                model_uri = getattr(item, "model_uri", None)
                if model_uri is None and hasattr(item, "info"):
                    model_uri = getattr(item.info, "model_uri", None)
                model_id = getattr(item, "model_id", None)
                if model_id is None and hasattr(item, "info"):
                    model_id = getattr(item.info, "model_id", None)

                if model_uri:
                    uris.append(str(model_uri))
                if model_id:
                    uris.append("models:/" + str(model_id))
            if uris:
                break
        except Exception:
            continue
    return uris


def model_uris_from_artifacts(run_id):
    uris = []
    try:
        client = MlflowClient()
        pending = [""]
        visited = set()
        while pending:
            path = pending.pop(0)
            if path in visited:
                continue
            visited.add(path)
            for artifact in client.list_artifacts(run_id, path):
                artifact_path = artifact.path
                if artifact.is_dir:
                    pending.append(artifact_path)
                elif artifact_path.endswith("MLmodel"):
                    normalized_path = artifact_path.replace("\\", "/")
                    if "/sparkml/stages/" in normalized_path:
                        continue
                    model_dir = str(Path(artifact_path).parent).replace("\\", "/")
                    if model_dir and model_dir != ".":
                        uris.append("runs:/" + run_id + "/" + model_dir)
    except Exception:
        pass
    deduped = []
    for uri in uris:
        if uri not in deduped:
            deduped.append(uri)
    return deduped


def load_spark_mlflow_model(model_uri):
    tmp_root = prepare_mlflow_tmp_dir()
    if not tmp_root:
        return mlflow.spark.load_model(model_uri)

    dst_path = str(Path(tmp_root) / ("model_" + uuid4().hex))
    try:
        return mlflow.spark.load_model(model_uri, dst_path=dst_path)
    except TypeError as exc:
        if "dst_path" not in str(exc):
            raise
        return mlflow.spark.load_model(model_uri)
    except Exception as dst_exc:
        try:
            return mlflow.spark.load_model(model_uri)
        except Exception:
            raise dst_exc


def load_mlflow_model(metric_row, model_cache, warn_on_failure=True):
    if not metric_row:
        return None
    uris = candidate_model_uris(metric_row)
    if not uris:
        return None
    cache_key = "|".join(uris)
    if cache_key in model_cache:
        return model_cache[cache_key]

    failures = []
    for model_uri in uris:
        try:
            if metric_row.get("model_type") == "spark_mllib":
                model = load_spark_mlflow_model(model_uri)
            elif metric_row.get("model_type") == "deep_learning":
                model = mlflow.keras.load_model(model_uri)
            else:
                return None
            model_cache[cache_key] = model
            return model
        except Exception as exc:
            failures.append(str(model_uri) + " -> " + str(exc)[:500])

    failure_preview = failures[:6]
    if len(failures) > len(failure_preview):
        failure_preview.append("... " + str(len(failures) - len(failure_preview)) + " more")
    if warn_on_failure:
        print("WARNING: could not load MLflow model candidates: " + " | ".join(failure_preview))
    model_cache[cache_key] = None
    return None


# COMMAND ----------

# Location refit fallback

def spark_preprocess_stages():
    index_cols = []
    stages = []
    for col_name in BASE_CATEGORICAL_FEATURES:
        index_col = col_name + "_idx"
        stages.append(StringIndexer(inputCol=col_name, outputCol=index_col, handleInvalid="keep"))
        index_cols.append(index_col)

    imputed_cols = [col_name + "_imputed" for col_name in BASE_NUMERIC_FEATURES]
    stages.extend(
        [
            Imputer(inputCols=BASE_NUMERIC_FEATURES, outputCols=imputed_cols, strategy="median"),
            VectorAssembler(inputCols=imputed_cols + index_cols, outputCol="raw_features", handleInvalid="keep"),
            StandardScaler(inputCol="raw_features", outputCol="features", withMean=False, withStd=True),
        ]
    )
    return stages


def estimator_from_metric_row(metric_row, label_col):
    name = str(metric_row.get("model_name") or "").lower()
    if "random forest" in name:
        return RandomForestRegressor(
            featuresCol="features",
            labelCol=label_col,
            predictionCol="prediction",
            numTrees=30,
            maxDepth=8,
            seed=42,
        )
    if "decision tree" in name:
        return DecisionTreeRegressor(
            featuresCol="features",
            labelCol=label_col,
            predictionCol="prediction",
            maxDepth=8,
            seed=42,
        )
    if "gbt" in name or "gradient" in name:
        return GBTRegressor(
            featuresCol="features",
            labelCol=label_col,
            predictionCol="prediction",
            maxIter=30,
            maxDepth=5,
            seed=42,
        )
    if "linear" in name:
        return LinearRegression(
            featuresCol="features",
            labelCol=label_col,
            predictionCol="prediction",
            maxIter=60,
            regParam=0.01,
        )
    return None


def fit_location_spark_model(metric_row, location_history, target_variable, location_id, model_cache):
    if not metric_row or metric_row.get("model_type") != "spark_mllib":
        return None
    label_col = TARGET_LABELS.get(target_variable)
    if not label_col or label_col not in location_history.columns:
        return None

    cache_key = "local_refit|" + str(location_id) + "|" + target_variable + "|" + str(metric_row.get("model_name"))
    if cache_key in model_cache:
        return model_cache[cache_key]

    fit_pdf = location_history.copy()
    for col_name in BASE_CATEGORICAL_FEATURES:
        if col_name not in fit_pdf.columns:
            fit_pdf[col_name] = "unknown"
        fit_pdf[col_name] = fit_pdf[col_name].fillna("unknown").astype(str)
    for col_name in BASE_NUMERIC_FEATURES + [label_col]:
        if col_name not in fit_pdf.columns:
            fit_pdf[col_name] = np.nan
        fit_pdf[col_name] = pd.to_numeric(fit_pdf[col_name], errors="coerce")
    for col_name in BASE_NUMERIC_FEATURES:
        if fit_pdf[col_name].notna().sum() == 0:
            fit_pdf[col_name] = 0.0

    fit_pdf = fit_pdf[fit_pdf[label_col].notna()].copy()
    if len(fit_pdf) < 100:
        model_cache[cache_key] = None
        return None

    estimator = estimator_from_metric_row(metric_row, label_col)
    if estimator is None:
        model_cache[cache_key] = None
        return None

    try:
        train_sdf = spark.createDataFrame(fit_pdf[BASE_CATEGORICAL_FEATURES + BASE_NUMERIC_FEATURES + [label_col]])  # type: ignore[name-defined]
        pipeline = Pipeline(stages=spark_preprocess_stages() + [estimator])
        model = pipeline.fit(train_sdf)
        model_cache[cache_key] = model
        print(
            "Refit forecast model locally: location="
            + str(location_id)
            + ", target="
            + target_variable
            + ", model="
            + str(metric_row.get("model_name"))
            + ", rows="
            + str(len(fit_pdf))
        )
        return model
    except Exception as exc:
        print(
            "WARNING: local refit failed for "
            + str(location_id)
            + "/"
            + target_variable
            + " using "
            + str(metric_row.get("model_name"))
            + ": "
            + str(exc)
        )
        model_cache[cache_key] = None
        return None


# COMMAND ----------

# Forecast helpers

def recent_biases(window_days):
    joined_table = fq_table("silver_weather_joined")
    if not table_exists("silver_weather_joined"):
        return pd.DataFrame(columns=["device_id", "location_id", "metric_type", "bias_value", "sample_count"])
    cutoff_expr = F.expr("current_timestamp() - INTERVAL " + str(window_days) + " DAYS")
    joined = spark.table(joined_table).where(F.col("device_id").isNotNull() & (F.col("hour_ts") >= cutoff_expr))  # type: ignore[name-defined]
    temp = joined.where(F.col("esp32_temperature_avg").isNotNull() & F.col("meteostat_temperature").isNotNull()).groupBy(
        "device_id", "location_id"
    ).agg(
        F.avg(F.col("esp32_temperature_avg") - F.col("meteostat_temperature")).alias("bias_value"),
        F.count("*").cast("int").alias("sample_count"),
    ).withColumn("metric_type", F.lit("temperature"))
    hum = joined.where(F.col("esp32_humidity_avg").isNotNull() & F.col("meteostat_humidity").isNotNull()).groupBy(
        "device_id", "location_id"
    ).agg(
        F.avg(F.col("esp32_humidity_avg") - F.col("meteostat_humidity")).alias("bias_value"),
        F.count("*").cast("int").alias("sample_count"),
    ).withColumn("metric_type", F.lit("humidity"))
    return temp.unionByName(hum).toPandas()


def fallback_forecast(series, horizon):
    clean = series.dropna().astype(float)
    if len(clean) < 24:
        return np.repeat(float(clean.iloc[-1]) if len(clean) else np.nan, horizon), "insufficient_data"
    if len(clean) >= 168:
        base = clean.tail(168).to_numpy()
        return np.tile(base, int(np.ceil(horizon / len(base))))[:horizon], "fallback_naive"
    x = np.arange(len(clean.tail(72)))
    y = clean.tail(72).to_numpy()
    slope, intercept = np.polyfit(x, y, 1)
    future_x = np.arange(len(y), len(y) + horizon)
    return intercept + slope * future_x, "fallback_trend"


def lag(values, step):
    return float(values[-step]) if len(values) >= step else float(values[-1])


def build_feature_row(history, future_ts, location_id, province_id, device_id, temp_values, humidity_values):
    row = {
        "location_id": str(location_id),
        "province_id": str(province_id) if province_id is not None else "unknown",
        "device_id": str(device_id),
        "temperature": float(temp_values[-1]),
        "humidity": float(humidity_values[-1]),
        "meteostat_temperature": float(history["meteostat_temperature"].dropna().iloc[-1]) if history["meteostat_temperature"].notna().any() else float(temp_values[-1]),
        "meteostat_humidity": float(history["meteostat_humidity"].dropna().iloc[-1]) if history["meteostat_humidity"].notna().any() else float(humidity_values[-1]),
        # Compatibility for MLflow models trained before Meteostat was reduced to temp/humidity only.
        "pressure": float(history["pressure"].dropna().iloc[-1]) if "pressure" in history.columns and history["pressure"].notna().any() else np.nan,
        "wind_speed": float(history["wind_speed"].dropna().iloc[-1]) if "wind_speed" in history.columns and history["wind_speed"].notna().any() else np.nan,
        "precipitation": float(history["precipitation"].dropna().tail(24).mean()) if "precipitation" in history.columns and history["precipitation"].notna().any() else 0.0,
        "hour": int(future_ts.hour),
        "day_of_week": int(future_ts.dayofweek + 1),
        "month": int(future_ts.month),
        "temp_lag_1": lag(temp_values, 1),
        "temp_lag_6": lag(temp_values, 6),
        "temp_lag_12": lag(temp_values, 12),
        "temp_lag_24": lag(temp_values, 24),
        "humidity_lag_1": lag(humidity_values, 1),
        "humidity_lag_6": lag(humidity_values, 6),
        "humidity_lag_12": lag(humidity_values, 12),
        "humidity_lag_24": lag(humidity_values, 24),
        "temp_rolling_24": float(np.mean(temp_values[-24:])),
        "humidity_rolling_24": float(np.mean(humidity_values[-24:])),
        "temp_rolling_168": float(np.mean(temp_values[-168:])),
        "humidity_rolling_168": float(np.mean(humidity_values[-168:])),
    }
    return pd.DataFrame([row])


def deep_learning_forecast(model, history, target_variable, horizon, input_window):
    values = history[SEQUENCE_COLS].copy()
    values = values.ffill().bfill().dropna()
    if len(values) < input_window:
        return None
    temp_values = history["temperature"].dropna().astype(float).tolist()
    humidity_values = history["humidity"].dropna().astype(float).tolist()
    preds = []
    seq = values.tail(input_window).to_numpy(dtype="float32").tolist()
    for step in range(horizon):
        arr = np.asarray([seq[-input_window:]], dtype="float32")
        pred = float(model.predict(arr, verbose=0).reshape(-1)[0])
        preds.append(pred)
        last = list(seq[-1])
        if target_variable == "temperature":
            last[0] = pred
            temp_values.append(pred)
            if len(last) > 2:
                last[2] = pred
        else:
            last[1] = pred
            humidity_values.append(pred)
            if len(last) > 3:
                last[3] = pred
        seq.append(last)
    return np.asarray(preds)


def model_forecast(model, metric_row, history, target_variable, horizon, location_id, province_id, device_id, start_ts):
    base_series = history["temperature"] if target_variable == "temperature" else history["humidity"]
    fallback_values, fallback_status = fallback_forecast(base_series, horizon)
    if model is None or metric_row is None or metric_row.get("model_type") == "baseline":
        return fallback_values, fallback_status

    if metric_row.get("model_type") == "deep_learning":
        try:
            input_window = int(metric_row.get("input_window_hours") or setting_any("INPUT_WINDOW_HOURS", "input_window_hours") or 168)
            dl_values = deep_learning_forecast(model, history, target_variable, horizon, input_window)
            if dl_values is not None:
                return dl_values, status_for(metric_row, "")
        except Exception as exc:
            print("WARNING: deep learning forecast failed for " + str(location_id) + "/" + target_variable + ": " + str(exc))
        return fallback_values, fallback_status

    temp_values = history["temperature"].dropna().astype(float).tolist()
    humidity_values = history["humidity"].dropna().astype(float).tolist()
    if len(temp_values) < 24 or len(humidity_values) < 24:
        return fallback_values, "insufficient_data"

    preds = []
    for step in range(horizon):
        feature_row = build_feature_row(history, start_ts + pd.Timedelta(hours=step), location_id, province_id, device_id, temp_values, humidity_values)
        try:
            if metric_row.get("model_type") == "spark_mllib":
                spark_feature_row = spark.createDataFrame(feature_row)  # type: ignore[name-defined]
                pred = float(model.transform(spark_feature_row).select("prediction").first()["prediction"])
            else:
                pred = float(model.predict(feature_row)[0])
        except Exception as exc:
            print("WARNING: model predict failed for " + str(location_id) + "/" + target_variable + ": " + str(exc))
            return fallback_values, fallback_status
        preds.append(pred)
        if target_variable == "temperature":
            temp_values.append(pred)
            humidity_values.append(float(fallback_forecast(pd.Series(humidity_values), horizon)[0][step]))
        else:
            humidity_values.append(pred)
            temp_values.append(float(fallback_forecast(pd.Series(temp_values), horizon)[0][step]))
    return np.asarray(preds), status_for(metric_row, "")


# COMMAND ----------

# Main execution

def main():
    create_widgets()
    warn_stale_catalog_once()
    horizon = int_setting(168, "FORECAST_HORIZON_HOURS", "forecast_horizon_hours")
    window_days = int_setting(7, "CALIBRATION_WINDOW_DAYS")
    generated_at = datetime.now(timezone.utc)
    print("Using Databricks namespace: " + setting("DATABRICKS_CATALOG") + "." + setting("DATABRICKS_SCHEMA"))
    history = load_history()
    devices = load_devices()
    metrics_pdf = load_metrics()
    print_forecast_plan(devices, metrics_pdf)
    biases = recent_biases(window_days)
    model_cache = {}
    forecast_rows = []
    calibration_rows = []

    for _, device in devices.iterrows():
        location_id = str(device["location_id"])
        device_id = str(device["device_id"])
        province_id = device.get("province_id")
        location_history = history[history["location_id"].astype(str) == location_id].copy()
        if location_history.empty:
            print("WARNING: no history for location " + location_id + "; skipping forecast.")
            continue
        device_history = location_history[location_history["device_id"].astype(str) == device_id]
        if device_history.empty:
            device_history = location_history
        device_history = device_history.sort_values("hour_ts")
        start_ts = pd.to_datetime(generated_at).floor("h") + pd.Timedelta(hours=1)

        for metric_type in ["temperature", "humidity"]:
            metric_row = select_best_model(metrics_pdf, metric_type, location_id)
            model = load_mlflow_model(metric_row, model_cache, warn_on_failure=False)
            if model is None and metric_row and metric_row.get("model_type") == "spark_mllib":
                model = fit_location_spark_model(metric_row, location_history, metric_type, location_id, model_cache)
            elif model is None and metric_row and metric_row.get("model_type") == "deep_learning":
                print(
                    "WARNING: could not load deep learning model for "
                    + str(location_id)
                    + "/"
                    + metric_type
                    + "; using fallback forecast."
                )
            base_predictions, status = model_forecast(model, metric_row, device_history, metric_type, horizon, location_id, province_id, device_id, start_ts)

            bias_match = biases[
                (biases["device_id"].astype(str) == device_id)
                & (biases["location_id"].astype(str) == location_id)
                & (biases["metric_type"] == metric_type)
            ]
            bias_value = float(bias_match["bias_value"].iloc[0]) if not bias_match.empty else 0.0
            sample_count = int(bias_match["sample_count"].iloc[0]) if not bias_match.empty else 0
            calibration_rows.append(
                {
                    "device_id": device_id,
                    "location_id": location_id,
                    "metric_type": metric_type,
                    "bias_value": bias_value,
                    "sample_count": sample_count,
                    "window_days": window_days,
                    "updated_at": generated_at,
                }
            )
            calibrated = base_predictions + bias_value
            for step, final_value in enumerate(calibrated, start=1):
                base_value = base_predictions[step - 1]
                forecast_rows.append(
                    {
                        "device_id": device_id,
                        "location_id": location_id,
                        "province_id": province_id,
                        "forecast_timestamp": start_ts + pd.Timedelta(hours=step - 1),
                        "metric_type": metric_type,
                        "forecast_value": float(final_value) if np.isfinite(final_value) else None,
                        "base_forecast_value": float(base_value) if np.isfinite(base_value) else None,
                        "bias_value": bias_value,
                        "calibrated_forecast_value": float(final_value) if np.isfinite(final_value) else None,
                        "horizon_step": step,
                        "horizon_days": 7,
                        "model_name": metric_row.get("model_name") if metric_row else "fallback",
                        "model_type": metric_row.get("model_type") if metric_row else "baseline",
                        "training_mode": metric_row.get("training_mode") if metric_row else "fallback",
                        "model_scope": model_scope_for(metric_row, location_id),
                        "model_status": status,
                        "model_uri": metric_row.get("model_uri") if metric_row else None,
                        "mlflow_run_id": metric_row.get("mlflow_run_id") if metric_row else None,
                        "generated_at": generated_at,
                    }
                )

    if not forecast_rows:
        raise RuntimeError("No forecast rows were produced.")

    forecast_df = spark.createDataFrame(pd.DataFrame(forecast_rows))  # type: ignore[name-defined]
    forecast_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("metric_type").saveAsTable(fq_table("gold_forecast_result"))

    calibration_df = spark.createDataFrame(pd.DataFrame(calibration_rows))  # type: ignore[name-defined]
    calibration_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("metric_type").saveAsTable(fq_table("device_calibration_profile"))
    print("Wrote " + str(len(forecast_rows)) + " forecast rows and " + str(len(calibration_rows)) + " calibration rows.")


# COMMAND ----------

main()