# Databricks notebook source
# MAGIC %pip install --no-cache-dir "numpy==1.26.4" "pandas==2.2.3" "protobuf>=3.20.3,<5" meteostat==1.6.8 "tensorflow==2.17.1"

# COMMAND ----------

dbutils.library.restartPython()  # type: ignore[name-defined]

# COMMAND ----------

import numpy as _numpy_version_check
import pandas as _pandas_version_check

print("NumPy version after restart:", _numpy_version_check.__version__)
print("Pandas version after restart:", _pandas_version_check.__version__)
if int(str(_numpy_version_check.__version__).split(".", 1)[0]) >= 2:
    raise RuntimeError(
        "08 requires numpy==1.26.4 for TensorFlow. Run the first %pip cell, then run dbutils.library.restartPython(), "
        "then rerun from this version-check cell."
    )
if int(str(_pandas_version_check.__version__).split(".", 1)[0]) >= 3:
    raise RuntimeError(
        "08 requires pandas<3 for PySpark pandas conversion. Run the first %pip cell, then run dbutils.library.restartPython(), "
        "then rerun from this version-check cell."
    )

# COMMAND ----------

"""Train deep-learning models on the full gold_training_dataset.

This is the Unity Catalog training flow:
  - reads dtdm.metrics_app_streaming.gold_training_dataset
  - uses a time-based holdout to evaluate Keras sequence models
  - writes canonical rows to gold_model_metrics for 09_select_best_models.py and 10_generate_7day_forecast.py

No sklearn, no hive_metastore, no catalog/schema/table creation.
"""

import math
import os
import time
import gc
import json
import logging
import tempfile
import traceback
import warnings
from datetime import datetime, timezone
from pathlib import Path

import mlflow
import numpy as np
import pandas as pd
import pyspark
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import Imputer, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor, GBTRegressor, GeneralizedLinearRegression, LinearRegression, RandomForestRegressor
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

try:
    from pyspark.ml.regression import FMRegressor
except Exception:
    FMRegressor = None

os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "2")
logging.getLogger("absl").setLevel(logging.ERROR)
logging.getLogger("tensorflow").setLevel(logging.ERROR)
warnings.filterwarnings("ignore", message=".*HDF5 file.*legacy.*")


# COMMAND ----------

# Configuration

DEFAULTS = {
    "DATABRICKS_CATALOG": "dtdm",
    "DATABRICKS_SCHEMA": "metrics_app_streaming",
    "MLFLOW_EXPERIMENT_NAME": "/Shared/metrics_app_streaming_deeplearning",
    "TRAIN_FAMILY": "deep_learning",
    "TRAIN_TARGETS": "temperature,humidity",
    "TRAIN_SCOPE": "global_then_location_best",
    "LOCATION_ENABLE_DEEP_LEARNING": "true",
    "LOCATION_ID": "",
    "LOCATION_MIN_ROWS": "5000",
    "LOCATION_MAX_ROWS": "50000",
    "LOCATION_TOP_N": "0",
    "LOCATION_DL_MAX_SEQUENCES": "8000",
    "LOCATION_DL_MIN_SEQUENCES": "80",
    "LOCATION_DL_EPOCHS": "3",
    "FORECAST_HORIZON_HOURS": "168",
    "INPUT_WINDOW_HOURS": "168",
    "HOLDOUT_RATIO": "0.2",
    "ENABLE_LINEAR_REGRESSION": "true",
    "ENABLE_GENERALIZED_LINEAR": "true",
    "ENABLE_DECISION_TREE": "true",
    "ENABLE_RANDOM_FOREST": "true",
    "ENABLE_GBT": "true",
    "ENABLE_FM_REGRESSOR": "true",
    "ENABLE_DEEP_LEARNING": "true",
    "DECISION_TREE_MAX_DEPTH": "10",
    "RANDOM_FOREST_TREES": "120",
    "RANDOM_FOREST_MAX_DEPTH": "12",
    "GBT_TREES": "120",
    "GBT_MAX_DEPTH": "6",
    "FM_MAX_ITER": "80",
    "DL_INPUT_WINDOW_HOURS": "168",
    "DL_MAX_ROWS_PER_SERIES": "0",
    "DL_MAX_SEQUENCES": "120000",
    "DL_MAX_DRIVER_ARRAY_MB": "2048",
    "DL_MIN_SEQUENCES": "80",
    "DL_EPOCHS": "10",
    "DL_BATCH_SIZE": "128",
    "DL_FINAL_REFIT_EPOCHS": "0",
}

NUMERIC_FEATURES = [
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
CATEGORICAL_FEATURES = ["location_id", "province_id", "device_id"]
TARGETS = [
    ("temperature", "temp_target_168"),
    ("humidity", "humidity_target_168"),
]
SEQUENCE_COLS = ["temperature", "humidity", "meteostat_temperature", "meteostat_humidity"]
SPARK_MODEL_PIP_REQUIREMENTS = ["mlflow", "pyspark==" + pyspark.__version__]
KERAS_MODEL_PIP_REQUIREMENTS = ["mlflow", "tensorflow==2.17.1", "protobuf>=3.20.3,<5", "numpy==1.26.4", "pandas==2.2.3"]
LIGHTWEIGHT_DL_WIDGETS = {
    "DL_INPUT_WINDOW_HOURS",
    "DL_MAX_ROWS_PER_SERIES",
    "DL_MAX_SEQUENCES",
    "DL_MAX_DRIVER_ARRAY_MB",
    "DL_MIN_SEQUENCES",
    "DL_EPOCHS",
    "DL_BATCH_SIZE",
    "DL_FINAL_REFIT_EPOCHS",
    "LOCATION_DL_MAX_SEQUENCES",
    "LOCATION_DL_MIN_SEQUENCES",
    "LOCATION_DL_EPOCHS",
}
LOCATION_REFIT_WIDGETS = {
    "TRAIN_SCOPE",
    "LOCATION_ENABLE_DEEP_LEARNING",
    "LOCATION_MIN_ROWS",
    "LOCATION_MAX_ROWS",
    "LOCATION_TOP_N",
}


def normalize_train_family(value):
    family = str(value or "spark_mllib").strip().lower().replace("-", "_")
    aliases = {
        "spark": "spark_mllib",
        "sparkml": "spark_mllib",
        "spark_ml": "spark_mllib",
        "ml": "spark_mllib",
        "machine_learning": "spark_mllib",
        "may_hoc": "spark_mllib",
        "deep": "deep_learning",
        "dl": "deep_learning",
        "deep_learning_light": "deep_learning",
        "hoc_sau": "deep_learning",
        "all": "both",
    }
    family = aliases.get(family, family)
    if family not in ("spark_mllib", "deep_learning", "both"):
        raise ValueError("TRAIN_FAMILY must be spark_mllib, deep_learning, or both.")
    return family


def selected_targets():
    raw = setting("TRAIN_TARGETS")
    requested = [item.strip().lower() for item in str(raw or "").split(",") if item.strip()]
    if not requested or "both" in requested or "all" in requested:
        return TARGETS
    aliases = {
        "temp": "temperature",
        "temperature": "temperature",
        "hum": "humidity",
        "humidity": "humidity",
    }
    selected_names = {aliases.get(item, item) for item in requested}
    selected = [target for target in TARGETS if target[0] in selected_names]
    if not selected:
        raise ValueError("TRAIN_TARGETS must include temperature, humidity, both, or all.")
    return selected


def train_scope():
    scope = str(setting("TRAIN_SCOPE") or "global").strip().lower().replace("-", "_")
    aliases = {
        "global_location_best": "global_then_location_best",
        "location_best": "global_then_location_best",
        "per_location": "global_then_location_best",
    }
    scope = aliases.get(scope, scope)
    if scope not in ("global", "global_then_location_best"):
        raise ValueError("TRAIN_SCOPE must be global or global_then_location_best.")
    return scope


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
    return os.getenv(name) or widget_value(name) or DEFAULTS.get(name, "")


def bool_setting(name):
    return str(setting(name)).strip().lower() in ("1", "true", "yes", "on")


def int_setting(name, default_value):
    try:
        return int(setting(name))
    except Exception:
        return int(default_value)


def float_setting(name, default_value):
    try:
        return float(setting(name))
    except Exception:
        return float(default_value)


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def create_widgets():
    try:
        for name, default in DEFAULTS.items():
            if name in LIGHTWEIGHT_DL_WIDGETS or name in LOCATION_REFIT_WIDGETS:
                try:
                    dbutils.widgets.remove(name)  # type: ignore[name-defined]
                except Exception:
                    pass
            else:
                try:
                    dbutils.widgets.get(name)  # type: ignore[name-defined]
                    continue
                except Exception:
                    pass
            dbutils.widgets.text(name, os.getenv(name, default))  # type: ignore[name-defined]
    except Exception:
        pass


def fq_table(name):
    return setting("DATABRICKS_CATALOG") + "." + setting("DATABRICKS_SCHEMA") + "." + name


def table_exists(table_name):
    name = table_name.split(".")[-1]
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


def sql_list(values):
    cleaned = [str(value).strip() for value in values if value is not None and str(value).strip()]
    if not cleaned:
        return "''"
    return ", ".join("'" + value.replace("'", "''") + "'" for value in cleaned)


# COMMAND ----------

# Metrics and MLflow artifact helpers

def metric_values(predictions, label_col):
    evaluators = {
        "rmse": RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="rmse"),
        "mae": RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="mae"),
        "mse": RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="mse"),
        "r2": RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="r2"),
    }
    values = {name: float(evaluator.evaluate(predictions)) for name, evaluator in evaluators.items()}
    values["rse"] = math.sqrt(values["mse"]) if values["mse"] is not None else None
    return values


def quality_label(metrics):
    r2 = metrics.get("r2") or 0.0
    rmse = metrics.get("rmse") or float("inf")
    if r2 >= 0.85:
        return "excellent", 0.95
    if r2 >= 0.65:
        return "good", 0.8
    if r2 >= 0.35 or rmse < float("inf"):
        return "usable", 0.6
    return "weak", 0.35


def log_run_summary_artifact(summary):
    with tempfile.TemporaryDirectory() as tmp_dir:
        artifact_path = Path(tmp_dir) / "run_summary.json"
        artifact_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        mlflow.log_artifact(str(artifact_path), artifact_path="summary")


# COMMAND ----------

# Training data preparation

def load_training_data():
    source_table = fq_table("gold_training_dataset")
    metrics_table = fq_table("gold_model_metrics")
    if not table_exists(source_table):
        raise RuntimeError(source_table + " does not exist. Run 06_gold_feature_engineering.py first.")
    if not table_exists(metrics_table):
        raise RuntimeError(metrics_table + " does not exist. Run 01_create_catalog_schema_tables.sql first.")

    sdf = spark.table(source_table)  # type: ignore[name-defined]
    if "province_id" not in sdf.columns and table_exists(fq_table("dim_location")):
        locations = spark.table(fq_table("dim_location")).select("location_id", "province_id").dropDuplicates(["location_id"])  # type: ignore[name-defined]
        sdf = sdf.join(locations, "location_id", "left")
    if "province_id" not in sdf.columns:
        sdf = sdf.withColumn("province_id", F.lit("unknown"))
    if "device_id" not in sdf.columns:
        sdf = sdf.withColumn("device_id", F.lit("meteostat_only"))

    for col_name in CATEGORICAL_FEATURES:
        sdf = sdf.withColumn(col_name, F.coalesce(F.col(col_name).cast("string"), F.lit("unknown")))
    for col_name in NUMERIC_FEATURES + [target for _, target in TARGETS]:
        if col_name in sdf.columns:
            sdf = sdf.withColumn(col_name, F.col(col_name).cast("double"))

    return sdf.where(F.col("hour_ts").isNotNull()).orderBy("hour_ts")


def split_by_time(sdf, target_col):
    usable = sdf.where(F.col(target_col).isNotNull())
    for feature in NUMERIC_FEATURES:
        usable = usable.where(F.col(feature).isNotNull())
    count = usable.count()
    if count < 100:
        raise RuntimeError("Not enough usable rows for " + target_col + ": " + str(count))

    holdout_ratio = max(0.05, min(0.5, float_setting("HOLDOUT_RATIO", 0.2)))
    split_position = max(1, int(count * (1.0 - holdout_ratio)))
    split_window = Window.orderBy("hour_ts")
    split_row = (
        usable.select("hour_ts")
        .withColumn("__rank", F.row_number().over(split_window))
        .where(F.col("__rank") == split_position)
        .select("hour_ts")
        .first()
    )
    split_ts = split_row["hour_ts"]
    train_df = usable.where(F.col("hour_ts") < F.lit(split_ts))
    test_df = usable.where(F.col("hour_ts") >= F.lit(split_ts))
    if train_df.count() == 0 or test_df.count() == 0:
        raise RuntimeError("Could not create train/test split for " + target_col)
    return usable, train_df, test_df


# COMMAND ----------

# Spark MLlib model definitions

def preprocess_stages():
    index_cols = []
    stages = []
    for col_name in CATEGORICAL_FEATURES:
        index_col = col_name + "_idx"
        stages.append(StringIndexer(inputCol=col_name, outputCol=index_col, handleInvalid="keep"))
        index_cols.append(index_col)

    imputed_cols = [col_name + "_imputed" for col_name in NUMERIC_FEATURES]
    stages.extend(
        [
            Imputer(inputCols=NUMERIC_FEATURES, outputCols=imputed_cols, strategy="median"),
            VectorAssembler(inputCols=imputed_cols + index_cols, outputCol="raw_features", handleInvalid="keep"),
            StandardScaler(inputCol="raw_features", outputCol="features", withMean=False, withStd=True),
        ]
    )
    return stages


def model_specs(label_col):
    specs = []
    if bool_setting("ENABLE_LINEAR_REGRESSION"):
        specs.append(
            (
                "Spark Linear Regression",
                LinearRegression(featuresCol="features", labelCol=label_col, predictionCol="prediction", maxIter=80, regParam=0.01),
            )
        )
    if bool_setting("ENABLE_GENERALIZED_LINEAR"):
        specs.append(
            (
                "Spark Generalized Linear Regression",
                GeneralizedLinearRegression(
                    featuresCol="features",
                    labelCol=label_col,
                    predictionCol="prediction",
                    family="gaussian",
                    link="identity",
                    maxIter=80,
                    regParam=0.01,
                ),
            )
        )
    if bool_setting("ENABLE_DECISION_TREE"):
        specs.append(
            (
                "Spark Decision Tree",
                DecisionTreeRegressor(
                    featuresCol="features",
                    labelCol=label_col,
                    predictionCol="prediction",
                    maxDepth=int_setting("DECISION_TREE_MAX_DEPTH", 10),
                    minInstancesPerNode=2,
                    seed=42,
                ),
            )
        )
    if bool_setting("ENABLE_RANDOM_FOREST"):
        specs.append(
            (
                "Spark Random Forest",
                RandomForestRegressor(
                    featuresCol="features",
                    labelCol=label_col,
                    predictionCol="prediction",
                    numTrees=int_setting("RANDOM_FOREST_TREES", 120),
                    maxDepth=int_setting("RANDOM_FOREST_MAX_DEPTH", 12),
                    minInstancesPerNode=2,
                    seed=42,
                ),
            )
        )
    if bool_setting("ENABLE_GBT"):
        specs.append(
            (
                "Spark GBT",
                GBTRegressor(
                    featuresCol="features",
                    labelCol=label_col,
                    predictionCol="prediction",
                    maxIter=int_setting("GBT_TREES", 120),
                    maxDepth=int_setting("GBT_MAX_DEPTH", 6),
                    stepSize=0.05,
                    seed=42,
                ),
            )
        )
    if bool_setting("ENABLE_FM_REGRESSOR"):
        if FMRegressor is None:
            specs.append(
                (
                    "Spark Linear Regression ElasticNet",
                    LinearRegression(
                        featuresCol="features",
                        labelCol=label_col,
                        predictionCol="prediction",
                        maxIter=80,
                        regParam=0.05,
                        elasticNetParam=0.5,
                    ),
                )
            )
        else:
            specs.append(
                (
                    "Spark FM Regressor",
                    FMRegressor(
                        featuresCol="features",
                        labelCol=label_col,
                        predictionCol="prediction",
                        maxIter=int_setting("FM_MAX_ITER", 80),
                        stepSize=0.01,
                        seed=42,
                    ),
                )
            )
    if not specs:
        raise RuntimeError("No SparkML models are enabled.")
    return specs


# COMMAND ----------

# Deep learning model definitions

def import_tensorflow():
    try:
        numpy_major = int(str(np.__version__).split(".", 1)[0])
        if numpy_major >= 2:
            raise RuntimeError(
                "TensorFlow on this Databricks runtime requires numpy==1.26.4, but current numpy is "
                + str(np.__version__)
                + ". Run the first %pip cell in 08_train_deep_learning_light.py, then run dbutils.library.restartPython(), then rerun from the version-check cell."
            )
        import tensorflow as tf
        from tensorflow import keras
        from tensorflow.keras import layers

        try:
            from absl import logging as absl_logging

            absl_logging.set_verbosity(absl_logging.ERROR)
        except Exception:
            pass
        try:
            tf.get_logger().setLevel("ERROR")
        except Exception:
            pass
        try:
            mlflow.keras.autolog(disable=True)
        except Exception:
            pass
        tf.random.set_seed(42)
        return tf, keras, layers
    except Exception as exc:
        print("WARNING: TensorFlow/Keras unavailable; skipping deep learning models: " + str(exc)[:1000])
        return None, None, None


def deep_learning_model_specs(input_window, feature_count, feature_mean=None, feature_variance=None):
    tf, keras, layers = import_tensorflow()
    if keras is None:
        return []

    def normalization_layer():
        if feature_mean is None or feature_variance is None:
            return layers.Normalization(axis=-1)
        return layers.Normalization(axis=-1, mean=feature_mean, variance=feature_variance)

    def lstm_builder():
        model = keras.Sequential(
            [
                layers.Input(shape=(input_window, feature_count)),
                normalization_layer(),
                layers.LSTM(64, dropout=0.15),
                layers.Dense(32, activation="relu"),
                layers.Dense(1),
            ]
        )
        model.compile(optimizer=keras.optimizers.Adam(learning_rate=0.001), loss="mse", metrics=["mae"])
        return model

    def gru_builder():
        model = keras.Sequential(
            [
                layers.Input(shape=(input_window, feature_count)),
                normalization_layer(),
                layers.GRU(64, dropout=0.15),
                layers.Dense(32, activation="relu"),
                layers.Dense(1),
            ]
        )
        model.compile(optimizer=keras.optimizers.Adam(learning_rate=0.001), loss="mse", metrics=["mae"])
        return model

    def cnn_lstm_builder():
        model = keras.Sequential(
            [
                layers.Input(shape=(input_window, feature_count)),
                normalization_layer(),
                layers.Conv1D(32, kernel_size=3, padding="causal", activation="relu"),
                layers.MaxPooling1D(pool_size=2),
                layers.LSTM(64, dropout=0.15),
                layers.Dense(32, activation="relu"),
                layers.Dense(1),
            ]
        )
        model.compile(optimizer=keras.optimizers.Adam(learning_rate=0.001), loss="mse", metrics=["mae"])
        return model

    return [
        ("Keras LSTM", lstm_builder),
        ("Keras GRU", gru_builder),
        ("Keras CNN-LSTM", cnn_lstm_builder),
    ]


def make_deep_learning_arrays(pdf, target_col, input_window, max_sequences, max_driver_array_mb):
    required = ["hour_ts", "location_id", "device_id"] + SEQUENCE_COLS + [target_col]
    frame = pdf[required].dropna().copy()
    if frame.empty:
        return None, None, None
    frame["hour_ts"] = pd.to_datetime(frame["hour_ts"])
    frame = frame.sort_values(["location_id", "device_id", "hour_ts"])

    windows = []
    labels = []
    stamps = []
    group_sizes = frame.groupby(["location_id", "device_id"], sort=False).size().to_numpy()
    possible = int(sum(max(0, int(size) - int(input_window)) for size in group_sizes))
    requested_limit = int(max_sequences or 0)
    sequence_limit = min(possible, requested_limit) if requested_limit > 0 else possible
    if sequence_limit <= 0:
        return None, None, None
    use_sequence_limit = requested_limit > 0
    estimated_array_mb = (float(sequence_limit) * float(input_window) * float(len(SEQUENCE_COLS)) * 4.0) / (1024.0 * 1024.0)
    if max_driver_array_mb > 0 and estimated_array_mb > float(max_driver_array_mb):
        raise RuntimeError(
            "Deep learning arrays would need about "
            + str(round(estimated_array_mb, 1))
            + " MB before overhead, above DL_MAX_DRIVER_ARRAY_MB="
            + str(max_driver_array_mb)
            + ". Increase driver memory or set DL_MAX_SEQUENCES to a lower value."
        )
    stride = max(1, possible // sequence_limit) if use_sequence_limit else 1

    for _, group in frame.groupby(["location_id", "device_id"], sort=False):
        values = group[SEQUENCE_COLS].to_numpy(dtype="float32")
        targets = group[target_col].to_numpy(dtype="float32")
        times = group["hour_ts"].to_numpy()
        if len(group) <= input_window:
            continue
        for end_idx in range(input_window, len(group), stride):
            label = targets[end_idx]
            if not np.isfinite(label):
                continue
            window = values[end_idx - input_window : end_idx]
            if not np.isfinite(window).all():
                continue
            windows.append(window)
            labels.append(label)
            stamps.append(times[end_idx])
            if sequence_limit > 0 and len(windows) >= sequence_limit:
                break
        if sequence_limit > 0 and len(windows) >= sequence_limit:
            break

    if not windows:
        return None, None, None
    order = np.argsort(np.asarray(stamps, dtype="datetime64[ns]"))
    x = np.asarray(windows, dtype="float32")[order]
    y = np.asarray(labels, dtype="float32")[order]
    return x, y, np.asarray(stamps, dtype="datetime64[ns]")[order]


def deep_learning_metrics(y_true, y_pred):
    y_true = np.asarray(y_true, dtype="float64").reshape(-1)
    y_pred = np.asarray(y_pred, dtype="float64").reshape(-1)
    errors = y_true - y_pred
    mse = float(np.mean(errors ** 2))
    rmse = float(np.sqrt(mse))
    mae = float(np.mean(np.abs(errors)))
    denom = float(np.sum((y_true - np.mean(y_true)) ** 2))
    r2 = float(1.0 - (np.sum(errors ** 2) / denom)) if denom > 0 else 0.0
    nonzero = np.abs(y_true) > 1e-9
    mape = float(np.mean(np.abs(errors[nonzero] / y_true[nonzero])) * 100.0) if nonzero.any() else None
    return {"rmse": rmse, "mae": mae, "mse": mse, "r2": r2, "rse": rmse, "mape": mape}


# COMMAND ----------

# Deep learning training

def train_deep_learning_target(sdf, target_variable, target_col, full_count, train_eval_count, test_count, location_count):
    if not bool_setting("ENABLE_DEEP_LEARNING"):
        return []

    input_window = int_setting("DL_INPUT_WINDOW_HOURS", 168)
    max_rows_per_series = int_setting("DL_MAX_ROWS_PER_SERIES", 0)
    max_sequences = int_setting("DL_MAX_SEQUENCES", 120000)
    max_driver_array_mb = int_setting("DL_MAX_DRIVER_ARRAY_MB", 2048)
    min_sequences = int_setting("DL_MIN_SEQUENCES", 80)
    epochs = int_setting("DL_EPOCHS", 10)
    batch_size = int_setting("DL_BATCH_SIZE", 128)
    final_refit_epochs = int_setting("DL_FINAL_REFIT_EPOCHS", 0)
    select_cols = ["hour_ts", "location_id", "device_id"] + SEQUENCE_COLS + [target_col]

    pdf_row_count = 0
    try:
        limited = sdf.select(*select_cols)
        for col_name in SEQUENCE_COLS + [target_col]:
            limited = limited.where(F.col(col_name).isNotNull())
        if max_rows_per_series > 0:
            ranked = limited.withColumn(
                "_dl_recent_rank",
                F.row_number().over(Window.partitionBy("location_id", "device_id").orderBy(F.col("hour_ts").desc())),
            )
            limited = ranked.where(F.col("_dl_recent_rank") <= F.lit(max_rows_per_series)).drop("_dl_recent_rank")
        pdf = limited.toPandas()
        pdf_row_count = len(pdf)
        x, y, _ = make_deep_learning_arrays(pdf, target_col, input_window, max_sequences, max_driver_array_mb)
    except Exception as exc:
        print("WARNING: could not prepare deep learning sequences for " + target_variable + ": " + str(exc)[:1000])
        print(traceback.format_exc()[:4000])
        return []

    if x is None or len(x) < min_sequences:
        sequence_count = 0 if x is None else len(x)
        print(
            "WARNING: not enough deep learning sequences for "
            + target_variable
            + ": pandas_rows="
            + str(pdf_row_count)
            + ", sequences="
            + str(sequence_count)
            + ", min_sequences="
            + str(min_sequences)
            + ", input_window="
            + str(input_window)
            + ", max_rows_per_series="
            + str(max_rows_per_series)
            + ", max_driver_array_mb="
            + str(max_driver_array_mb)
        )
        return []

    split_idx = max(1, int(len(x) * 0.8))
    if split_idx >= len(x):
        split_idx = len(x) - 1
    x_train, x_test = x[:split_idx], x[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]

    feature_mean = x_train.mean(axis=(0, 1)).astype("float32")
    feature_variance = np.maximum(x_train.var(axis=(0, 1)).astype("float32"), 1e-6)
    x_all = x

    candidates = []
    failures = []
    specs = deep_learning_model_specs(input_window, len(SEQUENCE_COLS), feature_mean, feature_variance)
    if not specs:
        raise RuntimeError("No deep learning model specs available for " + target_variable + ". TensorFlow/Keras may be unavailable.")
    print(
        "Deep learning sequence data for "
        + target_variable
        + ": pandas_rows="
        + str(pdf_row_count)
        + ", sequences="
        + str(len(x))
        + ", train_sequences="
        + str(len(x_train))
        + ", holdout_sequences="
        + str(len(x_test))
    )
    print("Deep learning models to train for " + target_variable + ": " + ", ".join(name for name, _ in specs))

    for model_name, builder in specs:
        started = time.time()
        try:
            tf, keras, _ = import_tensorflow()
            if keras is None:
                raise RuntimeError("TensorFlow/Keras unavailable during model training.")
            keras.backend.clear_session()
            callbacks = [keras.callbacks.EarlyStopping(monitor="val_loss", patience=1, restore_best_weights=True)]
            model = builder()
            model.fit(
                x_train,
                y_train,
                validation_data=(x_test, y_test),
                epochs=epochs,
                batch_size=batch_size,
                verbose=0,
                callbacks=callbacks,
            )
            predictions = model.predict(x_test, batch_size=batch_size, verbose=0).reshape(-1)
            metrics = deep_learning_metrics(y_test, predictions)

            if final_refit_epochs > 0:
                model.fit(x_all, y, epochs=final_refit_epochs, batch_size=batch_size, verbose=0)
            training_time_seconds = float(time.time() - started)
            print(
                target_variable
                + " / "
                + model_name
                + " holdout RMSE="
                + str(round(metrics["rmse"], 4))
                + ", MAE="
                + str(round(metrics["mae"], 4))
                + ", R2="
                + str(round(metrics["r2"], 4))
            )

            with mlflow.start_run(run_name="deeplearning_full_" + target_variable + "_" + model_name.replace(" ", "_")) as run:
                params = {
                    "model_name": model_name,
                    "sequence_columns": ",".join(SEQUENCE_COLS),
                    "full_training_points": full_count,
                    "max_rows_per_series": max_rows_per_series,
                    "sequence_points": len(x),
                    "eval_train_points": len(x_train),
                    "holdout_points": len(x_test),
                    "forecast_horizon_hours": int_setting("FORECAST_HORIZON_HOURS", 168),
                    "input_window_hours": input_window,
                    "epochs": epochs,
                    "final_refit_epochs": final_refit_epochs,
                    "batch_size": batch_size,
                }
                mlflow.set_tags(
                    {
                        "project": "weather_forecast_esp32_meteostat",
                        "training_engine": "keras",
                        "training_mode": "global",
                        "model_scope": "multi_location",
                        "target_variable": target_variable,
                        "trained_on_full_data": "true",
                    }
                )
                mlflow.log_params(params)
                mlflow.log_metrics(
                    {
                        "rmse": metrics["rmse"],
                        "mae": metrics["mae"],
                        "mse": metrics["mse"],
                        "r2": metrics["r2"],
                        "rse": metrics["rse"],
                        "training_time_seconds": training_time_seconds,
                    }
                )
                mlflow.keras.log_model(
                    model,
                    name="model",
                    signature=mlflow.models.infer_signature(x_test[:1], model.predict(x_test[:1], verbose=0)),
                    pip_requirements=KERAS_MODEL_PIP_REQUIREMENTS,
                )
                model_uri = "runs:/" + run.info.run_id + "/model"
                mlflow_run_id = run.info.run_id
                log_run_summary_artifact(
                    {
                        "run_id": mlflow_run_id,
                        "model_uri": model_uri,
                        "model_name": model_name,
                        "model_type": "deep_learning",
                        "target_variable": target_variable,
                        "metrics": metrics,
                        "params": params,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    }
                )

            candidates.append(
                {
                    "model_name": model_name,
                    "model_type": "deep_learning",
                    "metrics": metrics,
                    "seconds": training_time_seconds,
                    "model_uri": model_uri,
                    "mlflow_run_id": mlflow_run_id,
                    "training_points": len(x),
                    "test_points": len(x_test),
                    "input_window_hours": input_window,
                    "location_count": int(location_count),
                }
            )
            keras.backend.clear_session()
            gc.collect()
        except Exception as exc:
            message = model_name + " failed for " + target_variable + ": " + str(exc)[:1000]
            failures.append(message)
            print("WARNING: " + message)
            print(traceback.format_exc()[:4000])
            try:
                if keras is not None:
                    keras.backend.clear_session()
            except Exception:
                pass
            gc.collect()

    if not candidates and failures:
        raise RuntimeError("No deep learning candidates succeeded for " + target_variable + ". Failures: " + " | ".join(failures))

    return candidates


# COMMAND ----------

# Per-location deep learning refit

def location_candidates(sdf, target_col):
    min_rows = int_setting("LOCATION_MIN_ROWS", 5000)
    top_n = int_setting("LOCATION_TOP_N", 0)
    requested_location = str(setting("LOCATION_ID") or "").strip()

    usable = sdf.where(F.col(target_col).isNotNull())
    for col_name in SEQUENCE_COLS:
        usable = usable.where(F.col(col_name).isNotNull())
    grouped = usable.groupBy("location_id").agg(
        F.first("province_id", ignorenulls=True).alias("province_id"),
        F.count(F.lit(1)).alias("row_count"),
    )
    if requested_location:
        grouped = grouped.where(F.col("location_id") == F.lit(requested_location))
    grouped = grouped.where(F.col("row_count") >= F.lit(min_rows)).orderBy(F.col("row_count").desc(), F.col("location_id").asc())
    if top_n > 0:
        grouped = grouped.limit(top_n)
    rows = grouped.collect()
    return [
        {
            "location_id": row["location_id"],
            "province_id": row["province_id"],
            "row_count": int(row["row_count"] or 0),
        }
        for row in rows
    ]


def prepare_location_arrays(sdf, location_id, target_col):
    input_window = int_setting("DL_INPUT_WINDOW_HOURS", 168)
    max_rows = int_setting("LOCATION_MAX_ROWS", 50000)
    max_sequences = int_setting("LOCATION_DL_MAX_SEQUENCES", 8000)
    max_driver_array_mb = int_setting("DL_MAX_DRIVER_ARRAY_MB", 2048)
    select_cols = ["hour_ts", "location_id", "device_id"] + SEQUENCE_COLS + [target_col]

    limited = sdf.where(F.col("location_id") == F.lit(location_id)).select(*select_cols)
    for col_name in SEQUENCE_COLS + [target_col]:
        limited = limited.where(F.col(col_name).isNotNull())
    if max_rows > 0:
        limited = (
            limited.withColumn(
                "_location_recent_rank",
                F.row_number().over(Window.partitionBy("location_id").orderBy(F.col("hour_ts").desc())),
            )
            .where(F.col("_location_recent_rank") <= F.lit(max_rows))
            .drop("_location_recent_rank")
        )
    pdf = limited.toPandas()
    x, y, _ = make_deep_learning_arrays(pdf, target_col, input_window, max_sequences, max_driver_array_mb)
    return pdf, x, y


def train_location_deep_learning_models(sdf, global_rows, targets_to_train):
    if train_scope() != "global_then_location_best":
        return []
    if not bool_setting("LOCATION_ENABLE_DEEP_LEARNING"):
        print("Location deep learning refit is disabled. Set LOCATION_ENABLE_DEEP_LEARNING=true to enable it.")
        return []

    min_sequences = int_setting("LOCATION_DL_MIN_SEQUENCES", int_setting("DL_MIN_SEQUENCES", 80))
    epochs = int_setting("LOCATION_DL_EPOCHS", 3)
    batch_size = int_setting("DL_BATCH_SIZE", 128)
    input_window = int_setting("DL_INPUT_WINDOW_HOURS", 168)
    location_rows = []

    print(
        "Location deep learning refit enabled: min_sequences="
        + str(min_sequences)
        + ", max_sequences="
        + str(int_setting("LOCATION_DL_MAX_SEQUENCES", 8000))
        + ", epochs="
        + str(epochs)
    )

    best_by_target = {}
    for target_variable, _ in targets_to_train:
        target_rows = [
            row
            for row in global_rows
            if row.get("target_variable") == target_variable
            and row.get("training_mode") == "global"
            and row.get("model_type") == "deep_learning"
            and row.get("is_best")
        ]
        if not target_rows:
            target_rows = [
                row
                for row in global_rows
                if row.get("target_variable") == target_variable
                and row.get("training_mode") == "global"
                and row.get("model_type") == "deep_learning"
            ]
        if target_rows:
            best_by_target[target_variable] = sorted(target_rows, key=lambda row: (row["rmse"], row["mae"], -row["r2"]))[0]

    for target_variable, target_col in targets_to_train:
        best_global = best_by_target.get(target_variable)
        if not best_global:
            print("WARNING: no global deep learning best available for location refit: " + target_variable)
            continue

        locations = location_candidates(sdf, target_col)
        print(
            "Location DL refit for "
            + target_variable
            + ": model="
            + str(best_global["model_name"])
            + ", locations="
            + str(len(locations))
            + ", min_rows="
            + str(int_setting("LOCATION_MIN_ROWS", 5000))
            + ", max_rows="
            + str(int_setting("LOCATION_MAX_ROWS", 50000))
        )

        for location in locations:
            location_id = location["location_id"]
            province_id = location.get("province_id")
            started = time.time()
            keras = None
            try:
                pdf, x, y = prepare_location_arrays(sdf, location_id, target_col)
                sequence_count = 0 if x is None else len(x)
                if x is None or sequence_count < min_sequences:
                    print(
                        "WARNING: not enough location DL sequences for "
                        + str(location_id)
                        + "/"
                        + target_variable
                        + ": pandas_rows="
                        + str(len(pdf))
                        + ", sequences="
                        + str(sequence_count)
                    )
                    continue

                split_idx = max(1, int(len(x) * 0.8))
                if split_idx >= len(x):
                    split_idx = len(x) - 1
                x_train, x_test = x[:split_idx], x[split_idx:]
                y_train, y_test = y[:split_idx], y[split_idx:]
                feature_mean = x_train.mean(axis=(0, 1)).astype("float32")
                feature_variance = np.maximum(x_train.var(axis=(0, 1)).astype("float32"), 1e-6)
                specs = dict(deep_learning_model_specs(input_window, len(SEQUENCE_COLS), feature_mean, feature_variance))
                builder = specs.get(best_global["model_name"])
                if builder is None:
                    print("WARNING: could not rebuild Keras model for location refit: " + str(best_global["model_name"]))
                    continue

                tf, keras, _ = import_tensorflow()
                if keras is None:
                    raise RuntimeError("TensorFlow/Keras unavailable during location refit.")
                keras.backend.clear_session()
                callbacks = [keras.callbacks.EarlyStopping(monitor="val_loss", patience=1, restore_best_weights=True)]
                model = builder()
                model.fit(
                    x_train,
                    y_train,
                    validation_data=(x_test, y_test),
                    epochs=epochs,
                    batch_size=batch_size,
                    verbose=0,
                    callbacks=callbacks,
                )
                predictions = model.predict(x_test, batch_size=batch_size, verbose=0).reshape(-1)
                metrics = deep_learning_metrics(y_test, predictions)
                training_time_seconds = float(time.time() - started)
                print(
                    target_variable
                    + " / "
                    + str(location_id)
                    + " / "
                    + str(best_global["model_name"])
                    + " RMSE="
                    + str(round(metrics["rmse"], 4))
                    + ", MAE="
                    + str(round(metrics["mae"], 4))
                    + ", R2="
                    + str(round(metrics["r2"], 4))
                )

                with mlflow.start_run(
                    run_name="deeplearning_location_" + target_variable + "_" + str(location_id) + "_" + str(best_global["model_name"]).replace(" ", "_")
                ) as run:
                    params = {
                        "model_name": best_global["model_name"],
                        "sequence_columns": ",".join(SEQUENCE_COLS),
                        "location_id": location_id,
                        "province_id": province_id,
                        "source_global_model_name": best_global["model_name"],
                        "source_global_model_uri": best_global.get("model_uri"),
                        "sequence_points": len(x),
                        "eval_train_points": len(x_train),
                        "holdout_points": len(x_test),
                        "location_max_rows": int_setting("LOCATION_MAX_ROWS", 50000),
                        "location_max_sequences": int_setting("LOCATION_DL_MAX_SEQUENCES", 8000),
                        "forecast_horizon_hours": int_setting("FORECAST_HORIZON_HOURS", 168),
                        "input_window_hours": input_window,
                        "epochs": epochs,
                        "batch_size": batch_size,
                    }
                    mlflow.set_tags(
                        {
                            "project": "weather_forecast_esp32_meteostat",
                            "training_engine": "keras",
                            "training_mode": "per_location",
                            "model_scope": "location_refit_best_global_type",
                            "target_variable": target_variable,
                            "location_id": location_id,
                            "trained_on_full_data": "false",
                        }
                    )
                    mlflow.log_params(params)
                    mlflow.log_metrics(
                        {
                            "rmse": metrics["rmse"],
                            "mae": metrics["mae"],
                            "mse": metrics["mse"],
                            "r2": metrics["r2"],
                            "rse": metrics["rse"],
                            "training_time_seconds": training_time_seconds,
                        }
                    )
                    mlflow.keras.log_model(
                        model,
                        name="model",
                        signature=mlflow.models.infer_signature(x_test[:1], model.predict(x_test[:1], verbose=0)),
                        pip_requirements=KERAS_MODEL_PIP_REQUIREMENTS,
                    )
                    model_uri = "runs:/" + run.info.run_id + "/model"
                    mlflow_run_id = run.info.run_id

                label, confidence = quality_label(metrics)
                location_rows.append(
                    {
                        "model_name": best_global["model_name"],
                        "model_type": "deep_learning",
                        "training_mode": "per_location",
                        "model_scope": "location_refit_best_global_type",
                        "location_id": location_id,
                        "province_id": province_id,
                        "location_count": 1,
                        "target_variable": target_variable,
                        "horizon": int_setting("FORECAST_HORIZON_HOURS", 168),
                        "forecast_horizon_hours": int_setting("FORECAST_HORIZON_HOURS", 168),
                        "input_window_hours": input_window,
                        "mae": float(metrics["mae"]),
                        "rmse": float(metrics["rmse"]),
                        "rse": float(metrics["rse"]),
                        "mape": float(metrics["mape"]) if metrics.get("mape") is not None else None,
                        "r2": float(metrics["r2"]),
                        "training_points": int(len(x)),
                        "test_points": int(len(x_test)),
                        "quality_label": label,
                        "confidence_score": float(confidence),
                        "model_uri": model_uri,
                        "mlflow_run_id": mlflow_run_id,
                        "is_best": True,
                        "created_at": utc_now(),
                    }
                )
                keras.backend.clear_session()
                gc.collect()
            except Exception as exc:
                print("WARNING: location DL refit failed for " + str(location_id) + "/" + target_variable + ": " + str(exc)[:1000])
                print(traceback.format_exc()[:4000])
                try:
                    if keras is not None:
                        keras.backend.clear_session()
                except Exception:
                    pass
                gc.collect()

    return location_rows


# COMMAND ----------

# Metrics table schema

def metrics_schema():
    return StructType(
        [
            StructField("model_name", StringType(), True),
            StructField("model_type", StringType(), True),
            StructField("training_mode", StringType(), True),
            StructField("model_scope", StringType(), True),
            StructField("location_id", StringType(), True),
            StructField("province_id", StringType(), True),
            StructField("location_count", IntegerType(), True),
            StructField("target_variable", StringType(), True),
            StructField("horizon", IntegerType(), True),
            StructField("forecast_horizon_hours", IntegerType(), True),
            StructField("input_window_hours", IntegerType(), True),
            StructField("mae", DoubleType(), True),
            StructField("rmse", DoubleType(), True),
            StructField("rse", DoubleType(), True),
            StructField("mape", DoubleType(), True),
            StructField("r2", DoubleType(), True),
            StructField("training_points", IntegerType(), True),
            StructField("test_points", IntegerType(), True),
            StructField("quality_label", StringType(), True),
            StructField("confidence_score", DoubleType(), True),
            StructField("model_uri", StringType(), True),
            StructField("mlflow_run_id", StringType(), True),
            StructField("is_best", BooleanType(), True),
            StructField("created_at", TimestampType(), True),
        ]
    )


# COMMAND ----------

# Train target

def train_target(sdf, target_variable, target_col, train_family):
    full_df, train_df, test_df = split_by_time(sdf, target_col)
    full_count = full_df.count()
    train_eval_count = train_df.count()
    test_count = test_df.count()
    location_count = full_df.select("location_id").distinct().count()
    print(
        "Target "
        + target_variable
        + ": full_rows="
        + str(full_count)
        + ", eval_train_rows="
        + str(train_eval_count)
        + ", holdout_rows="
        + str(test_count)
    )

    candidates = []
    if train_family in ("spark_mllib", "both"):
        for model_name, estimator in model_specs(target_col):
            started = time.time()
            try:
                pipeline = Pipeline(stages=preprocess_stages() + [estimator])
                eval_model = pipeline.fit(train_df)
                predictions = eval_model.transform(test_df)
                metrics = metric_values(predictions, target_col)
                final_model = pipeline.fit(full_df)
                training_time_seconds = float(time.time() - started)
                print(
                    target_variable
                    + " / "
                    + model_name
                    + " holdout RMSE="
                    + str(round(metrics["rmse"], 4))
                    + ", MAE="
                    + str(round(metrics["mae"], 4))
                    + ", R2="
                    + str(round(metrics["r2"], 4))
                )

                with mlflow.start_run(run_name="sparkml_full_" + target_variable + "_" + model_name.replace(" ", "_")) as run:
                    params = {
                        "model_name": model_name,
                        "feature_columns": ",".join(NUMERIC_FEATURES + CATEGORICAL_FEATURES),
                        "full_training_points": full_count,
                        "eval_train_points": train_eval_count,
                        "holdout_points": test_count,
                        "forecast_horizon_hours": int_setting("FORECAST_HORIZON_HOURS", 168),
                        "input_window_hours": int_setting("INPUT_WINDOW_HOURS", 168),
                    }
                    mlflow.set_tags(
                        {
                            "project": "weather_forecast_esp32_meteostat",
                            "training_engine": "sparkml",
                            "training_mode": "global",
                            "model_scope": "multi_location",
                            "target_variable": target_variable,
                            "trained_on_full_data": "true",
                        }
                    )
                    mlflow.log_params(params)
                    mlflow.log_metrics(
                        {
                            "rmse": metrics["rmse"],
                            "mae": metrics["mae"],
                            "mse": metrics["mse"],
                            "r2": metrics["r2"],
                            "rse": metrics["rse"],
                            "training_time_seconds": training_time_seconds,
                        }
                    )
                    mlflow.spark.log_model(
                        final_model,
                        artifact_path="model",
                        pip_requirements=SPARK_MODEL_PIP_REQUIREMENTS,
                    )
                    model_uri = "runs:/" + run.info.run_id + "/model"
                    mlflow_run_id = run.info.run_id
                    log_run_summary_artifact(
                        {
                            "run_id": mlflow_run_id,
                            "model_uri": model_uri,
                            "model_name": model_name,
                            "model_type": "spark_mllib",
                            "target_variable": target_variable,
                            "metrics": metrics,
                            "params": params,
                            "created_at": datetime.now(timezone.utc).isoformat(),
                        }
                    )

                candidates.append(
                    {
                        "model_name": model_name,
                        "model_type": "spark_mllib",
                        "metrics": metrics,
                        "seconds": training_time_seconds,
                        "model_uri": model_uri,
                        "mlflow_run_id": mlflow_run_id,
                        "training_points": full_count,
                        "test_points": test_count,
                        "input_window_hours": int_setting("INPUT_WINDOW_HOURS", 168),
                        "location_count": int(location_count),
                    }
                )
            except Exception as exc:
                print("WARNING: " + model_name + " failed for " + target_variable + ": " + str(exc)[:1000])

    if train_family in ("deep_learning", "both"):
        candidates.extend(train_deep_learning_target(sdf, target_variable, target_col, full_count, train_eval_count, test_count, location_count))

    if not candidates:
        raise RuntimeError(
            "No successful candidates for "
            + target_variable
            + ". Check the WARNING lines above. For deep learning this usually means TensorFlow/Keras failed, MLflow model logging failed, "
            + "or not enough sequences were produced. If sequence count is low, lower DL_INPUT_WINDOW_HOURS or DL_MIN_SEQUENCES, or set DL_MAX_ROWS_PER_SERIES=0."
        )

    best = sorted(candidates, key=lambda row: (row["metrics"]["rmse"], row["metrics"]["mae"], -row["metrics"]["r2"]))[0]
    rows = []
    for candidate in candidates:
        label, confidence = quality_label(candidate["metrics"])
        rows.append(
            {
                "model_name": candidate["model_name"],
                "model_type": candidate.get("model_type", "spark_mllib"),
                "training_mode": "global",
                "model_scope": "multi_location_full_data",
                "location_id": None,
                "province_id": None,
                "location_count": int(candidate.get("location_count", location_count)),
                "target_variable": target_variable,
                "horizon": int_setting("FORECAST_HORIZON_HOURS", 168),
                "forecast_horizon_hours": int_setting("FORECAST_HORIZON_HOURS", 168),
                "input_window_hours": int(candidate.get("input_window_hours", int_setting("INPUT_WINDOW_HOURS", 168))),
                "mae": float(candidate["metrics"]["mae"]),
                "rmse": float(candidate["metrics"]["rmse"]),
                "rse": float(candidate["metrics"]["rse"]),
                "mape": float(candidate["metrics"]["mape"]) if candidate["metrics"].get("mape") is not None else None,
                "r2": float(candidate["metrics"]["r2"]),
                "training_points": int(candidate.get("training_points", full_count)),
                "test_points": int(candidate.get("test_points", test_count)),
                "quality_label": label,
                "confidence_score": float(confidence),
                "model_uri": candidate["model_uri"],
                "mlflow_run_id": candidate["mlflow_run_id"],
                "is_best": candidate["model_name"] == best["model_name"],
                "created_at": utc_now(),
            }
        )
    return rows


# COMMAND ----------

# Metrics writing

def write_model_metrics(result_rows, targets_to_train, train_family, trained_model_types):
    metrics_table = fq_table("gold_model_metrics")
    delete_types_sql = sql_list(trained_model_types)
    delete_targets_sql = sql_list([target_variable for target_variable, _ in targets_to_train])
    spark.sql(  # type: ignore[name-defined]
        "DELETE FROM "
        + metrics_table
        + " WHERE model_type IN ("
        + delete_types_sql
        + ") AND target_variable IN ("
        + delete_targets_sql
        + ") AND training_mode = 'global'"
    )
    per_location_ids = sorted(
        {
            row.get("location_id")
            for row in result_rows
            if row.get("training_mode") == "per_location" and row.get("location_id")
        }
    )
    if per_location_ids:
        spark.sql(  # type: ignore[name-defined]
            "DELETE FROM "
            + metrics_table
            + " WHERE model_type IN ("
            + delete_types_sql
            + ") AND target_variable IN ("
            + delete_targets_sql
            + ") AND training_mode = 'per_location'"
            + " AND location_id IN ("
            + sql_list(per_location_ids)
            + ")"
        )
    metrics_df = spark.createDataFrame(result_rows, schema=metrics_schema())  # type: ignore[name-defined]
    metrics_df.write.format("delta").mode("append").saveAsTable(metrics_table)
    print("Wrote " + str(len(result_rows)) + " " + train_family + " model metrics to " + metrics_table)
    metrics_df.orderBy("target_variable", F.col("is_best").desc(), F.col("rmse").asc()).select(
        "target_variable",
        "model_name",
        "training_mode",
        "location_id",
        "training_points",
        "test_points",
        "rmse",
        "mae",
        "r2",
        "is_best",
        "model_uri",
    ).show(50, truncate=False)


# COMMAND ----------

# Main execution

def main():
    create_widgets()
    train_family = "deep_learning"
    trained_model_types = ["spark_mllib", "deep_learning"] if train_family == "both" else [train_family]
    mlflow.set_experiment(setting("MLFLOW_EXPERIMENT_NAME"))
    sdf = load_training_data()
    source_count = sdf.count()
    print("Loaded full training source rows:", source_count)
    print("Training family:", train_family)
    print("Training scope:", train_scope())
    print(
        "Deep learning config: input_window="
        + str(int_setting("DL_INPUT_WINDOW_HOURS", 168))
        + ", max_rows_per_series="
        + str(int_setting("DL_MAX_ROWS_PER_SERIES", 0))
        + ", max_sequences="
        + str(int_setting("DL_MAX_SEQUENCES", 120000))
        + ", max_driver_array_mb="
        + str(int_setting("DL_MAX_DRIVER_ARRAY_MB", 2048))
        + ", epochs="
        + str(int_setting("DL_EPOCHS", 10))
        + ", batch_size="
        + str(int_setting("DL_BATCH_SIZE", 128))
    )
    print(
        "Location deep learning refit: enabled="
        + str(bool_setting("LOCATION_ENABLE_DEEP_LEARNING")).lower()
        + ", location_id="
        + (str(setting("LOCATION_ID") or "") or "all")
        + ", min_rows="
        + str(int_setting("LOCATION_MIN_ROWS", 5000))
        + ", max_rows="
        + str(int_setting("LOCATION_MAX_ROWS", 50000))
        + ", top_n="
        + str(int_setting("LOCATION_TOP_N", 0))
    )
    if source_count == 0:
        raise RuntimeError("gold_training_dataset is empty.")

    targets_to_train = selected_targets()
    print("Training targets:", ", ".join(target for target, _ in targets_to_train))
    result_rows = []
    for target_variable, target_col in targets_to_train:
        if target_col not in sdf.columns:
            raise RuntimeError("Missing target column: " + target_col)
        result_rows.extend(train_target(sdf, target_variable, target_col, train_family))

    result_rows.extend(train_location_deep_learning_models(sdf, result_rows, targets_to_train))
    write_model_metrics(result_rows, targets_to_train, train_family, trained_model_types)


# COMMAND ----------

main()