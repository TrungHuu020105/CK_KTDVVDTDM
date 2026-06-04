# Databricks notebook source
# MAGIC %pip install numpy pandas "protobuf>=3.20.3,<5" meteostat==1.6.8

# COMMAND ----------

"""Train machine-learning models on the full gold_training_dataset.

This is the Unity Catalog training flow:
  - reads dtdm.metrics_app_streaming.gold_training_dataset
  - uses a time-based holdout to evaluate Spark MLlib models
  - optionally refits each successful model for each target on 100% of usable rows
  - writes canonical rows to gold_model_metrics for 09_select_best_models.py and 10_generate_7day_forecast.py

No sklearn, no hive_metastore, no catalog/schema/table creation.
"""

import math
import os
import time
import gc
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import mlflow
import pyspark
from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor, GBTRegressor, GeneralizedLinearRegression, LinearRegression, RandomForestRegressor
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

try:
    from pyspark.ml.regression import FMRegressor
except Exception:
    FMRegressor = None


# COMMAND ----------

# Configuration

DEFAULTS = {
    "DATABRICKS_CATALOG": "dtdm",
    "DATABRICKS_SCHEMA": "metrics_app_streaming",
    "MLFLOW_EXPERIMENT_NAME": "/Shared/metrics_app_streaming_sparkml_full_data",
    "TRAIN_FAMILY": "spark_mllib",
    "TRAIN_TARGETS": "temperature,humidity",
    "TRAIN_PROFILE": "quick",
    "TRAIN_SCOPE": "global_then_location_best",
    "FINAL_REFIT": "false",
    "LOCATION_ID": "",
    "LOCATION_MIN_ROWS": "5000",
    "LOCATION_MAX_ROWS": "50000",
    "LOCATION_TOP_N": "0",
    "FORECAST_HORIZON_HOURS": "168",
    "INPUT_WINDOW_HOURS": "168",
    "HOLDOUT_RATIO": "0.2",
    "ENABLE_LINEAR_REGRESSION": "true",
    "ENABLE_GENERALIZED_LINEAR": "false",
    "ENABLE_DECISION_TREE": "true",
    "ENABLE_RANDOM_FOREST": "true",
    "ENABLE_GBT": "false",
    "ENABLE_FM_REGRESSOR": "false",
    "ENABLE_DEEP_LEARNING": "false",
    "DECISION_TREE_MAX_DEPTH": "8",
    "RANDOM_FOREST_TREES": "30",
    "RANDOM_FOREST_MAX_DEPTH": "8",
    "GBT_TREES": "120",
    "GBT_MAX_DEPTH": "6",
    "FM_MAX_ITER": "80",
    "DL_INPUT_WINDOW_HOURS": "72",
    "DL_MAX_ROWS_PER_SERIES": "240",
    "DL_MAX_SEQUENCES": "4000",
    "DL_MIN_SEQUENCES": "80",
    "DL_EPOCHS": "3",
    "DL_BATCH_SIZE": "64",
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
KERAS_MODEL_PIP_REQUIREMENTS = ["mlflow", "tensorflow>=2.16,<2.18", "protobuf>=3.20.3,<5", "numpy", "pandas"]
LIGHTWEIGHT_DL_WIDGETS = {
    "DL_INPUT_WINDOW_HOURS",
    "DL_MAX_ROWS_PER_SERIES",
    "DL_MAX_SEQUENCES",
    "DL_MIN_SEQUENCES",
    "DL_EPOCHS",
    "DL_BATCH_SIZE",
    "DL_FINAL_REFIT_EPOCHS",
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


def train_profile():
    profile = str(setting("TRAIN_PROFILE") or "quick").strip().lower()
    if profile not in ("quick", "full"):
        raise ValueError("TRAIN_PROFILE must be quick or full.")
    return profile


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


def quick_int_setting(name, default_value):
    return int_setting(name, default_value)


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
            if name in LIGHTWEIGHT_DL_WIDGETS:
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
    clean = (
        predictions.select(
            F.col(label_col).cast("double").alias("__label"),
            F.col("prediction").cast("double").alias("__prediction"),
        )
        .where(F.col("__label").isNotNull())
        .where(F.col("__prediction").isNotNull())
        .where(~F.isnan(F.col("__label")))
        .where(~F.isnan(F.col("__prediction")))
        .withColumn("__error", F.col("__label") - F.col("__prediction"))
    )
    stats = clean.agg(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.abs(F.col("__error"))).alias("sum_abs_error"),
        F.sum(F.col("__error") * F.col("__error")).alias("sum_squared_error"),
        F.sum(F.col("__label")).alias("sum_label"),
        F.sum(F.col("__label") * F.col("__label")).alias("sum_label_squared"),
    ).first()
    n = int(stats["n"] or 0)
    if n == 0:
        raise RuntimeError("No valid predictions to evaluate.")

    sse = float(stats["sum_squared_error"] or 0.0)
    mse = sse / n
    rmse = math.sqrt(mse)
    mae = float(stats["sum_abs_error"] or 0.0) / n
    sum_label = float(stats["sum_label"] or 0.0)
    sum_label_squared = float(stats["sum_label_squared"] or 0.0)
    denominator = sum_label_squared - ((sum_label * sum_label) / n)
    r2 = float(1.0 - (sse / denominator)) if denominator > 0 else 0.0
    return {"rmse": rmse, "mae": mae, "mse": mse, "r2": r2, "rse": rmse}


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

# Load training data

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

    return sdf.where(F.col("hour_ts").isNotNull())


# COMMAND ----------

# Split and preprocessing

def split_by_time(sdf, target_col):
    usable = sdf.where(F.col(target_col).isNotNull())
    for feature in NUMERIC_FEATURES:
        usable = usable.where(F.col(feature).isNotNull())
    usable = usable.persist(StorageLevel.MEMORY_AND_DISK)
    stats = usable.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct("location_id").alias("location_count"),
    ).first()
    count = int(stats["row_count"] or 0)
    location_count = int(stats["location_count"] or 0)
    if count < 100:
        usable.unpersist()
        raise RuntimeError("Not enough usable rows for " + target_col + ": " + str(count))

    holdout_ratio = max(0.05, min(0.5, float_setting("HOLDOUT_RATIO", 0.2)))
    split_probability = 1.0 - holdout_ratio
    split_source = usable.withColumn("__hour_ts_epoch", F.col("hour_ts").cast("long"))
    split_quantiles = split_source.approxQuantile("__hour_ts_epoch", [split_probability], 0.001)
    if not split_quantiles:
        usable.unpersist()
        raise RuntimeError("Could not determine train/test split for " + target_col)
    split_epoch = int(split_quantiles[0])
    train_df = (
        split_source.where(F.col("__hour_ts_epoch") < F.lit(split_epoch))
        .drop("__hour_ts_epoch")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    test_df = (
        split_source.where(F.col("__hour_ts_epoch") >= F.lit(split_epoch))
        .drop("__hour_ts_epoch")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    train_count = train_df.count()
    test_count = test_df.count()
    if train_count == 0 or test_count == 0:
        usable.unpersist()
        train_df.unpersist()
        test_df.unpersist()
        raise RuntimeError("Could not create train/test split for " + target_col)
    return usable, train_df, test_df, count, train_count, test_count, location_count


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


# COMMAND ----------

# Spark MLlib model specs

def model_specs(label_col):
    if train_profile() == "quick":
        return [
            (
                "Spark Linear Regression",
                LinearRegression(featuresCol="features", labelCol=label_col, predictionCol="prediction", maxIter=60, regParam=0.01),
            ),
            (
                "Spark Decision Tree",
                DecisionTreeRegressor(
                    featuresCol="features",
                    labelCol=label_col,
                    predictionCol="prediction",
                    maxDepth=quick_int_setting("DECISION_TREE_MAX_DEPTH", 8),
                    minInstancesPerNode=2,
                    seed=42,
                ),
            ),
            (
                "Spark Random Forest",
                RandomForestRegressor(
                    featuresCol="features",
                    labelCol=label_col,
                    predictionCol="prediction",
                    numTrees=quick_int_setting("RANDOM_FOREST_TREES", 30),
                    maxDepth=quick_int_setting("RANDOM_FOREST_MAX_DEPTH", 8),
                    minInstancesPerNode=2,
                    seed=42,
                ),
            ),
        ]

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


def model_spec_by_name(label_col):
    return {name: estimator for name, estimator in model_specs(label_col)}


# COMMAND ----------

# Per-location refit helpers

def limited_location_df(sdf, location_id, target_col):
    loc_df = sdf.where(F.col("location_id") == F.lit(location_id))
    max_rows = int_setting("LOCATION_MAX_ROWS", 50000)
    if max_rows > 0:
        loc_df = (
            loc_df.withColumn(
                "__location_recent_rank",
                F.row_number().over(Window.partitionBy("location_id").orderBy(F.col("hour_ts").desc())),
            )
            .where(F.col("__location_recent_rank") <= F.lit(max_rows))
            .drop("__location_recent_rank")
        )
    return loc_df


def location_candidates(sdf, target_col):
    min_rows = int_setting("LOCATION_MIN_ROWS", 5000)
    top_n = int_setting("LOCATION_TOP_N", 0)
    requested_location = str(setting("LOCATION_ID") or "").strip()

    usable = sdf.where(F.col(target_col).isNotNull())
    for feature in NUMERIC_FEATURES:
        usable = usable.where(F.col(feature).isNotNull())
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


def train_location_best_models(sdf, global_rows, targets_to_train):
    if train_scope() != "global_then_location_best":
        return []

    print("Location refit scope enabled: using each target's best global Spark ML model.")
    best_by_target = {}
    for target_variable, _ in targets_to_train:
        target_rows = [
            row
            for row in global_rows
            if row.get("target_variable") == target_variable
            and row.get("training_mode") == "global"
            and row.get("model_type") == "spark_mllib"
            and row.get("is_best")
        ]
        if not target_rows:
            target_rows = [
                row
                for row in global_rows
                if row.get("target_variable") == target_variable
                and row.get("training_mode") == "global"
                and row.get("model_type") == "spark_mllib"
            ]
        if target_rows:
            best_by_target[target_variable] = sorted(target_rows, key=lambda row: (row["rmse"], row["mae"], -row["r2"]))[0]

    location_rows = []
    for target_variable, target_col in targets_to_train:
        best_global = best_by_target.get(target_variable)
        if not best_global:
            print("WARNING: no Spark ML global best available for location refit: " + target_variable)
            continue

        specs = model_spec_by_name(target_col)
        estimator = specs.get(best_global["model_name"])
        if estimator is None:
            print("WARNING: could not rebuild estimator for location refit: " + str(best_global["model_name"]))
            continue

        locations = location_candidates(sdf, target_col)
        print(
            "Location refit for "
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
            loc_sdf = limited_location_df(sdf, location_id, target_col)
            try:
                full_df, train_df, test_df, full_count, train_eval_count, test_count, _ = split_by_time(loc_sdf, target_col)
                try:
                    pipeline = Pipeline(stages=preprocess_stages() + [estimator])
                    eval_model = pipeline.fit(train_df)
                    predictions = eval_model.transform(test_df)
                    metrics = metric_values(predictions, target_col)
                    logged_model = pipeline.fit(full_df) if bool_setting("FINAL_REFIT") else eval_model
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
                        run_name="sparkml_location_" + target_variable + "_" + str(location_id) + "_" + str(best_global["model_name"]).replace(" ", "_")
                    ) as run:
                        params = {
                            "model_name": best_global["model_name"],
                            "feature_columns": ",".join(NUMERIC_FEATURES + CATEGORICAL_FEATURES),
                            "location_id": location_id,
                            "province_id": province_id,
                            "source_global_model_name": best_global["model_name"],
                            "source_global_model_uri": best_global.get("model_uri"),
                            "full_training_points": full_count,
                            "eval_train_points": train_eval_count,
                            "holdout_points": test_count,
                            "location_max_rows": int_setting("LOCATION_MAX_ROWS", 50000),
                            "forecast_horizon_hours": int_setting("FORECAST_HORIZON_HOURS", 168),
                            "input_window_hours": int_setting("INPUT_WINDOW_HOURS", 168),
                            "train_profile": train_profile(),
                            "final_refit": str(bool_setting("FINAL_REFIT")).lower(),
                        }
                        mlflow.set_tags(
                            {
                                "project": "weather_forecast_esp32_meteostat",
                                "training_engine": "sparkml",
                                "training_mode": "per_location",
                                "model_scope": "location_refit_best_global_type",
                                "target_variable": target_variable,
                                "location_id": location_id,
                                "trained_on_full_data": str(bool_setting("FINAL_REFIT")).lower(),
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
                            logged_model,
                            artifact_path="model",
                            pip_requirements=SPARK_MODEL_PIP_REQUIREMENTS,
                        )
                        model_uri = "runs:/" + run.info.run_id + "/model"
                        mlflow_run_id = run.info.run_id

                    label, confidence = quality_label(metrics)
                    location_rows.append(
                        {
                            "model_name": best_global["model_name"],
                            "model_type": "spark_mllib",
                            "training_mode": "per_location",
                            "model_scope": "location_refit_best_global_type",
                            "location_id": location_id,
                            "province_id": province_id,
                            "location_count": 1,
                            "target_variable": target_variable,
                            "horizon": int_setting("FORECAST_HORIZON_HOURS", 168),
                            "forecast_horizon_hours": int_setting("FORECAST_HORIZON_HOURS", 168),
                            "input_window_hours": int_setting("INPUT_WINDOW_HOURS", 168),
                            "mae": float(metrics["mae"]),
                            "rmse": float(metrics["rmse"]),
                            "rse": float(metrics["rse"]),
                            "mape": float(metrics["mape"]) if metrics.get("mape") is not None else None,
                            "r2": float(metrics["r2"]),
                            "training_points": int(full_count if bool_setting("FINAL_REFIT") else train_eval_count),
                            "test_points": int(test_count),
                            "quality_label": label,
                            "confidence_score": float(confidence),
                            "model_uri": model_uri,
                            "mlflow_run_id": mlflow_run_id,
                            "is_best": True,
                            "created_at": utc_now(),
                        }
                    )
                finally:
                    full_df.unpersist()
                    train_df.unpersist()
                    test_df.unpersist()
            except Exception as exc:
                print("WARNING: location refit failed for " + str(location_id) + "/" + target_variable + ": " + str(exc)[:1000])

    return location_rows


# COMMAND ----------

# Lightweight deep learning model definitions

def import_tensorflow():
    try:
        import tensorflow as tf
        from tensorflow import keras
        from tensorflow.keras import layers

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
                layers.LSTM(16, dropout=0.1),
                layers.Dense(8, activation="relu"),
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
                layers.GRU(16, dropout=0.1),
                layers.Dense(8, activation="relu"),
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
                layers.Conv1D(12, kernel_size=3, padding="causal", activation="relu"),
                layers.MaxPooling1D(pool_size=2),
                layers.LSTM(16, dropout=0.1),
                layers.Dense(8, activation="relu"),
                layers.Dense(1),
            ]
        )
        model.compile(optimizer=keras.optimizers.Adam(learning_rate=0.001), loss="mse", metrics=["mae"])
        return model

    return [
        ("Light Keras LSTM", lstm_builder),
        ("Light Keras GRU", gru_builder),
        ("Light Keras CNN-LSTM", cnn_lstm_builder),
    ]


def make_deep_learning_arrays(pdf, target_col, input_window, max_sequences):
    required = ["hour_ts", "location_id", "device_id"] + SEQUENCE_COLS + [target_col]
    frame = pdf[required].dropna().copy()
    if frame.empty:
        return None, None, None
    frame["hour_ts"] = pd.to_datetime(frame["hour_ts"])
    frame = frame.sort_values(["location_id", "device_id", "hour_ts"])

    windows = []
    labels = []
    stamps = []
    possible = max(0, len(frame) - input_window)
    stride = max(1, possible // max(1, int(max_sequences)))

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
            if len(windows) >= max_sequences:
                break
        if len(windows) >= max_sequences:
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

# Lightweight deep learning training

def train_deep_learning_target(sdf, target_variable, target_col, full_count, train_eval_count, test_count, location_count):
    if not bool_setting("ENABLE_DEEP_LEARNING"):
        return []

    input_window = int_setting("DL_INPUT_WINDOW_HOURS", 72)
    max_rows_per_series = int_setting("DL_MAX_ROWS_PER_SERIES", 240)
    max_sequences = int_setting("DL_MAX_SEQUENCES", 4000)
    min_sequences = int_setting("DL_MIN_SEQUENCES", 80)
    epochs = int_setting("DL_EPOCHS", 3)
    batch_size = int_setting("DL_BATCH_SIZE", 64)
    final_refit_epochs = int_setting("DL_FINAL_REFIT_EPOCHS", 0)
    select_cols = ["hour_ts", "location_id", "device_id"] + SEQUENCE_COLS + [target_col]

    try:
        ranked = sdf.select(*select_cols).withColumn(
            "_dl_recent_rank",
            F.row_number().over(Window.partitionBy("location_id", "device_id").orderBy(F.col("hour_ts").desc())),
        )
        limited = ranked.where(F.col("_dl_recent_rank") <= F.lit(max_rows_per_series)).drop("_dl_recent_rank")
        pdf = limited.toPandas()
        x, y, _ = make_deep_learning_arrays(pdf, target_col, input_window, max_sequences)
    except Exception as exc:
        print("WARNING: could not prepare deep learning sequences for " + target_variable + ": " + str(exc)[:1000])
        return []

    if x is None or len(x) < min_sequences:
        print("WARNING: not enough deep learning sequences for " + target_variable)
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
    for model_name, builder in deep_learning_model_specs(input_window, len(SEQUENCE_COLS), feature_mean, feature_variance):
        started = time.time()
        try:
            tf, keras, _ = import_tensorflow()
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
                mlflow.keras.log_model(model, artifact_path="model", pip_requirements=KERAS_MODEL_PIP_REQUIREMENTS)
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
            print("WARNING: " + model_name + " failed for " + target_variable + ": " + str(exc)[:1000])
            try:
                if keras is not None:
                    keras.backend.clear_session()
            except Exception:
                pass
            gc.collect()

    return candidates


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
    specs = model_specs(target_col) if train_family in ("spark_mllib", "both") else []
    model_names = [name for name, _ in specs]
    print("Models to train for " + target_variable + ": " + (", ".join(model_names) if model_names else "none"))
    full_df, train_df, test_df, full_count, train_eval_count, test_count, location_count = split_by_time(sdf, target_col)
    try:
        should_final_refit = bool_setting("FINAL_REFIT")
        print(
            "Target "
            + target_variable
            + ": full_rows="
            + str(full_count)
            + ", eval_train_rows="
            + str(train_eval_count)
            + ", holdout_rows="
            + str(test_count)
            + ", final_refit="
            + str(should_final_refit).lower()
        )

        candidates = []
        if train_family in ("spark_mllib", "both"):
            for model_name, estimator in specs:
                started = time.time()
                try:
                    pipeline = Pipeline(stages=preprocess_stages() + [estimator])
                    eval_model = pipeline.fit(train_df)
                    predictions = eval_model.transform(test_df)
                    metrics = metric_values(predictions, target_col)
                    logged_model = pipeline.fit(full_df) if should_final_refit else eval_model
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
                            "train_profile": train_profile(),
                            "final_refit": str(should_final_refit).lower(),
                        }
                        mlflow.set_tags(
                            {
                                "project": "weather_forecast_esp32_meteostat",
                                "training_engine": "sparkml",
                                "training_mode": "global",
                                "model_scope": "multi_location",
                                "target_variable": target_variable,
                                "trained_on_full_data": str(should_final_refit).lower(),
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
                            logged_model,
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
                            "training_points": full_count if should_final_refit else train_eval_count,
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
            raise RuntimeError("No successful candidates for " + target_variable)

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
    finally:
        full_df.unpersist()
        train_df.unpersist()
        test_df.unpersist()


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
    train_family = "spark_mllib"
    trained_model_types = ["spark_mllib", "deep_learning"] if train_family == "both" else [train_family]
    mlflow.set_experiment(setting("MLFLOW_EXPERIMENT_NAME"))
    sdf = load_training_data().persist(StorageLevel.MEMORY_AND_DISK)
    try:
        source_count = sdf.count()
        print("Loaded full training source rows:", source_count)
        print("Training family:", train_family)
        print("Training profile:", train_profile())
        print("Training scope:", train_scope())
        print("Final refit:", str(bool_setting("FINAL_REFIT")).lower())
        print(
            "Location refit config: location_id="
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

        result_rows.extend(train_location_best_models(sdf, result_rows, targets_to_train))
        write_model_metrics(result_rows, targets_to_train, train_family, trained_model_types)
    finally:
        sdf.unpersist()


# COMMAND ----------

main()