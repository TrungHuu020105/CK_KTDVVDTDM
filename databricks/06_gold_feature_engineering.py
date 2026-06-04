"""Build the Gold training dataset for 168-hour weather forecasting."""

import os
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark import StorageLevel


DEFAULTS = {
    "DATABRICKS_CATALOG": "dtdm",
    "DATABRICKS_SCHEMA": "metrics_app_streaming",
    "FORECAST_HORIZON_HOURS": "168",
}

STALE_CATALOG_WARNING_SHOWN = False


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


def create_widgets():
    try:
        for name, default in DEFAULTS.items():
            dbutils.widgets.text(name, os.getenv(name, default))  # type: ignore[name-defined]
    except Exception:
        pass


def fq_table(name):
    return f"{setting('DATABRICKS_CATALOG')}.{setting('DATABRICKS_SCHEMA')}.{name}"


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


def overwrite_existing_table(df, table_name):
    target_table = fq_table(table_name)
    if not table_exists(table_name):
        raise RuntimeError(target_table + " does not exist. Run 01b_create_missing_silver_gold_tables.sql first.")

    target_schema = spark.table(target_table).schema  # type: ignore[name-defined]
    for field in target_schema:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
    df = df.select([F.col(field.name).cast(field.dataType).alias(field.name) for field in target_schema])

    spark.sql("TRUNCATE TABLE " + target_table)  # type: ignore[name-defined]
    df.write.format("delta").mode("append").saveAsTable(target_table)


def main():
    create_widgets()
    warn_stale_catalog_once()
    source_table = fq_table("silver_weather_joined")
    target_table = fq_table("gold_training_dataset")
    horizon = int(setting("FORECAST_HORIZON_HOURS") or "168")

    print("Using Databricks namespace: " + setting("DATABRICKS_CATALOG") + "." + setting("DATABRICKS_SCHEMA"))

    if not table_exists("silver_weather_joined"):
        raise RuntimeError(f"{source_table} does not exist. Run 05_silver_cleaning.py first.")
    if not table_exists("gold_training_dataset"):
        raise RuntimeError(f"{target_table} does not exist. Run 01b_create_missing_silver_gold_tables.sql first.")

    base = (
        spark.table(source_table)  # type: ignore[name-defined]
        .select(
            "location_id",
            "province_id",
            "device_id",
            "hour_ts",
            F.coalesce(F.col("esp32_temperature_avg"), F.col("meteostat_temperature")).alias("temperature"),
            F.coalesce(F.col("esp32_humidity_avg"), F.col("meteostat_humidity")).alias("humidity"),
            F.col("meteostat_temperature"),
            F.col("meteostat_humidity"),
        )
        .where(F.col("hour_ts").isNotNull())
        .where(F.col("temperature").isNotNull() & F.col("humidity").isNotNull())
        .dropDuplicates(["location_id", "device_id", "hour_ts"])
        .repartition("location_id", "device_id")
    )

    entity_window = Window.partitionBy("location_id", "device_id").orderBy("hour_ts")
    rolling_24 = entity_window.rowsBetween(-23, 0)
    rolling_168 = entity_window.rowsBetween(-167, 0)

    features = (
        base.withColumn("hour", F.hour("hour_ts"))
        .withColumn("day_of_week", F.dayofweek("hour_ts"))
        .withColumn("month", F.month("hour_ts"))
        .withColumn("temp_lag_1", F.lag("temperature", 1).over(entity_window))
        .withColumn("temp_lag_6", F.lag("temperature", 6).over(entity_window))
        .withColumn("temp_lag_12", F.lag("temperature", 12).over(entity_window))
        .withColumn("temp_lag_24", F.lag("temperature", 24).over(entity_window))
        .withColumn("humidity_lag_1", F.lag("humidity", 1).over(entity_window))
        .withColumn("humidity_lag_6", F.lag("humidity", 6).over(entity_window))
        .withColumn("humidity_lag_12", F.lag("humidity", 12).over(entity_window))
        .withColumn("humidity_lag_24", F.lag("humidity", 24).over(entity_window))
        .withColumn("temp_rolling_24", F.avg("temperature").over(rolling_24))
        .withColumn("humidity_rolling_24", F.avg("humidity").over(rolling_24))
        .withColumn("temp_rolling_168", F.avg("temperature").over(rolling_168))
        .withColumn("humidity_rolling_168", F.avg("humidity").over(rolling_168))
        .withColumn("temp_target_168", F.lead("temperature", horizon).over(entity_window))
        .withColumn("humidity_target_168", F.lead("humidity", horizon).over(entity_window))
        .withColumn("created_at", F.current_timestamp())
    )

    training = features.select(
        "location_id",
        "device_id",
        "hour_ts",
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
        "temp_target_168",
        "humidity_target_168",
        "created_at",
    )

    training = training.persist(StorageLevel.MEMORY_AND_DISK)
    row_count = training.count()

    try:
        overwrite_existing_table(training, "gold_training_dataset")
        print(f"Wrote {row_count} rows to {target_table}.")
    finally:
        training.unpersist()


if __name__ == "__main__":
    main()
