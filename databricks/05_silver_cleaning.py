"""Clean Bronze data and build Silver hourly joins.

Outputs:
  silver_esp32_cleaned
  gold_esp32_hourly
  silver_meteostat_cleaned
  silver_weather_joined
"""

import os
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


DEFAULTS = {
    "DATABRICKS_CATALOG": "dtdm",
    "DATABRICKS_SCHEMA": "metrics_app_streaming",
    "ESP32_EXPECTED_RECORDS_PER_HOUR": "720",
    "LOCATION_SET": "current_34",
    "location_set": "current_34",
    "INCLUDE_INACTIVE_LOCATIONS": "false",
    "include_inactive_locations": "false",
}


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


def setting_any(*names):
    for name in names:
        value = setting(name)
        if value not in ("", None):
            return value
    return ""


def bool_setting(*names):
    return str(setting_any(*names)).strip().lower() in ("1", "true", "yes", "on")


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


def filter_active_meteostat_locations(df):
    location_set = setting_any("LOCATION_SET", "location_set").strip().lower()
    if location_set in ("current_34", "34", "current"):
        df = df.where(F.col("location_id").startswith("loc34_"))
    elif location_set in ("legacy_63", "63", "legacy"):
        df = df.where(~F.col("location_id").startswith("loc34_"))

    if bool_setting("INCLUDE_INACTIVE_LOCATIONS", "include_inactive_locations") or not table_exists("dim_province"):
        return df

    active_provinces = (
        spark.table(fq_table("dim_province"))  # type: ignore[name-defined]
        .where(F.coalesce(F.col("is_active"), F.lit(True)) == F.lit(True))
        .select(F.col("province_id").alias("active_province_id"))
        .dropDuplicates(["active_province_id"])
    )
    return df.join(active_provinces, df.province_id == active_provinces.active_province_id, "inner").drop(
        "active_province_id"
    )


def empty_df(schema):
    return spark.createDataFrame([], schema)  # type: ignore[name-defined]


def write_delta(df, table_name, partition_cols=None):
    target_table = fq_table(table_name)
    if not table_exists(table_name):
        raise RuntimeError(target_table + " does not exist. Run 01_create_catalog_schema_tables.sql first.")

    target_schema = spark.table(target_table).schema  # type: ignore[name-defined]
    for field in target_schema:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
    df = df.select([F.col(field.name).cast(field.dataType).alias(field.name) for field in target_schema])

    spark.sql("DELETE FROM " + target_table)  # type: ignore[name-defined]
    df.write.format("delta").mode("append").saveAsTable(target_table)


def clean_esp32():
    schema = StructType(
        [
            StructField("device_id", StringType(), True),
            StructField("location_id", StringType(), True),
            StructField("province_id", StringType(), True),
            StructField("event_ts", TimestampType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("ingest_time", TimestampType(), True),
            StructField("quality_flag", StringType(), True),
        ]
    )
    if not table_exists("bronze_esp32_raw"):
        print("WARNING: bronze_esp32_raw does not exist; writing empty ESP32 Silver table.")
        return empty_df(schema)

    raw = spark.table(fq_table("bronze_esp32_raw"))  # type: ignore[name-defined]
    cleaned = raw.select(
        F.col("device_id").cast("string"),
        F.col("location_id").cast("string"),
        F.col("province_id").cast("string"),
        F.to_timestamp("event_ts").alias("event_ts"),
        F.col("temperature").cast("double").alias("temperature"),
        F.col("humidity").cast("double").alias("humidity"),
        F.coalesce(F.to_timestamp("ingest_time"), F.current_timestamp()).alias("ingest_time"),
    ).withColumn(
        "quality_flag",
        F.when(F.col("event_ts").isNull(), F.lit("invalid_timestamp"))
        .when(~F.col("temperature").between(-10.0, 60.0), F.lit("invalid_temperature"))
        .when(~F.col("humidity").between(0.0, 100.0), F.lit("invalid_humidity"))
        .otherwise(F.lit("valid")),
    )
    return cleaned.where(F.col("event_ts").isNotNull())


def build_esp32_hourly(silver_esp32):
    expected = float(setting("ESP32_EXPECTED_RECORDS_PER_HOUR") or "720")
    valid = silver_esp32.where("quality_flag = 'valid'")
    return (
        valid.withColumn("hour_ts", F.date_trunc("hour", F.col("event_ts")))
        .groupBy("device_id", "location_id", "province_id", "hour_ts")
        .agg(
            F.avg("temperature").alias("temperature_avg"),
            F.min("temperature").alias("temperature_min"),
            F.max("temperature").alias("temperature_max"),
            F.avg("humidity").alias("humidity_avg"),
            F.min("humidity").alias("humidity_min"),
            F.max("humidity").alias("humidity_max"),
            F.count("*").cast("int").alias("record_count"),
        )
        .withColumn("missing_ratio", F.greatest(F.lit(0.0), F.lit(1.0) - (F.col("record_count") / F.lit(expected))))
        .withColumn("created_at", F.current_timestamp())
    )


def clean_meteostat():
    schema = StructType(
        [
            StructField("location_id", StringType(), True),
            StructField("province_id", StringType(), True),
            StructField("event_ts", TimestampType(), True),
            StructField("temperature_c", DoubleType(), True),
            StructField("relative_humidity", DoubleType(), True),
            StructField("quality_flag", StringType(), True),
        ]
    )
    if not table_exists("bronze_meteostat_hourly"):
        raise RuntimeError("bronze_meteostat_hourly does not exist. Run 04_sync_meteostat_to_bronze.py first.")

    raw = filter_active_meteostat_locations(spark.table(fq_table("bronze_meteostat_hourly")))  # type: ignore[name-defined]
    cleaned = raw.select(
        F.col("location_id").cast("string"),
        F.col("province_id").cast("string"),
        F.to_timestamp("event_ts").alias("event_ts"),
        F.col("temperature_c").cast("double"),
        F.col("relative_humidity").cast("double"),
    ).withColumn(
        "quality_flag",
        F.when(F.col("event_ts").isNull(), F.lit("invalid_timestamp"))
        .when(~F.col("temperature_c").between(-10.0, 60.0), F.lit("invalid_temperature"))
        .when(~F.col("relative_humidity").between(0.0, 100.0), F.lit("invalid_humidity"))
        .otherwise(F.lit("valid")),
    )

    return cleaned.where(F.col("event_ts").isNotNull() & (F.col("quality_flag") == "valid")).dropDuplicates(
        ["location_id", "event_ts"]
    )


def build_joined(esp32_hourly, meteostat):
    meteo = meteostat.select(
        "location_id",
        "province_id",
        F.col("event_ts").alias("hour_ts"),
        F.col("temperature_c").alias("meteostat_temperature"),
        F.col("relative_humidity").alias("meteostat_humidity"),
    ).alias("meteo")

    esp = esp32_hourly.select(
        "device_id",
        "location_id",
        F.col("hour_ts").alias("esp_hour_ts"),
        F.col("temperature_avg").alias("esp32_temperature_avg"),
        F.col("humidity_avg").alias("esp32_humidity_avg"),
    ).alias("esp")

    return meteo.join(
        esp,
        (F.col("meteo.location_id") == F.col("esp.location_id")) & (F.col("meteo.hour_ts") == F.col("esp.esp_hour_ts")),
        "left",
    ).select(
        F.col("meteo.location_id").alias("location_id"),
        F.col("meteo.province_id").alias("province_id"),
        F.col("esp.device_id").alias("device_id"),
        F.col("meteo.hour_ts").alias("hour_ts"),
        F.col("esp.esp32_temperature_avg").alias("esp32_temperature_avg"),
        F.col("esp.esp32_humidity_avg").alias("esp32_humidity_avg"),
        F.col("meteo.meteostat_temperature").alias("meteostat_temperature"),
        F.col("meteo.meteostat_humidity").alias("meteostat_humidity"),
    )


def main():
    create_widgets()

    silver_esp32 = clean_esp32()
    write_delta(silver_esp32, "silver_esp32_cleaned", ["province_id"])

    esp32_hourly = build_esp32_hourly(silver_esp32)
    write_delta(esp32_hourly, "gold_esp32_hourly", ["province_id"])

    silver_meteostat = clean_meteostat()
    write_delta(silver_meteostat, "silver_meteostat_cleaned", ["province_id"])

    joined = build_joined(esp32_hourly, silver_meteostat)
    write_delta(joined, "silver_weather_joined", ["province_id"])

    print("Silver cleaning complete.")
    print(f"ESP32 cleaned rows: {silver_esp32.count()}")
    print(f"ESP32 hourly rows: {esp32_hourly.count()}")
    print(f"Meteostat cleaned rows: {silver_meteostat.count()}")
    print(f"Joined weather rows: {joined.count()}")


if __name__ == "__main__":
    main()
