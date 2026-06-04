"""Sync ESP32 temperature/humidity rows from PostgreSQL into Bronze Delta.

Expected output table:
  dtdm.metrics_app_streaming.bronze_esp32_raw

The script intentionally keeps ESP32 data as raw Bronze data. Training uses the
hourly Gold layer after cleaning/aggregation, and Meteostat remains the primary
historical source until ESP32 has enough history.
"""

import os
from pathlib import Path
from urllib.parse import urlparse

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F


DEFAULTS = {
    "DATABRICKS_CATALOG": "dtdm",
    "DATABRICKS_SCHEMA": "metrics_app_streaming",
    "POSTGRES_TABLE": "sensor_readings",
    "POSTGRES_LOOKBACK_DAYS": "30",
    "POSTGRES_SYNC_LOOKBACK_DAYS": "",
    "POSTGRES_SOURCE_QUERY": "",
    "POSTGRES_JDBC_URL": "",
    "DATABASE_URL": "",
    "POSTGRES_USER": "",
    "POSTGRES_PASSWORD": "",
    "DB_HOST": "20.214.247.102",
    "DB_PORT": "5432",
    "DB_DATABASE": "rtmps_db",
    "DB_USERNAME": "rtmps_user",
    "DB_PASSWORD": "123456",
    "POSTGRES_SECRET_SCOPE": "",
    "POSTGRES_PASSWORD_SECRET_KEY": "POSTGRES_PASSWORD",
    "DB_PASSWORD_SECRET_SCOPE": "",
    "DB_PASSWORD_SECRET_KEY": "DB_PASSWORD",
    "DATABRICKS_SECRET_SCOPE": "",
    "POSTGRES_DRIVER": "org.postgresql.Driver",
    "POSTGRES_DEVICE_ID_COLUMN": "",
    "POSTGRES_LOCATION_ID_COLUMN": "",
    "POSTGRES_PROVINCE_ID_COLUMN": "",
    "POSTGRES_EVENT_TS_COLUMN": "event_ts",
    "POSTGRES_TEMPERATURE_COLUMN": "temperature",
    "POSTGRES_HUMIDITY_COLUMN": "humidity",
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
        return value if value and value.lower() not in {"none", "null"} else None
    except Exception:
        return None


def setting(name):
    return os.getenv(name) or widget_value(name) or DEFAULTS.get(name, "")


def create_widgets():
    try:
        for name, default in DEFAULTS.items():
            dbutils.widgets.text(name, os.getenv(name, default))  # type: ignore[name-defined]
    except Exception:
        pass


def secret(scope_name, key_name):
    if not scope_name:
        return None
    try:
        return dbutils.secrets.get(scope=scope_name, key=key_name)  # type: ignore[name-defined]
    except Exception as exc:
        print(f"WARNING: cannot read Databricks secret {scope_name}/{key_name}: {exc}")
        return None


def jdbc_url():
    direct = setting("POSTGRES_JDBC_URL")
    if direct:
        return direct

    database_url = setting("DATABASE_URL")
    if database_url:
        normalized = database_url.replace("postgresql+psycopg2://", "postgresql://", 1)
        parsed = urlparse(normalized)
        if parsed.scheme not in {"postgresql", "postgres"}:
            raise ValueError("DATABASE_URL must use postgresql:// or postgres://")
        return f"jdbc:postgresql://{parsed.hostname}:{parsed.port or 5432}{parsed.path}"

    host = setting("DB_HOST")
    database = setting("DB_DATABASE")
    port = setting("DB_PORT") or "5432"
    if host and database:
        return f"jdbc:postgresql://{host}:{port}/{database}"

    raise ValueError("Set POSTGRES_JDBC_URL, DATABASE_URL, or DB_HOST/DB_PORT/DB_DATABASE before running the sync.")


def credentials():
    user = setting("POSTGRES_USER") or setting("DB_USERNAME")
    password = setting("POSTGRES_PASSWORD") or setting("DB_PASSWORD")

    database_url = setting("DATABASE_URL")
    if database_url and (not user or not password):
        parsed = urlparse(database_url.replace("postgresql+psycopg2://", "postgresql://", 1))
        user = user or (parsed.username or "")
        password = password or (parsed.password or "")

    if not password:
        password = secret(setting("POSTGRES_SECRET_SCOPE"), setting("POSTGRES_PASSWORD_SECRET_KEY")) or ""
    if not password:
        password = secret(setting("DB_PASSWORD_SECRET_SCOPE"), setting("DB_PASSWORD_SECRET_KEY")) or ""
    if not password:
        password = secret(setting("DATABRICKS_SECRET_SCOPE"), setting("DB_PASSWORD_SECRET_KEY")) or ""

    if not user or not password:
        raise ValueError(
            "Missing PostgreSQL credentials for 03_sync_postgres_to_bronze.py.\n"
            "DB_HOST/DB_DATABASE/DB_USERNAME already have project defaults, but DB_PASSWORD is intentionally blank.\n"
            "Choose one option before running this job:\n"
            "1) Databricks widget/Job parameter: DB_PASSWORD=<your PostgreSQL password>\n"
            "2) Databricks secret: DB_PASSWORD_SECRET_SCOPE=<scope>, DB_PASSWORD_SECRET_KEY=DB_PASSWORD\n"
            "3) Full DATABASE_URL=postgresql://rtmps_user:<password>@20.214.247.102:5432/rtmps_db\n"
            "Do not hard-code DB_PASSWORD in the source file."
        )
    return user, password


def fq_table(name):
    return f"{setting('DATABRICKS_CATALOG')}.{setting('DATABRICKS_SCHEMA')}.{name}"


def catalog_exists(catalog):
    rows = spark.sql("SHOW CATALOGS LIKE '" + catalog.replace("'", "''") + "'").collect()  # type: ignore[name-defined]
    return len(rows) > 0


def ensure_namespace():
    catalog = setting("DATABRICKS_CATALOG")
    schema = setting("DATABRICKS_SCHEMA")
    # Catalog creation requires a Unity Catalog metastore storage root or an
    # explicit MANAGED LOCATION. Use an existing catalog and create only schema.
    if not catalog_exists(catalog):
        raise RuntimeError(
            "Catalog "
            + catalog
            + " does not exist. Create it from Databricks UI using Default Storage, "
            + "or run CREATE CATALOG "
            + catalog
            + " MANAGED LOCATION '<abfss-path>' before this job. "
            + "This script intentionally does not CREATE CATALOG."
        )
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")  # type: ignore[name-defined]


def source_query():
    custom_query = setting("POSTGRES_SOURCE_QUERY")
    if custom_query:
        return custom_query

    table = setting("POSTGRES_TABLE")
    lookback_days = int(setting("POSTGRES_LOOKBACK_DAYS") or setting("POSTGRES_SYNC_LOOKBACK_DAYS") or "30")
    device_col = setting("POSTGRES_DEVICE_ID_COLUMN")
    location_col = setting("POSTGRES_LOCATION_ID_COLUMN")
    province_col = setting("POSTGRES_PROVINCE_ID_COLUMN")
    event_ts_col = setting("POSTGRES_EVENT_TS_COLUMN")
    temperature_col = setting("POSTGRES_TEMPERATURE_COLUMN")
    humidity_col = setting("POSTGRES_HUMIDITY_COLUMN")

    device_expr = f"CAST(sr.{device_col} AS TEXT)" if device_col else "CAST(COALESCE(sr.device_id, d.id) AS TEXT)"
    location_expr = (
        f"CAST(sr.{location_col} AS TEXT)"
        if location_col
        else "COALESCE(d.source, sr.sensor_id, sr.location, sr.location_province, CAST(COALESCE(sr.device_id, d.id) AS TEXT))"
    )
    province_expr = f"CAST(sr.{province_col} AS TEXT)" if province_col else "COALESCE(d.location_province, sr.location_province)"

    return f"""
      SELECT
        {device_expr} AS device_id,
        {location_expr} AS location_id,
        {province_expr} AS province_id,
        sr.{event_ts_col} AS event_ts,
        sr.{temperature_col} AS temperature,
        sr.{humidity_col} AS humidity
      FROM {table} sr
      LEFT JOIN iot_devices d ON sr.device_id = d.id
      WHERE sr.{event_ts_col} >= NOW() - INTERVAL '{lookback_days} days'
    """


def read_postgres():
    user, password = credentials()
    try:
        return (
            spark.read.format("jdbc")  # type: ignore[name-defined]
            .option("url", jdbc_url())
            .option("query", source_query())
            .option("user", user)
            .option("password", password)
            .option("driver", setting("POSTGRES_DRIVER"))
            .load()
        )
    except Exception as exc:
        raise RuntimeError(
            "Failed to read ESP32 rows from PostgreSQL. Check JDBC URL, driver, credentials, table, and column mappings."
        ) from exc


def prepare_bronze(raw):
    prepared = (
        raw.select(
            F.col("device_id").cast("string"),
            F.col("location_id").cast("string"),
            F.col("province_id").cast("string"),
            F.to_timestamp("event_ts").alias("event_ts"),
            F.col("temperature").cast("double").alias("temperature"),
            F.col("humidity").cast("double").alias("humidity"),
            F.current_timestamp().alias("ingest_time"),
            F.lit("postgres_esp32").alias("source"),
        )
        .where(F.col("device_id").isNotNull() & F.col("event_ts").isNotNull())
        .where(F.col("temperature").isNotNull() | F.col("humidity").isNotNull())
    )

    window = Window.partitionBy("device_id", "event_ts").orderBy(F.col("ingest_time").desc())
    return prepared.withColumn("rn", F.row_number().over(window)).where("rn = 1").drop("rn")


def table_exists(table_name):
    return spark.catalog.tableExists(table_name)  # type: ignore[name-defined]


def is_delta_table(target_table):
    try:
        detail = spark.sql("DESCRIBE DETAIL " + target_table).collect()[0].asDict()  # type: ignore[name-defined]
        return str(detail.get("format", "")).lower() == "delta"
    except Exception as exc:
        print("WARNING: cannot inspect table format for " + target_table + ": " + str(exc))
        return False


def target_has_canonical_schema(target_table):
    required_columns = {
        "device_id",
        "location_id",
        "province_id",
        "event_ts",
        "temperature",
        "humidity",
        "ingest_time",
        "source",
    }
    existing_columns = set(spark.table(target_table).columns)  # type: ignore[name-defined]
    missing_columns = sorted(required_columns - existing_columns)
    if missing_columns:
        print(
            "WARNING: "
            + target_table
            + " has an old/incompatible schema. Missing columns: "
            + ", ".join(missing_columns)
            + ". The table will be overwritten with the canonical bronze_esp32_raw schema."
        )
        return False
    return True


def replace_table(df, target_table, reason):
    print("Replacing " + target_table + " as a Delta table. Reason: " + reason)
    try:
        spark.sql("DROP TABLE IF EXISTS " + target_table)  # type: ignore[name-defined]
    except Exception as exc:
        print("WARNING: DROP TABLE failed, trying overwriteSchema instead: " + str(exc))
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    print(f"Created/replaced {target_table} with {df.count()} rows.")


def merge_bronze(df, target_table):
    if df.limit(1).count() == 0:
        print("No ESP32 rows to sync.")
        return

    if not table_exists(target_table):
        replace_table(df, target_table, "table does not exist")
        return

    if not is_delta_table(target_table):
        replace_table(df, target_table, "existing object is not a Delta table")
        return

    if not target_has_canonical_schema(target_table):
        replace_table(df, target_table, "existing Delta table has old/incompatible schema")
        return

    target = DeltaTable.forName(spark, target_table)  # type: ignore[name-defined]
    merge_values = {
        "device_id": "source.device_id",
        "location_id": "source.location_id",
        "province_id": "source.province_id",
        "event_ts": "source.event_ts",
        "temperature": "source.temperature",
        "humidity": "source.humidity",
        "ingest_time": "source.ingest_time",
        "source": "source.source",
    }
    (
        target.alias("target")
        .merge(
            df.alias("source"),
            "target.device_id = source.device_id AND target.event_ts = source.event_ts",
        )
        .whenMatchedUpdate(set=merge_values)
        .whenNotMatchedInsert(values=merge_values)
        .execute()
    )
    print(f"Upserted ESP32 Bronze rows into {target_table}.")


def main():
    create_widgets()
    ensure_namespace()
    target_table = fq_table("bronze_esp32_raw")
    bronze_df = prepare_bronze(read_postgres())
    merge_bronze(bronze_df, target_table)


if __name__ == "__main__":
    main()
