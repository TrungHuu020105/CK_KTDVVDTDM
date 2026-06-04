# Databricks notebook source

"""Sync Meteostat hourly weather data to Bronze by location-year batches.

Input:
  dim_location(location_id, province_id, latitude, longitude, altitude)

Output:
  bronze_meteostat_hourly
  meteostat_sync_status

Run 02_seed_dim_locations.py first when dim_location only has ESP32 locations.
Default sync range is START_YEAR=2022 through END_YEAR=2025.
"""

# COMMAND ----------

# Imports

import os
import subprocess
import sys
import warnings
from datetime import datetime, timezone
from math import asin, cos, radians, sin, sqrt
from pathlib import Path

import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    BooleanType,
)

warnings.filterwarnings("ignore", message="Support for nested sequences for 'parse_dates'.*", category=FutureWarning)
warnings.filterwarnings("ignore", message="'H' is deprecated.*", category=FutureWarning)


# COMMAND ----------

# Constants and table schemas

DEFAULTS = {
    "DATABRICKS_CATALOG": "dtdm",
    "DATABRICKS_SCHEMA": "metrics_app_streaming",
    "START_YEAR": "2022",
    "END_YEAR": "2025",
    "start_year": "2022",
    "end_year": "2025",
    "MAX_LOCATIONS": "0",
    "max_locations": "0",
    "LOCATION_SET": "current_34",
    "location_set": "current_34",
    "INCLUDE_INACTIVE_LOCATIONS": "false",
    "include_inactive_locations": "false",
    "OVERWRITE_EXISTING": "false",
    "overwrite_existing": "false",
    "RETRY_FAILED_ONLY": "false",
    "retry_failed_only": "false",
    "AUTO_INSTALL_METEOSTAT": "true",
    "METEOSTAT_MAX_NEARBY_STATIONS": "50",
    "METEOSTAT_STATION_RADIUS_METERS": "250000",
    "MAX_STATION_DISTANCE_KM": "200",
    "METEOSTAT_MIN_COVERAGE_RATIO": "0.6",
    "DEBUG": "false",
    "debug": "false",
}

COLLECTION_WARNINGS = []
COLLECTION_SUMMARY_ROWS = []


BRONZE_SCHEMA = StructType(
    [
        StructField("location_id", StringType(), True),
        StructField("province_id", StringType(), True),
        StructField("event_ts", TimestampType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("relative_humidity", DoubleType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("ingest_time", TimestampType(), True),
        StructField("source", StringType(), True),
        StructField("fetch_method", StringType(), True),
        StructField("station_id", StringType(), True),
        StructField("station_name", StringType(), True),
        StructField("station_distance_km", DoubleType(), True),
        StructField("station_latitude", DoubleType(), True),
        StructField("station_longitude", DoubleType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("data_quality", StringType(), True),
        StructField("province_name", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
    ]
)

STATUS_SCHEMA = StructType(
    [
        StructField("location_id", StringType(), True),
        StructField("province_id", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fetch_method", StringType(), True),
        StructField("station_id", StringType(), True),
        StructField("station_name", StringType(), True),
        StructField("station_distance_km", DoubleType(), True),
        StructField("expected_hours", IntegerType(), True),
        StructField("coverage_ratio", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("row_count", IntegerType(), True),
        StructField("error_message", StringType(), True),
        StructField("started_at", TimestampType(), True),
        StructField("finished_at", TimestampType(), True),
    ]
)

WEATHER_STATION_SCHEMA = StructType(
    [
        StructField("station_id", StringType(), True),
        StructField("station_name", StringType(), True),
        StructField("location_id", StringType(), True),
        StructField("province_id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("elevation", DoubleType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("is_primary", BooleanType(), True),
        StructField("data_source", StringType(), True),
        StructField("hourly_start", TimestampType(), True),
        StructField("hourly_end", TimestampType(), True),
        StructField("created_at", TimestampType(), True),
    ]
)

STATION_MAPPING_SCHEMA = StructType(
    [
        StructField("location_id", StringType(), True),
        StructField("province_id", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("station_id", StringType(), True),
        StructField("station_name", StringType(), True),
        StructField("fetch_method", StringType(), True),
        StructField("row_count", IntegerType(), True),
        StructField("expected_hours", IntegerType(), True),
        StructField("coverage_ratio", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("selected_at", TimestampType(), True),
    ]
)

COLLECTION_SUMMARY_SCHEMA = StructType(
    [
        StructField("location_id", StringType(), True),
        StructField("province_id", StringType(), True),
        StructField("province_name", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("row_count", IntegerType(), True),
        StructField("expected_hours", IntegerType(), True),
        StructField("coverage_ratio", DoubleType(), True),
        StructField("fetch_method", StringType(), True),
        StructField("station_id", StringType(), True),
        StructField("station_name", StringType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("data_quality", StringType(), True),
        StructField("warning_count", IntegerType(), True),
        StructField("error_message", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ]
)


# COMMAND ----------

# Environment, widgets, and general helpers

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


def patch_pandas_parse_dates_for_meteostat():
    if getattr(pd.read_csv, "_meteostat_parse_dates_patch", False):
        return
    original_read_csv = pd.read_csv

    def patched_read_csv(*args, **kwargs):
        parse_dates = kwargs.get("parse_dates")
        if isinstance(parse_dates, str):
            kwargs["parse_dates"] = [parse_dates]
        return original_read_csv(*args, **kwargs)

    patched_read_csv._meteostat_parse_dates_patch = True
    pd.read_csv = patched_read_csv


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


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


def int_setting(default_value, *names):
    raw = setting_any(*names)
    try:
        return int(raw)
    except Exception:
        return int(default_value)


def float_setting(default_value, *names):
    raw = setting_any(*names)
    try:
        return float(raw)
    except Exception:
        return float(default_value)


def create_widgets():
    try:
        for name, default in DEFAULTS.items():
            dbutils.widgets.text(name, os.getenv(name, default))  # type: ignore[name-defined]
    except Exception:
        pass


def fq_table(name):
    return setting("DATABRICKS_CATALOG") + "." + setting("DATABRICKS_SCHEMA") + "." + name


def debug_enabled():
    return bool_setting("DEBUG", "debug")


def debug_print(message):
    if debug_enabled():
        print(message)


def add_collection_warning(location_id, year, warning_type, message, station_id=None, distance_km=None):
    COLLECTION_WARNINGS.append(
        {
            "location_id": str(location_id) if location_id is not None else None,
            "year": int(year) if year is not None else None,
            "warning_type": str(warning_type),
            "message": str(message)[:1000],
            "station_id": str(station_id) if station_id is not None else None,
            "distance_km": float(distance_km) if distance_km is not None else None,
        }
    )
    debug_print("WARNING: " + str(message))


def data_quality_for_distance(distance_km, fetch_method=None):
    if fetch_method == "point" or distance_km is None:
        return "point"
    distance = float(distance_km)
    if distance <= 75.0:
        return "near_station"
    if distance <= 150.0:
        return "medium_distance_station"
    if distance <= 200.0:
        return "far_station"
    return "too_far_station"


def active_config():
    return {
        "LOCATION_SET": setting_any("LOCATION_SET", "location_set"),
        "OVERWRITE_EXISTING": setting_any("OVERWRITE_EXISTING", "overwrite_existing"),
        "MAX_STATION_DISTANCE_KM": setting("MAX_STATION_DISTANCE_KM"),
        "METEOSTAT_STATION_RADIUS_METERS": setting("METEOSTAT_STATION_RADIUS_METERS"),
        "METEOSTAT_MAX_NEARBY_STATIONS": setting("METEOSTAT_MAX_NEARBY_STATIONS"),
        "METEOSTAT_MIN_COVERAGE_RATIO": setting("METEOSTAT_MIN_COVERAGE_RATIO"),
        "DEBUG": setting_any("DEBUG", "debug"),
    }


def print_active_config():
    print("Meteostat active config:")
    for key, value in active_config().items():
        print("  " + key + "=" + str(value))


# COMMAND ----------

# Catalog and location loading

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


def namespace_exists(catalog, schema):
    rows = spark.sql("SHOW SCHEMAS IN " + catalog + " LIKE '" + schema + "'").collect()  # type: ignore[name-defined]
    return len(rows) > 0


def ensure_namespace_and_tables():
    catalog = setting("DATABRICKS_CATALOG")
    schema = setting("DATABRICKS_SCHEMA")
    if not namespace_exists(catalog, schema):
        raise RuntimeError(
            "Schema "
            + catalog
            + "."
            + schema
            + " does not exist. Run 01_create_catalog_schema_tables.sql first."
        )
    required_tables = [
        "dim_location",
        "bronze_meteostat_hourly",
        "meteostat_sync_status",
        "dim_weather_station",
        "meteostat_station_mapping",
    ]
    missing_tables = [name for name in required_tables if not table_exists(fq_table(name))]
    if missing_tables:
        raise RuntimeError(
            "Missing required Delta tables in "
            + catalog
            + "."
            + schema
            + ": "
            + ", ".join(missing_tables)
            + ". Run 01_create_catalog_schema_tables.sql first."
        )
    try:
        spark.sql(
            "ALTER TABLE "
            + fq_table("bronze_meteostat_hourly")
            + " ADD COLUMNS IF NOT EXISTS ("
            + "fetch_method STRING, "
            + "station_id STRING, "
            + "station_name STRING, "
            + "station_distance_km DOUBLE, "
            + "station_latitude DOUBLE, "
            + "station_longitude DOUBLE, "
            + "distance_km DOUBLE, "
            + "data_quality STRING, "
            + "province_name STRING, "
            + "precipitation DOUBLE, "
            + "wind_speed DOUBLE, "
            + "pressure DOUBLE"
            + ")"
        )  # type: ignore[name-defined]
    except Exception:
        pass
    try:
        spark.sql(
            "ALTER TABLE "
            + fq_table("meteostat_sync_status")
            + " ADD COLUMNS IF NOT EXISTS (fetch_method STRING, station_id STRING, station_name STRING, station_distance_km DOUBLE, expected_hours INT, coverage_ratio DOUBLE)"
        )  # type: ignore[name-defined]
    except Exception:
        pass
    try:
        spark.sql(
            "ALTER TABLE "
            + fq_table("meteostat_station_mapping")
            + " ADD COLUMNS IF NOT EXISTS (expected_hours INT, coverage_ratio DOUBLE)"
        )  # type: ignore[name-defined]
    except Exception:
        pass
    spark.sql(  # type: ignore[name-defined]
        "CREATE TABLE IF NOT EXISTS "
        + fq_table("meteostat_collection_summary")
        + " ("
        + "location_id STRING, "
        + "province_id STRING, "
        + "province_name STRING, "
        + "year INT, "
        + "status STRING, "
        + "row_count INT, "
        + "expected_hours INT, "
        + "coverage_ratio DOUBLE, "
        + "fetch_method STRING, "
        + "station_id STRING, "
        + "station_name STRING, "
        + "distance_km DOUBLE, "
        + "data_quality STRING, "
        + "warning_count INT, "
        + "error_message STRING, "
        + "created_at TIMESTAMP"
        + ") USING DELTA"
    )


def configured_years():
    start_year = int_setting(2022, "START_YEAR", "start_year")
    end_year = int_setting(2025, "END_YEAR", "end_year")
    if end_year < start_year:
        raise ValueError("END_YEAR must be >= START_YEAR.")
    return list(range(start_year, end_year + 1))


def load_locations():
    table_name = fq_table("dim_location")
    if not table_exists(table_name):
        raise RuntimeError(table_name + " does not exist. Run 02_seed_dim_locations.py before 04_sync_meteostat_to_bronze.py.")

    query = (
        spark.table(table_name)  # type: ignore[name-defined]
        .where(F.col("location_id").isNotNull() & F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
        .select("location_id", "province_id", "latitude", "longitude", "altitude")
    )
    location_set = setting_any("LOCATION_SET", "location_set").strip().lower()
    if location_set in ("current_34", "34", "current"):
        query = query.where(F.col("location_id").startswith("loc34_"))
    elif location_set in ("legacy_63", "63", "legacy"):
        query = query.where(~F.col("location_id").startswith("loc34_"))

    if not bool_setting("INCLUDE_INACTIVE_LOCATIONS", "include_inactive_locations") and table_exists(fq_table("dim_province")):
        active_provinces = (
            spark.table(fq_table("dim_province"))  # type: ignore[name-defined]
            .where(F.coalesce(F.col("is_active"), F.lit(True)) == F.lit(True))
            .select(F.col("province_id").alias("active_province_id"))
            .dropDuplicates(["active_province_id"])
        )
        query = query.join(active_provinces, query.province_id == active_provinces.active_province_id, "inner").drop("active_province_id")

    if table_exists(fq_table("dim_province")):
        provinces = (
            spark.table(fq_table("dim_province"))  # type: ignore[name-defined]
            .select("province_id", "province_name")
            .dropDuplicates(["province_id"])
        )
        query = query.join(provinces, "province_id", "left")
    else:
        query = query.withColumn("province_name", F.lit(None).cast("string"))

    query = query.orderBy("location_id")
    max_locations = int_setting(0, "MAX_LOCATIONS", "max_locations")
    if max_locations > 0:
        query = query.limit(max_locations)
    rows = query.collect()
    if not rows:
        raise RuntimeError(table_name + " has no rows with latitude/longitude.")
    return [row.asDict() for row in rows]


# COMMAND ----------

# Meteostat runtime and normalization helpers

def ensure_meteostat_runtime():
    patch_pandas_parse_dates_for_meteostat()
    try:
        from meteostat import Hourly, Point, Stations
        return Hourly, Point, Stations
    except ImportError as exc:
        if bool_setting("AUTO_INSTALL_METEOSTAT"):
            print("meteostat package is missing or incompatible. Installing meteostat==1.6.8 without dependencies...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", "meteostat==1.6.8", "--no-deps"])
            for module_name in list(sys.modules.keys()):
                if module_name == "meteostat" or module_name.startswith("meteostat."):
                    del sys.modules[module_name]
            from meteostat import Hourly, Point, Stations
            return Hourly, Point, Stations
        raise RuntimeError("Install meteostat on the cluster: %pip install meteostat==1.6.8 --no-deps") from exc


def safe_float(value):
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def safe_timestamp(value):
    try:
        parsed = pd.to_datetime(value)
        if pd.isna(parsed):
            return None
        return parsed.to_pydatetime().replace(tzinfo=None)
    except Exception:
        return None


def haversine_km(lat1, lon1, lat2, lon2):
    lat1 = radians(float(lat1))
    lon1 = radians(float(lon1))
    lat2 = radians(float(lat2))
    lon2 = radians(float(lon2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 6371.0 * 2 * asin(sqrt(a))


def row_value(row, names):
    for name in names:
        if name in row.index and not pd.isna(row[name]):
            return row[name]
    return None


def normalize_meteostat_frame(frame, location, fetch_method, station_meta):
    if frame.empty:
        return pd.DataFrame()
    frame = frame.reset_index().rename(
        columns={
            "time": "event_ts",
            "temp": "temperature_c",
            "rhum": "relative_humidity",
            "tavg": "temperature_c",
            "prcp": "precipitation",
            "wspd": "wind_speed",
            "pres": "pressure",
        }
    )
    for column in ["temperature_c", "relative_humidity", "precipitation", "wind_speed", "pressure"]:
        if column not in frame.columns:
            frame[column] = None

    frame["location_id"] = location["location_id"]
    frame["province_id"] = location.get("province_id")
    frame["province_name"] = location.get("province_name")
    frame["event_ts"] = pd.to_datetime(frame["event_ts"], utc=False)
    frame["year"] = frame["event_ts"].dt.year.astype("int32")
    frame["month"] = frame["event_ts"].dt.month.astype("int32")
    frame["ingest_time"] = utc_now()
    frame["source"] = "meteostat"
    frame["fetch_method"] = fetch_method
    frame["station_id"] = station_meta.get("station_id") if station_meta else None
    frame["station_name"] = station_meta.get("station_name") if station_meta else None
    frame["station_distance_km"] = station_meta.get("distance_km") if station_meta else None
    frame["station_latitude"] = station_meta.get("latitude") if station_meta else None
    frame["station_longitude"] = station_meta.get("longitude") if station_meta else None
    frame["distance_km"] = station_meta.get("distance_km") if station_meta else None
    frame["data_quality"] = data_quality_for_distance(
        station_meta.get("distance_km") if station_meta else None,
        fetch_method,
    )
    return frame[
        [
            "location_id",
            "province_id",
            "event_ts",
            "temperature_c",
            "relative_humidity",
            "year",
            "month",
            "ingest_time",
            "source",
            "fetch_method",
            "station_id",
            "station_name",
            "station_distance_km",
            "station_latitude",
            "station_longitude",
            "distance_km",
            "data_quality",
            "province_name",
            "precipitation",
            "wind_speed",
            "pressure",
        ]
    ]


# COMMAND ----------

# Coverage and station metadata helpers

def count_available_metric_columns(frame):
    count = 0
    for column in ["temperature_c", "relative_humidity"]:
        if column in frame.columns and frame[column].notna().any():
            count += 1
    return count


def year_matches_inventory(station_meta, year):
    hourly_start = station_meta.get("hourly_start")
    hourly_end = station_meta.get("hourly_end")
    if hourly_start is None and hourly_end is None:
        return True
    start_ok = hourly_start is None or hourly_start.year <= int(year)
    end_ok = hourly_end is None or hourly_end.year >= int(year)
    return start_ok and end_ok


def expected_hours_for_year(year):
    start_dt = datetime(int(year), 1, 1)
    end_dt = datetime(int(year) + 1, 1, 1)
    return int((end_dt - start_dt).total_seconds() / 3600) + 1


def coverage_ratio(row_count, expected_hours):
    if not expected_hours:
        return 0.0
    return float(row_count or 0) / float(expected_hours)


def station_meta_from_row(location, station_id, row):
    station_lat = safe_float(row_value(row, ["latitude", "lat"]))
    station_lon = safe_float(row_value(row, ["longitude", "lon"]))
    distance = safe_float(row_value(row, ["distance", "distance_km"]))
    if distance is None and station_lat is not None and station_lon is not None:
        distance = haversine_km(location["latitude"], location["longitude"], station_lat, station_lon)
    if distance is not None and distance > 1000:
        distance = distance / 1000.0
    return {
        "station_id": str(station_id),
        "station_name": row_value(row, ["name", "station_name"]),
        "location_id": str(location.get("location_id")),
        "province_id": str(location.get("province_id")) if location.get("province_id") is not None else None,
        "latitude": station_lat,
        "longitude": station_lon,
        "elevation": safe_float(row_value(row, ["elevation", "altitude"])),
        "distance_km": distance,
        "is_primary": False,
        "data_source": "meteostat",
        "hourly_start": safe_timestamp(row_value(row, ["hourly_start"])),
        "hourly_end": safe_timestamp(row_value(row, ["hourly_end"])),
        "created_at": utc_now(),
    }


# COMMAND ----------

# Station metadata table writes

def upsert_weather_stations(station_metas, selected_station_id):
    if not station_metas:
        return
    rows = []
    for meta in station_metas:
        copied = dict(meta)
        copied["is_primary"] = str(copied.get("station_id")) == str(selected_station_id)
        rows.append(
            (
                copied.get("station_id"),
                copied.get("station_name"),
                copied.get("location_id"),
                copied.get("province_id"),
                copied.get("latitude"),
                copied.get("longitude"),
                copied.get("elevation"),
                copied.get("distance_km"),
                copied.get("is_primary"),
                copied.get("data_source"),
                copied.get("hourly_start"),
                copied.get("hourly_end"),
                copied.get("created_at"),
            )
        )
    df = spark.createDataFrame(rows, schema=WEATHER_STATION_SCHEMA)  # type: ignore[name-defined]
    target = DeltaTable.forName(spark, fq_table("dim_weather_station"))  # type: ignore[name-defined]
    (
        target.alias("target")
        .merge(df.alias("source"), "target.station_id = source.station_id AND target.location_id = source.location_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def write_station_mapping(location, year, fetch_meta, row_count, expected_hours, coverage, status):
    row = [
        (
            str(location.get("location_id")),
            str(location.get("province_id")) if location.get("province_id") is not None else None,
            int(year),
            fetch_meta.get("station_id") if fetch_meta else None,
            fetch_meta.get("station_name") if fetch_meta else None,
            fetch_meta.get("fetch_method") if fetch_meta else None,
            int(row_count or 0),
            int(expected_hours or 0),
            float(coverage or 0.0),
            str(status),
            utc_now(),
        )
    ]
    df = spark.createDataFrame(row, schema=STATION_MAPPING_SCHEMA)  # type: ignore[name-defined]
    target = DeltaTable.forName(spark, fq_table("meteostat_station_mapping"))  # type: ignore[name-defined]
    (
        target.alias("target")
        .merge(df.alias("source"), "target.location_id = source.location_id AND target.year = source.year")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

# Meteostat fetch logic

def fetch_nearby_station_year(location, year, start_dt, end_dt):
    Hourly, Point, Stations = ensure_meteostat_runtime()
    max_stations = int_setting(50, "METEOSTAT_MAX_NEARBY_STATIONS")
    radius_meters = int_setting(200000, "METEOSTAT_STATION_RADIUS_METERS")
    max_distance_km = float_setting(200.0, "MAX_STATION_DISTANCE_KM")
    min_coverage = float_setting(0.6, "METEOSTAT_MIN_COVERAGE_RATIO")
    expected_hours = expected_hours_for_year(year)
    radius_meters = max(radius_meters, int(max_distance_km * 1000))
    try:
        try:
            stations = Stations().nearby(float(location["latitude"]), float(location["longitude"]), radius=radius_meters).fetch(max_stations)
        except TypeError:
            stations = Stations().nearby(float(location["latitude"]), float(location["longitude"]), radius_meters).fetch(max_stations)
    except Exception as exc:
        add_collection_warning(
            location["location_id"],
            year,
            "nearby_lookup_failed",
            "nearby station lookup failed for " + str(location["location_id"]) + " year " + str(year) + ": " + str(exc),
        )
        return pd.DataFrame(), None, []
    if stations.empty:
        add_collection_warning(location["location_id"], year, "no_nearby_stations", "no nearby Meteostat stations returned")
        return pd.DataFrame(), None, []

    station_metas = []
    candidates = []
    station_rows = []
    for station_id, station_row in stations.iterrows():
        station_meta = station_meta_from_row(location, str(station_id), station_row)
        station_metas.append(station_meta)
        station_rows.append((station_meta.get("distance_km") if station_meta.get("distance_km") is not None else 999999.0, str(station_id), station_row, station_meta))

    for _, station_id, station_row, station_meta in sorted(station_rows, key=lambda item: item[0]):
        station_id = str(station_id)
        try:
            if not year_matches_inventory(station_meta, year):
                add_collection_warning(
                    location["location_id"],
                    year,
                    "station_inventory_mismatch",
                    "station " + station_id + " has no hourly inventory for year " + str(year),
                    station_id,
                    station_meta.get("distance_km"),
                )
                continue
            distance_km = station_meta.get("distance_km")
            if distance_km is None:
                distance_km = 999999.0
            if float(distance_km) > max_distance_km:
                add_collection_warning(
                    location["location_id"],
                    year,
                    "station_too_far",
                    "station "
                    + station_id
                    + " skipped for "
                    + str(location["location_id"])
                    + " year "
                    + str(year)
                    + ": distance="
                    + str(round(float(distance_km), 1))
                    + " km > "
                    + str(round(max_distance_km, 1))
                    + " km",
                    station_id,
                    distance_km,
                )
                continue
            frame = Hourly(station_id, start_dt, end_dt).fetch()
            normalized = normalize_meteostat_frame(frame, location, "station", station_meta)
            if not normalized.empty:
                row_count = int(len(normalized))
                ratio = coverage_ratio(row_count, expected_hours)
                if ratio < min_coverage:
                    add_collection_warning(
                        location["location_id"],
                        year,
                        "station_low_coverage",
                        "station "
                        + station_id
                        + " skipped for "
                        + str(location["location_id"])
                        + " year "
                        + str(year)
                        + ": coverage="
                        + str(round(ratio * 100.0, 1))
                        + "% < "
                        + str(round(min_coverage * 100.0, 1))
                        + "%",
                        station_id,
                        distance_km,
                    )
                    continue
                candidates.append(
                    {
                        "frame": normalized,
                        "station_meta": station_meta,
                        "row_count": row_count,
                        "expected_hours": expected_hours,
                        "coverage_ratio": ratio,
                        "distance_km": float(distance_km),
                        "metric_count": count_available_metric_columns(normalized),
                    }
                )
            else:
                add_collection_warning(
                    location["location_id"],
                    year,
                    "station_no_rows",
                    "station " + station_id + " returned no hourly rows for year " + str(year),
                    station_id,
                    distance_km,
                )
        except Exception as exc:
            add_collection_warning(
                location["location_id"],
                year,
                "station_failed",
                "station " + station_id + " failed for " + str(location["location_id"]) + " year " + str(year) + ": " + str(exc),
                station_id,
                station_meta.get("distance_km"),
            )
    if not candidates:
        return pd.DataFrame(), None, station_metas
    best = sorted(candidates, key=lambda item: (item["distance_km"], -item["row_count"], -item["metric_count"]))[0]
    selected_meta = dict(best["station_meta"])
    selected_meta["fetch_method"] = "station"
    selected_meta["expected_hours"] = int(best["expected_hours"])
    selected_meta["coverage_ratio"] = float(best["coverage_ratio"])
    return best["frame"], selected_meta, station_metas


# COMMAND ----------

# Location-year fetch fallback

def fetch_location_year(location, year):
    Hourly, Point, Stations = ensure_meteostat_runtime()
    altitude = location.get("altitude")
    if altitude is None:
        point = Point(float(location["latitude"]), float(location["longitude"]))
    else:
        point = Point(float(location["latitude"]), float(location["longitude"]), float(altitude))

    start_dt = datetime(int(year), 1, 1)
    end_dt = datetime(int(year) + 1, 1, 1)
    expected_hours = expected_hours_for_year(year)
    min_coverage = float_setting(0.6, "METEOSTAT_MIN_COVERAGE_RATIO")
    point_meta = {
        "fetch_method": "point",
        "station_id": None,
        "station_name": None,
        "distance_km": None,
        "latitude": None,
        "longitude": None,
        "expected_hours": expected_hours,
        "coverage_ratio": 0.0,
    }
    try:
        frame = Hourly(point, start_dt, end_dt).fetch()
        normalized = normalize_meteostat_frame(frame, location, "point", point_meta)
    except Exception as exc:
        add_collection_warning(
            location["location_id"],
            year,
            "point_fetch_failed",
            "point fetch failed for " + str(location["location_id"]) + " year " + str(year) + ": " + str(exc),
        )
        normalized = pd.DataFrame()
    if not normalized.empty:
        point_meta["coverage_ratio"] = coverage_ratio(len(normalized), expected_hours)
    if not normalized.empty and point_meta["coverage_ratio"] >= min_coverage:
        return normalized, point_meta, []

    station_frame, station_meta, station_metas = fetch_nearby_station_year(location, year, start_dt, end_dt)
    if station_meta and not station_frame.empty:
        return station_frame, station_meta, station_metas
    if not normalized.empty:
        add_collection_warning(
            location["location_id"],
            year,
            "using_partial_point_data",
            "no station met coverage/distance constraints for "
            + str(location["location_id"])
            + " year "
            + str(year)
            + "; keeping partial point data with "
            + str(len(normalized))
            + " rows, coverage="
            + str(round(point_meta["coverage_ratio"] * 100.0, 1))
            + "%.",
        )
        return normalized, point_meta, station_metas
    return pd.DataFrame(), {"fetch_method": None, "station_id": None, "station_name": None, "distance_km": None, "latitude": None, "longitude": None, "expected_hours": expected_hours, "coverage_ratio": 0.0}, station_metas


# COMMAND ----------

# Status and collection summary helpers

def sql_string(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def latest_status(location_id, year):
    status_table = fq_table("meteostat_sync_status")
    if not table_exists(status_table):
        return None
    rows = (
        spark.table(status_table)  # type: ignore[name-defined]
        .where((F.col("location_id") == str(location_id)) & (F.col("year") == int(year)))
        .orderBy(F.col("finished_at").desc_nulls_last())
        .limit(1)
        .collect()
    )
    if not rows:
        return None
    return rows[0].asDict()


def should_skip(location, year):
    if bool_setting("OVERWRITE_EXISTING", "overwrite_existing"):
        return False, ""
    status = latest_status(location.get("location_id"), year)
    if bool_setting("RETRY_FAILED_ONLY", "retry_failed_only"):
        if status and status.get("status") == "failed":
            return False, ""
        return True, "retry_failed_only enabled and latest status is not failed"
    if status and status.get("status") == "success":
        return True, "already synced successfully"
    return False, ""


def write_status(location, year, fetch_meta, status, row_count, expected_hours, coverage, error_message, started_at, finished_at):
    row = [
        (
            str(location.get("location_id")),
            str(location.get("province_id")) if location.get("province_id") is not None else None,
            int(year),
            fetch_meta.get("fetch_method") if fetch_meta else None,
            fetch_meta.get("station_id") if fetch_meta else None,
            fetch_meta.get("station_name") if fetch_meta else None,
            fetch_meta.get("distance_km") if fetch_meta else None,
            int(expected_hours or 0),
            float(coverage or 0.0),
            str(status),
            int(row_count or 0),
            str(error_message)[:4000] if error_message else None,
            started_at,
            finished_at,
        )
    ]
    df = spark.createDataFrame(row, schema=STATUS_SCHEMA)  # type: ignore[name-defined]
    status_table = fq_table("meteostat_sync_status")
    target = DeltaTable.forName(spark, status_table)  # type: ignore[name-defined]
    (
        target.alias("target")
        .merge(df.alias("source"), "target.location_id = source.location_id AND target.year = source.year")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def warning_count_for(location_id, year):
    return sum(
        1
        for warning in COLLECTION_WARNINGS
        if warning.get("location_id") == str(location_id) and int(warning.get("year") or 0) == int(year)
    )


def add_summary_row(location, year, fetch_meta, status, row_count, expected_hours, coverage, error_message=None):
    COLLECTION_SUMMARY_ROWS.append(
        (
            str(location.get("location_id")),
            str(location.get("province_id")) if location.get("province_id") is not None else None,
            str(location.get("province_name")) if location.get("province_name") is not None else None,
            int(year),
            str(status),
            int(row_count or 0),
            int(expected_hours or 0),
            float(coverage or 0.0),
            fetch_meta.get("fetch_method") if fetch_meta else None,
            fetch_meta.get("station_id") if fetch_meta else None,
            fetch_meta.get("station_name") if fetch_meta else None,
            float(fetch_meta.get("distance_km")) if fetch_meta and fetch_meta.get("distance_km") is not None else None,
            data_quality_for_distance(
                fetch_meta.get("distance_km") if fetch_meta else None,
                fetch_meta.get("fetch_method") if fetch_meta else None,
            ),
            int(warning_count_for(location.get("location_id"), year)),
            str(error_message)[:4000] if error_message else None,
            utc_now(),
        )
    )


def write_collection_summary():
    if not COLLECTION_SUMMARY_ROWS:
        return
    summary_table = fq_table("meteostat_collection_summary")
    df = spark.createDataFrame(COLLECTION_SUMMARY_ROWS, schema=COLLECTION_SUMMARY_SCHEMA)  # type: ignore[name-defined]
    years = sorted({row[3] for row in COLLECTION_SUMMARY_ROWS})
    location_ids = sorted({row[0] for row in COLLECTION_SUMMARY_ROWS})
    spark.sql(
        "DELETE FROM "
        + summary_table
        + " WHERE year IN ("
        + ", ".join(str(int(year)) for year in years)
        + ") AND location_id IN ("
        + ", ".join(sql_string(location_id) for location_id in location_ids)
        + ")"
    )  # type: ignore[name-defined]
    df.write.format("delta").mode("append").saveAsTable(summary_table)


def print_collection_summary():
    max_distance_km = float_setting(200.0, "MAX_STATION_DISTANCE_KM")
    successful_pairs = {
        (row[0], row[3])
        for row in COLLECTION_SUMMARY_ROWS
        if row[4] in ("success", "partial") and int(row[5] or 0) > 0
    }
    failed_rows = [row for row in COLLECTION_SUMMARY_ROWS if row[4] not in ("success", "partial") or int(row[5] or 0) == 0]
    failed_by_location = {}
    for row in failed_rows:
        failed_by_location[row[0]] = failed_by_location.get(row[0], 0) + 1
    too_far = [warning for warning in COLLECTION_WARNINGS if warning.get("warning_type") == "station_too_far"]
    print("Meteostat collection summary:")
    print("  Successful location-years: " + str(len(successful_pairs)))
    print("  Missing location-years: " + str(len(failed_rows)))
    print("  Missing locations: " + str(len(failed_by_location)))
    if failed_by_location:
        top_missing = sorted(failed_by_location.items(), key=lambda item: (-item[1], item[0]))[:10]
        print("  Top missing locations: " + ", ".join([item[0] + "=" + str(item[1]) for item in top_missing]))
    if too_far:
        top_too_far = sorted(
            too_far,
            key=lambda item: (item.get("location_id") or "", item.get("year") or 0, item.get("distance_km") or 0),
        )[:10]
        print(
            "  Stations skipped because too far (>"
            + str(round(max_distance_km, 1))
            + " km): "
            + ", ".join(
                [
                    str(item.get("location_id"))
                    + "/"
                    + str(item.get("year"))
                    + "/"
                    + str(item.get("station_id"))
                    + "="
                    + str(round(float(item.get("distance_km") or 0.0), 1))
                    + "km"
                    for item in top_too_far
                ]
            )
        )


# COMMAND ----------

# Bronze batch writes

def write_location_year_batch(df, target_table, location_id, year):
    if df.limit(1).count() == 0:
        return 0
    if not table_exists(target_table):
        raise RuntimeError(target_table + " does not exist. Run 01_create_catalog_schema_tables.sql before file 04.")

    target_schema = spark.table(target_table).schema  # type: ignore[name-defined]
    for field in target_schema:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
    df = df.select([F.col(field.name).cast(field.dataType).alias(field.name) for field in target_schema])

    spark.sql(
        "DELETE FROM "
        + target_table
        + " WHERE location_id = "
        + sql_string(location_id)
        + " AND year = "
        + str(int(year))
    )  # type: ignore[name-defined]
    df.coalesce(1).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table)
    return df.count()


# COMMAND ----------

# Main execution

def main():
    create_widgets()
    ensure_namespace_and_tables()
    locations = load_locations()
    years = configured_years()
    target_table = fq_table("bronze_meteostat_hourly")
    total_synced_rows = 0

    print_active_config()
    print("Syncing Meteostat for " + str(len(locations)) + " locations, years " + str(years[0]) + "-" + str(years[-1]) + ".")
    for location in locations:
        for year in years:
            started_at = utc_now()
            skip, skip_reason = should_skip(location, year)
            if skip:
                latest = latest_status(location.get("location_id"), year) or {}
                debug_print("Skipped " + str(location["location_id"]) + " year " + str(year) + ": " + skip_reason)
                if not latest:
                    write_status(location, year, None, "skipped", 0, expected_hours_for_year(year), 0.0, skip_reason, started_at, utc_now())
                    add_summary_row(location, year, None, "skipped", 0, expected_hours_for_year(year), 0.0, skip_reason)
                continue
            try:
                frame, fetch_meta, station_metas = fetch_location_year(location, year)
                upsert_weather_stations(station_metas, fetch_meta.get("station_id") if fetch_meta else None)
                expected_hours = int(fetch_meta.get("expected_hours") or expected_hours_for_year(year)) if fetch_meta else expected_hours_for_year(year)
                if frame.empty:
                    error_message = "Meteostat returned no rows from point or nearby stations within constraints"
                    write_status(location, year, fetch_meta, "failed", 0, expected_hours, 0.0, error_message, started_at, utc_now())
                    write_station_mapping(location, year, fetch_meta, 0, expected_hours, 0.0, "failed")
                    add_collection_warning(location["location_id"], year, "no_meteostat_rows", "no Meteostat rows for " + str(location["location_id"]) + " year " + str(year))
                    add_summary_row(location, year, fetch_meta, "failed", 0, expected_hours, 0.0, error_message)
                    continue
                spark_frame = spark.createDataFrame(frame, schema=BRONZE_SCHEMA).dropDuplicates(["location_id", "event_ts"])  # type: ignore[name-defined]
                row_count = write_location_year_batch(spark_frame, target_table, location["location_id"], year)
                coverage = coverage_ratio(row_count, expected_hours)
                status = "success" if coverage >= float_setting(0.6, "METEOSTAT_MIN_COVERAGE_RATIO") else "partial"
                total_synced_rows += row_count
                write_status(location, year, fetch_meta, status, row_count, expected_hours, coverage, None, started_at, utc_now())
                write_station_mapping(location, year, fetch_meta, row_count, expected_hours, coverage, status)
                add_summary_row(location, year, fetch_meta, status, row_count, expected_hours, coverage)
                print(
                    "Synced "
                    + str(row_count)
                    + " rows for "
                    + str(location["location_id"])
                    + " year "
                    + str(year)
                    + " via "
                    + str(fetch_meta.get("fetch_method") if fetch_meta else None)
                    + ((" station " + str(fetch_meta.get("station_id"))) if fetch_meta and fetch_meta.get("station_id") else "")
                    + ", distance="
                    + (str(round(float(fetch_meta.get("distance_km")), 1)) + " km" if fetch_meta and fetch_meta.get("distance_km") is not None else "null")
                    + ", coverage="
                    + str(round(coverage * 100.0, 1))
                    + "%"
                )
            except Exception as exc:
                expected_hours = expected_hours_for_year(year)
                write_status(location, year, None, "failed", 0, expected_hours, 0.0, str(exc), started_at, utc_now())
                write_station_mapping(location, year, None, 0, expected_hours, 0.0, "failed")
                add_collection_warning(
                    location["location_id"],
                    year,
                    "location_year_failed",
                    "failed Meteostat fetch for " + str(location["location_id"]) + " year " + str(year) + ": " + str(exc),
                )
                add_summary_row(location, year, None, "failed", 0, expected_hours, 0.0, str(exc))

    if total_synced_rows == 0 or not table_exists(target_table):
        print("WARNING: no Meteostat rows were written. Check dim_location, internet access, Meteostat availability, and sync status.")
        write_collection_summary()
        print_collection_summary()
        return

    write_collection_summary()
    print_collection_summary()
    spark.sql("OPTIMIZE " + target_table + " ZORDER BY (location_id, event_ts)")  # type: ignore[name-defined]
    print("Meteostat sync complete. Rows written this run: " + str(total_synced_rows))


# COMMAND ----------

main()
