"""Databricks Bronze writer for IoT sensor readings."""

from __future__ import annotations

from contextlib import contextmanager

from iot_backend import config


class DatabricksWriteSkipped(RuntimeError):
    pass


def _table_name() -> str:
    return f"{config.DATABRICKS_CATALOG}.{config.DATABRICKS_SCHEMA}.{config.DATABRICKS_BRONZE_TABLE}"


def _is_configured() -> bool:
    return bool(
        config.DATABRICKS_ENABLED
        and config.DATABRICKS_SERVER_HOSTNAME
        and config.DATABRICKS_HTTP_PATH
        and config.DATABRICKS_TOKEN
    )


@contextmanager
def _connect():
    if not _is_configured():
        raise DatabricksWriteSkipped("Databricks is disabled or missing connection settings")
    try:
        from databricks import sql
    except ImportError as exc:
        raise DatabricksWriteSkipped("Missing dependency: databricks-sql-connector") from exc

    connection = sql.connect(
        server_hostname=config.DATABRICKS_SERVER_HOSTNAME,
        http_path=config.DATABRICKS_HTTP_PATH,
        access_token=config.DATABRICKS_TOKEN,
    )
    try:
        yield connection
    finally:
        connection.close()


def write_bronze_sensor_reading(reading: dict) -> tuple[bool, str]:
    """Insert one sensor-level reading into Databricks Bronze.

    This follows the requested style A: backend writes directly to Databricks.
    The caller should treat failures as non-fatal and keep PostgreSQL as the
    operational source of truth for realtime UX.
    """
    if not _is_configured():
        return False, "disabled"

    table = _table_name()
    sql_text = f"""
        INSERT INTO {table} (
          sensor_id, event_ts, temperature, humidity, temperature_unit,
          humidity_unit, source_type, provider, environment_type, location,
          location_province, latitude, longitude, ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp())
    """
    try:
        with _connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql_text,
                    (
                        reading.get("sensor_id"),
                        reading.get("event_ts"),
                        reading.get("temperature"),
                        reading.get("humidity"),
                        reading.get("temperature_unit") or "C",
                        reading.get("humidity_unit") or "%",
                        reading.get("source_type") or "physical_iot",
                        reading.get("provider") or "esp32",
                        reading.get("environment_type") or "indoor",
                        reading.get("location"),
                        reading.get("location_province"),
                        reading.get("latitude"),
                        reading.get("longitude"),
                    ),
                )
        return True, "written"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"
