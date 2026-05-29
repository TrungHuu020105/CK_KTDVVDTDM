"""Databricks SQL helper for Lakehouse result tables.

The project intentionally keeps model training inside Databricks notebooks/jobs.
Backend services only write Bronze sensor readings and read forecast/evaluation
results for the dashboard.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterable

from app import config


class DatabricksUnavailable(RuntimeError):
    pass


def _table_name(table: str) -> str:
    return f"{config.DATABRICKS_CATALOG}.{config.DATABRICKS_SCHEMA}.{table}"


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
        raise DatabricksUnavailable("Databricks is disabled or missing connection settings")
    try:
        from databricks import sql
    except ImportError as exc:
        raise DatabricksUnavailable("Missing dependency: databricks-sql-connector") from exc

    connection = sql.connect(
        server_hostname=config.DATABRICKS_SERVER_HOSTNAME,
        http_path=config.DATABRICKS_HTTP_PATH,
        access_token=config.DATABRICKS_TOKEN,
    )
    try:
        yield connection
    finally:
        connection.close()


def _rows_to_dicts(cursor) -> list[dict]:
    columns = [col[0] for col in cursor.description or []]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


class DatabricksService:
    @staticmethod
    def status() -> dict:
        return {
            "enabled": bool(config.DATABRICKS_ENABLED),
            "configured": _is_configured(),
            "catalog": config.DATABRICKS_CATALOG,
            "schema": config.DATABRICKS_SCHEMA,
            "bronze_table": _table_name(config.DATABRICKS_BRONZE_TABLE),
            "forecast_table": _table_name(config.DATABRICKS_FORECAST_TABLE),
            "evaluation_table": _table_name(config.DATABRICKS_EVALUATION_TABLE),
        }

    @staticmethod
    def fetch_forecast(sensor_id: str, limit: int = 200) -> dict:
        if not _is_configured():
            return {"enabled": False, "sensor_id": sensor_id, "forecasts": []}
        table = _table_name(config.DATABRICKS_FORECAST_TABLE)
        query = f"""
            SELECT *
            FROM {table}
            WHERE sensor_id = ?
            ORDER BY forecast_ts ASC
            LIMIT ?
        """
        with _connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (sensor_id, int(limit)))
                return {"enabled": True, "sensor_id": sensor_id, "forecasts": _rows_to_dicts(cursor)}

    @staticmethod
    def fetch_model_leaderboard(sensor_id: str, limit: int = 50) -> dict:
        if not _is_configured():
            return {"enabled": False, "sensor_id": sensor_id, "models": []}
        table = _table_name(config.DATABRICKS_EVALUATION_TABLE)
        query = f"""
            SELECT *
            FROM {table}
            WHERE sensor_id = ?
            ORDER BY is_best DESC, target ASC, rmse ASC
            LIMIT ?
        """
        with _connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (sensor_id, int(limit)))
                return {"enabled": True, "sensor_id": sensor_id, "models": _rows_to_dicts(cursor)}
