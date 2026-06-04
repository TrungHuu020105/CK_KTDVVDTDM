"""Databricks SQL helper for Lakehouse result tables."""

from __future__ import annotations

import os
import re
import unicodedata
from contextlib import contextmanager
from threading import Lock
from time import monotonic

from app import config
from app.database import SessionLocal
from app.models import IoTDevice


class DatabricksUnavailable(RuntimeError):
    pass


_FORECAST_CACHE_TTL_SECONDS = max(60, int(os.getenv("FORECAST_CACHE_TTL_SECONDS", "300")))
_FORECAST_CACHE: dict[tuple[str, int], tuple[float, dict]] = {}
_FORECAST_CACHE_LOCK = Lock()


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


def _query_dicts(query: str, params: tuple) -> list[dict]:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            return _rows_to_dicts(cursor)


def _normalized_candidates(*values: str | None) -> list[str]:
    seen: set[str] = set()
    candidates: list[str] = []
    for value in values:
        text = str(value or "").strip().lower()
        if not text or text in seen:
            continue
        seen.add(text)
        candidates.append(text)
    return candidates


def _slugify(text: str | None) -> str:
    raw = str(text or "").strip().lower()
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKD", raw)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "", ascii_text)
    return slug


def _text_location_variants(text: str | None) -> list[str]:
    raw = str(text or "").strip()
    if not raw:
        return []
    variants = {
        raw,
        raw.split(",")[0].strip(),
        re.sub(r"\([^)]*\)", "", raw).strip(),
        re.sub(r",\s*vietnam\b", "", raw, flags=re.IGNORECASE).strip(),
    }
    return _normalized_candidates(*variants)


def _location_text_candidates(*values: str | None) -> list[str]:
    expanded: list[str] = []
    for value in values:
        expanded.extend(_text_location_variants(value))
    return _normalized_candidates(*expanded)


def _location_slug_candidates(*values: str | None) -> list[str]:
    expanded: list[str] = []
    for value in values:
        for variant in _text_location_variants(value):
            slug = _slugify(variant)
            if not slug:
                continue
            expanded.extend([slug, f"loc_{slug}", f"loc34_{slug}"])
    return _normalized_candidates(*expanded)


def _context_from_metadata(sensor_id: str, metadata: dict | None) -> dict | None:
    if not isinstance(metadata, dict) or not metadata:
        return None

    remote_source = metadata.get("source") or metadata.get("sensor_id") or sensor_id
    direct_candidates = _normalized_candidates(
        sensor_id,
        str(metadata.get("id") or "").strip(),
        remote_source,
        metadata.get("name"),
    )
    location_text_candidates = _location_text_candidates(
        metadata.get("location"),
        metadata.get("location_query"),
        metadata.get("location_province"),
    )
    location_slug_candidates = _location_slug_candidates(
        metadata.get("location"),
        metadata.get("location_query"),
        metadata.get("location_province"),
    )
    location_candidates = _normalized_candidates(
        *direct_candidates,
        *location_text_candidates,
        *location_slug_candidates,
    )
    source_type = metadata.get("source_type")
    environment_type = metadata.get("environment_type")
    prefer_location = True
    if str(source_type or "").strip().lower() == "virtual_meteostat":
        prefer_location = True
    elif str(environment_type or "").strip().lower() == "outdoor":
        prefer_location = True

    return {
        "sensor_id": sensor_id,
        "device_pk": str(metadata.get("id")) if metadata.get("id") is not None else None,
        "source_type": source_type,
        "environment_type": environment_type,
        "direct_candidates": direct_candidates,
        "location_candidates": location_candidates,
        "location_text_candidates": location_text_candidates,
        "location_slug_candidates": location_slug_candidates,
        "prefer_location": prefer_location,
    }


def _sensor_context(sensor_id: str, sensor_metadata: dict | None = None) -> dict:
    metadata_context = _context_from_metadata(sensor_id, sensor_metadata)
    if metadata_context:
        return metadata_context

    db = SessionLocal()
    try:
        device = (
            db.query(IoTDevice)
            .filter(IoTDevice.source == sensor_id)
            .order_by(IoTDevice.id.asc())
            .first()
        )
        if not device:
            return {
                "sensor_id": sensor_id,
                "device_pk": None,
                "source_type": None,
                "environment_type": None,
                "direct_candidates": _normalized_candidates(sensor_id),
                "location_candidates": _normalized_candidates(sensor_id, *_location_text_candidates(sensor_id), *_location_slug_candidates(sensor_id)),
                "location_text_candidates": _location_text_candidates(sensor_id),
                "location_slug_candidates": _location_slug_candidates(sensor_id),
                "prefer_location": True,
            }
        direct_candidates = _normalized_candidates(sensor_id, str(device.id), device.source, device.name)
        location_text_candidates = _location_text_candidates(device.location, device.location_query, device.location_province)
        location_slug_candidates = _location_slug_candidates(device.location, device.location_query, device.location_province)
        location_candidates = _normalized_candidates(
            *direct_candidates,
            *location_text_candidates,
            *location_slug_candidates,
        )
        prefer_location = True
        if str(device.source_type or "").strip().lower() == "virtual_meteostat":
            prefer_location = True
        elif str(device.environment_type or "").strip().lower() == "outdoor":
            prefer_location = True
        return {
            "sensor_id": sensor_id,
            "device_pk": str(device.id),
            "source_type": device.source_type,
            "environment_type": device.environment_type,
            "direct_candidates": direct_candidates,
            "location_candidates": location_candidates,
            "location_text_candidates": location_text_candidates,
            "location_slug_candidates": location_slug_candidates,
            "prefer_location": prefer_location,
        }
    finally:
        db.close()


def _query_grouped_forecast(from_clause: str, condition_sql: str, params: tuple, limit: int) -> list[dict]:
    grouped_query = f"""
        WITH matched AS (
          SELECT
            fr.forecast_timestamp,
            fr.metric_type,
            COALESCE(fr.calibrated_forecast_value, fr.forecast_value) AS forecast_value,
            fr.generated_at,
            fr.model_name,
            fr.model_type
          FROM {from_clause}
          WHERE {condition_sql}
        ),
        latest_per_metric AS (
          SELECT metric_type, MAX(generated_at) AS generated_at
          FROM matched
          GROUP BY metric_type
        )
        SELECT
          m.forecast_timestamp AS forecast_ts,
          MAX(CASE WHEN m.metric_type = 'temperature' THEN m.forecast_value END) AS temperature,
          MAX(CASE WHEN m.metric_type = 'humidity' THEN m.forecast_value END) AS humidity,
          MAX(m.generated_at) AS generated_at,
          MAX(CASE WHEN m.metric_type = 'temperature' THEN m.model_name END) AS temperature_model_name,
          MAX(CASE WHEN m.metric_type = 'humidity' THEN m.model_name END) AS humidity_model_name,
          MAX(CASE WHEN m.metric_type = 'temperature' THEN m.model_type END) AS temperature_model_type,
          MAX(CASE WHEN m.metric_type = 'humidity' THEN m.model_type END) AS humidity_model_type
        FROM matched m
        INNER JOIN latest_per_metric latest
          ON latest.metric_type = m.metric_type
         AND latest.generated_at = m.generated_at
        GROUP BY m.forecast_timestamp
        ORDER BY m.forecast_timestamp ASC
        LIMIT ?
    """
    return _query_dicts(grouped_query, params + (int(limit),))


def _fallback_forecast_query(sensor_id: str, limit: int) -> list[dict]:
    table = _table_name(config.DATABRICKS_FORECAST_TABLE)
    query = f"""
        SELECT
          forecast_ts,
          MAX(CASE WHEN metric_type = 'temperature' THEN y_pred END) AS temperature,
          MAX(CASE WHEN metric_type = 'humidity' THEN y_pred END) AS humidity
        FROM {table}
        WHERE sensor_id = ?
        GROUP BY forecast_ts
        ORDER BY forecast_ts ASC
        LIMIT ?
    """
    return _query_dicts(query, (sensor_id, int(limit)))


def _resolve_forecast_rows(sensor_id: str, limit: int, sensor_metadata: dict | None = None) -> list[dict]:
    context = _sensor_context(sensor_id, sensor_metadata=sensor_metadata)
    forecast_table = _table_name("gold_forecast_result")
    location_table = _table_name("dim_location")

    location_attempts: list[tuple[str, str, tuple]] = []
    direct_attempts: list[tuple[str, str, tuple]] = []

    direct_candidates = context.get("direct_candidates") or []
    if direct_candidates:
        placeholders = ", ".join("?" for _ in direct_candidates)
        direct_attempts.append(
            (
                f"{forecast_table} fr",
                "("
                f"LOWER(COALESCE(fr.device_id, '')) IN ({placeholders}) "
                f"OR LOWER(COALESCE(fr.location_id, '')) IN ({placeholders})"
                ")",
                tuple(direct_candidates * 2),
            )
        )

    location_text_candidates = context.get("location_text_candidates") or []
    location_slug_candidates = context.get("location_slug_candidates") or []
    if location_text_candidates or location_slug_candidates:
        text_placeholders = ", ".join("?" for _ in location_text_candidates) if location_text_candidates else ""
        slug_placeholders = ", ".join("?" for _ in location_slug_candidates) if location_slug_candidates else ""
        clauses: list[str] = []
        params: list[str] = []
        if location_text_candidates:
            clauses.extend([
                f"LOWER(COALESCE(fr.location_id, '')) IN ({text_placeholders})",
                f"LOWER(COALESCE(dl.location_id, '')) IN ({text_placeholders})",
                f"LOWER(COALESCE(dl.location_name, '')) IN ({text_placeholders})",
                f"LOWER(COALESCE(dl.province_id, '')) IN ({text_placeholders})",
            ])
            params.extend(location_text_candidates * 4)
        if location_slug_candidates:
            clauses.extend([
                f"REGEXP_REPLACE(LOWER(COALESCE(fr.location_id, '')), '[^a-z0-9]', '') IN ({slug_placeholders})",
                f"REGEXP_REPLACE(LOWER(COALESCE(dl.location_id, '')), '[^a-z0-9]', '') IN ({slug_placeholders})",
                f"REGEXP_REPLACE(LOWER(COALESCE(dl.location_name, '')), '[^a-z0-9]', '') IN ({slug_placeholders})",
                f"REGEXP_REPLACE(LOWER(COALESCE(dl.province_id, '')), '[^a-z0-9]', '') IN ({slug_placeholders})",
            ])
            params.extend(location_slug_candidates * 4)
        location_attempts.append(
            (
                f"{forecast_table} fr LEFT JOIN {location_table} dl ON dl.location_id = fr.location_id",
                "(" + " OR ".join(clauses) + ")",
                tuple(params),
            )
        )

    attempts = location_attempts + direct_attempts if context.get("prefer_location", True) else direct_attempts + location_attempts

    for from_clause, condition_sql, params in attempts:
        try:
            rows = _query_grouped_forecast(from_clause, condition_sql, params, limit)
            if rows:
                return rows
        except Exception:
            continue

    try:
        return _fallback_forecast_query(sensor_id, limit)
    except Exception:
        return []


def _query_model_rows(query: str, params: tuple) -> list[dict]:
    rows = _query_dicts(query, params)
    normalized_rows: list[dict] = []
    for row in rows:
        normalized_rows.append(
            {
                "model_name": row.get("model_name"),
                "model_type": row.get("model_type"),
                "training_mode": row.get("training_mode"),
                "model_scope": row.get("model_scope"),
                "location_id": row.get("location_id"),
                "location_name": row.get("location_name"),
                "province_id": row.get("province_id"),
                "location_count": row.get("location_count"),
                "target_variable": row.get("target_variable") or row.get("metric_type") or row.get("target"),
                "mae": row.get("mae"),
                "rmse": row.get("rmse"),
                "rse": row.get("rse"),
                "mape": row.get("mape"),
                "r2": row.get("r2"),
                "forecast_horizon_hours": row.get("forecast_horizon_hours") or row.get("horizon_hours") or row.get("horizon"),
                "input_window_hours": row.get("input_window_hours"),
                "model_uri": row.get("model_uri"),
                "mlflow_run_id": row.get("mlflow_run_id") or row.get("run_id"),
                "train_rows": row.get("train_rows") or row.get("training_rows") or row.get("training_points"),
                "test_rows": row.get("test_rows") or row.get("test_points"),
                "is_best": row.get("is_best"),
                "created_at": row.get("created_at"),
                "rank_order": row.get("rank_order"),
                "sensor_id": row.get("sensor_id"),
            }
        )
    return normalized_rows


def _resolve_location_reference_ids(context: dict) -> tuple[list[str], list[str]]:
    location_table = _table_name("dim_location")
    text_candidates = context.get("location_text_candidates") or []
    slug_candidates = context.get("location_slug_candidates") or []
    clauses: list[str] = []
    params: list[str] = []

    if text_candidates:
        placeholders = ", ".join("?" for _ in text_candidates)
        clauses.extend(
            [
                f"LOWER(COALESCE(location_id, '')) IN ({placeholders})",
                f"LOWER(COALESCE(location_name, '')) IN ({placeholders})",
                f"LOWER(COALESCE(province_id, '')) IN ({placeholders})",
            ]
        )
        params.extend(text_candidates * 3)

    if slug_candidates:
        placeholders = ", ".join("?" for _ in slug_candidates)
        clauses.extend(
            [
                f"REGEXP_REPLACE(LOWER(COALESCE(location_id, '')), '[^a-z0-9]', '') IN ({placeholders})",
                f"REGEXP_REPLACE(LOWER(COALESCE(location_name, '')), '[^a-z0-9]', '') IN ({placeholders})",
                f"REGEXP_REPLACE(LOWER(COALESCE(province_id, '')), '[^a-z0-9]', '') IN ({placeholders})",
            ]
        )
        params.extend(slug_candidates * 3)

    if not clauses:
        return [], []

    query = f"""
        SELECT DISTINCT location_id, province_id
        FROM {location_table}
        WHERE {" OR ".join(clauses)}
    """
    try:
        rows = _query_dicts(query, tuple(params))
    except Exception:
        return [], []

    location_ids = _normalized_candidates(*(row.get("location_id") for row in rows))
    province_ids = _normalized_candidates(*(row.get("province_id") for row in rows))
    return location_ids, province_ids


def _resolve_model_rows(sensor_id: str, limit: int, sensor_metadata: dict | None = None) -> list[dict]:
    context = _sensor_context(sensor_id, sensor_metadata=sensor_metadata)
    evaluation_table = _table_name(config.DATABRICKS_EVALUATION_TABLE)
    leaderboard_table = _table_name("model_leaderboard")
    location_table = _table_name("dim_location")

    direct_candidates = context.get("direct_candidates") or []
    location_ids, province_ids = _resolve_location_reference_ids(context)

    attempts: list[tuple[str, tuple]] = []
    direct_query: str | None = None

    if direct_candidates:
        placeholders = ", ".join("?" for _ in direct_candidates)
        direct_query = f"""
            SELECT
              model_name,
              model_type,
              training_mode,
              model_scope,
              location_id,
              CAST(NULL AS STRING) AS location_name,
              CAST(NULL AS STRING) AS province_id,
              CAST(NULL AS INT) AS location_count,
              metric_type AS target_variable,
              mae,
              rmse,
              rse,
              mape,
              r2,
              CAST(NULL AS INT) AS forecast_horizon_hours,
              CAST(NULL AS INT) AS input_window_hours,
              model_uri,
              mlflow_run_id,
              training_rows AS train_rows,
              test_rows AS test_rows,
              is_best,
              created_at,
              CAST(NULL AS INT) AS rank_order,
              sensor_id
            FROM {evaluation_table}
            WHERE LOWER(COALESCE(sensor_id, '')) IN ({placeholders})
            ORDER BY metric_type ASC, is_best DESC, rmse ASC NULLS LAST, mae ASC NULLS LAST, created_at DESC
            LIMIT ?
        """
        attempts.append((direct_query, tuple(direct_candidates) + (int(limit),)))

    clauses: list[str] = []
    params: list[str] = []

    if location_ids:
        placeholders = ", ".join("?" for _ in location_ids)
        clauses.append(f"LOWER(COALESCE(ml.location_id, '')) IN ({placeholders})")
        params.extend(location_ids)

    if province_ids:
        placeholders = ", ".join("?" for _ in province_ids)
        clauses.append(f"LOWER(COALESCE(ml.province_id, '')) IN ({placeholders})")
        params.extend(province_ids)

    if clauses:
        location_query = f"""
            SELECT
              ml.model_name,
              ml.model_type,
              ml.training_mode,
              ml.model_scope,
              ml.location_id,
              dl.location_name,
              ml.province_id,
              ml.location_count,
              ml.target_variable,
              ml.mae,
              ml.rmse,
              ml.rse,
              ml.mape,
              ml.r2,
              ml.forecast_horizon_hours,
              ml.input_window_hours,
              ml.model_uri,
              ml.mlflow_run_id,
              ml.train_rows,
              ml.test_rows,
              ml.is_best,
              ml.created_at,
              CAST(NULL AS INT) AS rank_order,
              CAST(NULL AS STRING) AS sensor_id
            FROM {leaderboard_table} ml
            LEFT JOIN {location_table} dl ON dl.location_id = ml.location_id
            WHERE {" OR ".join(clauses)}
            ORDER BY
              ml.target_variable ASC,
              ml.is_best DESC,
              ml.rmse ASC NULLS LAST,
              ml.mae ASC NULLS LAST,
              ml.created_at DESC
            LIMIT ?
        """
        attempts.append((location_query, tuple(params) + (int(limit),)))

    for query, params in attempts:
        try:
            rows = _query_model_rows(query, params)
            if rows:
                if direct_query and query == direct_query:
                    return rows
                try:
                    global_query = f"""
                        SELECT
                          ml.model_name,
                          ml.model_type,
                          ml.training_mode,
                          ml.model_scope,
                          ml.location_id,
                          dl.location_name,
                          ml.province_id,
                          ml.location_count,
                          ml.target_variable,
                          ml.mae,
                          ml.rmse,
                          ml.rse,
                          ml.mape,
                          ml.r2,
                          ml.forecast_horizon_hours,
                          ml.input_window_hours,
                          ml.model_uri,
                          ml.mlflow_run_id,
                          ml.train_rows,
                          ml.test_rows,
                          ml.is_best,
                          ml.created_at,
                          CAST(NULL AS INT) AS rank_order,
                          CAST(NULL AS STRING) AS sensor_id
                        FROM {leaderboard_table} ml
                        LEFT JOIN {location_table} dl ON dl.location_id = ml.location_id
                        WHERE LOWER(COALESCE(ml.training_mode, '')) = 'global'
                        ORDER BY
                          ml.target_variable ASC,
                          ml.is_best DESC,
                          ml.rmse ASC NULLS LAST,
                          ml.mae ASC NULLS LAST,
                          ml.created_at DESC
                        LIMIT ?
                    """
                    global_rows = _query_model_rows(global_query, (max(0, int(limit) - len(rows)),))
                except Exception:
                    global_rows = []

                deduped: list[dict] = []
                seen_keys: set[tuple] = set()
                for row in rows + global_rows:
                    key = (
                        row.get("target_variable"),
                        row.get("model_name"),
                        row.get("training_mode"),
                        row.get("location_id"),
                        row.get("created_at"),
                    )
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    deduped.append(row)
                    if len(deduped) >= int(limit):
                        break
                return deduped
        except Exception:
            continue

    return []


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
            "forecast_refresh_seconds": _FORECAST_CACHE_TTL_SECONDS,
        }

    @staticmethod
    def fetch_forecast(sensor_id: str, limit: int = 200, sensor_metadata: dict | None = None) -> dict:
        if not _is_configured():
            return {"enabled": False, "sensor_id": sensor_id, "forecasts": []}

        cache_key = (sensor_id, int(limit))
        now = monotonic()
        with _FORECAST_CACHE_LOCK:
            cached = _FORECAST_CACHE.get(cache_key)
            if cached and cached[0] > now:
                return cached[1]

        payload = {
            "enabled": True,
            "sensor_id": sensor_id,
            "forecast_scope": "location",
            "refresh_interval_seconds": _FORECAST_CACHE_TTL_SECONDS,
            "forecasts": _resolve_forecast_rows(sensor_id, int(limit), sensor_metadata=sensor_metadata),
        }
        with _FORECAST_CACHE_LOCK:
            _FORECAST_CACHE[cache_key] = (now + _FORECAST_CACHE_TTL_SECONDS, payload)
        return payload

    @staticmethod
    def fetch_model_leaderboard(sensor_id: str, limit: int = 50, sensor_metadata: dict | None = None) -> dict:
        if not _is_configured():
            return {"enabled": False, "sensor_id": sensor_id, "models": []}
        context = _sensor_context(sensor_id, sensor_metadata=sensor_metadata)
        return {
            "enabled": True,
            "sensor_id": sensor_id,
            "leaderboard_scope": "location",
            "location_candidates": context.get("location_candidates") or [],
            "models": _resolve_model_rows(sensor_id, int(limit), sensor_metadata=sensor_metadata),
        }
