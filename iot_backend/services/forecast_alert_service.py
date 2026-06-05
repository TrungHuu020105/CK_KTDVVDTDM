"""Forecast-threshold alert scanning for future Databricks predictions."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

from iot_backend import crud
from iot_backend.database import SessionLocal
from iot_backend.models import Alert, IoTDevice
from iot_backend.schemas import AlertCreate
from iot_backend.services.alert_service import dispatch_alert_notifications
from iot_backend.services.sensor_reading_service import parse_event_ts


VN_TZ = timezone(timedelta(hours=7))
FORECAST_SCAN_INTERVAL_SECONDS = max(300, int(os.getenv("FORECAST_ALERT_SCAN_INTERVAL_SECONDS", "3600")))
FORECAST_LOOKAHEAD_HOURS = max(1, int(os.getenv("FORECAST_ALERT_LOOKAHEAD_HOURS", "24")))
FORECAST_DEDUPE_HOURS = max(1, int(os.getenv("FORECAST_ALERT_DEDUPE_HOURS", "48")))


def _device_metadata(device: IoTDevice) -> dict:
    return {
        "id": device.id,
        "sensor_id": device.source,
        "source": device.source,
        "name": device.name,
        "source_type": device.source_type,
        "environment_type": device.environment_type,
        "location": device.location,
        "location_query": device.location_query,
        "location_province": device.location_province,
        "latitude": device.latitude,
        "longitude": device.longitude,
    }


def _forecast_rows_for_device(device: IoTDevice) -> list[dict]:
    from app.services.databricks_service import DatabricksService

    # Forecast UI uses a much larger window. Keep scan aligned so we do not
    # miss next-day points when the latest forecast run also contains earlier
    # timestamps before "now".
    limit = max(200, FORECAST_LOOKAHEAD_HOURS + 24)
    payload = DatabricksService.fetch_forecast(
        device.source,
        limit=limit,
        sensor_metadata=_device_metadata(device),
    )
    return payload.get("forecasts") or []


def _dedupe_devices(devices: list[IoTDevice]) -> list[IoTDevice]:
    selected: dict[str, IoTDevice] = {}
    for device in devices:
        current = selected.get(device.source)
        if current is None:
            selected[device.source] = device
            continue
        current_priority = 1 if current.device_type == "temperature_humidity" else 0
        next_priority = 1 if device.device_type == "temperature_humidity" else 0
        if next_priority > current_priority or device.id < current.id:
            selected[device.source] = device
    return list(selected.values())


def _load_target_devices(user_id: int | None, is_admin: bool) -> list[IoTDevice]:
    db = SessionLocal()
    try:
        query = db.query(IoTDevice).filter(
            IoTDevice.is_active == True,
            IoTDevice.alert_enabled == True,
        )
        if not is_admin and user_id is not None:
            query = query.filter(IoTDevice.user_id == user_id)
        rows = query.order_by(IoTDevice.user_id.asc(), IoTDevice.id.asc()).all()
        rows = [
            row for row in rows
            if any(
                threshold is not None
                for threshold in (
                    row.temperature_min_threshold,
                    row.temperature_max_threshold,
                    row.humidity_min_threshold,
                    row.humidity_max_threshold,
                )
            )
        ]
        return _dedupe_devices(rows)
    finally:
        db.close()


def _forecast_window() -> tuple[datetime, datetime]:
    now = datetime.now(VN_TZ).replace(tzinfo=None)
    return now, now + timedelta(hours=FORECAST_LOOKAHEAD_HOURS)


def _to_vn_naive(value) -> datetime | None:
    if value is None:
        return None
    try:
        return parse_event_ts(value)
    except Exception:
        return None


def _first_breaches(device: IoTDevice, rows: list[dict]) -> list[dict]:
    now, until = _forecast_window()
    breaches: list[dict] = []
    seen_keys: set[tuple[str, str]] = set()

    ordered_rows = sorted(
        rows,
        key=lambda row: _to_vn_naive(row.get("forecast_ts") or row.get("timestamp") or row.get("event_ts")) or datetime.max,
    )
    for row in ordered_rows:
        forecast_ts = _to_vn_naive(row.get("forecast_ts") or row.get("timestamp") or row.get("event_ts"))
        if forecast_ts is None or forecast_ts <= now or forecast_ts > until:
            continue
        generated_at = _to_vn_naive(row.get("generated_at"))

        checks = [
            (
                "temperature",
                row.get("temperature"),
                device.temperature_min_threshold,
                device.temperature_max_threshold,
                "C",
            ),
            (
                "humidity",
                row.get("humidity"),
                device.humidity_min_threshold,
                device.humidity_max_threshold,
                "%",
            ),
        ]
        for metric_type, raw_value, min_threshold, max_threshold, unit in checks:
            if raw_value is None:
                continue
            try:
                value = float(raw_value)
            except Exception:
                continue

            if max_threshold is not None and value > float(max_threshold):
                key = (metric_type, "high")
                if key not in seen_keys:
                    seen_keys.add(key)
                    breaches.append(
                        {
                            "metric_type": metric_type,
                            "status": "critical",
                            "current_value": value,
                            "threshold": float(max_threshold),
                            "unit": unit,
                            "forecast_timestamp": forecast_ts,
                            "forecast_generated_at": generated_at,
                            "min_threshold": min_threshold,
                            "max_threshold": max_threshold,
                            "direction": "high",
                        }
                    )

            if min_threshold is not None and value < float(min_threshold):
                key = (metric_type, "low")
                if key not in seen_keys:
                    seen_keys.add(key)
                    breaches.append(
                        {
                            "metric_type": metric_type,
                            "status": "warning",
                            "current_value": value,
                            "threshold": float(min_threshold),
                            "unit": unit,
                            "forecast_timestamp": forecast_ts,
                            "forecast_generated_at": generated_at,
                            "min_threshold": min_threshold,
                            "max_threshold": max_threshold,
                            "direction": "low",
                        }
                    )
    return breaches


def _is_duplicate_forecast_alert(db, device: IoTDevice, breach: dict) -> bool:
    since = datetime.now(VN_TZ).replace(tzinfo=None) - timedelta(hours=FORECAST_DEDUPE_HOURS)
    existing = (
        db.query(Alert)
        .filter(
            Alert.alert_origin == "forecast",
            Alert.source == device.source,
            Alert.metric_type == breach["metric_type"],
            Alert.status == breach["status"],
            Alert.threshold == breach["threshold"],
            Alert.forecast_timestamp == breach["forecast_timestamp"],
            Alert.created_at >= since,
        )
        .order_by(Alert.created_at.desc())
        .first()
    )
    return existing is not None


def _forecast_message(device: IoTDevice, breach: dict) -> str:
    comparator = "vượt ngưỡng trên" if breach["direction"] == "high" else "thấp hơn ngưỡng dưới"
    forecast_time = breach["forecast_timestamp"].strftime("%Y-%m-%d %H:%M:%S")
    unit = breach["unit"]
    return (
        f"Forecast alert: {device.name or device.source} dự báo {breach['metric_type']} "
        f"{breach['current_value']:.1f}{unit} vào {forecast_time}, {comparator} {breach['threshold']:.1f}{unit}."
    )


def run_forecast_alert_scan(*, user_id: int | None = None, is_admin: bool = False, trigger: str = "manual") -> dict:
    devices = _load_target_devices(user_id=user_id, is_admin=is_admin)
    created_alert_ids: list[int] = []
    duplicate_count = 0
    scanned_sources: list[str] = []
    errors: list[str] = []
    details: list[dict] = []
    now, until = _forecast_window()

    db = SessionLocal()
    try:
        for device in devices:
            scanned_sources.append(device.source)
            detail = {
                "source": device.source,
                "device_name": device.name,
                "temperature_min_threshold": device.temperature_min_threshold,
                "temperature_max_threshold": device.temperature_max_threshold,
                "humidity_min_threshold": device.humidity_min_threshold,
                "humidity_max_threshold": device.humidity_max_threshold,
                "forecast_rows": 0,
                "future_rows_in_window": 0,
                "breach_candidates": 0,
                "created_alerts": 0,
                "duplicates": 0,
                "first_forecast_ts": None,
                "last_forecast_ts": None,
                "first_breach": None,
                "error": None,
            }
            try:
                rows = _forecast_rows_for_device(device)
                detail["forecast_rows"] = len(rows)
                forecast_times = [
                    _to_vn_naive(row.get("forecast_ts") or row.get("timestamp") or row.get("event_ts"))
                    for row in rows
                ]
                forecast_times = [ts for ts in forecast_times if ts is not None]
                if forecast_times:
                    detail["first_forecast_ts"] = min(forecast_times).isoformat(sep=" ")
                    detail["last_forecast_ts"] = max(forecast_times).isoformat(sep=" ")
                    detail["future_rows_in_window"] = sum(1 for ts in forecast_times if now < ts <= until)

                breaches = _first_breaches(device, rows)
                detail["breach_candidates"] = len(breaches)
                if breaches:
                    detail["first_breach"] = {
                        "metric_type": breaches[0]["metric_type"],
                        "direction": breaches[0]["direction"],
                        "current_value": breaches[0]["current_value"],
                        "threshold": breaches[0]["threshold"],
                        "forecast_timestamp": breaches[0]["forecast_timestamp"].isoformat(sep=" "),
                    }

                for breach in breaches:
                    if _is_duplicate_forecast_alert(db, device, breach):
                        duplicate_count += 1
                        detail["duplicates"] += 1
                        continue
                    alert = crud.create_alert(
                        db,
                        AlertCreate(
                            metric_type=breach["metric_type"],
                            status=breach["status"],
                            current_value=breach["current_value"],
                            threshold=breach["threshold"],
                            message=_forecast_message(device, breach),
                            source=device.source,
                            device_id=device.id,
                            device_name=device.name or device.source,
                            unit=breach["unit"],
                            min_threshold=breach["min_threshold"],
                            max_threshold=breach["max_threshold"],
                            alert_origin="forecast",
                            forecast_timestamp=breach["forecast_timestamp"],
                            forecast_generated_at=breach["forecast_generated_at"],
                            created_at=datetime.now(VN_TZ).replace(tzinfo=None),
                        ),
                    )
                    created_alert_ids.append(alert.id)
                    detail["created_alerts"] += 1
            except Exception as exc:
                message = f"{device.source}: {type(exc).__name__}: {exc}"
                errors.append(message)
                detail["error"] = message
            details.append(detail)
    finally:
        db.close()

    return {
        "status": "ok",
        "trigger": trigger,
        "scope": "all" if is_admin and user_id is None else "user",
        "scan_interval_seconds": FORECAST_SCAN_INTERVAL_SECONDS,
        "lookahead_hours": FORECAST_LOOKAHEAD_HOURS,
        "scanned_devices": len(devices),
        "scanned_sources": scanned_sources,
        "created_alert_count": len(created_alert_ids),
        "created_alert_ids": created_alert_ids,
        "duplicate_count": duplicate_count,
        "error_count": len(errors),
        "errors": errors,
        "details": details,
    }


async def dispatch_created_forecast_alerts(alert_ids: list[int]) -> None:
    if not alert_ids:
        return
    await asyncio.gather(
        *(dispatch_alert_notifications(alert_id) for alert_id in alert_ids),
        return_exceptions=True,
    )
