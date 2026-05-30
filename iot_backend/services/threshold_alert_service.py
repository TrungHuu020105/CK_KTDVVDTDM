"""Realtime threshold checking + alert/notification dispatch for IoT metrics."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime

from sqlalchemy.orm import Session

from iot_backend.crud import create_alert
from iot_backend.models import IoTDevice
from iot_backend.schemas import AlertCreate
from iot_backend.services.alert_service import dispatch_alert_notifications

ALERT_NOTIFY_COOLDOWN_SECONDS = 60
_last_alert_notification_ts: dict[str, float] = {}
_UNIT_BY_METRIC = {
    "temperature": "°C",
    "humidity": "%",
    "soil_moisture": "%",
    "light_intensity": "lux",
    "pressure": "hPa",
}


def _pick_thresholds(device: IoTDevice, metric_type: str) -> tuple[float | None, float | None]:
    if metric_type == "temperature":
        min_value = device.temperature_min_threshold
        max_value = device.temperature_max_threshold
        if min_value is None and max_value is None:
            return device.min_threshold, device.max_threshold
        return min_value, max_value
    if metric_type == "humidity":
        min_value = device.humidity_min_threshold
        max_value = device.humidity_max_threshold
        if min_value is None and max_value is None:
            return device.min_threshold, device.max_threshold
        return min_value, max_value
    return device.min_threshold, device.max_threshold


def _find_device_for_metric(db: Session, source: str, metric_type: str) -> IoTDevice | None:
    device = (
        db.query(IoTDevice)
        .filter(
            IoTDevice.source == source,
            IoTDevice.device_type == metric_type,
        )
        .first()
    )
    if device:
        return device
    return db.query(IoTDevice).filter(IoTDevice.source == source).first()


def check_and_trigger_metric_alert(
    db: Session,
    *,
    metric_type: str,
    source: str,
    value: float,
    metric_ts: datetime,
    origin: str = "realtime",
) -> None:
    device = _find_device_for_metric(db, source, metric_type)
    if not device:
        return
    if not device.is_active or not device.alert_enabled:
        return

    min_threshold, max_threshold = _pick_thresholds(device, metric_type)
    if min_threshold is None or max_threshold is None:
        return

    threshold = None
    status = None
    current = float(value)
    if current > float(max_threshold):
        threshold = float(max_threshold)
        status = "critical"
    elif current < float(min_threshold):
        threshold = float(min_threshold)
        status = "warning"

    alert_key = f"{source}:{metric_type}"
    if threshold is None or status is None:
        _last_alert_notification_ts.pop(alert_key, None)
        return

    now_ts = time.time()
    if now_ts - _last_alert_notification_ts.get(alert_key, 0) < ALERT_NOTIFY_COOLDOWN_SECONDS:
        return

    unit = _UNIT_BY_METRIC.get(metric_type, device.unit or "")
    threshold_text = f"> {threshold}" if status == "critical" else f"< {threshold}"
    message = (
        "IoT Alert\n"
        f"Device: {device.name or source}\n"
        f"Metric: {metric_type}\n"
        f"Current value: {current} {unit}\n"
        f"Threshold: {threshold_text} (range: {min_threshold} - {max_threshold})\n"
        f"Source: {origin}"
    )

    alert = create_alert(
        db,
        AlertCreate(
            metric_type=metric_type,
            status=status,
            current_value=current,
            threshold=float(threshold),
            message=message,
            source=source,
            device_id=device.id,
            device_name=device.name or source,
            unit=unit,
            min_threshold=float(min_threshold),
            max_threshold=float(max_threshold),
            created_at=metric_ts,
        ),
    )

    _last_alert_notification_ts[alert_key] = now_ts
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(dispatch_alert_notifications(alert.id))
    except RuntimeError:
        asyncio.run(dispatch_alert_notifications(alert.id))
