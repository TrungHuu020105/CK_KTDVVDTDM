"""IoT metric ingest, threshold checking, alert persistence, and notification."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from app import crud
from app.models import IoTDevice
from app.schemas import AlertCreate, MetricCreate
from app.services.alert_service import dispatch_alert_notifications


# Cooldown 5 phút — mỗi device+metric_type chỉ gửi notification tối đa 1 lần / 5 phút
ALERT_NOTIFY_COOLDOWN_SECONDS = 300
_alert_runtime_state: dict[str, dict[str, datetime | str | None]] = {}


def normalize_metric_source(payload: dict, fallback: Optional[str] = None) -> str:
    source = payload.get("source") or payload.get("sensor_id") or fallback or ""
    return str(source).strip()


def parse_metric_timestamp(timestamp: str | datetime | None) -> datetime:
    if isinstance(timestamp, datetime):
        return timestamp
    if not timestamp:
        return datetime.now()
    try:
        parsed = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            return parsed.astimezone().replace(tzinfo=None)
        return parsed
    except Exception:
        return datetime.now()


UNIT_MAP = {
    "temperature": "°C",
    "humidity": "%",
    "soil_moisture": "%",
    "light_intensity": "lux",
    "pressure": "hPa",
}


UNIT_MAP["temperature"] = "°C"


def _threshold_label(status: str, threshold: float) -> str:
    return f"> {threshold}" if status == "critical" else f"< {threshold}"


def _alert_state_key(source: str, metric_type: str) -> str:
    return f"{source}:{metric_type}"


def _get_alert_runtime_state(source: str, metric_type: str) -> dict[str, datetime | str | None]:
    return _alert_runtime_state.setdefault(
        _alert_state_key(source, metric_type),
        {"last_status": None, "last_sent_at": None},
    )


def _auto_register_metric_device(db: Session, *, source: str, metric_type: str) -> Optional[IoTDevice]:
    """Ensure there's an IoTDevice row for (source, metric_type) if possible.

    If metrics for a new metric_type arrive for a source that already belongs to exactly
    one user in IoTDevice, auto-create a new IoTDevice for that metric_type.

    This avoids hardcoding device names in the frontend and allows multi-metric
    sources (e.g., ESP32) to show multiple cards without manual creation.
    """
    try:
        existing = db.query(IoTDevice).filter(
            IoTDevice.source == source,
            IoTDevice.device_type == metric_type,
        ).first()
    except Exception:
        return None

    if existing:
        return existing

    try:
        same_source = db.query(IoTDevice).filter(IoTDevice.source == source).all()
    except Exception:
        return None

    if not same_source:
        return None

    user_ids = {d.user_id for d in same_source if d.user_id is not None}
    if len(user_ids) != 1:
        # Avoid creating devices when ownership is ambiguous.
        return None

    template = same_source[0]
    unit = template.unit or UNIT_MAP.get(metric_type, "")

    device = IoTDevice(
        user_id=template.user_id,
        name=template.name,
        device_type=metric_type,
        source=source,
        unit=unit or template.unit,
        location=template.location,
        environment_type=template.environment_type,
        location_query=template.location_query,
        latitude=template.latitude,
        longitude=template.longitude,
        timezone_name=template.timezone_name,
        task_description=template.task_description,
        priority_level=template.priority_level,
        action_hint=template.action_hint,
        is_active=True,
        alert_enabled=False,
        min_threshold=None,
        max_threshold=None,
        created_by=template.created_by,
    )

    try:
        db.add(device)
        db.commit()
        db.refresh(device)
        print(f"[ALERT] Auto-registered IoTDevice source={source} metric_type={metric_type} id={device.id}")
        return device
    except Exception as exc:
        db.rollback()
        print(f"[ALERT] Auto-register failed source={source} metric_type={metric_type}: {exc}")
        return None


def check_and_trigger_alert(
    db: Session,
    *,
    metric_type: str,
    source: str,
    value: float,
    metric_ts: datetime,
) -> None:
    print(f"[ALERT] Received metric source={source} metric_type={metric_type} value={value}")

    try:
        # Tìm device theo (source, device_type) — mỗi source có thể có nhiều device_type khác nhau
        device = db.query(IoTDevice).filter(
            IoTDevice.source == source,
            IoTDevice.device_type == metric_type,
        ).first()
    except Exception as exc:
        print(f"[ALERT] Failed to query IoTDevice for source={source}: {exc}")
        return

    if not device:
        # If this source already exists for exactly one user, auto-create the missing metric device.
        device = _auto_register_metric_device(db, source=source, metric_type=metric_type)
        if not device:
            print(f"[ALERT] No IoT device configured for source={source} metric_type={metric_type}")
            return

    # Chỉ kích hoạt alert khi user đã bật alert_enabled=True (user tự cấu hình ngưỡng)
    if not device.is_active:
        print(
            f"[ALERT CHECK] device={device.name} source={source} metric={metric_type} "
            f"value={value} min=None max=None alert_enabled={device.alert_enabled} skipped because device inactive"
        )
        return

    if not device.alert_enabled:
        print(
            f"[ALERT CHECK] device={device.name} source={source} metric={metric_type} "
            f"value={value} min=None max=None alert_enabled={device.alert_enabled}"
        )
        print("[ALERT CHECK] skipped because alert_enabled=false")
        return

    # Cần cả min_threshold VÀ max_threshold mới alert (yêu cầu 4)
    min_threshold = device.min_threshold
    max_threshold = device.max_threshold
    print(
        f"[ALERT CHECK] device={device.name} source={source} metric={metric_type} "
        f"value={value} min={min_threshold} max={max_threshold} alert_enabled={device.alert_enabled}"
    )
    if min_threshold is None or max_threshold is None:
        print("[ALERT CHECK] skipped because threshold missing")
        return

    threshold = None
    status = None
    if value > float(max_threshold):
        threshold = float(max_threshold)
        status = "critical"
    elif value < float(min_threshold):
        threshold = float(min_threshold)
        status = "warning"

    # In-memory cooldown + recovery reset
    now_dt = datetime.now()
    state = _get_alert_runtime_state(source, metric_type)

    if threshold is None or status is None:
        # Value trong range → reset cooldown (chuẩn bị cho lần OUT OF RANGE tiếp theo)
        state["last_status"] = "normal"
        state["last_sent_at"] = None
        print("[ALERT CHECK] status=NORMAL")
        return

    last_status = state.get("last_status")
    last_sent_at = state.get("last_sent_at")
    if last_status == "out_of_range" and isinstance(last_sent_at, datetime):
        elapsed = (now_dt - last_sent_at).total_seconds()
        if elapsed < ALERT_NOTIFY_COOLDOWN_SECONDS:
            remaining = int(ALERT_NOTIFY_COOLDOWN_SECONDS - elapsed)
            print(f"[EMAIL ALERT] skipped because cooldown active source={source} metric={metric_type} remaining={remaining}s")
            return

    print("[ALERT CHECK] status=OUT_OF_RANGE")
    unit = UNIT_MAP.get(metric_type, "")
    device_name = device.name or source
    print(f"[ALERT] OUT OF RANGE: {device_name} {metric_type} = {value} {unit}")

    message = (
        f"[MetricsPulse Alert] {device_name} {metric_type} OUT OF RANGE\n"
        f"Device: {device_name}\n"
        f"Source: {source}\n"
        f"Metric: {metric_type}\n"
        f"Current value: {value} {unit}\n"
        f"Threshold: min {min_threshold}, max {max_threshold}\n"
        f"Status: OUT OF RANGE"
    )

    try:
        alert = crud.create_alert(
            db,
            AlertCreate(
                metric_type=metric_type,
                status="OUT_OF_RANGE",
                current_value=float(value),
                threshold=float(threshold),
                message=message,
                source=source,
                device_id=device.id,
                device_name=device_name,
                unit=unit,
                min_threshold=float(min_threshold),
                max_threshold=float(max_threshold),
                created_at=metric_ts,
            ),
        )
        print(f"[ALERT CREATE] created alert id={alert.id} source={source} metric_type={metric_type}")
    except Exception as exc:
        db.rollback()
        print(f"[ALERT] Failed to create alert for {source}/{metric_type}: {exc}")
        return

    state["last_status"] = "out_of_range"
    state["last_sent_at"] = now_dt

    try:
        print("[ALERT] Dispatching notifications")
        loop = asyncio.get_running_loop()
        loop.create_task(dispatch_alert_notifications(alert.id))
    except RuntimeError:
        try:
            asyncio.run(dispatch_alert_notifications(alert.id))
        except Exception as dispatch_exc:
            print(f"[ALERT] Notification dispatch failed alert_id={alert.id}: {dispatch_exc}")


def ingest_iot_metric(
    db: Session,
    *,
    metric_type: str,
    source: str,
    value: float,
    location: Optional[str] = None,
    timestamp: str | datetime | None = None,
    unit: str = "",
    save_flag: bool = True,
):
    source = str(source).strip()
    metric_type = str(metric_type).strip()
    metric_ts = parse_metric_timestamp(timestamp)
    check_and_trigger_alert(
        db,
        metric_type=metric_type,
        source=source,
        value=float(value),
        metric_ts=metric_ts,
    )

    if not save_flag:
        return None

    return crud.create_metric(
        db,
        MetricCreate(
            event_ts=metric_ts,
            sensor_id=source,
            location=location,
            metric_type=metric_type,
            metric_value=float(value),
            unit=unit or None,
        ),
    )


