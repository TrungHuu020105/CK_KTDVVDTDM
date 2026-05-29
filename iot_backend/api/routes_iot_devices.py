"""IoT device management routes for the standalone IoT backend."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from iot_backend import crud
from iot_backend import mqtt_service
from iot_backend.api.routes_auth import get_current_user
from iot_backend.database import get_db
from iot_backend.models import Device, IoTDevice, User, UserDevicePermission
from iot_backend.services.weather_service import geocode_location


router = APIRouter(prefix="/api/iot-devices", tags=["iot-devices"])


class CreateIoTDeviceRequest(BaseModel):
    name: str
    device_type: Optional[str] = None
    metric_type: Optional[str] = None
    source: str
    unit: Optional[str] = None
    location: Optional[str] = None
    category: Optional[str] = None
    environment_type: str = "indoor"
    min_threshold: Optional[float] = None
    max_threshold: Optional[float] = None
    location_query: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    task_description: Optional[str] = None
    priority_level: Optional[str] = None
    action_hint: Optional[str] = None


class UpdateIoTDeviceRequest(BaseModel):
    name: Optional[str] = None
    source: Optional[str] = None
    device_type: Optional[str] = None
    metric_type: Optional[str] = None
    unit: Optional[str] = None
    location: Optional[str] = None
    category: Optional[str] = None
    is_active: Optional[bool] = None
    alert_enabled: Optional[bool] = None
    min_threshold: Optional[float] = None
    max_threshold: Optional[float] = None
    environment_type: Optional[str] = None
    location_query: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    task_description: Optional[str] = None
    priority_level: Optional[str] = None
    action_hint: Optional[str] = None


class UpdateAlertThresholdsRequest(BaseModel):
    alert_enabled: Optional[bool] = None
    min_threshold: Optional[float] = None
    max_threshold: Optional[float] = None


class GeocodeRequest(BaseModel):
    location_query: str


def _normalize_source(value: str) -> str:
    source = (value or "").strip().lower()
    match = re.fullmatch(r"sensor[-_]?0*(\d+)", source)
    if match:
        return f"sensor_{int(match.group(1))}"
    return source


def _normalize_environment_type(value: Optional[str]) -> str:
    raw = (value or "").strip().lower()
    if raw in {"outdoor", "ngoài trời", "ngoai troi"}:
        return "outdoor"
    return "indoor"


def _environment_label(value: Optional[str]) -> str:
    return "Ngoài trời" if (value or "").strip().lower() == "outdoor" else "Trong nhà"


def _normalize_metric_type(device_type: Optional[str], metric_type: Optional[str]) -> str:
    value = (metric_type or device_type or "").strip().lower()
    return value


def _default_unit_for_metric(metric_type: str) -> Optional[str]:
    normalized = (metric_type or "").strip().lower()
    if normalized == "temperature":
        return "°C"
    if normalized in {"humidity", "soil_moisture"}:
        return "%"
    if normalized == "light_intensity":
        return "lux"
    if normalized == "pressure":
        return "hPa"
    return None


def _serialize_device(device: IoTDevice) -> dict:
    return {
        "id": device.id,
        "user_id": device.user_id,
        "name": device.name,
        "device_type": device.device_type,
        "metric_type": device.device_type,
        "source": device.source,
        "unit": device.unit,
        "location": device.location,
        "category": _environment_label(device.environment_type),
        "environment_type": device.environment_type,
        "location_query": device.location_query,
        "latitude": device.latitude,
        "longitude": device.longitude,
        "timezone_name": device.timezone_name,
        "task_description": device.task_description,
        "priority_level": device.priority_level,
        "action_hint": device.action_hint,
        "is_active": device.is_active,
        "alert_enabled": device.alert_enabled,
        "min_threshold": device.min_threshold,
        "max_threshold": device.max_threshold,
        "created_at": device.created_at,
    }


def _thresholds_configured(device: IoTDevice) -> tuple[bool, Optional[float], Optional[float]]:
    min_threshold = device.min_threshold
    max_threshold = device.max_threshold
    return min_threshold is not None and max_threshold is not None, min_threshold, max_threshold


def _field_was_provided(payload: BaseModel, field_name: str) -> bool:
    fields_set = getattr(payload, "model_fields_set", None)
    if fields_set is None:
        fields_set = getattr(payload, "__fields_set__", set())
    return field_name in fields_set


def _resolve_threshold_values(
    payload: BaseModel,
    *,
    current_min: Optional[float],
    current_max: Optional[float],
) -> tuple[Optional[float], Optional[float]]:
    if _field_was_provided(payload, "min_threshold"):
        min_threshold = payload.min_threshold
    else:
        min_threshold = current_min

    if _field_was_provided(payload, "max_threshold"):
        max_threshold = payload.max_threshold
    else:
        max_threshold = current_max

    return min_threshold, max_threshold


def _publish_threshold_config_for_device(device: IoTDevice) -> bool:
    _, min_threshold, max_threshold = _thresholds_configured(device)
    return mqtt_service.publish_threshold_config(
        sensor_id=device.source,
        metric_type=device.device_type,
        min_threshold=min_threshold,
        max_threshold=max_threshold,
        alert_enabled=bool(device.alert_enabled),
        unit=device.unit or "",
        device_id=device.id,
    )


def _alert_status(device: IoTDevice, latest_value: Optional[float]) -> str:
    configured, min_threshold, max_threshold = _thresholds_configured(device)
    if not configured or latest_value is None:
        return "none"
    if latest_value < float(min_threshold) or latest_value > float(max_threshold):
        return "out_of_range"
    return "normal"


def _serialize_realtime_device(db: Session, user: User, device: IoTDevice) -> dict:
    server_now = datetime.now()
    metric = crud.get_latest_metric_for_user(
        db,
        user.id,
        metric_type=device.device_type,
        source=device.source,
    )
    latest_value = metric.metric_value if metric else None
    latest_event_ts = metric.event_ts if metric else None
    lag_seconds = None
    if latest_event_ts:
        try:
            event_ts = latest_event_ts.replace(tzinfo=None) if getattr(latest_event_ts, "tzinfo", None) else latest_event_ts
            lag_seconds = round((server_now - event_ts).total_seconds(), 3)
        except Exception:
            lag_seconds = None
    data = _serialize_device(device)
    data.update(
        {
            "latest_value": latest_value,
            "latest_event_ts": latest_event_ts,
            "server_now": server_now,
            "lag_seconds": lag_seconds,
            "alert_status": _alert_status(device, latest_value),
        }
    )
    return data


def _sync_device_row(db: Session, device: IoTDevice) -> None:
    row = db.query(Device).filter(Device.source == device.source).first()
    if row:
        row.name = device.name
        row.device_type = device.device_type
        row.location = device.location
        row.is_active = device.is_active
        return

    db.add(
        Device(
            name=device.name,
            device_type=device.device_type,
            source=device.source,
            location=device.location,
            is_active=device.is_active,
            created_by=device.user_id,
        )
    )


@router.get("")
async def get_my_iot_devices(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    devices = db.query(IoTDevice).filter(IoTDevice.user_id == user.id).order_by(IoTDevice.id.asc()).all()
    return {"devices": [_serialize_device(d) for d in devices], "count": len(devices)}


@router.get("/realtime")
async def get_my_iot_devices_realtime(
    response: Response,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    request_now = datetime.now()
    devices = (
        db.query(IoTDevice)
        .filter(IoTDevice.user_id == user.id)
        .order_by(IoTDevice.id.asc())
        .all()
    )
    rows = [_serialize_realtime_device(db, user, d) for d in devices]
    return {
        "updated_at": request_now.isoformat(),
        "server_now": request_now.isoformat(),
        "devices": rows,
        "count": len(rows),
    }


@router.post("")
async def create_iot_device(
    payload: CreateIoTDeviceRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    source = _normalize_source(payload.source)
    if not source:
        raise HTTPException(status_code=400, detail="Sensor source is required")

    metric_type = _normalize_metric_type(payload.device_type, payload.metric_type)
    if not metric_type:
        raise HTTPException(status_code=400, detail="metric_type (or device_type) is required")

    unit = (payload.unit or "").strip() or _default_unit_for_metric(metric_type)
    min_threshold = payload.min_threshold
    max_threshold = payload.max_threshold

    duplicate = db.query(IoTDevice).filter(
        IoTDevice.source == source,
        IoTDevice.device_type == metric_type,
    ).first()
    if duplicate:
        raise HTTPException(
            status_code=400,
            detail=f"Device with source '{source}' and type '{metric_type}' already exists",
        )

    device = IoTDevice(
        user_id=user.id,
        name=payload.name,
        device_type=metric_type,
        source=source,
        unit=unit,
        location=payload.location,
        environment_type=_normalize_environment_type(payload.category or payload.environment_type),
        location_query=(payload.location_query or "").strip() or None,
        latitude=payload.latitude,
        longitude=payload.longitude,
        task_description=(payload.task_description or "").strip() or None,
        priority_level=(payload.priority_level or "").strip().lower() or None,
        action_hint=(payload.action_hint or "").strip() or None,
        is_active=True,
        alert_enabled=(min_threshold is not None and max_threshold is not None),
        min_threshold=min_threshold,
        max_threshold=max_threshold,
        created_by=user.id,
    )
    db.add(device)
    db.flush()
    _sync_device_row(db, device)
    db.flush()
    metric_device = db.query(Device).filter(Device.source == source).first()
    if metric_device:
        existing_permission = db.query(UserDevicePermission).filter(
            UserDevicePermission.user_id == user.id,
            UserDevicePermission.device_id == metric_device.id,
        ).first()
        if not existing_permission:
            db.add(UserDevicePermission(user_id=user.id, device_id=metric_device.id, granted_by=user.id))
    db.commit()
    db.refresh(device)
    return _serialize_device(device)


@router.put("/{device_id}")
async def update_iot_device(
    device_id: int,
    payload: UpdateIoTDeviceRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    device = db.query(IoTDevice).filter(IoTDevice.id == device_id, IoTDevice.user_id == user.id).first()
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")

    if payload.name is not None:
        device.name = payload.name
    if payload.source is not None:
        source = _normalize_source(payload.source)
        if not source:
            raise HTTPException(status_code=400, detail="Sensor source is required")
        device.source = source
    metric_type = _normalize_metric_type(payload.device_type, payload.metric_type)
    if metric_type:
        device.device_type = metric_type
        if not device.unit:
            device.unit = _default_unit_for_metric(metric_type)
    if payload.unit is not None:
        device.unit = payload.unit.strip() or None
    for key in ("location", "latitude", "longitude"):
        value = getattr(payload, key)
        if value is not None:
            setattr(device, key, value)
    if payload.is_active is not None:
        device.is_active = payload.is_active
    if payload.environment_type is not None or payload.category is not None:
        device.environment_type = _normalize_environment_type(payload.category or payload.environment_type)
    if payload.location_query is not None:
        device.location_query = payload.location_query.strip() or None
    if payload.task_description is not None:
        device.task_description = payload.task_description.strip() or None
    if payload.priority_level is not None:
        device.priority_level = payload.priority_level.strip().lower() or None
    if payload.action_hint is not None:
        device.action_hint = payload.action_hint.strip() or None

    thresholds_touched = (
        _field_was_provided(payload, "min_threshold")
        or _field_was_provided(payload, "max_threshold")
        or _field_was_provided(payload, "alert_enabled")
    )
    if thresholds_touched:
        min_threshold, max_threshold = _resolve_threshold_values(
            payload,
            current_min=device.min_threshold,
            current_max=device.max_threshold,
        )
        device.min_threshold = min_threshold
        device.max_threshold = max_threshold
        if not _field_was_provided(payload, "alert_enabled"):
            device.alert_enabled = (min_threshold is not None and max_threshold is not None)
        else:
            device.alert_enabled = bool(payload.alert_enabled)

    duplicate = db.query(IoTDevice).filter(
        IoTDevice.id != device.id,
        IoTDevice.source == device.source,
        IoTDevice.device_type == device.device_type,
    ).first()
    if duplicate:
        raise HTTPException(
            status_code=400,
            detail=f"Device with source '{device.source}' and type '{device.device_type}' already exists",
        )

    _sync_device_row(db, device)
    db.commit()
    db.refresh(device)
    response = _serialize_device(device)
    if thresholds_touched:
        mqtt_ok = _publish_threshold_config_for_device(device)
        response["threshold_config_published"] = mqtt_ok
        response["threshold_sync_status"] = "published" if mqtt_ok else "db_only"
    return response


@router.delete("/{device_id}")
async def delete_iot_device(device_id: int, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    device = db.query(IoTDevice).filter(IoTDevice.id == device_id, IoTDevice.user_id == user.id).first()
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")
    metric_device = db.query(Device).filter(Device.source == device.source).first()
    if metric_device:
        metric_device.is_active = False
    db.delete(device)
    db.commit()
    return {"message": "Device deleted successfully"}


@router.put("/{device_id}/alert-thresholds")
async def update_alert_thresholds(
    device_id: int,
    payload: UpdateAlertThresholdsRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    device = db.query(IoTDevice).filter(IoTDevice.id == device_id, IoTDevice.user_id == user.id).first()
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")

    min_threshold, max_threshold = _resolve_threshold_values(
        payload,
        current_min=device.min_threshold,
        current_max=device.max_threshold,
    )
    device.min_threshold = min_threshold
    device.max_threshold = max_threshold
    if not _field_was_provided(payload, "alert_enabled"):
        device.alert_enabled = (min_threshold is not None and max_threshold is not None)
    else:
        device.alert_enabled = bool(payload.alert_enabled)

    db.commit()
    db.refresh(device)
    mqtt_ok = _publish_threshold_config_for_device(device)
    return {
        **_serialize_device(device),
        "message": "Alert thresholds updated successfully",
        "threshold_config_published": mqtt_ok,
        "threshold_sync_status": "published" if mqtt_ok else "db_only",
    }


@router.post("/geocode")
async def geocode_sensor_location(
    payload: GeocodeRequest,
    user: User = Depends(get_current_user),
):
    _ = user
    geo = geocode_location(payload.location_query)
    if not geo:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Location not found")
    return {
        "name": geo.name,
        "country": geo.country,
        "admin1": geo.admin1,
        "timezone": geo.timezone,
        "latitude": geo.latitude,
        "longitude": geo.longitude,
    }
