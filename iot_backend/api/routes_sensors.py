"""Sensor-level API for physical and virtual IoT devices."""

from __future__ import annotations

import csv
import io
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from iot_backend import mqtt_service
from iot_backend.api.routes_auth import get_current_user
from iot_backend.database import get_db
from iot_backend.models import IoTDevice, SensorReading, User
from iot_backend.services.sensor_reading_service import create_sensor_reading, parse_event_ts, serialize_reading
from iot_backend.services.threshold_alert_service import check_and_trigger_metric_alert
from iot_backend.services.weather_service import geocode_location

router = APIRouter(prefix="/api/sensors", tags=["sensors"])
VN_TZ = timezone(timedelta(hours=7))


def _normalize_source(value: str) -> str:
    source = (value or "").strip().lower()
    match = re.fullmatch(r"sensor[-_]?0*(\d+)", source)
    if match:
        return f"sensor_{int(match.group(1))}"
    return source


def _serialize_device(device: IoTDevice, latest: SensorReading | None = None) -> dict:
    return {
        "id": device.id,
        "user_id": device.user_id,
        "name": device.name,
        "sensor_id": device.source,
        "source": device.source,
        "device_type": "temperature_humidity",
        "capabilities": (device.capabilities or "temperature,humidity").split(","),
        "source_type": device.source_type or "physical_iot",
        "provider": "esp32",
        "environment_type": device.environment_type,
        "location": device.location,
        "location_province": device.location_province,
        "location_query": device.location_query,
        "latitude": device.latitude,
        "longitude": device.longitude,
        "timezone_name": device.timezone_name,
        "task_description": device.task_description,
        "priority_level": device.priority_level,
        "action_hint": device.action_hint,
        "is_active": device.is_active,
        "alert_enabled": device.alert_enabled,
        "temperature_min_threshold": device.temperature_min_threshold,
        "temperature_max_threshold": device.temperature_max_threshold,
        "humidity_min_threshold": device.humidity_min_threshold,
        "humidity_max_threshold": device.humidity_max_threshold,
        "created_at": device.created_at,
        "latest_reading": serialize_reading(latest) if latest else None,
    }


def _latest_for_sensor(db: Session, sensor_id: str) -> SensorReading | None:
    return (
        db.query(SensorReading)
        .filter(SensorReading.sensor_id == sensor_id)
        .order_by(SensorReading.event_ts.desc(), SensorReading.id.desc())
        .first()
    )


def _publish_threshold_configs_for_sensor(device: IoTDevice) -> tuple[bool, dict[str, bool]]:
    results = {
        "temperature": mqtt_service.publish_threshold_config(
            sensor_id=device.source,
            metric_type="temperature",
            min_threshold=device.temperature_min_threshold,
            max_threshold=device.temperature_max_threshold,
            alert_enabled=bool(device.alert_enabled),
            unit="C",
            device_id=device.id,
        ),
        "humidity": mqtt_service.publish_threshold_config(
            sensor_id=device.source,
            metric_type="humidity",
            min_threshold=device.humidity_min_threshold,
            max_threshold=device.humidity_max_threshold,
            alert_enabled=bool(device.alert_enabled),
            unit="%",
            device_id=device.id,
        ),
    }
    return all(results.values()), results


def _sensor_device_or_404(db: Session, sensor_id: str, user: User) -> IoTDevice:
    device = db.query(IoTDevice).filter(IoTDevice.source == sensor_id).first()
    if not device or device.source_type == "virtual_meteostat" or (user.role != "admin" and device.user_id != user.id):
        raise HTTPException(status_code=404, detail="Sensor not found")
    return device


def _resolve_history_window(
    from_date: str | None,
    to_date: str | None,
    minutes: int,
) -> tuple[datetime, datetime | None]:
    if from_date or to_date:
        normalized_from = from_date or to_date
        normalized_to = to_date or from_date
        start = datetime.fromisoformat(str(normalized_from)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = datetime.fromisoformat(str(normalized_to)).replace(hour=23, minute=59, second=59, microsecond=999999)
        if start > end:
            start, end = end.replace(hour=0, minute=0, second=0, microsecond=0), start.replace(hour=23, minute=59, second=59, microsecond=999999)
        return start, end

    return datetime.now(VN_TZ).replace(tzinfo=None) - timedelta(minutes=minutes), None


class SensorCreateRequest(BaseModel):
    name: str
    sensor_id: str = Field(..., alias="source")
    source_type: str = "physical_iot"
    environment_type: str = "indoor"
    location: Optional[str] = None
    location_province: Optional[str] = None
    location_query: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    task_description: Optional[str] = None
    priority_level: Optional[str] = None
    action_hint: Optional[str] = None
    alert_enabled: bool = False
    temperature_min_threshold: Optional[float] = None
    temperature_max_threshold: Optional[float] = None
    humidity_min_threshold: Optional[float] = None
    humidity_max_threshold: Optional[float] = None

    class Config:
        populate_by_name = True


class SensorUpdateRequest(BaseModel):
    name: Optional[str] = None
    source_type: Optional[str] = None
    environment_type: Optional[str] = None
    location: Optional[str] = None
    location_province: Optional[str] = None
    location_query: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    task_description: Optional[str] = None
    priority_level: Optional[str] = None
    action_hint: Optional[str] = None
    is_active: Optional[bool] = None
    alert_enabled: Optional[bool] = None
    temperature_min_threshold: Optional[float] = None
    temperature_max_threshold: Optional[float] = None
    humidity_min_threshold: Optional[float] = None
    humidity_max_threshold: Optional[float] = None


class SensorReadingRequest(BaseModel):
    sensor_id: str
    timestamp: Optional[datetime] = None
    temperature: Optional[float] = None
    humidity: Optional[float] = None
    source_type: str = "physical_iot"
    provider: str = "esp32"
    environment_type: Optional[str] = None
    location: Optional[str] = None
    location_province: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None


class GeocodeRequest(BaseModel):
    location_query: str


@router.get("")
def list_sensors(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    query = db.query(IoTDevice).filter(IoTDevice.source_type != "virtual_meteostat")
    if user.role != "admin":
        query = query.filter(IoTDevice.user_id == user.id)
    devices = query.order_by(IoTDevice.id.asc()).all()
    return {
        "sensors": [_serialize_device(device, _latest_for_sensor(db, device.source)) for device in devices],
        "count": len(devices),
    }


@router.post("", status_code=status.HTTP_201_CREATED)
def create_sensor(payload: SensorCreateRequest, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    sensor_id = _normalize_source(payload.sensor_id)
    if not sensor_id:
        raise HTTPException(status_code=400, detail="sensor_id/source is required")
    existing = db.query(IoTDevice).filter(IoTDevice.source == sensor_id).first()
    if existing:
        raise HTTPException(status_code=400, detail=f"Sensor '{sensor_id}' already exists")

    environment_type = (payload.environment_type or "indoor").strip().lower()
    source_type = (payload.source_type or "physical_iot").strip().lower()
    if source_type != "physical_iot":
        raise HTTPException(status_code=400, detail="Virtual sensors have been removed. Please create a physical ESP32 sensor.")

    latitude = payload.latitude
    longitude = payload.longitude
    timezone_name = None
    if payload.location_query and (latitude is None or longitude is None):
        geo = geocode_location(payload.location_query)
        if geo:
            latitude = geo.latitude
            longitude = geo.longitude
            timezone_name = geo.timezone

    device = IoTDevice(
        user_id=user.id,
        name=payload.name.strip(),
        device_type="temperature_humidity",
        source=sensor_id,
        unit="C,%",
        source_type=source_type,
        capabilities="temperature,humidity",
        location=payload.location,
        location_province=payload.location_province,
        environment_type=environment_type,
        location_query=payload.location_query,
        latitude=latitude,
        longitude=longitude,
        timezone_name=timezone_name,
        task_description=(payload.task_description or "").strip() or None,
        priority_level=(payload.priority_level or "").strip().lower() or None,
        action_hint=(payload.action_hint or "").strip() or None,
        is_active=True,
        alert_enabled=payload.alert_enabled,
        min_threshold=None,
        max_threshold=None,
        temperature_min_threshold=payload.temperature_min_threshold,
        temperature_max_threshold=payload.temperature_max_threshold,
        humidity_min_threshold=payload.humidity_min_threshold,
        humidity_max_threshold=payload.humidity_max_threshold,
        created_by=user.id,
    )
    db.add(device)
    db.commit()
    db.refresh(device)
    mqtt_ok, mqtt_results = _publish_threshold_configs_for_sensor(device)
    response = _serialize_device(device)
    response["threshold_config_published"] = mqtt_ok
    response["threshold_config_results"] = mqtt_results
    response["threshold_sync_status"] = "published" if mqtt_ok else "db_only"
    return response


@router.get("/{sensor_id}")
def get_sensor(sensor_id: str, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    device = _sensor_device_or_404(db, sensor_id, user)
    return _serialize_device(device, _latest_for_sensor(db, sensor_id))


@router.patch("/{sensor_id}")
def update_sensor(sensor_id: str, payload: SensorUpdateRequest, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    device = _sensor_device_or_404(db, sensor_id, user)
    if "source_type" in payload.model_fields_set and payload.source_type not in {None, "physical_iot"}:
        raise HTTPException(status_code=400, detail="Virtual sensors have been removed from this system.")
    threshold_fields = {
        "alert_enabled",
        "temperature_min_threshold",
        "temperature_max_threshold",
        "humidity_min_threshold",
        "humidity_max_threshold",
    }
    thresholds_touched = bool(payload.model_fields_set & threshold_fields)
    for field in payload.model_fields_set:
        setattr(device, field, getattr(payload, field))
    if device.device_type == "temperature_humidity":
        # Sensor-level devices store temperature/humidity thresholds separately.
        # Keeping generic min/max in sync would make one metric accidentally
        # affect the other through legacy fallback alert logic.
        device.min_threshold = None
        device.max_threshold = None
    db.commit()
    db.refresh(device)
    response = _serialize_device(device, _latest_for_sensor(db, sensor_id))
    if thresholds_touched:
        mqtt_ok, mqtt_results = _publish_threshold_configs_for_sensor(device)
        response["threshold_config_published"] = mqtt_ok
        response["threshold_config_results"] = mqtt_results
        response["threshold_sync_status"] = "published" if mqtt_ok else "db_only"
    return response


@router.delete("/{sensor_id}")
def delete_sensor(sensor_id: str, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    device = _sensor_device_or_404(db, sensor_id, user)
    db.delete(device)
    db.commit()
    return {"message": "Sensor deleted"}


@router.post("/readings", status_code=status.HTTP_201_CREATED)
def ingest_reading(payload: SensorReadingRequest, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    device = db.query(IoTDevice).filter(IoTDevice.source == payload.sensor_id).first()
    if device and user.role != "admin" and device.user_id != user.id:
        raise HTTPException(status_code=403, detail="No permission for this sensor")
    row = create_sensor_reading(
        db,
        sensor_id=payload.sensor_id,
        event_ts=payload.timestamp,
        temperature=payload.temperature,
        humidity=payload.humidity,
        source_type=payload.source_type,
        provider=payload.provider,
        environment_type=payload.environment_type,
        location=payload.location,
        location_province=payload.location_province,
        latitude=payload.latitude,
        longitude=payload.longitude,
    )
    metric_ts = parse_event_ts(payload.timestamp)
    if payload.temperature is not None:
        check_and_trigger_metric_alert(
            db,
            metric_type="temperature",
            source=payload.sensor_id,
            value=float(payload.temperature),
            metric_ts=metric_ts,
            origin="api/sensors/readings",
        )
    if payload.humidity is not None:
        check_and_trigger_metric_alert(
            db,
            metric_type="humidity",
            source=payload.sensor_id,
            value=float(payload.humidity),
            metric_ts=metric_ts,
            origin="api/sensors/readings",
        )
    db.commit()
    db.refresh(row)
    return serialize_reading(row)


@router.get("/{sensor_id}/latest")
def latest_reading(sensor_id: str, response: Response, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    response.headers["Cache-Control"] = "no-store"
    _sensor_device_or_404(db, sensor_id, user)
    row = _latest_for_sensor(db, sensor_id)
    return serialize_reading(row) if row else {"sensor_id": sensor_id, "temperature": None, "humidity": None}


@router.get("/{sensor_id}/history")
def reading_history(sensor_id: str, minutes: int = Query(120, ge=1, le=525600), user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    _sensor_device_or_404(db, sensor_id, user)
    since, until = _resolve_history_window(None, None, minutes)
    query = (
        db.query(SensorReading)
        .filter(SensorReading.sensor_id == sensor_id, SensorReading.event_ts >= since)
        .order_by(SensorReading.event_ts.asc())
    )
    if until is not None:
        query = query.filter(SensorReading.event_ts <= until)
    rows = query.all()
    return {"sensor_id": sensor_id, "readings": [serialize_reading(row) for row in rows], "count": len(rows)}


@router.get("/{sensor_id}/history/export")
def export_sensor_history_csv(
    sensor_id: str,
    from_date: str | None = Query(default=None),
    to_date: str | None = Query(default=None),
    minutes: int = Query(1440, ge=1, le=525600),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    device = _sensor_device_or_404(db, sensor_id, user)
    try:
        since, until = _resolve_history_window(from_date, to_date, minutes)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid from_date/to_date. Use YYYY-MM-DD.") from exc

    query = (
        db.query(SensorReading)
        .filter(SensorReading.sensor_id == sensor_id, SensorReading.event_ts >= since)
        .order_by(SensorReading.event_ts.asc())
    )
    if until is not None:
        query = query.filter(SensorReading.event_ts <= until)
    rows = query.all()

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow([
        "sensor_id",
        "sensor_name",
        "event_ts",
        "temperature",
        "humidity",
        "temperature_unit",
        "humidity_unit",
        "source_type",
        "provider",
        "environment_type",
        "location",
        "location_province",
        "latitude",
        "longitude",
    ])
    for row in rows:
        writer.writerow([
            row.sensor_id,
            device.name,
            row.event_ts.isoformat() if row.event_ts else "",
            row.temperature if row.temperature is not None else "",
            row.humidity if row.humidity is not None else "",
            row.temperature_unit or "",
            row.humidity_unit or "",
            row.source_type or "",
            row.provider or "",
            row.environment_type or "",
            row.location or "",
            row.location_province or "",
            row.latitude if row.latitude is not None else "",
            row.longitude if row.longitude is not None else "",
        ])

    filename = f"{sensor_id}_history.csv"
    return Response(
        content="\ufeff" + buffer.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/geocode")
def geocode_sensor_location(payload: GeocodeRequest, user: User = Depends(get_current_user)):
    _ = user
    geo = geocode_location(payload.location_query)
    if not geo:
        raise HTTPException(status_code=404, detail="Location not found")
    return {
        "name": geo.name,
        "country": geo.country,
        "admin1": geo.admin1,
        "timezone": geo.timezone,
        "latitude": geo.latitude,
        "longitude": geo.longitude,
    }
