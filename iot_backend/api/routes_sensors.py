"""Sensor-level API for physical and virtual IoT devices."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from iot_backend.api.routes_auth import get_current_user
from iot_backend.database import get_db
from iot_backend.models import IoTDevice, SensorReading, User
from iot_backend.services.sensor_reading_service import create_sensor_reading, parse_event_ts, serialize_reading
from iot_backend.services.weather_service import geocode_location, get_virtual_weather_readings

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
        "provider": "meteostat" if device.source_type == "virtual_meteostat" else "esp32",
        "environment_type": device.environment_type,
        "location": device.location,
        "location_province": device.location_province,
        "location_query": device.location_query,
        "latitude": device.latitude,
        "longitude": device.longitude,
        "timezone_name": device.timezone_name,
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


def _ensure_coordinates_for_device(device: IoTDevice, db: Session) -> None:
    if device.latitude is not None and device.longitude is not None:
        return

    query = device.location_query or device.location_province or device.location
    geo = geocode_location(query) if query else None
    if not geo:
        raise HTTPException(status_code=400, detail="Sensor location has no coordinates")

    device.latitude = geo.latitude
    device.longitude = geo.longitude
    device.timezone_name = geo.timezone
    db.flush()


@router.get("")
def list_sensors(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    query = db.query(IoTDevice)
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
    if source_type == "virtual_meteostat" and environment_type != "outdoor":
        raise HTTPException(status_code=400, detail="Virtual Meteostat sensors must be outdoor")

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
        is_active=True,
        alert_enabled=payload.alert_enabled,
        temperature_min_threshold=payload.temperature_min_threshold,
        temperature_max_threshold=payload.temperature_max_threshold,
        humidity_min_threshold=payload.humidity_min_threshold,
        humidity_max_threshold=payload.humidity_max_threshold,
        created_by=user.id,
    )
    db.add(device)
    db.commit()
    db.refresh(device)
    return _serialize_device(device)


@router.get("/{sensor_id}")
def get_sensor(sensor_id: str, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    device = db.query(IoTDevice).filter(IoTDevice.source == sensor_id).first()
    if not device or (user.role != "admin" and device.user_id != user.id):
        raise HTTPException(status_code=404, detail="Sensor not found")
    return _serialize_device(device, _latest_for_sensor(db, sensor_id))


@router.patch("/{sensor_id}")
def update_sensor(sensor_id: str, payload: SensorUpdateRequest, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    device = db.query(IoTDevice).filter(IoTDevice.source == sensor_id).first()
    if not device or (user.role != "admin" and device.user_id != user.id):
        raise HTTPException(status_code=404, detail="Sensor not found")
    for field in payload.model_fields_set:
        setattr(device, field, getattr(payload, field))
    if device.source_type == "virtual_meteostat" and device.environment_type != "outdoor":
        raise HTTPException(status_code=400, detail="Virtual Meteostat sensors must be outdoor")
    db.commit()
    db.refresh(device)
    return _serialize_device(device, _latest_for_sensor(db, sensor_id))


@router.delete("/{sensor_id}")
def delete_sensor(sensor_id: str, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    device = db.query(IoTDevice).filter(IoTDevice.source == sensor_id).first()
    if not device or (user.role != "admin" and device.user_id != user.id):
        raise HTTPException(status_code=404, detail="Sensor not found")
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
    db.commit()
    db.refresh(row)
    return serialize_reading(row)


@router.get("/{sensor_id}/latest")
def latest_reading(sensor_id: str, response: Response, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    response.headers["Cache-Control"] = "no-store"
    device = db.query(IoTDevice).filter(IoTDevice.source == sensor_id).first()
    if not device or (user.role != "admin" and device.user_id != user.id):
        raise HTTPException(status_code=404, detail="Sensor not found")
    row = _latest_for_sensor(db, sensor_id)
    return serialize_reading(row) if row else {"sensor_id": sensor_id, "temperature": None, "humidity": None}


@router.get("/{sensor_id}/history")
def reading_history(sensor_id: str, minutes: int = Query(120, ge=1, le=525600), user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    device = db.query(IoTDevice).filter(IoTDevice.source == sensor_id).first()
    if not device or (user.role != "admin" and device.user_id != user.id):
        raise HTTPException(status_code=404, detail="Sensor not found")
    since = datetime.now(VN_TZ).replace(tzinfo=None) - timedelta(minutes=minutes)
    rows = (
        db.query(SensorReading)
        .filter(SensorReading.sensor_id == sensor_id, SensorReading.event_ts >= since)
        .order_by(SensorReading.event_ts.asc())
        .all()
    )
    return {"sensor_id": sensor_id, "readings": [serialize_reading(row) for row in rows], "count": len(rows)}


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


@router.post("/{sensor_id}/sync-meteostat")
def sync_virtual_meteostat_sensor(
    sensor_id: str,
    hours: int = Query(24, ge=1, le=720),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    device = db.query(IoTDevice).filter(IoTDevice.source == sensor_id).first()
    if not device or (user.role != "admin" and device.user_id != user.id):
        raise HTTPException(status_code=404, detail="Sensor not found")
    if device.source_type != "virtual_meteostat":
        raise HTTPException(status_code=400, detail="Only virtual Meteostat sensors can be synced")
    if device.environment_type != "outdoor":
        raise HTTPException(status_code=400, detail="Virtual Meteostat sensors must be outdoor")

    _ensure_coordinates_for_device(device, db)
    readings, provider = get_virtual_weather_readings(
        latitude=device.latitude,
        longitude=device.longitude,
        hours=hours,
        timezone=device.timezone_name or "auto",
    )
    if not readings:
        raise HTTPException(status_code=502, detail="No weather readings available for this location")

    inserted = 0
    skipped = 0
    latest_row = None
    for item in readings:
        event_ts = item.get("timestamp")
        resolved_ts = parse_event_ts(event_ts)
        exists = (
            db.query(SensorReading)
            .filter(SensorReading.sensor_id == device.source, SensorReading.event_ts == resolved_ts)
            .first()
        )
        if exists:
            skipped += 1
            latest_row = exists
            continue

        latest_row = create_sensor_reading(
            db,
            sensor_id=device.source,
            event_ts=resolved_ts,
            temperature=item.get("temperature"),
            humidity=item.get("humidity"),
            source_type="virtual_meteostat",
            provider=item.get("provider") or provider,
            environment_type="outdoor",
            location=device.location,
            location_province=device.location_province,
            latitude=device.latitude,
            longitude=device.longitude,
        )
        inserted += 1

    db.commit()
    if latest_row:
        db.refresh(latest_row)
    return {
        "sensor_id": device.source,
        "provider": provider,
        "inserted": inserted,
        "skipped_duplicates": skipped,
        "latest_reading": serialize_reading(latest_row) if latest_row else None,
        "message": f"Synced {inserted} readings from {provider}",
    }
