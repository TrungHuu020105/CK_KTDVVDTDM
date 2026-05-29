"""Sensor-level reading persistence and compatibility helpers."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional

from sqlalchemy.orm import Session

from iot_backend.models import IoTDevice, Metric, SensorReading
from iot_backend.services.databricks_service import write_bronze_sensor_reading

VN_TZ = timezone(timedelta(hours=7))


def parse_event_ts(value: str | datetime | None) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(VN_TZ).replace(tzinfo=None)
        return value
    if value:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                return parsed.astimezone(VN_TZ).replace(tzinfo=None)
            return parsed
        except Exception:
            pass
    return datetime.now(VN_TZ).replace(tzinfo=None)


def _find_device(db: Session, sensor_id: str) -> Optional[IoTDevice]:
    return (
        db.query(IoTDevice)
        .filter(IoTDevice.source == sensor_id)
        .order_by(IoTDevice.id.asc())
        .first()
    )


def serialize_reading(row: SensorReading) -> dict:
    return {
        "id": row.id,
        "device_id": row.device_id,
        "sensor_id": row.sensor_id,
        "event_ts": row.event_ts,
        "timestamp": row.event_ts,
        "temperature": row.temperature,
        "humidity": row.humidity,
        "temperature_unit": row.temperature_unit,
        "humidity_unit": row.humidity_unit,
        "source_type": row.source_type,
        "provider": row.provider,
        "environment_type": row.environment_type,
        "location": row.location,
        "location_province": row.location_province,
        "latitude": row.latitude,
        "longitude": row.longitude,
        "databricks_status": row.databricks_status,
    }


def create_sensor_reading(
    db: Session,
    *,
    sensor_id: str,
    event_ts: str | datetime | None = None,
    temperature: float | None = None,
    humidity: float | None = None,
    temperature_unit: str = "C",
    humidity_unit: str = "%",
    source_type: str = "physical_iot",
    provider: str = "esp32",
    environment_type: str | None = None,
    location: str | None = None,
    location_province: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    write_databricks: bool = True,
) -> SensorReading:
    sensor_id = str(sensor_id or "").strip()
    if not sensor_id:
        raise ValueError("sensor_id is required")
    if temperature is None and humidity is None:
        raise ValueError("temperature or humidity is required")

    device = _find_device(db, sensor_id)
    resolved_ts = parse_event_ts(event_ts)
    row = SensorReading(
        device_id=device.id if device else None,
        sensor_id=sensor_id,
        event_ts=resolved_ts,
        temperature=float(temperature) if temperature is not None else None,
        humidity=float(humidity) if humidity is not None else None,
        temperature_unit=temperature_unit or "C",
        humidity_unit=humidity_unit or "%",
        source_type=source_type or getattr(device, "source_type", None) or "physical_iot",
        provider=provider or "esp32",
        environment_type=environment_type or getattr(device, "environment_type", None) or "indoor",
        location=location or getattr(device, "location", None),
        location_province=location_province or getattr(device, "location_province", None),
        latitude=latitude if latitude is not None else getattr(device, "latitude", None),
        longitude=longitude if longitude is not None else getattr(device, "longitude", None),
        databricks_status="pending",
    )
    db.add(row)
    db.flush()

    # Compatibility layer for old metric-based screens/scripts while the new
    # project architecture uses sensor_readings as the canonical table.
    if row.temperature is not None:
        db.add(Metric(
            event_ts=resolved_ts,
            sensor_id=sensor_id,
            location=row.location,
            metric_type="temperature",
            metric_value=row.temperature,
            unit=row.temperature_unit,
        ))
    if row.humidity is not None:
        db.add(Metric(
            event_ts=resolved_ts,
            sensor_id=sensor_id,
            location=row.location,
            metric_type="humidity",
            metric_value=row.humidity,
            unit=row.humidity_unit,
        ))

    if write_databricks:
        ok, status = write_bronze_sensor_reading({
            "sensor_id": row.sensor_id,
            "event_ts": row.event_ts,
            "temperature": row.temperature,
            "humidity": row.humidity,
            "temperature_unit": row.temperature_unit,
            "humidity_unit": row.humidity_unit,
            "source_type": row.source_type,
            "provider": row.provider,
            "environment_type": row.environment_type,
            "location": row.location,
            "location_province": row.location_province,
            "latitude": row.latitude,
            "longitude": row.longitude,
        })
        row.databricks_status = "written" if ok else f"skipped:{status}"[:30]

    return row
