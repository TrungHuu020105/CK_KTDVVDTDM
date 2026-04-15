"""IoT Device management routes backed by Databricks metadata."""

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from typing import Optional
from ..databricks_client import get_databricks_client

router = APIRouter(prefix="/api/iot-devices", tags=["iot-devices"])


# ============== SCHEMAS ==============

class CreateIoTDeviceRequest(BaseModel):
    """Request schema for creating IoT device"""
    name: str
    device_type: str
    source: str
    location: Optional[str] = None


class UpdateIoTDeviceRequest(BaseModel):
    """Request schema for updating IoT device"""
    name: Optional[str] = None
    location: Optional[str] = None
    is_active: Optional[bool] = None


class UpdateAlertThresholdsRequest(BaseModel):
    """Request schema for updating alert thresholds"""
    alert_enabled: bool = False
    lower_threshold: Optional[float] = None  # Lower threshold (values below trigger alert)
    upper_threshold: Optional[float] = None  # Upper threshold (values above trigger alert)


DEVICE_DEFAULTS = {
    "temperature": {"unit": "°C", "min": 15.0, "max": 35.0, "mean": 25.0, "std": 2.0},
    "humidity": {"unit": "%", "min": 30.0, "max": 80.0, "mean": 55.0, "std": 8.0},
    "soil_moisture": {"unit": "%", "min": 20.0, "max": 80.0, "mean": 50.0, "std": 10.0},
    "light_intensity": {"unit": "lux", "min": 100.0, "max": 1000.0, "mean": 500.0, "std": 120.0},
    "pressure": {"unit": "hPa", "min": 950.0, "max": 1050.0, "mean": 1000.0, "std": 12.0},
}


def _esc(value: str) -> str:
    return (value or "").replace("'", "''")


def _run_sql(statement: str):
    client = get_databricks_client()
    result = client.execute_query(statement, timeout=90)
    if result.get("error"):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Databricks query failed: {result['error']}",
        )
    return result


# ============== IoT DEVICE MANAGEMENT ==============

@router.post("")
async def create_iot_device(
    device_data: CreateIoTDeviceRequest,
):
    """Create a new IoT device directly in Databricks metadata tables."""
    try:
        device_type = (device_data.device_type or "").strip().lower()
        if device_type not in DEVICE_DEFAULTS:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported device_type '{device_type}'",
            )

        source = (device_data.source or "").strip()
        if not source:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="source is required")

        client = get_databricks_client()
        existing = client.get_all_devices()
        if any((d.get("device_id") or "").lower() == source.lower() for d in existing):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Device source '{source}' already exists in Databricks",
            )

        profile = DEVICE_DEFAULTS[device_type]
        name = _esc(device_data.name.strip())
        location = _esc((device_data.location or "Unknown").strip())
        source_esc = _esc(source)
        unit_esc = _esc(profile["unit"])

        # 1) Persist new device metadata to Databricks.
        _run_sql(
            f"""
            INSERT INTO {client.catalog}.{client.schema}.iot_device_metadata
            (device_id, device_name, device_type, location, unit, min_value, max_value, mean_value, std_dev, active, created_at)
            VALUES
            ('{source_esc}', '{name}', '{device_type}', '{location}', '{unit_esc}',
             {profile['min']}, {profile['max']}, {profile['mean']}, {profile['std']}, true, CURRENT_TIMESTAMP())
            """
        )

        # 2) Seed latest table so UI gets value immediately after refresh.
        _run_sql(
            f"""
            INSERT INTO {client.catalog}.{client.schema}.iot_latest_readings
            (device_id, device_name, device_type, location, latest_value, unit, last_update, status)
            VALUES
            ('{source_esc}', '{name}', '{device_type}', '{location}', {profile['mean']}, '{unit_esc}', CURRENT_TIMESTAMP(), 'NORMAL')
            """
        )

        # 3) Seed raw sensor table once so history chart can load immediately.
        _run_sql(
            f"""
            INSERT INTO {client.catalog}.{client.schema}.iot_sensor_data
            (timestamp, device_id, device_name, device_type, location, value, unit, batch_id, _processing_time)
            VALUES
            (CURRENT_TIMESTAMP(), '{source_esc}', '{name}', '{device_type}', '{location}', {profile['mean']}, '{unit_esc}',
             'manual_seed', CURRENT_TIMESTAMP())
            """
        )

        return {
            "id": source,
            "name": device_data.name.strip(),
            "device_type": device_type,
            "source": source,
            "location": device_data.location,
            "unit": profile["unit"],
            "min_value": profile["min"],
            "max_value": profile["max"],
            "mean_value": profile["mean"],
            "std_dev": profile["std"],
            "is_active": True,
            "alert_enabled": False,
            "lower_threshold": None,
            "upper_threshold": None,
            "created_at": None,
            "message": "Device created in Databricks successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create device in Databricks: {str(e)}"
        )


@router.get("")
async def get_my_iot_devices(
    
):
    """Get all IoT devices from Databricks metadata."""
    devices = get_databricks_client().get_all_devices()
    
    return {
        "devices": [
            {
                "id": d.get("device_id"),
                "name": d.get("device_name"),
                "device_type": d.get("device_type"),
                "source": d.get("device_id"),
                "location": d.get("location"),
                "unit": d.get("unit"),
                "min_value": d.get("min_value"),
                "max_value": d.get("max_value"),
                "mean_value": d.get("mean_value"),
                "std_dev": d.get("std_dev"),
                "is_active": d.get("active", True),
                "alert_enabled": False,
                "lower_threshold": None,
                "upper_threshold": None,
                "created_at": d.get("created_at")
            }
            for d in devices
        ],
        "count": len(devices)
    }


@router.put("/{device_id}")
async def update_iot_device(
    device_id: str,
    update_data: UpdateIoTDeviceRequest,
):
    """Update device metadata in Databricks."""
    client = get_databricks_client()
    dev = _esc(device_id)

    if update_data.name is not None:
        _run_sql(
            f"UPDATE {client.catalog}.{client.schema}.iot_device_metadata "
            f"SET device_name = '{_esc(update_data.name)}' WHERE device_id = '{dev}'"
        )
        _run_sql(
            f"UPDATE {client.catalog}.{client.schema}.iot_latest_readings "
            f"SET device_name = '{_esc(update_data.name)}' WHERE device_id = '{dev}'"
        )

    if update_data.location is not None:
        _run_sql(
            f"UPDATE {client.catalog}.{client.schema}.iot_device_metadata "
            f"SET location = '{_esc(update_data.location)}' WHERE device_id = '{dev}'"
        )
        _run_sql(
            f"UPDATE {client.catalog}.{client.schema}.iot_latest_readings "
            f"SET location = '{_esc(update_data.location)}' WHERE device_id = '{dev}'"
        )

    if update_data.is_active is not None:
        active_sql = "true" if update_data.is_active else "false"
        status_text = "NORMAL" if update_data.is_active else "INACTIVE"
        _run_sql(
            f"UPDATE {client.catalog}.{client.schema}.iot_device_metadata "
            f"SET active = {active_sql} WHERE device_id = '{dev}'"
        )
        _run_sql(
            f"UPDATE {client.catalog}.{client.schema}.iot_latest_readings "
            f"SET status = '{status_text}' WHERE device_id = '{dev}'"
        )

    updated = next((d for d in client.get_all_devices() if (d.get("device_id") or "") == device_id), None)
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Device not found")

    return {
        "id": updated.get("device_id"),
        "name": updated.get("device_name"),
        "device_type": updated.get("device_type"),
        "source": updated.get("device_id"),
        "location": updated.get("location"),
        "unit": updated.get("unit"),
        "min_value": updated.get("min_value"),
        "max_value": updated.get("max_value"),
        "mean_value": updated.get("mean_value"),
        "std_dev": updated.get("std_dev"),
        "is_active": updated.get("active", True),
        "alert_enabled": False,
        "lower_threshold": None,
        "upper_threshold": None,
        "created_at": updated.get("created_at")
    }


@router.delete("/{device_id}")
async def delete_iot_device(
    device_id: str,
):
    """Delete device metadata from Databricks (history table kept)."""
    client = get_databricks_client()
    dev = _esc(device_id)

    _run_sql(f"DELETE FROM {client.catalog}.{client.schema}.iot_latest_readings WHERE device_id = '{dev}'")
    _run_sql(f"DELETE FROM {client.catalog}.{client.schema}.iot_device_metadata WHERE device_id = '{dev}'")

    return {"message": "Device deleted from Databricks metadata successfully"}


@router.put("/{device_id}/alert-thresholds")
async def update_alert_thresholds(
    device_id: str,
    alert_data: UpdateAlertThresholdsRequest,
):
    """Alert thresholds are not persisted in Databricks metadata schema."""
    return {
        "id": device_id,
        "alert_enabled": alert_data.alert_enabled,
        "lower_threshold": alert_data.lower_threshold,
        "upper_threshold": alert_data.upper_threshold,
        "message": "Alert thresholds applied in UI only (Databricks schema has no threshold columns)"
    }
