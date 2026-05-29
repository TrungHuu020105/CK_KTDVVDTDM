"""Device control routes (ESP32 relay + WiFi config)."""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from iot_backend.state import runtime_state
from iot_backend import mqtt_service
from iot_backend.api.routes_auth import get_current_user
from iot_backend.database import get_db
from iot_backend.models import IoTDevice, User


router = APIRouter(prefix="/api/devices", tags=["devices"])


class ManualCommand(BaseModel):
    fan: Optional[bool] = None
    mist: Optional[bool] = None
    fog: Optional[bool] = None
    lamp: Optional[bool] = None
    auto: Optional[bool] = None


class WifiConfigRequest(BaseModel):
    ssid: str = Field(..., min_length=1, max_length=64)
    password: str = Field(default="", max_length=64)
    sensor_id: str = Field(default="esp32_devkit_v1", min_length=1, max_length=100)


class WifiScanRequest(BaseModel):
    sensor_id: str = Field(default="esp32_devkit_v1", min_length=1, max_length=100)


class ManualSensorCommandRequest(BaseModel):
    fan: Optional[bool] = None
    mist: Optional[bool] = None
    fog: Optional[bool] = None
    lamp: Optional[bool] = None
    auto: Optional[bool] = None


def _check_sensor_access(sensor_id: str, user: User, db: Session) -> None:
    is_admin = (user.role or "").strip().lower() == "admin"
    device_query = db.query(IoTDevice).filter(IoTDevice.source == sensor_id)
    if not is_admin:
        device_query = device_query.filter(IoTDevice.user_id == user.id)
    allowed_device = device_query.first()
    if not allowed_device:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission for this sensor",
        )


@router.get("")
async def get_devices():
    return runtime_state.response()


@router.post("")
async def set_devices(payload: ManualCommand):
    runtime_state.set_manual(
        fan=payload.fan,
        mist=payload.mist,
        fog=payload.fog,
        lamp=payload.lamp,
        auto=payload.auto,
    )
    response = runtime_state.response()
    mqtt_service.publish_commands("esp32_devkit_v1", response)
    return response


@router.post("/toggle-fan")
async def toggle_fan():
    runtime_state.set_fan_state(not runtime_state.devices.fan)
    response = runtime_state.response()
    mqtt_service.publish_commands("esp32_devkit_v1", response)
    return response


@router.post("/toggle-fog")
async def toggle_fog():
    runtime_state.set_fog_state(not runtime_state.devices.fog)
    response = runtime_state.response()
    mqtt_service.publish_commands("esp32_devkit_v1", response)
    return response


@router.post("/toggle-lamp")
async def toggle_lamp():
    runtime_state.devices.lamp = not runtime_state.devices.lamp
    response = runtime_state.response()
    mqtt_service.publish_commands("esp32_devkit_v1", response)
    return response


@router.post("/auto-mode")
async def enable_auto_mode():
    runtime_state.devices.auto = True
    response = runtime_state.response()
    mqtt_service.publish_commands("esp32_devkit_v1", response)
    return response


@router.post("/manual-mode")
async def enable_manual_mode():
    runtime_state.devices.auto = False
    response = runtime_state.response()
    mqtt_service.publish_commands("esp32_devkit_v1", response)
    return response


@router.post("/{sensor_id}/manual-command")
async def send_manual_command_to_sensor(
    sensor_id: str,
    payload: ManualSensorCommandRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _check_sensor_access(sensor_id, user, db)

    fog_value = payload.fog if payload.fog is not None else payload.mist
    ok = mqtt_service.publish_manual_command(
        sensor_id=sensor_id,
        fan=payload.fan,
        fog=fog_value,
        lamp=payload.lamp,
        auto=payload.auto,
    )
    if not ok:
        raise HTTPException(status_code=503, detail="MQTT unavailable, manual command not sent")

    return {
        "status": "sent",
        "sensor_id": sensor_id,
        "fan": payload.fan,
        "fog": fog_value,
        "lamp": payload.lamp,
        "auto": payload.auto,
    }


@router.get("/mqtt-status")
async def get_mqtt_status():
    return mqtt_service.status()


@router.post("/wifi-config")
async def update_wifi_config(
    payload: WifiConfigRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _check_sensor_access(payload.sensor_id, user, db)

    ok = mqtt_service.publish_wifi_config(
        sensor_id=payload.sensor_id,
        ssid=payload.ssid,
        password=payload.password,
    )
    if not ok:
        raise HTTPException(status_code=503, detail="MQTT unavailable, WiFi config not sent")
    return {"status": "sent", "sensor_id": payload.sensor_id, "ssid": payload.ssid}


@router.post("/wifi-scan")
async def request_wifi_scan(
    payload: WifiScanRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _check_sensor_access(payload.sensor_id, user, db)
    print(f"[WIFI SCAN] publish scan request to MQTT topic ptdl/devices/{payload.sensor_id}/commands")
    ok = mqtt_service.publish_wifi_scan_request(sensor_id=payload.sensor_id)
    if not ok:
        raise HTTPException(status_code=503, detail="MQTT unavailable, WiFi scan request not sent")
    return {"status": "requested", "sensor_id": payload.sensor_id}


@router.get("/wifi-scan")
async def get_wifi_scan(
    sensor_id: str = "esp32_devkit_v1",
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _check_sensor_access(sensor_id, user, db)
    print(f"[WIFI SCAN] waiting result from topic ptdl/devices/{sensor_id}/wifi-list")
    result = mqtt_service.get_wifi_scan_result(sensor_id=sensor_id)
    if not result:
        print("[WIFI SCAN] cached networks count=0")
        return {"status": "empty", "sensor_id": sensor_id, "networks": []}

    payload = result.get("payload") or {}
    networks = payload.get("networks") if isinstance(payload, dict) else []
    if not isinstance(networks, list):
        networks = []
    print(f"[WIFI SCAN] cached networks count={len(networks)}")
    return {
        "status": "ok",
        "sensor_id": sensor_id,
        "received_at": result.get("received_at"),
        "timestamp": payload.get("timestamp"),
        "networks": networks,
    }


@router.get("/{sensor_id}/wifi-status")
async def get_wifi_status_by_sensor(
    sensor_id: str,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _check_sensor_access(sensor_id, user, db)
    return mqtt_service.get_wifi_status(sensor_id=sensor_id)
