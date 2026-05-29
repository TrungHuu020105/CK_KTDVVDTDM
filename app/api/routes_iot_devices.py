"""Thin gateway routes for IoT device management."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from app.api.iot_backend_proxy import extract_bearer_token, proxy_iot_backend
from app.api.routes_auth import get_current_user


router = APIRouter(prefix="/api/iot-devices", tags=["iot-devices"])


class CreateIoTDeviceRequest(BaseModel):
    name: str
    device_type: str | None = None
    metric_type: str | None = None
    source: str
    unit: str | None = None
    location: str | None = None
    category: str | None = None
    environment_type: str = "indoor"
    min_threshold: float | None = None
    max_threshold: float | None = None
    location_query: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    task_description: str | None = None
    priority_level: str | None = None
    action_hint: str | None = None


class UpdateIoTDeviceRequest(BaseModel):
    name: str | None = None
    source: str | None = None
    device_type: str | None = None
    metric_type: str | None = None
    unit: str | None = None
    location: str | None = None
    category: str | None = None
    is_active: bool | None = None
    alert_enabled: bool | None = None
    min_threshold: float | None = None
    max_threshold: float | None = None
    environment_type: str | None = None
    location_query: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    task_description: str | None = None
    priority_level: str | None = None
    action_hint: str | None = None


class UpdateAlertThresholdsRequest(BaseModel):
    alert_enabled: bool | None = None
    min_threshold: float | None = None
    max_threshold: float | None = None


class GeocodeRequest(BaseModel):
    location_query: str


@router.get("")
async def get_my_iot_devices(request: Request, current_user=Depends(get_current_user)):
    _ = current_user
    return proxy_iot_backend("GET", "/api/iot-devices", bearer_token=extract_bearer_token(request))


@router.get("/realtime")
async def get_my_iot_devices_realtime(request: Request, current_user=Depends(get_current_user)):
    _ = current_user
    return proxy_iot_backend("GET", "/api/iot-devices/realtime", bearer_token=extract_bearer_token(request))


@router.post("")
async def create_iot_device(
    payload: CreateIoTDeviceRequest,
    request: Request,
    current_user=Depends(get_current_user),
):
    _ = current_user
    return proxy_iot_backend(
        "POST",
        "/api/iot-devices",
        payload=payload.model_dump(mode="json"),
        bearer_token=extract_bearer_token(request),
    )


@router.put("/{device_id}")
async def update_iot_device(
    device_id: int,
    payload: UpdateIoTDeviceRequest,
    request: Request,
    current_user=Depends(get_current_user),
):
    _ = current_user
    return proxy_iot_backend(
        "PUT",
        f"/api/iot-devices/{device_id}",
        payload=payload.model_dump(mode="json"),
        bearer_token=extract_bearer_token(request),
    )


@router.delete("/{device_id}")
async def delete_iot_device(device_id: int, request: Request, current_user=Depends(get_current_user)):
    _ = current_user
    return proxy_iot_backend(
        "DELETE",
        f"/api/iot-devices/{device_id}",
        bearer_token=extract_bearer_token(request),
    )


@router.put("/{device_id}/alert-thresholds")
async def update_alert_thresholds(
    device_id: int,
    payload: UpdateAlertThresholdsRequest,
    request: Request,
    current_user=Depends(get_current_user),
):
    _ = current_user
    return proxy_iot_backend(
        "PUT",
        f"/api/iot-devices/{device_id}/alert-thresholds",
        payload=payload.model_dump(mode="json"),
        bearer_token=extract_bearer_token(request),
    )


@router.post("/geocode")
async def geocode_sensor_location(
    payload: GeocodeRequest,
    request: Request,
    current_user=Depends(get_current_user),
):
    _ = current_user
    return proxy_iot_backend(
        "POST",
        "/api/iot-devices/geocode",
        payload=payload.model_dump(mode="json"),
        bearer_token=extract_bearer_token(request),
    )
