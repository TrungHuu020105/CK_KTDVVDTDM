"""Thin gateway routes for admin IoT management."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from app.api.iot_backend_proxy import extract_bearer_token, proxy_iot_backend
from app.api.routes_auth import get_current_user
from app.models import User


router = APIRouter(prefix="/api/admin", tags=["admin-iot"])


def verify_admin(user: User = Depends(get_current_user)) -> User:
    if user.role != "admin":
        from fastapi import HTTPException, status

        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Only admins can access this endpoint")
    return user


@router.get("/iot-devices")
async def get_all_iot_devices(request: Request, admin: User = Depends(verify_admin)):
    _ = admin
    return proxy_iot_backend("GET", "/api/admin/iot-devices", bearer_token=extract_bearer_token(request))


@router.get("/iot-devices/users-summary")
async def get_iot_devices_summary(request: Request, admin: User = Depends(verify_admin)):
    _ = admin
    return proxy_iot_backend(
        "GET",
        "/api/admin/iot-devices/users-summary",
        bearer_token=extract_bearer_token(request),
    )


@router.delete("/iot-devices/{device_id}")
async def delete_iot_device(device_id: int, request: Request, admin: User = Depends(verify_admin)):
    _ = admin
    return proxy_iot_backend(
        "DELETE",
        f"/api/admin/iot-devices/{device_id}",
        bearer_token=extract_bearer_token(request),
    )


@router.put("/iot-devices/{device_id}/disconnect")
async def disconnect_iot_device(device_id: int, request: Request, admin: User = Depends(verify_admin)):
    _ = admin
    return proxy_iot_backend(
        "PUT",
        f"/api/admin/iot-devices/{device_id}/disconnect",
        bearer_token=extract_bearer_token(request),
    )


@router.put("/iot-devices/{device_id}/reconnect")
async def reconnect_iot_device(device_id: int, request: Request, admin: User = Depends(verify_admin)):
    _ = admin
    return proxy_iot_backend(
        "PUT",
        f"/api/admin/iot-devices/{device_id}/reconnect",
        bearer_token=extract_bearer_token(request),
    )
