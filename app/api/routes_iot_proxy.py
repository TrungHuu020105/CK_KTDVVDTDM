"""Proxy IoT control routes from local app backend to IoT backend service."""

from __future__ import annotations

import json
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from app.api.routes_auth import get_current_user
from app.config import IOT_BACKEND_URL
from app.models import User


router = APIRouter(prefix="/api/iot-control", tags=["iot-control"])
devices_router = APIRouter(prefix="/api/devices", tags=["iot-device-control"])


class WifiConfigProxyRequest(BaseModel):
    ssid: str = Field(..., min_length=1, max_length=64)
    password: str = Field(default="", max_length=64)
    sensor_id: str = Field(default="esp32_devkit_v1", min_length=1, max_length=100)


class WifiScanProxyRequest(BaseModel):
    sensor_id: str = Field(default="esp32_devkit_v1", min_length=1, max_length=100)


class WifiConfigByDeviceRequest(BaseModel):
    ssid: str = Field(..., min_length=1, max_length=64)
    password: str = Field(default="", max_length=64)


class ManualCommandByDeviceRequest(BaseModel):
    fan: bool | None = None
    mist: bool | None = None
    fog: bool | None = None
    lamp: bool | None = None
    auto: bool | None = None


def _post_iot(path: str, payload: dict, bearer_token: str | None = None) -> dict:
    url = f"{IOT_BACKEND_URL}{path}"
    print(f"[WIFI PROXY] forwarding to {url}")
    headers = {"Content-Type": "application/json"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"

    body = json.dumps(payload).encode("utf-8")
    req = urlrequest.Request(url, data=body, headers=headers, method="POST")

    try:
        with urlrequest.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
            result = json.loads(raw) if raw else {"status": "ok"}
            print(f"[WIFI PROXY] response={result}")
            return result
    except HTTPError as exc:
        detail = f"IoT backend error HTTP {exc.code}"
        try:
            err = json.loads(exc.read().decode("utf-8"))
            if isinstance(err, dict) and err.get("detail"):
                detail = err["detail"]
        except Exception:
            pass
        if exc.code == 404:
            detail = "IoT backend route not found. Check IOT_BACKEND_URL and that iot_backend is running."
        print(f"[WIFI PROXY] error={detail}")
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=detail) from exc
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"[WIFI PROXY] error={exc}")
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"IoT backend chưa chạy hoặc IOT_BACKEND_URL sai: {str(exc)}",
        ) from exc


def _get_iot(path: str, bearer_token: str | None = None) -> dict:
    url = f"{IOT_BACKEND_URL}{path}"
    print(f"[WIFI PROXY] forwarding to {url}")
    headers = {"Content-Type": "application/json"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    req = urlrequest.Request(url, headers=headers, method="GET")
    try:
        with urlrequest.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
            result = json.loads(raw) if raw else {"status": "ok"}
            print(f"[WIFI PROXY] response={result}")
            return result
    except HTTPError as exc:
        detail = f"IoT backend error HTTP {exc.code}"
        try:
            err = json.loads(exc.read().decode("utf-8"))
            if isinstance(err, dict) and err.get("detail"):
                detail = err["detail"]
        except Exception:
            pass
        if exc.code == 404:
            detail = "IoT backend route not found. Check IOT_BACKEND_URL and that iot_backend is running."
        print(f"[WIFI PROXY] error={detail}")
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=detail) from exc
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"[WIFI PROXY] error={exc}")
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"IoT backend chưa chạy hoặc IOT_BACKEND_URL sai: {str(exc)}",
        ) from exc


@router.post("/wifi-config")
async def proxy_wifi_config(
    payload: WifiConfigProxyRequest,
    request: Request,
    current_user: User = Depends(get_current_user),
):
    _ = current_user
    auth_header = request.headers.get("Authorization", "")
    bearer_token = None
    if auth_header.lower().startswith("bearer "):
        bearer_token = auth_header[7:].strip()

    response = _post_iot(
        path="/api/devices/wifi-config",
        payload={
            "ssid": payload.ssid,
            "password": payload.password,
            "sensor_id": payload.sensor_id,
        },
        bearer_token=bearer_token,
    )
    return response


@router.post("/wifi-scan")
async def proxy_request_wifi_scan(
    payload: WifiScanProxyRequest,
    request: Request,
    current_user: User = Depends(get_current_user),
):
    _ = current_user
    auth_header = request.headers.get("Authorization", "")
    bearer_token = None
    if auth_header.lower().startswith("bearer "):
        bearer_token = auth_header[7:].strip()

    print(f"[WIFI PROXY] POST /api/iot-control/wifi-scan sensor_id={payload.sensor_id}")
    return _post_iot(
        path="/api/devices/wifi-scan",
        payload={"sensor_id": payload.sensor_id},
        bearer_token=bearer_token,
    )


@router.get("/wifi-scan")
async def proxy_get_wifi_scan(
    request: Request,
    sensor_id: str = "esp32_devkit_v1",
    current_user: User = Depends(get_current_user),
):
    _ = current_user
    auth_header = request.headers.get("Authorization", "") if request else ""
    bearer_token = None
    if auth_header.lower().startswith("bearer "):
        bearer_token = auth_header[7:].strip()
    encoded_sensor_id = quote(sensor_id, safe="")
    print(f"[WIFI PROXY] GET /api/iot-control/wifi-scan sensor_id={sensor_id}")
    return _get_iot(
        path=f"/api/devices/wifi-scan?sensor_id={encoded_sensor_id}",
        bearer_token=bearer_token,
    )


@devices_router.post("/{device_id}/scan-wifi")
async def proxy_scan_wifi_by_device_source(
    device_id: str,
    request: Request,
    current_user: User = Depends(get_current_user),
):
    _ = current_user
    auth_header = request.headers.get("Authorization", "")
    bearer_token = auth_header[7:].strip() if auth_header.lower().startswith("bearer ") else None
    print(f"[WIFI API] scan-wifi called device_id={device_id}")
    print(f"[WIFI PROXY] POST /api/devices/{device_id}/scan-wifi sensor_id={device_id}")
    return _post_iot(
        path="/api/devices/wifi-scan",
        payload={"sensor_id": device_id},
        bearer_token=bearer_token,
    )


@devices_router.get("/{device_id}/wifi-list")
async def proxy_wifi_list_by_device_source(
    device_id: str,
    request: Request,
    current_user: User = Depends(get_current_user),
):
    _ = current_user
    auth_header = request.headers.get("Authorization", "")
    bearer_token = auth_header[7:].strip() if auth_header.lower().startswith("bearer ") else None
    print(f"[WIFI API] wifi-list called device_id={device_id}")
    encoded_sensor_id = quote(device_id, safe="")
    result = _get_iot(
        path=f"/api/devices/wifi-scan?sensor_id={encoded_sensor_id}",
        bearer_token=bearer_token,
    )
    networks = result.get("networks") if isinstance(result, dict) else []
    count = len(networks) if isinstance(networks, list) else 0
    print(f"[WIFI API] networks count={count}")
    return result


@devices_router.get("/{device_id}/wifi-status")
async def proxy_wifi_status_by_device_source(
    device_id: str,
    request: Request,
    current_user: User = Depends(get_current_user),
):
    _ = current_user
    auth_header = request.headers.get("Authorization", "")
    bearer_token = auth_header[7:].strip() if auth_header.lower().startswith("bearer ") else None
    print(f"[WIFI API] wifi-status called device_id={device_id}")
    return _get_iot(
        path=f"/api/devices/{quote(device_id, safe='')}/wifi-status",
        bearer_token=bearer_token,
    )


@devices_router.post("/{device_id}/wifi-config")
async def proxy_wifi_config_by_device_source(
    device_id: str,
    payload: WifiConfigByDeviceRequest,
    request: Request,
    current_user: User = Depends(get_current_user),
):
    _ = current_user
    auth_header = request.headers.get("Authorization", "")
    bearer_token = auth_header[7:].strip() if auth_header.lower().startswith("bearer ") else None
    print(f"[WIFI API] wifi-config called device_id={device_id} ssid={payload.ssid}")
    print(f"[WIFI PROXY] POST /api/devices/{device_id}/wifi-config sensor_id={device_id}")
    return _post_iot(
        path="/api/devices/wifi-config",
        payload={
            "sensor_id": device_id,
            "ssid": payload.ssid,
            "password": payload.password,
        },
        bearer_token=bearer_token,
    )


@devices_router.post("/{device_id}/manual-command")
async def proxy_manual_command_by_device_source(
    device_id: str,
    payload: ManualCommandByDeviceRequest,
    request: Request,
    current_user: User = Depends(get_current_user),
):
    _ = current_user
    auth_header = request.headers.get("Authorization", "")
    bearer_token = auth_header[7:].strip() if auth_header.lower().startswith("bearer ") else None
    return _post_iot(
        path=f"/api/devices/{quote(device_id, safe='')}/manual-command",
        payload=payload.model_dump(mode="json"),
        bearer_token=bearer_token,
    )
