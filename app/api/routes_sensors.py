"""Gateway routes for sensor-level IoT and Databricks results."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel

from app.api.iot_backend_proxy import extract_bearer_token, proxy_iot_backend, proxy_iot_backend_raw
from app.api.routes_auth import get_current_user
from app.services.databricks_service import DatabricksService

router = APIRouter(prefix="/api/sensors", tags=["sensors"])


def _set_no_cache_headers(response: Response) -> None:
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"


class SensorCreateRequest(BaseModel):
    name: str
    source: str
    source_type: str = "physical_iot"
    environment_type: str = "indoor"
    location: str | None = None
    location_province: str | None = None
    location_query: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    task_description: str | None = None
    priority_level: str | None = None
    action_hint: str | None = None
    alert_enabled: bool = False
    temperature_min_threshold: float | None = None
    temperature_max_threshold: float | None = None
    humidity_min_threshold: float | None = None
    humidity_max_threshold: float | None = None


class SensorUpdateRequest(BaseModel):
    name: str | None = None
    source_type: str | None = None
    environment_type: str | None = None
    location: str | None = None
    location_province: str | None = None
    location_query: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    task_description: str | None = None
    priority_level: str | None = None
    action_hint: str | None = None
    is_active: bool | None = None
    alert_enabled: bool | None = None
    temperature_min_threshold: float | None = None
    temperature_max_threshold: float | None = None
    humidity_min_threshold: float | None = None
    humidity_max_threshold: float | None = None


class SensorReadingRequest(BaseModel):
    sensor_id: str
    timestamp: str | None = None
    temperature: float | None = None
    humidity: float | None = None
    source_type: str = "physical_iot"
    provider: str = "api"
    environment_type: str | None = None
    location: str | None = None
    location_province: str | None = None
    latitude: float | None = None
    longitude: float | None = None


class GeocodeRequest(BaseModel):
    location_query: str


@router.get("")
async def list_sensors(request: Request, current_user=Depends(get_current_user)):
    _ = current_user
    return proxy_iot_backend("GET", "/api/sensors", bearer_token=extract_bearer_token(request))


@router.post("")
async def create_sensor(payload: SensorCreateRequest, request: Request, current_user=Depends(get_current_user)):
    _ = current_user
    return proxy_iot_backend(
        "POST",
        "/api/sensors",
        payload=payload.model_dump(mode="json"),
        bearer_token=extract_bearer_token(request),
    )


@router.get("/databricks/status")
async def databricks_status(current_user=Depends(get_current_user)):
    _ = current_user
    return DatabricksService.status()


@router.post("/readings")
async def ingest_reading(payload: SensorReadingRequest, request: Request, current_user=Depends(get_current_user)):
    _ = current_user
    return proxy_iot_backend(
        "POST",
        "/api/sensors/readings",
        payload=payload.model_dump(mode="json"),
        bearer_token=extract_bearer_token(request),
    )


@router.post("/geocode")
async def geocode(payload: GeocodeRequest, request: Request, current_user=Depends(get_current_user)):
    _ = current_user
    return proxy_iot_backend(
        "POST",
        "/api/sensors/geocode",
        payload=payload.model_dump(mode="json"),
        bearer_token=extract_bearer_token(request),
    )


@router.get("/{sensor_id}")
async def get_sensor(sensor_id: str, request: Request, current_user=Depends(get_current_user)):
    _ = current_user
    return proxy_iot_backend("GET", f"/api/sensors/{sensor_id}", bearer_token=extract_bearer_token(request))


@router.patch("/{sensor_id}")
async def update_sensor(sensor_id: str, payload: SensorUpdateRequest, request: Request, current_user=Depends(get_current_user)):
    _ = current_user
    return proxy_iot_backend(
        "PATCH",
        f"/api/sensors/{sensor_id}",
        payload=payload.model_dump(mode="json", exclude_unset=True),
        bearer_token=extract_bearer_token(request),
    )


@router.delete("/{sensor_id}")
async def delete_sensor(sensor_id: str, request: Request, current_user=Depends(get_current_user)):
    _ = current_user
    return proxy_iot_backend("DELETE", f"/api/sensors/{sensor_id}", bearer_token=extract_bearer_token(request))


@router.get("/{sensor_id}/latest")
async def latest(sensor_id: str, request: Request, response: Response, current_user=Depends(get_current_user)):
    _ = current_user
    _set_no_cache_headers(response)
    return proxy_iot_backend("GET", f"/api/sensors/{sensor_id}/latest", bearer_token=extract_bearer_token(request))


@router.get("/{sensor_id}/history")
async def history(sensor_id: str, request: Request, response: Response, minutes: int = 120, current_user=Depends(get_current_user)):
    _ = current_user
    _set_no_cache_headers(response)
    return proxy_iot_backend("GET", f"/api/sensors/{sensor_id}/history?minutes={minutes}", bearer_token=extract_bearer_token(request))


@router.get("/{sensor_id}/history/export")
async def export_history_csv(
    sensor_id: str,
    request: Request,
    from_date: str | None = None,
    to_date: str | None = None,
    current_user=Depends(get_current_user),
):
    _ = current_user
    query_parts: list[str] = []
    if from_date:
        query_parts.append(f"from_date={from_date}")
    if to_date:
        query_parts.append(f"to_date={to_date}")
    query_string = f"?{'&'.join(query_parts)}" if query_parts else ""
    content, media_type, content_disposition = proxy_iot_backend_raw(
        "GET",
        f"/api/sensors/{sensor_id}/history/export{query_string}",
        bearer_token=extract_bearer_token(request),
    )
    headers: dict[str, str] = {}
    if content_disposition:
        headers["Content-Disposition"] = content_disposition
    return Response(content=content, media_type=media_type, headers=headers)


@router.get("/{sensor_id}/forecast")
async def forecast(sensor_id: str, request: Request, current_user=Depends(get_current_user)):
    _ = current_user
    sensor_metadata = {}
    try:
        sensor_metadata = proxy_iot_backend(
            "GET",
            f"/api/sensors/{sensor_id}",
            bearer_token=extract_bearer_token(request),
        )
    except HTTPException:
        sensor_metadata = {}
    return DatabricksService.fetch_forecast(sensor_id, sensor_metadata=sensor_metadata)


@router.get("/{sensor_id}/model-leaderboard")
async def model_leaderboard(sensor_id: str, request: Request, current_user=Depends(get_current_user)):
    _ = current_user
    sensor_metadata = {}
    try:
        sensor_metadata = proxy_iot_backend(
            "GET",
            f"/api/sensors/{sensor_id}",
            bearer_token=extract_bearer_token(request),
        )
    except HTTPException:
        sensor_metadata = {}
    return DatabricksService.fetch_model_leaderboard(sensor_id, sensor_metadata=sensor_metadata)
