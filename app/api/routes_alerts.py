"""API routes for alerts endpoints."""

from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.api.routes_auth import get_current_user
from app.api.iot_backend_proxy import extract_bearer_token, proxy_iot_backend
from app.schemas import AlertCreate, AlertListResponse, AlertResponse
from app.services.email_service import email_config_debug, send_test_email

router = APIRouter(prefix="/api", tags=["alerts"])


class TestEmailRequest(BaseModel):
    email: str


@router.post("/notifications/test-email")
async def test_notification_email(payload: TestEmailRequest):
    """Send a test email immediately using SMTP settings from .env."""
    email = (payload.email or "").strip()
    if not email:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": "email is required"},
        )
    ok, detail, stage = send_test_email(email)
    if not ok:
        return JSONResponse(
            status_code=500,
            content={"success": False, "stage": stage, "error": detail},
        )
    return {"success": True, "message": "Test email sent"}


@router.get("/notifications/email-config-debug")
async def get_email_config_debug():
    """Return resolved email config metadata without exposing the password."""
    return email_config_debug()


@router.post("/alerts", response_model=AlertResponse, status_code=201)
async def create_alert(
    alert: AlertCreate,
    request: Request,
):
    return proxy_iot_backend(
        "POST",
        "/api/alerts",
        payload=alert.model_dump(mode="json"),
        bearer_token=extract_bearer_token(request),
    )


@router.get("/alerts", response_model=AlertListResponse)
async def get_alerts(
    request: Request,
    hours: int = Query(24, ge=1, le=720, description="Last N hours to fetch alerts"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of alerts"),
    current_user=Depends(get_current_user),
):
    _ = current_user
    query = urlencode({"hours": hours, "limit": limit})
    return proxy_iot_backend("GET", f"/api/alerts?{query}", bearer_token=extract_bearer_token(request))


@router.get("/alerts/recent", response_model=AlertListResponse)
async def get_recent_alerts(
    request: Request,
    hours: int = Query(24, ge=1, le=720, description="Last N hours to fetch alerts"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of alerts"),
    current_user=Depends(get_current_user),
):
    _ = current_user
    query = urlencode({"hours": hours, "limit": limit})
    return proxy_iot_backend("GET", f"/api/alerts/recent?{query}", bearer_token=extract_bearer_token(request))


@router.get("/alerts/unresolved", response_model=AlertListResponse)
async def get_unresolved_alerts(
    request: Request,
    current_user=Depends(get_current_user),
):
    _ = current_user
    return proxy_iot_backend("GET", "/api/alerts/unresolved", bearer_token=extract_bearer_token(request))


@router.get("/alerts/by-metric/{metric_type}", response_model=AlertListResponse)
async def get_alerts_by_metric(
    request: Request,
    metric_type: str,
    hours: int = Query(24, ge=1, le=720, description="Last N hours"),
    current_user=Depends(get_current_user),
):
    _ = current_user
    query = urlencode({"hours": hours})
    return proxy_iot_backend("GET", f"/api/alerts/by-metric/{metric_type}?{query}", bearer_token=extract_bearer_token(request))


@router.patch("/alerts/{alert_id}/resolve", response_model=AlertResponse)
async def resolve_alert(
    alert_id: int,
    request: Request,
):
    return proxy_iot_backend(
        "PATCH",
        f"/api/alerts/{alert_id}/resolve",
        bearer_token=extract_bearer_token(request),
    )


@router.delete("/alerts/cleanup")
async def cleanup_old_alerts(
    request: Request,
    days: int = Query(30, ge=1, le=365, description="Delete alerts older than N days"),
    current_user=Depends(get_current_user),
):
    _ = current_user
    query = urlencode({"days": days})
    return proxy_iot_backend("DELETE", f"/api/alerts/cleanup?{query}", bearer_token=extract_bearer_token(request))


@router.get("/alerts/{alert_id}/explain-ai")
async def explain_alert_with_ai(
    alert_id: int,
    request: Request,
    current_user=Depends(get_current_user),
):
    _ = current_user
    return proxy_iot_backend(
        "GET",
        f"/api/alerts/{alert_id}/explain-ai",
        bearer_token=extract_bearer_token(request),
    )
