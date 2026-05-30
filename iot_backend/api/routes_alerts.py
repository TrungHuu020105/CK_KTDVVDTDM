"""API routes for alerts endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from iot_backend.database import get_db
from iot_backend.models import Alert, IoTDevice, User
from iot_backend.schemas import (
    AlertCreate,
    AlertResponse,
    AlertListResponse
)
from iot_backend import crud
from iot_backend.api.routes_auth import get_current_user
from iot_backend.services.ai_explanation_service import explain_alert_with_gemini
from iot_backend.services.weather_service import get_weather_for_timestamp

router = APIRouter(prefix="/api", tags=["alerts"])


def _owned_iot_device_ids(db: Session, user_id: int) -> set[int]:
    rows = db.query(IoTDevice.id).filter(IoTDevice.user_id == user_id).all()
    return {row[0] for row in rows}


def _filter_alerts_by_user(db: Session, current_user: User, alerts: list[Alert]) -> list[Alert]:
    if current_user.role == "admin":
        return alerts
    owned_device_ids = _owned_iot_device_ids(db, current_user.id)
    accessible_sources = set(crud.get_user_accessible_sources(db, current_user.id))
    visible: list[Alert] = []
    for alert in alerts:
        if alert.device_id is not None:
            if alert.device_id in owned_device_ids:
                visible.append(alert)
            continue
        if alert.source in accessible_sources:
            visible.append(alert)
    return visible


def _ensure_alert_access(db: Session, current_user: User, alert: Alert) -> None:
    if current_user.role == "admin":
        return
    owned_device_ids = _owned_iot_device_ids(db, current_user.id)
    if alert.device_id is not None and alert.device_id in owned_device_ids:
        return
    accessible_sources = set(crud.get_user_accessible_sources(db, current_user.id))
    if alert.source not in accessible_sources:
        raise HTTPException(status_code=403, detail="You do not have access to this alert")


@router.post("/alerts", response_model=AlertResponse, status_code=201)
async def create_alert(
    alert: AlertCreate,
    db: Session = Depends(get_db)
):
    """
    Create a new alert record when a threshold is exceeded.
    
    Example request body:
    {
        "metric_type": "cpu",
        "status": "critical",
        "current_value": 92.5,
        "threshold": 90.0,
        "message": "CPU usage exceeded critical threshold",
        "source": "system"
    }
    """
    db_alert = crud.create_alert(db, alert)
    return db_alert


@router.get("/alerts", response_model=AlertListResponse)
async def get_alerts(
    hours: int = Query(24, ge=1, le=720, description="Last N hours to fetch alerts"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of alerts"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Backward-compatible endpoint for dashboards expecting GET /api/alerts."""
    alerts = crud.get_recent_alerts(db, hours=hours, limit=limit)
    alerts = _filter_alerts_by_user(db, current_user, alerts)
    return {
        "alerts": alerts,
        "count": len(alerts)
    }


@router.get("/alerts/recent", response_model=AlertListResponse)
async def get_recent_alerts(
    hours: int = Query(24, ge=1, le=720, description="Last N hours to fetch alerts"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of alerts"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get recent alerts from last N hours"""
    alerts = crud.get_recent_alerts(db, hours=hours, limit=limit)
    alerts = _filter_alerts_by_user(db, current_user, alerts)
    return {
        "alerts": alerts,
        "count": len(alerts)
    }


@router.get("/alerts/unresolved", response_model=AlertListResponse)
async def get_unresolved_alerts(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get all unresolved alerts (currently active)"""
    alerts = crud.get_unresolved_alerts(db)
    alerts = _filter_alerts_by_user(db, current_user, alerts)
    return {
        "alerts": alerts,
        "count": len(alerts)
    }


@router.get("/alerts/by-metric/{metric_type}", response_model=AlertListResponse)
async def get_alerts_by_metric(
    metric_type: str,
    hours: int = Query(24, ge=1, le=720, description="Last N hours"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get alerts for a specific metric type"""
    # Validate metric type
    server_metrics = {"cpu", "memory"}
    iot_metrics = {"temperature", "humidity", "soil_moisture", "light_intensity", "pressure"}
    allowed = server_metrics | iot_metrics
    
    if metric_type not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid metric_type. Must be one of {allowed}"
        )
    
    alerts = crud.get_alerts_by_metric(db, metric_type, hours=hours)
    alerts = _filter_alerts_by_user(db, current_user, alerts)
    return {
        "alerts": alerts,
        "count": len(alerts)
    }


@router.patch("/alerts/{alert_id}/resolve", response_model=AlertResponse)
async def resolve_alert(
    alert_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Mark an alert as resolved"""
    existing = db.query(Alert).filter(Alert.id == alert_id).first()
    if not existing:
        raise HTTPException(
            status_code=404,
            detail=f"Alert with id {alert_id} not found"
        )
    _ensure_alert_access(db, current_user, existing)
    alert = crud.resolve_alert(db, alert_id)
    
    if not alert:
        raise HTTPException(
            status_code=404,
            detail=f"Alert with id {alert_id} not found"
        )
    
    return alert


@router.delete("/alerts/cleanup")
async def cleanup_old_alerts(
    days: int = Query(30, ge=1, le=365, description="Delete alerts older than N days"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Delete resolved alerts older than specified days (maintenance)"""
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Only admin can cleanup alerts")
    deleted_count = crud.delete_old_alerts(db, days=days)
    return {
        "status": "success",
        "message": f"Deleted {deleted_count} resolved alerts older than {days} days"
    }


@router.get("/alerts/{alert_id}/explain-ai")
async def explain_alert_with_ai(
    alert_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Generate AI explanation for a specific alert using Gemini + optional Open-Meteo context."""
    alert = db.query(Alert).filter(Alert.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    _ensure_alert_access(db, current_user, alert)

    device = db.query(IoTDevice).filter(IoTDevice.source == alert.source).first()
    environment_type = (device.environment_type if device and device.environment_type else "indoor").lower()
    is_outdoor = environment_type == "outdoor"

    weather_context = None
    weather_fetch_error = None
    if (
        is_outdoor
        and device
        and device.latitude is not None
        and device.longitude is not None
    ):
        try:
            weather_context = get_weather_for_timestamp(
                latitude=float(device.latitude),
                longitude=float(device.longitude),
                target_iso_time=alert.created_at.isoformat() if alert.created_at else "",
                timezone=device.timezone_name or "auto",
            )
        except Exception as exc:
            # Never let weather enrichment break AI explanation flow.
            weather_context = None
            weather_fetch_error = str(exc)

    alert_context = {
        "alert_id": alert.id,
        "metric_type": alert.metric_type,
        "status": alert.status,
        "current_value": alert.current_value,
        "threshold": alert.threshold,
        "source": alert.source,
        "message": alert.message,
        # Time fields are intentionally omitted for indoor sensors.
        "device": {
            "name": device.name if device else None,
            "location": device.location if device else None,
            "environment_type": environment_type,
            "location_query": device.location_query if device else None,
            "task_description": device.task_description if device else None,
            "priority_level": device.priority_level if device else None,
            "action_hint": device.action_hint if device else None,
        },
        "weather": weather_context,
    }

    if is_outdoor and alert.created_at:
        alert_context["alert_time"] = {
            "iso": alert.created_at.isoformat(),
            "display": alert.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "note": "Use this exact alert time for reasoning with weather context.",
        }

    ai_result = explain_alert_with_gemini(alert_context)
    return {
        "alert_id": alert.id,
        "success": ai_result["success"],
        "message": ai_result["message"],
        "explanation": ai_result["explanation"],
        "error_code": ai_result.get("error_code"),
        "error_detail": ai_result.get("error_detail"),
        "retry_after_seconds": ai_result.get("retry_after_seconds"),
        "context": {
            "has_weather": weather_context is not None,
            "environment_type": environment_type,
            "alert_time_included": bool(is_outdoor and alert.created_at),
            "weather_fetch_error": weather_fetch_error,
        },
    }
