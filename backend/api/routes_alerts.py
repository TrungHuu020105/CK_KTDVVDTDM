"""API routes for alerts endpoints"""

from typing import List, Optional
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from ..database import get_db
from ..schemas import (
    AlertCreate,
    AlertResponse,
    AlertListResponse
)
from .. import crud

router = APIRouter(prefix="/api", tags=["alerts"])


@router.post("/alerts", response_model=AlertResponse, status_code=201)
async def create_alert(
    alert: AlertCreate,
    db: Session = Depends(get_db)
):
    """
    Create a new alert record when a threshold is exceeded.
    
    Example request body:
    {
        "metric_type": "temperature",
        "status": "critical",
        "current_value": 38.5,
        "threshold": 35.0,
        "message": "Temperature exceeded critical threshold",
        "source": "sensor_1"
    }
    """
    db_alert = crud.create_alert(db, alert)
    return db_alert


@router.get("/alerts/recent", response_model=AlertListResponse)
async def get_recent_alerts(
    hours: int = Query(24, ge=1, le=720, description="Last N hours to fetch alerts"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of alerts"),
    db: Session = Depends(get_db)
):
    """Get recent alerts from last N hours"""
    alerts = crud.get_recent_alerts(db, hours=hours, limit=limit)
    return {
        "alerts": alerts,
        "count": len(alerts)
    }


@router.get("/alerts/unresolved", response_model=AlertListResponse)
async def get_unresolved_alerts(
    db: Session = Depends(get_db)
):
    """Get all unresolved alerts (currently active)"""
    alerts = crud.get_unresolved_alerts(db)
    return {
        "alerts": alerts,
        "count": len(alerts)
    }


@router.get("/alerts/by-metric/{metric_type}", response_model=AlertListResponse)
async def get_alerts_by_metric(
    metric_type: str,
    hours: int = Query(24, ge=1, le=720, description="Last N hours"),
    db: Session = Depends(get_db)
):
    """Get alerts for a specific metric type"""
    # Validate metric type
    allowed = {"temperature", "humidity", "soil_moisture", "light_intensity", "pressure"}
    
    if metric_type not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid metric_type. Must be one of {allowed}"
        )
    
    alerts = crud.get_alerts_by_metric(db, metric_type, hours=hours)
    return {
        "alerts": alerts,
        "count": len(alerts)
    }


@router.patch("/alerts/{alert_id}/resolve", response_model=AlertResponse)
async def resolve_alert(
    alert_id: int,
    db: Session = Depends(get_db)
):
    """Mark an alert as resolved"""
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
    db: Session = Depends(get_db)
):
    """Delete resolved alerts older than specified days (maintenance)"""
    deleted_count = crud.delete_old_alerts(db, days=days)
    return {
        "status": "success",
        "message": f"Deleted {deleted_count} resolved alerts older than {days} days"
    }
