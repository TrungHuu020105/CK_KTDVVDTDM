"""API routes for metrics endpoints"""

from typing import List, Optional
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from iot_backend.database import get_db
from iot_backend.models import User
from iot_backend.schemas import (
    MetricCreate,
    MetricBulkCreate,
    MetricResponse,
    LatestMetricsResponse,
    SummaryMetricsResponse,
    MetricsHistoryResponse,
    HealthResponse
)
from iot_backend import crud
from iot_backend.services.metrics_service import MetricsService
from iot_backend.system_metrics import SystemMetricsCollector
from iot_backend.api.routes_auth import get_current_user

router = APIRouter(prefix="/api", tags=["metrics"])

IOT_TYPES = {"temperature", "humidity", "soil_moisture", "light_intensity", "pressure"}


def _serialize_metric(metric) -> dict:
    return {
        "id": metric.id,
        "event_ts": metric.event_ts,
        "timestamp": metric.event_ts,
        "sensor_id": metric.sensor_id,
        "location": metric.location,
        "metric_type": metric.metric_type,
        "metric_value": metric.metric_value,
        "value": metric.metric_value,
        "unit": metric.unit,
    }


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "message": "Real-Time Metrics Processing System is running"
    }


@router.post("/metrics", response_model=MetricResponse, status_code=201)
async def create_metric(
    metric: MetricCreate,
    db: Session = Depends(get_db)
):
    """Create a single IoT metric record."""
    db_metric = crud.create_metric(db, metric)
    return _serialize_metric(db_metric)


@router.post("/metrics/bulk", response_model=List[MetricResponse], status_code=201)
async def create_metrics_bulk(
    bulk_data: MetricBulkCreate,
    db: Session = Depends(get_db)
):
    """Create multiple IoT metric records at once."""
    db_metrics = crud.create_metrics_bulk(db, bulk_data.metrics)
    return [_serialize_metric(m) for m in db_metrics]


@router.get("/metrics/latest", response_model=LatestMetricsResponse)
async def get_latest_metrics(
    source: Optional[str] = Query(None, description="Optional sensor_id filter, e.g. sensor_2"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get latest value for each IoT metric type."""
    (
        temperature_metric,
        humidity_metric,
        soil_moisture_metric,
        light_intensity_metric,
        pressure_metric,
    ) = crud.get_latest_metrics_for_user(db, current_user.id, source=source)

    vietnam_tz = timezone(timedelta(hours=7))

    return {
        "latest_temperature": temperature_metric.metric_value if temperature_metric else None,
        "latest_humidity": humidity_metric.metric_value if humidity_metric else None,
        "latest_soil_moisture": soil_moisture_metric.metric_value if soil_moisture_metric else None,
        "latest_light_intensity": light_intensity_metric.metric_value if light_intensity_metric else None,
        "latest_pressure": pressure_metric.metric_value if pressure_metric else None,
        "timestamp": datetime.now(vietnam_tz)
    }


@router.get("/metrics/history", response_model=MetricsHistoryResponse)
async def get_metrics_history(
    metric_type: str = Query(..., description="Type of metric: temperature, humidity, soil_moisture, light_intensity, pressure"),
    minutes: int = Query(5, ge=1, le=1440, description="Time range in minutes (1-1440)"),
    source: Optional[str] = Query(None, description="Optional sensor_id filter"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get historical IoT metric data filtered by user's accessible sensors."""
    if metric_type not in IOT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid metric_type. Must be one of {IOT_TYPES}"
        )

    metrics = crud.get_metrics_history_for_user(
        db,
        current_user.id,
        metric_type,
        minutes,
        source=source
    )

    return {
        "metric_type": metric_type,
        "data": [_serialize_metric(m) for m in metrics],
        "count": len(metrics)
    }


@router.get("/metrics/latest-one")
async def get_latest_metric_one(
    metric_type: str = Query(..., description="Type of metric: temperature, humidity, soil_moisture, light_intensity, pressure"),
    source: str = Query(..., description="sensor_id / source"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if metric_type not in IOT_TYPES:
        raise HTTPException(status_code=400, detail=f"Invalid metric_type. Must be one of {IOT_TYPES}")
    metric = crud.get_latest_metric_for_user(db, current_user.id, metric_type=metric_type, source=source)
    return {"metric": _serialize_metric(metric) if metric else None}


@router.get("/metrics/history-by-date", response_model=MetricsHistoryResponse)
async def get_metrics_history_by_date(
    metric_type: str = Query(..., description="Type of metric: temperature, humidity, soil_moisture, light_intensity, pressure"),
    from_date: str = Query(..., description="Start date (YYYY-MM-DD format)"),
    to_date: str = Query(..., description="End date (YYYY-MM-DD format)"),
    source: Optional[str] = Query(None, description="Optional sensor_id filter"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get historical IoT metrics for a date range."""
    if metric_type not in IOT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid metric_type. Must be one of {IOT_TYPES}"
        )

    try:
        from_dt = datetime.fromisoformat(from_date)
        to_dt = datetime.fromisoformat(to_date)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format. Use YYYY-MM-DD"
        ) from exc

    metrics = crud.get_metrics_history_by_date(
        db,
        current_user.id,
        metric_type,
        from_dt,
        to_dt,
        source=source
    )

    return {
        "metric_type": metric_type,
        "data": [_serialize_metric(m) for m in metrics],
        "count": len(metrics)
    }


@router.get("/metrics/summary", response_model=SummaryMetricsResponse)
async def get_metrics_summary(
    minutes: int = Query(1, ge=1, le=1440, description="Time range in minutes (1-1440)"),
    db: Session = Depends(get_db)
):
    """Get aggregated IoT metrics summary for dashboard."""
    summary = MetricsService.get_aggregated_summary(db, minutes)
    return summary


@router.post("/dev/generate-sample-data", status_code=201)
async def generate_sample_data(
    count: int = Query(50, ge=1, le=1000, description="Number of IoT sample records to generate"),
    db: Session = Depends(get_db)
):
    """Generate sample IoT sensor data for demo/testing purposes."""
    import random

    metrics_to_create = []

    iot_metrics = {
        "temperature": (15, 35, "C"),
        "humidity": (30, 90, "%"),
        "soil_moisture": (0, 100, "%"),
        "light_intensity": (0, 1000, "lux"),
        "pressure": (900, 1100, "hPa"),
    }

    for _ in range(count):
        metric_type = random.choice(list(iot_metrics.keys()))
        min_val, max_val, unit = iot_metrics[metric_type]
        value = random.uniform(min_val, max_val)
        sensor_id = random.choice([f"sensor_{i}" for i in range(1, 5)])
        location = random.choice(["Garden", "Room 1", "Room 2", "Office"])

        metrics_to_create.append(
            MetricCreate(
                event_ts=None,
                sensor_id=sensor_id,
                location=location,
                metric_type=metric_type,
                metric_value=value,
                unit=unit,
            )
        )

    bulk_data = MetricBulkCreate(metrics=metrics_to_create)
    created_metrics = crud.create_metrics_bulk(db, bulk_data.metrics)

    return {
        "message": f"Successfully generated {len(created_metrics)} sample IoT metrics",
        "count": len(created_metrics)
    }


@router.post("/dev/generate-iot-data", status_code=201)
async def generate_iot_data(
    count: int = Query(50, ge=1, le=1000, description="Number of IoT sample records to generate"),
    db: Session = Depends(get_db)
):
    """Alias endpoint for IoT sample generation."""
    return await generate_sample_data(count=count, db=db)


@router.get("/system/current")
async def get_current_system_metrics():
    """Get current CPU/RAM runtime stats (read-only, not persisted)."""
    metrics = SystemMetricsCollector.get_system_metrics()
    vietnam_tz = timezone(timedelta(hours=7))

    return {
        "timestamp": datetime.now(vietnam_tz).isoformat(),
        "metrics": {
            "cpu": {
                "value": round(metrics["cpu"], 2),
                "unit": "%"
            },
            "memory": {
                "value": round(metrics["memory"], 2),
                "unit": "%"
            }
        },
        "persisted_to_db": False
    }


@router.get("/system/detailed")
async def get_detailed_system_metrics():
    """Get detailed system information (read-only)."""
    details = SystemMetricsCollector.get_detailed_metrics()

    return {
        "status": "success",
        "data": details
    }


@router.post("/system/collect", status_code=410)
async def collect_and_save_system_metrics():
    """Disabled: CPU/RAM persistence removed by design."""
    raise HTTPException(
        status_code=410,
        detail="Saving CPU/RAM to metrics table has been disabled. Use IoT metrics endpoints instead."
    )


@router.post("/system/collect-cpu", status_code=410)
async def collect_cpu_only():
    """Disabled: CPU persistence removed by design."""
    raise HTTPException(
        status_code=410,
        detail="Saving CPU metrics has been disabled."
    )


@router.post("/system/collect-memory", status_code=410)
async def collect_memory_only():
    """Disabled: memory persistence removed by design."""
    raise HTTPException(
        status_code=410,
        detail="Saving memory metrics has been disabled."
    )
