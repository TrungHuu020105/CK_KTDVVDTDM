"""API routes for IoT metrics endpoints"""

from datetime import datetime, timedelta, timezone
import time
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from .. import crud
from ..database import get_db
from ..schemas import (
    HealthResponse,
    LatestMetricsResponse,
    MetricBulkCreate,
    MetricCreate,
    MetricResponse,
    MetricsHistoryResponse,
)

router = APIRouter(prefix="/api", tags=["metrics"])

_latest_metrics_cache = {}
_LATEST_CACHE_TTL_SECONDS = 1
IOT_METRIC_TYPES = {
    "temperature",
    "humidity",
    "soil_moisture",
    "light_intensity",
    "pressure",
}


def _normalize_metric_type(value: Optional[str]) -> str:
    return (value or "").strip().lower()


def _is_iot_metric(value: Optional[str]) -> bool:
    return _normalize_metric_type(value) in IOT_METRIC_TYPES


def _parse_timestamp(raw_value) -> Optional[datetime]:
    if isinstance(raw_value, datetime):
        return raw_value

    if raw_value is None:
        return None

    value = str(raw_value).strip()
    if not value:
        return None

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _filter_iot_rows(rows: List[dict]) -> List[dict]:
    return [row for row in rows if _is_iot_metric(row.get("device_type"))]


def _pick_device_for_metric(metric_type: str) -> Optional[str]:
    all_devices = crud.get_databricks_all_devices()
    for device in all_devices:
        if _normalize_metric_type(device.get("device_type")) == metric_type:
            return device.get("device_id")
    return None


def _format_history_item(metric_type: str, item: dict, timestamp_override: Optional[str] = None) -> dict:
    return {
        "id": None,
        "metric_type": metric_type,
        "value": item.get("value", 0),
        "source": item.get("device_id", ""),
        "timestamp": timestamp_override or item.get("timestamp") or datetime.now(timezone.utc).isoformat(),
    }


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "message": "Real-Time IoT Metrics Processing System is running",
    }


@router.post("/metrics", response_model=MetricResponse, status_code=201)
async def create_metric(
    metric: MetricCreate,
    db: Session = Depends(get_db),
):
    """Create a single IoT metric record in local storage."""
    db_metric = crud.create_metric(db, metric)
    return db_metric


@router.post("/metrics/bulk", response_model=List[MetricResponse], status_code=201)
async def create_metrics_bulk(
    bulk_data: MetricBulkCreate,
    db: Session = Depends(get_db),
):
    """Create multiple IoT metric records in local storage."""
    db_metrics = crud.create_metrics_bulk(db, bulk_data.metrics)
    return db_metrics


@router.get("/metrics/latest", response_model=LatestMetricsResponse)
async def get_latest_metrics():
    """Get latest IoT values by metric type from Databricks."""
    all_metrics = _filter_iot_rows(crud.get_databricks_latest_metrics())
    vietnam_tz = timezone(timedelta(hours=7))

    latest_by_type = {metric_type: None for metric_type in IOT_METRIC_TYPES}
    for metric in all_metrics:
        metric_type = _normalize_metric_type(metric.get("device_type"))
        if metric_type in latest_by_type and latest_by_type[metric_type] is None:
            latest_by_type[metric_type] = metric.get("value")

    return {
        "temperature": latest_by_type["temperature"],
        "humidity": latest_by_type["humidity"],
        "soil_moisture": latest_by_type["soil_moisture"],
        "light_intensity": latest_by_type["light_intensity"],
        "pressure": latest_by_type["pressure"],
        "timestamp": datetime.now(vietnam_tz),
    }


@router.get("/metrics/history", response_model=MetricsHistoryResponse)
async def get_metrics_history(
    metric_type: str = Query(..., description="Type of metric: temperature, humidity, soil_moisture, light_intensity, pressure"),
    source: Optional[str] = Query(None, description="Optional device source/device_id to fetch exact device history"),
    minutes: int = Query(5, ge=1, le=1440, description="Time range in minutes (1-1440)"),
):
    """Get historical IoT data for a specific metric type from Databricks."""
    metric_type = _normalize_metric_type(metric_type)
    if metric_type not in IOT_METRIC_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid metric_type. Must be one of {sorted(IOT_METRIC_TYPES)}",
        )

    device_id = source or _pick_device_for_metric(metric_type)
    if not device_id:
        return {
            "metric_type": metric_type,
            "data": [],
            "count": 0,
        }

    history = crud.get_databricks_metric_history(device_id, minutes=minutes, limit=5000)

    data = []
    for item in history:
        if _normalize_metric_type(item.get("device_type")) != metric_type:
            continue
        data.append(_format_history_item(metric_type, item))

    data.sort(key=lambda entry: str(entry.get("timestamp") or ""))

    return {
        "metric_type": metric_type,
        "data": data,
        "count": len(data),
    }


@router.get("/metrics/history-by-date", response_model=MetricsHistoryResponse)
async def get_metrics_history_by_date(
    metric_type: str = Query(..., description="Type of metric: temperature, humidity, soil_moisture, light_intensity, pressure"),
    from_date: str = Query(..., description="Start date (YYYY-MM-DD format)"),
    to_date: str = Query(..., description="End date (YYYY-MM-DD format)"),
    source: Optional[str] = Query(None, description="Optional source filter"),
):
    """Get IoT historical metrics by date range from Databricks."""
    metric_type = _normalize_metric_type(metric_type)
    if metric_type not in IOT_METRIC_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid metric_type. Must be one of {sorted(IOT_METRIC_TYPES)}",
        )

    try:
        from_dt = datetime.fromisoformat(from_date).replace(hour=0, minute=0, second=0, microsecond=0)
        to_dt = datetime.fromisoformat(to_date).replace(hour=23, minute=59, second=59, microsecond=999999)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    if to_dt < from_dt:
        raise HTTPException(status_code=400, detail="to_date must be greater than or equal to from_date")

    device_id = source or _pick_device_for_metric(metric_type)
    if not device_id:
        return {
            "metric_type": metric_type,
            "data": [],
            "count": 0,
        }

    minutes_back = max(1, int((datetime.now() - from_dt).total_seconds() / 60) + 60)
    history = crud.get_databricks_metric_history(device_id, minutes=min(minutes_back, 525600), limit=10000)

    filtered = []
    for item in history:
        if _normalize_metric_type(item.get("device_type")) != metric_type:
            continue

        parsed_ts = _parse_timestamp(item.get("timestamp"))
        if parsed_ts is None:
            continue

        parsed_for_compare = parsed_ts.replace(tzinfo=None) if parsed_ts.tzinfo else parsed_ts
        if parsed_for_compare < from_dt or parsed_for_compare > to_dt:
            continue

        filtered.append((parsed_for_compare, item))

    filtered.sort(key=lambda pair: pair[0])
    data = [
        _format_history_item(metric_type, item, timestamp_override=ts.isoformat())
        for ts, item in filtered
    ]

    return {
        "metric_type": metric_type,
        "data": data,
        "count": len(data),
    }


@router.post("/dev/generate-iot-data", status_code=201)
async def generate_iot_data(
    count: int = Query(50, ge=1, le=1000, description="Number of IoT sample records to generate"),
    db: Session = Depends(get_db),
):
    """Generate fake IoT sensor data for demo/testing purposes."""
    import random

    metrics_to_create = []

    iot_metrics = {
        "temperature": (15, 35),
        "humidity": (30, 90),
        "soil_moisture": (0, 100),
        "light_intensity": (0, 1000),
        "pressure": (900, 1100),
    }

    for _ in range(count):
        metric_type = random.choice(list(iot_metrics.keys()))
        min_val, max_val = iot_metrics[metric_type]
        value = random.uniform(min_val, max_val)
        source = random.choice([f"sensor_{i}" for i in range(1, 5)])

        metric = MetricCreate(
            metric_type=metric_type,
            value=value,
            source=source,
            timestamp=None,
        )
        metrics_to_create.append(metric)

    bulk_data = MetricBulkCreate(metrics=metrics_to_create)
    created_metrics = crud.create_metrics_bulk(db, bulk_data.metrics)

    return {
        "message": f"Successfully generated {len(created_metrics)} sample IoT metrics",
        "count": len(created_metrics),
        "iot_types": {
            "temperature": "Temperature in C (15-35)",
            "humidity": "Humidity in % (30-90)",
            "soil_moisture": "Soil Moisture in % (0-100)",
            "light_intensity": "Light Intensity in lux (0-1000)",
            "pressure": "Atmospheric Pressure in hPa (900-1100)",
        },
    }


@router.get("/databricks/latest")
async def get_databricks_latest(
    metric_type: Optional[str] = Query(None, description="Filter by metric type (temperature, humidity, soil_moisture, light_intensity, pressure)"),
):
    """Get latest IoT metrics from Databricks Delta Lake."""
    normalized_metric_type = _normalize_metric_type(metric_type) if metric_type else None
    if normalized_metric_type and normalized_metric_type not in IOT_METRIC_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid metric_type. Must be one of {sorted(IOT_METRIC_TYPES)}",
        )

    try:
        cache_key = normalized_metric_type or "__all__"
        now = time.time()
        cached = _latest_metrics_cache.get(cache_key)

        if cached and cached.get("expires_at", 0) > now:
            metrics = cached.get("data", [])
        else:
            metrics = crud.get_databricks_latest_metrics(metric_type=normalized_metric_type)
            metrics = _filter_iot_rows(metrics)
            _latest_metrics_cache[cache_key] = {
                "data": metrics,
                "expires_at": now + _LATEST_CACHE_TTL_SECONDS,
            }

        if normalized_metric_type:
            metrics = [
                row for row in metrics
                if _normalize_metric_type(row.get("device_type")) == normalized_metric_type
            ]

        vietnam_tz = timezone(timedelta(hours=7))
        return {
            "status": "success",
            "data": metrics,
            "count": len(metrics),
            "timestamp": datetime.now(vietnam_tz).isoformat(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch from Databricks: {str(e)}")


@router.get("/databricks/history/{device_id}")
async def get_databricks_history(
    device_id: str,
    minutes: int = Query(60, ge=1, le=1440, description="Time window in minutes"),
):
    """Get historical IoT data for a specific device from Databricks."""
    try:
        metrics = crud.get_databricks_metric_history(device_id, minutes=minutes, limit=10000)
        metrics = _filter_iot_rows(metrics)

        return {
            "status": "success",
            "device_id": device_id,
            "data": metrics,
            "count": len(metrics),
            "time_window_minutes": minutes,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch from Databricks: {str(e)}")


@router.get("/databricks/aggregated")
async def get_databricks_aggregated(
    metric_type: Optional[str] = Query(None, description="Filter by metric type"),
):
    """Get aggregated IoT metrics from Databricks (minute-level statistics)."""
    normalized_metric_type = _normalize_metric_type(metric_type) if metric_type else None
    if normalized_metric_type and normalized_metric_type not in IOT_METRIC_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid metric_type. Must be one of {sorted(IOT_METRIC_TYPES)}",
        )

    try:
        metrics = crud.get_databricks_aggregated_metrics(device_type=normalized_metric_type)
        metrics = _filter_iot_rows(metrics)

        return {
            "status": "success",
            "data": metrics,
            "count": len(metrics),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch from Databricks: {str(e)}")


@router.get("/databricks/devices")
async def get_databricks_devices():
    """Get all IoT device metadata from Databricks."""
    try:
        devices = crud.get_databricks_all_devices()
        devices = [
            device for device in devices
            if _is_iot_metric(device.get("device_type"))
        ]

        formatted_devices = []
        for device in devices:
            formatted_devices.append({
                "id": device.get("device_id"),
                "name": device.get("device_name"),
                "type": device.get("device_type"),
                "location": device.get("location"),
                "unit": device.get("unit"),
                "min_value": device.get("min_value"),
                "max_value": device.get("max_value"),
                "mean_value": device.get("mean_value"),
                "std_dev": device.get("std_dev"),
                "active": device.get("active", True),
                "created_at": device.get("created_at"),
                "device_id": device.get("device_id"),
                "device_name": device.get("device_name"),
                "device_type": device.get("device_type"),
            })

        return {
            "status": "success",
            "data": formatted_devices,
            "count": len(formatted_devices),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch from Databricks: {str(e)}")
