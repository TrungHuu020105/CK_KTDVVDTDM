"""Business logic for metrics processing and aggregation"""

from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from app.models import Metric
from app import crud


class MetricsService:
    """Service for metrics processing and aggregation"""

    @staticmethod
    def calculate_average(metrics: List[Metric]) -> Optional[float]:
        """Calculate average value from metrics"""
        if not metrics:
            return None
        return sum(m.metric_value for m in metrics) / len(metrics)

    @staticmethod
    def calculate_total(metrics: List[Metric]) -> float:
        """Calculate total value from metrics"""
        if not metrics:
            return 0.0
        return sum(m.metric_value for m in metrics)

    @staticmethod
    def get_aggregated_summary(db: Session, minutes: int = 1) -> Dict[str, Any]:
        """
        Get aggregated metrics summary for specified time range.
        
        Returns dict with IoT averages by metric type.
        """
        # Get all metrics in range
        temperature_metrics, humidity_metrics, soil_moisture_metrics, light_intensity_metrics, pressure_metrics = crud.get_all_metrics_in_range(
            db, minutes
        )

        # Calculate aggregations
        avg_temperature = MetricsService.calculate_average(temperature_metrics)
        avg_humidity = MetricsService.calculate_average(humidity_metrics)
        avg_soil_moisture = MetricsService.calculate_average(soil_moisture_metrics)
        avg_light_intensity = MetricsService.calculate_average(light_intensity_metrics)
        avg_pressure = MetricsService.calculate_average(pressure_metrics)

        return {
            "avg_temperature": avg_temperature,
            "avg_humidity": avg_humidity,
            "avg_soil_moisture": avg_soil_moisture,
            "avg_light_intensity": avg_light_intensity,
            "avg_pressure": avg_pressure,
            "timestamp": datetime.now(timezone(timedelta(hours=7)))
        }

    @staticmethod
    def get_latest_values(db: Session) -> Dict[str, Any]:
        """
        Get latest values for all metric types.
        
        Returns dict with latest values and timestamp.
        """
        latest_temperature, latest_humidity, latest_soil_moisture, latest_light_intensity, latest_pressure = crud.get_latest_metrics(db)

        return {
            "latest_temperature": latest_temperature.metric_value if latest_temperature else None,
            "latest_humidity": latest_humidity.metric_value if latest_humidity else None,
            "latest_soil_moisture": latest_soil_moisture.metric_value if latest_soil_moisture else None,
            "latest_light_intensity": latest_light_intensity.metric_value if latest_light_intensity else None,
            "latest_pressure": latest_pressure.metric_value if latest_pressure else None,
            "timestamp": datetime.now(timezone(timedelta(hours=7)))
        }
