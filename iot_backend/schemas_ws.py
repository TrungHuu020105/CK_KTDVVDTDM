"""
Pydantic schemas cho WebSocket metrics (Pydantic v2)
"""
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, timezone


class MetricsData(BaseModel):
    """
    Schema cho dữ liệu metrics nhận được từ client.
    - cpu: CPU usage in percentage (0-100)
    - ram: RAM usage in percentage (0-100)
    - timestamp: Timestamp khi dữ liệu được lấy
    """
    cpu: float = Field(..., ge=0, le=100, description="CPU usage percentage")
    ram: float = Field(..., ge=0, le=100, description="RAM usage percentage")
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "cpu": 45.5,
                "ram": 72.3,
                "timestamp": "2024-04-06T10:30:00.123456"
            }
        }
    }


class IotMetricsData(BaseModel):
    """
    Schema cho dữ liệu IoT sensor stream.
    - metric_type: Loại metric (temperature, humidity, soil_moisture, light_intensity, pressure)
    - value: Giá trị sensor
    - source: ID của sensor (sensor_1, sensor_2, etc.)
    - unit: Đơn vị (°C, %, lux, hPa)
    - timestamp: Khi dữ liệu được lấy
    - saved: Flag indicating if backend should persist to database (True) or realtime-only (False)
    """
    metric_type: str = Field(..., description="Type of metric (temperature, humidity, etc.)")
    value: float = Field(..., description="Sensor value")
    source: str = Field(..., description="Sensor source ID")
    location: Optional[str] = Field(default=None, description="Sensor location")
    unit: str = Field(default="", description="Unit of measurement")
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    saved: bool = Field(default=True, description="Whether to save to database (True) or realtime-only (False)")
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "metric_type": "temperature",
                "value": 24.5,
                "source": "sensor_1",
                "unit": "°C",
                "timestamp": "2024-04-06T10:30:00.123456",
                "saved": True
            }
        }
    }


class ClientStatus(BaseModel):
    """
    Schema cho thông tin status của một client.
    """
    client_id: str
    status: str
    connected_at: Optional[str] = None
    last_update: Optional[str] = None
    metrics: Optional[MetricsData] = None


class StatusResponse(BaseModel):
    """
    Schema cho response của endpoint /status.
    """
    total_clients: int
    timestamp: str
    clients: List[ClientStatus]

