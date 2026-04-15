"""Pydantic schemas for request/response validation"""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, validator


class MetricCreate(BaseModel):
    """Schema for creating a single metric"""
    metric_type: str = Field(..., description="Type of metric: temperature, humidity, soil_moisture, light_intensity, pressure")
    value: float = Field(..., description="Numeric value of the metric")
    source: str = Field(..., description="Source identifier (e.g., server name)")
    timestamp: Optional[datetime] = Field(default=None, description="Timestamp of metric (auto-generated if not provided)")

    @validator('metric_type')
    def validate_metric_type(cls, v):
        """Validate metric_type is one of allowed values"""
        allowed = {"temperature", "humidity", "soil_moisture", "light_intensity", "pressure"}
        if v not in allowed:
            raise ValueError(f"metric_type must be one of {allowed}")
        return v

    @validator('value')
    def validate_value(cls, v):
        """Validate value is positive number"""
        if v < 0:
            raise ValueError("value must be non-negative")
        if v > 1000000:
            raise ValueError("value seems unreasonably large")
        return v

    @validator('source')
    def validate_source(cls, v):
        """Validate source is not empty"""
        if not v or len(v.strip()) == 0:
            raise ValueError("source cannot be empty")
        return v.strip()


class MetricBulkCreate(BaseModel):
    """Schema for creating multiple metrics"""
    metrics: List[MetricCreate] = Field(..., description="List of metrics to create")

    @validator('metrics')
    def validate_metrics(cls, v):
        """Validate metrics list is not empty"""
        if len(v) == 0:
            raise ValueError("metrics list cannot be empty")
        if len(v) > 1000:
            raise ValueError("metrics list cannot exceed 1000 items")
        return v


class MetricResponse(BaseModel):
    """Schema for metric response"""
    id: Optional[int] = None
    metric_type: str
    value: float
    source: str
    timestamp: datetime

    class Config:
        from_attributes = True


class LatestMetricsResponse(BaseModel):
    """Schema for latest metrics response"""
    temperature: Optional[float] = None
    humidity: Optional[float] = None
    soil_moisture: Optional[float] = None
    light_intensity: Optional[float] = None
    pressure: Optional[float] = None
    timestamp: datetime


class SummaryMetricsResponse(BaseModel):
    """Schema for summary metrics response"""
    avg_temperature_1m: Optional[float] = None
    avg_humidity_1m: Optional[float] = None
    avg_soil_moisture_1m: Optional[float] = None
    avg_light_intensity_1m: Optional[float] = None
    avg_pressure_1m: Optional[float] = None
    timestamp: datetime


class MetricsHistoryResponse(BaseModel):
    """Schema for metrics history response"""
    metric_type: str
    data: List[MetricResponse]
    count: int


class HealthResponse(BaseModel):
    """Schema for health check response"""
    status: str
    message: str


# ============== ALERT SCHEMAS ==============

class AlertCreate(BaseModel):
    """Schema for creating an alert"""
    metric_type: str = Field(..., description="Type of metric that triggered alert")
    status: str = Field(..., description="Alert status: 'warning' or 'critical'")
    current_value: float = Field(..., description="Current metric value")
    threshold: float = Field(..., description="Threshold that was exceeded")
    message: str = Field(..., description="Alert message")
    source: str = Field(default="system", description="Source of the metric")

    @validator('metric_type')
    def validate_metric_type(cls, v):
        """Validate metric_type"""
        allowed = {"temperature", "humidity", "soil_moisture", "light_intensity", "pressure"}
        if v not in allowed:
            raise ValueError(f"metric_type must be one of {allowed}")
        return v

    @validator('status')
    def validate_status(cls, v):
        """Validate status"""
        if v not in {"warning", "critical"}:
            raise ValueError("status must be either 'warning' or 'critical'")
        return v


class AlertResponse(BaseModel):
    """Schema for alert response"""
    id: int
    metric_type: str
    status: str
    current_value: float
    threshold: float
    message: str
    source: str
    created_at: datetime
    resolved_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class AlertListResponse(BaseModel):
    """Schema for alerts list response"""
    alerts: List[AlertResponse] = Field(..., description="List of alerts")
    count: int = Field(..., description="Total number of alerts")


# ============== USER SCHEMAS ==============

class UserRegister(BaseModel):
    """Schema for user registration"""
    username: str = Field(..., min_length=3, max_length=50, description="Username")
    email: str = Field(..., description="Email address")
    password: str = Field(..., min_length=6, description="Password")

    @validator('username')
    def validate_username(cls, v):
        """Validate username format"""
        if not v.isalnum():
            raise ValueError("username must be alphanumeric")
        return v

    @validator('email')
    def validate_email(cls, v):
        """Validate email format"""
        if "@" not in v:
            raise ValueError("invalid email format")
        return v

class UserResponse(BaseModel):
    """Schema for user response"""
    id: int
    username: str
    email: str
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


class DeviceCreate(BaseModel):
    """Schema for creating a device"""
    name: str = Field(..., min_length=1, max_length=100)
    device_type: str = Field(..., description="Type of device: temperature, humidity, soil_moisture, light_intensity, pressure")
    source: str = Field(..., min_length=1, max_length=100, description="Unique identifier")
    location: Optional[str] = Field(None, max_length=255)


class DeviceUpdate(BaseModel):
    """Schema for updating a device"""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    device_type: Optional[str] = Field(None, description="Type of device: temperature, humidity, soil_moisture, light_intensity, pressure")
    location: Optional[str] = Field(None, max_length=255)
    is_active: Optional[bool] = Field(None, description="Enable/disable metric generation for this device")


class DeviceResponse(BaseModel):
    """Schema for device response"""
    id: int
    name: str
    device_type: str
    source: str
    is_active: bool
    location: Optional[str] = None
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


class UserDevicePermissionResponse(BaseModel):
    """Schema for user-device permission"""
    id: int
    user_id: int
    device_id: int
    granted_at: datetime

    class Config:
        from_attributes = True
