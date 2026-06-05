"""Pydantic schemas for request/response validation"""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, validator


class MetricCreate(BaseModel):
    """Schema for creating a single metric"""
    event_ts: Optional[datetime] = Field(default=None, description="Event timestamp (auto-generated if not provided)")
    sensor_id: str = Field(..., min_length=1, max_length=100, description="Sensor/device identifier")
    location: Optional[str] = Field(
        default=None,
        max_length=255,
        description="User-facing sensor label/location (for example: 'Living Room' or 'Goc duoi vuon')",
    )
    metric_type: str = Field(..., description="IoT metric type")
    metric_value: float = Field(..., description="Numeric metric value")
    unit: Optional[str] = Field(default=None, max_length=50, description="Measurement unit")

    @validator('metric_type')
    def validate_metric_type(cls, v):
        """Validate metric_type is one of allowed values"""
        iot_metrics = {"temperature", "humidity", "soil_moisture", "light_intensity", "pressure"}
        allowed = iot_metrics
        if v not in allowed:
            raise ValueError(f"metric_type must be one of {allowed}")
        return v

    @validator('metric_value')
    def validate_value(cls, v):
        """Validate value is positive number"""
        if v < 0:
            raise ValueError("value must be non-negative")
        if v > 1000000:
            raise ValueError("value seems unreasonably large")
        return v

    @validator('sensor_id')
    def validate_source(cls, v):
        """Validate sensor_id is not empty"""
        if not v or len(v.strip()) == 0:
            raise ValueError("sensor_id cannot be empty")
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
    id: int
    event_ts: datetime
    timestamp: Optional[datetime] = None
    sensor_id: str
    location: Optional[str] = None
    metric_type: str
    metric_value: float
    value: Optional[float] = None
    unit: Optional[str] = None

    class Config:
        from_attributes = True


class LatestMetricsResponse(BaseModel):
    """Schema for latest metrics response"""
    latest_temperature: Optional[float] = None
    latest_humidity: Optional[float] = None
    latest_soil_moisture: Optional[float] = None
    latest_light_intensity: Optional[float] = None
    latest_pressure: Optional[float] = None
    timestamp: datetime


class SummaryMetricsResponse(BaseModel):
    """Schema for summary metrics response"""
    avg_temperature: Optional[float] = None
    avg_humidity: Optional[float] = None
    avg_soil_moisture: Optional[float] = None
    avg_light_intensity: Optional[float] = None
    avg_pressure: Optional[float] = None
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
    device_id: Optional[int] = None
    device_name: Optional[str] = None
    unit: Optional[str] = None
    min_threshold: Optional[float] = None
    max_threshold: Optional[float] = None
    alert_origin: str = Field(default="realtime", description="Alert origin: realtime or forecast")
    forecast_timestamp: Optional[datetime] = Field(default=None, description="Forecast timestamp that is predicted to breach threshold")
    forecast_generated_at: Optional[datetime] = Field(default=None, description="When the forecast row was generated")
    created_at: Optional[datetime] = Field(default=None, description="Alert timestamp")

    @validator('metric_type')
    def validate_metric_type(cls, v):
        """Validate metric_type"""
        server_metrics = {"cpu", "memory"}
        iot_metrics = {"temperature", "humidity", "soil_moisture", "light_intensity", "pressure"}
        allowed = server_metrics | iot_metrics
        if v not in allowed:
            raise ValueError(f"metric_type must be one of {allowed}")
        return v

    @validator('status')
    def validate_status(cls, v):
        """Validate status"""
        if v not in {"warning", "critical"}:
            raise ValueError("status must be either 'warning' or 'critical'")
        return v

    @validator('alert_origin')
    def validate_alert_origin(cls, v):
        if v not in {"realtime", "forecast"}:
            raise ValueError("alert_origin must be either 'realtime' or 'forecast'")
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
    device_id: Optional[int] = None
    device_name: Optional[str] = None
    unit: Optional[str] = None
    min_threshold: Optional[float] = None
    max_threshold: Optional[float] = None
    alert_origin: str = "realtime"
    forecast_timestamp: Optional[datetime] = None
    forecast_generated_at: Optional[datetime] = None
    created_at: datetime
    resolved_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class AlertListResponse(BaseModel):
    """Schema for alerts list response"""
    alerts: List[AlertResponse] = Field(..., description="List of alerts")
    count: int = Field(..., description="Total number of alerts")


# ============== AUTH SCHEMAS ==============

class UserRegister(BaseModel):
    """Schema for user registration"""
    username: str = Field(..., min_length=3, max_length=50, description="Username")
    email: str = Field(..., description="Email address")
    password: str = Field(..., min_length=6, description="Password")
    role: str = Field(default="user", description="Role: 'admin' or 'user'")

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

    @validator('role')
    def validate_role(cls, v):
        """Validate role"""
        if v not in {"admin", "user"}:
            raise ValueError("role must be 'admin' or 'user'")
        return v


class UserLogin(BaseModel):
    """Schema for user login"""
    username: str = Field(..., description="Username")
    password: str = Field(..., description="Password")


class TokenResponse(BaseModel):
    """Schema for token response"""
    access_token: str
    token_type: str
    user: dict


class UserResponse(BaseModel):
    """Schema for user response"""
    id: int
    username: str
    email: str
    notification_email: Optional[str] = None
    email_enabled: bool = False
    telegram_chat_id: Optional[str] = None
    telegram_enabled: bool = False
    role: str
    is_active: bool
    is_approved: bool
    approved_at: Optional[datetime] = None
    created_at: datetime

    class Config:
        from_attributes = True


class DeviceCreate(BaseModel):
    """Schema for creating a device"""
    name: str = Field(..., min_length=1, max_length=100)
    device_type: str = Field(..., description="Type of device: cpu, memory, temperature, humidity, etc")
    source: str = Field(..., min_length=1, max_length=100, description="Unique identifier")
    location: Optional[str] = Field(None, max_length=255)


class DeviceUpdate(BaseModel):
    """Schema for updating a device"""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    device_type: Optional[str] = Field(None, description="Type of device: cpu, memory, temperature, humidity, etc")
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


# ============== CHAT SCHEMAS ==============

class ChatConversationResponse(BaseModel):
    id: int
    user_id: int
    assigned_admin_id: Optional[int] = None
    status: str
    subject: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    last_message_preview: Optional[str] = None
    unread_for_user: int = 0
    unread_for_admin: int = 0
    user_name: Optional[str] = None
    user_email: Optional[str] = None

    class Config:
        from_attributes = True


class ChatMessageResponse(BaseModel):
    id: int
    conversation_id: int
    sender_type: str
    sender_id: Optional[int] = None
    content: str
    created_at: datetime

    class Config:
        from_attributes = True


class ChatSendRequest(BaseModel):
    conversation_id: Optional[int] = None
    message: str = Field(..., min_length=1, max_length=4000)


class ChatEscalateRequest(BaseModel):
    conversation_id: Optional[int] = None
    reason: Optional[str] = Field(default=None, max_length=1000)


class ChatAdminReplyRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=4000)


class ChatConversationListResponse(BaseModel):
    conversations: List[ChatConversationResponse]
    count: int


class ChatMessagesListResponse(BaseModel):
    conversation: ChatConversationResponse
    messages: List[ChatMessageResponse]
    count: int


class ChatIssueTemplateCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(default=None, max_length=1000)
    sort_order: int = 0
    is_active: bool = True


class ChatIssueTemplateUpdate(BaseModel):
    title: Optional[str] = Field(default=None, min_length=1, max_length=255)
    description: Optional[str] = Field(default=None, max_length=1000)
    sort_order: Optional[int] = None
    is_active: Optional[bool] = None


class ChatIssueTemplateResponse(BaseModel):
    id: int
    title: str
    description: Optional[str] = None
    is_active: bool
    sort_order: int
    created_by: Optional[int] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
