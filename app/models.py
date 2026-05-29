"""SQLAlchemy ORM models"""

from datetime import datetime, timezone, timedelta
from sqlalchemy import Column, Integer, Float, String, DateTime, Index, Boolean, UniqueConstraint
from app.database import Base


class Metric(Base):
    """Metric record model"""
    __tablename__ = "metrics"

    id = Column(Integer, primary_key=True, index=True)
    event_ts = Column(DateTime, default=lambda: datetime.now(timezone(timedelta(hours=7))), index=True, nullable=False)
    sensor_id = Column(String(100), nullable=False, index=True)
    location = Column(String(255), nullable=True)  # User-facing sensor label/location snapshot
    metric_type = Column(String(50), index=True, nullable=False)  # temperature, humidity, soil_moisture, ...
    metric_value = Column(Float, nullable=False)
    unit = Column(String(50), nullable=True)

    # Composite index for efficient time-range queries
    __table_args__ = (
        Index('idx_metric_type_event_ts', 'metric_type', 'event_ts'),
    )

    def __repr__(self):
        return f"<Metric(sensor={self.sensor_id}, type={self.metric_type}, value={self.metric_value}, time={self.event_ts})>"


class Alert(Base):
    """Alert record model - stores triggered alerts"""
    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True, index=True)
    metric_type = Column(String(50), index=True, nullable=False)  # cpu, memory, temperature, etc
    status = Column(String(20), index=True, nullable=False)  # 'warning' or 'critical'
    current_value = Column(Float, nullable=False)  # Current metric value when alert triggered
    threshold = Column(Float, nullable=False)  # Threshold that was exceeded
    message = Column(String(255), nullable=False)  # Alert message
    source = Column(String(100), nullable=False, default="system")  # Source of the metric
    device_id = Column(Integer, nullable=True, index=True)
    device_name = Column(String(100), nullable=True)
    unit = Column(String(50), nullable=True)
    min_threshold = Column(Float, nullable=True)
    max_threshold = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.now, index=True, nullable=False)
    resolved_at = Column(DateTime, nullable=True)  # When alert was resolved (if applicable)

    # Composite index for efficient querying
    __table_args__ = (
        Index('idx_alert_status_created', 'status', 'created_at'),
        Index('idx_alert_metric_created', 'metric_type', 'created_at'),
    )

    def __repr__(self):
        return f"<Alert(type={self.metric_type}, status={self.status}, value={self.current_value}, at={self.created_at})>"


class User(Base):
    """User account model for authentication"""
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, index=True, nullable=False)
    email = Column(String(100), unique=True, index=True, nullable=False)
    notification_email = Column(String(100), nullable=True)
    email_enabled = Column(Boolean, default=False, nullable=False)
    telegram_chat_id = Column(String(64), unique=True, nullable=True)
    telegram_enabled = Column(Boolean, default=False, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    role = Column(String(20), nullable=False, default="user")  # 'admin' or 'user'
    is_active = Column(Boolean, default=True, nullable=False)
    is_approved = Column(Boolean, default=False, nullable=False)  # Admin must approve
    approved_by = Column(Integer, nullable=True)  # Admin ID who approved
    approved_at = Column(DateTime, nullable=True)  # When approved
    created_at = Column(DateTime, default=lambda: datetime.now(timezone(timedelta(hours=7))), nullable=False)

    def __repr__(self):
        return f"<User(username={self.username}, role={self.role}, approved={self.is_approved})>"


class UserNotificationTarget(Base):
    """Per-user notification targets (multiple telegram chat ids / emails)."""
    __tablename__ = "user_notification_targets"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False, index=True)
    target_type = Column(String(20), nullable=False, index=True)  # telegram | email
    target_value = Column(String(255), nullable=False)
    is_enabled = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone(timedelta(hours=7))), nullable=False)


class Device(Base):
    """Device model for managing sources (servers, IoT devices)"""
    __tablename__ = "devices"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)  # Device name (e.g., "Server 1", "Temperature Sensor 1")
    device_type = Column(String(50), nullable=False)  # 'cpu', 'memory', 'temperature', 'humidity', etc
    source = Column(String(100), unique=True, nullable=False, index=True)  # Unique identifier for metrics
    location = Column(String(255), nullable=True)  # User-facing management label
    is_active = Column(Boolean, default=True, nullable=False)
    created_by = Column(Integer, nullable=False)  # Admin ID who created
    created_at = Column(DateTime, default=lambda: datetime.now(timezone(timedelta(hours=7))), nullable=False)

    def __repr__(self):
        return f"<Device(name={self.name}, type={self.device_type}, source={self.source})>"


class UserDevicePermission(Base):
    """Permissions linking users to devices they can view"""
    __tablename__ = "user_device_permissions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False, index=True)  # User ID
    device_id = Column(Integer, nullable=False, index=True)  # Device ID
    granted_by = Column(Integer, nullable=False)  # Admin ID who granted
    granted_at = Column(DateTime, default=lambda: datetime.now(timezone(timedelta(hours=7))), nullable=False)

    def __repr__(self):
        return f"<UserDevicePermission(user_id={self.user_id}, device_id={self.device_id})>"


class IoTDevice(Base):
    """IoT Device model - User-owned and managed"""
    __tablename__ = "iot_devices"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False, index=True)  # Owner/creator
    name = Column(String(100), nullable=False)  # Device name (e.g., "Room 1 Temperature")
    device_type = Column(String(50), nullable=False)  # temperature, humidity, soil_moisture, light_intensity, pressure
    source = Column(String(100), nullable=False, index=True)  # Source identifier (NOT globally unique — unique per device_type)
    unit = Column(String(50), nullable=True)  # Legacy default unit.
    source_type = Column(String(30), nullable=False, default="physical_iot")  # physical_iot | virtual_meteostat
    capabilities = Column(String(100), nullable=False, default="temperature,humidity")
    location = Column(String(255), nullable=True)  # User-facing management label
    location_province = Column(String(100), nullable=True)
    environment_type = Column(String(20), nullable=False, default="indoor")  # indoor | outdoor
    location_query = Column(String(255), nullable=True)  # Raw user location string for geocoding/weather
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    timezone_name = Column(String(64), nullable=True)
    task_description = Column(String(500), nullable=True)
    priority_level = Column(String(20), nullable=True)  # low | medium | high
    action_hint = Column(String(500), nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    
    # Canonical alert threshold fields
    alert_enabled = Column(Boolean, default=False, nullable=False)  # Enable/disable alerts for this device
    min_threshold = Column(Float, nullable=True)
    max_threshold = Column(Float, nullable=True)
    temperature_min_threshold = Column(Float, nullable=True)
    temperature_max_threshold = Column(Float, nullable=True)
    humidity_min_threshold = Column(Float, nullable=True)
    humidity_max_threshold = Column(Float, nullable=True)

    # For audit/compatibility with requirement naming.
    created_by = Column(Integer, nullable=True)
    
    created_at = Column(DateTime, default=lambda: datetime.now(timezone(timedelta(hours=7))), nullable=False)

    # (source, device_type) must be unique: 1 device per metric type per source
    __table_args__ = (
        UniqueConstraint('source', 'device_type', name='uq_iot_device_source_type'),
    )

    def __repr__(self):
        return f"<IoTDevice(user_id={self.user_id}, name={self.name}, type={self.device_type})>"


class SensorReading(Base):
    """Sensor-level reading containing both temperature and humidity."""
    __tablename__ = "sensor_readings"

    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(Integer, nullable=True, index=True)
    sensor_id = Column(String(100), nullable=False, index=True)
    event_ts = Column(DateTime, default=lambda: datetime.now(timezone(timedelta(hours=7))), index=True, nullable=False)
    temperature = Column(Float, nullable=True)
    humidity = Column(Float, nullable=True)
    temperature_unit = Column(String(20), nullable=True, default="C")
    humidity_unit = Column(String(20), nullable=True, default="%")
    source_type = Column(String(30), nullable=False, default="physical_iot", index=True)
    provider = Column(String(50), nullable=False, default="esp32", index=True)
    environment_type = Column(String(20), nullable=False, default="indoor")
    location = Column(String(255), nullable=True)
    location_province = Column(String(100), nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    databricks_status = Column(String(30), nullable=False, default="pending")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone(timedelta(hours=7))), nullable=False)

    __table_args__ = (
        Index("idx_sensor_reading_sensor_ts", "sensor_id", "event_ts"),
        Index("idx_sensor_reading_source_type_ts", "source_type", "event_ts"),
    )


class ChatConversation(Base):
    """Conversation between user and bot/admin support."""
    __tablename__ = "chat_conversations"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False, index=True)
    assigned_admin_id = Column(Integer, nullable=True, index=True)
    status = Column(String(30), nullable=False, default="bot_active", index=True)
    subject = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone(timedelta(hours=7))), nullable=False, index=True)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone(timedelta(hours=7))), nullable=False, index=True)
    last_read_by_user_at = Column(DateTime, nullable=True, index=True)
    last_read_by_admin_at = Column(DateTime, nullable=True, index=True)

    def __repr__(self):
        return f"<ChatConversation(id={self.id}, user_id={self.user_id}, status={self.status})>"


class ChatMessage(Base):
    """Message in a chat conversation."""
    __tablename__ = "chat_messages"

    id = Column(Integer, primary_key=True, index=True)
    conversation_id = Column(Integer, nullable=False, index=True)
    sender_type = Column(String(20), nullable=False, index=True)  # user | bot | admin | system
    sender_id = Column(Integer, nullable=True, index=True)
    content = Column(String(4000), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone(timedelta(hours=7))), nullable=False, index=True)

    def __repr__(self):
        return f"<ChatMessage(conversation_id={self.conversation_id}, sender_type={self.sender_type})>"


class ChatIssueTemplate(Base):
    """Admin-managed common issues shown to users in support chat."""
    __tablename__ = "chat_issue_templates"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(255), nullable=False)
    description = Column(String(1000), nullable=True)
    is_active = Column(Boolean, default=True, nullable=False, index=True)
    sort_order = Column(Integer, default=0, nullable=False)
    created_by = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone(timedelta(hours=7))), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone(timedelta(hours=7))), nullable=False)

    def __repr__(self):
        return f"<ChatIssueTemplate(id={self.id}, title={self.title}, active={self.is_active})>"
