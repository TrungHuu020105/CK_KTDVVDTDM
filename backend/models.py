"""SQLAlchemy ORM models"""

from datetime import datetime, timezone, timedelta
from sqlalchemy import Column, Integer, Float, String, DateTime, Index, Boolean
from .database import Base


class Metric(Base):
    """Metric record model"""
    __tablename__ = "metrics"

    id = Column(Integer, primary_key=True, index=True)
    metric_type = Column(String(50), index=True, nullable=False)  # temperature, humidity, soil_moisture, etc.
    value = Column(Float, nullable=False)
    source = Column(String(100), nullable=False)  # e.g., "server_1", "server_2"
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone(timedelta(hours=7))), index=True, nullable=False)

    # Composite index for efficient time-range queries
    __table_args__ = (
        Index('idx_metric_type_timestamp', 'metric_type', 'timestamp'),
    )

    def __repr__(self):
        return f"<Metric(type={self.metric_type}, value={self.value}, source={self.source}, time={self.timestamp})>"


class Alert(Base):
    """Alert record model - stores triggered alerts"""
    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True, index=True)
    metric_type = Column(String(50), index=True, nullable=False)  # temperature, humidity, soil_moisture, etc.
    status = Column(String(20), index=True, nullable=False)  # 'warning' or 'critical'
    current_value = Column(Float, nullable=False)  # Current metric value when alert triggered
    threshold = Column(Float, nullable=False)  # Threshold that was exceeded
    message = Column(String(255), nullable=False)  # Alert message
    source = Column(String(100), nullable=False, default="system")  # Source of the metric
    created_at = Column(DateTime, default=lambda: datetime.now(timezone(timedelta(hours=7))), index=True, nullable=False)
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
    hashed_password = Column(String(255), nullable=False)
    role = Column(String(20), nullable=False, default="user")
    is_active = Column(Boolean, default=True, nullable=False)
    is_approved = Column(Boolean, default=True, nullable=False)
    approved_by = Column(Integer, nullable=True)
    approved_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone(timedelta(hours=7))), nullable=False)

    def __repr__(self):
        return f"<User(username={self.username}, active={self.is_active})>"


class Device(Base):
    """Device model for managing metric sources"""
    __tablename__ = "devices"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    device_type = Column(String(50), nullable=False)
    source = Column(String(100), unique=True, nullable=False, index=True)
    location = Column(String(255), nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    created_by = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone(timedelta(hours=7))), nullable=False)

    def __repr__(self):
        return f"<Device(name={self.name}, type={self.device_type}, source={self.source})>"


class UserDevicePermission(Base):
    """Permissions linking users to devices they can view"""
    __tablename__ = "user_device_permissions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False, index=True)
    device_id = Column(Integer, nullable=False, index=True)
    granted_by = Column(Integer, nullable=False)
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
    source = Column(String(100), unique=True, nullable=False, index=True)  # Unique identifier for metrics
    location = Column(String(255), nullable=True)  # Physical location
    is_active = Column(Boolean, default=True, nullable=False)
    
    # Alert threshold fields - upper and lower bounds
    alert_enabled = Column(Boolean, default=False, nullable=False)  # Enable/disable alerts for this device
    lower_threshold = Column(Float, nullable=True)  # Lower threshold (values below this trigger alert)
    upper_threshold = Column(Float, nullable=True)  # Upper threshold (values above this trigger alert)
    
    created_at = Column(DateTime, default=lambda: datetime.now(timezone(timedelta(hours=7))), nullable=False)

    def __repr__(self):
        return f"<IoTDevice(user_id={self.user_id}, name={self.name}, type={self.device_type})>"
