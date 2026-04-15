"""CRUD operations for database"""

from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict
from sqlalchemy.orm import Session
from sqlalchemy import func
from .models import Metric, Alert, User, Device, UserDevicePermission, IoTDevice
from .schemas import MetricCreate, AlertCreate, UserRegister, DeviceCreate


def create_metric(db: Session, metric: MetricCreate) -> Metric:
    """Create a single metric record"""
    # Use provided timestamp or current time (Vietnam timezone UTC+7)
    vietnam_tz = timezone(timedelta(hours=7))
    timestamp = metric.timestamp if metric.timestamp else datetime.now(vietnam_tz)
    
    db_metric = Metric(
        metric_type=metric.metric_type,
        value=metric.value,
        source=metric.source,
        timestamp=timestamp
    )
    db.add(db_metric)
    db.commit()
    db.refresh(db_metric)
    return db_metric


def create_metrics_bulk(db: Session, metrics: List[MetricCreate]) -> List[Metric]:
    """Create multiple metric records"""
    db_metrics = []
    for metric in metrics:
        vietnam_tz = timezone(timedelta(hours=7))
        timestamp = metric.timestamp if metric.timestamp else datetime.now(vietnam_tz)
        db_metric = Metric(
            metric_type=metric.metric_type,
            value=metric.value,
            source=metric.source,
            timestamp=timestamp
        )
        db_metrics.append(db_metric)
    
    db.add_all(db_metrics)
    db.commit()
    
    # Refresh all objects to get IDs
    for metric in db_metrics:
        db.refresh(metric)
    
    return db_metrics


def get_metrics_history(
    db: Session,
    metric_type: str,
    minutes: int = 5
) -> List[Metric]:
    """Get historical metrics for a specific type within a time range"""
    vietnam_tz = timezone(timedelta(hours=7))
    time_threshold = datetime.now(vietnam_tz) - timedelta(minutes=minutes)
    
    metrics = db.query(Metric).filter(
        Metric.metric_type == metric_type,
        Metric.timestamp >= time_threshold
    ).order_by(Metric.timestamp.asc()).all()
    
    return metrics


def get_metrics_in_range(
    db: Session,
    metric_type: str,
    minutes: int
) -> List[Metric]:
    """Get metrics within a time range"""
    vietnam_tz = timezone(timedelta(hours=7))
    time_threshold = datetime.now(vietnam_tz) - timedelta(minutes=minutes)
    
    metrics = db.query(Metric).filter(
        Metric.metric_type == metric_type,
        Metric.timestamp >= time_threshold
    ).order_by(Metric.timestamp.asc()).all()
    
    return metrics


def delete_old_metrics(db: Session, days: int = 30) -> int:
    """Delete metrics older than specified days (for maintenance)"""
    time_threshold = datetime.utcnow() - timedelta(days=days)
    
    deleted_count = db.query(Metric).filter(
        Metric.timestamp < time_threshold
    ).delete()
    
    db.commit()
    return deleted_count


# ============== ALERT CRUD OPERATIONS ==============

def create_alert(db: Session, alert: AlertCreate) -> Alert:
    """Create a new alert record"""
    db_alert = Alert(
        metric_type=alert.metric_type,
        status=alert.status,
        current_value=alert.current_value,
        threshold=alert.threshold,
        message=alert.message,
        source=alert.source
    )
    db.add(db_alert)
    db.commit()
    db.refresh(db_alert)
    return db_alert


def get_recent_alerts(db: Session, hours: int = 24, limit: int = 100) -> List[Alert]:
    """Get recent alerts from last N hours"""
    vietnam_tz = timezone(timedelta(hours=7))
    time_threshold = datetime.now(vietnam_tz) - timedelta(hours=hours)
    
    alerts = db.query(Alert).filter(
        Alert.created_at >= time_threshold
    ).order_by(Alert.created_at.desc()).limit(limit).all()
    
    return alerts


def get_unresolved_alerts(db: Session) -> List[Alert]:
    """Get all unresolved alerts (resolved_at is NULL)"""
    alerts = db.query(Alert).filter(
        Alert.resolved_at == None
    ).order_by(Alert.created_at.desc()).all()
    
    return alerts


def get_alerts_by_metric(db: Session, metric_type: str, hours: int = 24) -> List[Alert]:
    """Get alerts for a specific metric type"""
    vietnam_tz = timezone(timedelta(hours=7))
    time_threshold = datetime.now(vietnam_tz) - timedelta(hours=hours)
    
    alerts = db.query(Alert).filter(
        Alert.metric_type == metric_type,
        Alert.created_at >= time_threshold
    ).order_by(Alert.created_at.desc()).all()
    
    return alerts


def resolve_alert(db: Session, alert_id: int) -> Optional[Alert]:
    """Mark an alert as resolved"""
    vietnam_tz = timezone(timedelta(hours=7))
    alert = db.query(Alert).filter(Alert.id == alert_id).first()
    
    if alert:
        alert.resolved_at = datetime.now(vietnam_tz)
        db.commit()
        db.refresh(alert)
    
    return alert


def delete_old_alerts(db: Session, days: int = 15) -> int:
    """Delete all alerts older than specified days (default: 15 days)"""
    # Use Vietnam timezone for consistency
    vietnam_tz = timezone(timedelta(hours=7))
    time_threshold = datetime.now(vietnam_tz) - timedelta(days=days)
    
    # Delete ALL alerts (both resolved and unresolved) older than threshold
    deleted_count = db.query(Alert).filter(
        Alert.created_at < time_threshold
    ).delete()
    
    db.commit()
    return deleted_count


# ============== USER CRUD OPERATIONS ==============

def create_user(db: Session, user: UserRegister, hashed_password: str) -> User:
    """Create a new user account."""
    db_user = User(
        username=user.username,
        email=user.email,
        hashed_password=hashed_password,
        role="user",
        is_approved=True,
        approved_at=datetime.now(timezone(timedelta(hours=7)))
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user


def get_user_by_username(db: Session, username: str) -> Optional[User]:
    """Get user by username"""
    return db.query(User).filter(User.username == username).first()


def get_user_by_email(db: Session, email: str) -> Optional[User]:
    """Get user by email"""
    return db.query(User).filter(User.email == email).first()


def get_user_by_id(db: Session, user_id: int) -> Optional[User]:
    """Get user by ID"""
    return db.query(User).filter(User.id == user_id).first()


def get_all_users(db: Session) -> List[User]:
    """Get all users"""
    return db.query(User).all()


def delete_user(db: Session, user_id: int) -> bool:
    """Delete a user and dependent permission records."""
    user = db.query(User).filter(User.id == user_id).first()
    
    if user:
        # Delete user permissions first
        db.query(UserDevicePermission).filter(UserDevicePermission.user_id == user_id).delete()
        # Delete user
        db.delete(user)
        db.commit()
        return True
    return False


# ============== DEVICE MANAGEMENT ==============

def create_device(db: Session, device: DeviceCreate, created_by: int) -> Device:
    """Create a new device"""
    db_device = Device(
        name=device.name,
        device_type=device.device_type,
        source=device.source,
        location=device.location,
        created_by=created_by
    )
    db.add(db_device)
    db.commit()
    db.refresh(db_device)
    return db_device


def get_all_devices(db: Session) -> List[Device]:
    """Get all devices"""
    return db.query(Device).filter(Device.is_active == True).all()


def get_device_by_id(db: Session, device_id: int) -> Optional[Device]:
    """Get device by ID"""
    return db.query(Device).filter(Device.id == device_id).first()


def get_device_by_source(db: Session, source: str) -> Optional[Device]:
    """Get device by source identifier"""
    return db.query(Device).filter(Device.source == source).first()


def delete_device(db: Session, device_id: int) -> bool:
    """Soft delete device (mark inactive)"""
    device = db.query(Device).filter(Device.id == device_id).first()
    
    if device:
        device.is_active = False
        db.commit()
        return True
    return False


def update_device(db: Session, device_id: int, name: str, device_type: str = None, location: str = None) -> Optional[Device]:
    """Update device information (name, device_type, location)"""
    device = db.query(Device).filter(Device.id == device_id).first()
    
    if device:
        device.name = name
        if device_type:
            device.device_type = device_type
        if location:
            device.location = location
        db.commit()
        db.refresh(device)
    
    return device


# ============== USER-DEVICE PERMISSIONS ==============

def grant_device_permission(db: Session, user_id: int, device_id: int, granted_by: int) -> UserDevicePermission:
    """Grant user access to a device"""
    # Check if permission already exists
    existing = db.query(UserDevicePermission).filter(
        UserDevicePermission.user_id == user_id,
        UserDevicePermission.device_id == device_id
    ).first()
    
    if existing:
        return existing
    
    permission = UserDevicePermission(
        user_id=user_id,
        device_id=device_id,
        granted_by=granted_by
    )
    db.add(permission)
    db.commit()
    db.refresh(permission)
    return permission


def revoke_device_permission(db: Session, user_id: int, device_id: int) -> bool:
    """Revoke user access to a device"""
    permission = db.query(UserDevicePermission).filter(
        UserDevicePermission.user_id == user_id,
        UserDevicePermission.device_id == device_id
    ).first()
    
    if permission:
        db.delete(permission)
        db.commit()
        return True
    return False


def get_user_devices(db: Session, user_id: int) -> List[Device]:
    """Get all devices a user has access to"""
    permissions = db.query(UserDevicePermission).filter(
        UserDevicePermission.user_id == user_id
    ).all()
    
    device_ids = [p.device_id for p in permissions]
    devices = db.query(Device).filter(Device.id.in_(device_ids), Device.is_active == True).all()
    return devices


def get_device_users(db: Session, device_id: int) -> List[User]:
    """Get all users with access to a device"""
    permissions = db.query(UserDevicePermission).filter(
        UserDevicePermission.device_id == device_id
    ).all()
    
    user_ids = [p.user_id for p in permissions]
    users = db.query(User).filter(User.id.in_(user_ids)).all()
    return users


def get_user_accessible_sources(db: Session, user_id: int) -> List[str]:
    """Get list of metric sources (device sources) the user has access to"""
    sources = []

    user_devices = db.query(Device).filter(
        Device.created_by == user_id,
        Device.is_active == True
    ).all()
    sources.extend([d.source for d in user_devices])

    permitted_devices = db.query(Device).join(
        UserDevicePermission,
        UserDevicePermission.device_id == Device.id
    ).filter(
        UserDevicePermission.user_id == user_id,
        Device.is_active == True
    ).all()
    sources.extend([d.source for d in permitted_devices])

    iot_devices = db.query(IoTDevice).filter(
        IoTDevice.user_id == user_id,
        IoTDevice.is_active == True
    ).all()
    sources.extend([d.source for d in iot_devices])

    # Keep order while removing duplicates
    return list(dict.fromkeys(sources))


# ============== DATABRICKS OPERATIONS ==============

def get_databricks_latest_metrics(metric_type: Optional[str] = None) -> List[Dict]:
    """
    Get latest metrics from Databricks
    
    Args:
        metric_type: Filter by IoT type ('temperature', 'humidity', 'soil_moisture', etc). None for all.
        
    Returns:
        List of latest readings from Databricks
    """
    try:
        from .databricks_client import get_databricks_client
        client = get_databricks_client()
        return client.get_latest_metrics(device_type=metric_type)
    except Exception as e:
        print(f"❌ Error fetching from Databricks: {str(e)}")
        return []


def get_databricks_metric_history(device_id: str, minutes: int = 60, limit: int = 500) -> List[Dict]:
    """Get historical data for device from Databricks"""
    try:
        from .databricks_client import get_databricks_client
        client = get_databricks_client()
        return client.get_metric_history(device_id, minutes=minutes, limit=limit)
    except Exception as e:
        print(f"❌ Error fetching history from Databricks: {str(e)}")
        return []


def get_databricks_all_devices() -> List[Dict]:
    """Get all IoT devices from Databricks"""
    try:
        from .databricks_client import get_databricks_client
        client = get_databricks_client()
        return client.get_all_devices()
    except Exception as e:
        print(f"❌ Error fetching devices from Databricks: {str(e)}")
        return []


def get_databricks_aggregated_metrics(device_type: Optional[str] = None) -> List[Dict]:
    """Get aggregated metrics from Databricks"""
    try:
        from .databricks_client import get_databricks_client
        client = get_databricks_client()
        return client.get_aggregated_metrics(device_type=device_type)
    except Exception as e:
        print(f"❌ Error fetching aggregated metrics from Databricks: {str(e)}")
        return []


def get_metrics_history_for_user(
    db: Session,
    user_id: int,
    metric_type: str,
    minutes: int = 5,
    source: Optional[str] = None
) -> List[Metric]:
    """Get historical metrics for a specific type, filtered by user's accessible devices"""
    accessible_sources = get_user_accessible_sources(db, user_id)
    
    if not accessible_sources:
        return []
    
    vietnam_tz = timezone(timedelta(hours=7))
    time_threshold = datetime.now(vietnam_tz) - timedelta(minutes=minutes)
    
    if source:
        if source not in accessible_sources:
            return []

        metrics = db.query(Metric).filter(
            Metric.metric_type == metric_type,
            Metric.source == source,
            Metric.timestamp >= time_threshold
        ).order_by(Metric.timestamp.asc()).all()
    else:
        metrics = db.query(Metric).filter(
            Metric.metric_type == metric_type,
            Metric.source.in_(accessible_sources),
            Metric.timestamp >= time_threshold
        ).order_by(Metric.timestamp.asc()).all()
    
    return metrics


def get_metrics_history_by_date(
    db: Session,
    user_id: int,
    metric_type: str,
    from_date: datetime,
    to_date: datetime,
    source: Optional[str] = None
) -> List[Metric]:
    """Get historical metrics for a date range, filtered by user's accessible devices"""
    accessible_sources = get_user_accessible_sources(db, user_id)
    
    if not accessible_sources:
        return []
    
    # Convert dates to strings in YYYY-MM-DD format
    if isinstance(from_date, datetime):
        from_date_str = from_date.strftime('%Y-%m-%d')
    else:
        from_date_str = from_date
    
    if isinstance(to_date, datetime):
        to_date_str = to_date.strftime('%Y-%m-%d')
    else:
        to_date_str = to_date
    
    if source:
        if source not in accessible_sources:
            return []

        # Use strftime for date comparison since SQLite stores timestamps as strings
        metrics = db.query(Metric).filter(
            Metric.metric_type == metric_type,
            Metric.source == source,
            func.strftime('%Y-%m-%d', Metric.timestamp) >= from_date_str,
            func.strftime('%Y-%m-%d', Metric.timestamp) <= to_date_str
        ).order_by(Metric.timestamp.asc()).all()
    else:
        metrics = db.query(Metric).filter(
            Metric.metric_type == metric_type,
            Metric.source.in_(accessible_sources),
            func.strftime('%Y-%m-%d', Metric.timestamp) >= from_date_str,
            func.strftime('%Y-%m-%d', Metric.timestamp) <= to_date_str
        ).order_by(Metric.timestamp.asc()).all()
    
    return metrics


