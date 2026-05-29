"""CRUD operations for database"""

from datetime import datetime, timedelta, timezone
import time
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError
from iot_backend.models import (
    Metric,
    Alert,
    User,
    Device,
    UserDevicePermission,
    IoTDevice,
    ChatConversation,
    ChatMessage,
    ChatIssueTemplate,
)
from iot_backend.schemas import MetricCreate, AlertCreate, UserRegister, DeviceCreate


_METRIC_LOCATION_PLACEHOLDERS = {"", "unknown", "n/a", "na", "null", "none"}


def _normalize_metric_location_value(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip()
    if cleaned.lower() in _METRIC_LOCATION_PLACEHOLDERS:
        return None
    return cleaned or None


def _resolve_metric_location(db: Session, sensor_id: str, location: Optional[str]) -> Optional[str]:
    """Resolve canonical metric location as the user-facing sensor label.

    Source priority:
    1. IoT device location configured in DB
    2. Generic device/source location configured in DB
    3. Non-placeholder location supplied by incoming payload
    """
    normalized_location = _normalize_metric_location_value(location)

    device = (
        db.query(IoTDevice)
        .filter(IoTDevice.source == sensor_id)
        .order_by(IoTDevice.is_active.desc(), IoTDevice.id.asc())
        .first()
    )
    if device:
        canonical_location = _normalize_metric_location_value(device.location)
        if canonical_location:
            return canonical_location

    source_device = db.query(Device).filter(Device.source == sensor_id).first()
    if source_device:
        canonical_location = _normalize_metric_location_value(source_device.location)
        if canonical_location:
            return canonical_location

    return normalized_location


def create_metric(db: Session, metric: MetricCreate) -> Metric:
    """Create a single metric record"""
    event_ts = metric.event_ts if metric.event_ts else datetime.now()
    resolved_location = _resolve_metric_location(db, metric.sensor_id, metric.location)
    
    db_metric = Metric(
        event_ts=event_ts,
        sensor_id=metric.sensor_id,
        location=resolved_location,
        metric_type=metric.metric_type,
        metric_value=metric.metric_value,
        unit=metric.unit,
    )
    db.add(db_metric)
    db.commit()
    db.refresh(db_metric)
    return db_metric


def create_metrics_bulk(db: Session, metrics: List[MetricCreate]) -> List[Metric]:
    """Create multiple metric records"""
    db_metrics = []
    for metric in metrics:
        event_ts = metric.event_ts if metric.event_ts else datetime.now()
        resolved_location = _resolve_metric_location(db, metric.sensor_id, metric.location)
        db_metric = Metric(
            event_ts=event_ts,
            sensor_id=metric.sensor_id,
            location=resolved_location,
            metric_type=metric.metric_type,
            metric_value=metric.metric_value,
            unit=metric.unit,
        )
        db_metrics.append(db_metric)
    
    db.add_all(db_metrics)
    db.commit()
    
    # Refresh all objects to get IDs
    for metric in db_metrics:
        db.refresh(metric)
    
    return db_metrics


def get_latest_metrics(db: Session) -> tuple:
    """Get latest IoT metrics by type"""
    latest_temperature = db.query(Metric).filter(
        Metric.metric_type == "temperature"
    ).order_by(Metric.event_ts.desc()).first()
    latest_humidity = db.query(Metric).filter(
        Metric.metric_type == "humidity"
    ).order_by(Metric.event_ts.desc()).first()
    latest_soil_moisture = db.query(Metric).filter(
        Metric.metric_type == "soil_moisture"
    ).order_by(Metric.event_ts.desc()).first()
    latest_light_intensity = db.query(Metric).filter(
        Metric.metric_type == "light_intensity"
    ).order_by(Metric.event_ts.desc()).first()
    latest_pressure = db.query(Metric).filter(
        Metric.metric_type == "pressure"
    ).order_by(Metric.event_ts.desc()).first()
    return (
        latest_temperature,
        latest_humidity,
        latest_soil_moisture,
        latest_light_intensity,
        latest_pressure,
    )


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
        Metric.event_ts >= time_threshold
    ).order_by(Metric.event_ts.asc()).all()
    
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
        Metric.event_ts >= time_threshold
    ).order_by(Metric.event_ts.asc()).all()
    
    return metrics


def get_all_metrics_in_range(db: Session, minutes: int) -> tuple:
    """Get all IoT metric types within a time range"""
    vietnam_tz = timezone(timedelta(hours=7))
    time_threshold = datetime.now(vietnam_tz) - timedelta(minutes=minutes)
    
    temperature_metrics = db.query(Metric).filter(
        Metric.metric_type == "temperature",
        Metric.event_ts >= time_threshold
    ).all()
    
    humidity_metrics = db.query(Metric).filter(
        Metric.metric_type == "humidity",
        Metric.event_ts >= time_threshold
    ).all()
    soil_moisture_metrics = db.query(Metric).filter(
        Metric.metric_type == "soil_moisture",
        Metric.event_ts >= time_threshold
    ).all()
    light_intensity_metrics = db.query(Metric).filter(
        Metric.metric_type == "light_intensity",
        Metric.event_ts >= time_threshold
    ).all()
    pressure_metrics = db.query(Metric).filter(
        Metric.metric_type == "pressure",
        Metric.event_ts >= time_threshold
    ).all()
    
    return (
        temperature_metrics,
        humidity_metrics,
        soil_moisture_metrics,
        light_intensity_metrics,
        pressure_metrics,
    )


def delete_old_metrics(db: Session, days: int = 30) -> int:
    """Delete metrics older than specified days (for maintenance)"""
    time_threshold = datetime.utcnow() - timedelta(days=days)
    
    deleted_count = db.query(Metric).filter(
        Metric.event_ts < time_threshold
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
        source=alert.source,
        device_id=alert.device_id,
        device_name=alert.device_name,
        unit=alert.unit,
        min_threshold=alert.min_threshold,
        max_threshold=alert.max_threshold,
        created_at=alert.created_at if alert.created_at else datetime.now(),
    )
    db.add(db_alert)
    db.commit()
    db.refresh(db_alert)
    return db_alert


def get_recent_alerts(db: Session, hours: int = 24, limit: int = 100) -> List[Alert]:
    """Get recent alerts from last N hours"""
    time_threshold = datetime.now() - timedelta(hours=hours)
    
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
    time_threshold = datetime.now() - timedelta(hours=hours)
    
    alerts = db.query(Alert).filter(
        Alert.metric_type == metric_type,
        Alert.created_at >= time_threshold
    ).order_by(Alert.created_at.desc()).all()
    
    return alerts


def resolve_alert(db: Session, alert_id: int) -> Optional[Alert]:
    """Mark an alert as resolved"""
    alert = db.query(Alert).filter(Alert.id == alert_id).first()
    
    if alert:
        alert.resolved_at = datetime.now()
        db.commit()
        db.refresh(alert)
    
    return alert


def delete_old_alerts(db: Session, days: int = 15) -> int:
    """Delete all alerts older than specified days (default: 15 days)"""
    time_threshold = datetime.now() - timedelta(days=days)
    
    # Delete ALL alerts (both resolved and unresolved) older than threshold
    deleted_count = db.query(Alert).filter(
        Alert.created_at < time_threshold
    ).delete()
    
    db.commit()
    return deleted_count


# ============== USER CRUD OPERATIONS ==============

def create_user(db: Session, user: UserRegister, hashed_password: str) -> User:
    """Create a new user (auto-approve admin users)"""
    db_user = User(
        username=user.username,
        email=user.email,
        hashed_password=hashed_password,
        role=user.role,
        # Auto-approve admin users, regular users need approval
        is_approved=True if user.role == "admin" else False,
        approved_at=datetime.now(timezone(timedelta(hours=7))) if user.role == "admin" else None
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


# ============== USER APPROVAL ==============

def get_pending_users(db: Session) -> List[User]:
    """Get users pending approval"""
    return db.query(User).filter(User.is_approved == False).all()


def approve_user(db: Session, user_id: int, admin_id: int) -> Optional[User]:
    """Admin approves a user"""
    vietnam_tz = timezone(timedelta(hours=7))
    user = db.query(User).filter(User.id == user_id).first()
    
    if user:
        user.is_approved = True
        user.approved_by = admin_id
        user.approved_at = datetime.now(vietnam_tz)
        db.commit()
        db.refresh(user)
    
    return user


def reject_user(db: Session, user_id: int) -> bool:
    """Admin rejects a user (delete pending user)"""
    user = db.query(User).filter(User.id == user_id, User.is_approved == False).first()
    
    if user:
        db.delete(user)
        db.commit()
        return True
    return False


def delete_user(db: Session, user_id: int) -> bool:
    """Admin deletes any user (approved or pending)"""
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

def create_device(db: Session, device: DeviceCreate, admin_id: int) -> Device:
    """Create a new device"""
    db_device = Device(
        name=device.name,
        device_type=device.device_type,
        source=device.source,
        location=device.location,
        created_by=admin_id
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

def grant_device_permission(db: Session, user_id: int, device_id: int, admin_id: int) -> UserDevicePermission:
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
        granted_by=admin_id
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

    def _retry_all(query_fn, attempts: int = 3):
        last_exc = None
        for idx in range(attempts):
            try:
                return query_fn()
            except OperationalError as exc:
                db.rollback()
                text = str(exc)
                is_lock = "LockNotAvailable" in text or "lock timeout" in text.lower()
                if not is_lock or idx == attempts - 1:
                    last_exc = exc
                    break
                time.sleep(0.35 * (idx + 1))
        if last_exc:
            raise last_exc
        return []
    
    user = get_user_by_id(db, user_id)
    
    if user and user.role == "admin":
        # Admins see ALL devices (no restrictions)
        devices = _retry_all(lambda: db.query(Device).filter(Device.is_active == True).all())
        sources.extend([d.source for d in devices])
        
        iot_devices = _retry_all(lambda: db.query(IoTDevice).filter(IoTDevice.is_active == True).all())
        sources.extend([d.source for d in iot_devices])

        metric_sources = _retry_all(lambda: db.query(Metric.sensor_id).distinct().all())
        sources.extend([row[0] for row in metric_sources if row and row[0]])
    else:
        # Regular users see:
        # 1. System monitor (Server 1) - public
        sources.append("system_monitor")
        
        # 2. Devices they CREATED themselves
        user_devices = _retry_all(lambda: db.query(Device).filter(Device.created_by == user_id).all())
        sources.extend([d.source for d in user_devices])
        
        # 3. IoT devices they own
        iot_devices = _retry_all(lambda: db.query(IoTDevice).filter(IoTDevice.user_id == user_id).all())
        sources.extend([d.source for d in iot_devices])

    # Keep order while removing duplicates
    return list(dict.fromkeys(sources))


def get_latest_metrics_for_user(db: Session, user_id: int, source: Optional[str] = None) -> tuple:
    """Get latest values for each IoT metric type, filtered by user's accessible devices
    
    Returns None if metric is older than 30 seconds (server considered down)
    """
    from datetime import datetime, timezone, timedelta
    
    accessible_sources = get_user_accessible_sources(db, user_id)
    
    if not accessible_sources:
        return (None, None, None, None, None)
    
    # Check if metric is fresh (within last 30 seconds)
    vietnam_tz = timezone(timedelta(hours=7))
    now = datetime.now(vietnam_tz)
    stale_threshold = now - timedelta(seconds=30)
    
    if source:
        if source not in accessible_sources:
            return (None, None, None, None, None)

        latest_temperature = db.query(Metric).filter(
            Metric.metric_type == "temperature",
            Metric.sensor_id == source,
            Metric.event_ts >= stale_threshold
        ).order_by(Metric.event_ts.desc()).first()
        latest_humidity = db.query(Metric).filter(
            Metric.metric_type == "humidity",
            Metric.sensor_id == source,
            Metric.event_ts >= stale_threshold
        ).order_by(Metric.event_ts.desc()).first()
        latest_soil_moisture = db.query(Metric).filter(
            Metric.metric_type == "soil_moisture",
            Metric.sensor_id == source,
            Metric.event_ts >= stale_threshold
        ).order_by(Metric.event_ts.desc()).first()
        latest_light_intensity = db.query(Metric).filter(
            Metric.metric_type == "light_intensity",
            Metric.sensor_id == source,
            Metric.event_ts >= stale_threshold
        ).order_by(Metric.event_ts.desc()).first()
        latest_pressure = db.query(Metric).filter(
            Metric.metric_type == "pressure",
            Metric.sensor_id == source,
            Metric.event_ts >= stale_threshold
        ).order_by(Metric.event_ts.desc()).first()
    else:
        latest_temperature = db.query(Metric).filter(
            Metric.metric_type == "temperature",
            Metric.sensor_id.in_(accessible_sources),
            Metric.event_ts >= stale_threshold
        ).order_by(Metric.event_ts.desc()).first()
        latest_humidity = db.query(Metric).filter(
            Metric.metric_type == "humidity",
            Metric.sensor_id.in_(accessible_sources),
            Metric.event_ts >= stale_threshold
        ).order_by(Metric.event_ts.desc()).first()
        latest_soil_moisture = db.query(Metric).filter(
            Metric.metric_type == "soil_moisture",
            Metric.sensor_id.in_(accessible_sources),
            Metric.event_ts >= stale_threshold
        ).order_by(Metric.event_ts.desc()).first()
        latest_light_intensity = db.query(Metric).filter(
            Metric.metric_type == "light_intensity",
            Metric.sensor_id.in_(accessible_sources),
            Metric.event_ts >= stale_threshold
        ).order_by(Metric.event_ts.desc()).first()
        latest_pressure = db.query(Metric).filter(
            Metric.metric_type == "pressure",
            Metric.sensor_id.in_(accessible_sources),
            Metric.event_ts >= stale_threshold
        ).order_by(Metric.event_ts.desc()).first()
    
    return (
        latest_temperature,
        latest_humidity,
        latest_soil_moisture,
        latest_light_intensity,
        latest_pressure,
    )


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
            Metric.sensor_id == source,
            Metric.event_ts >= time_threshold
        ).order_by(Metric.event_ts.asc()).all()
    else:
        metrics = db.query(Metric).filter(
            Metric.metric_type == metric_type,
            Metric.sensor_id.in_(accessible_sources),
            Metric.event_ts >= time_threshold
        ).order_by(Metric.event_ts.asc()).all()
    
    return metrics


def get_latest_metric_for_user(
    db: Session,
    user_id: int,
    *,
    metric_type: str,
    source: str,
) -> Optional[Metric]:
    """Get the latest metric for a specific (source, metric_type) if user can access it."""
    accessible_sources = get_user_accessible_sources(db, user_id)
    if not accessible_sources:
        return None
    if source not in accessible_sources:
        return None

    return (
        db.query(Metric)
        .filter(
            Metric.metric_type == metric_type,
            Metric.sensor_id == source,
        )
        .order_by(Metric.event_ts.desc())
        .first()
    )


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
    
    # Normalize to whole-day range [from_date 00:00:00, to_date+1 00:00:00)
    if not isinstance(from_date, datetime):
        from_date = datetime.fromisoformat(str(from_date))
    if not isinstance(to_date, datetime):
        to_date = datetime.fromisoformat(str(to_date))

    vietnam_tz = timezone(timedelta(hours=7))

    if from_date.tzinfo is None:
        from_date = from_date.replace(tzinfo=vietnam_tz)
    else:
        from_date = from_date.astimezone(vietnam_tz)

    if to_date.tzinfo is None:
        to_date = to_date.replace(tzinfo=vietnam_tz)
    else:
        to_date = to_date.astimezone(vietnam_tz)

    range_start = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
    range_end = (to_date + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    if source:
        if source not in accessible_sources:
            return []

        metrics = db.query(Metric).filter(
            Metric.metric_type == metric_type,
            Metric.sensor_id == source,
            Metric.event_ts >= range_start,
            Metric.event_ts < range_end
        ).order_by(Metric.event_ts.asc()).all()
    else:
        metrics = db.query(Metric).filter(
            Metric.metric_type == metric_type,
            Metric.sensor_id.in_(accessible_sources),
            Metric.event_ts >= range_start,
            Metric.event_ts < range_end
        ).order_by(Metric.event_ts.asc()).all()
    
    return metrics


# ============== CHAT SUPPORT ==============

def create_chat_conversation(
    db: Session,
    user_id: int,
    status: str = "bot_active",
    subject: Optional[str] = None,
) -> ChatConversation:
    now = datetime.now(timezone(timedelta(hours=7)))
    row = ChatConversation(
        user_id=user_id,
        status=status,
        subject=subject,
        created_at=now,
        updated_at=now,
        last_read_by_user_at=now if status == "bot_active" else None,
        last_read_by_admin_at=None,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def get_chat_conversation(db: Session, conversation_id: int) -> Optional[ChatConversation]:
    return db.query(ChatConversation).filter(ChatConversation.id == conversation_id).first()


def get_latest_user_chat_conversation(db: Session, user_id: int) -> Optional[ChatConversation]:
    return (
        db.query(ChatConversation)
        .filter(ChatConversation.user_id == user_id)
        .order_by(ChatConversation.updated_at.desc())
        .first()
    )


def list_user_chat_conversations(db: Session, user_id: int) -> List[ChatConversation]:
    return (
        db.query(ChatConversation)
        .filter(ChatConversation.user_id == user_id)
        .order_by(ChatConversation.updated_at.desc())
        .all()
    )


def list_admin_chat_conversations(db: Session, status_filter: Optional[str] = None) -> List[ChatConversation]:
    query = db.query(ChatConversation)
    if status_filter and status_filter != "all":
        query = query.filter(ChatConversation.status == status_filter)
    return query.order_by(ChatConversation.updated_at.desc()).all()


def update_chat_conversation_status(
    db: Session,
    conversation: ChatConversation,
    status: str,
    assigned_admin_id: Optional[int] = None,
) -> ChatConversation:
    conversation.status = status
    if assigned_admin_id is not None:
        conversation.assigned_admin_id = assigned_admin_id
    conversation.updated_at = datetime.now(timezone(timedelta(hours=7)))
    db.commit()
    db.refresh(conversation)
    return conversation


def create_chat_message(
    db: Session,
    conversation_id: int,
    sender_type: str,
    content: str,
    sender_id: Optional[int] = None,
) -> ChatMessage:
    now = datetime.now(timezone(timedelta(hours=7)))
    row = ChatMessage(
        conversation_id=conversation_id,
        sender_type=sender_type,
        sender_id=sender_id,
        content=content,
        created_at=now,
    )
    db.add(row)

    conversation = get_chat_conversation(db, conversation_id)
    if conversation:
        conversation.updated_at = now
        if sender_type == "user":
            conversation.last_read_by_user_at = now
        elif sender_type in {"admin", "bot", "system"}:
            conversation.last_read_by_admin_at = now
    db.commit()
    db.refresh(row)
    return row


def list_chat_messages(db: Session, conversation_id: int) -> List[ChatMessage]:
    return (
        db.query(ChatMessage)
        .filter(ChatMessage.conversation_id == conversation_id)
        .order_by(ChatMessage.created_at.asc())
        .all()
    )


def delete_chat_conversation(db: Session, conversation: ChatConversation) -> None:
    db.query(ChatMessage).filter(ChatMessage.conversation_id == conversation.id).delete()
    db.delete(conversation)
    db.commit()


def list_chat_issue_templates(db: Session, active_only: bool = True) -> List[ChatIssueTemplate]:
    query = db.query(ChatIssueTemplate)
    if active_only:
        query = query.filter(ChatIssueTemplate.is_active == True)
    return query.order_by(ChatIssueTemplate.sort_order.asc(), ChatIssueTemplate.id.asc()).all()


def get_chat_issue_template(db: Session, template_id: int) -> Optional[ChatIssueTemplate]:
    return db.query(ChatIssueTemplate).filter(ChatIssueTemplate.id == template_id).first()


def create_chat_issue_template(
    db: Session,
    title: str,
    description: Optional[str],
    created_by: Optional[int],
    sort_order: int = 0,
    is_active: bool = True,
) -> ChatIssueTemplate:
    now = datetime.now(timezone(timedelta(hours=7)))
    row = ChatIssueTemplate(
        title=title.strip(),
        description=(description or "").strip() or None,
        is_active=is_active,
        sort_order=sort_order,
        created_by=created_by,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def update_chat_issue_template(db: Session, row: ChatIssueTemplate, data: dict) -> ChatIssueTemplate:
    if "title" in data and data["title"] is not None:
        row.title = data["title"].strip()
    if "description" in data:
        value = (data["description"] or "").strip()
        row.description = value or None
    if "sort_order" in data and data["sort_order"] is not None:
        row.sort_order = int(data["sort_order"])
    if "is_active" in data and data["is_active"] is not None:
        row.is_active = bool(data["is_active"])
    row.updated_at = datetime.now(timezone(timedelta(hours=7)))
    db.commit()
    db.refresh(row)
    return row


def delete_chat_issue_template(db: Session, row: ChatIssueTemplate) -> None:
    db.delete(row)
    db.commit()
