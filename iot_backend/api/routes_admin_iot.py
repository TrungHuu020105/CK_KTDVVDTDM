"""Admin IoT management routes for standalone iot-backend."""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from iot_backend.api.routes_auth import get_current_user
from iot_backend.database import get_db
from iot_backend.models import IoTDevice, User

router = APIRouter(prefix="/api/admin", tags=["admin-iot"])


def verify_admin(user: User = Depends(get_current_user)) -> User:
    if user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can access this endpoint",
        )
    return user


def _serialize(device: IoTDevice) -> dict:
    return {
        "id": device.id,
        "user_id": device.user_id,
        "name": device.name,
        "device_type": device.device_type,
        "metric_type": device.device_type,
        "source": device.source,
        "unit": device.unit,
        "location": device.location,
        "environment_type": device.environment_type,
        "location_query": device.location_query,
        "latitude": device.latitude,
        "longitude": device.longitude,
        "timezone_name": device.timezone_name,
        "task_description": device.task_description,
        "priority_level": device.priority_level,
        "action_hint": device.action_hint,
        "is_active": device.is_active,
        "alert_enabled": device.alert_enabled,
        "min_threshold": device.min_threshold,
        "max_threshold": device.max_threshold,
        "created_at": device.created_at,
    }


@router.get("/iot-devices")
async def get_all_iot_devices(admin: User = Depends(verify_admin), db: Session = Depends(get_db)):
    devices = db.query(IoTDevice).all()
    return {"devices": [_serialize(d) for d in devices], "count": len(devices)}


@router.get("/iot-devices/users-summary")
async def get_iot_devices_summary(admin: User = Depends(verify_admin), db: Session = Depends(get_db)):
    from sqlalchemy import func

    device_counts = (
        db.query(
            User.id,
            User.username,
            User.email,
            func.count(IoTDevice.id).label("device_count"),
        )
        .outerjoin(IoTDevice, User.id == IoTDevice.user_id)
        .group_by(User.id, User.username, User.email)
        .order_by(func.count(IoTDevice.id).desc())
        .all()
    )

    result = []
    total_devices = 0
    for user_id, username, email, count in device_counts:
        result.append(
            {
                "user_id": user_id,
                "username": username,
                "email": email,
                "role": "admin" if username == "admin" else "user",
                "device_count": count or 0,
            }
        )
        total_devices += count or 0

    return {
        "users_summary": result,
        "total_devices": total_devices,
        "total_users": len(result),
    }


@router.delete("/iot-devices/{device_id}")
async def delete_iot_device(device_id: int, admin: User = Depends(verify_admin), db: Session = Depends(get_db)):
    device = db.query(IoTDevice).filter(IoTDevice.id == device_id).first()
    if not device:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="IoT device not found")

    user_id = device.user_id
    device_name = device.name
    db.delete(device)
    db.commit()

    return {
        "message": f"IoT device '{device_name}' deleted successfully",
        "user_id": user_id,
        "device_id": device_id,
    }


@router.put("/iot-devices/{device_id}/disconnect")
async def disconnect_iot_device(device_id: int, admin: User = Depends(verify_admin), db: Session = Depends(get_db)):
    device = db.query(IoTDevice).filter(IoTDevice.id == device_id).first()
    if not device:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="IoT device not found")

    device.is_active = False
    db.commit()
    db.refresh(device)

    return {"message": f"IoT device '{device.name}' disconnected from user", "device": _serialize(device)}


@router.put("/iot-devices/{device_id}/reconnect")
async def reconnect_iot_device(device_id: int, admin: User = Depends(verify_admin), db: Session = Depends(get_db)):
    device = db.query(IoTDevice).filter(IoTDevice.id == device_id).first()
    if not device:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="IoT device not found")

    device.is_active = True
    db.commit()
    db.refresh(device)

    return {"message": f"IoT device '{device.name}' reconnected to user", "device": _serialize(device)}
