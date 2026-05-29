"""Admin management routes"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import User
from app.schemas import DeviceCreate, DeviceUpdate, DeviceResponse, UserResponse
from app.api.routes_auth import get_current_user
from app import crud

router = APIRouter(prefix="/api/admin", tags=["admin"])


def verify_admin(user: User = Depends(get_current_user)) -> User:
    """Verify that current user is admin"""
    if user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can access this endpoint"
        )
    return user


# ============== USER MANAGEMENT ==============

@router.get("/users/pending")
async def get_pending_users(admin: User = Depends(verify_admin), db: Session = Depends(get_db)):
    """Get users pending approval"""
    users = crud.get_pending_users(db)
    return {
        "users": [
            {
                "id": u.id,
                "username": u.username,
                "email": u.email,
                "created_at": u.created_at
            }
            for u in users
        ],
        "count": len(users)
    }


@router.post("/users/{user_id}/approve")
async def approve_user(user_id: int, admin: User = Depends(verify_admin), db: Session = Depends(get_db)):
    """Admin approves a pending user"""
    user = crud.approve_user(db, user_id, admin.id)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    return {
        "message": f"User {user.username} approved successfully",
        "user": {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "is_approved": user.is_approved,
            "approved_at": user.approved_at
        }
    }


@router.post("/users/{user_id}/reject")
async def reject_user(user_id: int, admin: User = Depends(verify_admin), db: Session = Depends(get_db)):
    """Admin rejects a pending user"""
    success = crud.reject_user(db, user_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found or already approved"
        )
    
    return {"message": "User rejected and deleted"}


@router.get("/users")
async def get_all_users(admin: User = Depends(verify_admin), db: Session = Depends(get_db)):
    """Get all users"""
    users = crud.get_all_users(db)
    return {
        "users": [
            {
                "id": u.id,
                "username": u.username,
                "email": u.email,
                "role": u.role,
                "is_approved": u.is_approved,
                "created_at": u.created_at
            }
            for u in users
        ],
        "count": len(users)
    }


@router.delete("/users/{user_id}")
async def delete_user(user_id: int, admin: User = Depends(verify_admin), db: Session = Depends(get_db)):
    """Admin deletes a user"""
    # Prevent deleting self
    if user_id == admin.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete yourself"
        )
    
    success = crud.delete_user(db, user_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    return {"message": "User deleted successfully"}


# ============== DEVICE MANAGEMENT ==============

@router.post("/devices", response_model=DeviceResponse, status_code=201)
async def create_device(device: DeviceCreate, admin: User = Depends(verify_admin), db: Session = Depends(get_db)):
    """Create a new device"""
    # Check if source already exists
    existing = crud.get_device_by_source(db, device.source)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Device with this source already exists"
        )
    
    db_device = crud.create_device(db, device, admin.id)
    return db_device


@router.get("/devices")
async def get_all_devices(admin: User = Depends(verify_admin), db: Session = Depends(get_db)):
    """Get all devices"""
    devices = crud.get_all_devices(db)
    payload = []
    for d in devices:
        users = crud.get_device_users(db, d.id)
        payload.append(
            {
                "id": d.id,
                "name": d.name,
                "device_type": d.device_type,
                "source": d.source,
                "location": d.location,
                "is_active": d.is_active,
                "created_by": d.created_by,
                "created_at": d.created_at,
                "users": [{"id": u.id, "username": u.username, "email": u.email} for u in users],
            }
        )
    return {
        "devices": payload,
        "count": len(payload)
    }


@router.delete("/devices/{device_id}")
async def delete_device(device_id: int, admin: User = Depends(verify_admin), db: Session = Depends(get_db)):
    """Delete a device"""
    success = crud.delete_device(db, device_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Device not found"
        )
    
    return {"message": "Device deleted successfully"}


@router.put("/devices/{device_id}", response_model=DeviceResponse)
async def update_device(device_id: int, device_update: DeviceUpdate, admin: User = Depends(verify_admin), db: Session = Depends(get_db)):
    """Update device information (name, device_type, location, is_active)"""
    # Get the device first to check if it exists
    db_device = crud.get_device_by_id(db, device_id)
    if not db_device:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Device not found"
        )
    
    # Update only provided fields
    if device_update.name is not None:
        db_device.name = device_update.name
    if device_update.device_type is not None:
        db_device.device_type = device_update.device_type
    if device_update.location is not None:
        db_device.location = device_update.location
    if device_update.is_active is not None:
        db_device.is_active = device_update.is_active
    
    db.commit()
    db.refresh(db_device)
    
    return db_device


@router.put("/devices/{device_id}/toggle")
async def toggle_device_active(device_id: int, admin: User = Depends(verify_admin), db: Session = Depends(get_db)):
    """Toggle device is_active status (enable/disable metric generation)"""
    db_device = crud.get_device_by_id(db, device_id)
    if not db_device:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Device not found"
        )
    
    db_device.is_active = not db_device.is_active
    db.commit()
    db.refresh(db_device)
    
    status_str = "enabled" if db_device.is_active else "disabled"
    return {
        "id": db_device.id,
        "name": db_device.name,
        "is_active": db_device.is_active,
        "message": f"Device {status_str} successfully. Metric generation will {'' if db_device.is_active else 'not '}continue."
    }




# ============== DEVICE PERMISSIONS ==============

@router.post("/users/{user_id}/devices/{device_id}/grant")
async def grant_permission(user_id: int, device_id: int, admin: User = Depends(verify_admin), db: Session = Depends(get_db)):
    """Grant user access to a device"""
    # Verify user exists
    user = crud.get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # Verify device exists
    device = crud.get_device_by_id(db, device_id)
    if not device:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Device not found"
        )
    
    # Grant permission
    crud.grant_device_permission(db, user_id, device_id, admin.id)
    
    return {
        "message": f"Granted {user.username} access to {device.name}",
        "user_id": user_id,
        "device_id": device_id
    }


@router.delete("/users/{user_id}/devices/{device_id}/revoke")
async def revoke_permission(user_id: int, device_id: int, admin: User = Depends(verify_admin), db: Session = Depends(get_db)):
    """Revoke user access to a device"""
    success = crud.revoke_device_permission(db, user_id, device_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Permission not found"
        )
    
    return {"message": "Permission revoked successfully"}


@router.get("/users/{user_id}/devices")
async def get_user_devices(user_id: int, admin: User = Depends(verify_admin), db: Session = Depends(get_db)):
    """Get devices a user has access to"""
    user = crud.get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    devices = crud.get_user_devices(db, user_id)
    return {
        "username": user.username,
        "devices": devices,
        "count": len(devices)
    }


@router.get("/devices/{device_id}/users")
async def get_device_users(device_id: int, admin: User = Depends(verify_admin), db: Session = Depends(get_db)):
    """Get users with access to a device"""
    device = crud.get_device_by_id(db, device_id)
    if not device:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Device not found"
        )
    
    users = crud.get_device_users(db, device_id)
    return {
        "device": device.name,
        "users": [
            {
                "id": u.id,
                "username": u.username,
                "email": u.email
            }
            for u in users
        ],
        "count": len(users)
    }
