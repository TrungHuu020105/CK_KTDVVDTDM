"""Authentication routes"""

from datetime import datetime, timedelta
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status, Header
from pydantic import BaseModel
from sqlalchemy.orm import Session
from jose import JWTError, jwt
from passlib.context import CryptContext
from iot_backend.database import get_db
from iot_backend.models import User, UserNotificationTarget
from iot_backend.schemas import UserRegister, UserLogin, TokenResponse, UserResponse, DeviceResponse
from iot_backend import crud
from iot_backend.config import SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES
from iot_backend.services.telegram_service import send_telegram_message
from iot_backend.services.email_service import send_email_alert

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

router = APIRouter(prefix="/api/auth", tags=["auth"])

SUCCESS_NOTIFICATION_TEXT = """✅ Kết nối Telegram thành công!

Bạn sẽ nhận được thông báo cảnh báo từ các thiết bị IoT của mình khi chúng vượt ngưỡng.

🔔 Tính năng:
• Real-time alerts khi thiết bị vượt ngưỡng
• Thông tin chi tiết: tên thiết bị, giá trị, ngưỡng
• Thời gian chính xác (Vietnam TZ)

Quản lý ngưỡng cảnh báo tại dashboard của bạn."""


def _success_notification_html() -> str:
    lines = SUCCESS_NOTIFICATION_TEXT.split("\n")
    html_lines = "<br>".join(lines)
    return f"<div style='font-family:Arial,sans-serif;line-height:1.6'>{html_lines}</div>"


class TelegramLinkRequest(BaseModel):
    chat_id: str


class EmailEnableRequest(BaseModel):
    email: str


class NotificationTargetCreateRequest(BaseModel):
    target_type: str  # telegram | email
    target_value: str


class NotificationTargetToggleRequest(BaseModel):
    is_enabled: bool


def hash_password(password: str) -> str:
    """Hash a password"""
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash"""
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(data: dict):
    """Create JWT access token"""
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def get_current_user(authorization: str = Header(None), db: Session = Depends(get_db)) -> User:
    """Get current user from JWT token in Authorization header"""
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Extract token from "Bearer <token>" format
    try:
        scheme, token = authorization.split()
        if scheme.lower() != "bearer":
            raise ValueError()
    except (ValueError, IndexError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
        )
    
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials",
            )
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
        )
    
    user = crud.get_user_by_username(db, username=username)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
        )
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Inactive user",
        )
    return user


@router.post("/register", response_model=UserResponse, status_code=201)
async def register(user_data: UserRegister, db: Session = Depends(get_db)):
    """Register a new user"""
    # Check if username exists
    existing_user = crud.get_user_by_username(db, user_data.username)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered"
        )
    
    # Check if email exists
    existing_email = crud.get_user_by_email(db, user_data.email)
    if existing_email:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )
    
    # Hash password
    hashed_password = hash_password(user_data.password)
    
    # Create user
    user = crud.create_user(db, user_data, hashed_password)
    return user


@router.post("/login", response_model=TokenResponse)
async def login(credentials: UserLogin, db: Session = Depends(get_db)):
    """Login user and get JWT token"""
    # Get user by username
    user = crud.get_user_by_username(db, credentials.username)
    
    if not user or not verify_password(credentials.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password"
        )
    
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User is inactive"
        )
    
    # Check if user is approved by admin (skip for admin role)
    if not user.is_approved and user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your account is pending admin approval"
        )
    
    # Create access token
    access_token = create_access_token(data={"sub": user.username})
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "role": user.role
        }
    }


@router.get("/me", response_model=UserResponse)
async def get_me(current_user: User = Depends(get_current_user)):
    """Get current user info"""
    return current_user


@router.get("/me/devices")
async def get_my_devices(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get current user's accessible devices"""
    devices = crud.get_user_devices(db, current_user.id)
    return {
        "devices": [
            {
                "id": d.id,
                "name": d.name,
                "device_type": d.device_type,
                "source": d.source,
                "location": d.location,
                "is_active": d.is_active,
                "created_at": d.created_at
            }
            for d in devices
        ],
        "count": len(devices)
    }


@router.get("/users/{user_id}")
async def get_user_info(user_id: int, db: Session = Depends(get_db)):
    """Get user information (public endpoint for fetching user names)"""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    return {
        "id": user.id,
        "username": user.username,
        "email": user.email,
        "role": user.role
    }


@router.post("/telegram/link")
async def link_telegram(
    payload: TelegramLinkRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    existing = db.query(User).filter(User.telegram_chat_id == payload.chat_id, User.id != current_user.id).first()
    if existing:
        raise HTTPException(status_code=400, detail="This chat_id is already linked to another account")
    current_user.telegram_chat_id = payload.chat_id
    current_user.telegram_enabled = True
    db.commit()
    ok, detail = send_telegram_message(payload.chat_id, SUCCESS_NOTIFICATION_TEXT)
    if not ok:
        return {"success": False, "message": detail}

    destination_email = current_user.notification_email or current_user.email
    if current_user.email_enabled and destination_email:
        send_email_alert(destination_email, "Kết nối cảnh báo IoT thành công", _success_notification_html())

    return {"success": True, "message": "Telegram linked and confirmation was sent"}


@router.post("/telegram/test")
async def test_telegram(
    current_user: User = Depends(get_current_user),
):
    if not current_user.telegram_chat_id:
        raise HTTPException(status_code=400, detail="Telegram is not linked")
    ok, detail = send_telegram_message(current_user.telegram_chat_id, "Test alert from Metrics system.")
    if not ok:
        raise HTTPException(status_code=400, detail=detail)
    return {"success": True, "message": "Telegram test message sent"}


@router.delete("/telegram/unlink")
async def unlink_telegram(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    current_user.telegram_chat_id = None
    current_user.telegram_enabled = False
    db.commit()
    return {"success": True, "message": "Telegram unlinked"}


@router.post("/email/enable")
async def enable_email_alerts(
    payload: EmailEnableRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    current_user.notification_email = str(payload.email)
    current_user.email_enabled = True
    db.commit()
    subject = "Kết nối cảnh báo IoT thành công"
    body = _success_notification_html()
    ok, detail = send_email_alert(str(payload.email), subject, body)
    if not ok:
        raise HTTPException(status_code=400, detail=detail)

    if current_user.telegram_enabled and current_user.telegram_chat_id:
        send_telegram_message(current_user.telegram_chat_id, SUCCESS_NOTIFICATION_TEXT)

    return {"success": True, "message": "Email enabled and test message sent"}


@router.post("/email/test")
async def test_email_alerts(
    current_user: User = Depends(get_current_user),
):
    to_email = current_user.notification_email or current_user.email
    if not to_email:
        raise HTTPException(status_code=400, detail="No email configured")
    ok, detail = send_email_alert(
        to_email,
        "Metrics alert test",
        "<h3>Test email</h3><p>Email alert channel is working.</p>",
    )
    if not ok:
        raise HTTPException(status_code=400, detail=detail)
    return {"success": True, "message": "Test email sent"}


@router.get("/email/status")
async def get_email_status(current_user: User = Depends(get_current_user)):
    return {
        "email": current_user.notification_email or current_user.email,
        "email_enabled": current_user.email_enabled,
    }


@router.patch("/email/toggle")
async def toggle_email_alerts(
    enabled: bool,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    current_user.email_enabled = enabled
    db.commit()
    return {"success": True, "email_enabled": current_user.email_enabled}


@router.patch("/email/update")
async def update_email_address(
    payload: EmailEnableRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    current_user.notification_email = str(payload.email)
    db.commit()
    return {"success": True, "email": current_user.notification_email}


@router.delete("/email/disable")
async def disable_email_alerts(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    current_user.email_enabled = False
    db.commit()
    return {"success": True, "message": "Email alerts disabled"}


@router.get("/notifications/targets")
async def list_notification_targets(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    rows = db.query(UserNotificationTarget).filter(UserNotificationTarget.user_id == current_user.id).all()
    return {
        "targets": [
            {
                "id": r.id,
                "target_type": r.target_type,
                "target_value": r.target_value,
                "is_enabled": r.is_enabled,
                "created_at": r.created_at,
            }
            for r in rows
        ]
    }


@router.post("/notifications/targets")
async def add_notification_target(
    payload: NotificationTargetCreateRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    ttype = (payload.target_type or "").strip().lower()
    value = (payload.target_value or "").strip()
    if ttype not in {"telegram", "email"}:
        raise HTTPException(status_code=400, detail="target_type must be telegram or email")
    if not value:
        raise HTTPException(status_code=400, detail="target_value is required")

    existing = db.query(UserNotificationTarget).filter(
        UserNotificationTarget.user_id == current_user.id,
        UserNotificationTarget.target_type == ttype,
        UserNotificationTarget.target_value == value,
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail="Target already exists")

    row = UserNotificationTarget(
        user_id=current_user.id,
        target_type=ttype,
        target_value=value,
        is_enabled=True,
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    if ttype == "telegram":
        send_telegram_message(value, SUCCESS_NOTIFICATION_TEXT)
    else:
        send_email_alert(value, "Kết nối cảnh báo IoT thành công", _success_notification_html())

    return {"success": True, "target_id": row.id}


@router.patch("/notifications/targets/{target_id}")
async def toggle_notification_target(
    target_id: int,
    payload: NotificationTargetToggleRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    row = db.query(UserNotificationTarget).filter(
        UserNotificationTarget.id == target_id,
        UserNotificationTarget.user_id == current_user.id,
    ).first()
    if not row:
        raise HTTPException(status_code=404, detail="Target not found")
    row.is_enabled = payload.is_enabled
    db.commit()
    return {"success": True, "is_enabled": row.is_enabled}


@router.post("/notifications/targets/{target_id}/test")
async def test_notification_target(
    target_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    row = db.query(UserNotificationTarget).filter(
        UserNotificationTarget.id == target_id,
        UserNotificationTarget.user_id == current_user.id,
    ).first()
    if not row:
        raise HTTPException(status_code=404, detail="Target not found")

    if row.target_type == "telegram":
        ok, detail = send_telegram_message(row.target_value, "Test alert from MetricsPulse.")
    elif row.target_type == "email":
        ok, detail = send_email_alert(row.target_value, "MetricsPulse alert test", "<h3>Test email</h3><p>Email alert channel is working.</p>")
    else:
        raise HTTPException(status_code=400, detail="Unsupported target type")

    if not ok:
        raise HTTPException(status_code=400, detail=detail)
    return {"success": True, "message": "Test notification sent"}


@router.delete("/notifications/targets/{target_id}")
async def delete_notification_target(
    target_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    row = db.query(UserNotificationTarget).filter(
        UserNotificationTarget.id == target_id,
        UserNotificationTarget.user_id == current_user.id,
    ).first()
    if not row:
        raise HTTPException(status_code=404, detail="Target not found")
    db.delete(row)
    db.commit()
    return {"success": True}
