"""Reset local/user IoT device seed to the expected ESP32 defaults.

This script is intentionally scoped to app database rows only. It does not
touch ESP32/Arduino code.
"""

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.api.routes_auth import hash_password
from app.crud import create_user, get_user_by_username
from app.database import SessionLocal, init_db, switch_to_sqlite_fallback
from app.models import IoTDevice
from app.schemas import UserRegister


ESP32_DEFAULTS = [
    {
        "name": "Living Room2",
        "source": "esp32_devkit_v1",
        "device_type": "temperature",
        "unit": "°C",
        "location": "nha",
        "environment_type": "indoor",
    },
    {
        "name": "Living Room2",
        "source": "esp32_devkit_v1",
        "device_type": "humidity",
        "unit": "%",
        "location": "nha",
        "environment_type": "indoor",
    },
]


def main():
    try:
        init_db()
    except Exception as exc:
        print(f"Primary database unavailable, using SQLite fallback: {type(exc).__name__}: {exc}")
        switch_to_sqlite_fallback()
        init_db()
    db = SessionLocal()
    try:
        admin = get_user_by_username(db, "admin")
        if not admin:
            admin = create_user(
                db,
                UserRegister(username="admin", email="admin@example.com", password="123456", role="admin"),
                hash_password("123456"),
            )

        user = get_user_by_username(db, "user")
        if not user:
            user = create_user(
                db,
                UserRegister(username="user", email="user@example.com", password="123456", role="user"),
                hash_password("123456"),
            )
            user.is_approved = True
            user.approved_by = admin.id
            db.commit()
            db.refresh(user)

        deleted_other_esp32 = db.query(IoTDevice).filter(
            IoTDevice.source == "esp32_devkit_v1",
            ~IoTDevice.device_type.in_(["temperature", "humidity"]),
        ).delete(synchronize_session=False)

        for item in ESP32_DEFAULTS:
            if item["source"] == "esp32_devkit_v1":
                item["name"] = "Living Room2"
            if item["device_type"] == "temperature":
                item["unit"] = "°C"

        for item in ESP32_DEFAULTS:
            device = db.query(IoTDevice).filter(
                IoTDevice.source == item["source"],
                IoTDevice.device_type == item["device_type"],
            ).first()
            if not device:
                device = IoTDevice(user_id=user.id, created_by=user.id, **item)
                db.add(device)
            else:
                device.user_id = user.id
                device.created_by = device.created_by or user.id
                device.name = item["name"]
                device.unit = item["unit"]
                device.location = item["location"]
                device.environment_type = item["environment_type"]
                device.is_active = True

            device.alert_enabled = False
            device.min_threshold = None
            device.max_threshold = None
            device.lower_threshold = None
            device.upper_threshold = None

        legacy_devices = []
        normalized_legacy = 0
        removed_legacy_duplicates = 0
        for device in legacy_devices:
            metric_type = (device.device_type or "").strip().lower()
            if metric_type not in {"temperature", "humidity"}:
                continue

            existing = db.query(IoTDevice).filter(
                IoTDevice.source == "sensor_1",
                IoTDevice.device_type == metric_type,
                IoTDevice.id != device.id,
            ).first()
            if existing:
                existing.name = "Living Room"
                existing.location = "Living Room"
                existing.environment_type = "indoor"
                existing.unit = "°C" if metric_type == "temperature" else "%"
                existing.is_active = True
                db.delete(device)
                removed_legacy_duplicates += 1
                continue

            device.name = "Living Room"
            device.source = "sensor_1"
            device.device_type = metric_type
            device.unit = "°C" if metric_type == "temperature" else "%"
            device.location = "Living Room"
            device.environment_type = "indoor"
            device.is_active = True
            normalized_legacy += 1

        db.commit()
        print(f"Deleted non-default ESP32 devices: {deleted_other_esp32}")
        print("Seed reset complete: esp32_devkit_v1 temperature + humidity, thresholds NULL. sensor_1 kept unchanged.")
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
