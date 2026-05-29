"""FastAPI application entry point"""

import asyncio
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import app.database as database
from app.database import init_db, SessionLocal, switch_to_sqlite_fallback
from app.config import get_cors_origins
from app.api import (
    routes_alerts,
    routes_auth,
    routes_admin,
    routes_admin_iot,
    routes_chat,
    routes_iot_devices,
    routes_metrics,
    routes_iot_proxy,
    routes_sensors,
    routes_websocket,
)
from app.crud import get_user_by_username, create_user
from app.models import IoTDevice
from app.schemas import UserRegister
from app.api.routes_auth import hash_password
from app.services.mqtt_ingest_service import start_mqtt_ingest, stop_mqtt_ingest

# Create FastAPI app
app = FastAPI(
    title="Real-Time Metrics Processing System",
    description="MVP backend for receiving, storing, and processing system metrics",
    version="2.0.0"
)

# Add CORS middleware for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Include routes (core backend only)
app.include_router(routes_auth.router)
app.include_router(routes_admin.router)
app.include_router(routes_admin_iot.router)
app.include_router(routes_iot_devices.router)
app.include_router(routes_metrics.router)
app.include_router(routes_alerts.router)
app.include_router(routes_chat.router)
app.include_router(routes_iot_proxy.router)
app.include_router(routes_iot_proxy.devices_router)
app.include_router(routes_sensors.router)
app.include_router(routes_websocket.router, prefix="/api")


async def _init_db_with_retry() -> str:
    attempts = max(1, int(os.getenv("DB_INIT_RETRIES", "2")))
    delay_seconds = max(0.5, float(os.getenv("DB_INIT_RETRY_DELAY", "1")))
    for attempt in range(1, attempts + 1):
        try:
            init_db()
            return "primary"
        except Exception as exc:
            if attempt >= attempts:
                allow_fallback = os.getenv("DB_SQLITE_FALLBACK", "true").strip().lower() in {"1", "true", "yes", "on"}
                if not allow_fallback:
                    print(f"[ERROR] [DB] Startup init failed after {attempts} attempts: {type(exc).__name__}: {exc}")
                    raise
                print(
                    f"[WARN] [DB] Primary database unavailable after {attempts} attempts: "
                    f"{type(exc).__name__}: {exc}"
                )
                switch_to_sqlite_fallback()
                init_db()
                return "sqlite_fallback"
            print(
                f"[WARN] [DB] Startup init failed "
                f"(attempt {attempt}/{attempts}): {type(exc).__name__}: {exc}. Retrying..."
            )
            await asyncio.sleep(delay_seconds)
    return "primary"


def _seed_dev_data(db_mode: str):
    should_seed = os.getenv("AUTO_SEED_IOT_DEVICES", "true").strip().lower() in {"1", "true", "yes", "on"}
    if not should_seed:
        return

    db = SessionLocal()
    try:
        admin_user = get_user_by_username(db, "admin")
        if not admin_user:
            admin_data = UserRegister(username="admin", email="admin@example.com", password="123456", role="admin")
            admin_user = create_user(db, admin_data, hash_password("123456"))
            print("[OK] [Startup] Created demo admin user (admin/123456)")

        user_user = get_user_by_username(db, "user")
        if not user_user:
            user_data = UserRegister(username="user", email="user@example.com", password="123456", role="user")
            user_user = create_user(db, user_data, hash_password("123456"))
            user_user.is_approved = True
            user_user.approved_by = admin_user.id
            db.commit()
            db.refresh(user_user)
            print("[OK] [Startup] Created demo user (user/123456)")

        demo_devices = [
            {
                "name": "Living Room Sensor",
                "device_type": "temperature_humidity",
                "source": "esp32_devkit_v1",
                "unit": "C,%",
                "location": "nha",
                "environment_type": "indoor",
            },
        ]
        for item in demo_devices:
            device = db.query(IoTDevice).filter(
                IoTDevice.source == item["source"],
                IoTDevice.device_type == item["device_type"],
            ).first()
            if not device:
                db.add(
                    IoTDevice(
                        user_id=user_user.id,
                        name=item["name"],
                        device_type=item["device_type"],
                        source=item["source"],
                        unit=item["unit"],
                        source_type="physical_iot",
                        capabilities="temperature,humidity",
                        location=item["location"],
                        environment_type=item["environment_type"],
                        alert_enabled=False,
                        min_threshold=None,
                        max_threshold=None,
                        is_active=True,
                        created_by=user_user.id,
                    )
                )
            else:
                if device.user_id != user_user.id:
                    device.user_id = user_user.id
                device.name = item["name"]
                device.device_type = item["device_type"]
                device.unit = item["unit"]
                device.source_type = "physical_iot"
                device.capabilities = "temperature,humidity"
                device.location = item["location"]
                device.environment_type = item["environment_type"]
                device.is_active = True
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"[ERROR] [Startup Seed Error] {str(e)}")
    finally:
        db.close()


def _normalize_legacy_fake_devices():
    """Normalize old demo fake-device rows such as X2/sensor_2.

    The fake stream publishes both temperature and humidity using source=sensor_1,
    so legacy cards must also point at sensor_1 to receive realtime values.
    """
    db = SessionLocal()
    try:
        legacy_devices = db.query(IoTDevice).filter(
            (IoTDevice.name == "X2") | (IoTDevice.source == "sensor_2")
        ).all()
        if not legacy_devices:
            return

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
                print(
                    "[Startup] Legacy fake device duplicated; "
                    f"kept sensor_1/{metric_type} id={existing.id}, removed legacy device id={device.id}"
                )
                continue

            old_name = device.name
            old_source = device.source
            device.name = "Living Room"
            device.source = "sensor_1"
            device.device_type = metric_type
            device.unit = "°C" if metric_type == "temperature" else "%"
            device.location = "Living Room"
            device.environment_type = "indoor"
            device.is_active = True
            print(
                "[Startup] Normalized legacy fake device "
                f"id={device.id}: name={old_name} source={old_source} -> "
                f"name=Living Room source=sensor_1 metric={metric_type}"
            )

        db.commit()
    except Exception as exc:
        db.rollback()
        print(f"[WARN] [Startup] Failed to normalize legacy fake devices: {exc}")
    finally:
        db.close()


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Welcome to Real-Time Metrics Processing System API",
        "docs": "/docs",
        "health": "/api/health"
    }


@app.on_event("startup")
async def startup_event():
    """Initialize on server startup"""
    db_mode = await _init_db_with_retry()
    _seed_dev_data(db_mode)
    iot_route_count = sum(1 for route in app.routes if "iot-devices" in getattr(route, "path", ""))
    print(f"[OK] [Startup] IoT device routes loaded: {iot_route_count}")
    if os.getenv("ENABLE_MQTT_INGEST", "false").strip().lower() in {"1", "true", "yes", "on"}:
        try:
            start_mqtt_ingest()
        except Exception as exc:
            print(f"[MQTT] Failed to start ingest: {exc}")


@app.on_event("shutdown")
async def shutdown_event():
    """Close pooled DB connections on reload/shutdown."""
    stop_mqtt_ingest()
    database.engine.dispose()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
