"""
WebSocket routes for real-time IoT metrics streaming.
"""

import asyncio
import json
import time
from datetime import datetime

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from jose import JWTError, jwt

from iot_backend.config import ALGORITHM, SECRET_KEY
from iot_backend.crud import create_alert, create_metrics_bulk, get_user_accessible_sources, get_user_by_username
from iot_backend.database import SessionLocal
from iot_backend.models import IoTDevice
from iot_backend.schemas import AlertCreate, MetricCreate
from iot_backend.schemas_ws import IotMetricsData, MetricsData, StatusResponse
from iot_backend.services.alert_service import dispatch_alert_notifications
from iot_backend.services.sensor_reading_service import create_sensor_reading
from iot_backend.websocket_manager import ConnectionManager

router = APIRouter(tags=["WebSocket Metrics"])

manager = ConnectionManager()
ALERT_NOTIFY_COOLDOWN_SECONDS = 5
_last_alert_notification_ts = {}


def _normalize_source(payload: dict, fallback: str | None = None) -> str:
    return str(payload.get("source") or payload.get("sensor_id") or fallback or "").strip()


def _decode_ws_token(token: str | None):
    if not token:
        return None
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if not username:
            return None
        db = SessionLocal()
        try:
            user = get_user_by_username(db, username)
            if not user or not user.is_active:
                return None
            return {
                "user_id": user.id,
                "role": user.role,
                "username": user.username,
            }
        finally:
            db.close()
    except JWTError:
        return None


def _can_receive_source(connection_info: dict, source: str) -> bool:
    metadata = connection_info.get("metadata") or {}
    user_id = metadata.get("user_id")
    role = metadata.get("role")
    if not user_id:
        # Unauthenticated clients are publishers-only.
        return False
    if role == "admin":
        return True

    db = SessionLocal()
    try:
        allowed_sources = set(get_user_accessible_sources(db, int(user_id)))
    finally:
        db.close()
    return source in allowed_sources


def _parse_metric_timestamp(timestamp: str | None) -> datetime:
    if not timestamp:
        return datetime.now()
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            return parsed.astimezone().replace(tzinfo=None)
        return parsed
    except Exception:
        return datetime.now()


def _check_and_trigger_alert(db, metric_type: str, source: str, value: float, metric_ts: datetime):
    print(f"[ALERT] Received metric source={source} metric_type={metric_type} value={value}")
    try:
        device = db.query(IoTDevice).filter(
            IoTDevice.source == source,
            IoTDevice.device_type == metric_type,
        ).first()
        if not device:
            device = db.query(IoTDevice).filter(IoTDevice.source == source).first()
    except Exception as exc:
        print(f"[ALERT] Failed to query IoTDevice for source={source}: {exc}")
        return

    if not device:
        print(f"[ALERT] No IoT device configured for source={source}")
        return

    min_threshold = device.min_threshold
    max_threshold = device.max_threshold
    if not device.is_active:
        print(f"[ALERT] Threshold check skipped source={source}: device inactive")
        return
    if not device.alert_enabled:
        print(f"[ALERT] Threshold check skipped source={source}: alerts disabled")
        return
    if min_threshold is None or max_threshold is None:
        print(f"[ALERT] Threshold check skipped source={source}: thresholds incomplete")
        return

    threshold = None
    status = None
    if value > float(max_threshold):
        threshold = float(max_threshold)
        status = "critical"
    elif value < float(min_threshold):
        threshold = float(min_threshold)
        status = "warning"

    alert_key = f"{source}:{metric_type}"

    if threshold is None or status is None:
        _last_alert_notification_ts.pop(alert_key, None)
        print(f"[ALERT] Threshold not matched source={source} metric_type={metric_type}")
        return

    now_ts = time.time()
    last_ts = _last_alert_notification_ts.get(alert_key, 0)
    if now_ts - last_ts < ALERT_NOTIFY_COOLDOWN_SECONDS:
        print(f"[ALERT] Threshold matched but cooldown active source={source} metric_type={metric_type}")
        return

    print("[ALERT] Threshold matched")
    threshold_text = f"> {threshold}" if status == "critical" else f"< {threshold}"
    message = (
        "IoT Alert\n"
        f"Device: {device.name or source}\n"
        f"Metric: {metric_type}\n"
        f"Current value: {value}\n"
        f"Threshold: {threshold_text}\n"
        "Source: metrics/live ESP32"
    )
    try:
        alert = create_alert(
            db,
            AlertCreate(
                metric_type=metric_type,
                status=status,
                current_value=value,
                threshold=float(threshold),
                message=message,
                source=source,
                device_id=device.id,
                device_name=device.name or source,
                unit=device.unit,
                min_threshold=float(min_threshold),
                max_threshold=float(max_threshold),
                created_at=metric_ts,
            ),
        )
        print(f"[ALERT] Created alert id={alert.id} source={source} metric_type={metric_type}")
    except Exception as exc:
        print(f"[ALERT] Failed to create alert for {source}/{metric_type}: {exc}")
        return

    _last_alert_notification_ts[alert_key] = now_ts
    try:
        print("[ALERT] Sending Telegram")
        print("[ALERT] Sending Gmail")
        loop = asyncio.get_running_loop()
        loop.create_task(dispatch_alert_notifications(alert.id))
    except RuntimeError:
        # Called from non-async thread (e.g., MQTT callback thread).
        # Run notification dispatch directly so alerts are still delivered.
        try:
            asyncio.run(dispatch_alert_notifications(alert.id))
        except Exception as dispatch_exc:
            print(f"[NOTIFY] Dispatch failed for alert_id={alert.id}: {dispatch_exc}")


def save_iot_metric_to_db(
    metric_type: str,
    source: str,
    location: str | None,
    timestamp: str | None,
    value: float,
    unit: str,
    save_flag: bool,
):
    source = str(source or "").strip()
    with open("backend_filtering.log", "a", encoding="utf-8") as f:
        f.write(f"{metric_type}={value} | saved={save_flag}\n")

    db = None
    try:
        db = SessionLocal()
        metric_ts = _parse_metric_timestamp(timestamp)
        _check_and_trigger_alert(db, metric_type, source, value, metric_ts)

        if not save_flag:
            return

        metric = MetricCreate(
            event_ts=metric_ts,
            sensor_id=source,
            location=location,
            metric_type=metric_type,
            metric_value=value,
            unit=unit,
        )
        create_metrics_bulk(db, [metric])
    except Exception as e:
        print(f"DB save error: {e}")
    finally:
        if db:
            db.close()


@router.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    """
    WebSocket endpoint:
    - Authenticated viewers connect with ?token=<JWT> and receive filtered realtime stream.
    - Publishers can connect without token and push metrics payloads.
    """
    try:
        ws_token = websocket.query_params.get("token")
        principal = _decode_ws_token(ws_token)
        metadata = principal or {"connection_mode": "publisher_only"}

        await manager.connect(client_id, websocket, metadata=metadata)

        while True:
            data = await websocket.receive_text()
            metrics_dict = json.loads(data)

            if "temperature" in metrics_dict or "humidity" in metrics_dict:
                try:
                    normalized_source = _normalize_source(metrics_dict)
                    if not normalized_source:
                        raise ValueError("sensor_id/source is required")
                    db = SessionLocal()
                    try:
                        row = create_sensor_reading(
                            db,
                            sensor_id=normalized_source,
                            event_ts=metrics_dict.get("timestamp"),
                            temperature=metrics_dict.get("temperature"),
                            humidity=metrics_dict.get("humidity"),
                            source_type=metrics_dict.get("source_type") or "physical_iot",
                            provider=metrics_dict.get("provider") or "websocket",
                            environment_type=metrics_dict.get("environment_type"),
                            location=metrics_dict.get("location"),
                            location_province=metrics_dict.get("location_province"),
                            latitude=metrics_dict.get("latitude"),
                            longitude=metrics_dict.get("longitude"),
                        )
                        db.commit()
                    finally:
                        db.close()

                    realtime_broadcast = {
                        "type": "sensor_reading",
                        "sensor_id": normalized_source,
                        "temperature": metrics_dict.get("temperature"),
                        "humidity": metrics_dict.get("humidity"),
                        "timestamp": metrics_dict.get("timestamp") or datetime.now().isoformat(),
                        "source_type": metrics_dict.get("source_type") or "physical_iot",
                    }
                    broadcast_payload = json.dumps(realtime_broadcast)
                    for target_client_id, info in list(manager.active_connections.items()):
                        if _can_receive_source(info, normalized_source):
                            await manager.send_to_client(target_client_id, broadcast_payload)

                    await websocket.send_text(
                        json.dumps(
                            {
                                "status": "ok",
                                "message": "Sensor reading received",
                                "server_time": datetime.now().isoformat(),
                            }
                        )
                    )
                except Exception as e:
                    await websocket.send_text(
                        json.dumps({"status": "error", "message": f"Sensor reading error: {str(e)}"})
                    )
            elif "metric_type" in metrics_dict:
                try:
                    normalized_source = _normalize_source(metrics_dict)
                    metrics_dict["source"] = normalized_source
                    metrics = IotMetricsData(**metrics_dict)
                    manager.save_metrics(client_id, metrics.model_dump())

                    realtime_broadcast = {
                        "type": "iot_metric",
                        "metric_type": metrics.metric_type,
                        "value": metrics.value,
                        "source": metrics.source,
                        "timestamp": metrics.timestamp or datetime.now().isoformat(),
                        "saved": metrics.saved,
                    }

                    broadcast_payload = json.dumps(realtime_broadcast)
                    for target_client_id, info in list(manager.active_connections.items()):
                        if _can_receive_source(info, metrics.source):
                            await manager.send_to_client(target_client_id, broadcast_payload)

                    save_iot_metric_to_db(
                        metrics.metric_type,
                        metrics.source,
                        metrics.location,
                        metrics.timestamp,
                        metrics.value,
                        metrics.unit,
                        metrics.saved,
                    )

                    await websocket.send_text(
                        json.dumps(
                            {
                                "status": "ok",
                                "message": f"IoT metric received: {metrics.metric_type}",
                                "server_time": datetime.now().isoformat(),
                            }
                        )
                    )
                except ValueError as e:
                    await websocket.send_text(
                        json.dumps({"status": "error", "message": f"IoT Validation error: {str(e)}"})
                    )
            else:
                try:
                    metrics = MetricsData(**metrics_dict)
                    manager.save_metrics(client_id, metrics.model_dump())
                    await websocket.send_text(
                        json.dumps(
                            {
                                "status": "ok",
                                "message": f"System metrics received from {client_id}",
                                "server_time": datetime.now().isoformat(),
                            }
                        )
                    )
                except ValueError as e:
                    await websocket.send_text(
                        json.dumps({"status": "error", "message": f"System Validation error: {str(e)}"})
                    )

    except json.JSONDecodeError as e:
        await websocket.send_text(json.dumps({"status": "error", "message": f"Invalid JSON format: {str(e)}"}))
    except WebSocketDisconnect:
        manager.disconnect(client_id)
    except Exception as e:
        print(f"WebSocket error with client {client_id}: {e}")
        manager.disconnect(client_id)


@router.get("/status", response_model=StatusResponse, tags=["Status"])
async def get_status():
    return manager.get_all_status()


@router.get("/status/{client_id}", tags=["Status"])
async def get_client_status(client_id: str):
    client_info = manager.get_client_info(client_id)
    if client_info is None:
        raise HTTPException(status_code=404, detail=f"Client {client_id} not found")
    return client_info


@router.get("/health", tags=["Health"])
async def health_check():
    return {
        "status": "healthy",
        "message": "WebSocket Metrics Server is running",
        "connected_clients": len(manager.active_connections),
        "timestamp": datetime.now().isoformat(),
    }
