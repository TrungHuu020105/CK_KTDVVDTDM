"""MQTT subscriber for live IoT metrics in the main FastAPI backend."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from app.config import MQTT_CLIENT_ID, MQTT_HOST, MQTT_PASSWORD, MQTT_PORT, MQTT_SENSOR_TOPIC, MQTT_USERNAME
from app.database import SessionLocal
from app.services.iot_alert_service import ingest_iot_metric, normalize_metric_source


client = None
connected = False


def _create_mqtt_client(mqtt_module, client_id=None):
    callback_api_version = getattr(mqtt_module, "CallbackAPIVersion", None)
    if callback_api_version is not None:
        return mqtt_module.Client(callback_api_version.VERSION2, client_id=client_id)
    if client_id is not None:
        return mqtt_module.Client(client_id=client_id)
    return mqtt_module.Client()


def _reason_code_is_success(reason_code) -> bool:
    if hasattr(reason_code, "is_failure"):
        return not reason_code.is_failure
    return int(reason_code) == 0


def _sensor_id_from_topic(topic: str) -> str:
    parts = topic.split("/")
    if len(parts) >= 3 and parts[0] == "sensors":
        return parts[1]
    return "esp32_devkit_v1"


def _parse_payload(raw_payload: bytes, topic: str) -> list[dict]:
    try:
        payload = json.loads(raw_payload.decode("utf-8", errors="ignore"))
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []

    fallback_source = _sensor_id_from_topic(topic)
    source = normalize_metric_source(payload, fallback=fallback_source)
    if not source:
        return []

    timestamp = payload.get("timestamp") or datetime.now(timezone.utc).isoformat()
    location = payload.get("location")

    if payload.get("metric_type") is not None and payload.get("value") is not None:
        return [
            {
                "metric_type": str(payload.get("metric_type")),
                "value": float(payload.get("value")),
                "source": source,
                "location": location,
                "unit": str(payload.get("unit") or ""),
                "timestamp": timestamp,
                "saved": bool(payload.get("saved", True)),
            }
        ]

    rows = []
    if payload.get("temperature") is not None or payload.get("temp") is not None:
        rows.append(
            {
                "metric_type": "temperature",
                "value": float(payload.get("temperature") if payload.get("temperature") is not None else payload.get("temp")),
                "source": source,
                "location": location,
                "unit": "degC",
                "timestamp": timestamp,
                "saved": True,
            }
        )
    if payload.get("humidity") is not None or payload.get("hum") is not None:
        rows.append(
            {
                "metric_type": "humidity",
                "value": float(payload.get("humidity") if payload.get("humidity") is not None else payload.get("hum")),
                "source": source,
                "location": location,
                "unit": "%",
                "timestamp": timestamp,
                "saved": True,
            }
        )
    return rows


def start_mqtt_ingest():
    global client, connected
    if client is not None:
        return client

    import paho.mqtt.client as mqtt

    client = _create_mqtt_client(mqtt, client_id=MQTT_CLIENT_ID)
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD or None)

    def on_connect(mqtt_client, _userdata, _flags, reason_code=0, _properties=None):
        global connected
        if _reason_code_is_success(reason_code):
            connected = True
            mqtt_client.subscribe(MQTT_SENSOR_TOPIC)
            print(f"[MQTT] Connected to {MQTT_HOST}:{MQTT_PORT}, subscribed to {MQTT_SENSOR_TOPIC}")
        else:
            connected = False
            print(f"[MQTT] Connection failed: {reason_code}")

    def on_disconnect(_mqtt_client, _userdata, *args):
        global connected
        connected = False
        reason_code = args[1] if len(args) >= 2 else (args[0] if args else 0)
        print(f"[MQTT] Disconnected: {reason_code}")

    def on_message(_mqtt_client, _userdata, message):
        rows = _parse_payload(message.payload, message.topic)
        if not rows:
            print(f"[MQTT] Payload skipped topic={message.topic}")
            return
        db = SessionLocal()
        try:
            for row in rows:
                ingest_iot_metric(
                    db,
                    metric_type=row["metric_type"],
                    source=row["source"],
                    value=row["value"],
                    location=row.get("location"),
                    timestamp=row.get("timestamp"),
                    unit=row.get("unit") or "",
                    save_flag=bool(row.get("saved", True)),
                )
        except Exception as exc:
            db.rollback()
            print(f"[MQTT] Failed to ingest metric topic={message.topic}: {exc}")
        finally:
            db.close()

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    return client


def stop_mqtt_ingest():
    global client, connected
    if client is None:
        return
    client.loop_stop()
    client.disconnect()
    client = None
    connected = False
    print("[MQTT] Stopped")
