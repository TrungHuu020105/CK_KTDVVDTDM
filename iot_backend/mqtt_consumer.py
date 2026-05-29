"""MQTT consumer -> IoT backend pipeline.

Subscribes to MQTT topic, normalizes payload, then forwards to IoT websocket endpoint.
"""

import json
import os
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from websocket import create_connection


MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "sensors/+/data")

IOT_WS_URL = os.getenv("IOT_WS_INGEST_URL", "ws://127.0.0.1:8100/api/ws/mqtt_ingestor")


def _create_mqtt_client(mqtt_module):
    callback_api_version = getattr(mqtt_module, "CallbackAPIVersion", None)
    if callback_api_version is not None:
        return mqtt_module.Client(callback_api_version.VERSION2)
    return mqtt_module.Client()


def _ensure_ws_conn():
    while True:
        try:
            return create_connection(IOT_WS_URL, timeout=8)
        except Exception as exc:
            print(f"[MQTT->IOT] WS connect failed: {exc}. Retrying in 2s...")
            time.sleep(2)


def _normalize_payload(raw_payload: bytes, topic: str) -> dict | None:
    try:
        payload = json.loads(raw_payload.decode("utf-8"))
    except Exception:
        return None

    metric_type = payload.get("metric_type")
    value = payload.get("value")
    source = payload.get("source") or payload.get("sensor_id")

    if metric_type is None or value is None or source is None:
        return None

    return {
        "metric_type": str(metric_type),
        "value": float(value),
        "source": str(source),
        "location": payload.get("location"),
        "unit": payload.get("unit", ""),
        "timestamp": payload.get("timestamp") or datetime.now(timezone.utc).isoformat(),
        "saved": bool(payload.get("saved", True)),
        "topic": topic,
    }


def main():
    ws = _ensure_ws_conn()

    def on_connect(client, userdata, flags, reason_code, properties=None):
        print(f"[MQTT->IOT] Connected to MQTT broker ({reason_code}), subscribing {MQTT_TOPIC}")
        client.subscribe(MQTT_TOPIC)

    def on_message(client, userdata, msg):
        nonlocal ws
        normalized = _normalize_payload(msg.payload, msg.topic)
        if not normalized:
            return
        try:
            ws.send(json.dumps(normalized))
        except Exception:
            try:
                ws.close()
            except Exception:
                pass
            ws = _ensure_ws_conn()
            ws.send(json.dumps(normalized))

    client = _create_mqtt_client(mqtt)
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    print(f"[MQTT->IOT] Connecting MQTT {MQTT_HOST}:{MQTT_PORT}")
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_forever()


if __name__ == "__main__":
    main()
