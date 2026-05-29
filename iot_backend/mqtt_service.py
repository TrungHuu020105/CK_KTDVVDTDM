"""MQTT service for ingesting sensor data and sending ESP32 commands."""

from datetime import datetime, timezone
import json
from iot_backend.config import (
    MQTT_CLIENT_ID,
    MQTT_COMMAND_TOPIC_PREFIX,
    MQTT_DEVICE_STATE_TOPIC,
    MQTT_HOST,
    MQTT_PASSWORD,
    MQTT_PORT,
    MQTT_SENSOR_TOPIC,
    MQTT_WIFI_LIST_TOPIC,
    MQTT_USERNAME,
)


client = None
connected = False
last_reading_topic = ""
last_reading_payload = ""
last_command_topic = ""
last_command_payload = ""
last_wifi_topic = ""
last_wifi_payload = ""
last_threshold_topic = ""
last_threshold_payload = ""
last_state_topic = ""
last_state_payload = ""
wifi_scan_cache = {}
device_state_cache = {}


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


def parse_sensor_payload(raw_payload, fallback_sensor_id):
    text = raw_payload.strip()
    if not text:
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None

    sensor_id = str(payload.get("source") or payload.get("sensor_id") or fallback_sensor_id)
    location = payload.get("location")
    if location is not None:
        location = str(location)

    temperature = payload.get("temperature") or payload.get("temp") or payload.get("t")
    humidity = payload.get("humidity") or payload.get("hum") or payload.get("h")
    if temperature is not None and humidity is not None:
        return {
            "sensor_id": sensor_id,
            "location": location,
            "temperature": float(temperature),
            "humidity": float(humidity),
        }

    metric_type = payload.get("metric_type")
    value = payload.get("value")
    if metric_type is not None and value is not None:
        return {
            "sensor_id": sensor_id,
            "location": location,
            "metric_type": str(metric_type),
            "value": float(value),
            "unit": str(payload.get("unit") or ""),
            "saved": bool(payload.get("saved", True)),
            "timestamp": payload.get("timestamp"),
        }
    return None


def sensor_id_from_topic(topic):
    parts = topic.split("/")
    if len(parts) >= 3 and parts[0] == "sensors":
        return parts[1]
    return "esp32_devkit_v1"


def wifi_sensor_id_from_topic(topic):
    parts = topic.split("/")
    if len(parts) >= 4 and parts[0] == "ptdl" and parts[1] == "devices" and parts[3] == "wifi-list":
        return parts[2]
    return None


def state_sensor_id_from_topic(topic):
    parts = topic.split("/")
    if len(parts) >= 4 and parts[0] == "ptdl" and parts[1] == "devices" and parts[3] == "state":
        return parts[2]
    return None


def command_topic(sensor_id):
    return f"{MQTT_COMMAND_TOPIC_PREFIX}/{sensor_id}/commands"


def command_payload(response):
    commands = response["commands"]
    return json.dumps(
        {"commands": commands, "serial": commands["fan"] + commands["fog"] + commands["lamp"], "state": response["state"]},
        ensure_ascii=False,
        separators=(",", ":"),
    )


def publish_commands(sensor_id, response):
    if client is None:
        print("[MQTT] Command not sent: client not connected")
        return False
    global last_command_topic, last_command_payload
    topic = command_topic(sensor_id)
    payload = command_payload(response)
    last_command_topic = topic
    last_command_payload = payload
    result = client.publish(topic, payload, qos=1)
    print(f"[MQTT] Command sent to {topic}: {payload} (rc={result.rc})")
    return result.rc == 0


def publish_manual_command(sensor_id, *, fan=None, fog=None, lamp=None, auto=None):
    if client is None:
        print("[MQTT] Manual command not sent: client not connected")
        return False

    commands = {}
    if fan is not None:
        commands["fan"] = "1" if bool(fan) else "2"
    if fog is not None:
        commands["fog"] = "3" if bool(fog) else "4"
    if lamp is not None:
        commands["lamp"] = "5" if bool(lamp) else "6"

    state = {}
    if fan is not None:
        state["fan"] = bool(fan)
    if fog is not None:
        state["fog"] = bool(fog)
    if lamp is not None:
        state["lamp"] = bool(lamp)
    if auto is not None:
        state["auto"] = bool(auto)

    payload_dict = {}
    if commands:
        payload_dict["commands"] = commands
    if state:
        payload_dict["state"] = state

    if not payload_dict:
        print("[MQTT] Manual command skipped: empty payload")
        return False

    global last_command_topic, last_command_payload
    topic = command_topic(sensor_id)
    payload = json.dumps(payload_dict, ensure_ascii=False, separators=(",", ":"))
    last_command_topic = topic
    last_command_payload = payload
    result = client.publish(topic, payload, qos=1)
    print(f"[MQTT] Manual command sent to {topic}: {payload} (rc={result.rc})")
    return result.rc == 0


def publish_wifi_config(sensor_id, ssid, password):
    if client is None:
        print("[MQTT] WiFi config not sent: client not connected")
        return False
    global last_wifi_topic, last_wifi_payload
    topic = command_topic(sensor_id)
    payload = json.dumps({"wifi": {"ssid": ssid, "password": password}}, ensure_ascii=False, separators=(",", ":"))
    last_wifi_topic = topic
    last_wifi_payload = payload
    result = client.publish(topic, payload, qos=1)
    print(f"[MQTT] WiFi config sent to {topic}: {payload} (rc={result.rc})")
    return result.rc == 0


def publish_wifi_scan_request(sensor_id):
    if client is None:
        print("[MQTT] WiFi scan request not sent: client not connected")
        return False
    topic = command_topic(sensor_id)
    payload = json.dumps({"scan_wifi": True}, ensure_ascii=False, separators=(",", ":"))
    result = client.publish(topic, payload, qos=1)
    print(f"[WIFI SCAN] publish scan request to MQTT topic {topic}: {payload} (rc={result.rc})")
    return result.rc == 0


def publish_threshold_config(
    sensor_id,
    *,
    metric_type,
    min_threshold,
    max_threshold,
    alert_enabled,
    unit="",
    device_id=None,
):
    if client is None:
        print("[MQTT] Threshold config not sent: client not connected")
        return False

    global last_threshold_topic, last_threshold_payload
    topic = command_topic(sensor_id)
    payload = json.dumps(
        {
            "threshold_config": {
                "device_id": device_id,
                "metric_type": metric_type,
                "min_threshold": min_threshold,
                "max_threshold": max_threshold,
                "alert_enabled": bool(alert_enabled),
                "unit": unit or "",
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    last_threshold_topic = topic
    last_threshold_payload = payload
    result = client.publish(topic, payload, qos=1)
    print(f"[MQTT] Threshold config sent to {topic}: {payload} (rc={result.rc})")
    return result.rc == 0


def get_wifi_scan_result(sensor_id):
    return wifi_scan_cache.get(sensor_id)


def get_device_state(sensor_id):
    return device_state_cache.get(sensor_id)


def get_wifi_status(sensor_id):
    cached_state = get_device_state(sensor_id) or {}
    payload = cached_state.get("payload") if isinstance(cached_state, dict) else {}
    wifi = payload.get("wifi") if isinstance(payload, dict) else {}
    if not isinstance(wifi, dict):
        wifi = {}

    return {
        "status": "ok" if payload else "empty",
        "sensor_id": sensor_id,
        "received_at": cached_state.get("received_at"),
        "timestamp": payload.get("timestamp") if isinstance(payload, dict) else None,
        "connected": bool(wifi.get("connected", False)),
        "ssid": wifi.get("ssid") or "",
        "ip": wifi.get("ip") or "",
        "rssi": wifi.get("rssi"),
        "configured_ssid": wifi.get("configured_ssid") or "",
        "state": payload.get("state") if isinstance(payload, dict) else None,
    }


def start_mqtt(on_reading, on_device_state=None):
    global client, connected
    import paho.mqtt.client as mqtt

    if client is not None:
        return client

    client = _create_mqtt_client(mqtt, client_id=MQTT_CLIENT_ID)
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD or None)

    def handle_connect(mqtt_client, _userdata, _flags, reason_code=0, _properties=None):
        global connected
        if _reason_code_is_success(reason_code):
            connected = True
            mqtt_client.subscribe(MQTT_SENSOR_TOPIC)
            mqtt_client.subscribe(MQTT_WIFI_LIST_TOPIC)
            mqtt_client.subscribe(MQTT_DEVICE_STATE_TOPIC)
            print(
                f"[MQTT] Connected to {MQTT_HOST}:{MQTT_PORT}, subscribed to "
                f"{MQTT_SENSOR_TOPIC}, {MQTT_WIFI_LIST_TOPIC}, and {MQTT_DEVICE_STATE_TOPIC}"
            )
        else:
            connected = False
            print(f"[MQTT] Connection failed: {reason_code}")

    def handle_disconnect(_mqtt_client, _userdata, *args):
        global connected
        connected = False
        reason_code = args[1] if len(args) >= 2 else (args[0] if args else 0)
        print(f"[MQTT] Disconnected: {reason_code}")

    def handle_message(_mqtt_client, _userdata, message):
        global last_reading_topic, last_reading_payload, last_state_topic, last_state_payload
        raw_payload = message.payload.decode("utf-8", errors="ignore")
        wifi_sensor_id = wifi_sensor_id_from_topic(message.topic)
        device_state_sensor_id = state_sensor_id_from_topic(message.topic)

        if wifi_sensor_id:
            try:
                parsed = json.loads(raw_payload)
            except json.JSONDecodeError:
                print(f"[MQTT] WiFi list payload skipped (invalid JSON): {raw_payload}")
                return
            if not isinstance(parsed, dict):
                print(f"[MQTT] WiFi list payload skipped (not object): {raw_payload}")
                return
            wifi_scan_cache[wifi_sensor_id] = {
                "sensor_id": wifi_sensor_id,
                "received_at": datetime.now(timezone.utc).isoformat(),
                "payload": parsed,
            }
            networks = parsed.get("networks") if isinstance(parsed, dict) else []
            count = len(networks) if isinstance(networks, list) else 0
            print(f"[WIFI SCAN] cached networks count={count} for {wifi_sensor_id}")
            return

        if device_state_sensor_id:
            try:
                parsed = json.loads(raw_payload)
            except json.JSONDecodeError:
                print(f"[MQTT] Device state payload skipped (invalid JSON): {raw_payload}")
                return
            if not isinstance(parsed, dict):
                print(f"[MQTT] Device state payload skipped (not object): {raw_payload}")
                return

            last_state_topic = message.topic
            last_state_payload = raw_payload
            device_state_cache[device_state_sensor_id] = {
                "sensor_id": device_state_sensor_id,
                "received_at": datetime.now(timezone.utc).isoformat(),
                "payload": parsed,
            }
            if on_device_state:
                try:
                    on_device_state(device_state_sensor_id, parsed)
                except Exception as exc:
                    print(f"[MQTT] Device state callback failed: {exc}")
            return

        sensor_id = sensor_id_from_topic(message.topic)
        last_reading_topic = message.topic
        last_reading_payload = raw_payload
        print(f"[MQTT] Received from {message.topic}: {raw_payload}")
        reading = parse_sensor_payload(raw_payload, sensor_id)
        if reading is None:
            print(f"[MQTT] Payload skipped (invalid format): {raw_payload}")
            return
        response = on_reading(reading)
        if response:
            publish_commands(sensor_id, response)

    client.on_connect = handle_connect
    client.on_disconnect = handle_disconnect
    client.on_message = handle_message
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    return client


def stop_mqtt():
    global client, connected
    if client is None:
        return
    client.loop_stop()
    client.disconnect()
    client = None
    connected = False
    print("[MQTT] Stopped")


def status():
    return {
        "connected": connected,
        "last_reading_topic": last_reading_topic,
        "last_reading_payload": last_reading_payload,
        "last_command_topic": last_command_topic,
        "last_command_payload": last_command_payload,
        "last_wifi_topic": last_wifi_topic,
        "last_wifi_payload": last_wifi_payload,
        "last_threshold_topic": last_threshold_topic,
        "last_threshold_payload": last_threshold_payload,
        "last_state_topic": last_state_topic,
        "last_state_payload": last_state_payload,
        "wifi_scan_cache_count": len(wifi_scan_cache),
        "device_state_cache_count": len(device_state_cache),
    }
