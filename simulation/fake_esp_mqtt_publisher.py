"""Fake ESP publisher: emits realistic IoT telemetry to MQTT."""

import json
import math
import os
import random
import signal
import time
from datetime import datetime, timezone
from typing import Dict

import paho.mqtt.client as mqtt


MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC_TELEMETRY", "esp/telemetry")
PUBLISH_INTERVAL_SECONDS = float(os.getenv("FAKE_ESP_PUBLISH_SECONDS", "1.0"))

SENSOR_PROFILES: Dict[str, Dict[str, float]] = {
    "LR_TEMP_001": {
        "device_name": "Living Room Temperature",
        "metric_type": "temperature",
        "unit": "C",
        "min_value": 15.0,
        "max_value": 35.0,
        "mean_value": 24.0,
        "std_dev": 1.6,
        "rw_sigma": 0.35,
        "trend_amp": 2.1,
    },
    "BR_HUM_001": {
        "device_name": "Bedroom Humidity",
        "metric_type": "humidity",
        "unit": "%",
        "min_value": 30.0,
        "max_value": 85.0,
        "mean_value": 58.0,
        "std_dev": 4.5,
        "rw_sigma": 1.2,
        "trend_amp": 3.6,
    },
    "GD_SOIL_001": {
        "device_name": "Garden Soil Moisture",
        "metric_type": "soil_moisture",
        "unit": "%",
        "min_value": 20.0,
        "max_value": 90.0,
        "mean_value": 52.0,
        "std_dev": 6.0,
        "rw_sigma": 1.5,
        "trend_amp": 3.0,
    },
    "KT_LIGHT_001": {
        "device_name": "Kitchen Light Intensity",
        "metric_type": "light_intensity",
        "unit": "lux",
        "min_value": 50.0,
        "max_value": 950.0,
        "mean_value": 420.0,
        "std_dev": 45.0,
        "rw_sigma": 18.0,
        "trend_amp": 90.0,
    },
    "OUT_PRESS_001": {
        "device_name": "Outdoor Pressure",
        "metric_type": "pressure",
        "unit": "hPa",
        "min_value": 940.0,
        "max_value": 1060.0,
        "mean_value": 1002.0,
        "std_dev": 3.8,
        "rw_sigma": 0.9,
        "trend_amp": 2.0,
    },
}

RUNNING = True


def _stop(_sig, _frame):
    global RUNNING
    RUNNING = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def _hour_of_day(ts: datetime) -> float:
    return ts.hour + ts.minute / 60.0 + ts.second / 3600.0


def _daily_wave(ts: datetime) -> float:
    hour = _hour_of_day(ts)
    return math.sin((2.0 * math.pi * (hour - 8.0)) / 24.0)


def _next_value(previous: float, profile: Dict[str, float], ts: datetime) -> float:
    mean_value = profile["mean_value"]
    target = mean_value + profile["trend_amp"] * _daily_wave(ts)

    random_walk = previous + random.gauss(0.0, profile["rw_sigma"])
    mean_reversion = 0.25 * (target - random_walk)
    value = random_walk + mean_reversion

    if random.random() < 0.02:
        spike = random.choice([-1.0, 1.0]) * random.uniform(2.0, 4.0) * profile["rw_sigma"]
        value += spike

    bounded = max(profile["min_value"], min(profile["max_value"], value))
    return round(float(bounded), 3)


def main() -> None:
    print("[FAKE ESP] Starting MQTT telemetry publisher")
    print(f"[FAKE ESP] MQTT broker: {MQTT_HOST}:{MQTT_PORT}")
    print(f"[FAKE ESP] MQTT topic : {MQTT_TOPIC}")

    client = mqtt.Client(client_id=f"fake-esp-{int(time.time())}")
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    state = {sensor_id: cfg["mean_value"] for sensor_id, cfg in SENSOR_PROFILES.items()}
    battery = {sensor_id: random.uniform(80.0, 100.0) for sensor_id in SENSOR_PROFILES}
    seq = 0

    try:
        while RUNNING:
            now = datetime.now(timezone.utc)

            for sensor_id, cfg in SENSOR_PROFILES.items():
                seq += 1
                value = _next_value(state[sensor_id], cfg, now)
                state[sensor_id] = value

                battery[sensor_id] = max(5.0, battery[sensor_id] - random.uniform(0.0001, 0.0020))
                rssi = random.randint(-75, -48)

                payload = {
                    "device_id": sensor_id,
                    "device_name": cfg["device_name"],
                    "metric_type": cfg["metric_type"],
                    "value": value,
                    "unit": cfg["unit"],
                    "ts": now.isoformat(),
                    "seq": seq,
                    "rssi": rssi,
                    "battery": round(battery[sensor_id], 2),
                }

                client.publish(MQTT_TOPIC, json.dumps(payload), qos=1)

            print(f"[FAKE ESP] batch_ts={now.isoformat()} published={len(SENSOR_PROFILES)}")
            jitter = random.uniform(-0.2, 0.3)
            time.sleep(max(0.2, PUBLISH_INTERVAL_SECONDS + jitter))
    finally:
        client.loop_stop()
        client.disconnect()
        print("[FAKE ESP] Stopped")


if __name__ == "__main__":
    main()
