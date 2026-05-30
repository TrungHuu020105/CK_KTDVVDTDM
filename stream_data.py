"""
Live IoT Data MQTT Publisher for sensor-level realtime readings.

This file follows the same idea as the old streaming flow:
- generate data every interval for smooth realtime MQTT updates
- mark each metric with saved=true/false using threshold + time filtering
- keep the payload schema compatible with the current backend MQTT ingest
- use Vietnam time for timestamps and logs

Usage:
    python stream_data.py
    python stream_data.py --broker 20.214.247.102 --port 1883
    python stream_data.py --interval 3
    python stream_data.py --topic-template sensors/{source}/data
    python stream_data.py --topic sensors/sensor_4/data
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("paho-mqtt not installed. Install with: pip install paho-mqtt")
    sys.exit(1)


DEFAULT_TIMEZONE = "Asia/Ho_Chi_Minh"
VN_TZ = timezone(timedelta(hours=7))
DEFAULT_TOPIC_TEMPLATE = "sensors/{source}/data"
ENV_FILE_CANDIDATES = (
    ".env",
    "app/.env",
    "iot_backend/.env",
)


def _load_env_files() -> None:
    root = Path(__file__).resolve().parent
    for relative_path in ENV_FILE_CANDIDATES:
        env_path = root / relative_path
        if not env_path.exists():
            continue

        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


FAKE_ALWAYS_SAVE = _env_bool("FAKE_ALWAYS_SAVE", False)
FAKE_SAVE_INTERVAL_SECONDS = _env_int("FAKE_SAVE_INTERVAL_SECONDS", 30)
FAKE_TEMP_DELTA_THRESHOLD = _env_float("FAKE_TEMP_DELTA_THRESHOLD", 0.3)
FAKE_HUMIDITY_DELTA_THRESHOLD = _env_float("FAKE_HUMIDITY_DELTA_THRESHOLD", 1.5)
STREAM_SOURCE_1 = "sensor_4"
STREAM_SOURCE_2 = "sensor_5"


def _now_vn() -> datetime:
    return datetime.now(VN_TZ)


def _create_mqtt_client() -> mqtt.Client:
    callback_api_version = getattr(mqtt, "CallbackAPIVersion", None)
    if callback_api_version is not None:
        return mqtt.Client(callback_api_version.VERSION2)
    return mqtt.Client()


def _reason_code_is_success(reason_code) -> bool:
    if hasattr(reason_code, "is_failure"):
        return not reason_code.is_failure
    return int(reason_code) == 0


@dataclass
class SensorConfig:
    metric_type: str
    source: str
    min_value: float
    max_value: float
    step_size: float
    save_threshold: float
    max_save_interval: int
    unit: str
    trend_enabled: bool = False
    trend_amplitude: float = 0.0
    trend_peak_hour: float = 14.0


SENSORS = [
    SensorConfig(
        metric_type="temperature",
        source=STREAM_SOURCE_1,
        min_value=_env_float("SENSOR_6_TEMP_MIN", 28.0),
        max_value=_env_float("SENSOR_6_TEMP_MAX", 35.0),
        step_size=_env_float("SENSOR_6_TEMP_STEP", 0.45),
        save_threshold=FAKE_TEMP_DELTA_THRESHOLD,
        max_save_interval=FAKE_SAVE_INTERVAL_SECONDS,
        unit="degC",
        trend_enabled=True,
        trend_amplitude=0.15,
        trend_peak_hour=14.0,
    ),
    SensorConfig(
        metric_type="humidity",
        source=STREAM_SOURCE_1,
        min_value=_env_float("SENSOR_6_HUMIDITY_MIN", 56.0),
        max_value=_env_float("SENSOR_6_HUMIDITY_MAX", 82.0),
        step_size=_env_float("SENSOR_6_HUMIDITY_STEP", 1.1),
        save_threshold=FAKE_HUMIDITY_DELTA_THRESHOLD,
        max_save_interval=FAKE_SAVE_INTERVAL_SECONDS,
        unit="%",
        trend_enabled=True,
        trend_amplitude=0.35,
        trend_peak_hour=4.0,
    ),
    SensorConfig(
        metric_type="temperature",
        source=STREAM_SOURCE_2,
        min_value=_env_float("SENSOR_9_TEMP_MIN", 26.0),
        max_value=_env_float("SENSOR_9_TEMP_MAX", 33.0),
        step_size=_env_float("SENSOR_9_TEMP_STEP", 0.4),
        save_threshold=FAKE_TEMP_DELTA_THRESHOLD,
        max_save_interval=FAKE_SAVE_INTERVAL_SECONDS,
        unit="degC",
        trend_enabled=True,
        trend_amplitude=0.13,
        trend_peak_hour=13.5,
    ),
    SensorConfig(
        metric_type="humidity",
        source=STREAM_SOURCE_2,
        min_value=_env_float("SENSOR_9_HUMIDITY_MIN", 50.0),
        max_value=_env_float("SENSOR_9_HUMIDITY_MAX", 85.0),
        step_size=_env_float("SENSOR_9_HUMIDITY_STEP", 1.30),
        save_threshold=FAKE_HUMIDITY_DELTA_THRESHOLD,
        max_save_interval=FAKE_SAVE_INTERVAL_SECONDS,
        unit="%",
        trend_enabled=True,
        trend_amplitude=0.45,
        trend_peak_hour=4.0,
    ),
]


@dataclass
class SensorState:
    metric_type: str
    source: str
    last_generated_value: float
    last_saved_value: float
    last_saved_timestamp: datetime
    generated_count: int = 0
    saved_count: int = 0
    dropped_count: int = 0


class StateManager:
    def __init__(self) -> None:
        self.states: Dict[str, SensorState] = {}

    def _key(self, source: str, metric_type: str) -> str:
        return f"{source}:{metric_type}"

    def initialize(self, config: SensorConfig, initial_value: float) -> None:
        now = _now_vn()
        self.states[self._key(config.source, config.metric_type)] = SensorState(
            metric_type=config.metric_type,
            source=config.source,
            last_generated_value=initial_value,
            last_saved_value=initial_value,
            last_saved_timestamp=now,
        )

    def get_state(self, source: str, metric_type: str) -> Optional[SensorState]:
        return self.states.get(self._key(source, metric_type))

    def update_generated(self, source: str, metric_type: str, value: float) -> None:
        state = self.get_state(source, metric_type)
        if state:
            state.last_generated_value = value
            state.generated_count += 1

    def update_saved(self, source: str, metric_type: str, value: float) -> None:
        state = self.get_state(source, metric_type)
        if state:
            state.last_saved_value = value
            state.last_saved_timestamp = _now_vn()
            state.saved_count += 1

    def mark_dropped(self, source: str, metric_type: str) -> None:
        state = self.get_state(source, metric_type)
        if state:
            state.dropped_count += 1


def get_time_trend(config: SensorConfig) -> float:
    if not config.trend_enabled:
        return 0.0

    now = _now_vn()
    hour = now.hour + now.minute / 60.0
    phase = (hour - config.trend_peak_hour) / 24.0 * (2 * math.pi)
    return config.trend_amplitude * math.sin(phase)


def clamp(value: float, config: SensorConfig) -> float:
    return max(config.min_value, min(config.max_value, value))


def generate_value(config: SensorConfig, state: SensorState) -> float:
    random_change = random.uniform(-config.step_size, config.step_size)
    range_size = config.max_value - config.min_value
    current_pos = state.last_generated_value
    distance_to_max = config.max_value - current_pos
    distance_to_min = current_pos - config.min_value

    if distance_to_max < range_size * 0.20:
        random_change = random.uniform(-config.step_size * 2.0, -config.step_size * 0.8)
    elif distance_to_min < range_size * 0.20:
        random_change = random.uniform(config.step_size * 0.8, config.step_size * 2.0)

    if distance_to_max > range_size * 0.25 and distance_to_min > range_size * 0.25:
        trend = get_time_trend(config)
    else:
        trend = 0.0

    new_value = state.last_generated_value + random_change + trend
    new_value = clamp(new_value, config)
    return round(new_value, 2)


def should_save(config: SensorConfig, state: SensorState, new_value: float) -> Tuple[bool, str]:
    if FAKE_ALWAYS_SAVE:
        return True, "fake_always_save=true"

    save_threshold = config.save_threshold
    max_save_interval = config.max_save_interval

    value_change = abs(new_value - state.last_saved_value)
    if value_change >= save_threshold:
        return True, f"change={value_change:.2f}>={save_threshold}"

    now = _now_vn()
    time_since_save = (now - state.last_saved_timestamp).total_seconds()
    if time_since_save >= max_save_interval:
        return True, f"time={time_since_save:.0f}s>={max_save_interval}s"

    return False, f"filtered(delta={value_change:.2f}, t={time_since_save:.0f}s)"


class Sensor69DataGenerator:
    def __init__(self) -> None:
        self.state_manager = StateManager()
        self.initialized = False
        self.active_sensors = list(SENSORS)

    def initialize(self) -> None:
        self.active_sensors = list(SENSORS)
        self.state_manager.states = {}

        for config in self.active_sensors:
            initial_value = round(random.uniform(config.min_value, config.max_value), 2)
            self.state_manager.initialize(config, initial_value)

        self.initialized = True
        sources = sorted({cfg.source for cfg in self.active_sensors})
        print(f"[INFO] Initialized {len(self.active_sensors)} metric streams across sources: {', '.join(sources)}")

    def run_once(self) -> Dict[str, object]:
        if not self.initialized:
            self.initialize()

        now = _now_vn()
        timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")
        timestamp_iso = now.isoformat()

        metrics_to_publish: List[Dict[str, object]] = []
        sensor_readings: Dict[str, Dict[str, object]] = {}
        saved_count = 0

        for config in self.active_sensors:
            state = self.state_manager.get_state(config.source, config.metric_type)
            if not state:
                continue

            value = generate_value(config, state)
            self.state_manager.update_generated(config.source, config.metric_type, value)

            save_flag, reason = should_save(config, state, value)
            if save_flag:
                self.state_manager.update_saved(config.source, config.metric_type, value)
                saved_count += 1
            else:
                self.state_manager.mark_dropped(config.source, config.metric_type)

            metrics_to_publish.append(
                {
                    "metric_type": config.metric_type,
                    "source": config.source,
                    "value": round(value, 2),
                    "unit": config.unit,
                    "timestamp": timestamp_str,
                    "saved": save_flag,
                    "reason": reason,
                }
            )

            reading = sensor_readings.setdefault(
                config.source,
                {
                    "source": config.source,
                    "timestamp": timestamp_str,
                    "timestamp_iso": timestamp_iso,
                    "temperature": None,
                    "humidity": None,
                    "saved": False,
                    "reasons": [],
                },
            )
            reading[config.metric_type] = round(value, 2)
            reading["saved"] = bool(reading["saved"]) or bool(save_flag)
            reading["reasons"].append(f"{config.metric_type}:{reason}")

        ready_readings = []
        for source, row in sensor_readings.items():
            if row["temperature"] is None or row["humidity"] is None:
                # Backend sensor_reading ingest requires both fields.
                continue
            row["reason"] = " | ".join(row["reasons"])
            ready_readings.append(row)

        return {
            "metrics": metrics_to_publish,
            "readings": ready_readings,
            "timestamp": timestamp_str,
            "timestamp_iso": timestamp_iso,
            "generated": len(metrics_to_publish),
            "saved": saved_count,
            "dropped": len(metrics_to_publish) - saved_count,
        }


class LiveSensor69MqttPublisher:
    def __init__(
        self,
        broker: str = "127.0.0.1",
        port: int = 1883,
        interval: int = 5,
        topic_template: str = DEFAULT_TOPIC_TEMPLATE,
        fixed_topic: str | None = None,
        username: str = "",
        password: str = "",
    ) -> None:
        self.broker = broker
        self.port = port
        self.interval = interval
        self.topic_template = topic_template
        self.fixed_topic = fixed_topic
        self.username = username
        self.password = password

        self.generator = Sensor69DataGenerator()
        self.batch_count = 0
        self.total_records = 0
        self.connected = False
        self.client = _create_mqtt_client()
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

        if self.username:
            self.client.username_pw_set(self.username, self.password)

    def _on_connect(self, _mqtt_client, _userdata, _flags, reason_code=0, _properties=None) -> None:
        self.connected = _reason_code_is_success(reason_code)
        if self.connected:
            print(f"[MQTT] Connected to {self.broker}:{self.port}")
        else:
            print(f"[MQTT] Connection failed: {reason_code}")

    def _on_disconnect(self, _mqtt_client, _userdata, *args) -> None:
        self.connected = False
        reason_code = args[1] if len(args) >= 2 else (args[0] if args else 0)
        print(f"[MQTT] Disconnected: {reason_code}")

    def connect(self) -> None:
        print(f"[MQTT] Connecting to {self.broker}:{self.port} ...")
        try:
            self.client.connect(self.broker, self.port, 60)
        except Exception as exc:
            print(f"[MQTT] Initial connect failed: {type(exc).__name__}: {exc}")
            self.connected = False
            return
        self.client.loop_start()
        for _ in range(10):
            if self.connected:
                return
            time.sleep(0.2)

    def disconnect(self) -> None:
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass

    def _ensure_connected(self) -> bool:
        if self.connected:
            return True
        try:
            self.client.reconnect()
            return True
        except Exception:
            try:
                self.client.connect(self.broker, self.port, 60)
                return True
            except Exception as exc:
                print(f"[MQTT] Reconnect failed: {type(exc).__name__}: {exc}")
                return False

    def _resolve_topic(self, source: str) -> str:
        if self.fixed_topic:
            return self.fixed_topic
        return self.topic_template.format(source=source)

    def publish_batch(self) -> None:
        if not self._ensure_connected():
            return

        self.batch_count += 1
        metrics_to_send = self.generator.run_once()
        readings = metrics_to_send.get("readings", [])
        metrics = metrics_to_send.get("metrics", [])
        if not readings:
            print("[INFO] No metrics generated in this batch")
            return

        self.total_records += len(readings)
        timestamp = str(metrics_to_send.get("timestamp", _now_vn().strftime("%Y-%m-%d %H:%M:%S")))
        db_worthy = len([r for r in readings if r.get("saved", True)])
        realtime_only = len(readings) - db_worthy

        for reading in readings:
            payload = {
                "timestamp": reading.get("timestamp_iso") or reading.get("timestamp", timestamp),
                "source": reading.get("source"),
                "sensor_id": reading.get("source"),
                "temperature": reading.get("temperature"),
                "humidity": reading.get("humidity"),
                "source_type": "physical_iot",
                "provider": "stream_data_fake",
                "environment_type": "indoor",
                "saved": reading.get("saved", True),
            }

            topic = self._resolve_topic(str(payload["source"]))
            result = self.client.publish(topic, json.dumps(payload), qos=0, retain=False)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                print(f"[MQTT] Publish failed topic={topic} rc={result.rc}")

        print(
            f"[{timestamp}] Batch #{self.batch_count} | Published: {len(readings)} sensor_readings | "
            f"DB-Worthy: {db_worthy} | Realtime-Only: {realtime_only} | TZ: {DEFAULT_TIMEZONE}"
        )

        for reading in readings:
            topic = self._resolve_topic(str(reading.get("source")))
            reason = reading.get("reason", "published")
            print(
                f"  MQTT source={reading['source']} | temp={float(reading['temperature']):6.2f} C | "
                f"humidity={float(reading['humidity']):6.2f} % | topic={topic} | saved={reading['saved']} | {reason}"
            )
        print()

    def run_continuous(self) -> None:
        print("=" * 100)
        print("LIVE IoT DATA MQTT PUBLISHER - SENSOR READINGS")
        print("=" * 100)
        print(f"Broker: {self.broker}:{self.port}")
        print(f"Interval: {self.interval}s per batch")
        print(f"Topic template: {self.topic_template}")
        sources = sorted({cfg.source for cfg in self.generator.active_sensors or SENSORS})
        print(f"Fake sources: {', '.join(sources)} (each source emits temperature + humidity)")
        print(f"Timezone: {DEFAULT_TIMEZONE}")
        if self.fixed_topic:
            print(f"Fixed topic override: {self.fixed_topic}")
        print("Press Ctrl+C to stop\n")

        self.connect()

        try:
            while True:
                if not self.connected:
                    print("[MQTT] Not connected. Retrying ...")
                    self._ensure_connected()
                self.publish_batch()
                time.sleep(self.interval)
        except KeyboardInterrupt:
            print("\n" + "=" * 100)
            print("STOPPED BY USER")
            print("=" * 100)
            print(f"Total batches published: {self.batch_count}")
            print(f"Total metrics published: {self.total_records}")
            print(f"Approx runtime: {self.batch_count * self.interval} seconds")
            print("=" * 100)
        finally:
            self.disconnect()
            print("Disconnected MQTT\n")


def main() -> None:
    _load_env_files()

    parser = argparse.ArgumentParser(description="Publish live IoT sensor readings (temperature + humidity) to MQTT")
    parser.add_argument("--broker", type=str, default=os.getenv("MQTT_HOST", "127.0.0.1"), help="MQTT broker host")
    parser.add_argument("--port", type=int, default=_env_int("MQTT_PORT", 1883), help="MQTT broker port")
    parser.add_argument(
        "--interval",
        type=int,
        default=_env_int("FAKE_STREAM_INTERVAL_SECONDS", 5),
        help="Interval seconds between batches",
    )
    parser.add_argument(
        "--topic-template",
        type=str,
        default=os.getenv("FAKE_MQTT_TOPIC_TEMPLATE", DEFAULT_TOPIC_TEMPLATE),
        help="Topic template. Use {source} placeholder",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="",
        help="Fixed topic override for all metrics (e.g., sensors/sensor_4/data)",
    )
    parser.add_argument("--username", type=str, default=os.getenv("MQTT_USERNAME", ""), help="MQTT username")
    parser.add_argument("--password", type=str, default=os.getenv("MQTT_PASSWORD", ""), help="MQTT password")
    parser.add_argument("--once", action="store_true", help="Publish one batch and exit")

    args = parser.parse_args()

    publisher = LiveSensor69MqttPublisher(
        broker=args.broker,
        port=args.port,
        interval=max(1, int(args.interval)),
        topic_template=args.topic_template,
        fixed_topic=args.topic or None,
        username=args.username,
        password=args.password,
    )

    if args.once:
        publisher.connect()
        try:
            publisher.publish_batch()
        finally:
            publisher.disconnect()
        return

    publisher.run_continuous()


if __name__ == "__main__":
    main()
