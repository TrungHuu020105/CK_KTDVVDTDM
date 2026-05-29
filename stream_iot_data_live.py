"""
Live IoT Data MQTT Publisher - publish sensor data continuously for integration testing.

Usage:
    python stream_iot_data_live.py
    python stream_iot_data_live.py --broker 127.0.0.1 --port 1883
    python stream_iot_data_live.py --interval 5
    python stream_iot_data_live.py --topic-template sensors/{source}/data
    python stream_iot_data_live.py --topic sensors/sensor-01/data
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("paho-mqtt not installed. Install with: pip install paho-mqtt")
    sys.exit(1)

from generate_iot_data import IoTDataGenerator

DEFAULT_TIMEZONE = "Asia/Ho_Chi_Minh"
# `sensor_1` and `sensor_2` each map to exactly 2 fake metrics in
# generate_iot_data.py: temperature and humidity.
DEFAULT_FAKE_SOURCES = "sensor_1,sensor_2"


def _create_mqtt_client():
    callback_api_version = getattr(mqtt, "CallbackAPIVersion", None)
    if callback_api_version is not None:
        return mqtt.Client(callback_api_version.VERSION2)
    return mqtt.Client()


class LiveIoTMqttPublisher:
    """Publish live sensor data to MQTT continuously."""

    def __init__(
        self,
        broker: str = "127.0.0.1",
        port: int = 1883,
        interval: int = 5,
        topic_template: str = "sensors/{source}/data",
        fixed_topic: str | None = None,
        username: str = "",
        password: str = "",
        forced_sources: list[str] | None = None,
    ):
        self.broker = broker
        self.port = port
        self.interval = interval
        self.topic_template = topic_template
        self.fixed_topic = fixed_topic
        self.username = username
        self.password = password
        self.forced_sources = forced_sources or []

        self.generator = IoTDataGenerator(forced_sources=self.forced_sources)
        self.batch_count = 0
        self.total_records = 0
        self.client = _create_mqtt_client()

        if self.username:
            self.client.username_pw_set(self.username, self.password)

    def connect(self):
        print(f"Connecting MQTT broker {self.broker}:{self.port} ...")
        self.client.connect(self.broker, self.port, 60)
        self.client.loop_start()
        print("Connected MQTT broker. Ready to publish.\n")

    def disconnect(self):
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass

    def _resolve_topic(self, source: str) -> str:
        if self.fixed_topic:
            return self.fixed_topic
        return self.topic_template.format(source=source)

    def publish_batch(self):
        self.batch_count += 1
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        metrics_to_send = self.generator.run_once(save_to_db=False)
        if not metrics_to_send:
            return

        metrics = metrics_to_send.get("metrics", [])
        self.total_records += len(metrics)

        db_worthy = len([m for m in metrics if m.get("saved", True)])
        realtime_only = len(metrics) - db_worthy

        for metric in metrics:
            payload = {
                "timestamp": metric.get("timestamp", datetime.now().isoformat()),
                "metric_type": metric.get("metric_type"),
                "value": metric.get("value"),
                "source": metric.get("source"),
                "unit": metric.get("unit", ""),
                "saved": metric.get("saved", False),
            }

            topic = self._resolve_topic(str(payload["source"]))
            result = self.client.publish(topic, json.dumps(payload), qos=0, retain=False)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                print(f"Publish failed topic={topic} rc={result.rc}")

        print(
            f"[{timestamp}] Batch #{self.batch_count} | Published: {len(metrics)} | "
            f"DB-Worthy: {db_worthy} | Realtime-Only: {realtime_only} | TZ: {DEFAULT_TIMEZONE}"
        )

        for metric in metrics:
            topic = self._resolve_topic(str(metric.get("source")))
            status = "MQTT"
            reason = metric.get("reason", "published")
            print(
                f"  {status} {metric['metric_type']:20} = {metric['value']:8.2f} {metric['unit']:5} | "
                f"topic={topic} | {reason}"
            )
        print()

    def run_continuous(self):
        print("=" * 100)
        print("LIVE IoT DATA MQTT PUBLISHER")
        print("=" * 100)
        print(f"Broker: {self.broker}:{self.port}")
        print(f"Interval: {self.interval}s per batch")
        print(f"Topic template: {self.topic_template}")
        print(f"Fake sources: {', '.join(self.forced_sources) if self.forced_sources else '(from local DB)'}")
        if self.fixed_topic:
            print(f"Fixed topic override: {self.fixed_topic}")
        print("Press Ctrl+C to stop\n")

        self.connect()

        try:
            while True:
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


def main():
    parser = argparse.ArgumentParser(description="Publish live IoT sensor data to MQTT continuously")
    parser.add_argument("--broker", type=str, default="127.0.0.1", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument(
        "--interval",
        type=int,
        default=int(os.getenv("FAKE_SAVE_INTERVAL_SECONDS", "2")),
        help="Interval seconds between batches",
    )
    parser.add_argument(
        "--topic-template",
        type=str,
        default="sensors/{source}/data",
        help="Topic template. Use {source} placeholder",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="",
        help="Fixed topic override for all metrics (e.g., sensors/sensor-01/data)",
    )
    parser.add_argument("--username", type=str, default="", help="MQTT username")
    parser.add_argument("--password", type=str, default="", help="MQTT password")
    parser.add_argument(
        "--sources",
        type=str,
        default=DEFAULT_FAKE_SOURCES,
        help=(
            "Comma-separated fake sources to emit. "
            f"Default: {DEFAULT_FAKE_SOURCES}. Use empty string to fall back to local DB active devices."
        ),
    )

    args = parser.parse_args()
    forced_sources = [item.strip().lower() for item in str(args.sources or "").split(",") if item.strip()]

    publisher = LiveIoTMqttPublisher(
        broker=args.broker,
        port=args.port,
        interval=args.interval,
        topic_template=args.topic_template,
        fixed_topic=args.topic or None,
        username=args.username,
        password=args.password,
        forced_sources=forced_sources,
    )
    publisher.run_continuous()


if __name__ == "__main__":
    main()
