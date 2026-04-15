"""Bridge MQTT telemetry messages into Kafka telemetry topic."""

import importlib
import json
import os
import signal
import threading
import time
from datetime import datetime, timezone


MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC_TELEMETRY", "esp/telemetry")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TELEMETRY_TOPIC = os.getenv("KAFKA_TELEMETRY_TOPIC", "telemetry.raw")

RUNNING = threading.Event()
RUNNING.set()


def _stop(_sig, _frame):
    RUNNING.clear()


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def _load_runtime_dependencies():
    kafka_module = importlib.import_module("confluent_kafka")
    mqtt_module = importlib.import_module("paho.mqtt.client")
    return kafka_module.Producer, mqtt_module


def main() -> None:
    KafkaProducer, mqtt = _load_runtime_dependencies()

    print("[BRIDGE] Starting MQTT -> Kafka bridge")
    print(f"[BRIDGE] MQTT : {MQTT_HOST}:{MQTT_PORT} topic={MQTT_TOPIC}")
    print(f"[BRIDGE] Kafka: {KAFKA_BOOTSTRAP_SERVERS} topic={KAFKA_TELEMETRY_TOPIC}")

    producer = KafkaProducer(
        {
            "bootstrap.servers": ",".join([x.strip() for x in KAFKA_BOOTSTRAP_SERVERS.split(",") if x.strip()]),
            "acks": "all",
            "linger.ms": 10,
        }
    )

    counters = {"published": 0, "errors": 0}

    def on_connect(client, _userdata, _flags, rc):
        if rc == 0:
            print("[BRIDGE] MQTT connected")
            client.subscribe(MQTT_TOPIC, qos=1)
            print(f"[BRIDGE] Subscribed MQTT topic: {MQTT_TOPIC}")
        else:
            print(f"[BRIDGE] MQTT connect failed rc={rc}")

    def on_message(_client, _userdata, msg):
        try:
            raw = msg.payload.decode("utf-8")
            payload = json.loads(raw)
            payload["ingested_at"] = datetime.now(timezone.utc).isoformat()
            payload["mqtt_topic"] = msg.topic

            key = str(payload.get("device_id") or payload.get("source") or "unknown").encode("utf-8")
            producer.produce(
                KAFKA_TELEMETRY_TOPIC,
                key=key,
                value=json.dumps(payload).encode("utf-8"),
            )
            producer.poll(0)
            counters["published"] += 1

            if counters["published"] % 25 == 0:
                producer.flush(5)
                print(
                    "[BRIDGE] forwarded="
                    f"{counters['published']} errors={counters['errors']}"
                )
        except Exception as exc:
            counters["errors"] += 1
            print(f"[BRIDGE] message error: {exc}")

    client = mqtt.Client(client_id=f"mqtt-kafka-bridge-{int(time.time())}")
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    try:
        while RUNNING.is_set():
            time.sleep(0.5)
    finally:
        client.loop_stop()
        client.disconnect()
        producer.flush(5)
        print("[BRIDGE] Stopped")


if __name__ == "__main__":
    main()
