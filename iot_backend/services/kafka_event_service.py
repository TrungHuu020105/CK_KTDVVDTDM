"""Kafka producer helper for optional IoT event streaming."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

from iot_backend.config import (
    KAFKA_ACKS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CLIENT_ID,
    KAFKA_ENABLED,
    KAFKA_METRIC_TOPIC,
    KAFKA_REQUEST_TIMEOUT_MS,
    KAFKA_SASL_MECHANISM,
    KAFKA_SASL_PASSWORD,
    KAFKA_SASL_USERNAME,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SENSOR_TOPIC,
)

try:
    from kafka import KafkaProducer
except Exception:
    KafkaProducer = None


_producer = None
_warned_unavailable = False


def _json_serializer(data: Any) -> bytes:
    return json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")


def _is_enabled() -> bool:
    return bool(KAFKA_ENABLED)


def _build_common_producer_options() -> dict[str, Any]:
    options: dict[str, Any] = {
        "bootstrap_servers": [item.strip() for item in KAFKA_BOOTSTRAP_SERVERS.split(",") if item.strip()],
        "client_id": KAFKA_CLIENT_ID,
        "acks": KAFKA_ACKS,
        "request_timeout_ms": KAFKA_REQUEST_TIMEOUT_MS,
        "value_serializer": _json_serializer,
        "key_serializer": lambda key: str(key).encode("utf-8"),
    }

    security_protocol = (KAFKA_SECURITY_PROTOCOL or "PLAINTEXT").strip().upper()
    options["security_protocol"] = security_protocol
    if security_protocol.startswith("SASL"):
        options["sasl_mechanism"] = (KAFKA_SASL_MECHANISM or "PLAIN").strip().upper()
        options["sasl_plain_username"] = KAFKA_SASL_USERNAME
        options["sasl_plain_password"] = KAFKA_SASL_PASSWORD
    return options


def _get_producer():
    global _producer, _warned_unavailable

    if not _is_enabled():
        return None
    if KafkaProducer is None:
        if not _warned_unavailable:
            print("[KAFKA] kafka-python is not available. Install requirements to enable Kafka producer.")
            _warned_unavailable = True
        return None
    if _producer is not None:
        return _producer

    try:
        _producer = KafkaProducer(**_build_common_producer_options())
        print(f"[KAFKA] Producer ready. bootstrap={KAFKA_BOOTSTRAP_SERVERS}")
        return _producer
    except Exception as exc:
        print(f"[KAFKA] Producer init failed: {exc}")
        return None


def close_producer() -> None:
    global _producer
    if _producer is None:
        return
    try:
        _producer.flush(timeout=5)
        _producer.close(timeout=5)
        print("[KAFKA] Producer closed.")
    except Exception as exc:
        print(f"[KAFKA] Producer close failed: {exc}")
    finally:
        _producer = None


def _publish(topic: str, key: str, payload: dict[str, Any]) -> None:
    producer = _get_producer()
    if producer is None:
        return
    try:
        future = producer.send(topic, key=key, value=payload)
        metadata = future.get(timeout=10)
        print(
            f"[KAFKA] published topic={metadata.topic} partition={metadata.partition} "
            f"offset={metadata.offset} key={key}"
        )
    except Exception as exc:
        print(f"[KAFKA] publish failed topic={topic} key={key}: {exc}")


def publish_sensor_reading_event(
    *,
    sensor_id: str,
    event_ts: str,
    temperature: float | None,
    humidity: float | None,
    source_type: str,
    provider: str,
    environment_type: str | None = None,
    location: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    ingest_source: str = "iot_backend",
) -> None:
    payload = {
        "event_id": str(uuid.uuid4()),
        "sensor_id": sensor_id,
        "event_ts": event_ts,
        "temperature": temperature,
        "humidity": humidity,
        "temperature_unit": "C",
        "humidity_unit": "%",
        "source_type": source_type,
        "provider": provider,
        "environment_type": environment_type,
        "location": location,
        "latitude": latitude,
        "longitude": longitude,
        "schema_version": "v1",
        "ingest_source": ingest_source,
        "ingested_at": datetime.now().isoformat(),
    }
    _publish(KAFKA_SENSOR_TOPIC, sensor_id, payload)


def publish_metric_event(
    *,
    sensor_id: str,
    metric_type: str,
    metric_value: float,
    unit: str,
    event_ts: str,
    ingest_source: str = "iot_backend",
) -> None:
    payload = {
        "event_id": str(uuid.uuid4()),
        "sensor_id": sensor_id,
        "metric_type": metric_type,
        "metric_value": metric_value,
        "unit": unit,
        "event_ts": event_ts,
        "schema_version": "v1",
        "ingest_source": ingest_source,
        "ingested_at": datetime.now().isoformat(),
    }
    _publish(KAFKA_METRIC_TOPIC, sensor_id, payload)
