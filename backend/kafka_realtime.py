"""Kafka realtime bridge: consume telemetry + insight topics and broadcast over WebSocket."""

import asyncio
import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .websocket_manager import ConnectionManager


class KafkaRealtimeBridge:
    """Bridge Kafka topics to WebSocket broadcasts for realtime frontend updates."""

    def __init__(
        self,
        manager: ConnectionManager,
        loop: asyncio.AbstractEventLoop,
        bootstrap_servers: str,
        telemetry_topic: str,
        insight_topic: str,
        group_prefix: str = "metricspulse-backend",
        poll_timeout_ms: int = 1000,
        reconnect_delay_seconds: int = 3,
    ):
        self.manager = manager
        self.loop = loop
        self.bootstrap_servers = bootstrap_servers
        self.telemetry_topic = telemetry_topic
        self.insight_topic = insight_topic
        self.group_prefix = group_prefix
        self.poll_timeout_ms = poll_timeout_ms
        self.reconnect_delay_seconds = reconnect_delay_seconds

        self._running = False
        self._threads = []

    @classmethod
    def from_env(cls, manager: ConnectionManager, loop: asyncio.AbstractEventLoop) -> "KafkaRealtimeBridge":
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        telemetry_topic = os.getenv("KAFKA_TELEMETRY_TOPIC", "telemetry.raw")
        insight_topic = os.getenv("KAFKA_INSIGHT_TOPIC", "insight.events")
        group_prefix = os.getenv("KAFKA_GROUP_PREFIX", "metricspulse-backend")

        return cls(
            manager=manager,
            loop=loop,
            bootstrap_servers=bootstrap_servers,
            telemetry_topic=telemetry_topic,
            insight_topic=insight_topic,
            group_prefix=group_prefix,
        )

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self) -> None:
        if self._running:
            return

        self._running = True
        self._threads = [
            threading.Thread(
                target=self._consume_loop,
                args=(self.telemetry_topic, self._handle_telemetry_message),
                name="kafka-telemetry-consumer",
                daemon=True,
            ),
            threading.Thread(
                target=self._consume_loop,
                args=(self.insight_topic, self._handle_insight_message),
                name="kafka-insight-consumer",
                daemon=True,
            ),
        ]

        for thread in self._threads:
            thread.start()

        print(
            "[OK] [Kafka Bridge] Started | "
            f"brokers={self.bootstrap_servers} telemetry={self.telemetry_topic} insight={self.insight_topic}"
        )

    def stop(self, timeout_seconds: int = 5) -> None:
        self._running = False

        for thread in self._threads:
            if thread.is_alive():
                thread.join(timeout=timeout_seconds)

        self._threads = []
        print("[OK] [Kafka Bridge] Stopped")

    def _build_consumer(self, topic: str):
        from confluent_kafka import Consumer

        servers = [item.strip() for item in self.bootstrap_servers.split(",") if item.strip()]
        if not servers:
            raise ValueError("No Kafka bootstrap server configured")

        group_id = f"{self.group_prefix}-{topic.replace('.', '-') }"

        consumer = Consumer(
            {
                "bootstrap.servers": ",".join(servers),
                "group.id": group_id,
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
                "session.timeout.ms": 10000,
            }
        )
        consumer.subscribe([topic])
        return consumer

    def _consume_loop(self, topic: str, handler) -> None:
        from confluent_kafka import KafkaError

        while self._running:
            consumer = None
            try:
                consumer = self._build_consumer(topic)
                print(f"[OK] [Kafka Bridge] Consuming topic: {topic}")

                while self._running:
                    message = consumer.poll(timeout=self.poll_timeout_ms / 1000.0)
                    if message is None:
                        continue

                    if message.error():
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise RuntimeError(message.error())

                    raw_value = message.value()
                    if raw_value is None:
                        continue

                    try:
                        parsed = json.loads(raw_value.decode("utf-8"))
                    except Exception:
                        continue

                    normalized = handler(parsed)
                    if normalized is None:
                        continue

                    self._broadcast(normalized)

            except Exception as exc:
                print(f"[WARN] [Kafka Bridge] Topic '{topic}' error: {exc}")
                if self._running:
                    time.sleep(self.reconnect_delay_seconds)
            finally:
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        pass

    def _broadcast(self, payload: Dict[str, Any]) -> None:
        if self.loop.is_closed():
            return

        try:
            future = asyncio.run_coroutine_threadsafe(
                self.manager.broadcast(json.dumps(payload)),
                self.loop,
            )
            future.result(timeout=2)
        except Exception as exc:
            print(f"[WARN] [Kafka Bridge] Broadcast failed: {exc}")

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _handle_telemetry_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(message, dict):
            return None

        metric_type = str(message.get("metric_type") or message.get("device_type") or "").strip().lower()
        value = self._to_float(message.get("value"))

        if not metric_type or value is None:
            return None

        source = str(
            message.get("device_id")
            or message.get("source")
            or message.get("sensor_id")
            or "unknown"
        ).strip()

        timestamp = str(message.get("timestamp") or message.get("ts") or self._now_iso())

        return {
            "type": "iot_metric",
            "metric_type": metric_type,
            "value": value,
            "source": source,
            "timestamp": timestamp,
            "saved": True,
            "from_kafka": True,
            "topic": self.telemetry_topic,
        }

    def _handle_insight_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(message, dict):
            return None

        metric_type = str(message.get("metric_type") or message.get("device_type") or "").strip().lower()
        source = str(message.get("device_id") or message.get("source") or "").strip()
        severity = str(message.get("severity") or message.get("status") or "info").strip().lower()
        insight_type = str(message.get("insight_type") or "analytics").strip().lower()
        timestamp = str(message.get("timestamp") or message.get("ts") or self._now_iso())

        payload = {
            "type": "insight_event",
            "metric_type": metric_type,
            "source": source,
            "severity": severity,
            "insight_type": insight_type,
            "message": str(message.get("message") or ""),
            "timestamp": timestamp,
            "from_kafka": True,
            "topic": self.insight_topic,
        }

        value = self._to_float(message.get("value"))
        if value is not None:
            payload["value"] = value

        score = self._to_float(message.get("score"))
        if score is not None:
            payload["score"] = score

        threshold = self._to_float(message.get("threshold"))
        if threshold is not None:
            payload["threshold"] = threshold

        return payload
