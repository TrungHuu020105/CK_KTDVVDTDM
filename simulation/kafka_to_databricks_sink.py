"""Consume Kafka telemetry and persist to real Databricks tables."""

import importlib
import os
import signal
import threading
import time
import uuid
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.databricks_client import get_databricks_client


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TELEMETRY_TOPIC = os.getenv("KAFKA_TELEMETRY_TOPIC", "telemetry.raw")
KAFKA_DATABRICKS_FLUSH_SECONDS = float(os.getenv("KAFKA_TO_DATABRICKS_FLUSH_SECONDS", "2"))
KAFKA_DATABRICKS_MAX_BATCH = int(os.getenv("KAFKA_TO_DATABRICKS_MAX_BATCH", "200"))

RUNNING = threading.Event()
RUNNING.set()


def _stop(_sig, _frame):
    RUNNING.clear()


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def _load_kafka_consumer():
    kafka_module = importlib.import_module("confluent_kafka")
    return kafka_module.Consumer, kafka_module.KafkaError


def _to_float(value) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _escape(value: str) -> str:
    return (value or "").replace("'", "''")


def _parse_ts(raw) -> datetime:
    if isinstance(raw, datetime):
        return raw.astimezone(timezone.utc)

    text = str(raw or "").strip()
    if not text:
        return datetime.now(timezone.utc)

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return datetime.now(timezone.utc)


def _normalize(payload: Dict) -> Optional[Dict]:
    if not isinstance(payload, dict):
        return None

    device_id = str(payload.get("device_id") or payload.get("source") or "").strip()
    metric_type = str(payload.get("metric_type") or payload.get("device_type") or "").strip().lower()
    value = _to_float(payload.get("value"))

    if not device_id or not metric_type or value is None:
        return None

    ts = _parse_ts(payload.get("ts") or payload.get("timestamp"))

    return {
        "timestamp": ts,
        "device_id": device_id,
        "device_name": str(payload.get("device_name") or device_id).strip(),
        "device_type": metric_type,
        "location": str(payload.get("location") or "Unknown").strip(),
        "value": value,
        "unit": str(payload.get("unit") or "").strip(),
    }


def _build_raw_insert_sql(table_name: str, rows: List[Dict]) -> str:
    batch_id = f"kafka_{uuid.uuid4().hex[:10]}"
    values_sql = []

    for row in rows:
        ts_text = row["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
        values_sql.append(
            "(" 
            f"CAST('{_escape(ts_text)}' AS TIMESTAMP), "
            f"'{_escape(row['device_id'])}', "
            f"'{_escape(row['device_name'])}', "
            f"'{_escape(row['device_type'])}', "
            f"'{_escape(row['location'])}', "
            f"{row['value']}, "
            f"'{_escape(row['unit'])}', "
            f"'{batch_id}', "
            "CURRENT_TIMESTAMP()"
            ")"
        )

    return (
        f"INSERT INTO {table_name} "
        "(timestamp, device_id, device_name, device_type, location, value, unit, batch_id, _processing_time) "
        "VALUES " + ", ".join(values_sql)
    )


def _latest_per_device(rows: List[Dict]) -> List[Dict]:
    latest = {}
    for row in rows:
        device_id = row["device_id"]
        old = latest.get(device_id)
        if old is None or row["timestamp"] >= old["timestamp"]:
            latest[device_id] = row
    return list(latest.values())


def _build_latest_merge_sql(table_name: str, rows: List[Dict]) -> str:
    source_selects = []
    for row in rows:
        ts_text = row["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
        source_selects.append(
            "SELECT "
            f"'{_escape(row['device_id'])}' AS device_id, "
            f"'{_escape(row['device_name'])}' AS device_name, "
            f"'{_escape(row['device_type'])}' AS device_type, "
            f"'{_escape(row['location'])}' AS location, "
            f"{row['value']} AS latest_value, "
            f"'{_escape(row['unit'])}' AS unit, "
            f"CAST('{_escape(ts_text)}' AS TIMESTAMP) AS last_update, "
            "'NORMAL' AS status"
        )

    source_sql = " UNION ALL ".join(source_selects)

    return f"""
MERGE INTO {table_name} AS t
USING ({source_sql}) AS s
ON t.device_id = s.device_id
WHEN MATCHED THEN UPDATE SET
  t.device_name = s.device_name,
  t.device_type = s.device_type,
  t.location = s.location,
  t.latest_value = s.latest_value,
  t.unit = s.unit,
  t.last_update = s.last_update,
  t.status = s.status
WHEN NOT MATCHED THEN INSERT (
  device_id, device_name, device_type, location, latest_value, unit, last_update, status
) VALUES (
  s.device_id, s.device_name, s.device_type, s.location, s.latest_value, s.unit, s.last_update, s.status
)
"""


def _persist_rows(rows: List[Dict]) -> None:
    if not rows:
        return

    client = get_databricks_client()
    raw_table = f"{client.catalog}.{client.schema}.iot_sensor_data"
    latest_table = f"{client.catalog}.{client.schema}.iot_latest_readings"

    raw_sql = _build_raw_insert_sql(raw_table, rows)
    raw_result = client.execute_query(raw_sql, timeout=120)
    if raw_result.get("error"):
        raise RuntimeError(f"raw insert failed: {raw_result['error']}")

    latest_rows = _latest_per_device(rows)
    merge_sql = _build_latest_merge_sql(latest_table, latest_rows)
    latest_result = client.execute_query(merge_sql, timeout=120)
    if latest_result.get("error"):
        raise RuntimeError(f"latest merge failed: {latest_result['error']}")


def main() -> None:
    KafkaConsumer, KafkaError = _load_kafka_consumer()

    servers = [x.strip() for x in KAFKA_BOOTSTRAP_SERVERS.split(",") if x.strip()]
    if not servers:
        raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is empty")

    print("[KAFKA->DATABRICKS] Starting sink")
    print(f"[KAFKA->DATABRICKS] brokers={servers}")
    print(f"[KAFKA->DATABRICKS] topic={KAFKA_TELEMETRY_TOPIC}")

    consumer = KafkaConsumer(
        {
            "bootstrap.servers": ",".join(servers),
            "group.id": "kafka-databricks-sink",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "session.timeout.ms": 10000,
        }
    )
    consumer.subscribe([KAFKA_TELEMETRY_TOPIC])

    buffered: List[Dict] = []
    last_flush = time.time()

    try:
        while RUNNING.is_set():
            message = consumer.poll(timeout=1.0)

            if message is not None:
                if message.error():
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[KAFKA->DATABRICKS] consume error: {message.error()}")
                else:
                    try:
                        payload = __import__("json").loads((message.value() or b"").decode("utf-8"))
                        row = _normalize(payload)
                        if row is not None:
                            buffered.append(row)
                    except Exception:
                        pass

            now = time.time()
            should_flush = buffered and (
                len(buffered) >= KAFKA_DATABRICKS_MAX_BATCH
                or (now - last_flush) >= KAFKA_DATABRICKS_FLUSH_SECONDS
            )

            if should_flush:
                batch = buffered
                buffered = []

                try:
                    _persist_rows(batch)
                    print(
                        "[KAFKA->DATABRICKS] persisted_rows="
                        f"{len(batch)} latest_devices={len(_latest_per_device(batch))}"
                    )
                except Exception as exc:
                    print(f"[KAFKA->DATABRICKS] persist error: {exc}")

                last_flush = now
    finally:
        if buffered:
            try:
                _persist_rows(buffered)
                print(f"[KAFKA->DATABRICKS] final_persisted_rows={len(buffered)}")
            except Exception as exc:
                print(f"[KAFKA->DATABRICKS] final persist error: {exc}")

        consumer.close()
        print("[KAFKA->DATABRICKS] Stopped")


if __name__ == "__main__":
    main()
