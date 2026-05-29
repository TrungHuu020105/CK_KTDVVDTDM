"""
Generate synthetic IoT dashboard data and write to PostgreSQL.

Default behavior:
- 5 sensors, same profiles as the original Databricks notebook
- 2 records per minute per sensor: second 00 and second 30
- 15 full calendar days in Asia/Ho_Chi_Minh timezone
- Writes to PostgreSQL table public.smart_filtered_measurements

Install:
    pip install psycopg2-binary

Run example:
    export POSTGRES_HOST=localhost
    export POSTGRES_PORT=5432
    export POSTGRES_DB=iot_analytics
    export POSTGRES_USER=postgres
    export POSTGRES_PASSWORD=your_password
    python generate_iot_postgres_data.py
"""

from __future__ import annotations

import math
import os
import random
import re
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Tuple
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import execute_values


# =========================
# Config
# =========================
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "20.214.247.102")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "rtmps_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "rtmps_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "123456")

POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "ThongKe")

DAYS_BACK = int(os.getenv("DAYS_BACK", "15"))
READINGS_PER_MINUTE = int(os.getenv("READINGS_PER_MINUTE", "2"))
SEED = int(os.getenv("SEED", "42"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))

# overwrite: TRUNCATE table before insert
# append: keep old data and upsert by unique key
WRITE_MODE = os.getenv("WRITE_MODE", "overwrite").lower()

# true  = generate 15 full calendar days, ending at today's 00:00:00, exclusive
# false = generate rolling last 15 days up to current minute, exclusive
FULL_CALENDAR_DAYS = os.getenv("FULL_CALENDAR_DAYS", "true").lower() in {"1", "true", "yes", "y"}

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
random.seed(SEED)


# =========================
# Sensor profiles
# =========================
SENSOR_PROFILES: List[Dict] = [
    {
        "sensor_id": "sensor_1",
        "location": "Living_Room",
        "metric_key": "temperature",
        "unit": "°C",
        "minimum": 15.0,
        "maximum": 40.0,
        "start": 24.5,
        "max_step": 0.45,
        "pull_strength": 0.24,
        "decimals": 2,
        "bias": 0.0,
    },
    {
        "sensor_id": "sensor_2",
        "location": "Living_Room",
        "metric_key": "humidity",
        "unit": "%",
        "minimum": 20.0,
        "maximum": 95.0,
        "start": 62.0,
        "max_step": 1.1,
        "pull_strength": 0.18,
        "decimals": 2,
        "bias": 0.0,
    },
    {
        "sensor_id": "sensor_3",
        "location": "Garden",
        "metric_key": "soil_moisture",
        "unit": "%",
        "minimum": 8.0,
        "maximum": 95.0,
        "start": 55.0,
        "max_step": 0.55,
        "pull_strength": 0.50,
        "decimals": 2,
        # Lower than the original 0.03 so the soil moisture does not stay too often at the max cap.
        "irrigation_chance": 0.006,
    },
    {
        "sensor_id": "sensor_4",
        "location": "Outdoor",
        "metric_key": "light_intensity",
        "unit": "lux",
        "minimum": 0.0,
        "maximum": 60000.0,
        "start": 10.0,
        "max_step": 80.0,
        "pull_strength": 0.30,
        "decimals": 0,
        "light_peak": 38000.0,
        "light_night": 3.0,
    },
    {
        "sensor_id": "sensor_5",
        "location": "Outdoor",
        "metric_key": "pressure",
        "unit": "hPa",
        "minimum": 990.0,
        "maximum": 1035.0,
        "start": 1012.0,
        "max_step": 0.25,
        "pull_strength": 0.16,
        "decimals": 2,
        "bias": 0.0,
    },
]


# =========================
# Helpers
# =========================
def quote_ident(identifier: str) -> str:
    """Safely quote a PostgreSQL identifier with a strict whitelist."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Invalid PostgreSQL identifier: {identifier!r}")
    return f'"{identifier}"'


def qualified_table_name() -> str:
    return f"{quote_ident(POSTGRES_SCHEMA)}.{quote_ident(POSTGRES_TABLE)}"


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(value, maximum))


def hour_fraction(dt: datetime) -> float:
    return dt.hour + dt.minute / 60 + dt.second / 3600


def bounded_random_walk(
    current: float,
    minimum: float,
    maximum: float,
    target: float,
    max_step: float,
    pull_strength: float,
) -> float:
    noise = random.uniform(-max_step, max_step)
    drift = (target - current) * pull_strength
    return clamp(current + noise + drift, minimum, maximum)


def target_for_metric(profile: Dict, current: float, now: datetime) -> float:
    metric_key = profile["metric_key"]
    hour = hour_fraction(now)

    temp_cycle = math.sin((2 * math.pi * (hour - 8)) / 24)
    daylight_factor = max(0.0, math.sin(math.pi * (hour - 6) / 12))
    pressure_cycle = math.sin((2 * math.pi * (hour - 3)) / 24)

    if metric_key == "temperature":
        return 24.5 + profile.get("bias", 0.0) + 5.5 * temp_cycle

    if metric_key == "humidity":
        return 62.0 + profile.get("bias", 0.0) - 12.0 * temp_cycle

    if metric_key == "soil_moisture":
        evap = 0.05 + 0.10 * daylight_factor + max(temp_cycle, 0.0) * 0.08
        target = current - evap
        if random.random() < profile.get("irrigation_chance", 0.006):
            target += random.uniform(4.0, 9.0)
        return target

    if metric_key == "light_intensity":
        light_night = profile.get("light_night", 3.0)
        light_peak = profile.get("light_peak", 38000.0)
        return light_night + daylight_factor * (light_peak - light_night)

    if metric_key == "pressure":
        return 1012.0 + profile.get("bias", 0.0) + 1.2 * pressure_cycle

    return current


def update_sensor_value(current: float, profile: Dict, now: datetime) -> float:
    target = target_for_metric(profile, current, now)
    return bounded_random_walk(
        current=current,
        minimum=profile["minimum"],
        maximum=profile["maximum"],
        target=target,
        max_step=profile["max_step"],
        pull_strength=profile["pull_strength"],
    )


def get_time_range() -> Tuple[datetime, datetime]:
    now = datetime.now(VN_TZ)

    if FULL_CALENDAR_DAYS:
        # Example: if today is 2026-05-07, generate from 2026-04-22 00:00:00
        # to 2026-05-07 00:00:00, exclusive. This gives 15 full days.
        end_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        # Rolling range ending at current minute.
        end_time = now.replace(second=0, microsecond=0)

    start_time = end_time - timedelta(days=DAYS_BACK)
    return start_time, end_time


def build_timestamps(start_time: datetime, end_time: datetime) -> Iterable[datetime]:
    if READINGS_PER_MINUTE <= 0:
        raise ValueError("READINGS_PER_MINUTE must be greater than 0")

    step_seconds = 60 / READINGS_PER_MINUTE
    total_steps = int((end_time - start_time).total_seconds() // step_seconds)

    for i in range(total_steps):
        yield start_time + timedelta(seconds=i * step_seconds)


def generate_rows() -> Iterable[Tuple[datetime, str, str, str, float, str]]:
    start_time, end_time = get_time_range()
    current_values = {p["sensor_id"]: float(p["start"]) for p in SENSOR_PROFILES}

    for ts in build_timestamps(start_time, end_time):
        for profile in SENSOR_PROFILES:
            sensor_id = profile["sensor_id"]
            current_value = update_sensor_value(current_values[sensor_id], profile, ts)
            current_values[sensor_id] = current_value

            decimals = profile["decimals"]
            measured_value = int(round(current_value)) if decimals == 0 else round(current_value, decimals)

            yield (
                ts,
                profile["sensor_id"],
                profile["location"],
                profile["metric_key"],
                float(measured_value),
                profile["unit"],
            )


def batched(iterable: Iterable[Tuple], batch_size: int) -> Iterable[List[Tuple]]:
    batch: List[Tuple] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


# =========================
# PostgreSQL write
# =========================
def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def prepare_table(conn) -> None:
    table = qualified_table_name()
    schema = quote_ident(POSTGRES_SCHEMA)

    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id BIGSERIAL PRIMARY KEY,
                event_ts TIMESTAMPTZ NOT NULL,
                sensor_id TEXT NOT NULL,
                location TEXT,
                metric_type TEXT NOT NULL,
                metric_value DOUBLE PRECISION NOT NULL,
                unit TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT uq_{POSTGRES_TABLE}_event_sensor_metric
                    UNIQUE (event_ts, sensor_id, metric_type)
            );
            """
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{POSTGRES_TABLE}_event_ts "
            f"ON {table} (event_ts);"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{POSTGRES_TABLE}_sensor_metric_ts "
            f"ON {table} (sensor_id, metric_type, event_ts);"
        )

        if WRITE_MODE == "overwrite":
            cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY;")
        elif WRITE_MODE == "append":
            pass
        else:
            raise ValueError("WRITE_MODE must be 'overwrite' or 'append'")

    conn.commit()


def insert_rows(conn) -> int:
    table = qualified_table_name()
    total_inserted = 0

    insert_sql = f"""
        INSERT INTO {table} (
            event_ts,
            sensor_id,
            location,
            metric_type,
            metric_value,
            unit
        ) VALUES %s
        ON CONFLICT (event_ts, sensor_id, metric_type)
        DO UPDATE SET
            location = EXCLUDED.location,
            metric_value = EXCLUDED.metric_value,
            unit = EXCLUDED.unit;
    """

    with conn.cursor() as cur:
        for batch in batched(generate_rows(), BATCH_SIZE):
            execute_values(cur, insert_sql, batch, page_size=BATCH_SIZE)
            total_inserted += len(batch)
            print(f"Inserted/upserted {total_inserted:,} rows...")

    conn.commit()
    return total_inserted


def print_validation(conn) -> None:
    table = qualified_table_name()
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                sensor_id,
                metric_type,
                COUNT(*) AS rows,
                MIN(event_ts) AS min_event_ts,
                MAX(event_ts) AS max_event_ts
            FROM {table}
            GROUP BY sensor_id, metric_type
            ORDER BY sensor_id;
            """
        )
        sensor_rows = cur.fetchall()

        cur.execute(
            f"""
            SELECT
                metric_type,
                COUNT(*) AS total_rows,
                ROUND(MIN(metric_value)::numeric, 2) AS min_value,
                ROUND(MAX(metric_value)::numeric, 2) AS max_value
            FROM {table}
            GROUP BY metric_type
            ORDER BY metric_type;
            """
        )
        metric_rows = cur.fetchall()

    print("\n=== Rows by sensor ===")
    for row in sensor_rows:
        print(row)

    print("\n=== Value range by metric ===")
    for row in metric_rows:
        print(row)


def main() -> None:
    start_time, end_time = get_time_range()
    expected_rows_per_sensor = DAYS_BACK * 24 * 60 * READINGS_PER_MINUTE
    expected_total_rows = expected_rows_per_sensor * len(SENSOR_PROFILES)

    print("Target:", qualified_table_name())
    print("Timezone:", VN_TZ)
    print("Time range:", start_time.isoformat(), "->", end_time.isoformat(), "exclusive")
    print("Days:", DAYS_BACK)
    print("Readings per minute per sensor:", READINGS_PER_MINUTE)
    print("Sensors:", len(SENSOR_PROFILES))
    print("Expected rows per sensor:", f"{expected_rows_per_sensor:,}")
    print("Expected total rows:", f"{expected_total_rows:,}")
    print("Write mode:", WRITE_MODE)

    with get_connection() as conn:
        prepare_table(conn)
        inserted = insert_rows(conn)
        print(f"\nDone. Generated {inserted:,} rows.")
        print_validation(conn)


if __name__ == "__main__":
    main()
