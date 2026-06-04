#!/usr/bin/env python3
"""
stream_ambient_network_mqtt.py

Lấy dữ liệu nhiệt độ + độ ẩm từ 4 trạm public trên Ambient Weather Network
và publish lên MQTT theo schema gần giống stream_weatherbit_mqtt.py.

4 trạm mặc định:
1. 3/132 Khâm Thiên - Đống Đa, Hà Nội
2. Olam Pepper Plantation - Chư Pưh, Gia Lai
3. NhaTrang - Nha Trang, Khánh Hòa
4. Bien Hoa Airbase - Thành phố Biên Hòa

Nguồn:
Ambient Weather Network public station qua aioambient.OpenAPI.
Không cần applicationKey/apiKey.

Cài thư viện:
    pip install aioambient aiohttp paho-mqtt

Test lấy dữ liệu, chưa gửi MQTT:
    python stream_ambient_network_mqtt.py --dry-run --once

Chạy liên tục 1 phút/lần và gửi MQTT:
    python stream_ambient_network_mqtt.py --broker 20.214.247.102 --port 1883 --interval 60

Chạy 1 lần và gửi MQTT:
    python stream_ambient_network_mqtt.py --broker 20.214.247.102 --port 1883 --once

Liệt kê trạm tìm được quanh 4 vị trí:
    python stream_ambient_network_mqtt.py --list-stations
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import os
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

try:
    from aioambient import OpenAPI
except ImportError:
    print("Thiếu thư viện aioambient. Cài bằng: pip install aioambient aiohttp")
    sys.exit(1)

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None


VN_TZ = timezone(timedelta(hours=7))
DEFAULT_TIMEZONE = "Asia/Ho_Chi_Minh"
DEFAULT_INTERVAL_SECONDS = 60
DEFAULT_TOPIC_TEMPLATE = "sensors/{source}/data"
DEFAULT_OUTPUT_CSV = ""

ENV_FILE_CANDIDATES = (
    ".env",
    "app/.env",
    "iot_backend/.env",
)


@dataclass
class StationTarget:
    name: str
    source: str
    latitude: float
    longitude: float
    radius_miles: float
    keywords: list[str]


# Tọa độ là điểm tìm kiếm gần trạm. aioambient sẽ tìm station public quanh tọa độ này.
# Nếu trạm không hiện, tăng radius_miles lên 20-50.
DEFAULT_TARGETS: list[StationTarget] = [
    StationTarget(
        name="HaNoi_KhamThien",
        source="sensor_hanoi",
        latitude=21.0189,
        longitude=105.8385,
        radius_miles=10,
        keywords=["3/132", "Kham Thien", "Khâm Thiên", "Dong Da", "Đống Đa"],
    ),
    StationTarget(
        name="GiaLai_OlamPepper",
        source="sensor_gialai",
        latitude=13.45,
        longitude=108.05,
        radius_miles=50,
        keywords=["Olam", "Pepper", "Plantation", "Chư Pưh", "Chu Puh"],
    ),
    StationTarget(
        name="NhaTrang",
        source="sensor_nhatrang",
        latitude=12.2388,
        longitude=109.1967,
        radius_miles=15,
        keywords=["NhaTrang", "Nha Trang", "URENCO"],
    ),
    StationTarget(
        name="BienHoa_Airbase",
        source="sensor_bienhoa",
        latitude=10.9763,
        longitude=106.8183,
        radius_miles=12,
        keywords=["Bien Hoa Airbase", "Biên Hòa Airbase", "IM1 G2L", "thành phố Biên Hòa", "Bien Hoa"],
    ),
]


# =============================================================================
# ENV
# =============================================================================

def load_env_files() -> None:
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


def env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


# =============================================================================
# Helpers
# =============================================================================

def now_vn() -> datetime:
    return datetime.now(VN_TZ).replace(microsecond=0)


def normalize_text(value: Any) -> str:
    text = str(value or "").lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text


def clean_float(value: Any) -> float | None:
    if value is None:
        return None

    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None

    if math.isnan(number):
        return None

    return round(number, 2)


def f_to_c(value_f: Any) -> float | None:
    number = clean_float(value_f)
    if number is None:
        return None
    return round((number - 32) * 5 / 9, 2)


def pick_value(data: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in data and data[key] not in (None, "", "null"):
            return data[key]
    return None


def calc_rh_from_temp_dewpoint(temp_c: float | None, dewpoint_c: float | None) -> float | None:
    """
    Tính độ ẩm tương đối từ nhiệt độ và điểm sương.
    Dùng khi public station không trả humidity trực tiếp.
    """
    if temp_c is None or dewpoint_c is None:
        return None

    a = 17.625
    b = 243.04

    rh = 100 * math.exp((a * dewpoint_c) / (b + dewpoint_c)) / math.exp(
        (a * temp_c) / (b + temp_c)
    )

    return round(max(0, min(100, rh)), 1)


def parse_ambient_time(value: Any) -> tuple[str, int | None]:
    """
    Ambient thường trả dateutc dạng milliseconds.
    Trả về:
        timestamp_iso_vn, timestamp_ms
    """
    if value is None or value == "":
        return "", None

    try:
        raw = int(float(str(value)))

        # milliseconds
        if raw > 10_000_000_000:
            dt = datetime.fromtimestamp(raw / 1000, tz=timezone.utc).astimezone(VN_TZ)
            return dt.isoformat(), raw

        # seconds
        dt = datetime.fromtimestamp(raw, tz=timezone.utc).astimezone(VN_TZ)
        return dt.isoformat(), raw * 1000
    except (TypeError, ValueError, OverflowError):
        pass

    # ISO string
    text = str(value).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        dt_vn = dt.astimezone(VN_TZ)
        return dt_vn.isoformat(), int(dt_vn.timestamp() * 1000)
    except ValueError:
        return str(value), None


# =============================================================================
# Ambient OpenAPI
# =============================================================================

def get_station_name(station: dict[str, Any]) -> str:
    info = station.get("info") or {}

    for item in [
        info.get("name"),
        station.get("name"),
        station.get("stationName"),
        station.get("station_name"),
        station.get("location"),
    ]:
        if item:
            return str(item)

    return "Unknown"


def get_station_mac(station: dict[str, Any]) -> str | None:
    for item in [
        station.get("macAddress"),
        station.get("mac_address"),
        station.get("mac"),
        station.get("id"),
    ]:
        if item:
            return str(item)

    return None


def station_search_text(station: dict[str, Any]) -> str:
    try:
        return normalize_text(json.dumps(station, ensure_ascii=False))
    except TypeError:
        return normalize_text(station)


def select_station(stations: list[dict[str, Any]], keywords: list[str]) -> dict[str, Any] | None:
    if not stations:
        return None

    normalized_keywords = [normalize_text(keyword) for keyword in keywords if keyword.strip()]

    for station in stations:
        text = station_search_text(station)
        if any(keyword in text for keyword in normalized_keywords):
            return station

    # fallback: lấy trạm đầu tiên nếu không match keyword
    return stations[0]


async def find_public_stations(api: OpenAPI, target: StationTarget) -> list[dict[str, Any]]:
    stations = await api.get_devices_by_location(
        target.latitude,
        target.longitude,
        radius=target.radius_miles,
    )

    if not isinstance(stations, list):
        return []

    return stations


async def fetch_station_detail(api: OpenAPI, mac: str) -> dict[str, Any]:
    detail = await api.get_device_details(mac)

    if not isinstance(detail, dict):
        return {}

    return detail


def extract_last_data(detail: dict[str, Any]) -> dict[str, Any]:
    for item in [
        detail.get("lastData"),
        detail.get("last_data"),
        detail.get("data"),
        detail,
    ]:
        if isinstance(item, dict) and item:
            return item

    return {}


def parse_weather_reading(
    target: StationTarget,
    station: dict[str, Any],
    detail: dict[str, Any],
) -> dict[str, Any]:
    last_data = extract_last_data(detail)

    # Ambient thường dùng Fahrenheit cho các field public.
    temp_c = None
    temp_f = pick_value(last_data, ["tempf", "tempF", "outdoorTempF", "temperatureF"])
    if temp_f is not None:
        temp_c = f_to_c(temp_f)
    else:
        temp_c = clean_float(pick_value(last_data, ["tempc", "tempC", "temperature", "temp"]))

    humidity = clean_float(
        pick_value(
            last_data,
            ["humidity", "humidityout", "humidityOut", "outdoorHumidity"],
        )
    )

    dewpoint_c = None
    dewpoint_f = pick_value(
        last_data,
        ["dewPoint", "dewpoint", "dewPointf", "dewpointf", "dewPointF"],
    )
    if dewpoint_f is not None:
        dewpoint_c = f_to_c(dewpoint_f)
    else:
        dewpoint_c = clean_float(pick_value(last_data, ["dewPointC", "dewpointC"]))

    if humidity is None:
        humidity = calc_rh_from_temp_dewpoint(temp_c, dewpoint_c)

    timestamp_raw = pick_value(
        last_data,
        ["dateutc", "lastUpdated", "last_updated", "created_at", "timestamp"],
    )
    timestamp_vn, timestamp_ms = parse_ambient_time(timestamp_raw)

    observed_dt = None
    lag_minutes = None

    if timestamp_ms:
        observed_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).astimezone(VN_TZ)
        lag_minutes = round((now_vn() - observed_dt).total_seconds() / 60, 1)

    station_name = get_station_name(station)
    mac = get_station_mac(station)

    return {
        "timestamp": timestamp_vn or now_vn().isoformat(),
        "timestamp_ms": timestamp_ms,
        "source": target.source,
        "sensor_id": target.source,
        "temperature": temp_c,
        "humidity": humidity,
        "source_type": "public_weather_station",
        "provider": "ambient_weather_network_public",
        "environment_type": "outdoor",
        "saved": True,

        # field bổ sung để debug/truy vết
        "location_name": target.name,
        "station_name": station_name,
        "mac": mac,
        "lag_minutes": lag_minutes,
    }


# =============================================================================
# CSV
# =============================================================================

def append_csv(rows: list[dict[str, Any]], csv_path: str) -> None:
    if not csv_path:
        return

    path = Path(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "timestamp",
        "timestamp_ms",
        "source",
        "sensor_id",
        "location_name",
        "station_name",
        "mac",
        "temperature",
        "humidity",
        "source_type",
        "provider",
        "environment_type",
        "saved",
        "lag_minutes",
    ]

    file_exists = path.exists()

    with path.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


# =============================================================================
# MQTT Publisher
# =============================================================================

def create_mqtt_client():
    if mqtt is None:
        raise RuntimeError("Thiếu paho-mqtt. Cài bằng: pip install paho-mqtt")

    callback_api_version = getattr(mqtt, "CallbackAPIVersion", None)
    if callback_api_version is not None:
        return mqtt.Client(callback_api_version.VERSION2)

    return mqtt.Client()


class AmbientMqttPublisher:
    def __init__(
        self,
        broker: str,
        port: int,
        topic_template: str,
        username: str = "",
        password: str = "",
    ) -> None:
        self.broker = broker
        self.port = port
        self.topic_template = topic_template
        self.username = username
        self.password = password
        self.connected = False

        self.client = create_mqtt_client()
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

        if username:
            self.client.username_pw_set(username, password)

    def _on_connect(self, _client, _userdata, _flags, reason_code=0, _properties=None) -> None:
        try:
            self.connected = not reason_code.is_failure
        except AttributeError:
            self.connected = int(reason_code) == 0

        if self.connected:
            print(f"[MQTT] Connected to {self.broker}:{self.port}")
        else:
            print(f"[MQTT] Connection failed: {reason_code}")

    def _on_disconnect(self, _client, _userdata, *args) -> None:
        self.connected = False
        print("[MQTT] Disconnected")

    def connect(self) -> None:
        print(f"[MQTT] Connecting to {self.broker}:{self.port} ...")

        try:
            self.client.connect(self.broker, self.port, 60)
        except Exception as exc:
            print(f"[MQTT] Initial connect failed: {type(exc).__name__}: {exc}")
            self.connected = False
            return

        self.client.loop_start()

        for _ in range(20):
            if self.connected:
                return
            time.sleep(0.2)

    def disconnect(self) -> None:
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass

    def ensure_connected(self) -> bool:
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

    def resolve_topic(self, source: str) -> str:
        return self.topic_template.format(source=source)

    def publish_rows(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return

        if not self.ensure_connected():
            print("[MQTT] Not connected. Skip publish.")
            return

        for row in rows:
            # Schema chính giữ gần giống stream_weatherbit_mqtt.py
            payload = {
                "timestamp": row["timestamp"],
                "source": row["source"],
                "sensor_id": row["sensor_id"],
                "temperature": row["temperature"],
                "humidity": row["humidity"],
                "source_type": row["source_type"],
                "provider": row["provider"],
                "environment_type": row["environment_type"],
                "saved": row["saved"],

                # Thông tin thêm để debug/truy vết
                "location_name": row.get("location_name"),
                "station_name": row.get("station_name"),
                "mac": row.get("mac"),
                "lag_minutes": row.get("lag_minutes"),
            }

            topic = self.resolve_topic(str(row["source"]))
            result = self.client.publish(topic, json.dumps(payload, ensure_ascii=False), qos=0, retain=False)

            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                print(f"[MQTT] Publish failed topic={topic} rc={result.rc}")
            else:
                print(f"[MQTT] Published topic={topic}")


# =============================================================================
# Stream logic
# =============================================================================

async def fetch_all_targets(
    targets: list[StationTarget],
    list_stations: bool = False,
    show_raw: bool = False,
) -> list[dict[str, Any]]:
    api = OpenAPI()
    rows: list[dict[str, Any]] = []

    for index, target in enumerate(targets, start=1):
        print(f"[{index}/{len(targets)}] Checking {target.name} ...")

        try:
            stations = await find_public_stations(api, target)
        except Exception as exc:
            print(f"  ERROR find station: {type(exc).__name__}: {exc}")
            continue

        if not stations:
            print("  Không tìm thấy trạm public quanh vị trí này.")
            continue

        if list_stations:
            print(f"  Found {len(stations)} station(s):")
            for st_index, station in enumerate(stations, start=1):
                print(f"    {st_index:02d}. {get_station_name(station)} | mac={get_station_mac(station)}")
            continue

        selected = select_station(stations, target.keywords)
        if selected is None:
            print("  Không chọn được trạm.")
            continue

        station_name = get_station_name(selected)
        mac = get_station_mac(selected)

        if not mac:
            print(f"  Trạm {station_name} không có mac/id hợp lệ.")
            continue

        print(f"  Selected: {station_name} | mac={mac}")

        try:
            detail = await fetch_station_detail(api, mac)
        except Exception as exc:
            print(f"  ERROR get detail: {type(exc).__name__}: {exc}")
            continue

        if show_raw:
            print("  Raw detail:")
            print(json.dumps(detail, ensure_ascii=False, indent=2))

        row = parse_weather_reading(target, selected, detail)
        rows.append(row)

        temp = row.get("temperature")
        humidity = row.get("humidity")
        lag = row.get("lag_minutes")
        timestamp = row.get("timestamp")

        print(
            f"  OK temp={temp} C | humidity={humidity}% | "
            f"lag={lag} min | timestamp={timestamp}"
        )

        # tránh gọi quá dồn dập
        await asyncio.sleep(0.5)

    return rows


def filter_duplicates(
    rows: list[dict[str, Any]],
    last_timestamp_by_source: dict[str, int],
    publish_duplicates: bool,
) -> tuple[list[dict[str, Any]], int]:
    if publish_duplicates:
        for row in rows:
            ts = row.get("timestamp_ms")
            if ts:
                last_timestamp_by_source[row["source"]] = int(ts)
        return rows, 0

    filtered_rows: list[dict[str, Any]] = []
    skipped = 0

    for row in rows:
        source = str(row.get("source"))
        ts = row.get("timestamp_ms")

        # Nếu không có timestamp, vẫn cho publish để không mất dữ liệu.
        if ts is None:
            filtered_rows.append(row)
            continue

        ts = int(ts)
        last_ts = last_timestamp_by_source.get(source)

        if last_ts is not None and ts <= last_ts:
            skipped += 1
            print(f"[SKIP] {source}: timestamp không đổi ({row.get('timestamp')})")
            continue

        last_timestamp_by_source[source] = ts
        filtered_rows.append(row)

    return filtered_rows, skipped


async def run(args: argparse.Namespace) -> None:
    targets = DEFAULT_TARGETS

    if args.only:
        only_text = normalize_text(args.only)
        targets = [
            target for target in DEFAULT_TARGETS
            if only_text in normalize_text(target.name)
            or only_text in normalize_text(target.source)
            or any(only_text in normalize_text(keyword) for keyword in target.keywords)
        ]

        if not targets:
            print(f"Không tìm thấy target nào khớp --only {args.only!r}")
            return

    publisher = None
    if not args.dry_run and not args.list_stations:
        publisher = AmbientMqttPublisher(
            broker=args.broker,
            port=args.port,
            topic_template=args.topic_template,
            username=args.username,
            password=args.password,
        )
        publisher.connect()

    last_timestamp_by_source: dict[str, int] = {}
    batch_count = 0

    try:
        while True:
            batch_count += 1
            print("\n" + "=" * 100)
            print(f"AMBIENT WEATHER NETWORK MQTT STREAM | Batch #{batch_count}")
            print(f"Time VN: {now_vn().isoformat()}")
            print(f"Interval: {args.interval}s")
            print("=" * 100)

            rows = await fetch_all_targets(
                targets=targets,
                list_stations=args.list_stations,
                show_raw=args.show_raw,
            )

            if args.list_stations:
                break

            rows_to_publish, skipped = filter_duplicates(
                rows=rows,
                last_timestamp_by_source=last_timestamp_by_source,
                publish_duplicates=args.publish_duplicates,
            )

            if args.csv and rows_to_publish:
                append_csv(rows_to_publish, args.csv)
                print(f"[CSV] Saved {len(rows_to_publish)} row(s) to {args.csv}")

            if args.dry_run:
                print("\nDRY RUN - không publish MQTT.")
                print(json.dumps(rows_to_publish, ensure_ascii=False, indent=2))
            elif publisher:
                publisher.publish_rows(rows_to_publish)

            print(
                f"\nBatch summary: fetched={len(rows)} | "
                f"published/saved={len(rows_to_publish)} | skipped_duplicate={skipped}"
            )

            if args.once:
                break

            print(f"\nSleep {args.interval}s...\n")
            await asyncio.sleep(max(30, int(args.interval)))

    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        if publisher:
            publisher.disconnect()


# =============================================================================
# CLI
# =============================================================================

def parse_args() -> argparse.Namespace:
    load_env_files()

    parser = argparse.ArgumentParser(
        description="Stream Ambient Weather Network public station data to MQTT."
    )

    parser.add_argument(
        "--broker",
        default=os.getenv("MQTT_HOST", "127.0.0.1"),
        help="MQTT broker host",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=env_int("MQTT_PORT", 1883),
        help="MQTT broker port",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=env_int("AMBIENT_INTERVAL_SECONDS", DEFAULT_INTERVAL_SECONDS),
        help="Số giây giữa mỗi lần lấy dữ liệu. Mặc định 60.",
    )
    parser.add_argument(
        "--topic-template",
        default=os.getenv("AMBIENT_MQTT_TOPIC_TEMPLATE", DEFAULT_TOPIC_TEMPLATE),
        help="MQTT topic template. Dùng {source}.",
    )
    parser.add_argument("--username", default=os.getenv("MQTT_USERNAME", ""))
    parser.add_argument("--password", default=os.getenv("MQTT_PASSWORD", ""))

    parser.add_argument(
        "--csv",
        default=os.getenv("AMBIENT_OUTPUT_CSV", DEFAULT_OUTPUT_CSV),
        help="CSV output path. Để rỗng nếu không muốn lưu CSV.",
    )
    parser.add_argument("--once", action="store_true", help="Chạy 1 batch rồi thoát.")
    parser.add_argument("--dry-run", action="store_true", help="Chỉ in dữ liệu, không publish MQTT.")
    parser.add_argument("--show-raw", action="store_true", help="In raw detail data để debug.")
    parser.add_argument("--list-stations", action="store_true", help="Chỉ liệt kê trạm tìm được quanh 4 vị trí.")
    parser.add_argument(
        "--publish-duplicates",
        action="store_true",
        help="Publish cả khi timestamp không đổi. Mặc định sẽ bỏ qua dữ liệu trùng timestamp.",
    )
    parser.add_argument(
        "--only",
        default="",
        help="Chỉ chạy một target, ví dụ: hanoi, olam, nhatrang, bienhoa.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run(parse_args()))
