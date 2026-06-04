"""
Stream Meteostat temperature + humidity data to MQTT using the same flow/schema as stream_data.py.

What this script does:
- Fetch hourly Meteostat data for a location, default 7 days.
- Fill missing hourly rows when possible.
- Replay those rows as live MQTT sensor readings every N seconds.
- Keep MQTT topic/payload style compatible with stream_data.py:
    topic: sensors/{source}/data
    payload fields: timestamp, source, sensor_id, temperature, humidity,
                    source_type, provider, environment_type, saved

Examples:
    python stream_meteostat_mqtt.py --location "Da Nang" --source sensor_danang --broker 20.214.247.102 --port 1883 --interval 5
    python stream_meteostat_mqtt.py --latitude 16.0471 --longitude 108.2068 --source sensor_danang --interval 5
    python stream_meteostat_mqtt.py --location "Ho Chi Minh City" --once
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib import parse as urlparse
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("paho-mqtt not installed. Install with: pip install paho-mqtt")
    sys.exit(1)


DEFAULT_TIMEZONE = "Asia/Ho_Chi_Minh"
VN_TZ = timezone(timedelta(hours=7))
DEFAULT_TOPIC_TEMPLATE = "sensors/{source}/data"
DEFAULT_LOCATION = "Ho Chi Minh City"
DEFAULT_LATITUDE = 10.8231
DEFAULT_LONGITUDE = 106.6297
DEFAULT_OUTPUT = "data/meteostat_stream_rows.csv"
GEOCODING_API = "https://geocoding-api.open-meteo.com/v1/search"
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


def _now_vn() -> datetime:
    return datetime.now(VN_TZ).replace(microsecond=0)


def _clean_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:  # NaN
        return None
    return round(number, 2)


def _parse_iso_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _vn_timestamp(event_utc: datetime) -> str:
    return event_utc.astimezone(VN_TZ).isoformat()


def make_sensor_source(location_label: str) -> str:
    """Create a backend-friendly source/sensor_id from location.

    Example:
        Dà Nẵng -> sensor_danang
        Da Nang -> sensor_danang
        Ho Chi Minh City -> sensor_hochiminhcity
    """
    normalized = unicodedata.normalize("NFKD", location_label)
    ascii_text = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    ascii_text = ascii_text.lower()
    ascii_text = re.sub(r"[^a-z0-9]+", "", ascii_text)
    return f"sensor_{ascii_text or 'meteostat'}"


def geocode_location(query: str) -> tuple[float, float, str]:
    params = {
        "name": query,
        "count": 1,
        "language": "en",
        "format": "json",
    }
    url = f"{GEOCODING_API}?{urlparse.urlencode(params)}"
    req = urlrequest.Request(url, method="GET")
    try:
        with urlrequest.urlopen(req, timeout=15) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Cannot geocode location '{query}': {exc}") from exc

    results = payload.get("results") or []
    if not results:
        raise RuntimeError(f"Cannot geocode location '{query}'")

    top = results[0]
    name_parts = [top.get("name"), top.get("admin1"), top.get("country")]
    label = ", ".join(str(part) for part in name_parts if part)
    return float(top["latitude"]), float(top["longitude"]), label or query


def _normalize_meteostat_frame(frame: Any) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []

    rows: list[dict[str, Any]] = []
    for _, row in frame.reset_index().iterrows():
        event_ts = row.get("time")
        if hasattr(event_ts, "to_pydatetime"):
            event_ts = event_ts.to_pydatetime()
        if not isinstance(event_ts, datetime):
            continue

        event_utc = event_ts.replace(tzinfo=timezone.utc)
        temperature = _clean_float(row.get("temp"))
        humidity = _clean_float(row.get("rhum"))
        if temperature is None and humidity is None:
            continue

        rows.append(
            {
                "timestamp": _vn_timestamp(event_utc),
                "timestamp_utc": event_utc.isoformat(),
                "temperature": temperature,
                "humidity": humidity,
                "source_type": "virtual_meteostat",
                "provider": "meteostat",
                "is_filled": "false",
                "fill_method": "original",
            }
        )

    rows.sort(key=lambda item: str(item["timestamp_utc"]))
    return rows


def _fetch_meteostat_window(
    latitude: float,
    longitude: float,
    start_utc: datetime,
    end_utc: datetime,
) -> list[dict[str, Any]]:
    from meteostat import Hourly, Point, Stations

    start_naive = start_utc.replace(tzinfo=None)
    end_naive = end_utc.replace(tzinfo=None)

    # First try by coordinates.
    try:
        frame = Hourly(Point(float(latitude), float(longitude)), start_naive, end_naive).fetch()
        rows = _normalize_meteostat_frame(frame)
        if rows:
            return rows
    except Exception:
        pass

    # Fallback: try nearby stations.
    try:
        stations = Stations().nearby(float(latitude), float(longitude)).fetch(8)
    except Exception:
        stations = None

    if stations is None or stations.empty:
        return []

    for station_id in list(stations.index):
        try:
            frame = Hourly(str(station_id), start_naive, end_naive).fetch()
            rows = _normalize_meteostat_frame(frame)
            if rows:
                for row in rows:
                    row["provider"] = f"meteostat_station:{station_id}"
                return rows
        except Exception:
            continue

    return []


def fetch_meteostat_hourly(
    latitude: float,
    longitude: float,
    hours: int,
    *,
    end_utc: datetime | None = None,
    max_search_days: int = 730,
) -> tuple[list[dict[str, Any]], datetime, datetime]:
    try:
        import meteostat  # noqa: F401
    except ImportError as exc:
        raise RuntimeError("Missing dependency: meteostat. Run: pip install meteostat") from exc

    search_end = (end_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
    search_end = search_end.replace(minute=0, second=0, microsecond=0)
    max_attempts = max(1, int(max_search_days / 7) + 1)

    for attempt in range(max_attempts):
        candidate_end = search_end - timedelta(days=attempt * 7)
        candidate_start = candidate_end - timedelta(hours=hours)
        rows = _fetch_meteostat_window(latitude, longitude, candidate_start, candidate_end)
        if rows:
            return rows, candidate_start, candidate_end

    return [], search_end - timedelta(hours=hours), search_end


def fill_missing_hourly_rows(
    rows: list[dict[str, Any]],
    start_utc: datetime,
    hours: int,
) -> list[dict[str, Any]]:
    if not rows:
        return []

    normalized_start = start_utc.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    by_ts = {_parse_iso_datetime(str(row["timestamp_utc"])): dict(row) for row in rows}

    values_by_hour: dict[int, list[dict[str, Any]]] = {}
    for ts, row in by_ts.items():
        values_by_hour.setdefault(ts.hour, []).append(row)

    def _average_for_hour(hour: int, key: str) -> float | None:
        values = [_clean_float(row.get(key)) for row in values_by_hour.get(hour, [])]
        values = [value for value in values if value is not None]
        if not values:
            return None
        return round(sum(values) / len(values), 2)

    def _nearest_value(target_ts: datetime, key: str) -> float | None:
        nearest: tuple[float, float] | None = None
        for ts, row in by_ts.items():
            value = _clean_float(row.get(key))
            if value is None:
                continue
            distance = abs((ts - target_ts).total_seconds())
            if nearest is None or distance < nearest[0]:
                nearest = (distance, value)
        return nearest[1] if nearest else None

    filled_rows: list[dict[str, Any]] = []
    for offset in range(hours):
        current_ts = normalized_start + timedelta(hours=offset)
        existing = by_ts.get(current_ts)
        if existing:
            existing.setdefault("timestamp", _vn_timestamp(current_ts))
            existing.setdefault("is_filled", "false")
            existing.setdefault("fill_method", "original")
            filled_rows.append(existing)
            continue

        temperature = _average_for_hour(current_ts.hour, "temperature")
        humidity = _average_for_hour(current_ts.hour, "humidity")
        method = "same_hour_average"

        if temperature is None:
            temperature = _nearest_value(current_ts, "temperature")
            method = "nearest_observation"
        if humidity is None:
            humidity = _nearest_value(current_ts, "humidity")
            method = "nearest_observation"

        filled_rows.append(
            {
                "timestamp": _vn_timestamp(current_ts),
                "timestamp_utc": current_ts.isoformat(),
                "temperature": temperature,
                "humidity": humidity,
                "source_type": "virtual_meteostat",
                "provider": "meteostat_filled",
                "is_filled": "true",
                "fill_method": method,
            }
        )

    return filled_rows


def write_csv(rows: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "timestamp",
        "timestamp_utc",
        "temperature",
        "humidity",
        "source_type",
        "provider",
        "is_filled",
        "fill_method",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def load_csv(input_path: Path) -> list[dict[str, Any]]:
    with input_path.open("r", newline="", encoding="utf-8") as file:
        rows = list(csv.DictReader(file))
    for row in rows:
        row["temperature"] = _clean_float(row.get("temperature"))
        row["humidity"] = _clean_float(row.get("humidity"))
    return rows


def _create_mqtt_client() -> mqtt.Client:
    callback_api_version = getattr(mqtt, "CallbackAPIVersion", None)
    if callback_api_version is not None:
        return mqtt.Client(callback_api_version.VERSION2)
    return mqtt.Client()


def _reason_code_is_success(reason_code) -> bool:
    if hasattr(reason_code, "is_failure"):
        return not reason_code.is_failure
    return int(reason_code) == 0


class MeteostatReplayGenerator:
    def __init__(self, rows: list[dict[str, Any]], source: str, loop_forever: bool = True) -> None:
        if not rows:
            raise RuntimeError("No Meteostat rows to stream")
        self.rows = rows
        self.source = source
        self.loop_forever = loop_forever
        self.index = 0
        self.last_saved_temperature: float | None = None
        self.last_saved_humidity: float | None = None
        self.last_saved_timestamp: datetime | None = None
        self.generated_count = 0
        self.saved_count = 0
        self.dropped_count = 0

    def _should_save(self, temperature: float | None, humidity: float | None) -> tuple[bool, str]:
        if self.last_saved_timestamp is None:
            return True, "first_reading"

        if FAKE_ALWAYS_SAVE:
            return True, "fake_always_save=true"

        reasons: list[str] = []
        should = False

        if temperature is not None and self.last_saved_temperature is not None:
            temp_delta = abs(temperature - self.last_saved_temperature)
            if temp_delta >= FAKE_TEMP_DELTA_THRESHOLD:
                should = True
                reasons.append(f"temperature_change={temp_delta:.2f}>={FAKE_TEMP_DELTA_THRESHOLD}")
            else:
                reasons.append(f"temperature_delta={temp_delta:.2f}")

        if humidity is not None and self.last_saved_humidity is not None:
            humidity_delta = abs(humidity - self.last_saved_humidity)
            if humidity_delta >= FAKE_HUMIDITY_DELTA_THRESHOLD:
                should = True
                reasons.append(f"humidity_change={humidity_delta:.2f}>={FAKE_HUMIDITY_DELTA_THRESHOLD}")
            else:
                reasons.append(f"humidity_delta={humidity_delta:.2f}")

        now = _now_vn()
        time_since_save = (now - self.last_saved_timestamp).total_seconds()
        if time_since_save >= FAKE_SAVE_INTERVAL_SECONDS:
            should = True
            reasons.append(f"time={time_since_save:.0f}s>={FAKE_SAVE_INTERVAL_SECONDS}s")
        else:
            reasons.append(f"t={time_since_save:.0f}s")

        if should:
            return True, " | ".join(reasons)
        return False, "filtered(" + " | ".join(reasons) + ")"

    def run_once(self) -> Dict[str, object]:
        if self.index >= len(self.rows):
            if not self.loop_forever:
                raise StopIteration("No more Meteostat rows to replay")
            self.index = 0

        source_row = self.rows[self.index]
        self.index += 1
        self.generated_count += 1

        now = _now_vn()
        timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")
        timestamp_iso = now.isoformat()

        temperature = _clean_float(source_row.get("temperature"))
        humidity = _clean_float(source_row.get("humidity"))
        saved, reason = self._should_save(temperature, humidity)

        if saved:
            self.last_saved_temperature = temperature
            self.last_saved_humidity = humidity
            self.last_saved_timestamp = now
            self.saved_count += 1
        else:
            self.dropped_count += 1

        reading = {
            "source": self.source,
            "timestamp": timestamp_str,
            "timestamp_iso": timestamp_iso,
            "temperature": temperature,
            "humidity": humidity,
            "saved": saved,
            "reason": reason,
            "meteostat_timestamp": source_row.get("timestamp"),
            "meteostat_provider": source_row.get("provider"),
            "meteostat_is_filled": source_row.get("is_filled"),
        }

        return {
            "readings": [reading],
            "timestamp": timestamp_str,
            "timestamp_iso": timestamp_iso,
            "generated": 1,
            "saved": 1 if saved else 0,
            "dropped": 0 if saved else 1,
            "next_index": self.index,
            "total_rows": len(self.rows),
        }


class MeteostatMqttPublisher:
    def __init__(
        self,
        rows: list[dict[str, Any]],
        source: str,
        broker: str = "127.0.0.1",
        port: int = 1883,
        interval: int = 5,
        topic_template: str = DEFAULT_TOPIC_TEMPLATE,
        fixed_topic: str | None = None,
        username: str = "",
        password: str = "",
        loop_dataset: bool = True,
        location_label: str = DEFAULT_LOCATION,
    ) -> None:
        self.broker = broker
        self.port = port
        self.interval = interval
        self.topic_template = topic_template
        self.fixed_topic = fixed_topic
        self.username = username
        self.password = password
        self.source = source
        self.location_label = location_label

        self.generator = MeteostatReplayGenerator(rows=rows, source=source, loop_forever=loop_dataset)
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

    def publish_batch(self) -> bool:
        if not self._ensure_connected():
            return False

        try:
            data = self.generator.run_once()
        except StopIteration:
            print("[INFO] Dataset finished. Use default loop mode or remove --no-loop to replay again.")
            return False

        self.batch_count += 1
        readings = data.get("readings", [])
        if not readings:
            print("[INFO] No readings generated in this batch")
            return True

        self.total_records += len(readings)
        timestamp = str(data.get("timestamp", _now_vn().strftime("%Y-%m-%d %H:%M:%S")))
        db_worthy = len([r for r in readings if r.get("saved", True)])
        realtime_only = len(readings) - db_worthy

        for reading in readings:
            # Keep schema compatible with stream_data.py MQTT ingest.
            payload = {
                "timestamp": reading.get("timestamp_iso") or reading.get("timestamp", timestamp),
                "source": reading.get("source"),
                "sensor_id": reading.get("source"),
                "temperature": reading.get("temperature"),
                "humidity": reading.get("humidity"),
                "source_type": "physical_iot",
                "provider": "meteostat_replay",
                "environment_type": "outdoor",
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
            print(
                f"  MQTT source={reading['source']} | temp={float(reading['temperature']):6.2f} C | "
                f"humidity={float(reading['humidity']):6.2f} % | topic={topic} | "
                f"saved={reading['saved']} | {reading.get('reason', 'published')} | "
                f"meteostat_ts={reading.get('meteostat_timestamp')}"
            )
        print()
        return True

    def run_continuous(self) -> None:
        print("=" * 100)
        print("METEOSTAT MQTT PUBLISHER - SENSOR READINGS")
        print("=" * 100)
        print(f"Location: {self.location_label}")
        print(f"Source/Sensor ID: {self.source}")
        print(f"Broker: {self.broker}:{self.port}")
        print(f"Interval: {self.interval}s per batch")
        print(f"Topic template: {self.topic_template}")
        print(f"Resolved topic: {self._resolve_topic(self.source)}")
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
                keep_going = self.publish_batch()
                if not keep_going:
                    break
                time.sleep(self.interval)
        except KeyboardInterrupt:
            print("\n" + "=" * 100)
            print("STOPPED BY USER")
            print("=" * 100)
            print(f"Total batches published: {self.batch_count}")
            print(f"Total readings published: {self.total_records}")
            print(f"Approx runtime: {self.batch_count * self.interval} seconds")
            print("=" * 100)
        finally:
            self.disconnect()
            print("Disconnected MQTT\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Meteostat data and replay it to MQTT")
    parser.add_argument("--location", default="", help="Location name, for example: Da Nang, Ho Chi Minh City")
    parser.add_argument("--latitude", type=float, default=None, help="Latitude override")
    parser.add_argument("--longitude", type=float, default=None, help="Longitude override")
    parser.add_argument("--source", default="", help="MQTT source/sensor_id override, for example: sensor_danang")
    parser.add_argument("--hours", type=int, default=24 * 7, help="Number of historical hours to fetch")
    parser.add_argument("--end-date", default="", help="Vietnam-time end date, for example: 2026-06-01T00:00:00+07:00")
    parser.add_argument("--max-search-days", type=int, default=730, help="How far back to search when recent data is empty")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="CSV output path for fetched rows")
    parser.add_argument("--no-fill", action="store_true", help="Keep raw Meteostat rows instead of filling missing hours")
    parser.add_argument("--stream-from", default="", help="Existing CSV file to stream instead of fetching first")
    parser.add_argument("--no-loop", action="store_true", help="Replay dataset once and stop after all rows are used")

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
        help="Fixed topic override for all readings, e.g. sensors/sensor_danang/data",
    )
    parser.add_argument("--username", type=str, default=os.getenv("MQTT_USERNAME", ""), help="MQTT username")
    parser.add_argument("--password", type=str, default=os.getenv("MQTT_PASSWORD", ""), help="MQTT password")
    parser.add_argument("--once", action="store_true", help="Publish one batch and exit")
    return parser.parse_args()


def main() -> int:
    _load_env_files()
    args = parse_args()
    hours = max(1, int(args.hours))

    latitude = args.latitude
    longitude = args.longitude
    location_query = args.location.strip() or DEFAULT_LOCATION
    location_label = location_query

    if args.stream_from:
        rows = load_csv(Path(args.stream_from))
        start_utc = None
        end_utc = None
        if latitude is None:
            latitude = DEFAULT_LATITUDE
        if longitude is None:
            longitude = DEFAULT_LONGITUDE
    else:
        if latitude is None or longitude is None:
            latitude, longitude, location_label = geocode_location(location_label)

        requested_end = None
        if args.end_date.strip():
            requested_end = datetime.fromisoformat(args.end_date.strip().replace("Z", "+00:00"))
            if requested_end.tzinfo is None:
                requested_end = requested_end.replace(tzinfo=VN_TZ)
            requested_end = requested_end.astimezone(timezone.utc)

        rows, start_utc, end_utc = fetch_meteostat_hourly(
            latitude=latitude,
            longitude=longitude,
            hours=hours,
            end_utc=requested_end,
            max_search_days=max(0, int(args.max_search_days)),
        )
        if not rows:
            print("No Meteostat rows were returned for this location/time range.", file=sys.stderr)
            return 1

        raw_count = len(rows)
        if not args.no_fill:
            rows = fill_missing_hourly_rows(rows, start_utc=start_utc, hours=hours)
        write_csv(rows, Path(args.output))

    # Use the user-provided location text to build the source.
    # This avoids geocoding labels like "Da Nang, Da Nang, Vietnam" becoming sensor_danangdanangvietnam.
    source = args.source.strip() or make_sensor_source(location_query)

    print(f"Location: {location_label} ({latitude}, {longitude})")
    print(f"Source/Sensor ID: {source}")
    print(f"Rows: {len(rows)} hourly records")
    if not args.stream_from and not args.no_fill:
        print(f"Raw Meteostat rows: {raw_count}; filled rows: {len(rows) - raw_count}")
    if start_utc and end_utc:
        print(f"Window UTC: {start_utc.isoformat()} -> {end_utc.isoformat()}")
        print(f"Window VN: {start_utc.astimezone(VN_TZ).isoformat()} -> {end_utc.astimezone(VN_TZ).isoformat()}")
    if not args.stream_from:
        print(f"CSV: {Path(args.output).resolve()}")

    publisher = MeteostatMqttPublisher(
        rows=rows,
        source=source,
        broker=args.broker,
        port=args.port,
        interval=max(1, int(args.interval)),
        topic_template=args.topic_template,
        fixed_topic=args.topic or None,
        username=args.username,
        password=args.password,
        loop_dataset=not args.no_loop,
        location_label=location_label,
    )

    if args.once:
        publisher.connect()
        try:
            publisher.publish_batch()
        finally:
            publisher.disconnect()
        return 0

    publisher.run_continuous()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
