"""Weather and geocoding service powered by Open-Meteo."""

from __future__ import annotations

import json
import unicodedata
from datetime import datetime
from dataclasses import dataclass
from typing import Optional
from urllib import parse as urlparse
from urllib import request as urlrequest
from urllib.error import URLError, HTTPError


GEOCODING_API = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_API = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_API = "https://archive-api.open-meteo.com/v1/archive"


@dataclass
class GeocodeResult:
    name: str
    latitude: float
    longitude: float
    country: Optional[str] = None
    admin1: Optional[str] = None
    timezone: Optional[str] = None


def _json_get(url: str, timeout: int = 12) -> dict:
    req = urlrequest.Request(url, method="GET")
    try:
        with urlrequest.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (URLError, HTTPError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Failed to fetch Open-Meteo data: {exc}")


def geocode_location(query: str) -> Optional[GeocodeResult]:
    """Resolve a location string to coordinates via Open-Meteo geocoding API."""
    if not query or not query.strip():
        return None
    original = query.strip()

    def _strip_accents(text: str) -> str:
        normalized = unicodedata.normalize("NFD", text)
        return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")

    def _query_once(name: str, country_code: Optional[str] = None) -> Optional[GeocodeResult]:
        params = {
            "name": name,
            "count": 1,
            "language": "en",
            "format": "json",
        }
        if country_code:
            params["countryCode"] = country_code
        url = f"{GEOCODING_API}?{urlparse.urlencode(params)}"
        payload = _json_get(url)
        results = payload.get("results") or []
        if not results:
            return None
        top = results[0]
        return GeocodeResult(
            name=top.get("name") or name,
            latitude=float(top["latitude"]),
            longitude=float(top["longitude"]),
            country=top.get("country"),
            admin1=top.get("admin1"),
            timezone=top.get("timezone"),
        )

    candidates = []
    for item in (
        original,
        _strip_accents(original),
        f"{original}, VN",
        f"{original}, Vietnam",
        f"{original} Province, Vietnam",
        f"{_strip_accents(original)} Province, Vietnam",
    ):
        if item and item not in candidates:
            candidates.append(item)

    # Prefer Vietnam search first for better province/city matching.
    for candidate in candidates:
        found = _query_once(candidate, country_code="VN")
        if found:
            return found

    # Fallback: global search without country filter.
    for candidate in candidates:
        found = _query_once(candidate, country_code=None)
        if found:
            return found

    return None


def get_current_weather(latitude: float, longitude: float, timezone: str = "auto") -> Optional[dict]:
    """Fetch current weather snapshot from Open-Meteo."""
    if latitude is None or longitude is None:
        return None
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m",
        "timezone": timezone or "auto",
    }
    url = f"{FORECAST_API}?{urlparse.urlencode(params)}"
    try:
        payload = _json_get(url)
    except RuntimeError:
        return None
    current = payload.get("current")
    if not current:
        return None
    return {
        "latitude": payload.get("latitude", latitude),
        "longitude": payload.get("longitude", longitude),
        "timezone": payload.get("timezone", timezone),
        "source": "current",
        "time": current.get("time"),
        "temperature_2m": current.get("temperature_2m"),
        "relative_humidity_2m": current.get("relative_humidity_2m"),
        "apparent_temperature": current.get("apparent_temperature"),
        "precipitation": current.get("precipitation"),
        "weather_code": current.get("weather_code"),
        "wind_speed_10m": current.get("wind_speed_10m"),
    }


def get_weather_for_timestamp(
    latitude: float,
    longitude: float,
    target_iso_time: str,
    timezone: str = "auto",
) -> Optional[dict]:
    """
    Fetch weather nearest to a target alert timestamp.
    Uses Open-Meteo archive hourly data for accurate historical context.
    """
    if latitude is None or longitude is None:
        return None

    if not target_iso_time:
        return get_current_weather(latitude=latitude, longitude=longitude, timezone=timezone)

    try:
        dt = datetime.fromisoformat(target_iso_time.replace("Z", "+00:00"))
    except ValueError:
        return get_current_weather(latitude=latitude, longitude=longitude, timezone=timezone)

    target_date = dt.date().isoformat()
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": target_date,
        "end_date": target_date,
        "hourly": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m",
        "timezone": timezone or "auto",
    }
    url = f"{ARCHIVE_API}?{urlparse.urlencode(params)}"
    try:
        payload = _json_get(url)
    except RuntimeError:
        return get_current_weather(latitude=latitude, longitude=longitude, timezone=timezone)
    hourly = payload.get("hourly") or {}
    hourly_times = hourly.get("time") or []
    if not hourly_times:
        return get_current_weather(latitude=latitude, longitude=longitude, timezone=timezone)

    # Pick the nearest hourly sample to target timestamp.
    nearest_index = 0
    smallest_delta = None
    for idx, time_str in enumerate(hourly_times):
        try:
            sample_dt = datetime.fromisoformat(time_str)
            delta = abs((sample_dt - dt.replace(tzinfo=None)).total_seconds())
        except Exception:
            continue
        if smallest_delta is None or delta < smallest_delta:
            smallest_delta = delta
            nearest_index = idx

    def _val(key: str):
        arr = hourly.get(key) or []
        return arr[nearest_index] if nearest_index < len(arr) else None

    return {
        "latitude": payload.get("latitude", latitude),
        "longitude": payload.get("longitude", longitude),
        "timezone": payload.get("timezone", timezone),
        "source": "archive_hourly",
        "target_time": target_iso_time,
        "matched_time": hourly_times[nearest_index],
        "temperature_2m": _val("temperature_2m"),
        "relative_humidity_2m": _val("relative_humidity_2m"),
        "apparent_temperature": _val("apparent_temperature"),
        "precipitation": _val("precipitation"),
        "weather_code": _val("weather_code"),
        "wind_speed_10m": _val("wind_speed_10m"),
    }
