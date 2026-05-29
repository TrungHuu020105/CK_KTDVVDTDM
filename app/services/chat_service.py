"""Chat support service: bot replies and user data context summarization."""

from __future__ import annotations

import json
import unicodedata
from datetime import datetime, timedelta, timezone
from urllib import parse as urlparse
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

from sqlalchemy.orm import Session

from app.config import GEMINI_API_KEY, GEMINI_MODEL
from app.models import IoTDevice, Metric, Alert, User
from app import crud


def _normalize_model_name(model_name: str) -> str:
    normalized = (model_name or "").strip()
    if normalized.startswith("models/"):
        normalized = normalized[len("models/") :]
    return normalized


def _http_post_json(url: str, payload: dict, timeout: int = 20) -> dict:
    req = urlrequest.Request(
        url,
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload).encode("utf-8"),
    )
    with urlrequest.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _extract_text(payload: dict) -> str:
    candidates = payload.get("candidates") or []
    if not candidates:
        return ""
    content = candidates[0].get("content") or {}
    parts = content.get("parts") or []
    texts = [p.get("text", "") for p in parts if isinstance(p, dict)]
    return "\n".join([t for t in texts if t]).strip()


def _candidate_models() -> list[str]:
    defaults = ["gemini-2.0-flash", "gemini-2.5-flash", "gemini-1.5-flash"]
    candidates: list[str] = []
    for name in [_normalize_model_name(GEMINI_MODEL), *defaults]:
        if name and name not in candidates:
            candidates.append(name)
    return candidates


def _strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def _normalize_text(text: str) -> str:
    raw = (text or "").lower().strip()
    return _strip_accents(raw)


def _summarize_user_context(db: Session, user: User) -> dict:
    sources = crud.get_user_accessible_sources(db, user.id)
    limited_sources = sources[:6]

    devices = (
        db.query(IoTDevice)
        .filter(IoTDevice.user_id == user.id, IoTDevice.is_active == True)
        .order_by(IoTDevice.created_at.desc())
        .all()
    )

    latest_metrics = []
    if limited_sources:
        rows = (
            db.query(Metric)
            .filter(Metric.sensor_id.in_(limited_sources))
            .order_by(Metric.sensor_id.asc(), Metric.event_ts.desc())
            .all()
        )
        latest_by_source = {}
        for row in rows:
            if row.sensor_id in latest_by_source:
                continue
            latest_by_source[row.sensor_id] = row
            if len(latest_by_source) == len(limited_sources):
                break

        for source in limited_sources:
            row = latest_by_source.get(source)
            if not row:
                continue
            latest_metrics.append(
                {
                    "source": row.sensor_id,
                    "metric_type": row.metric_type,
                    "value": row.metric_value,
                    "unit": row.unit,
                    "event_ts": row.event_ts.isoformat() if row.event_ts else None,
                }
            )

    threshold_time = datetime.now(timezone(timedelta(hours=7))) - timedelta(hours=24)
    alerts_24h = (
        db.query(Alert)
        .filter(Alert.source.in_(sources), Alert.created_at >= threshold_time)
        .order_by(Alert.created_at.desc())
        .limit(20)
        .all()
    )

    return {
        "user": {"id": user.id, "username": user.username},
        "devices": [
            {
                "name": d.name,
                "type": d.device_type,
                "source": d.source,
                "location": d.location,
                "environment_type": d.environment_type,
                "active": d.is_active,
            }
            for d in devices
        ],
        "latest_metrics": latest_metrics,
        "alerts_last_24h_count": len(alerts_24h),
        "alerts_last_24h_samples": [
            {
                "metric_type": a.metric_type,
                "value": a.current_value,
                "threshold": a.threshold,
                "source": a.source,
                "created_at": a.created_at.isoformat() if a.created_at else None,
            }
            for a in alerts_24h[:5]
        ],
    }


def _fallback_reply(user_message: str, context: dict) -> str:
    normalized_message = _normalize_text(user_message)

    if "canh bao" in normalized_message or "alert" in normalized_message:
        return (
            f"Mình thấy tài khoản của bạn có {context['alerts_last_24h_count']} alert trong 24h qua. "
            "Bạn có thể nói rõ sensor nào để mình hướng dẫn xử lý chi tiết hơn."
        )
    if "sensor" in normalized_message or "thiet bi" in normalized_message:
        device_names = [d["name"] for d in context["devices"][:4]]
        if not device_names:
            return "Hiện bạn chưa có sensor hoạt động. Bạn có thể vào IoT Devices để tạo hoặc bật sensor."
        return f"Các sensor chính của bạn: {', '.join(device_names)}. Bạn muốn kiểm tra sensor nào trước?"
    return (
        "Mình đã nhận yêu cầu. Bạn có thể mô tả cụ thể sensor/metric (temperature, humidity...) "
        "để mình tư vấn sát dữ liệu hiện tại hơn."
    )


def generate_user_bot_reply(db: Session, user: User, user_message: str) -> str:
    context = _summarize_user_context(db, user)
    if not GEMINI_API_KEY:
        return _fallback_reply(user_message, context)

    system_instruction = (
        "Bạn là trợ lý IoT cho người dùng cuối. "
        "Chỉ dùng dữ liệu được cung cấp trong CONTEXT. "
        "Trả lời tiếng Việt, ngắn gọn, thực tế, có bước hành động."
    )
    prompt = (
        "CONTEXT JSON:\n"
        f"{json.dumps(context, ensure_ascii=False)}\n\n"
        "USER MESSAGE:\n"
        f"{user_message}\n\n"
        "Yêu cầu format:\n"
        "1) Nhận định nhanh\n"
        "2) Vì sao (bám dữ liệu)\n"
        "3) Việc nên làm ngay"
    )

    req_payload = {
        "system_instruction": {"parts": [{"text": system_instruction}]},
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.3, "maxOutputTokens": 600},
    }

    api_key = urlparse.quote(GEMINI_API_KEY)
    for version in ("v1", "v1beta"):
        for model_name in _candidate_models():
            url = (
                f"https://generativelanguage.googleapis.com/{version}/models/"
                f"{urlparse.quote(model_name, safe='')}:generateContent?key={api_key}"
            )
            try:
                payload = _http_post_json(url, req_payload, timeout=20)
                text = _extract_text(payload)
                if text:
                    return text
            except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
                continue

    return _fallback_reply(user_message, context)
