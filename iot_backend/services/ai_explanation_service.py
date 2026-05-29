"""AI explanation service powered by Gemini."""

from __future__ import annotations

import json
import re
from typing import Iterable
from urllib import parse as urlparse
from urllib import request as urlrequest
from urllib.error import URLError, HTTPError

from iot_backend.config import GEMINI_API_KEY, GEMINI_MODEL


def _extract_retry_delay_seconds(raw_error_detail: str) -> int | None:
    """Extract retry delay in seconds from Gemini error payload text."""
    if not raw_error_detail:
        return None
    match = re.search(r'"retryDelay"\s*:\s*"(\d+)s"', raw_error_detail)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def _extract_text(payload: dict) -> str:
    candidates = payload.get("candidates") or []
    if not candidates:
        return ""
    content = candidates[0].get("content") or {}
    parts = content.get("parts") or []
    texts = [p.get("text", "") for p in parts if isinstance(p, dict)]
    return "\n".join([t for t in texts if t]).strip()


def _contains_vietnamese_diacritics(text: str) -> bool:
    return bool(re.search(r"[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]", text.lower()))


def _http_post_json(url: str, payload: dict, timeout: int = 20) -> dict:
    req = urlrequest.Request(
        url,
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload).encode("utf-8"),
    )
    with urlrequest.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _http_get_json(url: str, timeout: int = 12) -> dict:
    req = urlrequest.Request(url, method="GET")
    with urlrequest.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _normalize_model_name(model_name: str) -> str:
    normalized = (model_name or "").strip()
    if normalized.startswith("models/"):
        normalized = normalized[len("models/") :]
    return normalized


def _candidate_models() -> list[str]:
    """Return a deduplicated list of model names to try for generateContent."""
    defaults = [
        "gemini-3.1-flash-lite",
        "gemini-2.0-flash",
        "gemini-2.5-flash",
        "gemini-1.5-flash",
        "gemini-1.5-flash-001",
    ]
    candidates: list[str] = []
    for name in [_normalize_model_name(GEMINI_MODEL), *defaults]:
        if name and name not in candidates:
            candidates.append(name)
    return candidates


def _discover_models_from_api() -> list[str]:
    """Ask Gemini API for available models and return those supporting generateContent."""
    api_key = urlparse.quote(GEMINI_API_KEY)
    for version in ("v1", "v1beta"):
        url = f"https://generativelanguage.googleapis.com/{version}/models?key={api_key}"
        try:
            payload = _http_get_json(url)
            models = payload.get("models") or []
            available: list[str] = []
            for model in models:
                name = _normalize_model_name(model.get("name", ""))
                methods: Iterable[str] = model.get("supportedGenerationMethods") or []
                if name and "generateContent" in methods:
                    available.append(name)
            if available:
                return available
        except Exception:
            continue
    return []


def explain_alert_with_gemini(alert_context: dict) -> dict:
    """Generate human-friendly explanation for a sensor alert."""
    if not GEMINI_API_KEY:
        return {
            "success": False,
            "message": "GEMINI_API_KEY is not configured",
            "explanation": "",
            "error_code": "CONFIG_MISSING",
            "error_detail": "GEMINI_API_KEY is empty or missing in environment.",
        }

    system_instruction = (
        "You are an IoT operations assistant. Explain alerts in concise, practical language. "
        "Only use provided facts. Do not invent data."
    )
    facts_json = json.dumps(alert_context, ensure_ascii=False)
    user_prompt = (
        "Phân tích cảnh báo IoT sau và trả lời bằng tiếng Việt CÓ DẤU với ĐÚNG 3 mục, không thiếu mục nào.\n"
        "QUY TẮC BẮT BUỘC:\n"
        "- BẮT BUỘC viết tiếng Việt có dấu tự nhiên, không dùng kiểu không dấu.\n"
        "- Nếu device.environment_type = 'indoor': KHÔNG được suy luận theo thời gian/ngày-đêm/thời tiết.\n"
        "- Nếu device.environment_type = 'outdoor': PHẢI sử dụng alert_time và weather (nếu có) đúng theo thời điểm cảnh báo.\n"
        "- Không được dùng thời gian hiện tại để thay thế cho thời điểm cảnh báo.\n"
        "Mẫu bắt buộc:\n"
        "1) Nguyên nhân khả dĩ: ...\n"
        "2) Ảnh hưởng: ...\n"
        "3) Hành động đề xuất ngay: ...\n"
        "Mỗi mục tối đa 2 câu, ngắn gọn, thực tế, không chèn markdown.\n\n"
        f"FACTS JSON:\n{facts_json}"
    )
    req_payload_base = {
        "system_instruction": {"parts": [{"text": system_instruction}]},
        "contents": [{"parts": [{"text": user_prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 800},
    }

    model_pool = _candidate_models()
    api_discovered = _discover_models_from_api()
    for name in api_discovered:
        if name not in model_pool:
            model_pool.append(name)

    last_error = ""
    api_key = urlparse.quote(GEMINI_API_KEY)
    for version in ("v1", "v1beta"):
        for model_name in model_pool:
            url = (
                f"https://generativelanguage.googleapis.com/{version}/models/"
                f"{urlparse.quote(model_name, safe='')}:generateContent?key={api_key}"
            )
            try:
                payload = _http_post_json(url, req_payload_base, timeout=20)
                explanation = _extract_text(payload)
                # If the model output is cut/too short, do one stricter retry.
                has_full_3_sections = all(tag in explanation for tag in ("1)", "2)", "3)"))
                has_vietnamese_diacritics = _contains_vietnamese_diacritics(explanation)
                if explanation and (not has_full_3_sections or not has_vietnamese_diacritics):
                    retry_payload = {
                        "system_instruction": {"parts": [{"text": system_instruction}]},
                        "contents": [{
                            "parts": [{
                                "text": (
                                    "Trả lời lại bằng tiếng Việt CÓ DẤU với ĐÚNG 3 dòng, đầy đủ 3 mục 1) 2) 3), "
                                    "không được bỏ sót, mỗi dòng tối đa 1 câu.\n\n"
                                    f"FACTS JSON:\n{facts_json}"
                                )
                            }]
                        }],
                        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 450},
                    }
                    retry_result = _http_post_json(url, retry_payload, timeout=20)
                    retry_text = _extract_text(retry_result)
                    if retry_text:
                        explanation = retry_text
                        has_full_3_sections = all(tag in explanation for tag in ("1)", "2)", "3)"))
                        has_vietnamese_diacritics = _contains_vietnamese_diacritics(explanation)

                if explanation and has_full_3_sections and has_vietnamese_diacritics:
                    return {"success": True, "message": "ok", "explanation": explanation}
                last_error = f"Empty explanation from model {model_name}"
            except HTTPError as exc:
                try:
                    detail = exc.read().decode("utf-8")
                except Exception:
                    detail = str(exc)
                if exc.code in (404, 400):
                    last_error = f"{version}/{model_name}: {detail}"
                    continue
                if exc.code == 429:
                    retry_seconds = _extract_retry_delay_seconds(detail)
                    retry_hint = (
                        f" Thu lai sau {retry_seconds} giay."
                        if retry_seconds is not None
                        else ""
                    )
                    return {
                        "success": False,
                        "message": f"Vuot quota Gemini (HTTP 429).{retry_hint}",
                        "explanation": "",
                        "error_code": "HTTP_429_QUOTA_EXCEEDED",
                        "error_detail": detail,
                        "retry_after_seconds": retry_seconds,
                    }
                return {
                    "success": False,
                    "message": f"Dich vu AI tam thoi khong kha dung (HTTP {exc.code}). Vui long thu lai sau.",
                    "explanation": "",
                    "error_code": f"HTTP_{exc.code}",
                    "error_detail": detail,
                }
            except (URLError, TimeoutError, json.JSONDecodeError) as exc:
                last_error = str(exc)
                continue

    return {
        "success": False,
        "message": (
            "Khong tim thay model Gemini phu hop hoac cau hinh API key/model chua dung. "
            f"Chi tiet ky thuat: {last_error or 'unknown'}"
        ),
        "explanation": "",
        "error_code": "MODEL_OR_CONFIG_ERROR",
        "error_detail": last_error or "unknown",
    }
