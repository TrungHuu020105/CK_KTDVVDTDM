"""Telegram notification service."""

import json
from urllib import request, error

from iot_backend.config import TELEGRAM_BOT_TOKEN


def send_telegram_message(chat_id: str, message: str) -> tuple[bool, str]:
    """Send a Telegram message via Bot API."""
    if not TELEGRAM_BOT_TOKEN:
        return False, "TELEGRAM_BOT_TOKEN is not configured"
    if not chat_id:
        return False, "chat_id is required"

    payload = json.dumps(
        {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
    ).encode("utf-8")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    req = request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            if not body.get("ok"):
                return False, f"Telegram API error: {body}"
            return True, "sent"
    except error.HTTPError as exc:
        return False, f"HTTP {exc.code}"
    except Exception as exc:
        return False, str(exc)

