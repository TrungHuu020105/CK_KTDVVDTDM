"""Shared helpers for proxying IoT requests from the core app to iot_backend."""

from __future__ import annotations

import json
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

from fastapi import HTTPException, Request, status

from app.config import IOT_BACKEND_URL


def extract_bearer_token(request: Request | None) -> str | None:
    """Return the bearer token from the incoming request, if present."""
    if request is None:
        return None
    auth_header = request.headers.get("Authorization", "")
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()
    return None


def proxy_iot_backend(
    method: str,
    path: str,
    *,
    payload: dict | None = None,
    bearer_token: str | None = None,
    timeout: float = 20,
) -> dict:
    """Forward a JSON request to iot_backend and return the decoded JSON response."""
    url = f"{IOT_BACKEND_URL}{path}"
    headers = {"Content-Type": "application/json"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"

    body = None
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")

    req = urlrequest.Request(url, data=body, headers=headers, method=method.upper())

    try:
        with urlrequest.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {"status": "ok"}
    except HTTPError as exc:
        detail = f"IoT backend error HTTP {exc.code}"
        try:
            err = json.loads(exc.read().decode("utf-8"))
            if isinstance(err, dict) and err.get("detail"):
                detail = err["detail"]
        except Exception:
            pass
        if exc.code == 404:
            detail = "IoT backend route not found. Check IOT_BACKEND_URL and that iot_backend is running."
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=detail) from exc
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"IoT backend chưa chạy hoặc IOT_BACKEND_URL sai: {str(exc)}",
        ) from exc
