"""Thin WebSocket gateway that bridges clients to iot_backend."""

from __future__ import annotations

import asyncio
from urllib.parse import quote, urlsplit

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import websockets

from app.config import IOT_BACKEND_URL


router = APIRouter(tags=["websocket"])


def _backend_ws_url(client_id: str, token: str | None) -> str:
    parsed = urlsplit(IOT_BACKEND_URL)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    path = parsed.path.rstrip("/")
    if path:
        path = f"{path}/api/ws/{quote(client_id, safe='')}"
    else:
        path = f"/api/ws/{quote(client_id, safe='')}"
    base = f"{scheme}://{parsed.netloc}{path}"
    if token:
        return f"{base}?token={quote(token, safe='')}"
    return base


@router.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    token = websocket.query_params.get("token")
    backend_url = _backend_ws_url(client_id, token)
    await websocket.accept()

    try:
        async with websockets.connect(backend_url, ping_interval=20, ping_timeout=20) as backend_ws:
            async def client_to_backend():
                while True:
                    message = await websocket.receive_text()
                    await backend_ws.send(message)

            async def backend_to_client():
                async for message in backend_ws:
                    await websocket.send_text(message)

            tasks = [
                asyncio.create_task(client_to_backend()),
                asyncio.create_task(backend_to_client()),
            ]
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            for task in done:
                exc = task.exception()
                if exc and not isinstance(exc, WebSocketDisconnect):
                    raise exc
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        print(f"[WS GATEWAY] Error with client {client_id}: {exc}")
        try:
            await websocket.close()
        except Exception:
            pass
