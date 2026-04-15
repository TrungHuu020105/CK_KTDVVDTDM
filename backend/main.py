"""FastAPI application entry point"""

import asyncio
import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .database import init_db
from .api import routes_metrics, routes_alerts, routes_websocket, routes_iot_devices
from .api.routes_websocket import manager
from .kafka_realtime import KafkaRealtimeBridge


def _load_root_env() -> None:
    """Load .env from project root if present."""
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


_load_root_env()

REALTIME_SOURCE = (os.getenv("REALTIME_SOURCE", "kafka") or "kafka").strip().lower()
if REALTIME_SOURCE != "kafka":
    print(f"[WARN] [Startup] REALTIME_SOURCE={REALTIME_SOURCE} is not supported in this repo mode. Forcing kafka.")
    REALTIME_SOURCE = "kafka"

kafka_bridge = None

# Initialize database on startup (disabled - using Databricks instead)
init_db()

# Create FastAPI app
app = FastAPI(
    title="Real-Time Metrics Processing System",
    description="Backend for receiving, storing, and processing IoT metrics",
    version="2.1.0"
)

# Add CORS middleware for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000", "http://localhost:5173", "*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "*"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=600,
)

# Include routes
app.include_router(routes_metrics.router)
app.include_router(routes_alerts.router)
app.include_router(routes_iot_devices.router)
app.include_router(routes_websocket.router, prefix="/api", tags=["websocket"])


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Welcome to Real-Time Metrics Processing System API",
        "docs": "/docs",
        "health": "/api/health"
    }


@app.get("/debug/cors-test")
async def cors_test():
    """Test CORS - no auth required"""
    return {"message": "CORS working!", "timestamp": str(__import__('datetime').datetime.now())}


@app.on_event("startup")
async def startup_event():
    """Initialize on server startup."""
    global kafka_bridge

    print("[OK] [Startup] IoT-only mode enabled")
    print("[OK] [Startup] Authentication routes are disabled")
    print("[OK] [Startup] Realtime source: kafka-only")

    if kafka_bridge is None or not kafka_bridge.is_running:
        loop = asyncio.get_running_loop()
        kafka_bridge = KafkaRealtimeBridge.from_env(manager=manager, loop=loop)
        kafka_bridge.start()


@app.on_event("shutdown")
async def shutdown_event():
    """Stop background tasks on server shutdown."""
    global kafka_bridge

    if kafka_bridge is not None:
        kafka_bridge.stop(timeout_seconds=5)
        kafka_bridge = None


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
