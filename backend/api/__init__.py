"""API routes module"""
from . import routes_metrics
from . import routes_alerts
from . import routes_iot_devices
from . import routes_websocket

__all__ = [
    "routes_metrics",
    "routes_alerts", 
    "routes_iot_devices",
    "routes_websocket"
]
