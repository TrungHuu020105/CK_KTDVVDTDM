"""API routes module."""
from . import routes_auth
from . import routes_admin
from . import routes_chat
from . import routes_iot_proxy
from . import routes_metrics
from . import routes_alerts
from . import routes_websocket
from . import routes_iot_devices
from . import routes_admin_iot
from . import routes_sensors

__all__ = [
    "routes_auth",
    "routes_admin",
    "routes_chat",
    "routes_iot_proxy",
    "routes_metrics",
    "routes_alerts",
    "routes_websocket",
    "routes_iot_devices",
    "routes_admin_iot",
    "routes_sensors",
]
