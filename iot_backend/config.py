"""Application configuration"""

import os
from datetime import timedelta
from urllib.parse import quote_plus


def _load_dotenv():
    here_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(here_dir)
    candidates = [
        os.path.join(root_dir, ".env"),
        os.path.join(here_dir, ".env"),
    ]

    env_path = next((p for p in candidates if os.path.exists(p)), None)
    if not env_path:
        return

    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'").strip('"')
            os.environ.setdefault(key, value)


_load_dotenv()

# JWT Configuration
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-change-this-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Notification configuration
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
GMAIL_ADDRESS = os.getenv("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")

# AI explanation configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite")

# MQTT configuration
MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "iot-backend")
MQTT_SENSOR_TOPIC = os.getenv("MQTT_SENSOR_TOPIC", os.getenv("MQTT_TOPIC", "sensors/+/data"))
MQTT_COMMAND_TOPIC_PREFIX = os.getenv("MQTT_COMMAND_TOPIC_PREFIX", "ptdl/devices")
MQTT_WIFI_LIST_TOPIC = os.getenv("MQTT_WIFI_LIST_TOPIC", f"{MQTT_COMMAND_TOPIC_PREFIX}/+/wifi-list")
MQTT_DEVICE_STATE_TOPIC = os.getenv("MQTT_DEVICE_STATE_TOPIC", f"{MQTT_COMMAND_TOPIC_PREFIX}/+/state")

# Databricks Lakehouse integration.
# `DATABRICKS_ENABLED` controls Databricks connectivity for services.
# `DATABRICKS_DIRECT_WRITE_ENABLED` controls whether iot_backend writes Bronze
# directly during ingestion. Keep it false for scheduled PostgreSQL sync.
DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME", "")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG", "iot_cloud")
DATABRICKS_SCHEMA = os.getenv("DATABRICKS_SCHEMA", "sensor_analytics")
DATABRICKS_BRONZE_TABLE = os.getenv("DATABRICKS_BRONZE_TABLE", os.getenv("DATABRICKS_TABLE", "bronze_sensor_readings"))
DATABRICKS_ENABLED = os.getenv("DATABRICKS_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
DATABRICKS_DIRECT_WRITE_ENABLED = (
    os.getenv("DATABRICKS_DIRECT_WRITE_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
)

# Device automation thresholds
FAN_ON_TEMP = float(os.getenv("FAN_ON_TEMP", "32"))
LAMP_ON_TEMP = float(os.getenv("LAMP_ON_TEMP", "25"))
FOG_ON_HUMIDITY = float(os.getenv("FOG_ON_HUMIDITY", "60"))
FOG_OFF_HUMIDITY = float(os.getenv("FOG_OFF_HUMIDITY", "75"))


def get_cors_origins() -> list[str]:
    """Return CORS origins from env. Supports comma-separated values."""
    raw = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000")
    origins = [item.strip() for item in raw.split(",") if item.strip()]
    return origins or ["http://localhost:3000"]


def get_database_url() -> str:
    """Build PostgreSQL database URL from environment variables."""
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url

    db_host = os.getenv("DB_HOST")
    db_database = os.getenv("DB_DATABASE")
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")

    if not all([db_host, db_database, db_username, db_password]):
        raise RuntimeError(
            "Missing PostgreSQL settings. Required: DATABASE_URL or DB_HOST, DB_DATABASE, DB_USERNAME, DB_PASSWORD."
        )

    db_port = os.getenv("DB_PORT", "5432")
    username_enc = quote_plus(db_username)
    password_enc = quote_plus(db_password)
    return f"postgresql+psycopg2://{username_enc}:{password_enc}@{db_host}:{db_port}/{db_database}"
