"""Run this first in Databricks to verify widgets, PostgreSQL JDBC, and Delta namespace."""

import os
from pathlib import Path
from urllib.parse import urlparse


DEFAULTS = {
    "POSTGRES_JDBC_URL": "",
    "DATABASE_URL": "",
    "DB_HOST": "20.214.247.102",
    "DB_PORT": "5432",
    "DB_DATABASE": "rtmps_db",
    "DB_USERNAME": "rtmps_user",
    "DB_PASSWORD": "123456",
    "DB_PASSWORD_SECRET_SCOPE": "",
    "DB_PASSWORD_SECRET_KEY": "DB_PASSWORD",
    "DATABRICKS_CATALOG": "dtdm",
    "DATABRICKS_SCHEMA": "metrics_app_streaming",
    "POSTGRES_SENSOR_READINGS_TABLE": "sensor_readings",
    "POSTGRES_TABLE": "sensor_readings",
}


def load_local_env():
    candidates = []
    try:
        candidates.append(Path(__file__).resolve().parents[1] / ".env")
    except Exception:
        pass
    candidates.append(Path.cwd() / ".env")

    for env_path in candidates:
        if not env_path.exists():
            continue
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
        return


load_local_env()


def _widget_value(name):
    try:
        value = dbutils.widgets.get(name)  # noqa: F821
        value = value.strip()
        if not value or value.lower() in {"none", "null"} or value.startswith("<"):
            return None
        return value
    except Exception:
        return None


def _setting(name, default=None):
    return os.getenv(name) or _widget_value(name) or default or DEFAULTS.get(name)


def _ensure_widgets():
    for name, default in DEFAULTS.items():
        try:
            current = _widget_value(name)
            if current is None:
                try:
                    dbutils.widgets.remove(name)  # noqa: F821
                except Exception:
                    pass
            dbutils.widgets.text(name, _setting(name, default) or "")  # noqa: F821
        except Exception:
            pass


def _env_or_secret(name, default=None):
    value = _setting(name)
    if value:
        return value
    scope = _setting(f"{name}_SECRET_SCOPE") or _setting("DATABRICKS_SECRET_SCOPE")
    key = _setting(f"{name}_SECRET_KEY") or name
    if not scope:
        return None
    try:
        return dbutils.secrets.get(scope=scope, key=key)  # noqa: F821
    except Exception:
        return default


def _missing_config_message(missing):
    example = """
Fill these notebook widgets or Job parameters:
  DB_HOST=20.214.247.102
  DB_PORT=5432
  DB_DATABASE=rtmps_db
  DB_USERNAME=rtmps_user
  DB_PASSWORD=<use Databricks secret or secure Job parameter>
  DATABRICKS_CATALOG=dtdm
  DATABRICKS_SCHEMA=metrics_app_streaming
  POSTGRES_SENSOR_READINGS_TABLE=sensor_readings

Alternative: set POSTGRES_JDBC_URL or DATABASE_URL instead of DB_HOST/DB_PORT/DB_DATABASE.
"""
    return f"Missing required PostgreSQL config: {', '.join(missing)}\n{example}"


def _jdbc_url():
    postgres_jdbc_url = _setting("POSTGRES_JDBC_URL")
    if postgres_jdbc_url:
        return postgres_jdbc_url

    database_url = _setting("DATABASE_URL")
    if database_url:
        parsed = urlparse(database_url.replace("postgresql+psycopg2://", "postgresql://", 1))
        if parsed.scheme not in {"postgresql", "postgres"} or not parsed.hostname or not parsed.path:
            raise RuntimeError("DATABASE_URL must look like postgresql://user:password@host:5432/database")
        return f"jdbc:postgresql://{parsed.hostname}:{parsed.port or 5432}{parsed.path}"

    host = _setting("DB_HOST")
    port = _setting("DB_PORT")
    database = _setting("DB_DATABASE")
    missing = [name for name, value in {"DB_HOST": host, "DB_PORT": port, "DB_DATABASE": database}.items() if not value]
    if missing:
        raise RuntimeError(_missing_config_message(missing))
    return f"jdbc:postgresql://{host}:{port}/{database}"


def _jdbc_credentials():
    user = _setting("POSTGRES_USER") or _setting("DB_USERNAME")
    password = _setting("POSTGRES_PASSWORD") or _setting("DB_PASSWORD") or _env_or_secret("DB_PASSWORD")

    database_url = _setting("DATABASE_URL")
    if database_url and (not user or not password):
        parsed = urlparse(database_url.replace("postgresql+psycopg2://", "postgresql://", 1))
        user = user or parsed.username
        password = password or parsed.password

    missing = []
    if not user:
        missing.append("DB_USERNAME or POSTGRES_USER")
    if not password:
        missing.append("DB_PASSWORD, POSTGRES_PASSWORD, or DB_PASSWORD secret")
    if missing:
        raise RuntimeError(_missing_config_message(missing))
    return user, password


_ensure_widgets()

catalog = _setting("DATABRICKS_CATALOG", "dtdm")
schema = _setting("DATABRICKS_SCHEMA", "metrics_app_streaming")
jdbc_url = _jdbc_url()
user, password = _jdbc_credentials()
source_table = _setting("POSTGRES_TABLE") or _setting("POSTGRES_SENSOR_READINGS_TABLE", "sensor_readings")

print("Catalog/schema:", f"{catalog}.{schema}")
print("JDBC URL:", jdbc_url)
print("DB user:", user)
print("Password configured:", bool(password))

if not all([jdbc_url, user, password]):
    raise RuntimeError("Missing DB config. Fill notebook widgets or configure env/secrets first.")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

test_query = """
SELECT
  source_type,
  COUNT(*) AS row_count,
  MIN(event_ts) AS first_event_ts,
  MAX(event_ts) AS last_event_ts
FROM {source_table}
WHERE temperature IS NOT NULL OR humidity IS NOT NULL
GROUP BY source_type
ORDER BY source_type
"""
test_query = test_query.format(source_table=source_table)

df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("query", test_query)
    .option("user", user)
    .option("password", password)
    .option("driver", "org.postgresql.Driver")
    .load()
)

display(df)
print("Connection OK. If the table above has rows, run 01 then 02-07.")
