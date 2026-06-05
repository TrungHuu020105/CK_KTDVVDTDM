"""Database configuration and session management (PostgreSQL)."""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from iot_backend.config import get_database_url


DATABASE_URL = get_database_url()

engine_kwargs = {
    "echo": False,  # Set to True for SQL query debugging
    "pool_pre_ping": True,
    # Alerts can open a second DB session during notification dispatch while the
    # ingest session is still active. Keep defaults modest, but not so small
    # that a single alert burst starves the pool immediately.
    "pool_size": int(os.getenv("DB_POOL_SIZE", "5")),
    "max_overflow": int(os.getenv("DB_MAX_OVERFLOW", "10")),
    "pool_timeout": int(os.getenv("DB_POOL_TIMEOUT", "30")),
    "pool_recycle": int(os.getenv("DB_POOL_RECYCLE", "180")),
    "pool_use_lifo": True,
    # Avoid hanging forever when network/db has intermittent issues.
    "connect_args": {
        "connect_timeout": 5,
        # Postgres session-level timeouts to fail fast on DDL locks.
        # Keep statement timeout for safety, but relax lock timeout to reduce transient lock errors.
        "options": "-c statement_timeout=15000 -c lock_timeout=15000",
    },
}

# Create engine
engine = create_engine(DATABASE_URL, **engine_kwargs)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for models
Base = declarative_base()


def get_db():
    """Dependency for getting database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Initialize database tables"""
    Base.metadata.create_all(bind=engine)
    _ensure_chat_columns()
    _ensure_iot_device_required_columns()
    _ensure_alert_required_columns()
    _backfill_iot_device_threshold_columns()
    _migrate_iot_device_source_constraint()
    if _is_schema_auto_migrate_enabled():
        _run_schema_evolution()


def _ensure_iot_device_required_columns():
    """Ensure all iot_devices columns required by active runtime code exist."""
    from sqlalchemy import inspect

    inspector = inspect(engine)
    try:
        existing_cols = {c["name"] for c in inspector.get_columns("iot_devices")}
    except Exception:
        existing_cols = set()

    required = {
        "unit": "VARCHAR(50)",
        "source_type": "VARCHAR(30) DEFAULT 'physical_iot'",
        "capabilities": "VARCHAR(100) DEFAULT 'temperature,humidity'",
        "location_province": "VARCHAR(100)",
        "min_threshold": "FLOAT",
        "max_threshold": "FLOAT",
        "temperature_min_threshold": "FLOAT",
        "temperature_max_threshold": "FLOAT",
        "humidity_min_threshold": "FLOAT",
        "humidity_max_threshold": "FLOAT",
        "created_by": "INTEGER",
        "environment_type": "VARCHAR(20) DEFAULT 'indoor'",
        "location_query": "VARCHAR(255)",
        "latitude": "FLOAT",
        "longitude": "FLOAT",
        "timezone_name": "VARCHAR(64)",
        "task_description": "VARCHAR(500)",
        "priority_level": "VARCHAR(20)",
        "action_hint": "VARCHAR(500)",
    }

    with engine.begin() as conn:
        for col, sql_type in required.items():
            if col in existing_cols:
                continue
            try:
                conn.exec_driver_sql(
                    f"ALTER TABLE iot_devices ADD COLUMN IF NOT EXISTS {col} {sql_type}"
                )
            except Exception:
                pass


def _ensure_alert_required_columns():
    """Ensure alert metadata columns exist for unified alert schema."""
    from sqlalchemy import inspect

    inspector = inspect(engine)
    try:
        existing_cols = {c["name"] for c in inspector.get_columns("alerts")}
    except Exception:
        existing_cols = set()

    required = {
        "device_id": "INTEGER",
        "device_name": "VARCHAR(100)",
        "unit": "VARCHAR(50)",
        "min_threshold": "FLOAT",
        "max_threshold": "FLOAT",
        "alert_origin": "VARCHAR(30) DEFAULT 'realtime'",
        "forecast_timestamp": "TIMESTAMP",
        "forecast_generated_at": "TIMESTAMP",
    }

    with engine.begin() as conn:
        for col, sql_type in required.items():
            if col in existing_cols:
                continue
            try:
                conn.exec_driver_sql(
                    f"ALTER TABLE alerts ADD COLUMN IF NOT EXISTS {col} {sql_type}"
                )
            except Exception:
                pass


def _backfill_iot_device_threshold_columns():
    """Best-effort backfill from legacy threshold columns into canonical min/max."""
    from sqlalchemy import inspect

    inspector = inspect(engine)
    try:
        existing_cols = {c["name"] for c in inspector.get_columns("iot_devices")}
    except Exception:
        existing_cols = set()

    if not {"min_threshold", "max_threshold", "lower_threshold", "upper_threshold"}.issubset(existing_cols):
        return

    with engine.begin() as conn:
        try:
            conn.exec_driver_sql(
                """
                UPDATE iot_devices
                SET
                    min_threshold = COALESCE(min_threshold, lower_threshold),
                    max_threshold = COALESCE(max_threshold, upper_threshold)
                WHERE
                    (min_threshold IS NULL AND lower_threshold IS NOT NULL)
                    OR (max_threshold IS NULL AND upper_threshold IS NOT NULL)
                """
            )
        except Exception:
            pass


def _is_schema_auto_migrate_enabled() -> bool:
    """
    Guard risky runtime DDL behind an explicit flag.
    Default OFF to avoid startup hangs from table locks.
    """
    return os.getenv("DB_AUTO_SCHEMA_MIGRATION", "false").strip().lower() in {"1", "true", "yes", "on"}


def _run_schema_evolution():
    """Best-effort runtime schema evolution (explicitly enabled only)."""
    _ensure_iot_device_columns()
    _cleanup_metric_columns()


def _ensure_iot_device_columns():
    """Best-effort schema evolution for existing iot_devices table."""
    alter_statements = [
        "ALTER TABLE iot_devices ADD COLUMN IF NOT EXISTS environment_type VARCHAR(20) DEFAULT 'indoor'",
        "ALTER TABLE iot_devices ADD COLUMN IF NOT EXISTS location_query VARCHAR(255)",
        "ALTER TABLE iot_devices ADD COLUMN IF NOT EXISTS latitude FLOAT",
        "ALTER TABLE iot_devices ADD COLUMN IF NOT EXISTS longitude FLOAT",
        "ALTER TABLE iot_devices ADD COLUMN IF NOT EXISTS timezone_name VARCHAR(64)",
        "ALTER TABLE iot_devices ADD COLUMN IF NOT EXISTS task_description VARCHAR(500)",
        "ALTER TABLE iot_devices ADD COLUMN IF NOT EXISTS priority_level VARCHAR(20)",
        "ALTER TABLE iot_devices ADD COLUMN IF NOT EXISTS action_hint VARCHAR(500)",
    ]

    with engine.begin() as conn:
        for sql in alter_statements:
            try:
                conn.exec_driver_sql(sql)
            except Exception:
                # Ignore if column already exists or dialect-specific limitation.
                pass


def _cleanup_metric_columns():
    """Best-effort cleanup for metrics table schema."""
    alter_statements = [
        "ALTER TABLE metrics DROP COLUMN IF EXISTS timezone_name",
    ]
    with engine.begin() as conn:
        for sql in alter_statements:
            try:
                conn.exec_driver_sql(sql)
            except Exception:
                pass


def _migrate_iot_device_source_constraint():
    """Ensure iot_devices uses a composite unique key on (source, device_type)."""
    from sqlalchemy import inspect

    with engine.begin() as conn:
        inspector = inspect(conn)
        try:
            constraints = inspector.get_unique_constraints("iot_devices")
        except Exception:
            constraints = []

        existing_names = {c["name"] for c in constraints}
        old_constraint_names = [
            name for name in existing_names
            if name and "source" in name.lower() and "type" not in name.lower()
        ]
        for name in old_constraint_names:
            try:
                conn.exec_driver_sql(f'ALTER TABLE iot_devices DROP CONSTRAINT IF EXISTS "{name}"')
            except Exception:
                pass

        try:
            indexes = inspector.get_indexes("iot_devices")
        except Exception:
            indexes = []
        for idx in indexes:
            if idx.get("column_names") == ["source"] and idx.get("unique"):
                try:
                    conn.exec_driver_sql(f'DROP INDEX IF EXISTS "{idx["name"]}"')
                except Exception:
                    pass

        if "uq_iot_device_source_type" not in existing_names:
            try:
                conn.exec_driver_sql(
                    "ALTER TABLE iot_devices ADD CONSTRAINT uq_iot_device_source_type "
                    "UNIQUE (source, device_type)"
                )
            except Exception:
                pass


def _ensure_chat_columns():
    """Ensure chat read cursor columns exist for unified chat schema."""
    alter_statements = [
        "ALTER TABLE chat_conversations ADD COLUMN IF NOT EXISTS last_read_by_user_at TIMESTAMP",
        "ALTER TABLE chat_conversations ADD COLUMN IF NOT EXISTS last_read_by_admin_at TIMESTAMP",
    ]
    with engine.begin() as conn:
        for sql in alter_statements:
            try:
                conn.exec_driver_sql(sql)
            except Exception:
                pass
