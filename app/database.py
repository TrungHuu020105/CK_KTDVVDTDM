"""Database configuration and session management (PostgreSQL)."""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from app.config import get_database_url


DATABASE_URL = get_database_url()

def _engine_kwargs_for(url: str) -> dict:
    if url.startswith("sqlite"):
        return {
            "echo": False,
            "connect_args": {"check_same_thread": False},
        }

    return {
        "echo": False,  # Set to True for SQL query debugging
        "pool_pre_ping": True,
        # Tăng pool_size và max_overflow để xử lý nhiều request đồng thời
        # (WebSocket connections + HTTP API calls cùng lúc)
        "pool_size": int(os.getenv("DB_POOL_SIZE", "5")),
        "max_overflow": int(os.getenv("DB_MAX_OVERFLOW", "10")),
        "pool_timeout": int(os.getenv("DB_POOL_TIMEOUT", "30")),
        "pool_recycle": int(os.getenv("DB_POOL_RECYCLE", "300")),
        "pool_use_lifo": True,
        # Avoid hanging forever when network/db has intermittent issues.
        "connect_args": {
            "connect_timeout": 10,
            # Postgres session-level timeouts to fail fast on DDL locks.
            "options": "-c statement_timeout=30000 -c lock_timeout=15000",
        },
    }


def _create_app_engine(url: str):
    return create_engine(url, **_engine_kwargs_for(url))

# Create engine
engine = _create_app_engine(DATABASE_URL)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for models
Base = declarative_base()


def switch_to_sqlite_fallback() -> str:
    """Rebind the app to a local SQLite DB when the shared Postgres is full."""
    global DATABASE_URL, engine
    fallback_url = os.getenv(
        "SQLITE_FALLBACK_DATABASE_URL",
        f"sqlite:///{os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'local_dev.db'))}",
    )
    try:
        engine.dispose()
    except Exception:
        pass
    DATABASE_URL = fallback_url
    engine = _create_app_engine(DATABASE_URL)
    SessionLocal.configure(bind=engine)
    print(f"[WARN] [DB] Using local SQLite fallback: {fallback_url}")
    return fallback_url


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
    # Chat read-tracking is required by current API responses.
    _ensure_chat_columns()
    # Required for IoT device UX/alerts (safe, idempotent columns).
    _ensure_iot_device_required_columns()
    _ensure_alert_required_columns()
    _backfill_iot_device_threshold_columns()
    # Migration bắt buộc: chuyển unique constraint trên source sang (source, device_type)
    _migrate_iot_device_source_constraint()
    if _is_schema_auto_migrate_enabled():
        _run_schema_evolution()


def _ensure_iot_device_required_columns():
    """Ensure required columns exist on iot_devices for current features.

    This runs unconditionally but is designed to be low-risk:
    - Only ADD COLUMN when missing
    - Dialect-aware to avoid SQLite incompatibilities
    """
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

    dialect = engine.dialect.name
    with engine.begin() as conn:
        for col, sql_type in required.items():
            if col in existing_cols:
                continue
            try:
                if dialect == "sqlite":
                    conn.exec_driver_sql(f"ALTER TABLE iot_devices ADD COLUMN {col} {sql_type}")
                else:
                    conn.exec_driver_sql(
                        f"ALTER TABLE iot_devices ADD COLUMN IF NOT EXISTS {col} {sql_type}"
                    )
            except Exception:
                # Ignore if not supported or concurrently added.
                pass


def _ensure_alert_required_columns():
    """Ensure optional alert metadata columns exist for IoT alert UI/debugging."""
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
    }

    dialect = engine.dialect.name
    with engine.begin() as conn:
        for col, sql_type in required.items():
            if col in existing_cols:
                continue
            try:
                if dialect == "sqlite":
                    conn.exec_driver_sql(f"ALTER TABLE alerts ADD COLUMN {col} {sql_type}")
                else:
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
    """
    Migration bắt buộc: chuyển IoTDevice từ unique(source) sang unique(source, device_type).
    - Bỏ index/constraint cũ trên source
    - Thêm composite unique constraint (source, device_type)
    Chạy an toàn nhiều lần (idempotent).
    """
    from sqlalchemy import inspect, text
    with engine.begin() as conn:
        inspector = inspect(conn)
        try:
            constraints = inspector.get_unique_constraints('iot_devices')
        except Exception:
            constraints = []

        existing_names = {c['name'] for c in constraints}

        # Bước 1: Bỏ unique constraint cũ trên chỉ source (nếu còn tồn tại)
        old_constraint_names = [
            n for n in existing_names
            if n and 'source' in n.lower() and 'type' not in n.lower()
        ]
        for name in old_constraint_names:
            try:
                conn.exec_driver_sql(f'ALTER TABLE iot_devices DROP CONSTRAINT IF EXISTS "{name}"')
                print(f"[DB] Dropped old constraint: {name}")
            except Exception as e:
                print(f"[DB] Could not drop constraint {name}: {e}")

        # Bước 2: Bỏ index unique cũ trên source (PostgreSQL có thể có tên khác nhau)
        try:
            indexes = inspector.get_indexes('iot_devices')
            for idx in indexes:
                cols = idx.get('column_names', [])
                if cols == ['source'] and idx.get('unique'):
                    idx_name = idx['name']
                    try:
                        conn.exec_driver_sql(f'DROP INDEX IF EXISTS "{idx_name}"')
                        print(f"[DB] Dropped old unique index on source: {idx_name}")
                    except Exception as e:
                        print(f"[DB] Could not drop index {idx_name}: {e}")
        except Exception:
            pass

        # Bước 3: Thêm composite unique constraint mới nếu chưa có
        if 'uq_iot_device_source_type' not in existing_names:
            try:
                if engine.dialect.name == "sqlite":
                    conn.exec_driver_sql(
                        'CREATE UNIQUE INDEX IF NOT EXISTS uq_iot_device_source_type '
                        'ON iot_devices (source, device_type)'
                    )
                else:
                    conn.exec_driver_sql(
                        'ALTER TABLE iot_devices ADD CONSTRAINT uq_iot_device_source_type '
                        'UNIQUE (source, device_type)'
                    )
                print("[DB] Added composite unique constraint: (source, device_type)")
            except Exception as e:
                print(f"[DB] Could not add composite constraint (may already exist): {e}")


def _ensure_chat_columns():
    """Best-effort schema evolution for chat_conversations read-tracking columns."""
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
