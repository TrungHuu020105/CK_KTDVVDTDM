"""
FastAPI Backend with WebSocket + Databricks Analytics
Consume dữ liệu từ Kafka (WebSocket) + Query từ Databricks (REST API)
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import json
import logging
from typing import Set, Optional, List
import aiokafka
import os
from dotenv import load_dotenv
import ssl
from datetime import datetime, timedelta
import pandas as pd

# Import Databricks client
from databricks_client import get_databricks_client

# Load environment variables from backend/.env (explicit path)
load_dotenv(
    os.path.join(os.path.dirname(__file__), ".env"),
    override=True,
    encoding="utf-8",
)
# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_bool(value, default=False):
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def parse_origins(value):
    if not value:
        return []
    return [origin.strip() for origin in value.split(",") if origin.strip()]

# Cấu hình
def env(name, default=None):
    v = os.getenv(name, default)
    return v.strip() if isinstance(v, str) else v

# Confluent Cloud Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = env("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = env("KAFKA_TOPIC", "iot-sensor-data")
KAFKA_SECURITY_PROTOCOL = env("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
KAFKA_SASL_MECHANISM = env("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_SASL_USERNAME = env("KAFKA_SASL_USERNAME")
KAFKA_SASL_PASSWORD = env("KAFKA_SASL_PASSWORD")

DEFAULT_CORS_ORIGINS = "http://localhost:5173,http://127.0.0.1:5173"
CORS_ORIGINS = parse_origins(os.getenv("CORS_ORIGINS", DEFAULT_CORS_ORIGINS))
CORS_ALLOW_CREDENTIALS = parse_bool(os.getenv("CORS_ALLOW_CREDENTIALS"), default=True)

if "*" in CORS_ORIGINS and CORS_ALLOW_CREDENTIALS:
    logger.warning(
        "CORS_ORIGINS contains '*' with credentials enabled. Credentials have been disabled for safety."
    )
    CORS_ALLOW_CREDENTIALS = False

METRIC_UNITS = {
    "temperature": "°C",
    "humidity": "%",
    "soil_moisture": "%",
    "light_intensity": "lux",
    "pressure": "hPa",
}

# Tắt log từng bản tin cảm biến để tránh spam khi số lượng sensor lớn.
LOG_SENSOR_MESSAGES = parse_bool(os.getenv("LOG_SENSOR_MESSAGES"), default=False)

# Global variables (định nghĩa trước app)
kafka_consumer = None
consumer_task = None
kafka_connected = False
connected_clients: Set[WebSocket] = set()
client_lock = asyncio.Lock()

# Lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Quản lý startup và shutdown events"""
    global kafka_consumer, consumer_task, kafka_connected
    
    # === STARTUP ===
    logger.info("🚀 Khởi động FastAPI Backend...")
    try:
        # Kiểm tra cấu hình Kafka từ env
        def _validate_kafka_config():
            missing = []
            if not KAFKA_BOOTSTRAP_SERVERS:
                missing.append('KAFKA_BOOTSTRAP_SERVERS')
            if not KAFKA_SASL_USERNAME or not KAFKA_SASL_PASSWORD:
                logger.warning('⚠️  Kafka SASL credentials not set in environment; consumer may fail to connect to Confluent Cloud')
            if missing:
                logger.warning('⚠️  Missing Kafka config: %s', ','.join(missing))

        _validate_kafka_config()
        await init_kafka_consumer()
        # Tạo task consume kafka
        consumer_task = asyncio.create_task(consume_kafka())
        logger.info("✓ Backend sẵn sàng")
        yield  # App chạy
    except Exception as e:
        logger.error(f"✗ Lỗi startup: {e}")
        raise
    finally:
        # === SHUTDOWN ===
        logger.info("⏹️  Shutdown Backend...")
        if consumer_task:
            consumer_task.cancel()
        if kafka_consumer:
            await kafka_consumer.stop()
        kafka_connected = False
        logger.info("✓ Backend đã dừng")

# Khởi tạo FastAPI app với lifespan
app = FastAPI(title="IoT Real-time Dashboard API", version="1.0", lifespan=lifespan)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=CORS_ALLOW_CREDENTIALS,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === Kafka Consumer ===

async def init_kafka_consumer():
    global kafka_consumer, kafka_connected

    logger.info("bootstrap=%r", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("topic=%r", KAFKA_TOPIC)
    logger.info("protocol=%r", KAFKA_SECURITY_PROTOCOL)
    logger.info("mechanism=%r", KAFKA_SASL_MECHANISM)
    logger.info("username_prefix=%r", KAFKA_SASL_USERNAME[:4] + "***" if KAFKA_SASL_USERNAME else None)
    logger.info("password_len=%s", len(KAFKA_SASL_PASSWORD) if KAFKA_SASL_PASSWORD else None)

    ssl_context = ssl.create_default_context()

    kafka_consumer = aiokafka.AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        group_id="fastapi-consumer-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        security_protocol=KAFKA_SECURITY_PROTOCOL,
        sasl_mechanism=KAFKA_SASL_MECHANISM,
        sasl_plain_username=KAFKA_SASL_USERNAME,
        sasl_plain_password=KAFKA_SASL_PASSWORD,
        ssl_context=ssl_context,
    )

    await kafka_consumer.start()

def detect_metric_key(data):
    """Xác định metric hiện có trong payload single-metric."""
    metric_type = data.get("metric_type")
    if metric_type in METRIC_UNITS and data.get(metric_type) is not None:
        return metric_type

    for key in METRIC_UNITS:
        if data.get(key) is not None:
            return key

    return None

async def consume_kafka():
    """Consume messages từ Kafka và broadcast tới clients"""
    global kafka_connected
    try:
        async for message in kafka_consumer:
            data = message.value

            if LOG_SENSOR_MESSAGES:
                sensor_id = data.get("sensor_id", "N/A")
                metric_key = detect_metric_key(data)

                if metric_key:
                    value = data.get(metric_key)
                    unit = data.get("unit", METRIC_UNITS.get(metric_key, ""))
                    unit_suffix = f" {unit}" if unit else ""
                    logger.info(
                        "📥 Nhận từ Kafka | Sensor=%s | %s=%s%s",
                        sensor_id,
                        metric_key,
                        value,
                        unit_suffix,
                    )
                else:
                    logger.info("📥 Nhận từ Kafka | Sensor=%s | Payload=%s", sensor_id, data)
            
            # Broadcast tới tất cả connected clients
            await broadcast_to_clients(data)
    except Exception as e:
        kafka_connected = False
        logger.error(f"✗ Lỗi consume Kafka: {e}")


def is_kafka_ready():
    consumer_running = kafka_consumer is not None and kafka_connected
    task_running = consumer_task is not None and not consumer_task.done()
    return consumer_running and task_running

async def broadcast_to_clients(data):
    """Broadcast data tới tất cả connected WebSocket clients"""
    disconnected_clients = set()
    
    async with client_lock:
        for client in connected_clients:
            try:
                await client.send_json(data)
            except Exception as e:
                logger.warning(f"⚠️  Không thể gửi tới client: {e}")
                disconnected_clients.add(client)
        
        # Xóa clients đã bị ngắt
        connected_clients.difference_update(disconnected_clients)

# === WebSocket Endpoint ===

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint cho Frontend kết nối"""
    await websocket.accept()
    
    async with client_lock:
        connected_clients.add(websocket)
    
    logger.info(f"✓ Client kết nối. Tổng clients: {len(connected_clients)}")
    
    try:
        while True:
            # Chỉ đợi message từ client (để detect disconnect)
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        async with client_lock:
            connected_clients.discard(websocket)
        logger.info(f"✓ Client ngắt kết nối. Tổng clients: {len(connected_clients)}")
    except Exception as e:
        logger.error(f"✗ WebSocket error: {e}")
        async with client_lock:
            connected_clients.discard(websocket)

# === API Endpoints ===

@app.get("/api/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "ok",
        "connected_clients": len(connected_clients),
        "kafka_connected": is_kafka_ready()
    }

@app.get("/api/status")
async def status():
    """Status endpoint"""
    return {
        "app": "IoT Real-time Dashboard",
        "version": "1.0",
        "kafka_topic": KAFKA_TOPIC,
        "connected_clients": len(connected_clients),
        "kafka_broker": KAFKA_BOOTSTRAP_SERVERS,
        "kafka_connected": is_kafka_ready(),
    }

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "IoT Real-time Dashboard API",
        "docs": "/docs",
        "websocket": "ws://localhost:8000/ws",
        "analytics": "/api/analytics/measurements"
    }


# === Analytics API Endpoints ===

@app.get("/api/analytics/sensors")
async def get_sensors():
    """
    Lấy danh sách sensors có dữ liệu trong Databricks
    Returns: List[{sensor_id, location, metric_type, unit}]
    """
    try:
        client = get_databricks_client()
        if not client.is_connected():
            return {
                "status": "error",
                "message": "Databricks not connected",
                "data": []
            }
        
        sensors = client.get_sensors()
        return {
            "status": "ok",
            "data": sensors
        }
    except Exception as e:
        logger.error(f"✗ Error getting sensors: {e}")
        return {
            "status": "error",
            "message": str(e),
            "data": []
        }


@app.get("/api/analytics/measurements")
async def get_measurements(
    sensor_id: Optional[str] = Query(None, description="Sensor ID (e.g., sensor_1)"),
    metric_type: Optional[str] = Query(None, description="Metric type (e.g., temperature)"),
    from_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(10000, description="Max records"),
):
    """
    Truy vấn dữ liệu đo lường từ Databricks
    
    Query params:
    - sensor_id: sensor_1, sensor_2, ...
    - metric_type: temperature, humidity, soil_moisture, light_intensity, pressure
    - from_date: YYYY-MM-DD
    - to_date: YYYY-MM-DD
    - limit: Max records (default 10000)
    
    Returns: List[{event_ts, sensor_id, location, metric_type, metric_value, unit}]
    """
    try:
        # Validate dates
        if from_date:
            try:
                datetime.strptime(from_date, "%Y-%m-%d")
            except ValueError:
                return {
                    "status": "error",
                    "message": "Invalid from_date format (use YYYY-MM-DD)",
                    "data": []
                }
        
        if to_date:
            try:
                datetime.strptime(to_date, "%Y-%m-%d")
            except ValueError:
                return {
                    "status": "error",
                    "message": "Invalid to_date format (use YYYY-MM-DD)",
                    "data": []
                }
        
        client = get_databricks_client()
        if not client.is_connected():
            return {
                "status": "error",
                "message": "Databricks not connected",
                "data": []
            }
        
        df = client.query_measurements(
            sensor_id=sensor_id,
            metric_type=metric_type,
            from_date=from_date,
            to_date=to_date,
            limit=limit,
        )
        
        if df.empty:
            return {
                "status": "ok",
                "message": "No data found",
                "data": []
            }
        
        # Convert to list of dicts
        records = df.to_dict(orient="records")
        
        # Convert timestamps to ISO format strings
        for record in records:
            if isinstance(record.get("event_ts"), pd.Timestamp):
                record["event_ts"] = record["event_ts"].isoformat()
        
        logger.info(f"✓ Returned {len(records)} measurements")
        return {
            "status": "ok",
            "count": len(records),
            "data": records
        }
    
    except Exception as e:
        logger.error(f"✗ Error getting measurements: {e}")
        return {
            "status": "error",
            "message": str(e),
            "data": []
        }


@app.get("/api/analytics/recent-minutely")
async def get_recent_minutely(
    sensor_id: str = Query(..., description="Sensor ID (e.g., sensor_1)"),
    metric_type: str = Query(..., description="Metric type (e.g., temperature)"),
    lookback_minutes: int = Query(120, ge=1, le=720, description="Lookback window in minutes"),
):
    """Lấy dữ liệu 2 giờ gần nhất (hoặc lookback tùy chọn), gom nhóm theo từng phút."""
    try:
        client = get_databricks_client()
        if not client.is_connected():
            return {
                "status": "error",
                "message": "Databricks not connected",
                "data": []
            }

        df = client.query_recent_minutely(
            sensor_id=sensor_id,
            metric_type=metric_type,
            lookback_minutes=lookback_minutes,
        )

        if df.empty:
            return {
                "status": "ok",
                "message": "No data found",
                "data": []
            }

        records = df.to_dict(orient="records")

        for record in records:
            minute_ts = record.get("minute_ts")
            if isinstance(minute_ts, pd.Timestamp):
                record["minute_ts"] = minute_ts.isoformat()
            elif minute_ts is not None:
                record["minute_ts"] = str(minute_ts)

            avg_value = record.get("avg_value")
            if avg_value is not None:
                try:
                    record["avg_value"] = float(avg_value)
                except (TypeError, ValueError):
                    pass

        return {
            "status": "ok",
            "count": len(records),
            "data": records,
        }

    except Exception as e:
        logger.error(f"✗ Error getting recent minutely analytics: {e}")
        return {
            "status": "error",
            "message": str(e),
            "data": []
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
