"""
FastAPI Backend with WebSocket
Consume dữ liệu từ Kafka và broadcast qua WebSocket tới clients
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
import asyncio
import json
import logging
from typing import Set
import aiokafka
import os
from pathlib import Path

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cấu hình
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "iot-sensor-data"

# Global variables (định nghĩa trước app)
kafka_consumer = None
consumer_task = None
connected_clients: Set[WebSocket] = set()
client_lock = asyncio.Lock()

# Lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Quản lý startup và shutdown events"""
    global kafka_consumer, consumer_task
    
    # === STARTUP ===
    logger.info("🚀 Khởi động FastAPI Backend...")
    try:
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
        logger.info("✓ Backend đã dừng")

# Khởi tạo FastAPI app với lifespan
app = FastAPI(title="IoT Real-time Dashboard API", version="1.0", lifespan=lifespan)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === Kafka Consumer ===

async def init_kafka_consumer():
    """Khởi tạo Kafka Consumer"""
    global kafka_consumer
    try:
        kafka_consumer = aiokafka.AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            group_id='fastapi-consumer-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=1000
        )
        await kafka_consumer.start()
        logger.info(f"✓ Kafka Consumer khởi tạo, subscribe vào: {KAFKA_TOPIC}")
    except Exception as e:
        logger.error(f"✗ Lỗi khởi tạo Kafka: {e}")
        raise

async def consume_kafka():
    """Consume messages từ Kafka và broadcast tới clients"""
    try:
        async for message in kafka_consumer:
            data = message.value
            logger.info(f"📥 Nhận từ Kafka: Temp={data.get('temperature')}°C, "
                       f"Humidity={data.get('humidity')}%")
            
            # Broadcast tới tất cả connected clients
            await broadcast_to_clients(data)
    except Exception as e:
        logger.error(f"✗ Lỗi consume Kafka: {e}")

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
        "kafka_connected": kafka_consumer is not None
    }

@app.get("/api/status")
async def status():
    """Status endpoint"""
    return {
        "app": "IoT Real-time Dashboard",
        "version": "1.0",
        "kafka_topic": KAFKA_TOPIC,
        "connected_clients": len(connected_clients),
        "kafka_broker": KAFKA_BOOTSTRAP_SERVERS
    }

# === Root endpoint ===

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "IoT Real-time Dashboard API",
        "docs": "/docs",
        "websocket": "ws://localhost:8000/ws"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
