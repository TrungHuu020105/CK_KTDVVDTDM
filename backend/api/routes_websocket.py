"""
WebSocket routes cho Real-Time Metrics Monitoring
"""
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException
from ..websocket_manager import ConnectionManager
from ..schemas_ws import IotMetricsData, StatusResponse
import json
from datetime import datetime

# Khởi tạo router
router = APIRouter(tags=["WebSocket Metrics"])

# Khởi tạo ConnectionManager (global)
manager = ConnectionManager()


def save_iot_metric_to_db(metric_type: str, source: str, value: float, save_flag: bool):
    """
    Save IoT metric to database - DISABLED (using Databricks instead)
    
    Architecture:
    - Frontend receives 100% of realtime data (smooth display)
    - Metrics are streamed from Databricks
    """
    # Database save disabled - all data comes from Databricks
    pass
    # if not save_flag:
    #     return  # Skip DB save for non-important data
    # 
    # try:
    #     db = SessionLocal()
    #     metric = MetricCreate(
    #         metric_type=metric_type,
    #         value=value,
    #         source=source,
    #         timestamp=None
    #     )
    #     create_metrics_bulk(db, [metric])
    #     db.close()
    #     print(f"💾 Saved {metric_type}={value} (source={source}) to DB")
    # except Exception as e:
    #     print(f"❌ DB save error: {e}")
    #     if db:
    #         db.close()


@router.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    """
    WebSocket endpoint để client kết nối và gửi metrics.
    
    Endpoint: ws://SERVER_IP:8000/api/ws/{client_id}
    """
    try:
        # Kết nối client
        print(f"🔗 [DEBUG] Client {client_id} đang cố kết nối...")
        await manager.connect(client_id, websocket)
        print(f"✅ [DEBUG] Client {client_id} đã accept WebSocket!")
        
        # Lặp nhận dữ liệu từ client
        while True:
            try:
                # Nhận dữ liệu JSON từ client
                data = await websocket.receive_text()
                
                # Parse JSON
                metrics_dict = json.loads(data)
                try:
                    metrics = IotMetricsData(**metrics_dict)
                    manager.save_metrics(client_id, metrics.model_dump())
                    print(f"[DEBUG] Received: {metrics.metric_type} | saved={metrics.saved}")

                    realtime_broadcast = {
                        "type": "iot_metric",
                        "metric_type": metrics.metric_type,
                        "value": metrics.value,
                        "source": metrics.source,
                        "timestamp": metrics.timestamp or datetime.now().isoformat(),
                        "saved": metrics.saved
                    }
                    await manager.broadcast(json.dumps(realtime_broadcast))

                    save_iot_metric_to_db(
                        metrics.metric_type,
                        metrics.source,
                        metrics.value,
                        metrics.saved
                    )

                    await websocket.send_text(json.dumps({
                        "status": "ok",
                        "message": f"IoT metric received: {metrics.metric_type}",
                        "server_time": datetime.now().isoformat()
                    }))
                except ValueError as e:
                    print(f"⚠️ IoT Validation error từ {client_id}: {e}")
                    await websocket.send_text(json.dumps({
                        "status": "error",
                        "message": f"IoT Validation error: {str(e)}"
                    }))
                
            except json.JSONDecodeError as e:
                print(f"⚠️ JSON parsing error từ {client_id}: {e}")
                await websocket.send_text(json.dumps({
                    "status": "error",
                    "message": f"Invalid JSON format: {str(e)}"
                }))
                    
    except WebSocketDisconnect:
        manager.disconnect(client_id)
    except Exception as e:
        print(f"❌ Error với client {client_id}: {e}")
        manager.disconnect(client_id)


@router.get("/status", response_model=StatusResponse, tags=["Status"])
async def get_status():
    """
    GET endpoint để lấy trạng thái tất cả clients và metrics mới nhất.
    
    Response format:
    {
        "total_clients": 3,
        "timestamp": "2024-04-06T10:35:00",
        "clients": [
            {
                "client_id": "client_1",
                "status": "online",
                "connected_at": "2024-04-06T10:30:00",
                "last_update": "2024-04-06T10:35:00",
                "metrics": {
                    "metric_type": "temperature",
                    "value": 24.8,
                    "source": "sensor_1",
                    "timestamp": "2024-04-06T10:35:00"
                }
            }
        ]
    }
    """
    return manager.get_all_status()


@router.get("/status/{client_id}", tags=["Status"])
async def get_client_status(client_id: str):
    """
    GET endpoint để lấy thông tin của một client cụ thể.
    
    Parameters:
    - client_id: ID của client
    """
    client_info = manager.get_client_info(client_id)
    
    if client_info is None:
        raise HTTPException(
            status_code=404,
            detail=f"Client {client_id} không tìm thấy hoặc chưa kết nối"
        )
    
    return client_info


@router.get("/health", tags=["Health"])
async def health_check():
    """
    Health check endpoint.
    """
    return {
        "status": "healthy",
        "message": "WebSocket Metrics Server is running",
        "connected_clients": len(manager.active_connections),
        "timestamp": datetime.now().isoformat()
    }
