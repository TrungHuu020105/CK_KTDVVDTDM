"""
WebSocket Connection Manager để quản lý các client kết nối
"""
from typing import Dict, Set, Optional
from fastapi import WebSocket
from datetime import datetime
import json


class ConnectionManager:
    """
    Quản lý các client WebSocket đang kết nối.
    Lưu trữ client_id, websocket object, và dữ liệu metrics mới nhất.
    """
    
    def __init__(self):
        # Dictionary: {client_id: {"websocket": ws, "last_data": {...}, "status": "online", "connected_at": ...}}
        self.active_connections: Dict[str, Dict] = {}
        self.client_metrics: Dict[str, Dict] = {}  # Lưu metrics mới nhất của mỗi client
    
    async def connect(self, client_id: str, websocket: WebSocket):
        """
        Thêm một client mới vào danh sách kết nối.
        """
        await websocket.accept()
        self.active_connections[client_id] = {
            "websocket": websocket,
            "status": "online",
            "connected_at": datetime.now().isoformat(),
            "last_data": None
        }
        print(f"✅ Client {client_id} đã kết nối. Tổng clients: {len(self.active_connections)}")
    
    def disconnect(self, client_id: str):
        """
        Xóa client khỏi danh sách kết nối.
        """
        if client_id in self.active_connections:
            del self.active_connections[client_id]
            print(f"❌ Client {client_id} đã thoát. Tổng clients còn lại: {len(self.active_connections)}")
    
    def save_metrics(self, client_id: str, data: Dict):
        """
        Lưu metrics mới nhất từ một client.
        """
        if client_id in self.active_connections:
            self.active_connections[client_id]["last_data"] = data
            self.active_connections[client_id]["status"] = "online"
            self.active_connections[client_id]["last_update"] = datetime.now().isoformat()
            self.client_metrics[client_id] = data
            metric_type = data.get("metric_type", "unknown")
            value = data.get("value", "N/A")
            print(f"📊 Nhận dữ liệu từ Client {client_id}: {metric_type}={value}")
    
    def get_all_status(self) -> Dict:
        """
        Trả về trạng thái tất cả các clients cùng metrics mới nhất.
        """
        clients = []
        for client_id, info in self.active_connections.items():
            clients.append({
                "client_id": client_id,
                "status": info.get("status", "unknown"),
                "connected_at": info.get("connected_at"),
                "last_update": info.get("last_update", "N/A"),
                "metrics": info.get("last_data", {})
            })
        
        return {
            "total_clients": len(self.active_connections),
            "timestamp": datetime.now().isoformat(),
            "clients": clients
        }
    
    def get_client_info(self, client_id: str) -> Optional[Dict]:
        """
        Lấy thông tin của một client cụ thể.
        """
        if client_id in self.active_connections:
            info = self.active_connections[client_id]
            return {
                "client_id": client_id,
                "status": info.get("status"),
                "connected_at": info.get("connected_at"),
                "last_update": info.get("last_update", "N/A"),
                "metrics": info.get("last_data", {})
            }
        return None
    
    async def broadcast(self, message: str):
        """
        Gửi broadcast message tới tất cả các clients (không bắt buộc dùng).
        """
        for client_id, info in self.active_connections.items():
            try:
                await info["websocket"].send_text(message)
            except Exception as e:
                print(f"⚠️ Lỗi khi broadcast tới {client_id}: {e}")
