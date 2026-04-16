# 🚀 IoT Real-time Dashboard

Một hệ thống hiển thị dữ liệu IoT real-time với độ trễ cực thấp. Dữ liệu chảy từ Sensor → MQTT → Kafka → Backend → Frontend qua WebSocket.

## 🏗️ Kiến trúc Hệ thống

```
┌─────────────┐      ┌──────────────┐      ┌─────────┐      ┌─────────┐      ┌────────────┐
│   Sensor    │──→   │    MQTT      │──→   │  Bridge │──→   │ Kafka   │──→   │  Backend   │
│ Simulator   │      │   Broker     │      │  (Py)   │      │         │      │ (FastAPI)  │
└─────────────┘      └──────────────┘      └─────────┘      └─────────┘      └─────┬──────┘
                                                                                     │
                                                                                     │ WebSocket
                                                                                     ▼
                                                                              ┌────────────┐
                                                                              │ Frontend   │
                                                                              │(React+Vite)│
                                                                              └────────────┘
```

## 📋 Yêu cầu

- **Docker & Docker Compose** (cho MQTT & Kafka)
- **Python 3.8+**
- **Node.js 16+** & npm
- **Git** (tuỳ chọn)

## 🔧 Cấu trúc Thư mục

```
CK3_DTDM/
├── docker-compose.yml          # Docker orchestration
├── README.md                    # Documentation
├── QUICK_START.md              # 5-minute setup
├── ARCHITECTURE.md             # System design
├── TROUBLESHOOTING.md          # Problem solving
│
├── mosquitto/                  # MQTT configuration
│   └── config/mosquitto.conf
│
├── backend/                    # FastAPI Backend (Python)
│   ├── main.py
│   └── requirements.txt
│
├── bridge/                     # MQTT to Kafka Bridge (Python)
│   ├── mqtt_kafka_bridge.py
│   └── requirements.txt
│
├── sensor/                     # Sensor Simulator (Python)
│   ├── sensor_simulator.py
│   └── requirements.txt
│
└── frontend/                   # React + Vite Frontend
    ├── package.json
    ├── vite.config.js
    ├── public/index.html
    └── src/
        ├── main.jsx
        ├── App.jsx
        ├── App.css
        ├── index.css
        └── components/Dashboard.jsx
```

## ⚡ Chạy Nhanh (5 phút)

### 1. Khởi động Docker
```bash
docker-compose up -d
sleep 15  # Chờ Kafka khởi động
```

### 2. Cài đặt & Chạy Các Dịch vụ

**Terminal 1 - Backend:**
```bash
cd backend
pip install -r requirements.txt
python main.py
```

**Terminal 2 - Bridge:**
```bash
cd bridge
pip install -r requirements.txt
python mqtt_kafka_bridge.py
```

**Terminal 3 - Sensor:**
```bash
cd sensor
pip install -r requirements.txt
python sensor_simulator.py
```

**Terminal 4 - Frontend:**
```bash
cd frontend
npm install
npm run dev
```

### 3. Mở Browser
```
http://localhost:3000
```

---

## 📊 Kiểm tra Hệ thống

### Health Check
```bash
curl http://localhost:8000/api/health
```

### Test MQTT
```bash
docker-compose exec mosquitto mosquitto_sub -u iot_user -P iot_password -t "sensors/iot/data"
```

### Test Kafka
```bash
docker-compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic iot-sensor-data \
  --from-beginning
```

---

## 🌐 Ports & Services

| Service | Port | URL |
|---------|------|-----|
| Mosquitto MQTT | 1883 | mqtt://localhost:1883 |
| Kafka | 9092 | kafka://localhost:9092 |
| FastAPI Backend | 8000 | http://localhost:8000 |
| React Frontend | 3000 | http://localhost:3000 |
| WebSocket | 8000 | ws://localhost:8000/ws |

---

## 🛠️ Tech Stack

- **Message Broker:** Eclipse Mosquitto (MQTT)
- **Message Queue:** Apache Kafka 7.5.0 (KRaft mode)
- **Backend:** FastAPI + Aiokafka (Python)
- **Frontend:** React 18.2.0 + Vite (JavaScript)
- **Container:** Docker + Docker Compose
- **Build Tool:** Vite (lightning-fast build)

---

## 📚 Tài Liệu

- [QUICK_START.md](QUICK_START.md) - Setup nhanh 5 phút
- [ARCHITECTURE.md](ARCHITECTURE.md) - Thiết kế hệ thống chi tiết
- [ENDPOINTS.md](ENDPOINTS.md) - API & WebSocket documentation
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Giải quyết vấn đề phổ biến

---

## 🔴 Dừng Hệ thống

```bash
# 1. Ctrl+C tất cả terminals
# 2. Dừng Docker
docker-compose down

# 3. Xóa volumes (tuỳ chọn)
docker-compose down -v
```

---

## ✨ Tính Năng

✅ Real-time data streaming
✅ Low-latency WebSocket communication
✅ Async Kafka processing
✅ Auto-reconnect functionality
✅ Modern React UI with Vite
✅ Fast build & hot module reload
✅ Docker containerization
✅ Production-ready code

---

## 📞 Gặp Vấn đề?

1. Xem [TROUBLESHOOTING.md](TROUBLESHOOTING.md)
2. Kiểm tra logs: `docker-compose logs`
3. Đọc [QUICK_START.md](QUICK_START.md)

---

**Chúc mừng! Hệ thống của bạn đã sẵn sàng.** 🚀
