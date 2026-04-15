# ⚡ Quick Start - IoT Real-time Dashboard

**5 phút setup hoàn toàn!**

## Step 1: Start Docker (2 phút)

```bash
cd CK3_DTDM
docker-compose up -d
sleep 15  # Chờ Kafka khởi động
```

Kiểm tra:
```bash
docker-compose ps
```

## Step 2: Setup Python (1 phút)

```bash
# Backend
cd backend && pip install -r requirements.txt && cd ..

# Bridge
cd bridge && pip install -r requirements.txt && cd ..

# Sensor
cd sensor && pip install -r requirements.txt && cd ..
```

## Step 3: Setup Frontend (1 phút)

```bash
cd frontend
npm install
```

## Step 4: Run Services (30 giây mỗi cái)

**Terminal 1 - Backend:**
```bash
cd backend && python main.py
```
Khi thấy: `INFO:     Application startup complete [uvicorn]` ✓

**Terminal 2 - Bridge:**
```bash
cd bridge && python mqtt_kafka_bridge.py
```
Khi thấy: `✓ Bridge: MQTT -> Kafka` ✓

**Terminal 3 - Sensor:**
```bash
cd sensor && python sensor_simulator.py
```
Khi thấy: `[1] ✓ Gửi:` ✓

**Terminal 4 - Frontend:**
```bash
cd frontend && npm run dev
```
Khi thấy: `Local: http://localhost:3000` ✓

## Step 5: Xem Dashboard

Mở browser: **http://localhost:3000**

Bạn sẽ thấy:
- 🟢 WebSocket Status: Kết nối
- 📊 Dữ liệu cảm biến cập nhật real-time
- ⏰ Thời gian, nhiệt độ, độ ẩm, áp suất

---

## 🔍 Kiểm tra Nhanh

```bash
# Health check
curl http://localhost:8000/api/health

# MQTT messages
docker-compose exec mosquitto mosquitto_sub -t "sensors/iot/data"

# Kafka messages
docker-compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic iot-sensor-data \
  --from-beginning
```

---

## ⏹️ Dừng

```bash
# Ctrl+C mỗi terminal
# Sau đó
docker-compose down
```

---

**Xong! 🎉 Hệ thống đang chạy!**

Để hiểu thêm, xem [README.md](README.md)
