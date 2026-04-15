# Huong Dan Chay Nhanh (Quick Start)

Tai lieu nay chi huong dan mot luong duy nhat:

Fake Sensor -> MQTT (Docker) -> Kafka (Docker) ->
- Backend realtime qua Kafka -> WebSocket -> Frontend
- Kafka Sink -> Databricks thuc (khong fake Databricks)

## 1) Yeu cau moi truong

- Python 3.10+ (khuyen nghi 3.11)
- Node.js 18+
- Docker Desktop
- Databricks Workspace + SQL Warehouse

## 2) Cau hinh .env backend

1. Tao file .env tu .env.example trong thu muc goc.
2. Dien Databricks credentials:
   - DATABRICKS_WORKSPACE_URL (hoac DATABRICKS_HOST)
   - DATABRICKS_TOKEN
   - DATABRICKS_CATALOG
   - DATABRICKS_SCHEMA
   - DATABRICKS_PATH hoac DATABRICKS_WAREHOUSE_ID
3. Dam bao schema Databricks da co cac bang toi thieu:
   - iot_sensor_data
   - iot_latest_readings
   - iot_device_metadata
4. Dat realtime source:
   - REALTIME_SOURCE=kafka
5. Dat thong so Kafka/MQTT:
   - KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   - KAFKA_TELEMETRY_TOPIC=telemetry.raw
   - KAFKA_INSIGHT_TOPIC=insight.events (tuy chon)
   - KAFKA_GROUP_PREFIX=metricspulse-backend
   - KAFKA_TO_DATABRICKS_FLUSH_SECONDS=2
   - KAFKA_TO_DATABRICKS_MAX_BATCH=200
   - MQTT_HOST=localhost
   - MQTT_PORT=1883
   - MQTT_TOPIC_TELEMETRY=esp/telemetry

## 3) Cau hinh frontend env

1. Tao frontend/.env tu frontend/.env.example.
2. Dien:
   - VITE_SERVER_IP=localhost
   - VITE_SERVER_PORT=8000

## 4) Chay ha tang MQTT/Kafka bang Docker

```powershell
docker compose -f infra/docker-compose.realtime.yml up -d
```

Kiem tra:

```powershell
docker compose -f infra/docker-compose.realtime.yml ps
```

## 5) Cai dependencies

Backend:

```powershell
pip install -r requirements.txt
```

Frontend:

```powershell
cd frontend
npm install
```

## 6) Chay backend va frontend

Terminal 1 (backend):

```powershell
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

Terminal 2 (frontend):

```powershell
cd frontend
npm run dev
```

## 7) Chay pipeline sensor

Terminal 3 (fake sensor -> MQTT):

```powershell
python simulation/fake_esp_mqtt_publisher.py
```

Terminal 4 (MQTT -> Kafka bridge):

```powershell
python simulation/mqtt_to_kafka_bridge.py
```

Terminal 5 (Kafka -> Databricks sink):

```powershell
python simulation/kafka_to_databricks_sink.py
```

## 8) Kiem tra thong tuyen

1. Frontend co realtime data.
2. Backend log co dong Kafka Bridge consuming telemetry topic.
3. API GET /api/databricks/latest co data.
4. Databricks bang iot_sensor_data va iot_latest_readings duoc cap nhat.

## 9) Loi thuong gap

1. Frontend khong co data:
- Kiem tra VITE_SERVER_IP, VITE_SERVER_PORT.
- Kiem tra backend port 8000 dang chay.

2. MQTT/Kafka khong len:
- Kiem tra Docker Desktop.
- Chay lai docker compose up -d.

3. Sink khong ghi vao Databricks:
- Kiem tra token/warehouse/schema.
- Kiem tra quyen INSERT/MERGE tren schema.

## 10) Tai lieu chi tiet

- FAKE_ESP_MQTT_KAFKA_FLOW.md
