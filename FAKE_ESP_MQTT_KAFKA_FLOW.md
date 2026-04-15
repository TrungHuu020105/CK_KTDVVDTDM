# Fake Sensor -> MQTT -> Kafka -> Backend + Databricks (Real)

Tai lieu nay chay theo dung yeu cau:

- Chi fake du lieu sensor (thay cho ESP that)
- MQTT va Kafka la he thong that chay bang Docker
- Databricks la that (khong fake Databricks)

Luong chinh:

Fake Sensor -> MQTT -> Kafka
                   |      |
                   |      +-> Backend (realtime) -> WebSocket -> Frontend
                   |
                   +-> Kafka to Databricks Sink -> Databricks tables (history/analytics)

## 1) Chay ha tang local (MQTT + Kafka)

Tu thu muc goc project:

```powershell
docker compose -f infra/docker-compose.realtime.yml up -d
```

Kiem tra:

```powershell
docker compose -f infra/docker-compose.realtime.yml ps
```

## 2) Cai dependencies Python

```powershell
pip install -r requirements.txt
```

## 3) Cau hinh .env cho backend

Mo file `.env` (hoac copy tu `.env.example`) va set:

```env
REALTIME_SOURCE=kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TELEMETRY_TOPIC=telemetry.raw
KAFKA_INSIGHT_TOPIC=insight.events
KAFKA_GROUP_PREFIX=metricspulse-backend

KAFKA_TO_DATABRICKS_FLUSH_SECONDS=2
KAFKA_TO_DATABRICKS_MAX_BATCH=200

MQTT_HOST=localhost
MQTT_PORT=1883
MQTT_TOPIC_TELEMETRY=esp/telemetry
```

## 4) Chay backend + frontend

Terminal 1:

```powershell
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

Terminal 2:

```powershell
cd frontend
npm install
npm run dev
```

## 5) Chay pipeline gia lap sensor + ket noi that

Terminal 3 (fake ESP -> MQTT):

```powershell
python simulation/fake_esp_mqtt_publisher.py
```

Terminal 4 (MQTT -> Kafka bridge):

```powershell
python simulation/mqtt_to_kafka_bridge.py
```

Terminal 5 (Kafka -> Databricks that):

```powershell
python simulation/kafka_to_databricks_sink.py
```

## 6) Ket qua mong doi

- Backend log co thong bao Kafka bridge started va consuming telemetry topic.
- Frontend trang IoT Metrics co gia tri realtime cap nhat lien tuc.
- Databricks table `iot_sensor_data` va `iot_latest_readings` duoc cap nhat lien tuc tu Kafka.

## 7) Luu y

- Neu khong co Docker, ban can tu cung cap MQTT broker va Kafka brokers roi doi bien moi truong tuong ung.
- Ban can dam bao Databricks warehouse dang running va `.env` co dung token/workspace/schema.
