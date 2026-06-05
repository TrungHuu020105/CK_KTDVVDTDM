# IoT Lakehouse Platform - Databricks Edition

Du an nay da duoc chuyen huong tu do an "Phat trien ung dung" sang mon "Kien truc dich vu va dien toan dam may".
Trong phien ban nay, he thong tap trung vao kien truc dich vu cho IoT, Databricks Lakehouse, Delta tables va pipeline huan luyen/so sanh mo hinh du bao.

## Muc tieu

- Giam sat realtime nhiet do va do am tu cam bien ESP32/DHT11.
- Chuan hoa mot `sensor_id` thanh mot sensor duy nhat, moi reading gom ca `temperature` va `humidity`.
- Ghi du lieu sensor-level vao Databricks Bronze table theo kieu backend write truc tiep.
- Databricks dam nhiem data lakehouse, ETL, feature table, training/evaluation va forecast result.
- Frontend hien thi sensor card, forecast chart va model leaderboard.

## Kien truc hien tai

```text
ESP32 / MQTT
  -> iot_backend
      -> PostgreSQL operational DB
      -> WebSocket realtime
      -> Databricks Bronze Delta table

Databricks Lakehouse
  -> bronze_sensor_readings
  -> silver_sensor_readings
  -> gold_sensor_features
  -> model_evaluation_results
  -> forecast_results

app backend
  -> Auth/RBAC
  -> Sensor gateway API
  -> Databricks forecast/leaderboard query

frontend
  -> Sensor dashboard
  -> Realtime temperature/humidity
  -> Forecast and model comparison
```

## Thanh phan

| Thanh phan | Thu muc | Vai tro |
|---|---|---|
| Frontend | `frontend/` | Dashboard React cho sensors, forecast va model leaderboard |
| App backend | `app/` | API gateway, auth, admin, proxy IoT, query Databricks results |
| IoT backend | `iot_backend/` | MQTT ingest, sensor readings, alerts, realtime WebSocket, Databricks Bronze write |
| Scripts | `scripts/`, root scripts | Seed/reset/demo data |

## Sensor-level schema

Reading moi co dang:

```json
{
  "sensor_id": "esp32_devkit_v1",
  "timestamp": "2026-05-30T10:00:00+07:00",
  "temperature": 30.5,
  "humidity": 72.1,
  "source_type": "physical_iot",
  "provider": "esp32",
  "environment_type": "indoor",
  "location_province": "Ho Chi Minh City"
}
```

`source_type` gom:

- `physical_iot`: cam bien ESP32 that.

## Databricks environment

Cau hinh trong `app/.env` va `iot_backend/.env`:

```env
DATABRICKS_ENABLED=true
DATABRICKS_SERVER_HOSTNAME=<your-workspace-host>
DATABRICKS_HTTP_PATH=<sql-warehouse-http-path>
DATABRICKS_TOKEN=<personal-access-token>
DATABRICKS_CATALOG=iot_cloud
DATABRICKS_SCHEMA=sensor_analytics
DATABRICKS_BRONZE_TABLE=bronze_sensor_readings
DATABRICKS_FORECAST_TABLE=forecast_results
DATABRICKS_EVALUATION_TABLE=model_evaluation_results
```

## API moi

```text
GET    /api/sensors
POST   /api/sensors
GET    /api/sensors/{sensor_id}
PATCH  /api/sensors/{sensor_id}
DELETE /api/sensors/{sensor_id}
POST   /api/sensors/readings
GET    /api/sensors/{sensor_id}/latest
GET    /api/sensors/{sensor_id}/history
GET    /api/sensors/{sensor_id}/forecast
GET    /api/sensors/{sensor_id}/model-leaderboard
GET    /api/sensors/databricks/status
```

## Demo flow tren frontend

1. Vao menu `Sensors`.
2. Tao `Physical ESP32` de demo du lieu thiet bi that.
3. Chay notebook Databricks theo thu tu `01_create_lakehouse_tables` -> `02_clean_bronze_to_silver` -> `03_build_gold_sensor_features` -> `04_train_compare_models`.
4. Bam `Analytics and Models` tren sensor card de xem actual chart, forecast chart va leaderboard MAE/RMSE tu `model_evaluation_results`.

### Local realtime producer

Khi chua co ESP32/MQTT that, co the stream du lieu demo vao API theo schema moi:

```powershell
python scripts\stream_sensor_readings.py --interval 3
```

Moi batch se tao mot `sensor_readings` row cho tung sensor, gom ca `temperature` va `humidity`, nen frontend se cap nhat theo polling realtime.

## Databricks notebooks nen co

```text
01_create_lakehouse_tables
02_ingest_meteostat_virtual_sensor_to_bronze
03_clean_bronze_to_silver
04_build_gold_sensor_features
05_train_compare_models
06_batch_forecast_results
```

## Chay local

Backend trung tam:

```powershell
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

IoT backend:

```powershell
python -m uvicorn iot_backend.main:app --host 0.0.0.0 --port 8100 --reload
```

Frontend:

```powershell
cd frontend
npm install
npm run dev
```

## Diem nhan bao cao

- Kien truc dich vu ro rang: frontend, app gateway, iot_backend, Databricks Lakehouse.
- Sensor-level data model tranh viec mot `sensor_id` bi tach thanh 2 device.
- Backend ghi truc tiep Databricks Bronze table theo kieu A.
- Databricks thuc hien ETL Bronze/Silver/Gold va training/evaluation.
- Frontend hien thi model leaderboard va forecast result de bao cao truc quan.

