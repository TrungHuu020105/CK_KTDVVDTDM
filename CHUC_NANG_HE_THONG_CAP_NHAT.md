# Chuc Nang He Thong - Databricks Edition

He thong da duoc tinh gon de phu hop mon Kien truc dich vu va dien toan dam may.

## Thanh phan chinh

- `frontend/`: Dashboard React hien thi sensor-level card, realtime temperature/humidity, forecast va model leaderboard.
- `app/`: API gateway, auth/RBAC, admin, chat, proxy IoT, query Databricks result tables.
- `iot_backend/`: MQTT ingest, PostgreSQL operational write, WebSocket realtime, Databricks Bronze write.
- `Databricks Lakehouse`: Bronze/Silver/Gold Delta tables, Meteostat ingestion, feature engineering, training/evaluation va batch forecast.

## Diem thiet ke moi

- Mot `sensor_id` la mot sensor duy nhat.
- Mot reading gom ca `temperature` va `humidity`.
- `Virtual Meteostat Sensor` dai dien nguon du lieu thoi tiet ngoai troi theo tinh/thanh.
- Backend ghi Bronze vao Databricks theo kieu A khi `DATABRICKS_ENABLED=true`.
- Viec train va chon best model nam trong Databricks notebooks/jobs.

## API chinh

- `GET /api/sensors`
- `POST /api/sensors`
- `POST /api/sensors/readings`
- `GET /api/sensors/{sensor_id}/latest`
- `GET /api/sensors/{sensor_id}/history`
- `GET /api/sensors/{sensor_id}/forecast`
- `GET /api/sensors/{sensor_id}/model-leaderboard`
- `GET /api/sensors/databricks/status`

## Databricks tables

- `bronze_sensor_readings`
- `silver_sensor_readings`
- `gold_sensor_features`
- `model_evaluation_results`
- `forecast_results`
