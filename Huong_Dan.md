# Huong Dan Chay Local - Databricks Edition

Huong dan nay tap trung vao chay local phuc vu demo kien truc dich vu va Databricks Lakehouse.\r\n\r\n## 1. Chuan bi moi truong

- Python 3.11+
- Node.js 18+
- PostgreSQL
- Databricks SQL Warehouse hoac Cluster co HTTP Path

## 2. Chay app backend

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## 3. Chay IoT backend

```powershell
pip install -r iot_backend\requirements.txt
python -m uvicorn iot_backend.main:app --host 0.0.0.0 --port 8100 --reload
```

## 4. Chay frontend

```powershell
cd frontend
npm install
npm run dev
```

## 5. Databricks

Tao cac bang Delta trong Unity Catalog/schema da cau hinh:

- `bronze_sensor_readings`
- `silver_sensor_readings`
- `gold_sensor_features`
- `model_evaluation_results`
- `forecast_results`

Backend ghi Bronze qua Databricks SQL Connector khi `DATABRICKS_ENABLED=true`.

## 6. Cach demo tren giao dien

- Vao `Sensors` de xem dashboard moi.
- `How to use this demo` giai thich thu tu thao tac khi bao cao.
- `Lakehouse flow` giai thich luong ESP32/Meteostat -> backend write kieu A -> Databricks Bronze/Silver/Gold -> frontend.
- `Model Lab` cho biet model duoc danh gia trong Databricks, ket qua nam o `model_evaluation_results`, forecast nam o `forecast_results`.
- Voi sensor `Virtual Meteostat`, bam `Sync Meteostat` de nap du lieu nhiet do/do am theo tinh thanh.
- Voi sensor `Physical IoT`, gui MQTT tu ESP32; nut `Manual Demo Reading` chi dung de smoke test giao dien/backend khi chua cam thiet bi.
- Bam `Analytics and Models` de xem chart actual/forecast va bang MAE/RMSE chon best model.

## 7. Chay realtime demo data

Neu chua cam ESP32/MQTT that, chay producer API nay de UI cap nhat realtime:

```powershell
python scripts\stream_sensor_readings.py --interval 3
```

Script nay moi 3 giay se ghi mot reading moi cho moi sensor, trong do mot dong co ca `temperature` va `humidity`.

