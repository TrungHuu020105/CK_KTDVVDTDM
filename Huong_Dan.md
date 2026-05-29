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

