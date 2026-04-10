from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from databricks import sql
import os, json, asyncio
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# JSON Encoder for datetime
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        return obj.isoformat() if isinstance(obj, datetime) else super().default(obj)

# Databricks Setup
APP_CONFIG = {
    "server": os.getenv("DATABRICKS_HOST"),
    "path": os.getenv("DATABRICKS_PATH"),
    "token": os.getenv("DATABRICKS_TOKEN"),
}

if not all(APP_CONFIG.values()):
    raise ValueError("Missing Databricks credentials: DATABRICKS_HOST, DATABRICKS_PATH, DATABRICKS_TOKEN")

db_conn = None

def get_db():
    global db_conn
    if db_conn is None:
        db_conn = sql.connect(
            server_hostname=APP_CONFIG["server"],
            http_path=APP_CONFIG["path"],
            personal_access_token=APP_CONFIG["token"],
            timeout_seconds=10
        )
    return db_conn

def query_db(sql_query):
    cursor = get_db().cursor()
    cursor.execute(sql_query)
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]

# FastAPI App
app = FastAPI(title="IoT API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.get("/api/health")
async def health():
    return {"status": "ok"}

@app.get("/api/devices")
async def get_devices():
    try:
        # Get latest reading per device with status
        data = query_db("""
            WITH latest_readings AS (
                SELECT
                    device_id,
                    device_name,
                    device_type,
                    location,
                    value,
                    unit,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY timestamp DESC) as rn
                FROM workspace.metrics_app_streaming.iot_sensor_data
            )
            SELECT
                lr.device_id,
                lr.device_name,
                lr.device_type,
                lr.location,
                lr.value as latest_value,
                lr.unit,
                lr.timestamp as last_update,
                'NORMAL' as status
            FROM latest_readings lr
            WHERE lr.rn = 1
            ORDER BY lr.device_type, lr.location
        """)
        return {"success": True, "data": data}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/device/{device_id}/stats")
async def get_device_stats(device_id: str):
    try:
        # Calculate stats fresh from raw data (Last 2 hours)
        data = query_db(f"""
            SELECT 
                ROUND(MIN(value), 2) as min_value,
                ROUND(MAX(value), 2) as max_value,
                ROUND(AVG(value), 2) as avg_value
            FROM workspace.metrics_app_streaming.iot_sensor_data
            WHERE device_id = '{device_id}'
            AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
        """)
        return {"success": True, "data": data[0] if data else {}}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/device/{device_id}/timeseries")
async def get_device_timeseries(device_id: str):
    try:
        # Get 1-minute aggregated timeseries (Last 2 hours)
        data = query_db(f"""
            SELECT 
                DATE_TRUNC('MINUTE', timestamp) as timestamp,
                ROUND(AVG(value), 2) as value
            FROM workspace.metrics_app_streaming.iot_sensor_data
            WHERE device_id = '{device_id}'
            AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
            GROUP BY DATE_TRUNC('MINUTE', timestamp)
            ORDER BY timestamp ASC
        """)
        return {"success": True, "data": data}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.websocket("/ws/devices")
async def websocket_devices(websocket: WebSocket):
    await websocket.accept()
    print("✅ WebSocket connected")
    try:
        while True:
            try:
                data = query_db("SELECT device_id, device_name, device_type, location, latest_value, unit, last_update, status FROM workspace.metrics_app_streaming.dashboard_summary ORDER BY device_type, location")
                await websocket.send_text(json.dumps({"success": True, "data": data}, cls=DateTimeEncoder))
                print(f"📤 Sent {len(data)} devices")
                await asyncio.sleep(2)
            except Exception as e:
                print(f"⚠️ Error: {e}")
                await websocket.send_text(json.dumps({"success": False, "error": str(e)}, cls=DateTimeEncoder))
                await asyncio.sleep(5)
    except Exception as e:
        print(f"❌ WebSocket error: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
