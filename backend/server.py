from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from databricks import sql
import os
from dotenv import load_dotenv
from typing import List, Dict, Any

# Load environment variables
load_dotenv()

app = FastAPI(title="IoT Backend API")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # React dev server on 5173
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Databricks Configuration
# Load from .env file - required for production
DATABRICKS_CONFIG = {
    "server_hostname": os.getenv("DATABRICKS_HOST"),
    "http_path": os.getenv("DATABRICKS_PATH"),
    "personal_access_token": os.getenv("DATABRICKS_TOKEN"),
}

# Validate required environment variables
if not all([DATABRICKS_CONFIG["server_hostname"], DATABRICKS_CONFIG["http_path"], DATABRICKS_CONFIG["personal_access_token"]]):
    raise ValueError("Missing required environment variables: DATABRICKS_HOST, DATABRICKS_PATH, DATABRICKS_TOKEN")

# Cache for connection
db_connection = None

def get_connection():
    """Get or create Databricks connection"""
    global db_connection
    try:
        if db_connection is None:
            db_connection = sql.connect(
                server_hostname=DATABRICKS_CONFIG["server_hostname"],
                http_path=DATABRICKS_CONFIG["http_path"],
                personal_access_token=DATABRICKS_CONFIG["personal_access_token"],
                timeout_seconds=10
            )
        return db_connection
    except Exception as e:
        print(f"Databricks connection error: {e}")
        raise

def execute_query(query: str) -> List[Dict[str, Any]]:
    """Execute query and return results as list of dicts"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Fetch all rows and convert to list of dicts
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        
        cursor.close()
        return results
    except Exception as e:
        print(f"Query execution error: {e}")
        raise

# Routes

@app.get("/api/health")
async def health():
    """Health check endpoint"""
    return {"status": "ok", "message": "Backend is running"}

@app.get("/api/devices")
async def get_devices():
    """Get all IoT devices with latest readings"""
    try:
        query = """
        SELECT
            device_id, device_name, device_type, location,
            latest_value, unit, last_update, status
        FROM workspace.metrics_app_streaming.iot_latest_readings
        ORDER BY device_type, location
        """
        data = execute_query(query)
        return {"success": True, "data": data}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/device/{device_id}/stats")
async def get_device_stats(device_id: str):
    """Get statistics for a specific device"""
    try:
        query = f"""
        SELECT
            ROUND(MIN(value), 2) as min_value,
            ROUND(MAX(value), 2) as max_value,
            ROUND(AVG(value), 2) as avg_value
        FROM workspace.metrics_app_streaming.iot_sensor_data
        WHERE device_id = '{device_id}'
        """
        data = execute_query(query)
        return {"success": True, "data": data[0] if data else {}}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/device/{device_id}/timeseries")
async def get_device_timeseries(device_id: str):
    """Get time series data for a specific device"""
    try:
        query = f"""
        SELECT
            timestamp, value
        FROM workspace.metrics_app_streaming.iot_sensor_data
        WHERE device_id = '{device_id}'
            AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
        ORDER BY timestamp ASC
        """
        data = execute_query(query)
        return {"success": True, "data": data}
    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
