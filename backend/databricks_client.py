"""
Databricks SQL Connector
Kết nối tới Databricks để lấy dữ liệu từ Delta tables
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict
import pandas as pd
import os
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

logger = logging.getLogger(__name__)

# Databricks configuration
DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG", "workspace")
DATABRICKS_SCHEMA = os.getenv("DATABRICKS_SCHEMA", "iot_analytics")
DATABRICKS_TABLE = os.getenv("DATABRICKS_TABLE", "smart_filtered_measurements")
DATABRICKS_TARGET_TABLE = os.getenv("DATABRICKS_TARGET_TABLE")

# Prefer explicit target table if provided. Otherwise use schema.table as requested.
TARGET_TABLE = DATABRICKS_TARGET_TABLE or f"{DATABRICKS_SCHEMA}.{DATABRICKS_TABLE}"


class DatabricksClient:
    """Client để kết nối và truy vấn Databricks"""
    
    def __init__(self, max_retries: int = 3, retry_delay: float = 1.0):
        self.connection = None
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._connect()
    
    def _connect(self):
        """Kết nối tới Databricks với retry logic"""
        try:
            from databricks import sql
            
            if not all([DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN]):
                logger.warning(
                    "⚠️  Databricks credentials not fully configured. "
                    "Set DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN in .env"
                )
                return
            
            for attempt in range(self.max_retries):
                try:
                    self.connection = sql.connect(
                        server_hostname=DATABRICKS_SERVER_HOSTNAME,
                        http_path=DATABRICKS_HTTP_PATH,
                        personal_access_token=DATABRICKS_TOKEN,
                        catalog=DATABRICKS_CATALOG,
                        schema=DATABRICKS_SCHEMA,
                    )
                    
                    # Test connection
                    with self.connection.cursor() as cursor:
                        cursor.execute("SELECT 1")
                    
                    logger.info(f"✅ Kết nối Databricks thành công: {DATABRICKS_SERVER_HOSTNAME}")
                    return
                    
                except Exception as e:
                    if attempt < self.max_retries - 1:
                        logger.warning(f"⚠️  Connection attempt {attempt + 1} failed, retrying in {self.retry_delay}s...")
                        time.sleep(self.retry_delay)
                    else:
                        raise
                        
        except Exception as e:
            logger.error(f"❌ Lỗi kết nối Databricks sau {self.max_retries} attempts: {e}")
            self.connection = None
    
    def is_connected(self) -> bool:
        """Kiểm tra kết nối"""
        if self.connection is None:
            return False
        
        try:
            # Test connection với query đơn giản
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
        except Exception as e:
            logger.warning(f"⚠️  Connection test failed: {e}")
            return False
    
    def query_measurements(
        self,
        sensor_id: Optional[str] = None,
        metric_type: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        limit: int = 10000,
    ) -> pd.DataFrame:
        """
        Truy vấn dữ liệu từ bảng smart_filtered_measurements
        
        Args:
            sensor_id: Mã sensor (ví dụ: sensor_1)
            metric_type: Loại chỉ số (ví dụ: temperature)
            from_date: Ngày bắt đầu (YYYY-MM-DD)
            to_date: Ngày kết thúc (YYYY-MM-DD)
            limit: Giới hạn số bản ghi
        
        Returns:
            DataFrame với các cột: event_ts, sensor_id, location, metric_type, metric_value, unit
        """
        if not self.is_connected():
            logger.error("❌ Databricks chưa kết nối")
            return pd.DataFrame()
        
        try:
            # Xây dựng WHERE clause
            where_conditions = []
            
            if sensor_id:
                where_conditions.append(f"sensor_id = '{sensor_id}'")
            
            if metric_type:
                where_conditions.append(f"metric_type = '{metric_type}'")
            
            if from_date:
                where_conditions.append(f"CAST(event_ts AS DATE) >= CAST('{from_date}' AS DATE)")
            
            if to_date:
                where_conditions.append(f"CAST(event_ts AS DATE) <= CAST('{to_date}' AS DATE)")
            
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            # Xây dựng query
            query = f"""
            SELECT 
                event_ts,
                sensor_id,
                location,
                metric_type,
                metric_value,
                unit
            FROM {TARGET_TABLE}
            WHERE {where_clause}
            ORDER BY event_ts DESC
            LIMIT {limit}
            """
            
            logger.debug(f"📊 Executing query on {TARGET_TABLE}...")
            
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                
                if result:
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(result, columns=columns)
                    
                    # Convert timestamp to string if needed
                    if 'event_ts' in df.columns:
                        df['event_ts'] = df['event_ts'].astype(str)
                    
                    logger.info(f"✅ Lấy {len(df)} bản ghi từ Databricks")
                    return df
                else:
                    logger.info("ℹ️  Không có dữ liệu khớp điều kiện")
                    return pd.DataFrame()
        
        except Exception as e:
            logger.error(f"❌ Lỗi truy vấn: {e}")
            return pd.DataFrame()
    
    def get_sensors(self) -> List[Dict[str, str]]:
        """Lấy danh sách sensors và metrics"""
        if not self.is_connected():
            logger.error("❌ Databricks chưa kết nối")
            return []
        
        try:
            query = f"""
            SELECT DISTINCT 
                sensor_id,
                location,
                metric_type,
                unit
            FROM {TARGET_TABLE}
            ORDER BY sensor_id, metric_type
            """
            
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                
                sensors = []
                for row in result:
                    sensors.append({
                        "sensor_id": row[0],
                        "location": row[1],
                        "metric_type": row[2],
                        "unit": row[3],
                    })
                
                logger.info(f"✅ Lấy {len(sensors)} sensor-metric combinations")
                return sensors
        
        except Exception as e:
            logger.error(f"❌ Lỗi lấy danh sách sensors: {e}")
            return []
    
    def get_table_info(self) -> Dict:
        """Lấy thông tin bảng (record count, date range, etc.)"""
        if not self.is_connected():
            return {}
        
        try:
            query = f"""
            SELECT 
                COUNT(*) as total_records,
                MIN(event_ts) as earliest,
                MAX(event_ts) as latest
            FROM {TARGET_TABLE}
            """
            
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                
                if result:
                    return {
                        "total_records": result[0],
                        "earliest": str(result[1]) if result[1] else None,
                        "latest": str(result[2]) if result[2] else None,
                    }
        
        except Exception as e:
            logger.error(f"❌ Lỗi lấy thông tin bảng: {e}")
        
        return {}
    
    def close(self):
        """Đóng kết nối"""
        if self.connection:
            try:
                self.connection.close()
                logger.info("✓ Đóng kết nối Databricks")
            except Exception as e:
                logger.error(f"✗ Lỗi đóng kết nối: {e}")


# Global instance
_databricks_client = None


def get_databricks_client() -> DatabricksClient:
    """Lấy Databricks client instance (singleton)"""
    global _databricks_client
    if _databricks_client is None:
        _databricks_client = DatabricksClient()
    return _databricks_client
