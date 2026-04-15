"""Databricks client for fetching metrics and IoT data."""

import os
import time
import requests
from pathlib import Path
from typing import List, Dict, Optional


def _load_root_env() -> None:
    """Load key=value entries from project root .env into process env if missing."""
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _normalize_workspace_url(value: str) -> str:
    """Ensure workspace URL includes scheme and no trailing slash."""
    value = (value or "").strip()
    if not value:
        return ""
    if not value.startswith("http://") and not value.startswith("https://"):
        value = f"https://{value}"
    return value.rstrip("/")


def _extract_warehouse_id(path_value: str) -> Optional[str]:
    """Extract SQL warehouse id from DATABRICKS_PATH style value."""
    if not path_value:
        return None

    parts = [p for p in path_value.strip().strip("/").split("/") if p]
    if not parts:
        return None

    if "warehouses" in parts:
        idx = parts.index("warehouses")
        if idx + 1 < len(parts):
            return parts[idx + 1]

    return parts[-1]

class DatabricksClient:
    """Client to interact with Databricks SQL endpoints for metrics data."""
    
    def __init__(
        self,
        workspace_url: str,
        token: str,
        catalog: str = "workspace",
        schema: str = "metrics_app_streaming",
        warehouse_id: Optional[str] = None,
        http_path: Optional[str] = None,
    ):
        """
        Initialize Databricks client
        
        Args:
            workspace_url: Databricks workspace URL (e.g., https://adb-123.azuredatabricks.net)
            token: Personal Access Token (PAT)
            catalog: Catalog name
            schema: Schema name
        """
        self.workspace_url = workspace_url.rstrip("/")
        self.token = token
        self.catalog = catalog
        self.schema = schema
        self.warehouse_id = warehouse_id
        self.http_path = http_path
        self.server_hostname = self.workspace_url.replace("https://", "").replace("http://", "")
        self._connector_conn = None
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        self.statement_endpoints = [
            f"{self.workspace_url}/api/2.0/sql/statements",
            f"{self.workspace_url}/api/2.1/sql/statements",
        ]

    def _build_payload(self, statement: str, timeout: int) -> Dict:
        payload = {
            "statement": statement,
            "wait_timeout": f"{timeout}s",
            "on_wait_timeout": "CONTINUE",
        }
        if self.warehouse_id:
            payload["warehouse_id"] = self.warehouse_id
        return payload

    def _poll_statement(self, endpoint: str, statement_id: str, timeout: int) -> Dict:
        deadline = time.time() + timeout
        url = f"{endpoint}/{statement_id}"
        last_result = {}

        while time.time() < deadline:
            response = requests.get(url, headers=self.headers, timeout=15)
            if response.status_code == 404:
                return {"error": f"Statement polling endpoint not found: {url}"}

            response.raise_for_status()
            last_result = response.json()
            state = (last_result.get("status") or {}).get("state", "")

            if state in {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}:
                return last_result

            time.sleep(1)

        return last_result

    def _execute_query_with_sql_api(self, statement: str, timeout: int) -> Dict:
        payload = self._build_payload(statement, timeout)
        last_error = None

        for endpoint in self.statement_endpoints:
            try:
                response = requests.post(endpoint, json=payload, headers=self.headers, timeout=timeout + 10)

                # Some workspaces expose only one API version.
                if response.status_code == 404:
                    last_error = f"404 at {endpoint}"
                    continue

                response.raise_for_status()
                result = response.json()

                statement_id = result.get("statement_id")
                state = (result.get("status") or {}).get("state", "")
                if statement_id and state in {"PENDING", "RUNNING"}:
                    result = self._poll_statement(endpoint, statement_id, timeout)

                if "error" in result:
                    return result

                return result
            except requests.exceptions.RequestException as exc:
                last_error = str(exc)

        return {"error": last_error or "Databricks SQL API request failed"}

    def _execute_query_with_connector(self, statement: str, timeout: int) -> Dict:
        try:
            from databricks import sql as databricks_sql
        except Exception as exc:
            return {"error": f"SQL API failed and databricks-sql-connector not available: {exc}"}

        http_path = self.http_path or (f"/sql/1.0/warehouses/{self.warehouse_id}" if self.warehouse_id else None)
        if not (self.server_hostname and http_path and self.token):
            return {"error": "Missing Databricks host/path/token for connector fallback"}

        try:
            if self._connector_conn is None:
                self._connector_conn = databricks_sql.connect(
                    server_hostname=self.server_hostname,
                    http_path=http_path,
                    access_token=self.token,
                )

            cursor = self._connector_conn.cursor()
            try:
                cursor.execute(statement)
                columns = [d[0] for d in (cursor.description or [])]

                rows = []
                if columns:
                    # Prefer Arrow fetch to avoid connector/pandas conversion issues on Windows.
                    try:
                        arrow_table = cursor.fetchall_arrow()
                        py_rows = arrow_table.to_pylist() if arrow_table is not None else []
                        rows = [[row.get(col) for col in columns] for row in py_rows]
                    except Exception:
                        fallback_rows = cursor.fetchall()
                        rows = [list(row) for row in fallback_rows]
            finally:
                cursor.close()

            return {
                "result": {
                    "data_array": rows,
                    "row_count": len(rows),
                    "columns": columns,
                }
            }
        except Exception as exc:
            return {"error": str(exc)}

    @staticmethod
    def _extract_data_array(result: Dict) -> List[List]:
        payload = (result or {}).get("result") or {}
        if "data_array" in payload:
            return payload.get("data_array") or []

        if "rows" in payload and isinstance(payload["rows"], list):
            rows = payload["rows"]
            if rows and isinstance(rows[0], list):
                return rows

        return []
    
    def execute_query(self, sql: str, timeout: int = 300) -> Dict:
        """
        Execute SQL query using Databricks SQL Warehouse API
        
        Args:
            sql: SQL query to execute
            timeout: Query timeout in seconds
            
        Returns:
            Query result as dictionary
        """
        result = self._execute_query_with_sql_api(sql, timeout)
        if result.get("error"):
            sql_api_error = result.get("error")
            connector_result = self._execute_query_with_connector(sql, timeout)
            if connector_result.get("error"):
                connector_result["error"] = (
                    f"SQL API error: {sql_api_error} | "
                    f"Connector error: {connector_result.get('error')}"
                )
            result = connector_result
        return result
    
    def get_latest_metrics(self, device_type: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """
        Get latest metric readings from all devices or specific type
        
        Args:
            device_type: Filter by IoT device type (e.g., 'temperature', 'humidity'). None for all.
            limit: Maximum results to return
            
        Returns:
            List of latest readings
        """
        where_clause = f"WHERE device_type = '{device_type}'" if device_type else ""

        # Prefer dashboard_summary because it is fed from RAM-first serving snapshot in the notebook.
        primary_sql = f"""
            SELECT
                device_id,
                device_name,
                device_type,
                location,
                latest_value as value,
                unit,
                last_update as timestamp,
                status
            FROM {self.catalog}.{self.schema}.dashboard_summary
            {where_clause}
            ORDER BY updated_at DESC, last_update DESC
            LIMIT {limit}
        """

        # Fallback for compatibility if dashboard_summary is not available yet.
        fallback_sql = f"""
            SELECT
                device_id,
                device_name,
                device_type,
                location,
                latest_value as value,
                unit,
                last_update as timestamp,
                status
            FROM {self.catalog}.{self.schema}.iot_latest_readings
            {where_clause}
            ORDER BY last_update DESC
            LIMIT {limit}
        """
        
        try:
            queries = [
                ("dashboard_summary", primary_sql),
                ("iot_latest_readings", fallback_sql),
            ]

            for source_name, sql in queries:
                result = self.execute_query(sql)

                if result.get("error"):
                    print(f"Databricks query error: {result['error']}")
                    continue

                data = self._extract_data_array(result)
                metrics = []
                for row in data:
                    metrics.append({
                        "device_id": row[0],
                        "device_name": row[1],
                        "device_type": row[2],
                        "location": row[3],
                        "value": row[4],
                        "unit": row[5],
                        "timestamp": row[6],
                        "status": row[7]
                    })

                # If dashboard_summary exists but has no rows, continue to fallback table.
                if source_name == "dashboard_summary" and not metrics:
                    continue

                return metrics
            
            return []
            
        except Exception as e:
            print(f"❌ Error fetching latest metrics: {str(e)}")
            return []
    
    def get_metric_history(self, device_id: str, minutes: int = 60, limit: int = 500) -> List[Dict]:
        """
        Get historical data for specific device
        
        Args:
            device_id: Device ID to fetch history for
            minutes: Historical window in minutes
            limit: Maximum records to return
            
        Returns:
            List of historical readings
        """
        sql = f"""
            SELECT
                device_id,
                device_name,
                device_type,
                location,
                value,
                unit,
                timestamp
            FROM {self.catalog}.{self.schema}.iot_sensor_data
            WHERE device_id = '{device_id}'
              AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL {minutes} MINUTES
            ORDER BY timestamp DESC
            LIMIT {limit}
        """
        
        try:
            result = self.execute_query(sql)
            
            if result.get("error"):
                print(f"Databricks query error: {result['error']}")
                return []

            data = self._extract_data_array(result)
            metrics = []
            for row in data:
                metrics.append({
                    "device_id": row[0],
                    "device_name": row[1],
                    "device_type": row[2],
                    "location": row[3],
                    "value": row[4],
                    "unit": row[5],
                    "timestamp": row[6]
                })
            return metrics
            
            return []
            
        except Exception as e:
            print(f"❌ Error fetching metric history: {str(e)}")
            return []
    
    def get_all_devices(self) -> List[Dict]:
        """Get all IoT devices metadata"""
        
        sql = f"""
            SELECT
                device_id,
                device_name,
                device_type,
                location,
                unit,
                min_value,
                max_value,
                mean_value,
                std_dev,
                active,
                created_at
            FROM {self.catalog}.{self.schema}.iot_device_metadata
            ORDER BY device_id
        """
        
        try:
            result = self.execute_query(sql)
            
            if result.get("error"):
                print(f"Databricks query error: {result['error']}")
                return []

            data = self._extract_data_array(result)
            devices = []
            for row in data:
                devices.append({
                    "device_id": row[0],
                    "device_name": row[1],
                    "device_type": row[2],
                    "location": row[3],
                    "unit": row[4],
                    "min_value": row[5],
                    "max_value": row[6],
                    "mean_value": row[7],
                    "std_dev": row[8],
                    "active": row[9],
                    "created_at": row[10]
                })
            return devices
            
            return []
            
        except Exception as e:
            print(f"❌ Error fetching devices: {str(e)}")
            return []
    
    def get_aggregated_metrics(self, device_type: Optional[str] = None) -> List[Dict]:
        """Get aggregated metrics by minute"""
        
        where_clause = f"WHERE device_type = '{device_type}'" if device_type else ""
        
        sql = f"""
            SELECT
                device_id,
                device_name,
                device_type,
                minute_time,
                avg_value,
                min_value,
                max_value,
                stddev_value,
                reading_count
            FROM {self.catalog}.{self.schema}.iot_sensor_data_minutely
            {where_clause}
            ORDER BY minute_time DESC
            LIMIT 500
        """
        
        try:
            result = self.execute_query(sql)
            
            if result.get("error"):
                print(f"Databricks query error: {result['error']}")
                return []

            data = self._extract_data_array(result)
            metrics = []
            for row in data:
                metrics.append({
                    "device_id": row[0],
                    "device_name": row[1],
                    "device_type": row[2],
                    "minute_time": row[3],
                    "avg_value": row[4],
                    "min_value": row[5],
                    "max_value": row[6],
                    "stddev_value": row[7],
                    "reading_count": row[8]
                })
            return metrics
            
            return []
            
        except Exception as e:
            print(f"❌ Error fetching aggregated metrics: {str(e)}")
            return []


# Singleton instance
_databricks_client: Optional[DatabricksClient] = None


def get_databricks_client() -> DatabricksClient:
    """Get or create Databricks client singleton"""
    global _databricks_client
    
    if _databricks_client is None:
        _load_root_env()

        workspace_url = _normalize_workspace_url(
            os.getenv("DATABRICKS_WORKSPACE_URL")
            or os.getenv("DATABRICKS_HOST")
            or "https://adb-123456.azuredatabricks.net"
        )
        token = os.getenv("DATABRICKS_TOKEN", "dapi...")
        catalog = os.getenv("DATABRICKS_CATALOG", "workspace")
        schema = os.getenv("DATABRICKS_SCHEMA", "metrics_app_streaming")
        http_path = os.getenv("DATABRICKS_PATH")
        warehouse_id = (
            os.getenv("DATABRICKS_WAREHOUSE_ID")
            or _extract_warehouse_id(http_path or "")
        )
        
        _databricks_client = DatabricksClient(
            workspace_url,
            token,
            catalog,
            schema,
            warehouse_id=warehouse_id,
            http_path=http_path,
        )
    
    return _databricks_client
