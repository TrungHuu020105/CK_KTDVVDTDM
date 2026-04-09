# Databricks notebook source
# ============================================================================
# IoT Real-Time Dashboard (Run in Separate Notebook)
# ============================================================================
# Purpose: View live data from streaming notebooks in real-time
# Location: /Workspace/Repos/Shared/metrics-pulse/databricks_realtime_dashboard.py
# ============================================================================

# COMMAND ----------

# ============================================================================
# SETUP: Connect to Live Data
# ============================================================================

from datetime import datetime

catalog = "workspace"
schema = "metrics_app_streaming"

spark.sql(f"USE {catalog}.{schema}")

print("=" * 80)
print("🎯 IoT REAL-TIME DASHBOARD")
print("=" * 80)
print(f"Connected to: {catalog}.{schema}")
print(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

# COMMAND ----------

# ============================================================================
# WIDGET 1: Latest Device Readings (Real-Time Status)
# ============================================================================

print("\n📡 LATEST DEVICE READINGS (Real-Time Status)")
print("=" * 80)

latest_readings = spark.sql("""
    SELECT
        device_id,
        device_name,
        device_type,
        location,
        latest_value,
        unit,
        last_update,
        status
    FROM iot_latest_readings
    ORDER BY device_type, location
""")

display(latest_readings)

# COMMAND ----------

# ============================================================================
# WIDGET 2: Total Data Summary
# ============================================================================

print("\n📊 DATA SUMMARY")
print("=" * 80)

summary = spark.sql("""
    SELECT
        device_type,
        COUNT(DISTINCT device_id) as num_devices,
        COUNT(*) as total_readings,
        ROUND(MIN(value), 2) as min_value,
        ROUND(MAX(value), 2) as max_value,
        ROUND(AVG(value), 2) as avg_value,
        MAX(timestamp) as latest_timestamp
    FROM iot_sensor_data
    GROUP BY device_type
    ORDER BY device_type
""")

display(summary)

# COMMAND ----------

# ============================================================================
# WIDGET 3: Temperature Trends (Last 2 Hours)
# ============================================================================

print("\n🌡️  TEMPERATURE TRENDS (Last 2 Hours)")
print("=" * 80)

temp_trends = spark.sql("""
    SELECT
        timestamp,
        device_name,
        location,
        value as temperature,
        unit
    FROM iot_sensor_data
    WHERE device_type = 'temperature'
      AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
    ORDER BY timestamp DESC, device_name
    LIMIT 100
""")

display(temp_trends)

# COMMAND ----------

# ============================================================================
# WIDGET 4: Humidity Trends (Last 2 Hours)
# ============================================================================

print("\n💧 HUMIDITY TRENDS (Last 2 Hours)")
print("=" * 80)

humidity_trends = spark.sql("""
    SELECT
        timestamp,
        device_name,
        location,
        value as humidity,
        unit
    FROM iot_sensor_data
    WHERE device_type = 'humidity'
      AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
    ORDER BY timestamp DESC, device_name
    LIMIT 100
""")

display(humidity_trends)

# COMMAND ----------

# ============================================================================
# WIDGET 5: Soil Moisture (Last 2 Hours)
# ============================================================================

print("\n🌱 SOIL MOISTURE (Last 2 Hours)")
print("=" * 80)

moisture_trends = spark.sql("""
    SELECT
        timestamp,
        device_name,
        location,
        value as soil_moisture,
        unit
    FROM iot_sensor_data
    WHERE device_type = 'soil_moisture'
      AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
    ORDER BY timestamp DESC
    LIMIT 100
""")

display(moisture_trends)

# COMMAND ----------

# ============================================================================
# WIDGET 6: Device Activity Status
# ============================================================================

print("\n🔌 DEVICE ACTIVITY STATUS")
print("=" * 80)

device_status = spark.sql("""
    SELECT
        device_id,
        device_name,
        device_type,
        location,
        COUNT(*) as readings_last_hour,
        MAX(timestamp) as last_reading,
        ROUND(DATEDIFF(MINUTE, MAX(timestamp), CURRENT_TIMESTAMP()), 1) as minutes_since_last,
        CASE
            WHEN DATEDIFF(MINUTE, MAX(timestamp), CURRENT_TIMESTAMP()) > 5 THEN '🔴 OFFLINE'
            WHEN DATEDIFF(MINUTE, MAX(timestamp), CURRENT_TIMESTAMP()) > 2 THEN '🟡 SLOW'
            ELSE '🟢 ONLINE'
        END as status
    FROM iot_sensor_data
    WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
    GROUP BY device_id, device_name, device_type, location
    ORDER BY status, device_name
""")

display(device_status)

# COMMAND ----------

# ============================================================================
# WIDGET 7: Hourly Statistics
# ============================================================================

print("\n📈 HOURLY STATISTICS (Last Hour)")
print("=" * 80)

hourly_stats = spark.sql("""
    SELECT
        device_name,
        device_type,
        location,
        COUNT(*) as sample_count,
        ROUND(MIN(value), 2) as min_value,
        ROUND(MAX(value), 2) as max_value,
        ROUND(AVG(value), 2) as avg_value,
        ROUND(STDDEV(value), 2) as std_dev
    FROM iot_sensor_data
    WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
    GROUP BY device_name, device_type, location
    ORDER BY device_type, location
""")

display(hourly_stats)

# COMMAND ----------

# ============================================================================
# WIDGET 8: Data Quality Metrics
# ============================================================================

print("\n✅ DATA QUALITY METRICS")
print("=" * 80)

quality = spark.sql("""
    SELECT
        device_id,
        device_name,
        COUNT(*) as total_readings,
        COUNT(DISTINCT DATE(timestamp)) as days_with_data,
        MAX(timestamp) as latest_reading,
        DATEDIFF(SECOND, MIN(timestamp), MAX(timestamp)) as time_span_seconds
    FROM iot_sensor_data
    GROUP BY device_id, device_name
    ORDER BY device_id
""")

display(quality)

# COMMAND ----------

print("\n" + "=" * 80)
print("✨ Dashboard loaded! Run this notebook frequently to refresh data.")
print("=" * 80)
