# Databricks notebook source
# ============================================================================
# IoT Data Streaming - Continuous Fake Data Generation with PySpark
# ============================================================================
# Purpose: Generate realistic fake IoT sensor data (Temperature, Humidity, Soil Moisture)
#          and stream to Delta Lake in real-time
# Location: /Workspace/Repos/Shared/metrics-pulse/databricks_iot_streaming.py
# ============================================================================

# COMMAND ----------

# ============================================================================
# PART 1: Initialize Catalog, Schema & Configuration
# ============================================================================

import os
import json
import time
from datetime import datetime, timedelta
import random
import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Create catalog and schema
catalog = "workspace"
schema = "metrics_app_streaming"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE {catalog}.{schema}")

print(f"✅ Catalog '{catalog}' and Schema '{schema}' ready")

# COMMAND ----------

# ============================================================================
# PART 2: Define IoT Sensor Definitions
# ============================================================================

IOT_DEVICES = {
    "living_room_temp": {
        "device_id": "LR_TEMP_001",
        "name": "Living Room Temperature",
        "type": "temperature",
        "location": "Living Room",
        "unit": "°C",
        "min_value": 15,
        "max_value": 28,
        "mean_value": 22,
        "std_dev": 2
    },
    "bedroom_humidity": {
        "device_id": "BR_HUM_001",
        "name": "Bedroom Humidity",
        "type": "humidity",
        "location": "Bedroom",
        "unit": "%",
        "min_value": 30,
        "max_value": 70,
        "mean_value": 50,
        "std_dev": 8
    },
    "garden_soil_moisture": {
        "device_id": "GD_SOIL_001",
        "name": "Garden Soil Moisture",
        "type": "soil_moisture",
        "location": "Garden",
        "unit": "%",
        "min_value": 20,
        "max_value": 80,
        "mean_value": 50,
        "std_dev": 10
    },
    "outdoor_temp": {
        "device_id": "OUT_TEMP_001",
        "name": "Outdoor Temperature",
        "type": "temperature",
        "location": "Outdoor",
        "unit": "°C",
        "min_value": 5,
        "max_value": 35,
        "mean_value": 20,
        "std_dev": 5
    },
    "kitchen_humidity": {
        "device_id": "KT_HUM_001",
        "name": "Kitchen Humidity",
        "type": "humidity",
        "location": "Kitchen",
        "unit": "%",
        "min_value": 30,
        "max_value": 75,
        "mean_value": 55,
        "std_dev": 10
    }
}

print(f"✅ Defined {len(IOT_DEVICES)} IoT devices")
for key, device in IOT_DEVICES.items():
    print(f"   - {device['device_id']}: {device['name']}")

# COMMAND ----------

# ============================================================================
# PART 3: Create or Replace Delta Lake Tables
# ============================================================================

# Table 1: IoT Sensor Raw Data (append-only, streaming)
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.iot_sensor_data")
spark.sql(f"""
    CREATE TABLE {catalog}.{schema}.iot_sensor_data (
        timestamp TIMESTAMP,
        device_id STRING,
        device_name STRING,
        device_type STRING,
        location STRING,
        value DOUBLE,
        unit STRING,
        batch_id STRING,
        _processing_time TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (device_type)
""")

# Table 2: Latest sensor readings (curated, for fast queries)
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.iot_latest_readings")
spark.sql(f"""
    CREATE TABLE {catalog}.{schema}.iot_latest_readings (
        device_id STRING,
        device_name STRING,
        device_type STRING,
        location STRING,
        latest_value DOUBLE,
        unit STRING,
        last_update TIMESTAMP,
        status STRING
    )
    USING DELTA
""")

# Table 3: Device metadata
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.iot_device_metadata")
spark.sql(f"""
    CREATE TABLE {catalog}.{schema}.iot_device_metadata (
        device_id STRING,
        device_name STRING,
        device_type STRING,
        location STRING,
        unit STRING,
        min_value DOUBLE,
        max_value DOUBLE,
        mean_value DOUBLE,
        std_dev DOUBLE,
        active BOOLEAN,
        created_at TIMESTAMP
    )
    USING DELTA
""")

# Table 4: Alerts (threshold violations)
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.iot_alerts")
spark.sql(f"""
    CREATE TABLE {catalog}.{schema}.iot_alerts (
        alert_id STRING,
        device_id STRING,
        device_name STRING,
        alert_type STRING,
        value DOUBLE,
        threshold DOUBLE,
        message STRING,
        created_at TIMESTAMP
    )
    USING DELTA
""")

print("✅ All Delta Lake tables created successfully")

# COMMAND ----------

# ============================================================================
# PART 4: Populate Device Metadata Table
# ============================================================================

metadata_rows = []
for key, device in IOT_DEVICES.items():
    metadata_rows.append({
        "device_id": device["device_id"],
        "device_name": device["name"],
        "device_type": device["type"],
        "location": device["location"],
        "unit": device["unit"],
        "min_value": device["min_value"],
        "max_value": device["max_value"],
        "mean_value": device["mean_value"],
        "std_dev": device["std_dev"],
        "active": True,
        "created_at": datetime.now()
    })

# Define schema matching the table structure
metadata_schema = StructType([
    StructField("device_id", StringType()),
    StructField("device_name", StringType()),
    StructField("device_type", StringType()),
    StructField("location", StringType()),
    StructField("unit", StringType()),
    StructField("min_value", DoubleType()),
    StructField("max_value", DoubleType()),
    StructField("mean_value", DoubleType()),
    StructField("std_dev", DoubleType()),
    StructField("active", BooleanType()),
    StructField("created_at", TimestampType())
])

metadata_df = spark.createDataFrame(metadata_rows, schema=metadata_schema)
metadata_df.write.mode("overwrite").option("mergeSchema", "false").saveAsTable("iot_device_metadata")

print("✅ Device metadata populated")
spark.sql("SELECT * FROM iot_device_metadata").show()

# COMMAND ----------

# ============================================================================
# PART 5: Sensor Data Generator Function (Realistic + Trending)
# ============================================================================

class IoTSensorSimulator:
    """Simulates realistic IoT sensor readings with noise and trends"""
    
    def __init__(self, device_config):
        self.device_id = device_config["device_id"]
        self.name = device_config["name"]
        self.type = device_config["type"]
        self.location = device_config["location"]
        self.unit = device_config["unit"]
        self.min_val = device_config["min_value"]
        self.max_val = device_config["max_value"]
        self.mean = device_config["mean_value"]
        self.std_dev = device_config["std_dev"]
        self.current_value = self.mean
        self.drift = random.gauss(0, 0.5)  # Trend component
    
    def generate_reading(self):
        """Generate next sensor reading with realistic behavior"""
        # Add random noise (Gaussian)
        noise = random.gauss(0, self.std_dev)
        
        # Add drift (slow trend changes)
        self.drift = self.drift * 0.95 + random.gauss(0, 0.2)
        
        # Combine components
        reading = self.current_value + noise + self.drift
        
        # Clamp to realistic range
        reading = max(self.min_val, min(self.max_val, reading))
        
        # Update current value for next reading
        self.current_value = reading * 0.7 + self.mean * 0.3  # Mean reversion
        
        return round(reading, 2)

# Create simulator instances
simulators = {key: IoTSensorSimulator(config) for key, config in IOT_DEVICES.items()}

print(f"✅ Created {len(simulators)} sensor simulators")

# COMMAND ----------

# ============================================================================
# PART 6: Generate Streaming Data Batch (Run Every Minute)
# ============================================================================

def generate_iot_batch(batch_number=1):
    """Generate one batch of IoT sensor readings (one sample per device)"""
    
    readings = []
    current_time = datetime.now()
    batch_id = str(uuid.uuid4())
    
    for key, simulator in simulators.items():
        value = simulator.generate_reading()
        
        readings.append({
            "timestamp": current_time,
            "device_id": simulator.device_id,
            "device_name": simulator.name,
            "device_type": simulator.type,
            "location": simulator.location,
            "value": value,
            "unit": simulator.unit,
            "batch_id": batch_id,
            "_processing_time": datetime.now()
        })
    
    return readings

# Generate first batch
print("🔧 Generating first batch of sensor data...")
batch_data = generate_iot_batch(1)

# Create DataFrame from batch
batch_schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("device_id", StringType()),
    StructField("device_name", StringType()),
    StructField("device_type", StringType()),
    StructField("location", StringType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType()),
    StructField("batch_id", StringType()),
    StructField("_processing_time", TimestampType())
])

batch_df = spark.createDataFrame(batch_data, schema=batch_schema)

# Append to Delta Lake
batch_df.write.mode("append").saveAsTable("iot_sensor_data")

print(f"✅ Batch inserted ({len(batch_data)} readings)")

# COMMAND ----------

# ============================================================================
# PART 7: Update Latest Readings View (Materialized)
# ============================================================================

latest_df = spark.sql(f"""
    WITH latest_per_device AS (
        SELECT
            device_id,
            device_name,
            device_type,
            location,
            value,
            unit,
            timestamp,
            ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY timestamp DESC) as rn
        FROM iot_sensor_data
    )
    SELECT
        l.device_id,
        l.device_name,
        l.device_type,
        l.location,
        l.value as latest_value,
        l.unit,
        l.timestamp as last_update,
        CASE
            WHEN l.value < m.min_value THEN 'BELOW_MIN'
            WHEN l.value > m.max_value THEN 'ABOVE_MAX'
            ELSE 'NORMAL'
        END as status
    FROM latest_per_device l
    LEFT JOIN iot_device_metadata m ON l.device_id = m.device_id
    WHERE l.rn = 1
""")

latest_df.write.mode("overwrite").saveAsTable("iot_latest_readings")

print("✅ Latest readings view updated")

# COMMAND ----------

# ============================================================================
# PART 8: Continuous Data Generation Loop
# ============================================================================

# This will run continuously and generate new data every 10 seconds
iteration = 0
max_iterations = 10  # Reduced for testing (10 iterations = ~100 seconds)

print("🚀 Starting continuous data generation...")
print(f"📊 Generating {len(IOT_DEVICES)} devices × 1 reading = {len(IOT_DEVICES)} readings per batch")
print(f"⏱️  Running for {max_iterations} iterations (check dashboards for live updates)")

for iteration in range(1, max_iterations + 1):
    try:
        # Generate batch
        batch_data = generate_iot_batch(iteration)
        batch_df = spark.createDataFrame(batch_data, schema=batch_schema)
        
        # Append to Delta Lake
        batch_df.write.mode("append").saveAsTable("iot_sensor_data")
        
        # Update latest readings
        latest_df = spark.sql(f"""
            WITH latest_per_device AS (
                SELECT
                    device_id,
                    device_name,
                    device_type,
                    location,
                    value,
                    unit,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY timestamp DESC) as rn
                FROM iot_sensor_data
            )
            SELECT
                l.device_id,
                l.device_name,
                l.device_type,
                l.location,
                l.value as latest_value,
                l.unit,
                l.timestamp as last_update,
                CASE
                    WHEN l.value < m.min_value THEN 'BELOW_MIN'
                    WHEN l.value > m.max_value THEN 'ABOVE_MAX'
                    ELSE 'NORMAL'
                END as status
            FROM latest_per_device l
            LEFT JOIN iot_device_metadata m ON l.device_id = m.device_id
            WHERE l.rn = 1
        """)
        
        latest_df.write.mode("overwrite").saveAsTable("iot_latest_readings")
        
        # Print progress every 60 iterations
        if iteration % 60 == 0:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            total_readings = spark.sql("SELECT COUNT(*) as cnt FROM iot_sensor_data").collect()[0]["cnt"]
            print(f"[{current_time}] Iteration {iteration}: {total_readings} total readings generated ✅")
        
        # Wait 10 seconds before next batch
        time.sleep(10)
        
    except Exception as e:
        print(f"❌ Error at iteration {iteration}: {str(e)}")
        time.sleep(5)
        continue

print(f"✅ Streaming completed after {iteration} iterations")

# COMMAND ----------

# ============================================================================
# PART 9: View Summary Statistics
# ============================================================================

# Ensure we're in the right schema context
spark.sql(f"USE {catalog}.{schema}")

print("=" * 80)
print("SUMMARY: IoT Sensor Data Generated")
print("=" * 80)

summary = spark.sql(f"""
    SELECT
        device_id,
        device_name,
        device_type,
        COUNT(*) as total_readings,
        ROUND(MIN(value), 2) as min_value,
        ROUND(MAX(value), 2) as max_value,
        ROUND(AVG(value), 2) as avg_value,
        ROUND(STDDEV(value), 2) as stddev,
        MAX(timestamp) as last_reading_time
    FROM iot_sensor_data
    GROUP BY device_id, device_name, device_type
    ORDER BY device_id
""")

summary.show(truncate=False)

print("✅ All streaming data ready for visualization!")
