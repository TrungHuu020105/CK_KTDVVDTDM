#!/usr/bin/env python3
"""
Script to populate IoT sensor metrics data into the metrics.db database
Generates realistic historical metrics for IoT sensors
"""

import sqlite3
from datetime import datetime, timedelta, timezone
import random

# Database configuration
DB_PATH = "metrics.db"
VIETNAM_TZ = timezone(timedelta(hours=7))

# Metric types and their realistic ranges (IoT sensors only)
METRIC_CONFIGS = {
    "temperature": {
        "sources": ["sensor_1", "sensor_2"],
        "range": (15, 35),
        "unit": "°C"
    },
    "humidity": {
        "sources": ["sensor_2", "sensor_3"],
        "range": (30, 90),
        "unit": "%"
    },
    "soil_moisture": {
        "sources": ["sensor_4"],
        "range": (0, 100),
        "unit": "%"
    },
    "light_intensity": {
        "sources": ["sensor_3", "sensor_5"],
        "range": (0, 1000),
        "unit": "lux"
    },
    "pressure": {
        "sources": ["sensor_1"],
        "range": (900, 1100),
        "unit": "hPa"
    }
}

def generate_metrics(days=14, records_per_hour=6):
    """
    Generate metrics data for the last N days
    
    Args:
        days: Number of days of historical data to generate (default: 14)
        records_per_hour: Number of metric records to generate per hour (default: 6 for better distribution)
    
    Returns:
        List of metric records (tuples)
    """
    metrics = []
    end_time = datetime.now(VIETNAM_TZ)
    start_time = end_time - timedelta(days=days)
    
    current_time = start_time
    
    while current_time <= end_time:
        # Generate multiple records per hour for variability
        for _ in range(records_per_hour):
            # Pick random metric type
            metric_type = random.choice(list(METRIC_CONFIGS.keys()))
            config = METRIC_CONFIGS[metric_type]
            
            # Pick random source for this metric type
            source = random.choice(config["sources"])
            
            # Generate value with MORE variation for diversity
            min_val, max_val = config["range"]
            # Increased variation amplitude
            variation = (max_val - min_val) / 3  # More range of values
            value = random.uniform(min_val, max_val)
            value = max(min_val, min(max_val, value))
            value = round(value, 2)
            
            # Distribute timestamps evenly across the hour (not just at end)
            # Generate 6 records per hour = each gets ~10 min interval
            minute_offset = random.randint(0, 59)  # Full range across the hour
            second_offset = random.randint(0, 59)
            timestamp = current_time + timedelta(minutes=minute_offset, seconds=second_offset)
            
            metrics.append((
                metric_type,
                value,
                source,
                timestamp.isoformat()
            ))
        
        current_time += timedelta(hours=1)
    
    return metrics

def insert_metrics_to_db(metrics):
    """
    Insert metrics into the database
    
    Args:
        metrics: List of metric records (tuples)
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Insert metrics
        cursor.executemany(
            """
            INSERT INTO metrics (metric_type, value, source, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            metrics
        )
        
        conn.commit()
        count = cursor.rowcount
        conn.close()
        
        return count
    except Exception as e:
        print(f"❌ Error inserting metrics: {e}")
        return 0

def get_metrics_count():
    """Get current count of metrics in database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM metrics")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except Exception as e:
        print(f"❌ Error counting metrics: {e}")
        return 0

def main():
    print("=" * 60)
    print("📊 Metrics Data Population Script")
    print("=" * 60)
    
    # Check current metrics count
    current_count = get_metrics_count()
    print(f"\n📈 Current metrics in database: {current_count}")
    
    # Generate metrics
    print("\n🔄 Generating metrics data...")
    days = 14  # Generate 14 days of historical data for better distribution
    records_per_hour = 6  # 6 records per hour for more variety and better 24h coverage
    
    metrics = generate_metrics(days=days, records_per_hour=records_per_hour)
    print(f"✓ Generated {len(metrics)} metric records")
    
    # Display sample data
    print("\n📋 Sample data (first 5 records):")
    print("-" * 60)
    print(f"{'Metric Type':<20} {'Value':<10} {'Source':<15} {'Timestamp':<20}")
    print("-" * 60)
    for metric in metrics[:5]:
        print(f"{metric[0]:<20} {metric[1]:<10} {metric[2]:<15} {metric[3]:<20}")
    print("-" * 60)
    
    # Insert into database
    print("\n💾 Inserting into database...")
    inserted_count = insert_metrics_to_db(metrics)
    
    if inserted_count > 0:
        print(f"✓ Successfully inserted {inserted_count} records")
        new_count = get_metrics_count()
        print(f"✓ Total metrics now: {new_count}")
        
        print("\n" + "=" * 60)
        print("✅ Metrics data population completed successfully!")
        print("=" * 60)
        
        # Display metric type distribution
        print("\n📊 Data distribution by metric type:")
        print("-" * 60)
        for metric_type in METRIC_CONFIGS.keys():
            count = sum(1 for m in metrics if m[0] == metric_type)
            if count > 0:
                config = METRIC_CONFIGS[metric_type]
                print(f"  • {metric_type:<20} {count:>6} records ({config['unit']})")
        print("-" * 60)
    else:
        print("❌ Failed to insert metrics")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
