"""
Production-grade IoT Sensor Data Generator

Features:
- Mỗi sensor sinh đúng 1 metric (1-to-1 mapping)
- Stateful random walk (dữ liệu mượt)
- Smart storage filtering (threshold + time-based)
- Bulk insert optimization
- Detailed logging per batch

Usage:
    python generate_iot_data.py                      # Run once (1 batch = 5 metrics)
    python generate_iot_data.py --continuous        # Continuous (5s interval)
    python generate_iot_data.py --continuous --interval 10  # Custom interval
"""

import os
import sys
import time
import math
import random
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, List

sys.path.insert(0, '.')

from app.schemas import MetricCreate, MetricBulkCreate
from app.crud import create_metrics_bulk
from app.database import SessionLocal, init_db, switch_to_sqlite_fallback


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


FAKE_ALWAYS_SAVE = _env_bool("FAKE_ALWAYS_SAVE", True)
FAKE_SAVE_INTERVAL_SECONDS = _env_int("FAKE_SAVE_INTERVAL_SECONDS", 2)
FAKE_TEMP_DELTA_THRESHOLD = _env_float("FAKE_TEMP_DELTA_THRESHOLD", 0)
FAKE_HUMIDITY_DELTA_THRESHOLD = _env_float("FAKE_HUMIDITY_DELTA_THRESHOLD", 0)


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class SensorConfig:
    """Configuration for each sensor"""
    metric_type: str
    source: str
    min_value: float
    max_value: float
    step_size: float
    save_threshold: float
    max_save_interval: int
    unit: str
    trend_enabled: bool = False
    trend_amplitude: float = 0.0


# Sensor configurations (1 sensor = 1 metric type)
SENSORS = [
    SensorConfig(
        metric_type="temperature",
        source="sensor_1",
        min_value=28.0,
        max_value=35.0,
        step_size=0.5,          # Increased step size for better variation
        save_threshold=0.5,
        max_save_interval=300,
        unit="°C",
        trend_enabled=True,
        trend_amplitude=3.0     # Reduced from 5.0 to avoid clamping at max
    ),
    SensorConfig(
        metric_type="humidity",
        source="sensor_1",
        min_value=40.0,
        max_value=80.0,
        step_size=1.5,          # Increased step size
        save_threshold=2.0,
        max_save_interval=300,
        unit="%",
        trend_enabled=True,
        trend_amplitude=5.0     # Reduced from 10.0 to avoid clamping
    ),
    SensorConfig(
        metric_type="temperature",
        source="sensor_2",
        min_value=27.0,
        max_value=34.0,
        step_size=0.45,
        save_threshold=0.5,
        max_save_interval=300,
        unit="Â°C",
        trend_enabled=True,
        trend_amplitude=2.5
    ),
    SensorConfig(
        metric_type="humidity",
        source="sensor_2",
        min_value=45.0,
        max_value=85.0,
        step_size=1.4,
        save_threshold=2.0,
        max_save_interval=300,
        unit="%",
        trend_enabled=True,
        trend_amplitude=4.5
    ),
    SensorConfig(
        metric_type="soil_moisture",
        source="sensor_3",
        min_value=0.0,
        max_value=100.0,
        step_size=3.0,          # Increased from 2.0
        save_threshold=3.0,
        max_save_interval=600,
        unit="%",
        trend_enabled=False,
        trend_amplitude=0.0
    ),
    SensorConfig(
        metric_type="light_intensity",
        source="sensor_4",
        min_value=200.0,        # Changed from 0.0 (avoid constant 0)
        max_value=900.0,        # Changed from 1000.0 (avoid constant max)
        step_size=80.0,         # Increased from 50.0
        save_threshold=100.0,
        max_save_interval=300,
        unit="lux",
        trend_enabled=True,
        trend_amplitude=200.0   # Reduced from 500.0 significantly
    ),
    SensorConfig(
        metric_type="pressure",
        source="sensor_5",
        min_value=900.0,
        max_value=1100.0,
        step_size=0.5,
        save_threshold=1.0,
        max_save_interval=600,
        unit="hPa",
        trend_enabled=False,
        trend_amplitude=0.0
    ),
]


# ============================================================================
# STATE MANAGEMENT
# ============================================================================

@dataclass
class SensorState:
    """State for a single sensor"""
    metric_type: str
    source: str
    last_generated_value: float
    last_saved_value: float
    last_saved_timestamp: datetime
    generated_count: int = 0
    saved_count: int = 0
    dropped_count: int = 0


class StateManager:
    """Manage state for all sensors"""
    
    def __init__(self):
        self.states: Dict[str, SensorState] = {}

    def _key(self, source: str, metric_type: str) -> str:
        return f"{source}:{metric_type}"
    
    def initialize(self, config: SensorConfig, initial_value: float):
        """Initialize sensor with random initial value"""
        now = datetime.now(timezone(timedelta(hours=7)))
        self.states[self._key(config.source, config.metric_type)] = SensorState(
            metric_type=config.metric_type,
            source=config.source,
            last_generated_value=initial_value,
            last_saved_value=initial_value,
            last_saved_timestamp=now
        )
    
    def get_state(self, source: str, metric_type: str) -> Optional[SensorState]:
        """Get state for a sensor"""
        return self.states.get(self._key(source, metric_type))
    
    def update_generated(self, source: str, metric_type: str, value: float):
        """Update after generation"""
        state = self.get_state(source, metric_type)
        if state:
            state.last_generated_value = value
            state.generated_count += 1
    
    def update_saved(self, source: str, metric_type: str, value: float):
        """Update after saving"""
        state = self.get_state(source, metric_type)
        if state:
            state.last_saved_value = value
            state.last_saved_timestamp = datetime.now(timezone(timedelta(hours=7)))
            state.saved_count += 1
    
    def mark_dropped(self, source: str, metric_type: str):
        """Mark when data is dropped"""
        state = self.get_state(source, metric_type)
        if state:
            state.dropped_count += 1
    
    def get_all_states(self) -> List[SensorState]:
        """Get all states"""
        return list(self.states.values())


# ============================================================================
# DATA GENERATION
# ============================================================================

def get_time_trend(config: SensorConfig) -> float:
    """
    Calculate time-based trend (sine wave for daily cycle).
    Peak at 14:00, minimum at 02:00
    """
    if not config.trend_enabled:
        return 0.0
    
    now = datetime.now(timezone(timedelta(hours=7)))
    hour = now.hour + now.minute / 60.0
    phase = (hour - 14) / 24.0 * (2 * math.pi)
    trend = config.trend_amplitude * math.sin(phase)
    
    return trend


def clamp(value: float, config: SensorConfig) -> float:
    """Clamp value to min/max range"""
    return max(config.min_value, min(config.max_value, value))


def generate_value(config: SensorConfig, state: SensorState) -> float:
    """Generate realistic sensor value using random walk with boundary reflection"""
    # Random walk with direction bias to avoid getting stuck at boundaries
    random_change = random.uniform(-config.step_size, config.step_size)
    
    # Check if we're too close to boundaries - if so, force bounce back
    range_size = config.max_value - config.min_value
    current_pos = state.last_generated_value
    distance_to_max = config.max_value - current_pos
    distance_to_min = current_pos - config.min_value
    
    # If too close to max, force STRONG negative to bounce back
    if distance_to_max < range_size * 0.20:  # Within 20% of max
        random_change = random.uniform(-config.step_size * 2.0, -config.step_size * 0.8)
    # If too close to min, force STRONG positive to bounce back
    elif distance_to_min < range_size * 0.20:  # Within 20% of min
        random_change = random.uniform(config.step_size * 0.8, config.step_size * 2.0)
    
    # Time trend (but disable when near boundaries to allow escape)
    if distance_to_max > range_size * 0.25 and distance_to_min > range_size * 0.25:
        trend = get_time_trend(config)
    else:
        trend = 0.0  # Disable trend when near boundary to allow escape
    
    # New value
    new_value = state.last_generated_value + random_change + trend
    
    # Clamp to limits
    new_value = clamp(new_value, config)
    if config.source == "sensor_1" and config.metric_type == "temperature":
        new_value = max(28.0, new_value)
    
    return round(new_value, 2)


def should_save(config: SensorConfig, state: SensorState, new_value: float) -> Tuple[bool, str]:
    """Decide if should save"""
    if config.source == "sensor_1":
        if FAKE_ALWAYS_SAVE:
            return True, "fake_always_save=true"

        save_threshold = config.save_threshold
        if config.metric_type == "temperature":
            save_threshold = FAKE_TEMP_DELTA_THRESHOLD
        elif config.metric_type == "humidity":
            save_threshold = FAKE_HUMIDITY_DELTA_THRESHOLD
        max_save_interval = FAKE_SAVE_INTERVAL_SECONDS
    else:
        save_threshold = config.save_threshold
        max_save_interval = config.max_save_interval

    # Check 1: Threshold-based
    value_change = abs(new_value - state.last_saved_value)
    if value_change >= save_threshold:
        return True, f"change={value_change:.2f}>={save_threshold}"
    
    # Check 2: Time-based fallback
    now = datetime.now(timezone(timedelta(hours=7)))
    time_since_save = (now - state.last_saved_timestamp).total_seconds()
    
    if time_since_save >= max_save_interval:
        return True, f"time={time_since_save:.0f}s>={max_save_interval}s"
    
    return False, f"filtered(Δ={value_change:.2f}, t={time_since_save:.0f}s)"


# ============================================================================
# MAIN GENERATOR
# ============================================================================

class IoTDataGenerator:
    """Main IoT data generator"""
    
    def __init__(self, forced_sources: Optional[List[str]] = None):
        self.state_manager = StateManager()
        self.initialized = False
        self.active_sensors = []  # Only active sensors
        self.run_count = 0  # Track runs for periodic re-check
        self.using_sqlite_fallback = False
        normalized_sources = []
        for source in forced_sources or []:
            value = str(source or "").strip().lower()
            if value and value not in normalized_sources:
                normalized_sources.append(value)
        self.forced_sources = normalized_sources or None

    def _ensure_database_ready(self):
        try:
            init_db()
        except Exception as exc:
            if self.using_sqlite_fallback:
                raise
            print(f"[WARN] Primary DB unavailable for generator: {type(exc).__name__}: {exc}")
            switch_to_sqlite_fallback()
            self.using_sqlite_fallback = True
            init_db()
    
    def initialize(self):
        """Initialize sensors from database (only active devices)"""
        self._ensure_database_ready()
        db = SessionLocal()
        try:
            # Clear previous sensors when re-initializing
            self.active_sensors = []
            self.state_manager.states = {}

            if self.forced_sources is not None:
                forced_source_set = set(self.forced_sources)
                active_devices_to_use = []
                loaded_keys = set()
                for config in SENSORS:
                    if config.source not in forced_source_set:
                        continue
                    key = (config.source, config.metric_type)
                    if key in loaded_keys:
                        continue
                    active_devices_to_use.append(config)
                    loaded_keys.add(key)
                print(
                    "[INFO] Using forced fake sources for generator: "
                    f"{', '.join(self.forced_sources) if self.forced_sources else '(none)'}"
                )
            else:
                # Query active devices from both legacy Device and user-owned IoTDevice tables.
                from app.models import Device, IoTDevice
                active_devices = db.query(Device).filter(Device.is_active == True).all()
                active_iot_devices = db.query(IoTDevice).filter(IoTDevice.is_active == True).all()
                active_devices = list({d.source: d for d in [*active_devices, *active_iot_devices]}.values())
                
                if not active_devices:
                    if self.using_sqlite_fallback:
                        active_devices_to_use = [
                            config for config in SENSORS
                            if config.source in {"sensor_1"}
                        ]
                        print("[INFO] No local devices found. Using default fake sensor_1 for local dev.")
                    else:
                        active_devices_to_use = []
                        print("[INFO] No active devices found. Data generation disabled until admin enables sensors.")
                else:
                    # One active fake source can emit multiple metric types.
                    active_devices_to_use = []
                    loaded_keys = set()
                    for device in active_devices:
                        matched = False
                        for config in SENSORS:
                            if config.source == device.source:
                                matched = True
                                key = (config.source, config.metric_type)
                                if key not in loaded_keys:
                                    active_devices_to_use.append(config)
                                    loaded_keys.add(key)

                        if matched:
                            print(f"[OK] Loaded active device: {device.name} ({device.source})")
                        else:
                            print(f"[WARNING] Device {device.name} ({device.source}) not in SENSORS config, skipping")

            # Initialize only active sensors
            for config in active_devices_to_use:
                initial_value = random.uniform(config.min_value, config.max_value)
                initial_value = round(initial_value, 2)
                self.state_manager.initialize(config, initial_value)
                self.active_sensors.append(config)

            print(f"[OK] Initialized {len(self.active_sensors)} active sensors for data generation")
        
        except Exception as e:
            print(f"[ERROR] Failed to initialize from DB: {str(e)}")
            print("[INFO] Data generation disabled - no active sensors")
            self.active_sensors = []
        finally:
            db.close()
        
        self.initialized = True
    
    def run_once(self, save_to_db: bool = True):
        """
        Generate one batch (1 metric per sensor = 5 metrics)
        
        Args:
            save_to_db: If True, save to database. If False, return metrics without saving.
        
        Returns:
            dict: {"metrics": [...batch details...]} when save_to_db=False
            None: when save_to_db=True (regular behavior)
        
        IMPORTANT: When save_to_db=False (streaming mode):
        - Return ALL generated metrics (no filtering)
        - Frontend receives 100% of data for smooth realtime display
        - Database only saves "important" data (filtered by threshold)
        """
        if not self.initialized:
            self.initialize()
        
        # Periodic re-check: every run, verify if devices were added/removed (5 seconds)
        self.run_count += 1
        if self.forced_sources is None and self.run_count % 1 == 0:  # Check EVERY run (5 seconds) for faster detection
            print(f"[DEBUG] Periodic check (run #{self.run_count})... checking for active devices changes")
            from app.database import SessionLocal
            from app.models import Device, IoTDevice
            db = SessionLocal()
            should_reinitialize = False
            try:
                device_sources = {d.source for d in db.query(Device).filter(Device.is_active == True).all()}
                iot_device_sources = {d.source for d in db.query(IoTDevice).filter(IoTDevice.is_active == True).all()}
                supported_sources = {config.source for config in SENSORS}
                current_active_sources = (device_sources | iot_device_sources) & supported_sources
                existing_sources = {s.source for s in self.active_sensors}
                
                # Re-initialize if:
                # 1. Number of active devices changed (increased or decreased)
                # 2. Active devices list changed (different sources)
                if current_active_sources != existing_sources:
                    print(f"[INFO] Detected device changes! DB: {current_active_sources}, Local: {existing_sources}. Re-initializing...")
                    should_reinitialize = True
            finally:
                db.close()
            if should_reinitialize:
                self.initialize()
        
        # IMPORTANT: If no active sensors, re-check database immediately
        # (user might have disabled all devices after startup)
        if self.forced_sources is None and not self.active_sensors:
            print("[DEBUG] No active sensors, re-checking database...")
            self.initialize()
        
        # Early exit if still no active sensors
        if not self.active_sensors:
            print("[INFO] No active sensors - skipping data generation")
            return None
        
        now = datetime.now(timezone(timedelta(hours=7)))
        timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")
        
        metrics_to_save = []
        batch_log = []
        all_metrics = []  # For return when save_to_db=False (ALL data, no filtering)
        
        # Generate 1 metric per active sensor ONLY
        for config in self.active_sensors:
            state = self.state_manager.get_state(config.source, config.metric_type)
            if not state:
                continue
            
            # Generate value
            value = generate_value(config, state)
            self.state_manager.update_generated(config.source, config.metric_type, value)
            
            # Decide save or drop (for DB filtering)
            should_save_flag, reason = should_save(config, state, value)
            
            if should_save_flag:
                metrics_to_save.append((config.metric_type, config.source, value))
                self.state_manager.update_saved(config.source, config.metric_type, value)
                status = "✅ SAVE"
            else:
                self.state_manager.mark_dropped(config.source, config.metric_type)
                status = "⏭️  SKIP"
            
            batch_log.append({
                "metric_type": config.metric_type,
                "source": config.source,
                "value": value,
                "status": status,
                "reason": reason
            })
            
            # Add to all_metrics for streaming (includes unit)
            # IMPORTANT: Include ALL metrics (even filtered ones) for realtime display
            all_metrics.append({
                "metric_type": config.metric_type,
                "source": config.source,
                "value": round(value, 2),
                "unit": config.unit,
                "timestamp": timestamp_str,
                "saved": should_save_flag,  # Flag for DB filtering decision
                "reason": reason
            })
        
        # If streaming mode (no DB save), return ALL metrics
        if not save_to_db:
            return {
                "metrics": all_metrics,  # ALL data for frontend
                "timestamp": timestamp_str,
                "generated": len(batch_log),
                "saved": len(metrics_to_save),
                "dropped": len(batch_log) - len(metrics_to_save)
            }
        
        # Write to database (normal mode)
        saved_count = self._write_batch(metrics_to_save)
        
        # Print batch log
        self._print_batch_log(timestamp_str, batch_log, len(metrics_to_save))
    
    def _write_batch(self, metrics_to_save: List[Tuple[str, str, float]]) -> int:
        """Write batch to database"""
        if not metrics_to_save:
            return 0
        
        db = SessionLocal()
        try:
            metric_creates = [
                MetricCreate(
                    metric_type=metric_type,
                    value=value,
                    source=source,
                    timestamp=None
                )
                for metric_type, source, value in metrics_to_save
            ]
            
            bulk_data = MetricBulkCreate(metrics=metric_creates)
            created = create_metrics_bulk(db, bulk_data.metrics)
            
            return len(created)
        except Exception as e:
            print(f"❌ DB Error: {e}")
            return 0
        finally:
            db.close()
    
    def _print_batch_log(self, timestamp_str: str, batch_log: list, saved_count: int):
        """Print batch statistics"""
        total = len(batch_log)
        dropped = total - saved_count
        
        print(f"\n[{timestamp_str}] Generated: {total}, Saved: {saved_count}, Dropped: {dropped}")
        print("-" * 120)
        
        for log in batch_log:
            print(
                f"  {log['status']} | {log['metric_type']:20} ({log['source']:10}) = "
                f"{log['value']:8.2f} | {log['reason']}"
            )
        
        print("")
    
    def run_continuous(self, interval: int = 5):
        """Run continuously"""
        if not self.initialized:
            self.initialize()
        
        print("\n" + "=" * 120)
        print("🚀 IoT DATA GENERATOR - CONTINUOUS MODE")
        print("=" * 120)
        print(f"📊 Sensors: {len(SENSORS)}")
        print(f"⏰ Interval: {interval}s")
        print("=" * 120)
        
        try:
            while True:
                self.run_once()
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n\n⚠️  Interrupted by user")
            self._print_summary()
    
    def _print_summary(self):
        """Print final summary"""
        print("\n" + "=" * 120)
        print("📋 FINAL SUMMARY")
        print("=" * 120)
        
        total_generated = 0
        total_saved = 0
        total_dropped = 0
        
        for state in self.state_manager.get_all_states():
            total_generated += state.generated_count
            total_saved += state.saved_count
            total_dropped += state.dropped_count
            
            pct = (state.saved_count / state.generated_count * 100) if state.generated_count > 0 else 0
            print(
                f"  {state.metric_type:20} ({state.source:10}) | "
                f"Gen: {state.generated_count:4} | "
                f"Saved: {state.saved_count:4} | "
                f"Dropped: {state.dropped_count:4} | "
                f"SavePct: {pct:5.1f}%"
            )
        
        if total_generated > 0:
            save_pct = (total_saved / total_generated * 100)
            print(f"\n  Overall: Generated={total_generated}, Saved={total_saved}, "
                  f"Dropped={total_dropped}, SavePct={save_pct:.1f}%")
        
        print("=" * 120 + "\n")


# ============================================================================
# ENTRY POINT & CLI
# ============================================================================

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Production-grade IoT Sensor Data Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python generate_iot_data.py                      # Generate 1 batch (5 metrics)
  python generate_iot_data.py --continuous         # Continuous (5s interval)
  python generate_iot_data.py --continuous --interval 10  # Custom interval
        """
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run in continuous mode (default: False)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Interval in seconds (default: 5)"
    )
    
    args = parser.parse_args()
    
    generator = IoTDataGenerator()
    
    if args.continuous:
        generator.run_continuous(interval=args.interval)
    else:
        generator.run_once()


if __name__ == "__main__":
    main()
