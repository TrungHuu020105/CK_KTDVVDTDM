# Databricks notebook source

"""Seed representative Vietnam province/location rows for Meteostat ingest.

This script adds representative Vietnam province/city locations to:
  - dim_province
  - dim_location

It uses MERGE so repeated runs do not create duplicates and it does not delete
existing ESP32 locations such as esp32_devkit_v1, sensor_4, or sensor_5.

Use LOCATION_SET=current_34 for the current 34 provincial-level units, or
LOCATION_SET=legacy_63 for the previous 63 province/city set.
"""

# COMMAND ----------

# Imports

import os
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F


# COMMAND ----------

# Constants and configuration

DEFAULTS = {
    "DATABRICKS_CATALOG": "dtdm",
    "DATABRICKS_SCHEMA": "metrics_app_streaming",
    "LOCATION_SET": "current_34",
    "UPDATE_ACTIVE_PROVINCES": "true",
}


# COMMAND ----------

# Environment and widget helpers

def load_local_env():
    candidates = []
    try:
        candidates.append(Path(__file__).resolve().parents[1] / ".env")
    except Exception:
        pass
    candidates.append(Path.cwd() / ".env")
    for env_path in candidates:
        if not env_path.exists():
            continue
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
        return


load_local_env()


def widget_value(name):
    try:
        value = dbutils.widgets.get(name).strip()  # type: ignore[name-defined]
        return value if value else None
    except Exception:
        return None


def setting(name):
    return os.getenv(name) or widget_value(name) or DEFAULTS.get(name, "")


def bool_setting(name):
    return str(setting(name)).strip().lower() in ("1", "true", "yes", "on")


def create_widgets():
    try:
        for name, default in DEFAULTS.items():
            dbutils.widgets.text(name, os.getenv(name, default))  # type: ignore[name-defined]
    except Exception:
        pass


def fq_table(name):
    return setting("DATABRICKS_CATALOG") + "." + setting("DATABRICKS_SCHEMA") + "." + name


def namespace_exists(catalog, schema):
    rows = spark.sql("SHOW SCHEMAS IN " + catalog + " LIKE '" + schema + "'").collect()  # type: ignore[name-defined]
    return len(rows) > 0


def table_exists(name):
    rows = spark.sql(  # type: ignore[name-defined]
        "SHOW TABLES IN "
        + setting("DATABRICKS_CATALOG")
        + "."
        + setting("DATABRICKS_SCHEMA")
        + " LIKE '"
        + name
        + "'"
    ).collect()
    return len(rows) > 0


def ensure_namespace():
    catalog = setting("DATABRICKS_CATALOG")
    schema = setting("DATABRICKS_SCHEMA")
    if not namespace_exists(catalog, schema):
        raise RuntimeError(
            "Schema "
            + catalog
            + "."
            + schema
            + " does not exist. Create it first with a managed location, or run "
            + "01_create_catalog_schema_tables.sql in an existing Unity Catalog schema."
        )


# COMMAND ----------

# Location seed data

LOCATIONS = [
    {"province_id": "hanoi", "province_name": "Ha Noi", "location_id": "loc_hanoi", "location_name": "Ha Noi", "latitude": 21.0278, "longitude": 105.8342, "altitude": 10.0, "region": "North"},
    {"province_id": "hcm", "province_name": "Ho Chi Minh City", "location_id": "loc_hcm", "location_name": "Ho Chi Minh City", "latitude": 10.8231, "longitude": 106.6297, "altitude": 5.0, "region": "South"},
    {"province_id": "haiphong", "province_name": "Hai Phong", "location_id": "loc_haiphong", "location_name": "Hai Phong", "latitude": 20.8449, "longitude": 106.6881, "altitude": 5.0, "region": "North"},
    {"province_id": "danang", "province_name": "Da Nang", "location_id": "loc_danang", "location_name": "Da Nang", "latitude": 16.0471, "longitude": 108.2068, "altitude": 8.0, "region": "Central"},
    {"province_id": "cantho", "province_name": "Can Tho", "location_id": "loc_cantho", "location_name": "Can Tho", "latitude": 10.0452, "longitude": 105.7469, "altitude": 3.0, "region": "South"},
    {"province_id": "angiang", "province_name": "An Giang", "location_id": "loc_angiang", "location_name": "Long Xuyen", "latitude": 10.3864, "longitude": 105.4352, "altitude": 4.0, "region": "South"},
    {"province_id": "bariavungtau", "province_name": "Ba Ria Vung Tau", "location_id": "loc_bariavungtau", "location_name": "Ba Ria", "latitude": 10.5417, "longitude": 107.2429, "altitude": 15.0, "region": "South"},
    {"province_id": "bacgiang", "province_name": "Bac Giang", "location_id": "loc_bacgiang", "location_name": "Bac Giang", "latitude": 21.2731, "longitude": 106.1946, "altitude": 20.0, "region": "North"},
    {"province_id": "backan", "province_name": "Bac Kan", "location_id": "loc_backan", "location_name": "Bac Kan", "latitude": 22.1470, "longitude": 105.8348, "altitude": 150.0, "region": "North"},
    {"province_id": "baclieu", "province_name": "Bac Lieu", "location_id": "loc_baclieu", "location_name": "Bac Lieu", "latitude": 9.2940, "longitude": 105.7216, "altitude": 2.0, "region": "South"},
    {"province_id": "bacninh", "province_name": "Bac Ninh", "location_id": "loc_bacninh", "location_name": "Bac Ninh", "latitude": 21.1861, "longitude": 106.0763, "altitude": 8.0, "region": "North"},
    {"province_id": "bentre", "province_name": "Ben Tre", "location_id": "loc_bentre", "location_name": "Ben Tre", "latitude": 10.2434, "longitude": 106.3756, "altitude": 2.0, "region": "South"},
    {"province_id": "binhdinh", "province_name": "Binh Dinh", "location_id": "loc_binhdinh", "location_name": "Quy Nhon", "latitude": 13.7820, "longitude": 109.2190, "altitude": 6.0, "region": "Central"},
    {"province_id": "binhduong", "province_name": "Binh Duong", "location_id": "loc_binhduong", "location_name": "Thu Dau Mot", "latitude": 10.9804, "longitude": 106.6519, "altitude": 20.0, "region": "South"},
    {"province_id": "binhphuoc", "province_name": "Binh Phuoc", "location_id": "loc_binhphuoc", "location_name": "Dong Xoai", "latitude": 11.5349, "longitude": 106.8832, "altitude": 90.0, "region": "South"},
    {"province_id": "binhthuan", "province_name": "Binh Thuan", "location_id": "loc_binhthuan", "location_name": "Phan Thiet", "latitude": 10.9333, "longitude": 108.1000, "altitude": 8.0, "region": "South Central"},
    {"province_id": "camau", "province_name": "Ca Mau", "location_id": "loc_camau", "location_name": "Ca Mau", "latitude": 9.1768, "longitude": 105.1524, "altitude": 1.0, "region": "South"},
    {"province_id": "caobang", "province_name": "Cao Bang", "location_id": "loc_caobang", "location_name": "Cao Bang", "latitude": 22.6666, "longitude": 106.2639, "altitude": 190.0, "region": "North"},
    {"province_id": "daklak", "province_name": "Dak Lak", "location_id": "loc_daklak", "location_name": "Buon Ma Thuot", "latitude": 12.6667, "longitude": 108.0500, "altitude": 536.0, "region": "Central Highlands"},
    {"province_id": "daknong", "province_name": "Dak Nong", "location_id": "loc_daknong", "location_name": "Gia Nghia", "latitude": 12.0042, "longitude": 107.6907, "altitude": 580.0, "region": "Central Highlands"},
    {"province_id": "dienbien", "province_name": "Dien Bien", "location_id": "loc_dienbien", "location_name": "Dien Bien Phu", "latitude": 21.3860, "longitude": 103.0230, "altitude": 475.0, "region": "North"},
    {"province_id": "dongnai", "province_name": "Dong Nai", "location_id": "loc_dongnai", "location_name": "Bien Hoa", "latitude": 10.9574, "longitude": 106.8427, "altitude": 30.0, "region": "South"},
    {"province_id": "dongthap", "province_name": "Dong Thap", "location_id": "loc_dongthap", "location_name": "Cao Lanh", "latitude": 10.4602, "longitude": 105.6329, "altitude": 2.0, "region": "South"},
    {"province_id": "gialai", "province_name": "Gia Lai", "location_id": "loc_gialai", "location_name": "Pleiku", "latitude": 13.9833, "longitude": 108.0000, "altitude": 740.0, "region": "Central Highlands"},
    {"province_id": "hagiang", "province_name": "Ha Giang", "location_id": "loc_hagiang", "location_name": "Ha Giang", "latitude": 22.8233, "longitude": 104.9836, "altitude": 100.0, "region": "North"},
    {"province_id": "hanam", "province_name": "Ha Nam", "location_id": "loc_hanam", "location_name": "Phu Ly", "latitude": 20.5411, "longitude": 105.9139, "altitude": 5.0, "region": "North"},
    {"province_id": "hatinh", "province_name": "Ha Tinh", "location_id": "loc_hatinh", "location_name": "Ha Tinh", "latitude": 18.3428, "longitude": 105.9057, "altitude": 5.0, "region": "Central"},
    {"province_id": "haiduong", "province_name": "Hai Duong", "location_id": "loc_haiduong", "location_name": "Hai Duong", "latitude": 20.9373, "longitude": 106.3146, "altitude": 5.0, "region": "North"},
    {"province_id": "haugiang", "province_name": "Hau Giang", "location_id": "loc_haugiang", "location_name": "Vi Thanh", "latitude": 9.7845, "longitude": 105.4701, "altitude": 2.0, "region": "South"},
]


# COMMAND ----------

# More legacy location seed data

LOCATIONS.extend([
    {"province_id": "hoabinh", "province_name": "Hoa Binh", "location_id": "loc_hoabinh", "location_name": "Hoa Binh", "latitude": 20.8172, "longitude": 105.3376, "altitude": 25.0, "region": "North"},
    {"province_id": "hungyen", "province_name": "Hung Yen", "location_id": "loc_hungyen", "location_name": "Hung Yen", "latitude": 20.6464, "longitude": 106.0511, "altitude": 5.0, "region": "North"},
    {"province_id": "khanhhoa", "province_name": "Khanh Hoa", "location_id": "loc_khanhhoa", "location_name": "Nha Trang", "latitude": 12.2388, "longitude": 109.1967, "altitude": 5.0, "region": "South Central"},
    {"province_id": "kiengiang", "province_name": "Kien Giang", "location_id": "loc_kiengiang", "location_name": "Rach Gia", "latitude": 10.0125, "longitude": 105.0809, "altitude": 1.0, "region": "South"},
    {"province_id": "kontum", "province_name": "Kon Tum", "location_id": "loc_kontum", "location_name": "Kon Tum", "latitude": 14.3545, "longitude": 108.0076, "altitude": 525.0, "region": "Central Highlands"},
    {"province_id": "laichau", "province_name": "Lai Chau", "location_id": "loc_laichau", "location_name": "Lai Chau", "latitude": 22.3862, "longitude": 103.4703, "altitude": 920.0, "region": "North"},
    {"province_id": "lamdong", "province_name": "Lam Dong", "location_id": "loc_lamdong", "location_name": "Da Lat", "latitude": 11.9404, "longitude": 108.4583, "altitude": 1500.0, "region": "Central Highlands"},
    {"province_id": "langson", "province_name": "Lang Son", "location_id": "loc_langson", "location_name": "Lang Son", "latitude": 21.8537, "longitude": 106.7615, "altitude": 260.0, "region": "North"},
    {"province_id": "laocai", "province_name": "Lao Cai", "location_id": "loc_laocai", "location_name": "Lao Cai", "latitude": 22.4856, "longitude": 103.9707, "altitude": 80.0, "region": "North"},
    {"province_id": "longan", "province_name": "Long An", "location_id": "loc_longan", "location_name": "Tan An", "latitude": 10.5359, "longitude": 106.4137, "altitude": 2.0, "region": "South"},
    {"province_id": "namdinh", "province_name": "Nam Dinh", "location_id": "loc_namdinh", "location_name": "Nam Dinh", "latitude": 20.4388, "longitude": 106.1621, "altitude": 3.0, "region": "North"},
    {"province_id": "nghean", "province_name": "Nghe An", "location_id": "loc_nghean", "location_name": "Vinh", "latitude": 18.6796, "longitude": 105.6813, "altitude": 7.0, "region": "Central"},
    {"province_id": "ninhbinh", "province_name": "Ninh Binh", "location_id": "loc_ninhbinh", "location_name": "Ninh Binh", "latitude": 20.2506, "longitude": 105.9745, "altitude": 5.0, "region": "North"},
    {"province_id": "ninhthuan", "province_name": "Ninh Thuan", "location_id": "loc_ninhthuan", "location_name": "Phan Rang Thap Cham", "latitude": 11.5643, "longitude": 108.9886, "altitude": 9.0, "region": "South Central"},
    {"province_id": "phutho", "province_name": "Phu Tho", "location_id": "loc_phutho", "location_name": "Viet Tri", "latitude": 21.3227, "longitude": 105.4020, "altitude": 20.0, "region": "North"},
    {"province_id": "phuyen", "province_name": "Phu Yen", "location_id": "loc_phuyen", "location_name": "Tuy Hoa", "latitude": 13.0955, "longitude": 109.3209, "altitude": 5.0, "region": "South Central"},
    {"province_id": "quangbinh", "province_name": "Quang Binh", "location_id": "loc_quangbinh", "location_name": "Dong Hoi", "latitude": 17.4689, "longitude": 106.6223, "altitude": 5.0, "region": "Central"},
    {"province_id": "quangnam", "province_name": "Quang Nam", "location_id": "loc_quangnam", "location_name": "Tam Ky", "latitude": 15.5736, "longitude": 108.4740, "altitude": 10.0, "region": "Central"},
    {"province_id": "quangngai", "province_name": "Quang Ngai", "location_id": "loc_quangngai", "location_name": "Quang Ngai", "latitude": 15.1205, "longitude": 108.7923, "altitude": 8.0, "region": "Central"},
    {"province_id": "quangninh", "province_name": "Quang Ninh", "location_id": "loc_quangninh", "location_name": "Ha Long", "latitude": 20.9712, "longitude": 107.0448, "altitude": 5.0, "region": "North"},
    {"province_id": "quangtri", "province_name": "Quang Tri", "location_id": "loc_quangtri", "location_name": "Dong Ha", "latitude": 16.8163, "longitude": 107.1003, "altitude": 8.0, "region": "Central"},
    {"province_id": "soctrang", "province_name": "Soc Trang", "location_id": "loc_soctrang", "location_name": "Soc Trang", "latitude": 9.6025, "longitude": 105.9739, "altitude": 2.0, "region": "South"},
    {"province_id": "sonla", "province_name": "Son La", "location_id": "loc_sonla", "location_name": "Son La", "latitude": 21.3270, "longitude": 103.9141, "altitude": 600.0, "region": "North"},
    {"province_id": "tayninh", "province_name": "Tay Ninh", "location_id": "loc_tayninh", "location_name": "Tay Ninh", "latitude": 11.3352, "longitude": 106.1099, "altitude": 15.0, "region": "South"},
    {"province_id": "thaibinh", "province_name": "Thai Binh", "location_id": "loc_thaibinh", "location_name": "Thai Binh", "latitude": 20.4463, "longitude": 106.3366, "altitude": 3.0, "region": "North"},
    {"province_id": "thainguyen", "province_name": "Thai Nguyen", "location_id": "loc_thainguyen", "location_name": "Thai Nguyen", "latitude": 21.5942, "longitude": 105.8482, "altitude": 40.0, "region": "North"},
    {"province_id": "thanhhoa", "province_name": "Thanh Hoa", "location_id": "loc_thanhhoa", "location_name": "Thanh Hoa", "latitude": 19.8067, "longitude": 105.7852, "altitude": 5.0, "region": "Central"},
])


# COMMAND ----------

# Final legacy location seed data

LOCATIONS.extend([
    {"province_id": "thuathienhue", "province_name": "Thua Thien Hue", "location_id": "loc_hue", "location_name": "Hue", "latitude": 16.4637, "longitude": 107.5909, "altitude": 5.0, "region": "Central"},
    {"province_id": "tiengiang", "province_name": "Tien Giang", "location_id": "loc_tiengiang", "location_name": "My Tho", "latitude": 10.3600, "longitude": 106.3600, "altitude": 2.0, "region": "South"},
    {"province_id": "travinh", "province_name": "Tra Vinh", "location_id": "loc_travinh", "location_name": "Tra Vinh", "latitude": 9.9347, "longitude": 106.3453, "altitude": 2.0, "region": "South"},
    {"province_id": "tuyenquang", "province_name": "Tuyen Quang", "location_id": "loc_tuyenquang", "location_name": "Tuyen Quang", "latitude": 21.8236, "longitude": 105.2140, "altitude": 30.0, "region": "North"},
    {"province_id": "vinhlong", "province_name": "Vinh Long", "location_id": "loc_vinhlong", "location_name": "Vinh Long", "latitude": 10.2537, "longitude": 105.9722, "altitude": 2.0, "region": "South"},
    {"province_id": "vinhphuc", "province_name": "Vinh Phuc", "location_id": "loc_vinhphuc", "location_name": "Vinh Yen", "latitude": 21.3089, "longitude": 105.6049, "altitude": 20.0, "region": "North"},
    {"province_id": "yenbai", "province_name": "Yen Bai", "location_id": "loc_yenbai", "location_name": "Yen Bai", "latitude": 21.7050, "longitude": 104.8750, "altitude": 40.0, "region": "North"},
])


LEGACY_63_LOCATIONS = LOCATIONS


# COMMAND ----------

# Current 34 province-level locations

CURRENT_34_LOCATIONS = [
    {"province_id": "hanoi", "province_name": "Ha Noi", "location_id": "loc34_hanoi", "location_name": "Ha Noi", "latitude": 21.0278, "longitude": 105.8342, "altitude": 10.0, "region": "North"},
    {"province_id": "haiphong", "province_name": "Hai Phong", "location_id": "loc34_haiphong", "location_name": "Hai Phong", "latitude": 20.8449, "longitude": 106.6881, "altitude": 5.0, "region": "North"},
    {"province_id": "hue", "province_name": "Hue", "location_id": "loc34_hue", "location_name": "Hue", "latitude": 16.4637, "longitude": 107.5909, "altitude": 5.0, "region": "Central"},
    {"province_id": "danang", "province_name": "Da Nang", "location_id": "loc34_danang", "location_name": "Da Nang", "latitude": 16.0471, "longitude": 108.2068, "altitude": 8.0, "region": "Central"},
    {"province_id": "hcm", "province_name": "Ho Chi Minh City", "location_id": "loc34_hcm", "location_name": "Ho Chi Minh City", "latitude": 10.8231, "longitude": 106.6297, "altitude": 5.0, "region": "South"},
    {"province_id": "cantho", "province_name": "Can Tho", "location_id": "loc34_cantho", "location_name": "Can Tho", "latitude": 10.0452, "longitude": 105.7469, "altitude": 3.0, "region": "South"},
    {"province_id": "tuyenquang", "province_name": "Tuyen Quang", "location_id": "loc34_tuyenquang", "location_name": "Tuyen Quang", "latitude": 21.8236, "longitude": 105.2140, "altitude": 30.0, "region": "North"},
    {"province_id": "laocai", "province_name": "Lao Cai", "location_id": "loc34_laocai", "location_name": "Lao Cai", "latitude": 22.4856, "longitude": 103.9707, "altitude": 80.0, "region": "North"},
    {"province_id": "thainguyen", "province_name": "Thai Nguyen", "location_id": "loc34_thainguyen", "location_name": "Thai Nguyen", "latitude": 21.5942, "longitude": 105.8482, "altitude": 40.0, "region": "North"},
    {"province_id": "phutho", "province_name": "Phu Tho", "location_id": "loc34_phutho", "location_name": "Viet Tri", "latitude": 21.3227, "longitude": 105.4020, "altitude": 20.0, "region": "North"},
    {"province_id": "bacninh", "province_name": "Bac Ninh", "location_id": "loc34_bacninh", "location_name": "Bac Ninh", "latitude": 21.1861, "longitude": 106.0763, "altitude": 8.0, "region": "North"},
    {"province_id": "hungyen", "province_name": "Hung Yen", "location_id": "loc34_hungyen", "location_name": "Hung Yen", "latitude": 20.6464, "longitude": 106.0511, "altitude": 5.0, "region": "North"},
    {"province_id": "ninhbinh", "province_name": "Ninh Binh", "location_id": "loc34_ninhbinh", "location_name": "Ninh Binh", "latitude": 20.2506, "longitude": 105.9745, "altitude": 5.0, "region": "North"},
    {"province_id": "quangtri", "province_name": "Quang Tri", "location_id": "loc34_quangtri", "location_name": "Dong Ha", "latitude": 16.8163, "longitude": 107.1003, "altitude": 8.0, "region": "Central"},
    {"province_id": "quangngai", "province_name": "Quang Ngai", "location_id": "loc34_quangngai", "location_name": "Quang Ngai", "latitude": 15.1205, "longitude": 108.7923, "altitude": 8.0, "region": "Central"},
    {"province_id": "gialai", "province_name": "Gia Lai", "location_id": "loc34_gialai", "location_name": "Quy Nhon", "latitude": 13.7820, "longitude": 109.2190, "altitude": 6.0, "region": "Central Highlands"},
    {"province_id": "khanhhoa", "province_name": "Khanh Hoa", "location_id": "loc34_khanhhoa", "location_name": "Nha Trang", "latitude": 12.2388, "longitude": 109.1967, "altitude": 5.0, "region": "South Central"},
    {"province_id": "lamdong", "province_name": "Lam Dong", "location_id": "loc34_lamdong", "location_name": "Da Lat", "latitude": 11.9404, "longitude": 108.4583, "altitude": 1500.0, "region": "Central Highlands"},
    {"province_id": "daklak", "province_name": "Dak Lak", "location_id": "loc34_daklak", "location_name": "Buon Ma Thuot", "latitude": 12.6667, "longitude": 108.0500, "altitude": 536.0, "region": "Central Highlands"},
    {"province_id": "dongnai", "province_name": "Dong Nai", "location_id": "loc34_dongnai", "location_name": "Bien Hoa", "latitude": 10.9574, "longitude": 106.8427, "altitude": 30.0, "region": "South"},
    {"province_id": "tayninh", "province_name": "Tay Ninh", "location_id": "loc34_tayninh", "location_name": "Tay Ninh", "latitude": 11.3352, "longitude": 106.1099, "altitude": 15.0, "region": "South"},
    {"province_id": "vinhlong", "province_name": "Vinh Long", "location_id": "loc34_vinhlong", "location_name": "Vinh Long", "latitude": 10.2537, "longitude": 105.9722, "altitude": 2.0, "region": "South"},
    {"province_id": "dongthap", "province_name": "Dong Thap", "location_id": "loc34_dongthap", "location_name": "Cao Lanh", "latitude": 10.4602, "longitude": 105.6329, "altitude": 2.0, "region": "South"},
    {"province_id": "camau", "province_name": "Ca Mau", "location_id": "loc34_camau", "location_name": "Ca Mau", "latitude": 9.1768, "longitude": 105.1524, "altitude": 1.0, "region": "South"},
    {"province_id": "angiang", "province_name": "An Giang", "location_id": "loc34_angiang", "location_name": "Long Xuyen", "latitude": 10.3864, "longitude": 105.4352, "altitude": 4.0, "region": "South"},
    {"province_id": "caobang", "province_name": "Cao Bang", "location_id": "loc34_caobang", "location_name": "Cao Bang", "latitude": 22.6666, "longitude": 106.2639, "altitude": 190.0, "region": "North"},
    {"province_id": "dienbien", "province_name": "Dien Bien", "location_id": "loc34_dienbien", "location_name": "Dien Bien Phu", "latitude": 21.3860, "longitude": 103.0230, "altitude": 475.0, "region": "North"},
    {"province_id": "hatinh", "province_name": "Ha Tinh", "location_id": "loc34_hatinh", "location_name": "Ha Tinh", "latitude": 18.3428, "longitude": 105.9057, "altitude": 5.0, "region": "Central"},
    {"province_id": "laichau", "province_name": "Lai Chau", "location_id": "loc34_laichau", "location_name": "Lai Chau", "latitude": 22.3862, "longitude": 103.4703, "altitude": 920.0, "region": "North"},
    {"province_id": "langson", "province_name": "Lang Son", "location_id": "loc34_langson", "location_name": "Lang Son", "latitude": 21.8537, "longitude": 106.7615, "altitude": 260.0, "region": "North"},
    {"province_id": "nghean", "province_name": "Nghe An", "location_id": "loc34_nghean", "location_name": "Vinh", "latitude": 18.6796, "longitude": 105.6813, "altitude": 7.0, "region": "Central"},
    {"province_id": "quangninh", "province_name": "Quang Ninh", "location_id": "loc34_quangninh", "location_name": "Ha Long", "latitude": 20.9712, "longitude": 107.0448, "altitude": 5.0, "region": "North"},
    {"province_id": "thanhhoa", "province_name": "Thanh Hoa", "location_id": "loc34_thanhhoa", "location_name": "Thanh Hoa", "latitude": 19.8067, "longitude": 105.7852, "altitude": 5.0, "region": "Central"},
    {"province_id": "sonla", "province_name": "Son La", "location_id": "loc34_sonla", "location_name": "Son La", "latitude": 21.3270, "longitude": 103.9141, "altitude": 600.0, "region": "North"},
]


# COMMAND ----------

# Data preparation and table writes

def selected_locations():
    location_set = setting("LOCATION_SET").strip().lower()
    if location_set in ("current_34", "34", "current"):
        return CURRENT_34_LOCATIONS, "current_34"
    if location_set in ("legacy_63", "63", "legacy"):
        return LEGACY_63_LOCATIONS, "legacy_63"
    raise ValueError("LOCATION_SET must be current_34 or legacy_63.")


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def prepare_frames():
    rows = []
    locations_source, location_set = selected_locations()
    for item in locations_source:
        row = dict(item)
        row["description"] = "Representative " + location_set + " location for " + row["province_name"]
        rows.append(row)
    source = spark.createDataFrame(rows)  # type: ignore[name-defined]
    provinces = (
        source.select(
            "province_id",
            "province_name",
            "region",
            F.lit(True).cast("boolean").alias("is_active"),
            F.current_timestamp().alias("created_at"),
        )
        .dropDuplicates(["province_id"])
    )
    locations = source.select(
        "location_id",
        "province_id",
        "location_name",
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("altitude").cast("double").alias("altitude"),
        "description",
        F.current_timestamp().alias("created_at"),
    ).dropDuplicates(["location_id"])
    return provinces, locations


def deactivate_unselected_provinces(province_ids):
    if not bool_setting("UPDATE_ACTIVE_PROVINCES"):
        return
    if not province_ids:
        return
    active_ids = ", ".join(sql_string(province_id) for province_id in sorted(province_ids))
    spark.sql(  # type: ignore[name-defined]
        "UPDATE "
        + fq_table("dim_province")
        + " SET is_active = false WHERE province_id NOT IN ("
        + active_ids
        + ")"
    )


def merge_table(source_df, target_table, key_column):
    table_name = target_table.split(".")[-1]
    if not table_exists(table_name):
        raise RuntimeError(
            "Table "
            + target_table
            + " does not exist. Run 01_create_catalog_schema_tables.sql before 02_seed_dim_locations.py."
        )
    target = DeltaTable.forName(spark, target_table)  # type: ignore[name-defined]
    (
        target.alias("target")
        .merge(source_df.alias("source"), "target." + key_column + " = source." + key_column)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

# Main execution

def main():
    create_widgets()
    ensure_namespace()
    provinces, locations = prepare_frames()
    merge_table(provinces, fq_table("dim_province"), "province_id")
    merge_table(locations, fq_table("dim_location"), "location_id")
    deactivate_unselected_provinces([row["province_id"] for row in locations.select("province_id").distinct().collect()])
    location_count = spark.table(fq_table("dim_location")).count()  # type: ignore[name-defined]
    seeded_count = locations.count()
    print("Seeded/merged " + str(seeded_count) + " representative Vietnam locations for LOCATION_SET=" + setting("LOCATION_SET") + ".")
    print("Current dim_location count: " + str(location_count))


# COMMAND ----------

main()
