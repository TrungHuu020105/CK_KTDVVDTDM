"""
IoT Real-Time Dashboard - Streamlit Web App
============================================================================
Purpose: Interactive card-based dashboard for live IoT data visualization
Run: streamlit run streamlit_dashboard.py
URL: http://localhost:8501
============================================================================
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from databricks import sql

# ============================================================================
# PAGE CONFIG
# ============================================================================

st.set_page_config(
    page_title="IoT Devices",
    page_icon="📱",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ============================================================================
# CUSTOM CSS - Dark Navy Theme
# ============================================================================

st.markdown("""
<style>
    body {
        background-color: #0f1419;
    }
    
    .main {
        background-color: #0f1419;
    }
    
    .stButton > button {
        width: 100%;
    }
    
    .device-card-container {
        background: linear-gradient(135deg, #1a2640 0%, #0f1e35 100%);
        border: 2px solid #00d4ff;
        border-radius: 12px;
        padding: 20px;
        margin-bottom: 15px;
        min-height: 280px;
    }
    
    .device-card-title {
        font-size: 18px;
        font-weight: bold;
        color: #ffffff;
        margin-bottom: 5px;
    }
    
    .device-card-meta {
        font-size: 11px;
        color: #888888;
        margin-bottom: 15px;
        line-height: 1.4;
    }
    
    .device-card-location {
        font-size: 13px;
        color: #cccccc;
        margin-bottom: 15px;
    }
    
    .device-card-value-box {
        background: rgba(0, 0, 0, 0.3);
        border-radius: 8px;
        padding: 15px;
        text-align: center;
        margin: 15px 0;
    }
    
    .device-card-value-label {
        font-size: 12px;
        color: #888888;
        margin-bottom: 8px;
    }
    
    .device-card-value {
        font-size: 42px;
        font-weight: bold;
        color: #00d4ff;
        margin: 5px 0;
    }
    
    .device-card-unit {
        font-size: 14px;
        color: #aaaaaa;
    }
    
    .device-card-status {
        font-size: 12px;
        color: #4ade80;
        font-weight: bold;
        margin: 10px 0;
    }
    
    .device-card-buttons {
        display: flex;
        gap: 10px;
        margin-top: 12px;
        border-top: 1px solid rgba(0, 212, 255, 0.2);
        padding-top: 12px;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# DATABRICKS CONNECTION
# ============================================================================

DATABRICKS_CONFIG = {
    "server_hostname": "dbc-8ffd6052-91ee.cloud.databricks.com",
    "http_path": "/sql/1.0/warehouses/3920086a375b89dc",
    "personal_access_token": "dapixxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "catalog": "workspace",
    "schema": "metrics_app_streaming"
}

@st.cache_resource
def get_databricks_connection():
    try:
        conn = sql.connect(
            server_hostname=DATABRICKS_CONFIG["server_hostname"],
            http_path=DATABRICKS_CONFIG["http_path"],
            personal_access_token=DATABRICKS_CONFIG["personal_access_token"],
            timeout_seconds=10
        )
        return conn
    except Exception as e:
        st.error(f"❌ Connection Error: {str(e)}")
        st.stop()

def query_databricks(sql_query):
    try:
        conn = get_databricks_connection()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        return pd.DataFrame(result, columns=columns)
    except Exception as e:
        st.warning(f"⚠️ Query error: {str(e)}")
        return None

# ============================================================================
# DATA FUNCTIONS
# ============================================================================

@st.cache_data(ttl=30)
def get_all_devices():
    query = f"""
    SELECT
        device_id, device_name, device_type, location,
        latest_value, unit, last_update, status
    FROM {DATABRICKS_CONFIG["catalog"]}.{DATABRICKS_CONFIG["schema"]}.iot_latest_readings
    ORDER BY device_type, location
    """
    return query_databricks(query)

def get_device_stats(device_id):
    query = f"""
    SELECT
        ROUND(MIN(value), 2) as min_value,
        ROUND(MAX(value), 2) as max_value,
        ROUND(AVG(value), 2) as avg_value
    FROM {DATABRICKS_CONFIG["catalog"]}.{DATABRICKS_CONFIG["schema"]}.iot_sensor_data
    WHERE device_id = '{device_id}'
    """
    return query_databricks(query)

def get_device_timeseries(device_id):
    query = f"""
    SELECT timestamp, value
    FROM {DATABRICKS_CONFIG["catalog"]}.{DATABRICKS_CONFIG["schema"]}.iot_sensor_data
    WHERE device_id = '{device_id}'
      AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
    ORDER BY timestamp ASC
    """
    return query_databricks(query)

# ============================================================================
# SESSION STATE
# ============================================================================

if 'selected_device' not in st.session_state:
    st.session_state.selected_device = None

# ============================================================================
# PAGE HEADER
# ============================================================================

st.markdown("# 📱 IoT Devices")
st.markdown("Manage your IoT sensors and devices")

col1, col2 = st.columns([1, 0.15])
with col2:
    if st.button("🔄 Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ============================================================================
# LOAD AND DISPLAY DEVICES
# ============================================================================

devices_df = get_all_devices()

if devices_df is None or len(devices_df) == 0:
    st.error("❌ No devices found")
else:
    # 3-column layout
    cols = st.columns(3)
    
    for idx, (_, device) in enumerate(devices_df.iterrows()):
        with cols[idx % 3]:
            device_id = device['device_id']
            device_name = device['device_name']
            device_type = device['device_type']
            location = device['location']
            latest_value = device['latest_value']
            unit = device['unit']
            
            # Card container
            with st.container(border=True):
                # Title and type badge
                col_t, col_b = st.columns([1, 0.3])
                with col_t:
                    st.markdown(f"<div class='device-card-title'>{device_name}</div>", unsafe_allow_html=True)
                with col_b:
                    st.markdown(f"<span style='background: rgba(0, 212, 255, 0.2); padding: 3px 8px; border-radius: 4px; color: #00d4ff; font-size: 11px;'>{device_type}</span>", unsafe_allow_html=True)
                
                # Meta info
                st.markdown(f"<div class='device-card-meta'>Source: sensor_1<br>Created by: letrunghuu</div>", unsafe_allow_html=True)
                
                # Location
                st.markdown(f"<div class='device-card-location'>📍 {location}</div>", unsafe_allow_html=True)
                
                # Value display
                st.markdown(f"""
                <div class='device-card-value-box'>
                    <div class='device-card-value-label'>Real-time Value</div>
                    <div class='device-card-value'>{latest_value}</div>
                    <div class='device-card-unit'>{unit}</div>
                </div>
                """, unsafe_allow_html=True)
                
                # Status
                st.markdown("<div class='device-card-status'>● Active</div>", unsafe_allow_html=True)
                
                # View Details button
                if st.button("👁️ View Details", key=f"view_{device_id}", use_container_width=True):
                    st.session_state.selected_device = device_id
                    st.rerun()
                
                # Action buttons
                col_d1, col_d2 = st.columns(2)
                with col_d1:
                    st.button("🔌 Disconnect", key=f"disc_{device_id}", use_container_width=True)
                with col_d2:
                    st.button("🗑️ Delete", key=f"del_{device_id}", use_container_width=True)

# ============================================================================
# MODAL DETAILS VIEW
# ============================================================================

if st.session_state.selected_device is not None:
    selected_df = devices_df[devices_df['device_id'] == st.session_state.selected_device]
    
    if len(selected_df) > 0:
        device = selected_df.iloc[0]
        device_id = device['device_id']
        device_name = device['device_name']
        latest_value = device['latest_value']
        unit = device['unit']
        
        # Modal header
        st.markdown("---")
        col_title, col_close = st.columns([1, 0.1])
        
        with col_title:
            st.markdown(f"## {device_name} - Detailed Statistics")
        
        with col_close:
            if st.button("❌ Close", key="close_modal", use_container_width=True):
                st.session_state.selected_device = None
                st.rerun()
        
        st.markdown("---")
        
        # Current value
        st.markdown("**📊 Current Value**")
        st.metric("", f"{latest_value} {unit}")
        
        # Statistics
        st.markdown("**📈 Statistics (Last 2 Hours)**")
        
        stats_df = get_device_stats(device_id)
        if stats_df is not None and len(stats_df) > 0:
            stats = stats_df.iloc[0]
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("📊 Average", f"{stats['avg_value']}")
            with col2:
                st.metric("📉 Minimum", f"{stats['min_value']}")
            with col3:
                st.metric("📈 Maximum", f"{stats['max_value']}")
        
        # Chart
        st.markdown("**📉 Trend Chart (Last 2 Hours)**")
        
        timeseries_df = get_device_timeseries(device_id)
        if timeseries_df is not None and len(timeseries_df) > 0:
            timeseries_df['timestamp'] = pd.to_datetime(timeseries_df['timestamp'])
            
            fig = px.line(
                timeseries_df,
                x='timestamp',
                y='value',
                title=f"{device_name} Trend",
                labels={'value': f"Value ({unit})", 'timestamp': 'Time'},
                height=400
            )
            fig.update_layout(
                hovermode='x unified',
                template='plotly_dark'
            )
            st.plotly_chart(fig, use_container_width=True)

# Footer
st.divider()
st.markdown(
    "<p style='text-align: center; color: #666; font-size: 12px;'>"
    "📡 Real-time IoT Data | 🗄️ Databricks Delta Lake | ⚡ Live Monitoring"
    "</p>",
    unsafe_allow_html=True
)
