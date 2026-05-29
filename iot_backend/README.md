# Standalone IoT Backend (Upload this folder only)

This `iot_backend` directory is self-contained.
You can upload ONLY this folder to VPS and run it independently.

## 1) Install dependencies

```bash
cd iot_backend
python -m venv .venv
source .venv/bin/activate  # Linux
pip install -r requirements.txt
```

## 2) Environment variables

Create `.env` inside `iot_backend/` (or parent dir).  
With your VPS setup (MQTT + PostgreSQL on the same machine), you can start from `iot_backend/.env.example`.

```env
# Shared auth with core backend
SECRET_KEY=your-shared-jwt-secret

# DB (same PostgreSQL as current system)
DB_HOST=127.0.0.1
DB_PORT=5432
DB_DATABASE=...
DB_USERNAME=...
DB_PASSWORD=...
# or DATABASE_URL=postgresql+psycopg2://...

# Optional notifications
TELEGRAM_BOT_TOKEN=
GMAIL_ADDRESS=
GMAIL_APP_PASSWORD=

# Optional AI explain
GEMINI_API_KEY=
GEMINI_MODEL=gemini-3.1-flash-lite

# MQTT bridge settings
MQTT_HOST=127.0.0.1
MQTT_PORT=1883
MQTT_USERNAME=
MQTT_PASSWORD=
MQTT_TOPIC=iot/metrics/#
IOT_WS_INGEST_URL=ws://127.0.0.1:8100/api/ws/mqtt_ingestor
```

## 3) Run IoT API service

```bash
uvicorn iot_backend.main:app --host 0.0.0.0 --port 8100
```

## 4) Run MQTT consumer (separate process)

```bash
python -m iot_backend.mqtt_consumer
```

## 5) WebSocket auth model

Viewer clients connect with token (public IP example):

```text
ws://20.214.247.102/api/ws/<client_id>?token=<jwt>
```

- `admin`: receives all sources
- `user`: receives only allowed sources
- no token: publisher-only connection

## 6) Keep core backend as gateway/orchestrator

Use Nginx path routing from root repo file:
`deploy/nginx_iot_split.conf`

So frontend can keep same old API base URL.
