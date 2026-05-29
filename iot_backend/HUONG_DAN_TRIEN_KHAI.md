# HUONG DAN TRIEN KHAI IOT-BACKEND (TIENG VIET)

Tai lieu nay huong dan trien khai service `iot_backend` len VPS va van giu backend cu hoat dong.

## 1) Kien truc sau khi tach

- Backend cu (core):
  - Dang nhap, JWT
  - Admin tong, server/rental, chat, cac route non-IoT
- IoT backend moi (`iot_backend`):
  - `/api/iot-devices`
  - `/api/metrics`
  - `/api/alerts`
  - `/api/ws`
  - MQTT ingest -> ghi PostgreSQL + realtime websocket
- Nginx route theo path de frontend van goi 1 domain/IP cu.

## 2) Dieu kien dau vao

VPS cua ban da co:
- MQTT broker
- PostgreSQL
- IP public: `20.214.247.102`

## 3) Chuan bi source tren VPS

Upload NGUYEN thu muc `iot_backend` len VPS, vi du:

```bash
/home/ubuntu/iot_backend
```

## 4) Tao moi truong Python va cai dependencies

```bash
cd /home/ubuntu/iot_backend
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## 5) Tao file .env

Copy mau:

```bash
cp .env.example .env
```

Cap nhat `.env` toi thieu:

```env
SECRET_KEY=replace_with_shared_jwt_secret
ALGORITHM=HS256

DB_HOST=127.0.0.1
DB_PORT=5432
DB_DATABASE=metrics_db
DB_USERNAME=postgres
DB_PASSWORD=replace_with_db_password

MQTT_HOST=127.0.0.1
MQTT_PORT=1883
MQTT_USERNAME=
MQTT_PASSWORD=
MQTT_TOPIC=iot/metrics/#

IOT_WS_INGEST_URL=ws://127.0.0.1:8100/api/ws/mqtt_ingestor
```

Luu y:
- `SECRET_KEY` phai GIONG voi backend cu de verify JWT user/admin.
- Neu PostgreSQL khac host local thi doi `DB_HOST` tuong ung.

## 6) Chay thu thu cong truoc khi daemon

### 6.1 Chay API IoT backend

```bash
cd /home/ubuntu/iot_backend
source .venv/bin/activate
uvicorn iot_backend.main:app --host 0.0.0.0 --port 8100
```

Kiem tra:
- `http://127.0.0.1:8100/docs`
- `http://127.0.0.1:8100/api/health`

### 6.2 Chay MQTT consumer

Mo terminal khac:

```bash
cd /home/ubuntu/iot_backend
source .venv/bin/activate
python -m iot_backend.mqtt_consumer
```

Neu ok, log se bao connected MQTT + subscribed topic.

## 7) Cau hinh Nginx route

Dung file mau: `deploy/nginx_iot_split.conf`

Y tuong:
- `/api/iot-devices`, `/api/metrics`, `/api/alerts`, `/api/ws` -> `127.0.0.1:8100`
- cac route con lai -> backend cu `127.0.0.1:8000`

Ap dung nhanh (tham khao):

```bash
sudo cp /path/to/deploy/nginx_iot_split.conf /etc/nginx/sites-available/iot_split
sudo ln -s /etc/nginx/sites-available/iot_split /etc/nginx/sites-enabled/iot_split
sudo nginx -t
sudo systemctl reload nginx
```

## 8) WebSocket va phan quyen admin/user

Frontend/viewer ket noi WS theo dang:

```text
ws://20.214.247.102/api/ws/<client_id>?token=<JWT>
```

Logic phan quyen:
- admin: nhan tat ca source
- user: chi nhan source duoc cap quyen
- khong co token: chi duoc xem nhu publisher-only (khong nhan broadcast viewer)

## 9) Chay duoi systemd (khuyen nghi)

Tao 2 service:

1. `iot-backend.service` (uvicorn)
2. `iot-mqtt-consumer.service` (mqtt bridge)

Sau do:

```bash
sudo systemctl daemon-reload
sudo systemctl enable iot-backend
sudo systemctl enable iot-mqtt-consumer
sudo systemctl start iot-backend
sudo systemctl start iot-mqtt-consumer
```

Kiem tra:

```bash
sudo systemctl status iot-backend
sudo systemctl status iot-mqtt-consumer
journalctl -u iot-backend -f
journalctl -u iot-mqtt-consumer -f
```

## 10) Checklist nghiem thu nhanh

1. Login bang admin va user o frontend.
2. Mo trang realtime IoT, xac nhan WS co token.
3. Publish 1 ban tin MQTT hop le, xem frontend co cap nhat.
4. Kiem tra user chi thay source duoc phep.
5. Kiem tra admin thay full source.
6. Kiem tra metrics/alerts da ghi vao PostgreSQL.
7. Kiem tra alerts trigger dung nguong.

## 11) Loi thuong gap

- Loi 401/403 WS:
  - sai `SECRET_KEY` giua 2 backend
  - token het han
- Khong nhan MQTT:
  - sai `MQTT_HOST/MQTT_PORT/topic`
  - firewall chan ket noi broker
- Khong ghi du lieu DB:
  - sai `DB_*`
  - user PostgreSQL khong du quyen
- Frontend mat realtime:
  - chua them `?token=` vao URL WS
  - Nginx chua bat `Upgrade/Connection` cho websocket

## 12) Rollback nhanh

Neu can rollback:
1. Tra route Nginx `/api/iot*` va `/api/ws` ve backend cu.
2. Reload Nginx.
3. Dung `iot-backend` va `iot-mqtt-consumer`.

He thong frontend se ve dung luong cu.
