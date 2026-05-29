# HƯỚNG DẪN TRIỂN KHAI IOT BACKEND TRÊN VPS

Tài liệu này hướng dẫn triển khai service `iot_backend` trên VPS thực tế đang dùng:

- VPS user: `azureuser`
- IP public: `20.214.247.102`
- Thư mục triển khai: `/home/azureuser/iot_backend`
- IoT backend chạy ở port: `8100`
- Backend cũ nếu có sẽ chạy ở port: `8000`
- Nginx route theo path để frontend vẫn gọi chung một IP/domain.

---

## 1. Kiến trúc sau khi tách service

Sau khi triển khai, hệ thống hoạt động như sau:

```text
Frontend
   |
   | gọi http://20.214.247.102
   |
 Nginx
   |
   |-- /api/iot-devices  -> IoT backend mới, port 8100
   |-- /api/metrics      -> IoT backend mới, port 8100
   |-- /api/alerts       -> IoT backend mới, port 8100
   |-- /api/ws           -> IoT backend mới, port 8100
   |
   |-- các route còn lại -> backend cũ, port 8000
```

IoT backend mới phụ trách:

- Quản lý thiết bị IoT.
- Nhận metrics.
- Xử lý alerts.
- WebSocket realtime.
- MQTT consumer ghi dữ liệu vào PostgreSQL và đẩy realtime.

Backend cũ nếu có sẽ phụ trách:

- Đăng nhập.
- JWT.
- Admin tổng.
- Server/rental.
- Chat.
- Các route không liên quan IoT.

---

## 2. Điều kiện đầu vào

VPS cần có sẵn:

- Python 3.
- PostgreSQL.
- MQTT broker.
- Nginx.
- Source code `iot_backend`.

Kiểm tra nhanh:

```bash
python3 --version
nginx -v
psql --version
```

Kiểm tra user hiện tại:

```bash
whoami
pwd
```

Kết quả đúng với VPS này thường là:

```text
azureuser
/home/azureuser
```

---

## 3. Upload source lên VPS

### 3.1. Cấu trúc thư mục mong muốn

Source nên nằm tại:

```bash
/home/azureuser/iot_backend
```

Cấu trúc đúng:

```text
/home/azureuser/iot_backend
├── HUONG_DAN_TRIEN_KHAI.md
├── README.md
├── __init__.py
├── api
├── config.py
├── crud.py
├── database.py
├── main.py
├── models.py
├── mqtt_consumer.py
├── requirements.txt
├── schemas.py
├── schemas_ws.py
├── services
├── system_metrics.py
└── websocket_manager.py
```

Lưu ý quan trọng:

Thư mục `/home/azureuser/iot_backend` vừa là thư mục chứa source, vừa là Python package vì có file:

```text
__init__.py
```

Vì vậy khi chạy bằng systemd cần chạy từ thư mục cha:

```bash
/home/azureuser
```

và app path là:

```bash
iot_backend.main:app
```

Không nên chạy systemd bằng:

```bash
uvicorn main:app
```

vì có thể gây lỗi import:

```text
ModuleNotFoundError: No module named 'iot_backend'
```

---

### 3.2. Upload bằng PowerShell từ Windows

Trên máy Windows, dùng `scp`, không dùng `cp`.

Ví dụ source nằm ở:

```text
D:\DuLieuCuaHuu\HK2_20252026\PTUD\CK\CK4\iot_backend
```

Upload lên VPS:

```powershell
scp -r D:\DuLieuCuaHuu\HK2_20252026\PTUD\CK\CK4\iot_backend azureuser@20.214.247.102:/home/azureuser/
```

Sau khi nhập mật khẩu xong, SSH vào VPS kiểm tra:

```bash
ssh azureuser@20.214.247.102
ls -la /home/azureuser
ls -la /home/azureuser/iot_backend
```

---

### 3.3. Nếu upload nhầm bị lồng thư mục

Nếu bị thành:

```text
/home/azureuser/iot_backend/iot_backend
```

hãy kiểm tra trước:

```bash
ls -la /home/azureuser/iot_backend/iot_backend
```

Nếu đó là thư mục upload lộn, xóa bằng:

```bash
rm -rf /home/azureuser/iot_backend/iot_backend
```

Sau đó kiểm tra lại:

```bash
ls -la /home/azureuser/iot_backend
```

---

## 4. Cài môi trường Python

Vào thư mục source:

```bash
cd /home/azureuser/iot_backend
```

Tạo virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Cài dependencies:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

Kiểm tra Python đang dùng đúng venv:

```bash
which python
```

Kết quả đúng:

```text
/home/azureuser/iot_backend/.venv/bin/python
```

Kiểm tra `uvicorn`:

```bash
/home/azureuser/iot_backend/.venv/bin/uvicorn --version
```

Nếu thiếu `uvicorn`, cài thêm:

```bash
pip install uvicorn
```

---

## 5. Tạo file `.env`

Nếu có file `.env.example`, copy ra `.env`:

```bash
cd /home/azureuser/iot_backend
cp .env.example .env
nano .env
```

Nội dung tối thiểu:

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

Lưu ý:

- `SECRET_KEY` phải giống backend cũ nếu frontend dùng JWT từ backend cũ.
- Nếu PostgreSQL không nằm local thì đổi `DB_HOST`.
- Nếu MQTT broker có username/password thì điền `MQTT_USERNAME` và `MQTT_PASSWORD`.

Lưu file trong nano:

```text
Ctrl + O
Enter
Ctrl + X
```

---

## 6. Chạy thử IoT backend thủ công

Vì code import theo package `iot_backend`, cần chạy từ thư mục cha:

```bash
cd /home/azureuser
/home/azureuser/iot_backend/.venv/bin/uvicorn iot_backend.main:app --host 0.0.0.0 --port 8100
```

Nếu chạy đúng sẽ thấy:

```text
Uvicorn running on http://0.0.0.0:8100
```

Mở thêm một SSH terminal khác để test:

```bash
curl -I http://127.0.0.1:8100/docs
curl http://127.0.0.1:8100/api/health
```

Kết quả tốt:

```text
HTTP/1.1 200 OK
```

và:

```json
{"status":"healthy","message":"Real-Time Metrics Processing System is running"}
```

Sau khi test xong, quay lại terminal đang chạy uvicorn và bấm:

```text
Ctrl + C
```

---

## 7. Chạy thử MQTT consumer thủ công

Chạy từ thư mục cha:

```bash
cd /home/azureuser
/home/azureuser/iot_backend/.venv/bin/python -m iot_backend.mqtt_consumer
```

Nếu kết nối được MQTT, log thường báo đã connected/subscribed topic.

Nếu lỗi MQTT, kiểm tra lại trong `.env`:

```env
MQTT_HOST=
MQTT_PORT=
MQTT_TOPIC=
MQTT_USERNAME=
MQTT_PASSWORD=
```

---

## 8. Cấu hình Nginx route

### 8.1. Kiểm tra thư mục Nginx

```bash
ls -la /etc/nginx
ls -la /etc/nginx/sites-available
ls -la /etc/nginx/sites-enabled
```

VPS hiện tại có:

```text
/etc/nginx/sites-available
/etc/nginx/sites-enabled
```

---

### 8.2. Tạo file Nginx config

Tạo hoặc sửa file:

```bash
sudo nano /etc/nginx/sites-available/iot_split
```

Nội dung mẫu:

```nginx
server {
    listen 80;
    server_name 20.214.247.102;

    # IoT devices route
    location /api/iot-devices {
        proxy_pass http://127.0.0.1:8100;
        proxy_http_version 1.1;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Metrics route
    location /api/metrics {
        proxy_pass http://127.0.0.1:8100;
        proxy_http_version 1.1;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Alerts route
    location /api/alerts {
        proxy_pass http://127.0.0.1:8100;
        proxy_http_version 1.1;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # WebSocket route
    location /api/ws {
        proxy_pass http://127.0.0.1:8100;
        proxy_http_version 1.1;

        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        proxy_read_timeout 86400;
    }

    # Tùy chọn: cho xem docs của IoT backend qua IP public
    location /docs {
        proxy_pass http://127.0.0.1:8100;
        proxy_http_version 1.1;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location /openapi.json {
        proxy_pass http://127.0.0.1:8100;
        proxy_http_version 1.1;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location /redoc {
        proxy_pass http://127.0.0.1:8100;
        proxy_http_version 1.1;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Các route còn lại đưa về backend cũ, nếu backend cũ chạy port 8000
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

---

### 8.3. Bật config Nginx

Nếu chưa có symlink:

```bash
sudo ln -s /etc/nginx/sites-available/iot_split /etc/nginx/sites-enabled/iot_split
```

Nếu báo file đã tồn tại thì bỏ qua.

Kiểm tra:

```bash
ls -la /etc/nginx/sites-enabled/
```

Test Nginx:

```bash
sudo nginx -t
```

Nếu báo:

```text
syntax is ok
test is successful
```

reload:

```bash
sudo systemctl reload nginx
```

---

### 8.4. Test Nginx route

Đảm bảo IoT backend đang chạy ở port 8100 rồi test:

```bash
curl -I http://20.214.247.102/docs
curl http://20.214.247.102/api/health
```

Test các route IoT:

```bash
curl -I http://20.214.247.102/api/metrics
curl -I http://20.214.247.102/api/alerts
curl -I http://20.214.247.102/api/iot-devices
```

Nếu trả về:

```text
405 Method Not Allowed
allow: POST
```

thì chưa chắc là lỗi. Điều đó thường có nghĩa là route tồn tại nhưng method bạn gọi không đúng, vì `curl -I` dùng HEAD còn API chỉ cho POST.

Nếu trả về:

```text
502 Bad Gateway
```

thì backend phía sau chưa chạy hoặc Nginx đang proxy sai port.

---

## 9. Tạo systemd cho IoT backend

### 9.1. Tạo file service

```bash
sudo nano /etc/systemd/system/iot-backend.service
```

Dán nội dung:

```ini
[Unit]
Description=IoT Backend FastAPI Service
After=network.target

[Service]
User=azureuser
WorkingDirectory=/home/azureuser
Environment="PATH=/home/azureuser/iot_backend/.venv/bin"
Environment="PYTHONPATH=/home/azureuser"
ExecStart=/home/azureuser/iot_backend/.venv/bin/uvicorn iot_backend.main:app --host 0.0.0.0 --port 8100
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Lưu ý quan trọng:

- `WorkingDirectory` phải là `/home/azureuser`.
- `ExecStart` phải dùng `iot_backend.main:app`.
- Không dùng `main:app` trong systemd.
- Có thêm `PYTHONPATH=/home/azureuser` để Python tìm thấy package `iot_backend`.

---

### 9.2. Start service

```bash
sudo systemctl daemon-reload
sudo systemctl enable iot-backend
sudo systemctl start iot-backend
```

Kiểm tra:

```bash
sudo systemctl status iot-backend
```

Nếu đúng sẽ thấy:

```text
active (running)
```

Test:

```bash
curl http://127.0.0.1:8100/api/health
```

---

### 9.3. Nếu service bị lỗi

Xem log:

```bash
journalctl -u iot-backend -n 100 --no-pager
```

Nếu thấy lỗi:

```text
ModuleNotFoundError: No module named 'iot_backend'
```

thì kiểm tra lại file service, đặc biệt 3 dòng này:

```ini
WorkingDirectory=/home/azureuser
Environment="PYTHONPATH=/home/azureuser"
ExecStart=/home/azureuser/iot_backend/.venv/bin/uvicorn iot_backend.main:app --host 0.0.0.0 --port 8100
```

Sau khi sửa:

```bash
sudo systemctl daemon-reload
sudo systemctl restart iot-backend
sudo systemctl status iot-backend
```

---

## 10. Tạo systemd cho MQTT consumer

### 10.1. Tạo file service

```bash
sudo nano /etc/systemd/system/iot-mqtt-consumer.service
```

Dán nội dung:

```ini
[Unit]
Description=IoT MQTT Consumer Service
After=network.target iot-backend.service
Requires=iot-backend.service

[Service]
User=azureuser
WorkingDirectory=/home/azureuser
Environment="PATH=/home/azureuser/iot_backend/.venv/bin"
Environment="PYTHONPATH=/home/azureuser"
ExecStart=/home/azureuser/iot_backend/.venv/bin/python -m iot_backend.mqtt_consumer
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

---

### 10.2. Start service

```bash
sudo systemctl daemon-reload
sudo systemctl enable iot-mqtt-consumer
sudo systemctl start iot-mqtt-consumer
```

Kiểm tra:

```bash
sudo systemctl status iot-mqtt-consumer
```

Xem log:

```bash
journalctl -u iot-mqtt-consumer -n 100 --no-pager
```

---

## 11. Kiểm tra sau khi triển khai

Kiểm tra 2 service:

```bash
sudo systemctl status iot-backend
sudo systemctl status iot-mqtt-consumer
```

Kiểm tra port:

```bash
sudo ss -tulpn | grep -E ':8000|:8100'
```

Kết quả tối thiểu cần có:

```text
0.0.0.0:8100
```

Nếu backend cũ còn dùng, cần có thêm:

```text
127.0.0.1:8000
```

Test IoT backend local:

```bash
curl http://127.0.0.1:8100/api/health
```

Test qua public IP:

```bash
curl http://20.214.247.102/api/health
curl -I http://20.214.247.102/docs
```

Test route IoT:

```bash
curl -I http://20.214.247.102/api/metrics
curl -I http://20.214.247.102/api/alerts
curl -I http://20.214.247.102/api/iot-devices
```

---

## 12. WebSocket frontend

Frontend/viewer kết nối WebSocket theo dạng:

```text
ws://20.214.247.102/api/ws/<client_id>?token=<JWT>
```

Ví dụ:

```text
ws://20.214.247.102/api/ws/user_123?token=eyJ...
```

Quyền truy cập:

- Admin: nhận tất cả source.
- User: chỉ nhận source được cấp quyền.
- Không có token: không nhận broadcast viewer hoặc chỉ được xử lý theo mode publisher-only, tùy logic code.

Nếu lỗi WebSocket 401/403:

- Kiểm tra `SECRET_KEY`.
- Kiểm tra token đã truyền qua `?token=` chưa.
- Kiểm tra token có hết hạn không.
- Kiểm tra Nginx có cấu hình `Upgrade` và `Connection "upgrade"` chưa.

---

## 13. Checklist nghiệm thu nhanh

1. `iot-backend` active running.
2. `iot-mqtt-consumer` active running.
3. `curl http://127.0.0.1:8100/api/health` trả về healthy.
4. Nginx test thành công: `sudo nginx -t`.
5. Public docs mở được: `http://20.214.247.102/docs`.
6. Các route IoT không bị `502`.
7. Publish thử một bản tin MQTT hợp lệ.
8. Frontend realtime nhận dữ liệu.
9. User chỉ thấy source được cấp quyền.
10. Admin thấy toàn bộ source.
11. Metrics/alerts được ghi vào PostgreSQL.
12. Alerts trigger đúng ngưỡng.

---

## 14. Lỗi thường gặp và cách xử lý

### 14.1. Lỗi `ModuleNotFoundError: No module named 'iot_backend'`

Nguyên nhân:

- Chạy service từ sai thư mục.
- Dùng `uvicorn main:app` trong khi code import theo package `iot_backend`.

Cách sửa:

File `/etc/systemd/system/iot-backend.service` phải có:

```ini
WorkingDirectory=/home/azureuser
Environment="PYTHONPATH=/home/azureuser"
ExecStart=/home/azureuser/iot_backend/.venv/bin/uvicorn iot_backend.main:app --host 0.0.0.0 --port 8100
```

Sau đó:

```bash
sudo systemctl daemon-reload
sudo systemctl restart iot-backend
```

---

### 14.2. Lỗi `502 Bad Gateway`

Nguyên nhân thường gặp:

- Service phía sau chưa chạy.
- Nginx proxy đến sai port.
- Backend cũ port 8000 chưa chạy nhưng Nginx vẫn route `/` về 8000.

Kiểm tra:

```bash
sudo ss -tulpn | grep -E ':8000|:8100'
curl http://127.0.0.1:8100/api/health
curl -I http://127.0.0.1:8000/docs
```

Nếu IoT backend chạy tốt nhưng `/docs` public vẫn 502, kiểm tra Nginx có route `/docs` về `8100` chưa.

---

### 14.3. Route trả về `405 Method Not Allowed`

Ví dụ:

```bash
curl -I http://20.214.247.102/api/metrics
```

trả về:

```text
405 Method Not Allowed
allow: POST
```

Đây thường không phải lỗi. Nghĩa là route tồn tại nhưng bạn gọi sai method.

`curl -I` dùng HEAD, trong khi API có thể chỉ nhận POST.

---

### 14.4. Không nhận MQTT

Kiểm tra log:

```bash
journalctl -u iot-mqtt-consumer -n 100 --no-pager
```

Kiểm tra `.env`:

```env
MQTT_HOST=
MQTT_PORT=
MQTT_TOPIC=
MQTT_USERNAME=
MQTT_PASSWORD=
```

Kiểm tra MQTT broker có chạy không:

```bash
sudo ss -tulpn | grep 1883
```

---

### 14.5. Không ghi được PostgreSQL

Kiểm tra `.env`:

```env
DB_HOST=
DB_PORT=
DB_DATABASE=
DB_USERNAME=
DB_PASSWORD=
```

Test PostgreSQL:

```bash
psql -h 127.0.0.1 -U postgres -d metrics_db
```

Nếu không vào được thì lỗi nằm ở database, user, password hoặc quyền truy cập.

---

## 15. Rollback nhanh

Nếu cần rollback:

1. Dừng IoT backend:

```bash
sudo systemctl stop iot-backend
sudo systemctl stop iot-mqtt-consumer
```

2. Sửa Nginx route `/api/iot-devices`, `/api/metrics`, `/api/alerts`, `/api/ws` về backend cũ nếu cần.

3. Reload Nginx:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

4. Kiểm tra lại frontend.

---

## 16. Lệnh tóm tắt triển khai nhanh

```bash
cd /home/azureuser/iot_backend
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
cp .env.example .env
nano .env
```

Test thủ công:

```bash
cd /home/azureuser
/home/azureuser/iot_backend/.venv/bin/uvicorn iot_backend.main:app --host 0.0.0.0 --port 8100
```

Tạo service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable iot-backend
sudo systemctl enable iot-mqtt-consumer
sudo systemctl start iot-backend
sudo systemctl start iot-mqtt-consumer
```

Kiểm tra:

```bash
sudo systemctl status iot-backend
sudo systemctl status iot-mqtt-consumer
curl http://127.0.0.1:8100/api/health
sudo nginx -t
```

---

## 17. Ghi chú quan trọng cho VPS này

Với VPS này, tuyệt đối lưu ý:

```text
Project path: /home/azureuser/iot_backend
Python package path: /home/azureuser/iot_backend
WorkingDirectory systemd: /home/azureuser
Uvicorn app path: iot_backend.main:app
Port IoT backend: 8100
Public IP: 20.214.247.102
```

Không dùng cấu hình cũ dạng:

```text
/home/ubuntu/iot_backend
```

và không dùng systemd dạng:

```text
uvicorn main:app
```

vì dễ gặp lỗi import package.
