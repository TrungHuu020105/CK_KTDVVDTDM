# 🌐 Hệ Thống Giám Sát Dữ Liệu IoT Thời Gian Thực

**Real-Time IoT Data Monitoring System**

---

## 📋 Thông Tin Cơ Bản

### Tên Đề Tài
**Hệ Thống Giám Sát Dữ Liệu IoT Thời Gian Thực với Giao Diện Web Tương Tác**

**Mô tả**: Một nền tảng hoàn chỉnh để theo dõi, quản lý và phân tích dữ liệu từ các cảm biến IoT (Internet of Things). Hệ thống sử dụng WebSocket để streaming 100% dữ liệu real-time đến dashboard, đồng thời thực hiện lọc thông minh để lưu trữ (~33% dữ liệu quan trọng) vào cơ sở dữ liệu, giảm thiểu dung lượng lưu trữ mà vẫn đảm bảo tính liên tục của dữ liệu phân tích.

### 👥 Thành Viên Nhóm

| STT | Họ Tên | MSSV | Vai Trò |
|-----|--------|------|---------|
| 1 | **Lê Trung Hữu** | 23666491 | Trưởng nhóm, Backend architecture |
| 2 | **Huỳnh Nhật Hào** | 23663871 | Backend WebSocket, Database |
| 3 | **Phan Gia Huy** | 23674141 | Frontend React, UI/UX |
| 4 | **Trần Quốc Huy** | 23637731 | Testing, Documentation |

**Lớp**: PTUD_CE_01 | **HK**: 2, năm 2025-2026 | **Tiến độ**: 100% ✅

---

## 📖 Tổng Quan Đề Tài

### Bài Toán Đặt Ra
Trong xu thế Internet of Things (IoT), việc giám sát và quản lý các dữ liệu từ hàng ngàn cảm biến đồng thời trở nên phức tạp. Các thách thức chính bao gồm:

1. **Tính thời gian thực**: Dữ liệu từ cảm biến cần được hiển thị ngay lập tức trên dashboard
2. **Khối lượng dữ liệu lớn**: Lưu trữ 100% dữ liệu gây tốn dung lượng database
3. **Phân quyền truy cập**: Cần quản lý quyền cho admin và người dùng bình thường
4. **Cảnh báo thông minh**: Tự động phát hiện và thông báo khi dữ liệu vượt ngưỡng
5. **Phân tích lịch sử**: Cần công cụ để xem xu hướng dữ liệu trong thời gian dài

### Giải Pháp Đề Xuất
Xây dựng một hệ thống gồm:
- **Backend FastAPI**: Cung cấp API, WebSocket streaming, logic kinh doanh
- **Frontend React**: Dashboard tương tác, biểu đồ động, quản lý device
- **Cơ sở dữ liệu SQLite**: Lưu trữ dữ liệu quan trọng + thông tin hệ thống
- **Dual-Layer Architecture**: 
  - Layer 1 (Real-time): 100% dữ liệu stream qua WebSocket → Dashboard
  - Layer 2 (Storage): ~33% dữ liệu lừa chọn save vào Database

### Ưu Điểm
✅ **Khách hàng thấy 100% dữ liệu không bị lag/delay**  
✅ **Database tiết kiệm dung lượng (chỉ lưu dữ liệu thay đổi)**  
✅ **Phân tích lịch sử chính xác từ dữ liệu lưu trữ**  
✅ **Dễ bảo trì, mở rộng**  
✅ **Hỗ trợ multi-user với role-based access**

---

## ✨ Những Tính Năng Đã Thực Hiện

### 1. **🔐 Xác Thực & Phân Quyền**
- ✅ System đăng ký tài khoản (Signup/Registration)
- ✅ Đăng nhập với JWT tokens (JWT-based authentication)
- ✅ Hai vai trò người dùng: Admin & Regular User
- ✅ Approval workflow: Admin phê duyệt người dùng mới
- ✅ Mã hóa mật khẩu an toàn (bcrypt hashing)
- ✅ Token expiration & refresh logic

### 2. **📱 Quản Lý Thiết Bị IoT (User-Owned Devices)**
- ✅ Users tạo device mới với sensor type (temperature, humidity, soil_moisture, light_intensity, pressure)
- ✅ Chỉnh sửa thông tin device (name, location, status)
- ✅ Xóa device
- ✅ Auto-sync: Tạo device trong bảng `iot_devices` (user view) + `devices` (metrics generation)
- ✅ Mỗi device có `source` duy nhất để tracking metrics
- ✅ Device có thể bật/tắt (is_active flag)

### 3. **🎯 Hệ Thống Cảnh Báo (Alert System)**
- ✅ Threshold-based alerts: Lower threshold (min) & Upper threshold (max)
- ✅ Tự động trigger when: `value < lower_threshold` OR `value > upper_threshold`
- ✅ Visual feedback: Red border khi triggered, cyan border bình thường
- ✅ Lưu trữ alert history: Xem cảnh báo cũ với timestamp
- ✅ Resolve alerts: Đánh dấu alert đã xử lý
- ✅ Auto-cleanup: Xóa alert cũ (> 15 ngày) tự động

### 4. **📊 Dashboard & Biểu Đồ Dữ Liệu**
- ✅ **User Dashboard** với:
  - Device selector dropdown (chọn cảm biến để xem)
  - Date range picker (từ ngày → đến ngày)
  - **Hourly aggregation** (1 ngày → 24 điểm/giờ)
  - **Daily aggregation** (2+ ngày → N điểm/ngày)
  - Recharts LineChart visualization
  - Loading states & empty states
  - Debug logging
  
- ✅ **Admin Dashboard** với:
  - Total servers, users, pending users
  - Total devices, alerts, monthly revenue
  - Auto-refresh mỗi 30 giây

- ✅ **Device Manager**:
  - Grid layout hiển thị tất cả devices
  - Real-time metrics values
  - Chart modal với history data
  - Alert threshold editor

### 5. **🔄 Real-Time Streaming (WebSocket)**
- ✅ Endpoint: `WS /api/ws/{client_id}`
- ✅ 100% metrics broadcast đến tất cả connected clients
- ✅ Smart filtering: Chỉ save DB nếu `saved=true`
- ✅ Dual-layer:
  - Layer 1: Realtime stream (100% data) → Frontend dashboard
  - Layer 2: Smart DB save (~33% data) → Persistent storage
- ✅ Connection management (connect/disconnect/broadcast)

### 6. **📈 API History Queries** (✅ **FIXED**)
- ✅ `GET /api/metrics/history-by-date` endpoint
- ✅ Params: `metric_type`, `source`, `from_date`, `to_date`
- ✅ **FIX Applied**: Sử dụng SQLite `strftime()` để so sánh date strings
- ✅ Hoạt động chính xác cho:
  - Today (1-day view): 86+ records ✅
  - Past dates (2026-04-05): 12 records ✅
  - Date ranges (2026-04-04 to 2026-04-05): 28 records ✅
- ✅ Role-based access control: Mỗi user chỉ thấy devices của mình

### 7. **👨‍💼 Admin Management**
- ✅ Approve/Reject pending users
- ✅ Delete users
- ✅ View all users & devices
- ✅ View user summary statistics
- ✅ Disconnect/Reconnect devices
- ✅ Cannot edit user data (designed limitation)

### 8. **💾 Database & Data Storage**
- ✅ SQLite database (metrics.db)
- ✅ 8 models: User, Device, Metric, Alert, IoTDevice, AvailableServer, ServerSubscription, Request
- ✅ Foreign key support
- ✅ Indexes for performance (metric_type, timestamp)
- ✅ 2,977 test records (14-day historical data)
- ✅ Timezone-aware (Vietnam TZ: UTC+7)
- ✅ ISO 8601 timestamp format

### 9. **🎨 Frontend UI/UX**
- ✅ Dark theme with neon colors (cyan, purple, orange, red, green, yellow)
- ✅ Responsive design (mobile-friendly)
- ✅ React 18 with Hooks (useState, useEffect, useContext)
- ✅ Context API for state management (AuthContext, DeviceContext)
- ✅ Recharts for data visualization
- ✅ Tailwind CSS for styling
- ✅ Lucide icons for UI elements
- ✅ Modal dialogs for add/edit operations
- ✅ Loading states, error handling, empty states

### 10. **🗂️ Project Structure & Organization**
- ✅ Clean folder structure (app/, frontend/, scripts)
- ✅ Separation of concerns (models, schemas, crud, routes, services)
- ✅ API versioning
- ✅ Configuration management
- ✅ Error handling middleware
- ✅ CORS enabled for frontend

### 11. **🧪 Test Data & Development**
- ✅ Demo accounts (admin/user with credentials)
- ✅ Demo servers & devices on startup
- ✅ `populate_metrics.py`: Generate 14-day historical data (6 records/hour)
- ✅ `stream_iot_data_live.py`: Continuous streaming for testing
- ✅ Test scripts for API validation

### 12. **📐 Fixed Issues During Development**
1. ✅ **Single-day query returns 0 records** → Fixed using SQLite `strftime()` for date comparison
2. ✅ **Alert colors not changing** → Fixed alert trigger logic & metrics fetching
3. ✅ **Default dashboard shows 7 days** → Changed to show today (1-day view)
4. ✅ **Demo devices auto-creation** → Removed unnecessary startup code

---

## 🛠️ Tech Stack

### Backend
| Technology | Version | Purpose |
|-----------|---------|---------|
| **Python** | 3.11 | Programming language |
| **FastAPI** | 0.104.1 | Web framework, async support |
| **Uvicorn** | 0.24.0 | ASGI server |
| **SQLAlchemy** | 2.0.23 | ORM for database |
| **Pydantic** | 2.5.0 | Data validation |
| **WebSockets** | 12.0 | Real-time communication |
| **JWT (python-jose)** | 3.3.0 | Authentication tokens |
| **bcrypt (passlib)** | 1.7.4 | Password hashing |
| **psutil** | 5.9.6 | System metrics |
| **SQLite** | - | Database |

### Frontend
| Technology | Version | Purpose |
|-----------|---------|---------|
| **React** | 18 | UI framework |
| **Vite** | Latest | Build tool |
| **Axios** | Latest | HTTP client |
| **Recharts** | Latest | Data visualization |
| **Tailwind CSS** | Latest | Styling |
| **Lucide React** | Latest | Icons |
| **React Context** | - | State management |

### Infrastructure
| Tool | Purpose |
|------|---------|
| **Git** | Version control |
| **npm** | Package manager (frontend) |
| **pip** | Package manager (backend) |
| **Vite Dev Server** | Frontend development |

---

## 📊 Kiến Trúc Hệ Thống

### Architecture Diagram
```
┌─────────────────────────────────────────────────────────────────┐
│                    FRONTEND (React 18)                          │
│  - User Dashboard (date range, sensor selector, charts)         │
│  - Admin Dashboard (stats, user management)                     │
│  - Device Manager (CRUD, alerts, real-time metrics)             │
│  - Login/Signup                                                 │
└────────────────────────┬────────────────────────────────────────┘
                         │ Axios HTTP + WebSocket
                         │
┌────────────────────────┴────────────────────────────────────────┐
│              BACKEND (FastAPI + WebSocket)                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ API Routes                                              │   │
│  │ - /api/auth (login, register)                          │   │
│  │ - /api/iot-devices (CRUD)                              │   │
│  │ - /api/metrics/history-by-date (date range query)      │   │
│  │ - /api/alerts (CRUD)                                   │   │
│  │ - /api/admin (user/device management)                  │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ WebSocket Endpoint: /api/ws/{client_id}                │   │
│  │ - 100% data broadcast to all connected clients         │   │
│  │ - Smart DB filtering (save ~33% of data)              │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ CRUD Operations (app/crud.py)                           │   │
│  │ - User management, Device CRUD, Metrics queries         │   │
│  │ - Permissions & Access control                         │   │
│  └─────────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         │ SQLAlchemy ORM
                         │
┌────────────────────────┴────────────────────────────────────────┐
│              DATABASE (SQLite)                                  │
│  - metrics (2,977 records, 14-day history)                      │
│  - users, iot_devices, alerts, servers, subscriptions           │
│  - indexes on (metric_type, timestamp) for fast queries         │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow (Dual-Layer Architecture)
```
IoT Sensor Generator
        │
        ├─── WebSocket Send (IotMetricsData)
        │
        ▼
┌──────────────────────┐
│ Backend /ws endpoint │
└──────────────────────┘
        │
        ├─ Branch 1 (Real-time):
        │   └─ Broadcast 100% to all connected frontend clients
        │      └─ Immediate chart update (no delay)
        │
        └─ Branch 2 (Storage):
            └─ Check "saved" flag
            └── YES: Save to Database (~33% important data)
            └── NO: Skip (reduces DB size)
```

### Alert Trigger Logic
```
Device Settings:
  ├─ alert_enabled: boolean
  ├─ lower_threshold: float (min value)
  └─ upper_threshold: float (max value)

When new metric arrives:
  if (value < lower_threshold) OR (value > upper_threshold):
      └─ ALERT TRIGGERED ❌ (Red border)
  else:
      └─ Normal status ✅ (Cyan border)

Alert stored in:
  └─ alerts table (timestamp, value, threshold, status)
  └─ Auto-cleanup old alerts (> 15 days)
```

---

## 🚀 Hướng Dẫn Chạy Hệ Thống

---

## ⚡ HƯỚNG DẪN CHẠY NHANH

### ⚠️ BẮT BUỘC TRƯỚC TIÊN - Chạy Trên Databricks

1. Mở Databricks Workspace
2. Upload file: `databricks_iot_streaming.py` → tạo notebook → **Run All**
   - ⏳ Chạy ~1 giờ để sinh 720 batches dữ liệu
3. Upload file: `databricks_data_aggregation.ipynb` → **Run All**
   - Tạo aggregation tables

**Nếu bước này chưa chạy → Frontend sẽ không có dữ liệu!**

---

### Yêu Cầu Tiên Quyết

- **Python**: 3.8+
- **Node.js**: 16+ với npm  
- **Terminal** hoặc PowerShell
- **Databricks**: Đã chạy streaming & aggregation trước

### 🔧 Backend Setup

**Bước 1: Cấu hình `.env`**

Mở/tạo file: `.env` tại thư mục gốc project

```env
```

**Bước 2: Cài đặt Dependencies (chỉ 1 lần)**

```powershell
pip install -r requirements.txt
```

**Bước 3: Chạy Backend**

Mở PowerShell, chạy:
```powershell
python -m uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

**Output mong đợi:**
```
INFO:     Uvicorn running on http://0.0.0.0:8000
INFO:     Application startup complete
```

**Truy cập API:**
- Swagger: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

---

### 🎨 Frontend Setup

**Bước 1: Cài đặt Dependencies (chỉ 1 lần)**

```powershell
cd frontend
npm install
```

**Bước 2: Chạy Frontend**

Mở PowerShell mới, chạy:
```powershell
npm run dev
```

**Output mong đợi:**
```
➜  Local:   http://localhost:3000/
```

---

### 🚀 Running the System (Tóm tắt)

**Đảm bảo Databricks chạy xong trước!**

**PowerShell 1 - Backend:**
```powershell
python -m uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

**PowerShell 2 - Frontend:**
```powershell
cd frontend
npm run dev
```

**Browser:** 
- Frontend: http://localhost:3000 
- Backend API: http://localhost:8000/docs

---

---

### 📊 Architecture Flow

```
Browser (http://localhost:3000)
    ↓
Frontend (React) - npm run dev
    ↓ (gọi /api/metrics/latest)
Backend (FastAPI) - port 8000
    ↓ (kết nối Databricks)
Databricks SQL
    ↓ (truy vấn Delta Lake)
Database (Databricks)
```

---

### 🐛 Troubleshooting

| Lỗi | Giải Pháp |
|-----|---------|
| Port 8000 đang dùng | Đổi sang port khác: `--port 8001` |
| Port 3000 đang dùng | `npm run dev -- --port 3002` |
| `Module 'backend' not found` | Xoá `backend/__pycache__`, chạy lại |
| Frontend không fetch dữ liệu | Kiểm tra Backend chạy? Token Databricks đúng? |
| No data on dashboard | Databricks streaming chạy xong? (~1 giờ) |
| npm: command not found | Cài Node.js từ https://nodejs.org/ |

---

### ✅ Checklist

- [ ] Databricks streaming chạy (~1 giờ)
- [ ] Databricks aggregation chạy
- [ ] `.env` file cấu hình ✓
- [ ] `pip install -r requirements.txt` ✓
- [ ] Backend chạy port 8000 ✓
- [ ] Frontend chạy port 3000 ✓
- [ ] Login: user/123456 ✓
- [ ] Xem dữ liệu từ Databricks ✓

---

## 📈 Hướng Phát Triển Tương Lai

### Phase 2: Enhanced Features
- 🔄 **Data Export**: Export metrics to CSV/PDF
- 📧 **Email Notifications**: Send alerts via email when threshold exceeded
- 🌍 **Multi-language**: Support Vietnamese, English, Chinese
- 🔔 **Push Notifications**: Mobile push alerts for critical incidents
- 📱 **Mobile App**: Native mobile app using React Native

### Phase 3: Advanced Analytics
- 🤖 **Machine Learning**: Predictive analytics for anomaly detection
- 📊 **Advanced Reports**: Custom reporting & business intelligence
- 💹 **Data Aggregation**: Combine metrics from multiple sources
- 📉 **Trend Analysis**: Automatic trend detection & forecasting
- 🔍 **Data Search**: Full-text search across historical data

### Phase 4: Scalability & Performance
- ☁️ **Cloud Deployment**: Deploy to AWS/Azure/GCP
- 🗄️ **PostgreSQL Migration**: Replace SQLite with PostgreSQL for production
- ⚡ **Redis Caching**: In-memory caching for faster queries
- 🔀 **Load Balancing**: Multiple backend instances
- 📦 **Docker Containerization**: Docker & Kubernetes deployment
- 🟢 **GraphQL API**: Alternative to REST for flexible queries

### Phase 5: Enterprise Features
- 🔐 **Advanced Security**: 2FA, OAuth2, SAML integration
- 🎯 **RBAC**: Fine-grained role-based access control
- 🌐 **Geo-Mapping**: Map-based device visualization
- 📡 **IoT Integration**: Direct integration with popular IoT platforms (Azure IoT Hub, AWS IoT Core)
- 💼 **White-label Solution**: Customizable branding
- 🔔 **Webhook Support**: External system integrations
- 📊 **Data Stream Processing**: Apache Kafka for high-volume streaming
- 🛡️ **Audit Logging**: Comprehensive audit trails

### Phase 6: Developer Experience
- 📚 **SDK/Libraries**: Official SDKs for popular languages (Python, JS, Go)
- 🧪 **Comprehensive Testing**: Unit tests, integration tests, E2E tests
- 📖 **API Documentation**: OpenAPI/Swagger documentation
- 🎓 **Training Materials**: Video tutorials, blog posts, courses
- 🔧 **CLI Tool**: Command-line tool for system management

---

## 📝 Ghi Chú Kỹ Thuật

### Database Statistics
- **Current Records**: 2,977 metrics
- **Date Range**: 2026-03-30 to 2026-04-13 (14 days)
- **Sensor Types**: 5 types (temperature, humidity, soil_moisture, light_intensity, pressure)
- **Distribution**: 6 records/hour × 24 hours × 14 days
- **File Size**: ~212 KB (metrics.db)

### Performance Metrics
- **WebSocket Latency**: <100ms (local network)
- **API Response Time**: <50ms (typical)
- **Streaming Rate**: 5 metrics/batch, ~60 metrics/minute
- **Database Queries**: Indexed for O(log n) complexity
- **Memory Usage**: ~150MB (backend + frontend)

### Security Features
- ✅ JWT-based authentication with token expiration
- ✅ Password hashing using bcrypt
- ✅ CORS enabled for controlled frontend access
- ✅ Input validation using Pydantic schemas
- ✅ SQL injection prevention (SQLAlchemy parameterized queries)
- ✅ Role-based access control (RBAC)

### Known Limitations
- SQLite for single-machine deployment (not recommended for high-concurrency)
- Basic authentication (no 2FA or OAuth)
- Limited to local network WebSocket connections
- Historical data aggregation done in-memory (not database-level)

---

## 📞 Liên Hệ & Hỗ Trợ

### Thắc Mắc Kỹ Thuật
- Liên hệ trưởng nhóm (Lê Trung Hữu - 23666491)
- Hoặc check repo GitHub (nếu có)

### Báo Cáo Lỗi (Bug Report)
1. Mô tả rõ lỗi xảy ra
2. Cung cấp reproduction steps
3. Screenshots/logs nếu có thể
4. Phiên bản Python/Node/React của bạn

---

## 📄 License

Dự án này được phát triển cho mục đích học tập.  
Tự do sử dụng, sửa đổi cho mục đích giáo dục.

---

## 🙏 Cảm Ơn

Cảm ơn thầy cô đã hướng dẫn dự án này.  
Cảm ơn toàn bộ bạn bè trong nhóm vì sự cố gắng và dedications! 💪

---

**Last Updated**: April 13, 2026  
**Version**: 2.0.0  
**Status**: ✅ 100% Complete & Functional
