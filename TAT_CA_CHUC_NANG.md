# Tat ca chuc nang trong code

Tai lieu nay tong hop cac chuc nang va ham/class chinh dang co trong workspace hien tai. No duoc chia theo thu muc de ban co the tim nhanh tung nhom logic.

## 1. Root script: `stream_ambient_network_mqtt.py`

Day la script lay du lieu thoi tiet cong khai tu Ambient Weather Network, chuyen thanh sensor reading va co the publish len MQTT.

### Classes
- `StationTarget`: mo ta diem tim kiem station, source, toa do, ban kinh va tu khoa.
- `AmbientMqttPublisher`: dong goi logic publish MQTT cho du lieu da parse.

### Functions
- `load_env_files()`: nap bien moi truong tu `.env`, `app/.env`, `iot_backend/.env`.
- `env_int(name, default)`: doc bien moi truong kieu so nguyen.
- `now_vn()`: lay thoi gian hien tai theo gio Viet Nam.
- `normalize_text(value)`: chuan hoa text de tim kiem/so khop khong phan biet dau.
- `clean_float(value)`: ep ve so thuc va lam sach gia tri khong hop le.
- `f_to_c(value_f)`: doi Fahrenheit sang Celsius.
- `pick_value(data, keys)`: chon gia tri dau tien ton tai trong mot tap key.
- `calc_rh_from_temp_dewpoint(temp_c, dewpoint_c)`: tinh do am tu nhiet do va dewpoint.
- `parse_ambient_time(value)`: parse timestamp dang Ambient sang ISO VN va millis.
- `get_station_name(station)`: lay ten station tu payload Ambient.
- `get_station_mac(station)`: lay MAC/id cua station.
- `station_search_text(station)`: tao chuoi tim kiem gom ten + metadata.
- `select_station(stations, keywords)`: chon station phu hop nhat theo tu khoa.
- `find_public_stations(api, target)`: tim danh sach public stations quanh target.
- `fetch_station_detail(api, mac)`: lay chi tiet station.
- `extract_last_data(detail)`: tach payload du lieu moi nhat.
- `parse_weather_reading(...)`: chuyen du lieu Ambient thanh sensor reading chuan.
- `append_csv(rows, csv_path)`: ghi du lieu ra CSV neu co cau hinh.
- `create_mqtt_client()`: tao MQTT client.
- `fetch_all_targets(...)`: lay du lieu cho tat ca target da khai bao.
- `filter_duplicates(...)`: loc cac ban ghi trung lap.
- `run(args)`: workflow chinh cua script.
- `parse_args()`: parse tham so dong lenh.

---

## 2. Backend gateway: `app/`

### 2.1 `app/main.py`

Day la entrypoint FastAPI chinh cua gateway backend.

#### Functions
- `_init_db_with_retry()`: khoi tao database voi co che retry va fallback SQLite.
- `_seed_dev_data(db_mode)`: tao du lieu demo cho admin/user va device mau.
- `_normalize_legacy_fake_devices()`: chuan hoa cac device demo cu sang schema sensor moi.
- `root()`: endpoint goc tra ve thong tin he thong.
- `startup_event()`: khoi tao khi service startup.
- `shutdown_event()`: don dep tai shutdown.

### 2.2 `app/config.py`

#### Functions
- `_load_dotenv()`: nap cau hinh `.env`.
- `get_cors_origins()`: lay danh sach CORS origins.
- `get_database_url()`: lay connection string database.

### 2.3 `app/database.py`

#### Functions
- `_engine_kwargs_for(url)`: chon tham so engine theo loai DB.
- `_create_app_engine(url)`: tao SQLAlchemy engine cho app.
- `switch_to_sqlite_fallback()`: chuyen sang SQLite khi DB chinh loi.
- `get_db()`: dependency lay session DB.
- `init_db()`: tao va check schema ban dau.
- `_ensure_iot_device_required_columns()`: dam bao cot can thiet cho IoT device.
- `_ensure_alert_required_columns()`: dam bao cot can thiet cho alert.
- `_backfill_iot_device_threshold_columns()`: backfill threshold columns.
- `_is_schema_auto_migrate_enabled()`: check co cho phep auto migrate hay khong.
- `_run_schema_evolution()`: chay evolution schema neu duoc bat.
- `_ensure_iot_device_columns()`: dam bao cot device mo rong.
- `_cleanup_metric_columns()`: don cac cot metric cu.
- `_migrate_iot_device_source_constraint()`: migrate rang buoc source cua device.
- `_ensure_chat_columns()`: dam bao cot chat can thiet.

### 2.4 `app/crud.py`

#### Metrics / sensor readings
- `_now_vn()`: lay thoi gian VN hien tai.
- `_normalize_metric_location_value(value)`: chuan hoa location cua metric.
- `_resolve_metric_location(db, sensor_id, location)`: suy ra location hop le cho metric.
- `create_metric(db, metric)`: tao 1 metric.
- `create_metrics_bulk(db, metrics)`: tao nhieu metric cung luc.
- `get_latest_metrics(db)`: lay latest metrics.
- `get_metrics_history(...)`: lay history cho 1 sensor/metric.
- `get_metrics_in_range(...)`: lay metric trong khoang thoi gian.
- `get_all_metrics_in_range(db, minutes)`: lay tat ca metric trong N phut.
- `delete_old_metrics(db, days)`: xoa metric cu.

#### Alerts
- `create_alert(db, alert)`: tao alert.
- `get_recent_alerts(db, hours, limit)`: lay alert gan day.
- `get_unresolved_alerts(db)`: lay alert chua xu ly.
- `get_alerts_by_metric(db, metric_type, hours)`: loc alert theo metric.
- `resolve_alert(db, alert_id)`: danh dau alert da giai quyet.
- `delete_old_alerts(db, days)`: xoa alert cu.

#### Users
- `create_user(db, user, hashed_password)`: tao user moi.
- `get_user_by_username(db, username)`: tim user theo username.
- `get_user_by_email(db, email)`: tim user theo email.
- `get_user_by_id(db, user_id)`: tim user theo id.
- `get_all_users(db)`: lay tat ca user.
- `get_pending_users(db)`: lay user dang cho duyet.
- `approve_user(db, user_id, admin_id)`: duyet user.
- `reject_user(db, user_id)`: tu choi user.
- `delete_user(db, user_id)`: xoa user.

#### Devices
- `create_device(db, device, admin_id)`: tao device.
- `get_all_devices(db)`: lay tat ca device.
- `get_device_by_id(db, device_id)`: tim device theo id.
- `get_device_by_source(db, source)`: tim device theo source.
- `delete_device(db, device_id)`: xoa device.
- `update_device(db, device_id, name, device_type, location)`: cap nhat device.

#### Permissions
- `grant_device_permission(db, user_id, device_id, admin_id)`: cap quyen device cho user.
- `revoke_device_permission(db, user_id, device_id)`: thu hoi quyen.
- `get_user_devices(db, user_id)`: lay device ma user co quyen.
- `get_device_users(db, device_id)`: lay user co quyen voi device.
- `get_user_accessible_sources(db, user_id)`: lay cac source user duoc phep xem.

#### User-scoped metrics
- `get_latest_metrics_for_user(db, user_id, source)`: lay latest metrics cho user.
- `get_metrics_history_for_user(...)`: lay history cho user.
- `get_latest_metric_for_user(...)`: lay 1 latest metric cho user.
- `get_metrics_history_by_date(...)`: lay history theo ngay.

#### Chat
- `create_chat_conversation(...)`: tao cuoc hoi thoai chat.
- `get_chat_conversation(db, conversation_id)`: lay conversation.
- `get_latest_user_chat_conversation(db, user_id)`: lay conversation moi nhat cua user.
- `list_user_chat_conversations(db, user_id)`: liet ke conversation cua user.
- `list_admin_chat_conversations(db, status_filter)`: liet ke conversation cho admin.
- `update_chat_conversation_status(...)`: cap nhat trang thai conversation.
- `create_chat_message(...)`: tao message moi.
- `list_chat_messages(db, conversation_id)`: lay tat ca message trong conversation.
- `delete_chat_conversation(db, conversation)`: xoa conversation.
- `list_chat_issue_templates(db, active_only)`: lay issue templates.
- `get_chat_issue_template(db, template_id)`: lay 1 template.
- `create_chat_issue_template(...)`: tao template.
- `update_chat_issue_template(db, row, data)`: cap nhat template.
- `delete_chat_issue_template(db, row)`: xoa template.

### 2.5 `app/models.py`

#### Data models
- `Metric`, `Alert`, `User`, `UserNotificationTarget`, `Device`, `UserDevicePermission`, `IoTDevice`, `SensorReading`, `ChatConversation`, `ChatMessage`, `ChatIssueTemplate`.

### 2.6 `app/schemas.py`

#### Pydantic schemas
- `MetricCreate`, `MetricBulkCreate`, `MetricResponse`, `LatestMetricsResponse`, `SummaryMetricsResponse`, `MetricsHistoryResponse`, `HealthResponse`.
- `AlertCreate`, `AlertResponse`, `AlertListResponse`.
- `UserRegister`, `UserLogin`, `TokenResponse`, `UserResponse`.
- `DeviceCreate`, `DeviceUpdate`, `DeviceResponse`, `UserDevicePermissionResponse`.
- `ChatConversationResponse`, `ChatMessageResponse`, `ChatSendRequest`, `ChatEscalateRequest`, `ChatAdminReplyRequest`, `ChatConversationListResponse`, `ChatMessagesListResponse`.
- `ChatIssueTemplateCreate`, `ChatIssueTemplateUpdate`, `ChatIssueTemplateResponse`.

### 2.7 `app/api/`

#### `iot_backend_proxy.py`
- `extract_bearer_token(request)`: lay bearer token tu request.
- `proxy_iot_backend(...)`: proxy request sang iot_backend.
- `proxy_iot_backend_raw(...)`: proxy raw response sang iot_backend.

#### `routes_auth.py`
- `_success_notification_html()`: tao HTML thong bao thanh cong.
- `hash_password(password)`: bam mat khau.
- `verify_password(plain_password, hashed_password)`: kiem tra mat khau.
- `create_access_token(data)`: tao JWT access token.
- `get_current_user(...)`: resolve user hien tai tu token.
- `register(user_data, db)`: dang ky user.
- `login(credentials, db)`: dang nhap.
- `get_me(current_user)`: lay thong tin user hien tai.
- `get_my_devices(...)`: lay device cua user.
- `get_user_info(user_id, db)`: lay thong tin user theo id.
- `link_telegram(...)`: lien ket Telegram.
- `test_telegram(...)`: test Telegram notification.
- `unlink_telegram(...)`: huy lien ket Telegram.
- `enable_email_alerts(...)`: bat email alert.
- `test_email_alerts(...)`: test email alert.
- `get_email_status(current_user)`: lay trang thai email.
- `toggle_email_alerts(...)`: bat/tat email alert.
- `update_email_address(...)`: doi dia chi email.
- `disable_email_alerts(...)`: tat email alert.
- `list_notification_targets(...)`: lay danh sach notification targets.
- `add_notification_target(...)`: them notification target.
- `toggle_notification_target(...)`: bat/tat target.
- `test_notification_target(...)`: test target.
- `delete_notification_target(...)`: xoa target.

#### `routes_chat.py`
- `_conversation_to_response(db, conversation)`: chuyen conversation sang response.
- `_validate_conversation_access(conversation, current_user)`: kiem tra quyen truy cap.
- `_ensure_default_issue_templates(db)`: tao template mac dinh neu can.
- `list_my_conversations(...)`: lay conversation cua user.
- `list_issue_templates(...)`: lay issue templates.
- `create_issue_template(...)`: tao issue template.
- `update_issue_template(...)`: cap nhat issue template.
- `delete_issue_template(...)`: xoa issue template.
- `list_admin_conversations(...)`: admin xem danh sach conversation.
- `get_conversation_messages(...)`: lay messages cua conversation.
- `create_new_conversation(...)`: tao conversation moi.
- `delete_conversation(...)`: xoa conversation.
- `send_message(...)`: gui tin nhan.
- `escalate_to_admin(...)`: chuyen len admin.
- `admin_reply(...)`: admin tra loi.
- `close_conversation(...)`: dong conversation.

#### `routes_metrics.py`
- `_serialize_metric(metric)`: serialize metric object.
- `health_check()`: health endpoint.
- `create_metric(...)`: tao 1 metric.
- `create_metrics_bulk(...)`: tao metric nhieu ban ghi.
- `get_latest_metrics(...)`: lay latest metrics.
- `get_metrics_history(...)`: lay history.
- `get_latest_metric_one(...)`: lay 1 latest metric.
- `get_metrics_history_by_date(...)`: lay history theo ngay.
- `get_metrics_summary(...)`: tong hop metrics.
- `generate_sample_data(...)`: tao du lieu mau.
- `generate_iot_data(...)`: tao du lieu IoT demo.
- `get_current_system_metrics()`: lay metrics he thong hien tai.
- `get_detailed_system_metrics()`: lay metrics he thong chi tiet.
- `collect_and_save_system_metrics()`: thu thap va luu metrics he thong.
- `collect_cpu_only()`: thu thap CPU.
- `collect_memory_only()`: thu thap memory.

#### `routes_sensors.py`
- `_set_no_cache_headers(response)`: khong cho cache.
- `list_sensors(...)`: lay danh sach sensor.
- `create_sensor(...)`: tao sensor.
- `databricks_status(...)`: kiem tra trang thai Databricks.
- `ingest_reading(...)`: ingest sensor reading.
- `geocode(...)`: geocode location.
- `get_sensor(sensor_id, ...)`: lay sensor theo id.
- `update_sensor(sensor_id, ...)`: cap nhat sensor.
- `delete_sensor(sensor_id, ...)`: xoa sensor.
- `latest(sensor_id, ...)`: lay latest reading.
- `history(sensor_id, ...)`: lay history.
- `export_history_csv(...)`: export history ra CSV.
- `forecast(sensor_id, ...)`: lay forecast.
- `model_leaderboard(sensor_id, ...)`: lay model leaderboard.

#### `routes_iot_proxy.py`
- `WifiConfigProxyRequest`, `WifiScanProxyRequest`, `WifiConfigByDeviceRequest`, `ManualCommandByDeviceRequest`: request models cho proxy.
- `_post_iot(path, payload, bearer_token)`: POST sang iot backend.
- `_get_iot(path, bearer_token)`: GET sang iot backend.
- `proxy_wifi_config(...)`: proxy cau hinh wifi.
- `proxy_request_wifi_scan(...)`: proxy yeu cau scan wifi.
- `proxy_get_wifi_scan(...)`: proxy lay ket qua scan wifi.
- `proxy_scan_wifi_by_device_source(...)`: scan wifi theo source.
- `proxy_wifi_list_by_device_source(...)`: lay danh sach wifi theo source.
- `proxy_wifi_status_by_device_source(...)`: lay trang thai wifi theo source.
- `proxy_wifi_config_by_device_source(...)`: cau hinh wifi theo source.
- `proxy_manual_command_by_device_source(...)`: gui lenh thu cong theo source.

#### `routes_websocket.py`
- `_backend_ws_url(client_id, token)`: tao URL websocket backend.
- `websocket_endpoint(websocket, client_id)`: endpoint websocket gateway.

#### `routes_admin.py`
- `verify_admin(user)`: guard chi admin.
- `get_pending_users(admin, db)`: lay user dang cho duyet.
- `approve_user(user_id, admin, db)`: duyet user.
- `reject_user(user_id, admin, db)`: tu choi user.
- `get_all_users(admin, db)`: lay tat ca user.
- `delete_user(user_id, admin, db)`: xoa user.
- `create_device(device, admin, db)`: tao device moi.
- `get_all_devices(admin, db)`: lay tat ca device.
- `delete_device(device_id, admin, db)`: xoa device.
- `update_device(device_id, device_update, admin, db)`: cap nhat device.
- `toggle_device_active(device_id, admin, db)`: bat/tat device.
- `grant_permission(user_id, device_id, admin, db)`: cap quyen cho user.
- `revoke_permission(user_id, device_id, admin, db)`: thu hoi quyen.
- `get_user_devices(user_id, admin, db)`: lay device cua user.
- `get_device_users(device_id, admin, db)`: lay user co quyen.

#### `routes_admin_iot.py`
- `verify_admin(user)`: guard chi admin.
- `get_all_iot_devices(request, admin)`: lay tat ca IoT device.
- `get_iot_devices_summary(request, admin)`: lay tong hop IoT device.
- `delete_iot_device(device_id, request, admin)`: xoa IoT device.
- `disconnect_iot_device(device_id, request, admin)`: ngat ket noi device.
- `reconnect_iot_device(device_id, request, admin)`: ket noi lai device.

#### `routes_alerts.py`
- `TestEmailRequest`: request model test email.
- `test_notification_email(payload)`: test gui email thong bao.
- `get_email_config_debug()`: xem debug cau hinh email.
- `create_alert(...)`: tao alert thu cong.
- `run_forecast_alert_scan(request, current_user)`: chay forecast scan.
- `get_alerts(...)`: lay danh sach alert.
- `get_recent_alerts(...)`: lay alert gan day.
- `get_unresolved_alerts(...)`: lay alert chua xu ly.
- `get_alerts_by_metric(...)`: loc alert theo metric.
- `resolve_alert(...)`: danh dau alert da xu ly.
- `cleanup_old_alerts(...)`: don alert cu.
- `explain_alert_with_ai(...)`: giai thich alert bang AI.

### 2.8 `app/services/`

#### `email_service.py`
- `EmailConfigValue`, `EmailConfig`: cau truc cau hinh email.
- `_first_env(names)`: lay bien moi truong dau tien tim duoc.
- `resolve_email_config()`: tong hop cau hinh email.
- `email_config_debug()`: debug cau hinh email.
- `_log_config(config)`: log cau hinh email.
- `_validate_config(config)`: validate cau hinh.
- `_send_email(to_email, subject, body, log_label)`: gui email lower-level.
- `send_test_email(to_email)`: gui email test.
- `send_email_alert(to_email, subject, body)`: gui email alert.

#### `iot_alert_service.py`
- `normalize_metric_source(payload, fallback)`: chuan hoa source cho metric.
- `parse_metric_timestamp(timestamp)`: parse timestamp metric.
- `_threshold_label(status, threshold)`: tao nhan nguong.
- `_alert_state_key(source, metric_type)`: key cho trang thai alert runtime.
- `_get_alert_runtime_state(source, metric_type)`: lay trang thai runtime.
- `_auto_register_metric_device(db, source, metric_type)`: tu dong dang ky device metric.
- `check_and_trigger_alert(...)`: check nguong va tao alert.
- `ingest_iot_metric(...)`: ingest metric IoT.

#### `ai_explanation_service.py`
- `_extract_retry_delay_seconds(raw_error_detail)`: lay so giay retry.
- `_extract_text(payload)`: tach text tu response AI.
- `_contains_vietnamese_diacritics(text)`: check co dau tieng Viet.
- `_http_post_json(url, payload, timeout)`: POST JSON.
- `_http_get_json(url, timeout)`: GET JSON.
- `_normalize_model_name(model_name)`: chuan hoa ten model.
- `_candidate_models()`: danh sach model co the dung.
- `_discover_models_from_api()`: tim model tu API.
- `explain_alert_with_gemini(alert_context)`: tao giai thich AI cho alert.

#### `alert_service.py`
- `_metric_label(metric_type)`: ten metric.
- `_device_label(alert, device)`: ten device.
- `_unit(alert, device)`: don vi.
- `_format_value(value, unit)`: format gia tri.
- `_threshold_text(alert, device, html)`: text nguong.
- `_allowed_range_text(alert, device, html)`: text khoang an toan.
- `_status_label(alert)`: nhan trang thai.
- `_alert_time(alert)`: format thoi gian alert.
- `_is_forecast_alert(alert)`: check alert forecast.
- `_title(alert)`: tao tieu de alert.
- `_value_label(alert)`: nhan gia tri.
- `_forecast_time(alert)`: lay forecast time.
- `_forecast_generated_time(alert)`: lay thoi diem tao forecast.
- `build_alert_text(alert, device)`: tao text alert tong hop.
- `_build_telegram_message(alert, device)`: tao message Telegram.
- `_build_email_html(alert, device)`: tao noi dung email HTML.
- `_build_email_subject(alert, device)`: tao subject email.
- `_send_email_alert_logged(email, subject, body)`: gui email co log.
- `send_email_alert_to_enabled_recipients(db, alert, device, owner)`: gui email cho recipients.
- `dispatch_alert_notifications(alert_id)`: phat alert sang cac kenh.

#### `chat_service.py`
- `_normalize_model_name(model_name)`: chuan hoa ten model.
- `_http_post_json(url, payload, timeout)`: POST JSON.
- `_extract_text(payload)`: tach text tu AI response.
- `_candidate_models()`: danh sach model co the dung.
- `_strip_accents(text)`: bo dau.
- `_normalize_text(text)`: chuan hoa text.
- `_format_context_time(value)`: format context time.
- `_summarize_user_context(db, user)`: tom tat boi canh user.
- `_fallback_reply(user_message, context)`: tra loi du phong.
- `generate_user_bot_reply(db, user, user_message)`: tao bot reply cho user.

#### `databricks_service.py`
- `DatabricksUnavailable`: exception khi Databricks khong san sang.
- `_table_name(table)`: tao ten table day du.
- `_is_configured()`: check cau hinh Databricks.
- `_connect()`: mo ket noi Databricks.
- `_rows_to_dicts(cursor)`: convert rows thanh dict.
- `_query_dicts(query, params)`: query tra ve dict.
- `_normalized_candidates(...)`: tao danh sach candidate da chuan hoa.
- `_slugify(text)`: slugify text.
- `_text_location_variants(text)`: bien the location text.
- `_location_text_candidates(...)`: candidate text location.
- `_location_slug_candidates(...)`: candidate slug location.
- `_context_from_metadata(sensor_id, metadata)`: tao context tu metadata.
- `_sensor_context(sensor_id, sensor_metadata)`: tao context sensor.
- `_query_grouped_forecast(...)`: query forecast grouped.
- `_fallback_forecast_query(sensor_id, limit)`: fallback forecast query.
- `_resolve_forecast_rows(sensor_id, limit, sensor_metadata)`: resolve forecast rows.
- `_query_model_rows(query, params)`: query model rows.
- `_resolve_location_reference_ids(context)`: resolve location ids.
- `_resolve_model_rows(sensor_id, limit, sensor_metadata)`: resolve model rows.
- `DatabricksService`: public service facade cho forecast/model leaderboard.

#### `metrics_service.py`
- `MetricsService`: service class quan ly metrics.

#### `mqtt_ingest_service.py`
- `_create_mqtt_client(mqtt_module, client_id)`: tao MQTT client.
- `_reason_code_is_success(reason_code)`: check MQTT reason code.
- `_sensor_id_from_topic(topic)`: lay sensor id tu topic.
- `_parse_payload(raw_payload, topic)`: parse MQTT payload.
- `start_mqtt_ingest()`: bat dau ingest MQTT.
- `stop_mqtt_ingest()`: dung ingest MQTT.

#### `telegram_service.py`
- `send_telegram_message(chat_id, message)`: gui Telegram message.

#### `weather_service.py`
- `GeocodeResult`: ket qua geocode.
- `_json_get(url, timeout)`: GET JSON.
- `geocode_location(query)`: geocode dia chi.
- `get_current_weather(latitude, longitude, timezone)`: lay thoi tiet hien tai.
- `get_meteostat_hourly_readings(latitude, longitude, hours)`: lay du lieu Meteostat theo gio.
- `get_virtual_weather_readings(...)`: tao virtual weather readings.
- `get_weather_for_timestamp(...)`: lay weather theo moc thoi gian.

---

## 3. IoT backend service: `iot_backend/`

### 3.1 `iot_backend/main.py`

#### Functions
- `_init_db_with_retry()`: khoi tao DB voi retry.
- `root()`: endpoint goc.
- `startup_event()`: startup flow, bat MQTT, scheduler forecast alert.
- `shutdown_event()`: shutdown flow, dung scheduler va MQTT.
- `_forecast_alert_scheduler()`: job quet canh bao forecast theo chu ky.
- `handle_mqtt_reading(reading)`: xu ly du lieu MQTT, luu DB, canh bao, broadcast websocket.

### 3.2 `iot_backend/config.py`

#### Functions
- `_load_dotenv()`: nap env.
- `get_cors_origins()`: lay CORS origins.
- `get_database_url()`: lay DB URL.

### 3.3 `iot_backend/database.py`

#### Functions
- `get_db()`: dependency session DB.
- `init_db()`: tao/kiem tra schema.
- `_ensure_iot_device_required_columns()`: dam bao cot thiet yeu.
- `_ensure_alert_required_columns()`: dam bao cot alert thiet yeu.
- `_backfill_iot_device_threshold_columns()`: backfill threshold.
- `_is_schema_auto_migrate_enabled()`: check auto migrate.
- `_run_schema_evolution()`: chay evolution schema.
- `_ensure_iot_device_columns()`: dam bao cot mo rong.
- `_cleanup_metric_columns()`: don cot metric cu.
- `_migrate_iot_device_source_constraint()`: migrate constraint source.
- `_ensure_chat_columns()`: dam bao cot chat.

### 3.4 `iot_backend/crud.py`

Phan nay ve co ban trung voi `app/crud.py`, nhung co them ham chat va user/device flow cho service IoT doc lap.

#### Metrics / alerts / users / devices / permissions
- `_normalize_metric_location_value(value)`
- `_resolve_metric_location(db, sensor_id, location)`
- `create_metric(db, metric)`
- `create_metrics_bulk(db, metrics)`
- `get_latest_metrics(db)`
- `get_metrics_history(...)`
- `get_metrics_in_range(...)`
- `get_all_metrics_in_range(db, minutes)`
- `delete_old_metrics(db, days)`
- `create_alert(db, alert)`
- `get_recent_alerts(db, hours, limit)`
- `get_unresolved_alerts(db)`
- `get_alerts_by_metric(db, metric_type, hours)`
- `resolve_alert(db, alert_id)`
- `delete_old_alerts(db, days)`
- `create_user(db, user, hashed_password)`
- `get_user_by_username(db, username)`
- `get_user_by_email(db, email)`
- `get_user_by_id(db, user_id)`
- `get_all_users(db)`
- `get_pending_users(db)`
- `approve_user(db, user_id, admin_id)`
- `reject_user(db, user_id)`
- `delete_user(db, user_id)`
- `create_device(db, device, admin_id)`
- `get_all_devices(db)`
- `get_device_by_id(db, device_id)`
- `get_device_by_source(db, source)`
- `delete_device(db, device_id)`
- `update_device(db, device_id, name, device_type, location)`
- `grant_device_permission(db, user_id, device_id, admin_id)`
- `revoke_device_permission(db, user_id, device_id)`
- `get_user_devices(db, user_id)`
- `get_device_users(db, device_id)`
- `get_user_accessible_sources(db, user_id)`
- `get_latest_metrics_for_user(db, user_id, source)`
- `get_metrics_history_for_user(...)`
- `get_latest_metric_for_user(...)`
- `get_metrics_history_by_date(...)`

#### Chat
- `create_chat_conversation(...)`
- `get_chat_conversation(db, conversation_id)`
- `get_latest_user_chat_conversation(db, user_id)`
- `list_user_chat_conversations(db, user_id)`
- `list_admin_chat_conversations(db, status_filter)`
- `update_chat_conversation_status(...)`
- `create_chat_message(...)`
- `list_chat_messages(db, conversation_id)`
- `delete_chat_conversation(db, conversation)`
- `list_chat_issue_templates(db, active_only)`
- `get_chat_issue_template(db, template_id)`
- `create_chat_issue_template(...)`
- `update_chat_issue_template(db, row, data)`
- `delete_chat_issue_template(db, row)`

### 3.5 `iot_backend/models.py`

#### Data models
- `Metric`, `Alert`, `User`, `UserNotificationTarget`, `Device`, `UserDevicePermission`, `IoTDevice`, `SensorReading`, `ChatConversation`, `ChatMessage`, `ChatIssueTemplate`.

### 3.6 `iot_backend/schemas.py`

#### Pydantic schemas
- `MetricCreate`, `MetricBulkCreate`, `MetricResponse`, `LatestMetricsResponse`, `SummaryMetricsResponse`, `MetricsHistoryResponse`, `HealthResponse`.
- `AlertCreate`, `AlertResponse`, `AlertListResponse`.
- `UserRegister`, `UserLogin`, `TokenResponse`, `UserResponse`.
- `DeviceCreate`, `DeviceUpdate`, `DeviceResponse`, `UserDevicePermissionResponse`.
- `ChatConversationResponse`, `ChatMessageResponse`, `ChatSendRequest`, `ChatEscalateRequest`, `ChatAdminReplyRequest`, `ChatConversationListResponse`, `ChatMessagesListResponse`.
- `ChatIssueTemplateCreate`, `ChatIssueTemplateUpdate`, `ChatIssueTemplateResponse`.

### 3.7 `iot_backend/api/`

#### `routes_auth.py`
- `_success_notification_html()`
- `hash_password(password)`
- `verify_password(plain_password, hashed_password)`
- `create_access_token(data)`
- `get_current_user(authorization, db)`
- `register(user_data, db)`
- `login(credentials, db)`
- `get_me(current_user)`
- `get_my_devices(...)`
- `get_user_info(user_id, db)`
- `link_telegram(...)`
- `test_telegram(...)`
- `unlink_telegram(...)`
- `enable_email_alerts(...)`
- `test_email_alerts(...)`
- `get_email_status(current_user)`
- `toggle_email_alerts(...)`
- `update_email_address(...)`
- `disable_email_alerts(...)`
- `list_notification_targets(...)`
- `add_notification_target(...)`
- `toggle_notification_target(...)`
- `test_notification_target(...)`
- `delete_notification_target(...)`

#### `routes_sensors.py`
- `_normalize_source(value)`
- `_serialize_device(device, latest)`
- `_latest_for_sensor(db, sensor_id)`
- `_publish_threshold_configs_for_sensor(device)`
- `_sensor_device_or_404(db, sensor_id, user)`
- `_resolve_history_window(...)`
- `list_sensors(user, db)`
- `create_sensor(payload, user, db)`
- `get_sensor(sensor_id, user, db)`
- `update_sensor(sensor_id, payload, user, db)`
- `delete_sensor(sensor_id, user, db)`
- `ingest_reading(payload, user, db)`
- `latest_reading(sensor_id, response, user, db)`
- `reading_history(sensor_id, minutes, user, db)`
- `export_sensor_history_csv(...)`
- `geocode_sensor_location(payload, user)`

#### `routes_iot_devices.py`
- `_normalize_source(value)`
- `_normalize_environment_type(value)`
- `_environment_label(value)`
- `_normalize_metric_type(device_type, metric_type)`
- `_default_unit_for_metric(metric_type)`
- `_serialize_device(device)`
- `_thresholds_configured(device)`
- `_field_was_provided(payload, field_name)`
- `_resolve_threshold_values(...)`
- `_publish_threshold_config_for_device(device)`
- `_alert_status(device, latest_value)`
- `_serialize_realtime_device(db, user, device)`
- `_sync_device_row(db, device)`
- `get_my_iot_devices(user, db)`
- `get_my_iot_devices_realtime(user, db)`
- `create_iot_device(...)`
- `update_iot_device(...)`
- `delete_iot_device(device_id, user, db)`
- `update_alert_thresholds(...)`
- `geocode_sensor_location(...)`

#### `routes_websocket.py`
- `_normalize_source(payload, fallback)`
- `_decode_ws_token(token)`
- `_can_receive_source(connection_info, source)`
- `_parse_metric_timestamp(timestamp)`
- `save_iot_metric_to_db(...)`
- `websocket_endpoint(websocket, client_id)`
- `get_status()`
- `get_client_status(client_id)`
- `health_check()`

#### `routes_devices.py`
- `ManualCommand`
- `WifiConfigRequest`
- `WifiScanRequest`
- `ManualSensorCommandRequest`

#### `routes_alerts.py` trong `iot_backend/`
- `_owned_iot_device_ids(db, user_id)`: lay danh sach device user so huu.
- `_filter_alerts_by_user(db, current_user, alerts)`: loc alert theo quyen user.
- `_ensure_alert_access(db, current_user, alert)`: dam bao user co quyen xem alert.
- `create_alert(...)`: tao alert thu cong.
- `run_forecast_alert_scan_now(...)`: chay quet forecast ngay lap tuc.
- `get_alerts(...)`: lay alert.
- `get_recent_alerts(...)`: lay alert gan day.
- `get_unresolved_alerts(...)`: lay alert chua xu ly.
- `get_alerts_by_metric(...)`: loc alert theo metric.
- `resolve_alert(...)`: resolve alert.
- `cleanup_old_alerts(...)`: don alert cu.
- `explain_alert_with_ai(...)`: giai thich alert bang AI.

#### `routes_admin_iot.py` trong `iot_backend/`
- `verify_admin(user)`: guard admin.
- `_serialize(device)`: serialize device IoT.
- `get_all_iot_devices(admin, db)`: lay tat ca IoT device.
- `get_iot_devices_summary(admin, db)`: lay tong hop IoT device.
- `delete_iot_device(device_id, admin, db)`: xoa IoT device.
- `disconnect_iot_device(device_id, admin, db)`: ngat ket noi device.
- `reconnect_iot_device(device_id, admin, db)`: ket noi lai device.


### 3.8 `iot_backend/services/`

#### `forecast_alert_service.py`
- `_device_metadata(device)`
- `_forecast_rows_for_device(device)`
- `_dedupe_devices(devices)`
- `_load_target_devices(user_id, is_admin)`
- `_forecast_window()`
- `_to_vn_naive(value)`
- `_first_breaches(device, rows)`
- `_is_duplicate_forecast_alert(db, device, breach)`
- `_forecast_message(device, breach)`
- `run_forecast_alert_scan(...)`
- `dispatch_created_forecast_alerts(alert_ids)`

#### `databricks_service.py`
- `DatabricksWriteSkipped`
- `_table_name()`
- `_is_configured()`
- `_connect()`
- `write_bronze_sensor_reading(reading)`

#### `email_service.py`
- `send_email_alert(to_email, subject, html_body)`

#### `ai_explanation_service.py`
- `_extract_retry_delay_seconds(raw_error_detail)`
- `_extract_text(payload)`
- `_contains_vietnamese_diacritics(text)`
- `_http_post_json(url, payload, timeout)`
- `_http_get_json(url, timeout)`
- `_normalize_model_name(model_name)`
- `_candidate_models()`
- `_discover_models_from_api()`
- `explain_alert_with_gemini(alert_context)`

#### `alert_service.py`
- `_metric_label(metric_type)`
- `_device_label(alert, device)`
- `_unit(alert, device)`
- `_format_value(value, unit)`
- `_threshold_text(alert, device, html)`
- `_allowed_range_text(alert, device, html)`
- `_status_label(alert)`
- `_alert_time(alert)`
- `_is_forecast_alert(alert)`
- `_title(alert)`
- `_value_label(alert)`
- `_forecast_time(alert)`
- `_forecast_generated_time(alert)`
- `build_alert_text(alert, device)`
- `_build_telegram_message(alert, device)`
- `_build_email_html(alert, device)`
- `_build_email_subject(alert, device)`
- `dispatch_alert_notifications(alert_id)`

#### `metrics_service.py`
- `MetricsService`

#### `sensor_reading_service.py`
- `parse_event_ts(value)`
- `_find_device(db, sensor_id)`
- `serialize_reading(row)`
- `create_sensor_reading(...)`

#### `telegram_service.py`
- `send_telegram_message(chat_id, message)`

#### `threshold_alert_service.py`
- `_pick_thresholds(device, metric_type)`
- `_find_device_for_metric(db, source, metric_type)`
- `check_and_trigger_metric_alert(...)`

#### `weather_service.py`
- `GeocodeResult`
- `_json_get(url, timeout)`
- `geocode_location(query)`
- `get_current_weather(latitude, longitude, timezone)`
- `get_meteostat_hourly_readings(latitude, longitude, hours)`
- `get_virtual_weather_readings(...)`
- `get_weather_for_timestamp(...)`

#### `mqtt_consumer.py`
- `_create_mqtt_client(mqtt_module)`
- `_ensure_ws_conn()`
- `_normalize_payload(raw_payload, topic)`
- `main()`

#### `mqtt_service.py`
- MQTT wrapper used by the backend startup flow to manage MQTT lifecycle and callbacks.

---

## 4. Databricks pipeline: `databricks/`

### 4.1 `00_check_databricks_connection.py`
- `load_local_env()`
- `_widget_value(name)`
- `_setting(name, default)`
- `_ensure_widgets()`
- `_env_or_secret(name, default)`
- `_missing_config_message(missing)`
- `_jdbc_url()`
- `_jdbc_credentials()`

### 4.2 `03_sync_postgres_to_bronze.py`
- `load_local_env()`
- `widget_value(name)`
- `setting(name)`
- `create_widgets()`
- `secret(scope_name, key_name)`
- `jdbc_url()`
- `credentials()`
- `fq_table(name)`
- `catalog_exists(catalog)`
- `ensure_namespace()`
- `source_query()`
- `read_postgres()`
- `prepare_bronze(raw)`
- `table_exists(table_name)`
- `is_delta_table(target_table)`
- `target_has_canonical_schema(target_table)`
- `replace_table(df, target_table, reason)`
- `merge_bronze(df, target_table)`
- `main()`

### 4.3 `04_sync_meteostat_to_bronze.py`
- `load_local_env()`
- `patch_pandas_parse_dates_for_meteostat()`
- `utc_now()`
- `widget_value(name)`
- `setting(name)`
- `setting_any(*names)`
- `bool_setting(*names)`
- `int_setting(default_value, *names)`
- `float_setting(default_value, *names)`
- `create_widgets()`
- `fq_table(name)`
- `debug_enabled()`
- `debug_print(message)`
- `add_collection_warning(...)`
- `data_quality_for_distance(distance_km, fetch_method)`
- `active_config()`
- `print_active_config()`
- `table_exists(table_name)`
- `namespace_exists(catalog, schema)`
- `ensure_namespace_and_tables()`
- `configured_years()`
- `load_locations()`
- `ensure_meteostat_runtime()`
- `safe_float(value)`
- `safe_timestamp(value)`
- `haversine_km(lat1, lon1, lat2, lon2)`
- `row_value(row, names)`
- `normalize_meteostat_frame(frame, location, fetch_method, station_meta)`
- `count_available_metric_columns(frame)`
- `year_matches_inventory(station_meta, year)`
- `expected_hours_for_year(year)`
- `coverage_ratio(row_count, expected_hours)`
- `station_meta_from_row(location, station_id, row)`
- `upsert_weather_stations(...)`
- `write_station_mapping(...)`
- `fetch_nearby_station_year(...)`
- `fetch_location_year(location, year)`
- `sql_string(value)`
- `latest_status(location_id, year)`
- `should_skip(location, year)`
- `write_status(...)`
- `warning_count_for(location_id, year)`
- `add_summary_row(...)`
- `write_collection_summary()`
- `print_collection_summary()`
- `write_location_year_batch(...)`
- `main()`

### 4.4 `05_silver_cleaning.py`
- `load_local_env()`
- `widget_value(name)`
- `setting(name)`
- `setting_any(*names)`
- `bool_setting(*names)`
- `create_widgets()`
- `fq_table(name)`
- `table_exists(name)`
- `filter_active_meteostat_locations(df)`
- `empty_df(schema)`
- `write_delta(df, table_name, partition_cols)`
- `clean_esp32()`
- `build_esp32_hourly(silver_esp32)`
- `clean_meteostat()`
- `build_joined(esp32_hourly, meteostat)`
- `main()`

### 4.5 `06_gold_feature_engineering.py`
- `load_local_env()`
- `widget_value(name)`
- `setting(name)`
- `warn_stale_catalog_once()`
- `create_widgets()`
- `fq_table(name)`
- `table_exists(name)`
- `overwrite_existing_table(df, table_name)`
- `main()`

### 4.6 `07_train_machine_learning.py`
- `normalize_train_family(value)`
- `selected_targets()`
- `train_profile()`
- `train_scope()`
- `quick_int_setting(name, default_value)`
- `load_local_env()`
- `widget_value(name)`
- `setting(name)`
- `bool_setting(name)`
- `int_setting(name, default_value)`
- `float_setting(name, default_value)`
- `utc_now()`
- `create_widgets()`
- `fq_table(name)`
- `table_exists(table_name)`
- `sql_list(values)`
- `metric_values(predictions, label_col)`
- `quality_label(metrics)`
- `log_run_summary_artifact(summary)`
- `load_training_data()`
- `split_by_time(sdf, target_col)`
- `preprocess_stages()`
- `model_specs(label_col)`
- `model_spec_by_name(label_col)`
- `limited_location_df(sdf, location_id, target_col)`
- `location_candidates(sdf, target_col)`
- `train_location_best_models(sdf, global_rows, targets_to_train)`
- `import_tensorflow()`
- `deep_learning_model_specs(...)`
- `make_deep_learning_arrays(...)`
- `deep_learning_metrics(y_true, y_pred)`

### 4.7 `08_train_deep_learning_light.py`
- `normalize_train_family(value)`
- `selected_targets()`
- `train_scope()`
- `load_local_env()`
- `widget_value(name)`
- `setting(name)`
- `bool_setting(name)`
- `int_setting(name, default_value)`
- `float_setting(name, default_value)`
- `utc_now()`
- `create_widgets()`
- `fq_table(name)`
- `table_exists(table_name)`
- `sql_list(values)`
- `metric_values(predictions, label_col)`
- `quality_label(metrics)`
- `log_run_summary_artifact(summary)`
- `load_training_data()`
- `split_by_time(sdf, target_col)`
- `preprocess_stages()`
- `model_specs(label_col)`
- `import_tensorflow()`
- `deep_learning_model_specs(...)`
- `make_deep_learning_arrays(...)`
- `deep_learning_metrics(y_true, y_pred)`
- `train_deep_learning_target(...)`
- `location_candidates(sdf, target_col)`
- `prepare_location_arrays(sdf, location_id, target_col)`
- `train_location_deep_learning_models(sdf, global_rows, targets_to_train)`
- `metrics_schema()`
- `train_target(sdf, target_variable, target_col, train_family)`
- `write_model_metrics(...)`
- `main()`

### 4.8 `09_select_best_models.py`
- Model selection logic and leaderboard ranking for the trained models.

### 4.9 `10_generate_7day_forecast.py`
- `load_local_env()`
- `widget_value(name)`
- `setting(name)`
- `warn_stale_catalog_once()`
- `setting_any(*names)`
- `int_setting(default_value, *names)`
- `bool_setting(*names)`
- `requested_location_ids()`
- `create_widgets()`
- `fq_table(name)`
- `table_exists(name)`
- `prepare_mlflow_tmp_dir()`
- `filter_locations_df(locations_df)`
- `status_for(metric_row, fallback)`
- `load_history()`
- `load_devices()`
- `load_metrics()`
- `select_best_model(metrics_pdf, target_variable, location_id)`
- `model_scope_for(metric_row, location_id)`
- `print_forecast_plan(devices, metrics_pdf)`
- `candidate_model_uris(metric_row)`
- `run_id_from_model_uri(model_uri)`
- `dedupe_model_uris(candidates)`
- `model_uris_from_run_metadata(run_id)`
- `model_uris_from_logged_models(run_id)`
- `model_uris_from_artifacts(run_id)`
- `load_spark_mlflow_model(model_uri)`
- `load_mlflow_model(metric_row, model_cache, warn_on_failure)`
- `spark_preprocess_stages()`
- `estimator_from_metric_row(metric_row, label_col)`
- `fit_location_spark_model(...)`
- `recent_biases(window_days)`
- `fallback_forecast(series, horizon)`
- `lag(values, step)`
- `build_feature_row(history, future_ts, location_id, province_id, device_id, temp_values, humidity_values)`
- `deep_learning_forecast(model, history, target_variable, horizon, input_window)`
- `model_forecast(...)`
- `main()`

### 4.10 `00_check_databricks_connection.py`
- `load_local_env()`
- `_widget_value(name)`
- `_setting(name, default)`
- `_ensure_widgets()`
- `_env_or_secret(name, default)`
- `_missing_config_message(missing)`
- `_jdbc_url()`
- `_jdbc_credentials()`

---

## 5. Frontend: `frontend/src/`

### 5.1 App bootstrap
- `AppContent()` in `App.jsx`: chon man hinh theo menu.
- `App()` in `App.jsx`: wrapper root cho app React.

### 5.2 Contexts
#### `context/AuthContext.jsx`
- `AuthProvider({ children })`
- `logout()`
- `useAuth()`

#### `context/DeviceContext.jsx`
- `DeviceProvider({ children })`
- `useDevices()`

#### `context/NotificationContext.jsx`
- `normalizeMessage(message)`
- `inferTypeFromMessage(message)`
- `NotificationProvider({ children })`
- `handleNotifyEvent(event)`
- `useNotification()`

### 5.3 Core components
- `AddDeviceModal(...)`
- `AdminDashboard()`
- `AdminIssueTemplates()`
- `AdminIoTOverview()`
- `AdminPanel()`
- `Alerts()`
- `CPUMetrics()`
- `ClientMonitor()`
- `DatabricksModelReport()`
- `Dashboard()`
- `EditAlertThresholdsModal(...)`
- `GaugeChart(...)`
- `IoTMetrics()`
- `IoTDeviceManager()`
- `Login()`
- `MemoryMetrics()`
- `MetricCard(...)`
- `ProtectedFeature(...)`
- `OptionalFeature(...)`
- `DeviceTypeFilter(...)`
- `Sidebar(...)`
- `SensorAiContextModal(...)`
- `SensorAlertThresholdModal(...)`
- `SensorEditModal(...)`
- `SensorWifiModal(...)`
- `SimpleGauge(...)`
- `SupportChat()`
- `UserDashboard()`

### 5.4 Notable helper functions in components
#### `Alerts.jsx`
- `formatMetricName(metric)`
- `getMetricUnit(metric)`
- `formatVNDateTime(value)`
- `formatVNTime(value)`
- `formatOneDecimal(value)`
- `getAlertIcon()`
- `getAlertColor()`
- `getStatusColor()`
- `isForecastAlert(alert)`
- `runForecastScan()`
- `explainWithAI(alert)`

#### `DatabricksModelReport.jsx`
- `getSensorId(sensor)`
- `fmt(value, digits)`
- `intFmt(value)`
- `targetLabel(value)`
- `trainingLabel(value)`
- `familyLabel(value)`
- `shortTarget(value)`
- `shortModelName(value)`
- `SummaryCard(...)`
- `WinnerCard(...)`
- `ChartCard(...)`
- `MetricLineChart(...)`
- `App-level data normalization and best-model selection helpers inside `DatabricksModelReport()``

#### `IoTDeviceManager.jsx`
- `getSensorId(sensor)`
- `fmt(value, suffix)`
- `onlyTime(value)`
- `noCacheConfig(params)`
- `toNumberOrNull(value)`
- `isOutOfRange(value, minValue, maxValue)`
- `formatThreshold(minValue, maxValue, unit)`
- `numericValues(rows, key)`
- `stat(rows, key, mode)`
- `minuteKey(value)`
- `minuteLabel(value)`
- `toMinuteChartData(historyRows)`
- `showToast(message, tone)`
- `handleVisibilityChange()`
- `connect()`
- `openSettingModal(kind, sensor)`
- `SensorSettingMenu(...)`
- `DeviceToggleRow(...)`
- `SensorChartModal(...)`
- `StatBox(...)`
- `EmptyState(...)`
- `NotificationSettingsModal(...)`
- `TargetList(...)`

#### `SupportChat.jsx`
- `content = (contentOverride ?? message).trim()` in message formatting flow.
- `editIssueTemplate(item)`
- `isOther` helper for template handling.
- `handleSend()`

#### `UserDashboard.jsx`
- `getSensorId(sensor)`
- `today()`
- `fmt(value, digits)`
- `onlyTime(value)`
- `noCacheConfig(params)`
- `startOfDay(value)`
- `endOfDay(value)`
- `dayDiffInclusive(fromDate, toDate)`
- `hourKey(value)`
- `average(values)`
- `addForecastBridge(rows)`
- `listDateKeys(fromDate, toDate)`
- `handleVisibilityChange()`
- `connect()`
- `rawActual` / `predicted` processing blocks inside the component.
- `SummaryCard(...)`

### 5.5 Utilities
#### `api.js`
- `isIotApi(url)`: check API path belongs to iot backend.

#### `utils/vnTime.js`
- `toDateSafe(value, options)`
- `formatVNTime(value, withSeconds, options)`
- `formatVNDate(value, options)`
- `formatVNDateTime(value, withSeconds, options)`
- `getVNDateInputValue()`

#### `utils/alertService.js`
- `saveAlert(...)` in the alert persistence flow.
- `getThresholdForStatus(metricType, status)`

#### `utils/alertUtils.js`
- `checkAlert(metricType, value)`
- `createAlert(metricType, value, timestamp)`
- `getAlertStyle(status)`
- `checkMetricAlert(metricType, value)`

---

## 6. Quick functional map

- Realtime ingest: MQTT / websocket / sensor reading services.
- Operational DB: users, devices, permissions, alerts, sensor readings.
- Forecast pipeline: Databricks Bronze / Silver / Gold / training / leaderboard / forecast.
- Forecast alerting: iot_backend forecast scan + duplicate detection + notification dispatch.
- AI support: chat assistant and alert explanation service.
- Frontend: dashboard, device manager, alerts, model report, support chat.

---

## 7. Notes

- Tai lieu nay phan anh trang thai code hien tai cua workspace.
- Neu ban them/xoa ham, nen cap nhat lai file nay de doc de doi chieu nhanh hon.
