#include <WiFi.h>
#include <WiFiManager.h>
#include <Preferences.h>
#include <PubSubClient.h>
#include <DHT.h>
#include <time.h>
#include <ArduinoJson.h>
#include <ctype.h>

// =====================================================
// ESP32 + DHT11 + MQTT + Relay Fan/Fog
// Current project flow:
// - Publish one sensor-level MQTT payload containing both temperature and humidity
// - Receive manual/auto relay commands via ptdl/devices/{sensor_id}/commands
// - Publish WiFi scan results and device state back to MQTT
// Final wiring:
// - Fan GPIO18: active LOW
// - Fog GPIO19: active HIGH
// - No lamp
// =====================================================

// ===== WiFi setup portal =====
const char* WIFI_CONFIG_AP_NAME = "ESP32_Setup";

// ===== MQTT broker VPS =====
const char* MQTT_HOST = "20.214.247.102";
const int MQTT_PORT = 1883;
const char* MQTT_USER = "sensor_user";
const char* MQTT_PASSWORD = "123456";

// ===== Device =====
// Keep these metadata values aligned with the sensor you create in the web app.
const char* SENSOR_ID = "esp32_devkit_v1";
const char* LOCATION = "Lab";
const char* LOCATION_PROVINCE = "Ho Chi Minh City";
const char* SOURCE_TYPE = "physical_iot";
const char* PROVIDER = "esp32";
const char* ENVIRONMENT_TYPE = "indoor";
const char* ALERT_CONFIG_NAMESPACE = "alert_cfg";
const char* MQTT_GLOBAL_STATUS_TOPIC = "ptdl/status";

// ===== Pins =====
#define DHT_PIN 4
#define DHT_TYPE DHT11
#define RELAY_FAN 18
#define RELAY_FOG 19

// Fan relay active LOW: LOW = ON, HIGH = OFF.
const int FAN_RELAY_OFF = HIGH;
const int FAN_RELAY_ON = LOW;

// Fog relay active HIGH: HIGH = ON, LOW = OFF.
const int FOG_RELAY_OFF = LOW;
const int FOG_RELAY_ON = HIGH;

// ===== Timing =====
const unsigned long SEND_INTERVAL_MS = 5000;
const unsigned long WIFI_RETRY_INTERVAL_MS = 5000;
const unsigned long MQTT_RETRY_INTERVAL_MS = 5000;
const unsigned long AUTO_CONTROL_INTERVAL_MS = 5000;
const unsigned long RELAY_CHANGE_MIN_INTERVAL_MS = 10000;

// ===== Auto mode hysteresis =====
const float FAN_ON_TEMP = 50.0;
const float FAN_OFF_TEMP = 15.0;
const float FOG_ON_HUMIDITY = 40.0;
const float FOG_OFF_HUMIDITY = 60.0;

const bool RUN_RELAY_TEST_ON_BOOT = false;

DHT dht(DHT_PIN, DHT_TYPE);
WiFiClient wifiClient;
PubSubClient mqtt(wifiClient);
Preferences preferences;
WiFiManager wifiManager;

String wifiSsid;
String wifiPassword;

unsigned long lastSendMs = 0;
unsigned long lastWifiRetryMs = 0;
unsigned long lastMqttRetryMs = 0;
unsigned long lastAutoControlMs = 0;
unsigned long lastFanChangeMs = 0;
unsigned long lastFogChangeMs = 0;

bool fanState = false;
bool fogState = false;
bool autoMode = true;
bool timeSynced = false;

struct ThresholdConfig {
  bool loaded = false;
  bool alertEnabled = false;
  bool hasMin = false;
  bool hasMax = false;
  float minValue = 0.0;
  float maxValue = 0.0;
};

ThresholdConfig temperatureThresholdConfig;
ThresholdConfig humidityThresholdConfig;

bool connectWiFi(bool force = false, bool allowPortalFallback = false);
void connectMQTT();
bool applyWiFiCredentials(String newSsid, String newPassword, const char* source);
void publishDeviceState();
void publishWifiList();
void handleCommandPayload(const String& payload);
String metricStorageSuffix(const String& metricType);
void saveThresholdConfig(const String& metricType, JsonObjectConst configObj);
ThresholdConfig loadThresholdConfig(const String& metricType);
void refreshThresholdConfigs();
bool hasStoredWiFiCredentials();
bool startWiFiConfigPortal();
void syncConnectedWiFiToPreferences(const char* source);
String getSensorReadingTopic();
String getDeviceTopic(const char* suffix);
String getIsoTimestamp();
void publishSensorReading(float temperature, float humidity, const String& timestamp);

// ===== Time =====
bool isTimeValid() {
  time_t now = time(nullptr);
  return now > 1700000000;
}

String getIsoTimestamp() {
  time_t now = time(nullptr);
  struct tm* timeinfo = localtime(&now);
  char buffer[32];

  if (timeinfo == nullptr || !isTimeValid()) {
    snprintf(buffer, sizeof(buffer), "1970-01-01T00:00:00+07:00");
  } else {
    snprintf(
      buffer,
      sizeof(buffer),
      "%04d-%02d-%02dT%02d:%02d:%02d+07:00",
      timeinfo->tm_year + 1900,
      timeinfo->tm_mon + 1,
      timeinfo->tm_mday,
      timeinfo->tm_hour,
      timeinfo->tm_min,
      timeinfo->tm_sec
    );
  }

  return String(buffer);
}

bool syncClock() {
  if (WiFi.status() != WL_CONNECTED) {
    return false;
  }

  Serial.println("[Time] Syncing NTP UTC+7...");
  configTime(7 * 3600, 0, "pool.ntp.org", "time.google.com", "time.nist.gov");

  unsigned long startMs = millis();
  while (!isTimeValid() && millis() - startMs < 10000) {
    Serial.print(".");
    delay(500);
  }
  Serial.println();

  timeSynced = isTimeValid();
  if (timeSynced) {
    Serial.print("[Time] Synced: ");
    Serial.println(getIsoTimestamp());
  } else {
    Serial.println("[Time] NTP sync failed. Sensor publish will wait.");
  }

  return timeSynced;
}

String getSensorReadingTopic() {
  return String("sensors/") + SENSOR_ID + "/data";
}

String getDeviceTopic(const char* suffix) {
  return String("ptdl/devices/") + SENSOR_ID + "/" + suffix;
}

// ===== WiFi storage =====
void loadWiFiCredentials() {
  preferences.begin("wifi_cfg", true);
  String savedSsid = preferences.getString("ssid", "");
  String savedPass = preferences.getString("pass", "");
  preferences.end();

  if (savedSsid.length() > 0) {
    wifiSsid = savedSsid;
    wifiPassword = savedPass;
    Serial.println("[WiFi] Loaded saved WiFi from memory");
  } else {
    wifiSsid = "";
    wifiPassword = "";
    Serial.println("[WiFi] No saved WiFi in memory");
  }
}

bool saveWiFiCredentials(const String& ssid, const String& password) {
  preferences.begin("wifi_cfg", false);
  size_t ssidBytes = preferences.putString("ssid", ssid);
  size_t passBytes = preferences.putString("pass", password);
  preferences.end();
  return ssidBytes > 0 && (password.length() == 0 || passBytes > 0);
}

void resetWiFiCredentials() {
  preferences.begin("wifi_cfg", false);
  preferences.clear();
  preferences.end();
  wifiManager.resetSettings();
  WiFi.disconnect(true, true);

  wifiSsid = "";
  wifiPassword = "";

  Serial.println("[WiFi] Cleared saved WiFi credentials");
}

bool hasStoredWiFiCredentials() {
  return wifiSsid.length() > 0;
}

void syncConnectedWiFiToPreferences(const char* source) {
  String connectedSsid = WiFi.SSID();
  if (connectedSsid.length() == 0) {
    Serial.println("[WiFi] Skip sync: connected SSID is empty");
    return;
  }

  String connectedPass = WiFi.psk();
  bool saved = saveWiFiCredentials(connectedSsid, connectedPass);

  wifiSsid = connectedSsid;
  wifiPassword = connectedPass;

  Serial.print("[WiFi] Synced connected WiFi from ");
  Serial.print(source);
  Serial.print(": ");
  Serial.print(connectedSsid);
  Serial.print(" (save=");
  Serial.print(saved ? "OK" : "FAILED");
  Serial.println(")");
}

bool startWiFiConfigPortal() {
  Serial.println();
  Serial.println("===== WIFI MANAGER PORTAL =====");
  Serial.println("[WiFiManager] Starting captive portal");
  Serial.print("[WiFiManager] AP name: ");
  Serial.println(WIFI_CONFIG_AP_NAME);
  Serial.println("[WiFiManager] Connect your phone/laptop and open 192.168.4.1 if the portal does not appear automatically");

  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  WiFi.setAutoReconnect(true);
  wifiManager.setDebugOutput(true);
  wifiManager.setConnectTimeout(15);
  wifiManager.setAPCallback([](WiFiManager* manager) {
    (void)manager;
    Serial.println("[WiFiManager] Captive portal ready");
    Serial.print("[WiFiManager] AP IP: ");
    Serial.println(WiFi.softAPIP());
    Serial.print("[WiFiManager] AP name: ");
    Serial.println(WIFI_CONFIG_AP_NAME);
  });
  wifiManager.setSaveConfigCallback([]() {
    Serial.println("[WiFiManager] New WiFi credentials received from portal");
  });

  bool ok = wifiManager.autoConnect(WIFI_CONFIG_AP_NAME);
  if (!ok || WiFi.status() != WL_CONNECTED) {
    Serial.println("[WiFiManager] Portal ended without WiFi connection");
    return false;
  }

  Serial.println("[WiFiManager] WiFi connected from captive portal");
  Serial.print("[WiFiManager] SSID: ");
  Serial.println(WiFi.SSID());
  Serial.print("[WiFiManager] IP: ");
  Serial.println(WiFi.localIP());

  syncConnectedWiFiToPreferences("WiFiManager");
  timeSynced = false;
  syncClock();
  return true;
}

String metricStorageSuffix(const String& metricType) {
  String value = metricType;
  value.trim();
  value.toLowerCase();

  if (value == "temperature") return "temp";
  if (value == "humidity") return "hum";
  if (value == "soil_moisture") return "soil";
  if (value == "light_intensity") return "light";
  if (value == "pressure") return "press";

  if (value.length() > 8) {
    value = value.substring(0, 8);
  }

  for (unsigned int i = 0; i < value.length(); i++) {
    char c = value.charAt(i);
    if (!isalnum(c)) {
      value.setCharAt(i, '_');
    }
  }

  return value;
}

void saveThresholdConfig(const String& metricType, JsonObjectConst configObj) {
  if (metricType.length() == 0) {
    Serial.println("[ALERT CFG] MQTT update ignored: missing metric_type");
    return;
  }

  DynamicJsonDocument savedDoc(256);
  savedDoc["metric_type"] = metricType;
  savedDoc["alert_enabled"] = configObj["alert_enabled"] | false;
  savedDoc["unit"] = configObj["unit"] | "";
  savedDoc["updated_at"] = configObj["updated_at"] | "";
  savedDoc["device_id"] = configObj["device_id"] | 0;

  JsonVariantConst minVar = configObj["min_threshold"];
  JsonVariantConst maxVar = configObj["max_threshold"];
  if (minVar.isNull()) {
    savedDoc["min_threshold"] = nullptr;
  } else {
    savedDoc["min_threshold"] = minVar.as<float>();
  }
  if (maxVar.isNull()) {
    savedDoc["max_threshold"] = nullptr;
  } else {
    savedDoc["max_threshold"] = maxVar.as<float>();
  }

  String storageKey = "cfg_" + metricStorageSuffix(metricType);
  String jsonValue;
  serializeJson(savedDoc, jsonValue);

  preferences.begin(ALERT_CONFIG_NAMESPACE, false);
  preferences.putString(storageKey.c_str(), jsonValue);
  preferences.end();

  Serial.print("[ALERT CFG] Saved ");
  Serial.print(metricType);
  Serial.print(" to NVS key ");
  Serial.print(storageKey);
  Serial.print(": ");
  Serial.println(jsonValue);

  refreshThresholdConfigs();
}

ThresholdConfig loadThresholdConfig(const String& metricType) {
  ThresholdConfig config;
  String storageKey = "cfg_" + metricStorageSuffix(metricType);

  preferences.begin(ALERT_CONFIG_NAMESPACE, true);
  String raw = preferences.getString(storageKey.c_str(), "");
  preferences.end();

  if (raw.length() == 0) {
    return config;
  }

  DynamicJsonDocument doc(256);
  DeserializationError err = deserializeJson(doc, raw);
  if (err) {
    Serial.print("[ALERT CFG] Failed to parse ");
    Serial.print(storageKey);
    Serial.print(": ");
    Serial.println(err.c_str());
    return config;
  }

  JsonObjectConst root = doc.as<JsonObjectConst>();
  config.loaded = true;
  config.alertEnabled = root["alert_enabled"] | false;
  config.hasMin = !root["min_threshold"].isNull();
  config.hasMax = !root["max_threshold"].isNull();
  if (config.hasMin) {
    config.minValue = root["min_threshold"].as<float>();
  }
  if (config.hasMax) {
    config.maxValue = root["max_threshold"].as<float>();
  }
  return config;
}

void refreshThresholdConfigs() {
  temperatureThresholdConfig = loadThresholdConfig("temperature");
  humidityThresholdConfig = loadThresholdConfig("humidity");

  Serial.print("[ALERT CFG] Temperature loaded=");
  Serial.print(temperatureThresholdConfig.loaded ? "true" : "false");
  Serial.print(" alert_enabled=");
  Serial.println(temperatureThresholdConfig.alertEnabled ? "true" : "false");

  Serial.print("[ALERT CFG] Humidity loaded=");
  Serial.print(humidityThresholdConfig.loaded ? "true" : "false");
  Serial.print(" alert_enabled=");
  Serial.println(humidityThresholdConfig.alertEnabled ? "true" : "false");
}

// ===== Relay =====
bool relayCanChange(unsigned long lastChangeMs, bool force) {
  if (force || lastChangeMs == 0) {
    return true;
  }
  return millis() - lastChangeMs >= RELAY_CHANGE_MIN_INTERVAL_MS;
}

bool setFan(bool on, bool force = false) {
  if (fanState == on) {
    return false;
  }
  if (!relayCanChange(lastFanChangeMs, force)) {
    Serial.println("[RELAY] Fan change ignored: debounce 10s");
    return false;
  }

  fanState = on;
  lastFanChangeMs = millis();
  digitalWrite(RELAY_FAN, fanState ? FAN_RELAY_ON : FAN_RELAY_OFF);

  Serial.print("[RELAY] Fan ");
  Serial.println(fanState ? "ON (GPIO18 LOW)" : "OFF (GPIO18 HIGH)");
  return true;
}

bool setFog(bool on, bool force = false) {
  if (fogState == on) {
    return false;
  }
  if (!relayCanChange(lastFogChangeMs, force)) {
    Serial.println("[RELAY] Fog change ignored: debounce 10s");
    return false;
  }

  fogState = on;
  lastFogChangeMs = millis();
  digitalWrite(RELAY_FOG, fogState ? FOG_RELAY_ON : FOG_RELAY_OFF);

  Serial.print("[RELAY] Fog ");
  Serial.println(fogState ? "ON (GPIO19 HIGH)" : "OFF (GPIO19 LOW)");
  return true;
}

void turnAllRelaysOff() {
  digitalWrite(RELAY_FAN, FAN_RELAY_OFF);
  digitalWrite(RELAY_FOG, FOG_RELAY_OFF);

  fanState = false;
  fogState = false;
  lastFanChangeMs = 0;
  lastFogChangeMs = 0;

  Serial.println("[RELAY] Startup safe state: FAN OFF, FOG OFF");
}

void testRelaySequence() {
  Serial.println("===== RELAY TEST START =====");

  setFan(true, true);
  publishDeviceState();
  delay(3000);
  setFan(false, true);
  publishDeviceState();
  delay(800);

  setFog(true, true);
  publishDeviceState();
  delay(3000);
  setFog(false, true);
  publishDeviceState();
  delay(800);

  Serial.println("===== RELAY TEST DONE =====");
}

// ===== Serial commands =====
void handleSerialLine(const String& rawLine) {
  String line = rawLine;
  line.trim();

  if (line.length() == 0) return;

  if (line.startsWith("WIFI:")) {
    String payload = line.substring(5);

    if (payload.equalsIgnoreCase("RESET")) {
      resetWiFiCredentials();
      connectWiFi(true, true);
      connectMQTT();
      return;
    }

    if (payload.equalsIgnoreCase("PORTAL")) {
      if (mqtt.connected()) {
        mqtt.disconnect();
        delay(200);
      }
      WiFi.disconnect(true, true);
      connectWiFi(true, true);
      connectMQTT();
      return;
    }

    int comma = payload.indexOf(',');
    if (comma <= 0) {
      Serial.println("[WiFi] Sai cu phap. Dung: WIFI:ten_wifi,mat_khau");
      return;
    }

    String newSsid = payload.substring(0, comma);
    String newPass = payload.substring(comma + 1);
    newSsid.trim();
    newPass.trim();

    if (newSsid.length() == 0) {
      Serial.println("[WiFi] SSID khong duoc rong");
      return;
    }

    applyWiFiCredentials(newSsid, newPass, "Serial");
    return;
  }

  if (line.equalsIgnoreCase("WIFI?")) {
    Serial.print("[WiFi] Current SSID: ");
    Serial.println(hasStoredWiFiCredentials() ? wifiSsid : String("(none)"));
    Serial.println("[WiFi] Change: WIFI:ten_wifi,mat_khau");
    Serial.println("[WiFi] Reset: WIFI:RESET");
    Serial.println("[WiFi] Open portal: WIFI:PORTAL");
    return;
  }

  bool changedAny = false;
  for (int i = 0; i < line.length(); i++) {
    char c = line[i];
    if (c == '1') changedAny = setFan(true, true) || changedAny;
    else if (c == '2') changedAny = setFan(false, true) || changedAny;
    else if (c == '3') changedAny = setFog(true, true) || changedAny;
    else if (c == '4') changedAny = setFog(false, true) || changedAny;
    else if (c == '5' || c == '6') Serial.println("[CMD] Lamp ignored");
  }
  if (changedAny) {
    autoMode = false;
    publishDeviceState();
  }
}

void handleSerialInput() {
  if (!Serial.available()) return;
  String line = Serial.readStringUntil('\n');
  handleSerialLine(line);
}

// ===== WiFi =====
bool connectWiFi(bool force, bool allowPortalFallback) {
  if (!force && WiFi.status() == WL_CONNECTED) return true;

  Serial.println();
  Serial.println("===== WIFI CONNECT =====");

  if (force) {
    Serial.println("[WiFi] Force reconnect requested");
    Serial.print("[WiFi] Disconnecting old WiFi, status=");
    Serial.println(WiFi.status());
    WiFi.disconnect(true, true);
    delay(800);
    Serial.print("[WiFi] Disconnect done, status=");
    Serial.println(WiFi.status());
  }

  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  WiFi.setAutoReconnect(true);

  if (hasStoredWiFiCredentials()) {
    Serial.print("[WiFi] SSID: ");
    Serial.println(wifiSsid);
    Serial.println("[WiFi] Starting connection with WiFi.begin()");
    WiFi.begin(wifiSsid.c_str(), wifiPassword.c_str());

    unsigned long startMs = millis();
    while (WiFi.status() != WL_CONNECTED && millis() - startMs < 15000) {
      Serial.print(".");
      delay(500);
    }
    Serial.println();

    if (WiFi.status() == WL_CONNECTED) {
      Serial.println("[WiFi] Connected");
      Serial.print("[WiFi] IP: ");
      Serial.println(WiFi.localIP());
      timeSynced = false;
      syncClock();
      return true;
    }

    Serial.println("[WiFi] Failed with saved credentials.");
  } else {
    Serial.println("[WiFi] No saved credentials to connect");
  }

  if (allowPortalFallback) {
    return startWiFiConfigPortal();
  }

  Serial.println("[WiFi] Failed. Will retry automatically.");
  return false;
}

bool applyWiFiCredentials(String newSsid, String newPassword, const char* source) {
  newSsid.trim();
  newPassword.trim();

  Serial.println();
  Serial.println("===== WIFI CHANGE REQUEST =====");
  Serial.print("[WiFi] Source: ");
  Serial.println(source);
  Serial.print("[WiFi] Old SSID: ");
  Serial.println(wifiSsid);
  Serial.print("[WiFi] New SSID: ");
  Serial.println(newSsid);

  if (newSsid.length() == 0) {
    Serial.println("[WiFi] Change ignored: empty SSID");
    return false;
  }

  bool saved = saveWiFiCredentials(newSsid, newPassword);
  Serial.print("[WiFi] Preferences save: ");
  Serial.println(saved ? "OK" : "FAILED");
  if (!saved) {
    Serial.println("[WiFi] Change stopped because Preferences save failed");
    return false;
  }

  wifiSsid = newSsid;
  wifiPassword = newPassword;

  if (mqtt.connected()) {
    Serial.println("[MQTT] Disconnecting before WiFi change");
    mqtt.disconnect();
    delay(200);
  }

  bool connected = connectWiFi(true, false);

  if (connected && WiFi.status() == WL_CONNECTED) {
    Serial.println("[WiFi] New WiFi connected, reconnecting MQTT");
    connectMQTT();
    return true;
  }

  Serial.println("[WiFi] New WiFi connection failed. Saved credentials remain in memory.");
  Serial.println("[WiFi] You can open the setup portal with Serial command WIFI:PORTAL");
  return false;
}

// ===== MQTT =====
void connectMQTT() {
  if (mqtt.connected()) return;

  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("[MQTT] Skip: WiFi not connected");
    return;
  }

  Serial.println();
  Serial.println("===== MQTT CONNECT =====");
  Serial.print("[MQTT] Broker: ");
  Serial.print(MQTT_HOST);
  Serial.print(":");
  Serial.println(MQTT_PORT);

  String mqttCommandTopic = getDeviceTopic("commands");
  String mqttConfigTopic = getDeviceTopic("config");
  String clientId = String("ptdl-") + SENSOR_ID + "-" + String((uint32_t)ESP.getEfuseMac(), HEX);
  bool ok = mqtt.connect(clientId.c_str(), MQTT_USER, MQTT_PASSWORD);

  if (ok) {
    Serial.println("[MQTT] Connected");

    mqtt.subscribe(mqttCommandTopic.c_str());
    mqtt.subscribe(mqttConfigTopic.c_str());

    Serial.print("[MQTT] Subscribed: ");
    Serial.println(mqttCommandTopic);
    Serial.print("[MQTT] Subscribed: ");
    Serial.println(mqttConfigTopic);

    mqtt.publish(MQTT_GLOBAL_STATUS_TOPIC, "ESP32 connected", false);
    mqtt.publish(getDeviceTopic("logs").c_str(), "ESP32 firmware connected", false);
    publishDeviceState();
  } else {
    Serial.print("[MQTT] Failed, rc=");
    Serial.println(mqtt.state());
    Serial.println("[MQTT] Will retry automatically.");
  }
}

void publishDeviceState() {
  if (!mqtt.connected()) return;

  StaticJsonDocument<512> doc;
  doc["sensor_id"] = SENSOR_ID;
  doc["location"] = LOCATION;
  doc["timestamp"] = getIsoTimestamp();

  JsonObject state = doc.createNestedObject("state");
  state["fan"] = fanState;
  state["fog"] = fogState;
  state["auto"] = autoMode;

  JsonObject wifi = doc.createNestedObject("wifi");
  wifi["connected"] = WiFi.status() == WL_CONNECTED;
  wifi["ssid"] = WiFi.status() == WL_CONNECTED ? WiFi.SSID() : "";
  wifi["configured_ssid"] = wifiSsid;
  wifi["ip"] = WiFi.status() == WL_CONNECTED ? WiFi.localIP().toString() : "";
  wifi["rssi"] = WiFi.status() == WL_CONNECTED ? WiFi.RSSI() : 0;

  char payload[512];
  serializeJson(doc, payload, sizeof(payload));

  String topic = getDeviceTopic("state");
  bool ok = mqtt.publish(topic.c_str(), payload, true);

  Serial.print("[MQTT] Publish state ");
  Serial.println(ok ? "OK" : "FAILED");
  Serial.println(payload);
}

void publishSensorReading(float temperature, float humidity, const String& timestamp) {
  if (!mqtt.connected()) return;

  StaticJsonDocument<512> doc;
  doc["timestamp"] = timestamp;
  doc["sensor_id"] = SENSOR_ID;
  doc["source"] = SENSOR_ID;
  doc["location"] = LOCATION;
  doc["location_province"] = LOCATION_PROVINCE;
  doc["temperature"] = temperature;
  doc["humidity"] = humidity;
  doc["temperature_unit"] = "C";
  doc["humidity_unit"] = "%";
  doc["source_type"] = SOURCE_TYPE;
  doc["provider"] = PROVIDER;
  doc["environment_type"] = ENVIRONMENT_TYPE;
  doc["saved"] = true;

  char payload[512];
  serializeJson(doc, payload, sizeof(payload));

  String topic = getSensorReadingTopic();
  bool ok = mqtt.publish(topic.c_str(), payload, false);

  Serial.print("[MQTT] Published sensor reading ");
  Serial.println(ok ? "OK" : "FAILED");
  Serial.println(payload);
}

// ===== Command helpers =====
bool parseBoolFlexible(JsonVariantConst v, bool defaultValue = false) {
  if (v.is<bool>()) return v.as<bool>();
  if (v.is<int>()) return v.as<int>() != 0;

  if (v.is<const char*>()) {
    String s = v.as<const char*>();
    s.trim();
    s.toLowerCase();
    if (s == "1" || s == "3" || s == "true" || s == "on" || s == "yes") return true;
    if (s == "2" || s == "4" || s == "false" || s == "off" || s == "no") return false;
  }

  return defaultValue;
}

bool applySerialCommandChars(const String& serialCommands) {
  bool changedAny = false;
  bool hasRelayCommand = false;

  for (int i = 0; i < serialCommands.length(); i++) {
    char c = serialCommands[i];
    if (c >= '1' && c <= '4') {
      hasRelayCommand = true;
      break;
    }
  }

  if (hasRelayCommand && autoMode) {
    autoMode = false;
    changedAny = true;
    Serial.println("[CMD] Manual serial command received. Auto mode: OFF");
  }

  for (int i = 0; i < serialCommands.length(); i++) {
    char c = serialCommands[i];
    if (c == '1') changedAny = setFan(true, true) || changedAny;
    else if (c == '2') changedAny = setFan(false, true) || changedAny;
    else if (c == '3') changedAny = setFog(true, true) || changedAny;
    else if (c == '4') changedAny = setFog(false, true) || changedAny;
    else if (c == '5' || c == '6') Serial.println("[CMD] Lamp ignored");
  }

  return changedAny;
}

void handleCommandPayload(const String& payload) {
  StaticJsonDocument<1024> doc;
  DeserializationError err = deserializeJson(doc, payload);

  if (err) {
    Serial.print("[MQTT] JSON parse failed: ");
    Serial.println(err.c_str());

    if (applySerialCommandChars(payload)) {
      publishDeviceState();
    }
    return;
  }

  JsonObjectConst root = doc.as<JsonObjectConst>();

  JsonObjectConst thresholdObj = root["threshold_config"].as<JsonObjectConst>();
  if (!thresholdObj.isNull()) {
    String metricType = thresholdObj["metric_type"] | "";
    metricType.trim();
    saveThresholdConfig(metricType, thresholdObj);
    return;
  }

  const char* command = root["command"] | "";
  bool scanWifi = root["scan_wifi"] | false;
  if (scanWifi || String(command).equalsIgnoreCase("scan_wifi")) {
    publishWifiList();
    return;
  }

  if (root["wifi"].is<JsonObjectConst>()) {
    JsonObjectConst wifiObj = root["wifi"].as<JsonObjectConst>();
    String newSsid = wifiObj["ssid"] | "";
    String newPass = wifiObj["password"] | "";
    newSsid.trim();
    newPass.trim();

    if (newSsid.length() == 0) {
      Serial.println("[WiFi] MQTT update ignored: empty SSID");
      return;
    }

    applyWiFiCredentials(newSsid, newPass, "MQTT/API");
    return;
  }

  if (!root["state"].is<JsonObjectConst>() && root.containsKey("serial")) {
    if (applySerialCommandChars(root["serial"].as<String>())) {
      publishDeviceState();
    }
    return;
  }

  JsonObjectConst stateObj;
  if (root["state"].is<JsonObjectConst>()) {
    stateObj = root["state"].as<JsonObjectConst>();
  } else {
    stateObj = root;
  }

  bool changedAny = false;
  bool explicitAuto = stateObj.containsKey("auto");
  bool hasRelayCommand = stateObj.containsKey("fan") || stateObj.containsKey("fog");
  bool applyRelayCommands = hasRelayCommand;

  if (explicitAuto) {
    bool value = parseBoolFlexible(stateObj["auto"], autoMode);
    if (autoMode != value) {
      autoMode = value;
      changedAny = true;
      Serial.print("[CMD] Auto mode: ");
      Serial.println(autoMode ? "ON" : "OFF");
    }

    if (value) {
      applyRelayCommands = false;
    }
  }

  if (applyRelayCommands && autoMode) {
    autoMode = false;
    changedAny = true;
    Serial.println("[CMD] Manual fan/fog command received. Auto mode: OFF");
  }

  if (applyRelayCommands && stateObj.containsKey("fan")) {
    changedAny = setFan(parseBoolFlexible(stateObj["fan"], fanState), true) || changedAny;
  }

  if (applyRelayCommands && stateObj.containsKey("fog")) {
    changedAny = setFog(parseBoolFlexible(stateObj["fog"], fogState), true) || changedAny;
  }

  if (stateObj.containsKey("lamp")) {
    Serial.println("[CMD] Lamp ignored");
  }

  if (!root["state"].is<JsonObjectConst>() && root["commands"].is<JsonObjectConst>()) {
    JsonObjectConst commands = root["commands"].as<JsonObjectConst>();
    if (autoMode) {
      autoMode = false;
      changedAny = true;
      Serial.println("[CMD] Manual commands object received. Auto mode: OFF");
    }
    if (commands.containsKey("fan")) {
      changedAny = setFan(parseBoolFlexible(commands["fan"], fanState), true) || changedAny;
    }
    if (commands.containsKey("fog")) {
      changedAny = setFog(parseBoolFlexible(commands["fog"], fogState), true) || changedAny;
    }
  }

  if (changedAny) {
    publishDeviceState();
  } else {
    Serial.println("[CMD] No state change");
  }
}

void publishWifiList() {
  if (!mqtt.connected()) {
    Serial.println("[WiFi Scan] MQTT not connected, cannot publish WiFi list");
    return;
  }

  Serial.println("[WiFi Scan] Scanning nearby networks...");
  int networkCount = WiFi.scanNetworks(false, true);

  DynamicJsonDocument doc(4096);
  doc["device_id"] = SENSOR_ID;
  doc["timestamp"] = getIsoTimestamp();

  JsonArray networks = doc.createNestedArray("networks");
  int maxNetworks = min(networkCount, 15);

  for (int i = 0; i < maxNetworks; i++) {
    String ssid = WiFi.SSID(i);
    ssid.trim();
    if (ssid.length() == 0) {
      continue;
    }

    JsonObject item = networks.createNestedObject();
    item["ssid"] = ssid;
    item["rssi"] = WiFi.RSSI(i);
    item["channel"] = WiFi.channel(i);
    item["encryption"] = WiFi.encryptionType(i) != WIFI_AUTH_OPEN;
  }

  String body;
  serializeJson(doc, body);
  String topic = getDeviceTopic("wifi-list");
  bool ok = mqtt.publish(topic.c_str(), body.c_str(), false);

  Serial.print("[WiFi Scan] Found ");
  Serial.print(networkCount);
  Serial.print(" networks, published ");
  Serial.print(networks.size());
  Serial.print(" to ");
  Serial.print(topic);
  Serial.print(" (ok=");
  Serial.print(ok ? "true" : "false");
  Serial.println(")");

  WiFi.scanDelete();
}

void mqttCallback(char* topic, byte* payload, unsigned int length) {
  String body;
  body.reserve(length + 1);

  for (unsigned int i = 0; i < length; i++) {
    body += (char)payload[i];
  }

  Serial.println();
  Serial.println("[MQTT] Message arrived");
  Serial.print("[MQTT] Topic: ");
  Serial.println(topic);
  Serial.print("[MQTT] Payload: ");
  Serial.println(body);

  handleCommandPayload(body);
}

// ===== Auto mode =====
void handleAutoMode(float temperature, float humidity) {
  if (!autoMode) return;

  unsigned long now = millis();
  if (now - lastAutoControlMs < AUTO_CONTROL_INTERVAL_MS) return;
  lastAutoControlMs = now;

  bool changedAny = false;
  float fanOnThreshold = FAN_ON_TEMP;
  float fanOffThreshold = FAN_OFF_TEMP;
  float fogOnThreshold = FOG_ON_HUMIDITY;
  float fogOffThreshold = FOG_OFF_HUMIDITY;

  if (temperatureThresholdConfig.alertEnabled) {
    if (temperatureThresholdConfig.hasMax) {
      fanOnThreshold = temperatureThresholdConfig.maxValue;
    }
    if (temperatureThresholdConfig.hasMin) {
      fanOffThreshold = temperatureThresholdConfig.minValue;
    } else if (temperatureThresholdConfig.hasMax) {
      fanOffThreshold = temperatureThresholdConfig.maxValue - 2.0;
    }
  }

  if (humidityThresholdConfig.alertEnabled) {
    if (humidityThresholdConfig.hasMin) {
      fogOnThreshold = humidityThresholdConfig.minValue;
    }
    if (humidityThresholdConfig.hasMax) {
      fogOffThreshold = humidityThresholdConfig.maxValue;
    } else if (humidityThresholdConfig.hasMin) {
      fogOffThreshold = humidityThresholdConfig.minValue + 10.0;
    }
  }

  if (fanOffThreshold > fanOnThreshold) {
    fanOffThreshold = fanOnThreshold;
  }
  if (fogOffThreshold < fogOnThreshold) {
    fogOffThreshold = fogOnThreshold;
  }

  if (temperature >= fanOnThreshold && !fanState) {
    Serial.print("[AUTO] Temp >= ");
    Serial.print(fanOnThreshold, 1);
    Serial.println(" -> Fan ON");
    changedAny = setFan(true) || changedAny;
  } else if (temperature <= fanOffThreshold && fanState) {
    Serial.print("[AUTO] Temp <= ");
    Serial.print(fanOffThreshold, 1);
    Serial.println(" -> Fan OFF");
    changedAny = setFan(false) || changedAny;
  }

  if (humidity <= fogOnThreshold && !fogState) {
    Serial.print("[AUTO] Hum <= ");
    Serial.print(fogOnThreshold, 1);
    Serial.println(" -> Fog ON");
    changedAny = setFog(true) || changedAny;
  } else if (humidity >= fogOffThreshold && fogState) {
    Serial.print("[AUTO] Hum >= ");
    Serial.print(fogOffThreshold, 1);
    Serial.println(" -> Fog OFF");
    changedAny = setFog(false) || changedAny;
  }

  if (changedAny) {
    publishDeviceState();
  }
}

// ===== Setup =====
void setup() {
  Serial.begin(115200);
  delay(1000);

  Serial.println();
  Serial.println("======================================");
  Serial.println("ESP32 DHT11 MQTT RELAY FAN/FOG");
  Serial.println("======================================");

  pinMode(RELAY_FAN, OUTPUT);
  pinMode(RELAY_FOG, OUTPUT);
  turnAllRelaysOff();

  if (RUN_RELAY_TEST_ON_BOOT) {
    testRelaySequence();
    turnAllRelaysOff();
  }

  dht.begin();
  Serial.print("[DHT11] DATA GPIO: ");
  Serial.println(DHT_PIN);

  loadWiFiCredentials();
  refreshThresholdConfigs();

  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setCallback(mqttCallback);
  mqtt.setKeepAlive(60);
  mqtt.setSocketTimeout(10);
  mqtt.setBufferSize(4096);

  connectWiFi(false, true);
  connectMQTT();
}

// ===== Loop =====
void loop() {
  unsigned long now = millis();

  handleSerialInput();

  if (WiFi.status() != WL_CONNECTED && now - lastWifiRetryMs >= WIFI_RETRY_INTERVAL_MS) {
    lastWifiRetryMs = now;
    Serial.println("[WiFi] Disconnected. Reconnecting...");
    connectWiFi();
  }

  if (!mqtt.connected() && now - lastMqttRetryMs >= MQTT_RETRY_INTERVAL_MS) {
    lastMqttRetryMs = now;
    connectMQTT();
  }

  if (mqtt.connected()) {
    mqtt.loop();
  }

  if (now - lastSendMs < SEND_INTERVAL_MS) return;
  lastSendMs = now;

  float humidity = dht.readHumidity();
  float temperature = dht.readTemperature();

  if (isnan(humidity) || isnan(temperature)) {
    Serial.println("[DHT11] Read failed. Check VCC, GND, DATA GPIO4.");
    return;
  }

  Serial.print("[DHT11] Temp=");
  Serial.print(temperature, 1);
  Serial.print(" C, Hum=");
  Serial.print(humidity, 1);
  Serial.println(" %");

  handleAutoMode(temperature, humidity);

  if (!timeSynced) {
    timeSynced = isTimeValid() || syncClock();
  }
  if (!timeSynced) {
    Serial.println("[MQTT] Sensor publish skipped: waiting for valid NTP time");
    return;
  }

  String timestamp = getIsoTimestamp();
  publishSensorReading(temperature, humidity, timestamp);
}
