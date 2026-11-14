/*
 * CSI Radar for classic ESP32 / ESP32-WROOM DevKit.
 * Arduino IDE: board "ESP32 Dev Module", Espressif core 3.3.x.
 * Output: CSI,timestamp_us,rssi,52,amp0,...,amp51 (amplitude x 100).
 * Streams over USB Serial and TCP port 9000 (mDNS: csi-radar.local) for the web dashboard.
 * Compatible with the supplied Python dashboard; NOT a fall detector.
 * Set WIFI_PASSWORD below before upload. Do not publish your password.
 */
#include <Arduino.h>
#include <WiFi.h>
#include <ESPmDNS.h>
#include <math.h>
#include <inttypes.h>
#include "esp_wifi.h"
#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"
#include "ping/ping_sock.h"
#include "lwip/ip_addr.h"

#if !defined(CONFIG_IDF_TARGET_ESP32)
#error "Select ESP32 Dev Module for the classic ESP32. This sketch is not for UNO R4 or ESP32-S3."
#endif

// ---- EDIT THESE SETTINGS ----
const char *WIFI_SSID = "Korn";
const char *WIFI_PASSWORD = "YOUR_WIFI_PASSWORD";
const bool ENABLE_TCP_SERVER = true; // Web dashboard connects to ESP32 IP:9000.
const uint16_t TCP_PORT = 9000;
const char *MDNS_HOSTNAME = "csi-radar"; // Reachable as csi-radar.local on LAN.
const bool ENABLE_TRAFFIC_PING = true; // Local AP ping to encourage incoming CSI.
// ----------------------------

constexpr size_t AMP_COUNT = 52;
constexpr int64_t OUTPUT_INTERVAL_US = 50000; // At most 20 frames/second.
struct CsiFrame {
  int64_t timestamp;
  int8_t rssi;
  int8_t samples[AMP_COUNT * 2];
};
QueueHandle_t frameQueue = nullptr;
portMUX_TYPE captureMux = portMUX_INITIALIZER_UNLOCKED;
uint8_t accessPointMac[6] = {};
bool captureReady = false;
uint32_t queueDrops = 0;
WiFiServer tcpServer(TCP_PORT, 2);
WiFiClient tcpClient;
bool tcpServerReady = false;
bool mdnsReady = false;
esp_ping_handle_t pingHandle = nullptr;
bool connectedBefore = false;
uint32_t lastReconnect = 0; // Status log timer; does not force reconnection.
uint32_t lastRescanMs = 0;
uint32_t lastTcpHeartbeatMs = 0;
bool disconnectPending = false;
uint8_t disconnectReason = 0;
uint32_t disconnectCount = 0;

// Event callbacks run on another task. Only update protected diagnostic state.
void onWiFiEvent(WiFiEvent_t event, WiFiEventInfo_t info) {
  if (event == ARDUINO_EVENT_WIFI_STA_DISCONNECTED) {
    portENTER_CRITICAL(&captureMux);
    disconnectReason = info.wifi_sta_disconnected.reason;
    disconnectPending = true;
    disconnectCount++;
    captureReady = false;
    portEXIT_CRITICAL(&captureMux);
  }
}
uint32_t lastReport = 0;
uint32_t emitted = 0;
int64_t lastEmitUs = 0;

bool checked(esp_err_t result, const char *operation) {
  if (result == ESP_OK) return true;
  Serial.printf("# ERROR %s: %s\n", operation, esp_err_to_name(result));
  return false;
}

// Called by the Wi-Fi TASK, not an interrupt. Copy only; never print or send UDP.
void onCsi(void *context, wifi_csi_info_t *info) {
  if (!info || !info->buf || info->len < AMP_COUNT * 2) return;
  portENTER_CRITICAL(&captureMux);
  bool accept = captureReady && memcmp(info->mac, accessPointMac, 6) == 0;
  portEXIT_CRITICAL(&captureMux);
  if (!accept) return;
  CsiFrame frame;
  frame.timestamp = esp_timer_get_time();
  frame.rssi = info->rx_ctrl.rssi;
  memcpy(frame.samples, info->buf, sizeof(frame.samples));
  // Keep indices consistent. First four CSI bytes can be hardware-invalid.
  // Mask these two pairs on ALL frames; recalibrate/retrain after migration.
  memset(frame.samples, 0, 4);
  if (xQueueSend(frameQueue, &frame, 0) != pdTRUE) {
    portENTER_CRITICAL(&captureMux);
    queueDrops++;
    portEXIT_CRITICAL(&captureMux);
  }
}

void stopCapture() {
  portENTER_CRITICAL(&captureMux);
  captureReady = false;
  portEXIT_CRITICAL(&captureMux);
  esp_wifi_set_csi(false);
  esp_wifi_set_promiscuous(false);
  if (pingHandle) {
    esp_ping_stop(pingHandle);
    esp_ping_delete_session(pingHandle);
    pingHandle = nullptr;
  }
  if (tcpClient) tcpClient.stop();
  if (tcpServerReady) {
    tcpServer.stop();
    tcpServerReady = false;
  }
  if (mdnsReady) {
    MDNS.end();
    mdnsReady = false;
  }
  xQueueReset(frameQueue);
}

void startTraffic() {
  if (!ENABLE_TRAFFIC_PING) return;
  IPAddress gateway = WiFi.gatewayIP();
  esp_ping_config_t config = ESP_PING_DEFAULT_CONFIG();
  IP_ADDR4(&config.target_addr, gateway[0], gateway[1], gateway[2], gateway[3]);
  config.count = 0; // Continuous, until disconnect.
  config.interval_ms = 100; // Lighter traffic while diagnosing Wi-Fi stability.
  config.timeout_ms = 500;
  esp_ping_callbacks_t callbacks = {};
  if (checked(esp_ping_new_session(&config, &callbacks, &pingHandle), "create AP ping")) {
    checked(esp_ping_start(pingHandle), "start AP ping");
  }
  // Some APs suppress ICMP replies. Actual CSI rate must be measured.
}

bool startCapture() {
  wifi_ap_record_t ap = {};
  if (!checked(esp_wifi_sta_get_ap_info(&ap), "read AP")) return false;
  portENTER_CRITICAL(&captureMux);
  memcpy(accessPointMac, ap.bssid, 6);
  portEXIT_CRITICAL(&captureMux);

  wifi_csi_config_t config = {};
  config.lltf_en = true;
  config.htltf_en = false; // Fixed LLTF segment, avoid mixing LTF formats.
  config.stbc_htltf2_en = false;
  config.ltf_merge_en = false;
  config.channel_filter_en = false;
  config.manu_scale = false;
  config.shift = 0;
  wifi_promiscuous_filter_t filter = {};
  filter.filter_mask = WIFI_PROMIS_FILTER_MASK_DATA | WIFI_PROMIS_FILTER_MASK_MGMT;
  if (!checked(esp_wifi_set_promiscuous_filter(&filter), "frame filter") ||
      !checked(esp_wifi_set_promiscuous(true), "promiscuous mode") ||
      !checked(esp_wifi_set_csi_config(&config), "CSI configuration") ||
      !checked(esp_wifi_set_csi_rx_cb(onCsi, nullptr), "CSI callback") ||
      !checked(esp_wifi_set_csi(true), "enable CSI")) {
    stopCapture();
    return false;
  }
  xQueueReset(frameQueue);
  lastEmitUs = 0;
  portENTER_CRITICAL(&captureMux);
  captureReady = true;
  portEXIT_CRITICAL(&captureMux);
  if (ENABLE_TCP_SERVER) {
    tcpServer.begin(TCP_PORT);
    tcpServer.setNoDelay(true);
    tcpServerReady = (bool)tcpServer;
    mdnsReady = MDNS.begin(MDNS_HOSTNAME);
    if (mdnsReady) MDNS.addService("csi-radar", "tcp", TCP_PORT);
    if (!tcpServerReady) Serial.println("# WARNING: TCP server failed to start; Serial remains available");
  }
  String localIp = WiFi.localIP().toString();
  Serial.printf("# WiFi connected. ESP32 IP=%s\n", localIp.c_str());
  if (tcpServerReady) {
    Serial.printf("# Web TCP stream ready on %s:%u", localIp.c_str(), TCP_PORT);
    if (mdnsReady) Serial.printf(" (or %s.local:%u)", MDNS_HOSTNAME, TCP_PORT);
    Serial.println();
  }
  Serial.println("# CSI enabled. Enter ESP32 IP in the web dashboard and click Connect.");
  startTraffic();
  return true;
}

void connectToConfiguredWiFi() {
  String targetSsid = WIFI_SSID;
  int found = WiFi.scanNetworks(false, false, false, 150);
  if (found > 0) {
    bool exactFound = false;
    String ciMatch;
    for (int i = 0; i < found; i++) {
      String ssid = WiFi.SSID(i);
      if (ssid == WIFI_SSID) {
        exactFound = true;
        break;
      }
      if (ciMatch.isEmpty() && ssid.equalsIgnoreCase(WIFI_SSID)) {
        ciMatch = ssid;
      }
    }
    if (!exactFound && !ciMatch.isEmpty()) {
      targetSsid = ciMatch;
      Serial.printf("# Matched SSID '%s' for '%s'\n", targetSsid.c_str(), WIFI_SSID);
    }
  }
  WiFi.scanDelete();
  WiFi.begin(targetSsid.c_str(), WIFI_PASSWORD);
  Serial.printf("# Connecting to %s (2.4 GHz)...\n", targetSsid.c_str());
}

void serviceTcpClient() {
  if (!tcpServerReady) return;
  WiFiClient incoming = tcpServer.accept();
  if (incoming) {
    if (tcpClient) tcpClient.stop();
    tcpClient = incoming;
    tcpClient.setNoDelay(true);
    tcpClient.setTimeout(2000);
    lastTcpHeartbeatMs = millis();
    Serial.printf("# Dashboard connected from %s\n", tcpClient.remoteIP().toString().c_str());
  }
  if (tcpClient && !tcpClient.connected()) {
    tcpClient.stop();
  }
  while (tcpClient && tcpClient.available() > 0) {
    tcpClient.read();
  }
  if (tcpClient && tcpClient.connected() && millis() - lastTcpHeartbeatMs >= 1000) {
    lastTcpHeartbeatMs = millis();
    static const char HB[] = "# HB\n";
    if (tcpClient.write((const uint8_t *)HB, sizeof(HB) - 1) != sizeof(HB) - 1) {
      Serial.println("# Dashboard TCP heartbeat incomplete; closing client");
      tcpClient.stop();
    }
  }
}

void setup() {
  Serial.begin(115200);
  delay(800);
  Serial.println("# CSI Radar v2 / classic ESP32 / Arduino / Wi-Fi TCP + Serial");
  if (strcmp(WIFI_PASSWORD, "YOUR_WIFI_PASSWORD") == 0) {
    Serial.println("# Set WIFI_PASSWORD in this sketch, then upload again.");
    while (true) delay(1000);
  }
  frameQueue = xQueueCreate(32, sizeof(CsiFrame));
  if (!frameQueue) {
    Serial.println("# ERROR: cannot allocate CSI queue");
    while (true) delay(1000);
  }
  WiFi.onEvent(onWiFiEvent);
  WiFi.persistent(false);
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  WiFi.setAutoReconnect(true);
  connectToConfiguredWiFi();
}

void loop() {
  portENTER_CRITICAL(&captureMux);
  bool lost = disconnectPending;
  uint8_t reason = disconnectReason;
  uint32_t totalDisconnects = disconnectCount;
  disconnectPending = false;
  portEXIT_CRITICAL(&captureMux);
  if (lost) {
    Serial.printf("# WIFI_DISCONNECT reason=%u count=%lu uptime_ms=%lu\n",
                  reason, (unsigned long)totalDisconnects, (unsigned long)millis());
    // A fast disconnect/reconnect must also reset the CSI/AP binding.
    if (connectedBefore) {
      stopCapture();
      connectedBefore = false;
    }
  }
  bool connected = WiFi.status() == WL_CONNECTED;
  if (!connected) {
    if (connectedBefore) {
      stopCapture();
      connectedBefore = false;
      lastRescanMs = millis();
      Serial.println("# WiFi disconnected; waiting to reconnect");
    }
    if (millis() - lastReconnect >= 10000) {
      lastReconnect = millis();
      // Arduino owns reconnection; rescan periodically if hotspot SSID casing changed or appeared late.
      Serial.printf("# WiFi waiting; status=%d. Automatic reconnect enabled.\n", (int)WiFi.status());
      if (millis() - lastRescanMs >= 20000 && WiFi.status() != WL_IDLE_STATUS) {
        lastRescanMs = millis();
        connectToConfiguredWiFi();
      }
    }
    delay(20);
    return;
  }
  lastRescanMs = millis();
  if (!connectedBefore) {
    if (!startCapture()) { delay(2000); return; }
    connectedBefore = true;
  }

  serviceTcpClient();

  CsiFrame latest;
  bool available = false;
  // Take at most a bounded queue's worth of frames; keep the freshest one.
  for (int i = 0; i < 32 && xQueueReceive(frameQueue, &latest, 0) == pdTRUE; i++) available = true;
  if (available && latest.timestamp - lastEmitUs >= OUTPUT_INTERVAL_US) {
    lastEmitUs = latest.timestamp;
    char line[768];
    int used = snprintf(line, sizeof(line), "CSI,%" PRId64 ",%d,%u",
                        latest.timestamp, (int)latest.rssi, (unsigned)AMP_COUNT);
    bool valid = used > 0 && (size_t)used < sizeof(line);
    for (size_t i = 0; valid && i < AMP_COUNT; i++) {
      float imag = latest.samples[i * 2];
      float real = latest.samples[i * 2 + 1];
      int amp = (int)(sqrtf(real * real + imag * imag) * 100.0f);
      int written = snprintf(line + used, sizeof(line) - used, ",%d", amp);
      if (written < 0 || (size_t)written >= sizeof(line) - used) valid = false;
      else used += written;
    }
    if (valid && (size_t)used + 1 < sizeof(line)) {
      line[used++] = '\n';
      line[used] = '\0';
      Serial.write((const uint8_t *)line, used);
      if (tcpClient && tcpClient.connected()) {
        size_t sent = tcpClient.write((const uint8_t *)line, used);
        if (sent != (size_t)used) {
          Serial.println("# Dashboard TCP write incomplete; closing client");
          tcpClient.stop();
        } else {
          lastTcpHeartbeatMs = millis();
        }
      }
      emitted++;
    }
  }
  if (millis() - lastReport >= 5000) {
    lastReport = millis();
    portENTER_CRITICAL(&captureMux);
    uint32_t dropped = queueDrops;
    portEXIT_CRITICAL(&captureMux);
    Serial.printf("# Output frames=%lu queue_drops=%lu RSSI=%d IP=%s tcp=%d\n",
                  (unsigned long)emitted, (unsigned long)dropped, WiFi.RSSI(),
                  WiFi.localIP().toString().c_str(), (tcpClient && tcpClient.connected()) ? 1 : 0);
  }
  delay(2);
}
