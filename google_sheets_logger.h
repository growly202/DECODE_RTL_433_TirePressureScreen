#ifndef GOOGLE_SHEETS_LOGGER_H
#define GOOGLE_SHEETS_LOGGER_H

#include <Arduino.h>
#include <ArduinoJson.h>
#include <ArduinoLog.h>

#if defined(ESP32) && defined(ENABLE_GOOGLE_SHEETS)

#include <HTTPClient.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <esp_wifi.h>
#include <freertos/FreeRTOS.h>
#include <freertos/queue.h>
#include <freertos/semphr.h>
#include <freertos/task.h>

extern "C" uint8_t* g_wifi_nvs;

#ifndef WIFI_SSID
#  define WIFI_SSID "YOUR_WIFI_SSID"
#endif

#ifndef WIFI_PASSWORD
#  define WIFI_PASSWORD "YOUR_WIFI_PASSWORD"
#endif

#ifndef GOOGLE_SHEETS_WEBHOOK_URL
#  define GOOGLE_SHEETS_WEBHOOK_URL ""
#endif

#ifndef RF_MODULE_FREQUENCY
#  define RF_MODULE_FREQUENCY 433.92f
#endif

// Maximum number of pulse/gap pairs serialized into the raw_pulses column per signal
#ifndef SHEETS_MAX_RAW_PULSES
#  define SHEETS_MAX_RAW_PULSES 400
#endif

// Maximum number of signals held in RAM queue waiting for HTTPS upload
#ifndef SHEETS_QUEUE_SIZE
#  define SHEETS_QUEUE_SIZE 16
#endif

// Maximum number of signals grouped into a single HTTP POST batch
#ifndef SHEETS_MAX_BATCH_SIZE
#  define SHEETS_MAX_BATCH_SIZE 5
#endif

// Short window (ms) to coalesce back-to-back signals into one HTTP POST
#ifndef SHEETS_BATCH_WAIT_MS
#  define SHEETS_BATCH_WAIT_MS 250
#endif

// Set to 1 to follow Google Apps Script 302 redirects to script.googleusercontent.com.
// Leave 0 (default) to save ~1.5s and ~50% battery per upload: Google Apps Script
// executes doPost(e) and appends rows BEFORE returning HTTP 302.
#ifndef SHEETS_FOLLOW_REDIRECTS
#  define SHEETS_FOLLOW_REDIRECTS 0
#endif

// Wi-Fi TX power for ESP32-C3 Super Mini stability on battery & reduced RF noise
#ifndef SHEETS_WIFI_TX_POWER
#  define SHEETS_WIFI_TX_POWER WIFI_POWER_8_5dBm
#endif

// Set to 1 to also upload undecoded (RAW_ONLY) signals that pass noise filters.
// Leave 0 (default) to only push BOOT_READY and DECODED signals to Google Sheets.
#ifndef SHEETS_PUSH_RAW_ONLY
#  define SHEETS_PUSH_RAW_ONLY 0
#endif

// Minimum RSSI (dBm) required to accept an undecoded (RAW_ONLY) signal.
// Decoded signals (DECODED) are ALWAYS accepted regardless of RSSI.
#ifndef SHEETS_RAW_MIN_RSSI
#  define SHEETS_RAW_MIN_RSSI -65
#endif

// Minimum duration (us) for an undecoded (RAW_ONLY) signal to reject 1-ms FSK
// RSSI spikes while accepting 6-10 ms TPMS single-frame bursts.
// Decoded FSK/OOK signals (DECODED) are ALWAYS accepted regardless of duration.
#ifndef SHEETS_RAW_MIN_DURATION_US
#  define SHEETS_RAW_MIN_DURATION_US 5500UL
#endif

// Maximum duration (us) for an undecoded (RAW_ONLY) signal before treating it
// as continuous RF background noise (default 800,000 us = 800 ms).
#ifndef SHEETS_RAW_MAX_DURATION_US
#  define SHEETS_RAW_MAX_DURATION_US 800000UL
#endif

// Maximum average pulse+gap period (us) for a RAW_ONLY signal to filter out
// sparse random noise spikes (default 5,000 us).
#ifndef SHEETS_RAW_MAX_AVG_PERIOD_US
#  define SHEETS_RAW_MAX_AVG_PERIOD_US 5000UL
#endif

// Cooldown window (ms) after Wi-Fi HTTPS upload finishes during which undecoded
// RAW_ONLY captures triggered by Wi-Fi TX coupling into CC1101 are ignored.
#ifndef SHEETS_WIFI_TX_COOLDOWN_MS
#  define SHEETS_WIFI_TX_COOLDOWN_MS 600UL
#endif

#if defined(OOK_MODULATION) && !OOK_MODULATION
#  define SHEETS_MODULATION_STR "FSK"
#else
#  define SHEETS_MODULATION_STR "OOK"
#endif

struct SheetsSignalRecord {
  uint32_t seq;
  unsigned long timestampMs;
  float frequencyMhz;
  int rssi;
  unsigned long durationUs;
  unsigned int numPulses;
  bool pulsesTruncated;
  uint8_t decodedCount;
  char model[48];
  char deviceId[32];
  char protocol[96];
  char decodedJson[512];
  char rawPulses[]; // Flexible array member holding "+pulse -gap ..." string
};

namespace GoogleSheetsLogger {

static QueueHandle_t _uploadQueue = nullptr;
static TaskHandle_t _uploaderTaskHandle = nullptr;
static portMUX_TYPE _stagingMux = portMUX_INITIALIZER_UNLOCKED;

// Staging buffer populated by rtl_433_Callback (which runs immediately before
// rtl_433_RawCallback on the same rtl_433_DecoderTask thread).
static uint8_t _stagedDecodedCount = 0;
static char _stagedModel[48] = {0};
static char _stagedDeviceId[32] = {0};
static char _stagedProtocol[96] = {0};
static char _stagedDecodedJson[512] = {0};

static volatile uint32_t _signalSeq = 0;
static volatile uint32_t _uploadedCount = 0;
static volatile uint32_t _droppedQueueCount = 0;
static volatile uint32_t _httpErrorCount = 0;
static volatile uint32_t _filteredNoiseCount = 0;
static volatile bool _isUploading = false;
static volatile unsigned long _uploadEndMs = 0;
static unsigned long _lastWiFiBeginMs = 0;

inline bool isBusyUploading() {
  return _isUploading ||
         (_uploadEndMs > 0 && (millis() - _uploadEndMs) < SHEETS_WIFI_TX_COOLDOWN_MS);
}

inline const char* currentModulationStr() {
  return rtl_433_ESP::ookModulation ? "OOK" : "FSK";
}

inline bool isWebhookConfigured() {
  const char* url = GOOGLE_SHEETS_WEBHOOK_URL;
  return url != nullptr && strlen(url) > 12 &&
         strncmp(url, "https://", 8) == 0 &&
         strstr(url, "YOUR_DEPLOYMENT_ID") == nullptr;
}

inline void startWiFiConnection() {
  // Disable WPA3-SAE capability flag before starting STA so ESP-IDF v4.4.4 selects
  // WPA2-PSK on WPA2/WPA3 transition-mode routers instead of hitting the WPA3
  // Association Comeback timer underflow bug (4294926 ms lockup).
  g_wifi_feature_caps &= ~CONFIG_FEATURE_WPA3_SAE_BIT;

  WiFi.persistent(false);
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  WiFi.setAutoReconnect(true);
  WiFi.setMinSecurity(WIFI_AUTH_WPA2_PSK);
  WiFi.setTxPower(SHEETS_WIFI_TX_POWER);

  // Configure STA credentials without connecting yet so we can clear PMF capable
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD, 0, nullptr, false);

  // Clear 802.11w PMF capable/required flags in internal Wi-Fi config (offset 864/865)
  // to avoid AP SA Query / Comeback rejections (Status 30) after ESP32-C3 resets.
  if (g_wifi_nvs != nullptr) {
    g_wifi_nvs[864] = 0;
    g_wifi_nvs[865] = 0;
  }

  _lastWiFiBeginMs = millis();
  esp_wifi_connect();
}

inline void connectWiFiNonBlocking() {
  if (WiFi.status() == WL_CONNECTED) {
    return;
  }
  Log.notice(F("[Sheets] Connecting to Wi-Fi SSID: %s" CR), WIFI_SSID);
  startWiFiConnection();

  unsigned long startAttempt = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - startAttempt < 10000UL) {
    delay(250);
  }
  if (WiFi.status() == WL_CONNECTED) {
    Log.notice(F("[Sheets] Wi-Fi connected! IP: %s, RSSI: %d dBm" CR),
               WiFi.localIP().toString().c_str(), WiFi.RSSI());
  } else {
    Log.warning(F("[Sheets] Wi-Fi not connected yet; will retry in background task" CR));
  }
}

// Called from rtl_433_Callback when a signal is decoded into JSON
inline void onDecodedMessage(const char* jsonMessage, const JsonDocument& doc) {
  portENTER_CRITICAL(&_stagingMux);
  _stagedDecodedCount++;
  const char* msgType = doc["type"] | "";
  bool isTpms = (strcmp(msgType, "TPMS") == 0);
  // Keep the first decoded event's metadata, or overwrite if a TPMS decoder matches
  if (_stagedDecodedCount == 1 || isTpms) {
    strlcpy(_stagedDecodedJson, jsonMessage ? jsonMessage : "",
            sizeof(_stagedDecodedJson));
    const char* model = doc["model"] | "";
    strlcpy(_stagedModel, model, sizeof(_stagedModel));

    const char* protocol = doc["protocol"] | "";
    strlcpy(_stagedProtocol, protocol, sizeof(_stagedProtocol));

    if (!doc["id"].isNull()) {
      if (doc["id"].is<const char*>()) {
        snprintf(_stagedDeviceId, sizeof(_stagedDeviceId), "%s",
                 doc["id"].as<const char*>());
      } else {
        snprintf(_stagedDeviceId, sizeof(_stagedDeviceId), "%ld",
                 doc["id"].as<long>());
      }
    } else {
      _stagedDeviceId[0] = '\0';
    }

    if (!doc["channel"].isNull()) {
      size_t len = strlen(_stagedDeviceId);
      if (doc["channel"].is<const char*>()) {
        snprintf(_stagedDeviceId + len, sizeof(_stagedDeviceId) - len, "%sCh:%s",
                 len > 0 ? " / " : "", doc["channel"].as<const char*>());
      } else {
        snprintf(_stagedDeviceId + len, sizeof(_stagedDeviceId) - len, "%sCh:%ld",
                 len > 0 ? " / " : "", doc["channel"].as<long>());
      }
    }
  }
  portEXIT_CRITICAL(&_stagingMux);
}

// Called from rtl_433_RawCallback for EVERY captured signal (decoded or undecoded).
// Returns true if the signal is valid and queued for upload, or false if filtered out as noise.
inline bool onRawSignal(const int* pulse_us, const int* gap_us,
                        unsigned int num_pulses, unsigned long duration_us,
                        int rssi) {
  if (!_uploadQueue) {
    return false;
  }

  uint8_t decodedCount = 0;
  char model[48];
  char deviceId[32];
  char protocol[96];
  char decodedJson[512];

  portENTER_CRITICAL(&_stagingMux);
  decodedCount = _stagedDecodedCount;
  strlcpy(model, _stagedModel, sizeof(model));
  strlcpy(deviceId, _stagedDeviceId, sizeof(deviceId));
  strlcpy(protocol, _stagedProtocol, sizeof(protocol));
  strlcpy(decodedJson, _stagedDecodedJson, sizeof(decodedJson));

  _stagedDecodedCount = 0;
  _stagedModel[0] = '\0';
  _stagedDeviceId[0] = '\0';
  _stagedProtocol[0] = '\0';
  _stagedDecodedJson[0] = '\0';
  portEXIT_CRITICAL(&_stagingMux);

  unsigned long nowMs = millis();

  // Only push DECODED signals (decodedCount > 0) by default; skip RAW_ONLY unless enabled.
  if (decodedCount == 0) {
#if !SHEETS_PUSH_RAW_ONLY
    return false;
#else
    unsigned long durationMs = duration_us / 1000UL;
    unsigned long signalStartMs = (nowMs > durationMs) ? (nowMs - durationMs) : 0;
    unsigned long lastUploadEnd = _uploadEndMs;

    // 1. Reject RAW_ONLY noise captured while ESP32-C3 Wi-Fi TX was active or cooling down
    if (_isUploading ||
        (lastUploadEnd > 0 &&
         ((nowMs - lastUploadEnd) < SHEETS_WIFI_TX_COOLDOWN_MS ||
          signalStartMs <= (lastUploadEnd + 100UL)))) {
      _filteredNoiseCount++;
      return false;
    }

    // 2. Reject weak, too short (1ms FSK pretrigger spike), or overly long/sparse RAW_ONLY background noise
    unsigned long avgPeriodUs = (num_pulses > 0) ? (duration_us / num_pulses) : 999999UL;
    if (rssi < SHEETS_RAW_MIN_RSSI ||
        duration_us < SHEETS_RAW_MIN_DURATION_US ||
        duration_us > SHEETS_RAW_MAX_DURATION_US ||
        avgPeriodUs > SHEETS_RAW_MAX_AVG_PERIOD_US) {
      _filteredNoiseCount++;
      return false;
    }
    strlcpy(protocol, currentModulationStr(), sizeof(protocol));
#endif
  }

  unsigned int limit = num_pulses;
  bool truncated = false;
  if (limit > SHEETS_MAX_RAW_PULSES) {
    limit = SHEETS_MAX_RAW_PULSES;
    truncated = true;
  }

  // Estimate string buffer size for "+pulse -gap " pairs (~16 bytes per pair max)
  size_t rawBufSize = (size_t)limit * 16U + 16U;
  SheetsSignalRecord* rec = static_cast<SheetsSignalRecord*>(
       malloc(sizeof(SheetsSignalRecord) + rawBufSize));
  if (!rec) {
    _droppedQueueCount++;
    return true;
  }

  rec->seq = ++_signalSeq;
  rec->timestampMs = nowMs;
  rec->frequencyMhz = RF_MODULE_FREQUENCY;
  rec->rssi = rssi;
  rec->durationUs = duration_us;
  rec->numPulses = num_pulses;
  rec->pulsesTruncated = truncated;
  rec->decodedCount = decodedCount;
  strlcpy(rec->model, model, sizeof(rec->model));
  strlcpy(rec->deviceId, deviceId, sizeof(rec->deviceId));
  strlcpy(rec->protocol, protocol, sizeof(rec->protocol));
  strlcpy(rec->decodedJson, decodedJson, sizeof(rec->decodedJson));

  // Format raw pulses string: "+pulse0 -gap0 +pulse1 -gap1 ..."
  size_t pos = 0;
  rec->rawPulses[0] = '\0';
  for (unsigned int i = 0; i < limit; i++) {
    int written = snprintf(rec->rawPulses + pos, rawBufSize - pos,
                           "%s+%d -%d", (i == 0) ? "" : " ",
                           pulse_us[i], gap_us[i]);
    if (written <= 0 || (size_t)written >= rawBufSize - pos) {
      rec->pulsesTruncated = true;
      break;
    }
    pos += (size_t)written;
  }

  if (xQueueSend(_uploadQueue, &rec, 0) != pdTRUE) {
    free(rec);
    _droppedQueueCount++;
    Log.warning(F("[Sheets] Upload queue full! Dropped signal #%u (total dropped=%u)" CR),
                (unsigned int)_signalSeq, (unsigned int)_droppedQueueCount);
  } else {
    Log.notice(F("[Sheets] Queued signal #%u (%s, %u pulses, %d dBm) for upload" CR),
               (unsigned int)rec->seq,
               (rec->decodedCount > 0) ? "DECODED" : "RAW_ONLY",
               rec->numPulses, rec->rssi);
  }
  return true;
}

inline bool sendBatchToGoogleSheets(SheetsSignalRecord** batch, size_t count) {
  if (!isWebhookConfigured()) {
    Log.warning(F("[Sheets] GOOGLE_SHEETS_WEBHOOK_URL is not configured; skipping HTTP POST" CR));
    return false;
  }

  _isUploading = true;

  if (WiFi.status() != WL_CONNECTED) {
    if (millis() - _lastWiFiBeginMs >= 15000UL) {
      Log.notice(F("[Sheets] Reconnecting Wi-Fi to %s..." CR), WIFI_SSID);
      esp_wifi_disconnect();
      startWiFiConnection();
    }
    unsigned long waitStart = millis();
    while (WiFi.status() != WL_CONNECTED && millis() - waitStart < 8000UL) {
      vTaskDelay(pdMS_TO_TICKS(200));
    }
    if (WiFi.status() != WL_CONNECTED) {
      _uploadEndMs = millis();
      _isUploading = false;
      Log.warning(F("[Sheets] Wi-Fi disconnected (status=%d); cannot push batch (%u records)" CR),
                  (int)WiFi.status(), (unsigned int)count);
      return false;
    }
  }

  JsonDocument doc;
  doc["device"] = "ESP32-C3_CC1101";
  doc["free_heap"] = ESP.getFreeHeap();
  doc["wifi_rssi"] = WiFi.RSSI();
  doc["dropped_queue"] = (uint32_t)_droppedQueueCount;

  JsonArray rows = doc["rows"].to<JsonArray>();
  for (size_t i = 0; i < count; i++) {
    SheetsSignalRecord* r = batch[i];
    JsonObject obj = rows.add<JsonObject>();
    obj["seq"] = r->seq;
    obj["time_ms"] = r->timestampMs;
    obj["freq_mhz"] = serialized(String(r->frequencyMhz, 2));
    obj["rssi"] = r->rssi;
    obj["duration_us"] = r->durationUs;
    obj["num_pulses"] = r->numPulses;
    if (r->decodedCount == 0xFF) {
      obj["status"] = "BOOT_READY";
      obj["decoded_events"] = 0;
    } else {
      obj["status"] = (r->decodedCount > 0) ? "DECODED" : "RAW_ONLY";
      obj["decoded_events"] = r->decodedCount;
    }
    obj["model"] = r->model;
    obj["device_id"] = r->deviceId;
    obj["protocol"] = r->protocol;
    obj["decoded_json"] = r->decodedJson;
    obj["raw_pulses"] = r->rawPulses;
    obj["truncated"] = r->pulsesTruncated;
  }

  String payload;
  serializeJson(doc, payload);

  WiFiClientSecure client;
  client.setInsecure(); // Skip CA chain verification to save RAM & flash on ESP32-C3
  client.setTimeout(10);

  HTTPClient http;
  http.setTimeout(10000);
#if SHEETS_FOLLOW_REDIRECTS
  http.setFollowRedirects(HTTPC_FORCE_FOLLOW_REDIRECTS);
#else
  http.setFollowRedirects(HTTPC_DISABLE_FOLLOW_REDIRECTS);
#endif

  Log.notice(F("[Sheets] Uploading %u record(s) (free_heap=%u)..." CR),
             (unsigned int)count, (unsigned int)ESP.getFreeHeap());

  if (!http.begin(client, GOOGLE_SHEETS_WEBHOOK_URL)) {
    _uploadEndMs = millis();
    _isUploading = false;
    Log.error(F("[Sheets] HTTP begin failed" CR));
    _httpErrorCount++;
    return false;
  }

  http.addHeader("Content-Type", "application/json");
  unsigned long postStart = millis();
  int httpCode = http.POST(payload);
  unsigned long elapsed = millis() - postStart;
  http.end();
  _uploadEndMs = millis();
  _isUploading = false;

  // Google Apps Script executes doPost(e) and writes to the sheet BEFORE
  // returning HTTP 302 (redirect to script.googleusercontent.com) or HTTP 200.
  if (httpCode == HTTP_CODE_OK || httpCode == HTTP_CODE_FOUND ||
      httpCode == HTTP_CODE_MOVED_PERMANENTLY) {
    _uploadedCount += count;
    Log.notice(F("[Sheets] Pushed %u signal(s) OK (HTTP %d in %u ms, total_uploaded=%u, free_heap=%u)" CR),
               (unsigned int)count, httpCode, (unsigned int)elapsed,
               (unsigned int)_uploadedCount, (unsigned int)ESP.getFreeHeap());
    return true;
  } else {
    _httpErrorCount++;
    char sslErr[96] = {0};
    int lastErr = client.lastError(sslErr, sizeof(sslErr));
    Log.error(F("[Sheets] HTTP POST failed: code=%d (%s) ssl_err=%d (%s) after %u ms (free_heap=%u)" CR),
              httpCode, http.errorToString(httpCode).c_str(), lastErr, sslErr,
              (unsigned int)elapsed, (unsigned int)ESP.getFreeHeap());
    return false;
  }
}

inline void uploaderTask(void* pvParameters) {
  (void)pvParameters;
  SheetsSignalRecord* batch[SHEETS_MAX_BATCH_SIZE];

  for (;;) {
    SheetsSignalRecord* first = nullptr;
    if (xQueueReceive(_uploadQueue, &first, portMAX_DELAY) != pdTRUE || !first) {
      continue;
    }

    size_t count = 0;
    batch[count++] = first;

    // If this is the startup BOOT_READY ping, wait 2.5s so rf.initReceiver()
    // finishes freeing cfg->devices (25KB) and completes initial RSSI calibration.
    if (first->decodedCount == 0xFF) {
      vTaskDelay(pdMS_TO_TICKS(2500));
    } else if (SHEETS_BATCH_WAIT_MS > 0) {
      // Wait briefly to coalesce multi-burst signals into a single HTTPS request
      vTaskDelay(pdMS_TO_TICKS(SHEETS_BATCH_WAIT_MS));
    }

    while (count < SHEETS_MAX_BATCH_SIZE) {
      SheetsSignalRecord* next = nullptr;
      if (xQueueReceive(_uploadQueue, &next, 0) == pdTRUE && next) {
        batch[count++] = next;
      } else {
        break;
      }
    }

    sendBatchToGoogleSheets(batch, count);

    for (size_t i = 0; i < count; i++) {
      free(batch[i]);
    }
  }
}

inline void enqueueBootPing() {
  if (!_uploadQueue || !isWebhookConfigured()) {
    return;
  }
  const char* rawMsg = "ESP32-C3_ONLINE";
  size_t rawLen = strlen(rawMsg) + 1;
  SheetsSignalRecord* rec = static_cast<SheetsSignalRecord*>(
      malloc(sizeof(SheetsSignalRecord) + rawLen));
  if (!rec) {
    return;
  }
  rec->seq = 0;
  rec->timestampMs = millis();
  rec->frequencyMhz = RF_MODULE_FREQUENCY;
  rec->rssi = WiFi.RSSI();
  rec->durationUs = 0;
  rec->numPulses = 0;
  rec->pulsesTruncated = false;
  rec->decodedCount = 0xFF; // Marks BOOT_READY status
  strlcpy(rec->model, "ESP32-C3_CC1101", sizeof(rec->model));
  strlcpy(rec->deviceId, WiFi.localIP().toString().c_str(), sizeof(rec->deviceId));
  snprintf(rec->protocol, sizeof(rec->protocol), "WIFI:%s (FSK+OOK)",
           WIFI_SSID);
  snprintf(rec->decodedJson, sizeof(rec->decodedJson),
           "{\"event\":\"boot_ready\",\"modulation\":\"FSK+OOK_AUTO\",\"ssid\":\"%s\",\"ip\":\"%s\",\"rssi\":%d}",
           WIFI_SSID,
           WiFi.localIP().toString().c_str(), (int)WiFi.RSSI());
  memcpy(rec->rawPulses, rawMsg, rawLen);

  if (xQueueSend(_uploadQueue, &rec, 0) != pdTRUE) {
    free(rec);
  }
}

inline void begin() {
  if (_uploadQueue == nullptr) {
    _uploadQueue = xQueueCreate(SHEETS_QUEUE_SIZE, sizeof(SheetsSignalRecord*));
  }
  connectWiFiNonBlocking();
  if (isWebhookConfigured()) {
    Log.notice(F("[Sheets] Google Sheets logger ready (queue=%u, batch=%u, free_heap=%u)" CR),
               (unsigned int)SHEETS_QUEUE_SIZE, (unsigned int)SHEETS_MAX_BATCH_SIZE,
               (unsigned int)ESP.getFreeHeap());
  } else {
    Log.warning(F("[Sheets] GOOGLE_SHEETS_WEBHOOK_URL is not configured yet!" CR));
  }

  if (_uploaderTaskHandle == nullptr && _uploadQueue != nullptr) {
    // Priority 1 so ReceiverTask (Priority 2) and DecoderTask (Priority 3)
    // always preempt the HTTPS uploader when an RF signal arrives.
    BaseType_t ok = xTaskCreate(uploaderTask, "SheetsUploader", 8192, nullptr, 1,
                                &_uploaderTaskHandle);
    if (ok != pdPASS) {
      Log.error(F("[Sheets] Failed to create SheetsUploader task (free_heap=%u)!" CR),
                (unsigned int)ESP.getFreeHeap());
    }
  }
  enqueueBootPing();
}

} // namespace GoogleSheetsLogger

#endif // defined(ESP32) && defined(ENABLE_GOOGLE_SHEETS)

#endif // GOOGLE_SHEETS_LOGGER_H
