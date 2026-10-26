/**
 * Google Apps Script Webhook cho dự án rtl_433_ESP (ESP32-C3 Super Mini + CC1101)
 *
 * HƯỚNG DẪN CÀI ĐẶT TRÊN GOOGLE SHEETS:
 * 1. Tạo một file Google Sheets mới tại https://sheets.new
 * 2. Vào menu: Tiện ích mở rộng (Extensions) -> Apps Script
 * 3. Xóa toàn bộ code mặc định trong file Code.gs và dán toàn bộ nội dung file này vào.
 * 4. Nhấn biểu tượng Đĩa mềm (Lưu / Ctrl+S).
 * 5. Nhấn nút "Triển khai" (Deploy) ở góc trên bên phải -> Chọn "Tùy chọn triển khai mới" (New deployment).
 * 6. Ở mục "Chọn loại" (Select type - hình bánh răng), chọn "Ứng dụng web" (Web app).
 * 7. Cấu hình như sau:
 *    - Thực thi dưới dạng (Execute as): "Tôi" (Me)
 *    - Ai có quyền truy cập (Who has access): "Bất kỳ ai" (Anyone)  <-- RẤT QUAN TRỌNG!
 * 8. Nhấn "Triển khai" (Deploy), cấp quyền truy cập (Authorize access -> Advanced -> Go to ... unsafe -> Allow).
 * 9. Copy đường dẫn "URL ứng dụng web" (Web app URL) có dạng:
 *    https://script.google.com/macros/s/AKfycb.../exec
 * 10. Dán URL đó vào cờ GOOGLE_SHEETS_WEBHOOK_URL trong file platformio.ini.
 */

var SHEET_NAME = 'RF_Raw_Signals';

var HEADERS = [
  'Timestamp (Giờ nhận)',
  'Seq #',
  'Uptime (ms)',
  'Freq (MHz)',
  'RSSI (dBm)',
  'Duration (us)',
  'Pulse Count',
  'Status',
  'Decoded Events',
  'Model',
  'Device ID / Ch',
  'Protocol',
  'Decoded JSON',
  'Raw Pulses (+pulse -gap us)',
  'Truncated?',
  'ESP Free Heap',
  'Wi-Fi RSSI (dBm)'
];

function getOrInitSheet() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEET_NAME);
  if (!sheet) {
    sheet = ss.insertSheet(SHEET_NAME);
  }
  if (sheet.getLastRow() === 0) {
    sheet.appendRow(HEADERS);
    sheet.setFrozenRows(1);
    var headerRange = sheet.getRange(1, 1, 1, HEADERS.length);
    headerRange.setFontWeight('bold');
    headerRange.setBackground('#d9ead3');
  }
  return sheet;
}

function doGet(e) {
  return ContentService.createTextOutput(
    JSON.stringify({
      status: 'ok',
      message: 'rtl_433_ESP Google Sheets Webhook is active',
      sheet: SHEET_NAME
    })
  ).setMimeType(ContentService.MimeType.JSON);
}

function doPost(e) {
  var lock = LockService.getScriptLock();
  lock.tryLock(10000);

  try {
    if (!e || !e.postData || !e.postData.contents) {
      return ContentService.createTextOutput(
        JSON.stringify({ status: 'error', message: 'Empty payload' })
      ).setMimeType(ContentService.MimeType.JSON);
    }

    var payload = JSON.parse(e.postData.contents);
    var sheet = getOrInitSheet();
    var now = Utilities.formatDate(
      new Date(),
      Session.getScriptTimeZone() || 'Asia/Ho_Chi_Minh',
      'yyyy-MM-dd HH:mm:ss.SSS'
    );

    var rowsInput = payload.rows || [payload];
    var freeHeap = payload.free_heap !== undefined ? payload.free_heap : '';
    var wifiRssi = payload.wifi_rssi !== undefined ? payload.wifi_rssi : '';

    var values = [];
    for (var i = 0; i < rowsInput.length; i++) {
      var r = rowsInput[i];
      var status = r.status || 'RAW_ONLY';
      if (status !== 'BOOT_READY' && status !== 'DECODED') {
        continue;
      }
      values.push([
        now,
        r.seq !== undefined ? r.seq : '',
        r.time_ms !== undefined ? r.time_ms : '',
        r.freq_mhz !== undefined ? r.freq_mhz : '',
        r.rssi !== undefined ? r.rssi : '',
        r.duration_us !== undefined ? r.duration_us : '',
        r.num_pulses !== undefined ? r.num_pulses : '',
        status,
        r.decoded_events !== undefined ? r.decoded_events : 0,
        r.model || '',
        r.device_id || '',
        r.protocol || '',
        r.decoded_json || '',
        r.raw_pulses || '',
        r.truncated ? 'TRUE' : 'FALSE',
        freeHeap,
        wifiRssi
      ]);
    }

    if (values.length > 0) {
      var startRow = sheet.getLastRow() + 1;
      sheet.getRange(startRow, 1, values.length, HEADERS.length).setValues(values);
      SpreadsheetApp.flush();
    }

    return ContentService.createTextOutput(
      JSON.stringify({ status: 'ok', inserted: values.length })
    ).setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    return ContentService.createTextOutput(
      JSON.stringify({ status: 'error', message: err.toString() })
    ).setMimeType(ContentService.MimeType.JSON);
  } finally {
    lock.releaseLock();
  }
}
