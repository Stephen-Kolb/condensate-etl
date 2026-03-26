// ============================================================
// CONDENSATE RECONCILIATION ETL
// Google Apps Script → Supabase REST API
// ============================================================

// ============================================================
// CONFIGURATION — loaded from Script Properties
// Run setSecrets() once manually to store values, then delete it
// Never hardcode secrets in this file
// ============================================================
const props             = PropertiesService.getScriptProperties();
const SUPABASE_URL      = props.getProperty('SUPABASE_URL');
const SUPABASE_ANON_KEY = props.getProperty('SUPABASE_ANON_KEY');
const ZYN_FOLDER_ID     = props.getProperty('ZYN_FOLDER_ID');
const SF_FOLDER_ID      = props.getProperty('SF_FOLDER_ID');

// Non-secret config — safe to hardcode
const ZYN_TIMEZONE_OFFSET_HRS  = 2;   // Zynatech Eastern → Central (add 2hrs)
const SF_TIMEZONE_OFFSET_HRS   = 0;   // SF already Central (no adjustment)
const TIME_GAP_FLAG_HRS        = 4;   // flag if SF transaction > 4hrs after Zynatech stop

// ============================================================
// MAIN ENTRY POINT — run this on a daily trigger
// ============================================================
function runDailyETL() {
  Logger.log('=== Starting Daily ETL ===');
  try {
    ingestZynatechFiles();
    ingestSFTrucksFiles();
    ingestSFTransactionsFiles();
    runReconciliation();
    updateRTUAccuracySummary();
    Logger.log('=== ETL Complete ===');
  } catch (err) {
    Logger.log('ETL ERROR: ' + err.message);
    MailApp.sendEmail(
      Session.getActiveUser().getEmail(),
      'Condensate ETL Error - ' + new Date().toLocaleDateString(),
      'ETL failed with error:\n\n' + err.message + '\n\n' + err.stack
    );
  }
}

// ============================================================
// ZYNATECH INGESTION
// ============================================================
function ingestZynatechFiles() {
  const folder = DriveApp.getFolderById(ZYN_FOLDER_ID);
  const files  = folder.getFilesByType(MimeType.MICROSOFT_EXCEL);
  const today  = new Date();
  today.setHours(0, 0, 0, 0);

  let processed = 0;

  while (files.hasNext()) {
    const file = files.next();
    if (file.getDateCreated() < today) continue;
    if (!file.getName().includes('Transaction Breakdown')) continue; // skip RTU Breakdown

    Logger.log('Processing Zynatech file: ' + file.getName());
    const data = parseZynatechExcel(file);

    data.forEach(row => {
      const record = normalizeZynatechRow(row, file.getName());
      if (!record) return;
      upsertRecord('zynatech_raw', record, ['truck_ticket', 'rtu', 'railcar', 'start_time_utc']);
    });

    processed++;
  }

  Logger.log('Zynatech files processed: ' + processed);
}

function parseZynatechExcel(file) {
  // Open directly — file is already Google Sheets format in Drive
  const ss    = SpreadsheetApp.openById(file.getId());
  const sheet = ss.getSheets()[0];
  const data  = sheet.getDataRange().getValues();

  // Find header row (row with 'BOL#')
  let headerRow = -1;
  for (let i = 0; i < data.length; i++) {
    if (String(data[i][0]).trim() === 'BOL#') {
      headerRow = i;
      break;
    }
  }

  if (headerRow === -1) {
    Logger.log('Could not find header row in Zynatech file');
    return [];
  }

  const headers = data[headerRow].map(h => String(h).trim());
  const rows    = [];

  for (let i = headerRow + 1; i < data.length; i++) {
    if (!data[i][0]) continue; // skip empty rows
    const row = {};
    headers.forEach((h, idx) => {
      row[h] = data[i][idx];
    });
    rows.push(row);
  }

  return rows;
}

function normalizeZynatechRow(row, fileName) {
  try {
    const dateStr  = String(row['Date']).trim();         // '2026/03/25'
    const startStr = String(row['Start Time']).trim();   // '00:24:43'
    const stopStr  = String(row['Stop Time']).trim();

    // Parse Eastern datetime and convert to UTC (Eastern + 5hrs = UTC, or +2 for Central)
    const startEastern = new Date(`${dateStr.replace(/\//g, '-')}T${startStr}`);
    const stopEastern  = new Date(`${dateStr.replace(/\//g, '-')}T${stopStr}`);

    // Add offset to normalize to Central time
    const startCentral = new Date(startEastern.getTime() + ZYN_TIMEZONE_OFFSET_HRS * 3600000);
    const stopCentral  = new Date(stopEastern.getTime()  + ZYN_TIMEZONE_OFFSET_HRS * 3600000);

    // Normalize well name using typo map
    const wellRaw      = String(row['Well'] || '').trim().toUpperCase();
    const wellCorrected = lookupWellName(wellRaw);

    // Normalize railcar — pad PROX numbers to 6 digits, strip spaces
    const railcarRaw   = String(row['Railcar #'] || '').trim();
    const railcar      = normalizeRailcar(railcarRaw);

    // Normalize truck number
    const truckNumber  = String(row['Trk #'] || '').trim();

    // Ticket number — store as string for consistent matching
    const ticketRaw    = String(row['Trk Ticket'] || '').trim();

    return {
      source_file:          fileName,
      bol_number:           parseInt(row['BOL#']) || null,
      card_number:          String(row['Card Number'] || '').trim(),
      rtu:                  String(row['RTU'] || '').trim(),
      transaction_date:     dateStr.replace(/\//g, '-'),
      start_time_raw:       startStr,
      stop_time_raw:        stopStr,
      start_time_utc:       startCentral.toISOString(),
      stop_time_utc:        stopCentral.toISOString(),
      railcar:              railcar,
      truck_number:         truckNumber,
      well_name_raw:        row['Well'] || '',
      well_name:            wellCorrected,
      truck_ticket_raw:     ticketRaw,
      truck_ticket:         ticketRaw,
      batch_number:         String(row['Batch #'] || '').trim(),
      mass_delivered_lbs:   parseNumeric(row['Mass Delivered']),
      gross_volume_gal:     parseNumeric(row['Gross Volume']),
      net_volume_gal:       parseNumeric(row['Net Volume']),
      avg_temperature_f:    parseNumeric(row['Avg. Temperature']),
      observed_gravity:     parseNumeric(row['Observed Gravity/Density']),
      corrected_gravity:    parseNumeric(row['Corrected Gravity/Density']),
      avg_gross_gpm:        parseNumeric(row['Average Gross Gallons per Minute']),
      avg_net_gpm:          parseNumeric(row['Average Net Gallons per Minute'])
    };
  } catch (err) {
    Logger.log('Error normalizing Zynatech row: ' + err.message + ' | row: ' + JSON.stringify(row));
    return null;
  }
}

// ============================================================
// SF TRUCKS INGESTION
// ============================================================
function ingestSFTrucksFiles() {
  const folder = DriveApp.getFolderById(SF_FOLDER_ID);
  const files  = folder.getFilesByType(MimeType.CSV);
  const today  = new Date();
  today.setHours(0, 0, 0, 0);

  while (files.hasNext()) {
    const file = files.next();
    if (file.getDateCreated() < today) continue;
    if (!file.getName().includes('Trucks')) continue;

    Logger.log('Processing SF Trucks file: ' + file.getName());

    const rows = Utilities.parseCsv(file.getBlob().getDataAsString());
    if (rows.length < 2) return;

    const headers = rows[0].map(h => h.trim());

    for (let i = 1; i < rows.length; i++) {
      const row = {};
      headers.forEach((h, idx) => { row[h] = rows[i][idx] || ''; });

      const record = normalizeSFTrucksRow(row, file.getName());
      if (!record) continue;

      upsertRecord('sf_trucks_raw', record, ['equipment_cycle_id', 'original_ticket']);
    }
  }
}

function normalizeSFTrucksRow(row, fileName) {
  try {
    const arrivalRaw = row['Destination Arrival Date'] || '';
    const arrivalDate = arrivalRaw ? new Date(arrivalRaw) : null;

    return {
      source_file:              fileName,
      truck_number:             String(row['Equipment ID: Equipment ID'] || '').trim(),
      equipment_cycle_id:       String(row['Equipment Cycle ID'] || '').trim(),
      carrier_name:             String(row['Vehicle Carrier: Truck Carrier Name'] || '').trim(),
      customer_name:            String(row['Customer: Customer Name'] || '').trim(),
      arrival_datetime_raw:     arrivalRaw,
      arrival_datetime_utc:     arrivalDate ? arrivalDate.toISOString() : null,
      terminal_time_min:        parseNumeric(row['Terminal Time (Min)']),
      reference_number:         String(row['Reference Number'] || '').trim(),
      original_ticket_raw:      String(row['Original Ticket #'] || '').trim(),
      original_ticket:          String(row['Original Ticket #'] || '').trim(),
      amount_discharged_lbs:    parseNumeric(row['Amount DIscharged (Lbs)']),
      original_ticket_gallons:  parseNumeric(row['Original Ticket Gallons']),
      gallons_discharged:       parseNumeric(row['Gallons Discharged']),
      api_gravity:              parseNumeric(row['API Gravity']),
      // gross and tare will arrive tomorrow
      gross_weight_lbs:         parseNumeric(row['Gross Weight (Lbs)'] || null),
      tare_weight_lbs:          parseNumeric(row['Tare Weight (Lbs)'] || null)
    };
  } catch (err) {
    Logger.log('Error normalizing SF Trucks row: ' + err.message);
    return null;
  }
}

// ============================================================
// SF TRANSACTIONS INGESTION
// ============================================================
function ingestSFTransactionsFiles() {
  const folder = DriveApp.getFolderById(SF_FOLDER_ID);
  const files  = folder.getFilesByType(MimeType.CSV);
  const today  = new Date();
  today.setHours(0, 0, 0, 0);

  while (files.hasNext()) {
    const file = files.next();
    if (file.getDateCreated() < today) continue;
    if (!file.getName().includes('Transactions')) continue;

    Logger.log('Processing SF Transactions file: ' + file.getName());

    const rows = Utilities.parseCsv(file.getBlob().getDataAsString());
    if (rows.length < 2) return;

    const headers = rows[0].map(h => h.trim());

    for (let i = 1; i < rows.length; i++) {
      const row = {};
      headers.forEach((h, idx) => { row[h] = rows[i][idx] || ''; });

      const record = normalizeSFTransactionsRow(row, file.getName());
      if (!record) continue;

      upsertRecord('sf_transactions_raw', record, ['origin_cycle_id', 'destination_railcar', 'original_ticket']);
    }
  }
}

function normalizeSFTransactionsRow(row, fileName) {
  try {
    const dtRaw  = row['Transaction Date/Time'] || '';
    const dtDate = dtRaw ? new Date(dtRaw) : null;

    return {
      source_file:              fileName,
      origin_cycle_id:          String(row['Origin Equipment: Equipment Cycle ID'] || '').trim(),
      inbound_po:               String(row['Inbound PO'] || '').trim(),
      transaction_datetime_raw: dtRaw,
      transaction_datetime_utc: dtDate ? dtDate.toISOString() : null,
      truck_carrier:            String(row['Truck Carrier'] || '').trim(),
      reference_number:         String(row['Origin Equipment: Reference Number'] || '').trim(),
      original_ticket_raw:      String(row['Original Ticket #'] || '').trim(),
      original_ticket:          String(row['Original Ticket #'] || '').trim(),
      truck_number:             String(row['Truck Number'] || '').trim(),
      destination_railcar:      normalizeRailcar(row['Destination Equipment'] || ''),
      destination_cycle_id:     String(row['Destination Equipment: Equipment Cycle ID'] || '').trim(),
      amount_transferred_lbs:   parseNumeric(row['Amount Transferred (lbs)']),
      gallons_loaded:           parseNumeric(row['Gallons Loaded into Equipment']),
      barrels:                  parseNumeric(row['Barrels']),
      api_gravity:              parseNumeric(row['Origin Equipment: API Gravity'])
    };
  } catch (err) {
    Logger.log('Error normalizing SF Transactions row: ' + err.message);
    return null;
  }
}

// ============================================================
// RECONCILIATION ENGINE
// ============================================================
function runReconciliation() {
  Logger.log('Starting reconciliation...');

  // Pull unreconciled Zynatech records
  const zynRecords = supabaseQuery(
    'zynatech_raw',
    'id,truck_ticket,truck_number,railcar,rtu,well_name,start_time_utc,stop_time_utc,' +
    'mass_delivered_lbs,net_volume_gal,transaction_date'
  );

  // Pull unreconciled SF transaction records
  const sfTransRecords = supabaseQuery(
    'sf_transactions_raw',
    'id,original_ticket,truck_number,destination_railcar,transaction_datetime_utc,' +
    'amount_transferred_lbs,gallons_loaded,origin_cycle_id,inbound_po'
  );

  // Pull SF trucks for driver ticket gallons + carrier
  const sfTrucksRecords = supabaseQuery(
    'sf_trucks_raw',
    'id,original_ticket,truck_number,carrier_name,original_ticket_gallons,' +
    'gallons_discharged,amount_discharged_lbs,gross_weight_lbs,tare_weight_lbs,arrival_datetime_utc'
  );

  // Build lookup maps for fast matching
  const sfTransByKey = {};
  sfTransRecords.forEach(r => {
    const key = `${r.original_ticket}_${r.truck_number}`;
    if (!sfTransByKey[key]) sfTransByKey[key] = [];
    sfTransByKey[key].push(r);
  });

  const sfTrucksByTicket = {};
  sfTrucksRecords.forEach(r => {
    sfTrucksByTicket[r.original_ticket] = r;
  });

  const matched    = [];
  const exceptions = [];
  const matchedZynIds  = new Set();
  const matchedSfIds   = new Set();

  // Match each Zynatech record
  zynRecords.forEach(zyn => {
    const key     = `${zyn.truck_ticket}_${zyn.truck_number}`;
    const sfMatches = sfTransByKey[key] || [];

    // Filter to matching railcar
    const railcarMatches = sfMatches.filter(sf =>
      normalizeRailcar(sf.destination_railcar) === normalizeRailcar(zyn.railcar)
    );

    if (railcarMatches.length === 0) {
      // No match found
      exceptions.push(buildException({
        exception_date:   zyn.transaction_date,
        zynatech_id:      zyn.id,
        truck_number:     zyn.truck_number,
        truck_ticket:     zyn.truck_ticket,
        railcar:          zyn.railcar,
        rtu:              zyn.rtu,
        exception_type:   'no_sf_match',
        exception_detail: `Zynatech BOL ticket ${zyn.truck_ticket} truck ${zyn.truck_number} railcar ${zyn.railcar} not found in SF Transactions`
      }));
      return;
    }

    // Take best match (closest timestamp)
    const sfMatch = railcarMatches.sort((a, b) => {
      const zynStop  = new Date(zyn.stop_time_utc).getTime();
      const aDelta   = Math.abs(new Date(a.transaction_datetime_utc).getTime() - zynStop);
      const bDelta   = Math.abs(new Date(b.transaction_datetime_utc).getTime() - zynStop);
      return aDelta - bDelta;
    })[0];

    // Get SF truck record for driver ticket gallons
    const sfTruck = sfTrucksByTicket[zyn.truck_ticket] || {};

    // Compute deltas
    const massDelta     = sfMatch.amount_transferred_lbs - zyn.mass_delivered_lbs;
    const massDeltaPct  = zyn.mass_delivered_lbs > 0
      ? (massDelta / zyn.mass_delivered_lbs) * 100 : null;
    const volDelta      = sfMatch.gallons_loaded - zyn.net_volume_gal;
    const volDeltaPct   = zyn.net_volume_gal > 0
      ? (volDelta / zyn.net_volume_gal) * 100 : null;

    // Driver vs RTU delta
    const driverTicketGal   = parseFloat(sfTruck.original_ticket_gallons) || null;
    const driverVsRTUDelta  = driverTicketGal
      ? driverTicketGal - zyn.net_volume_gal : null;

    // Time gap check
    const zynStop    = new Date(zyn.stop_time_utc).getTime();
    const sfTime     = new Date(sfMatch.transaction_datetime_utc).getTime();
    const gapMinutes = (sfTime - zynStop) / 60000;

    // Scalehouse override flag — SF BOL gallons match driver ticket, not RTU
    const scaleOverride = driverTicketGal &&
      Math.abs(sfMatch.gallons_loaded - driverTicketGal) < 10;

    // Split load — check if same ticket appears for multiple railcars
    const allRailcars = sfMatches.map(m => m.destination_railcar);
    const isSplit     = allRailcars.length > 1;

    // Anomaly thresholds
    const massAnomaly   = Math.abs(massDeltaPct) > 2;    // > 2% mass delta
    const volAnomaly    = Math.abs(volDeltaPct)  > 2;    // > 2% volume delta
    const timeGapFlag   = Math.abs(gapMinutes)   > TIME_GAP_FLAG_HRS * 60;

    matchedZynIds.add(zyn.id);
    matchedSfIds.add(sfMatch.id);

    const record = {
      match_date:               zyn.transaction_date,
      zynatech_id:              zyn.id,
      sf_transaction_id:        sfMatch.id,
      sf_truck_id:              sfTruck.id || null,
      truck_number:             zyn.truck_number,
      truck_ticket:             zyn.truck_ticket,
      railcar:                  zyn.railcar,
      rtu:                      zyn.rtu,
      well_name:                zyn.well_name,
      carrier_name:             sfTruck.carrier_name || null,
      zynatech_start_utc:       zyn.start_time_utc,
      sf_transaction_utc:       sfMatch.transaction_datetime_utc,
      time_delta_minutes:       Math.round(gapMinutes),
      zyn_mass_lbs:             zyn.mass_delivered_lbs,
      sf_mass_lbs:              sfMatch.amount_transferred_lbs,
      mass_delta_pct:           massDeltaPct ? Math.round(massDeltaPct * 100) / 100 : null,
      zyn_net_volume_gal:       zyn.net_volume_gal,
      sf_gallons_loaded:        sfMatch.gallons_loaded,
      volume_delta_pct:         volDeltaPct ? Math.round(volDeltaPct * 100) / 100 : null,
      driver_ticket_gallons:    driverTicketGal,
      driver_vs_rtu_delta_gal:  driverVsRTUDelta ? Math.round(driverVsRTUDelta * 10) / 10 : null,
      scalehouse_override_flag: !!scaleOverride,
      split_load_flag:          isSplit,
      mass_anomaly_flag:        massAnomaly,
      volume_anomaly_flag:      volAnomaly,
      match_confidence:         timeGapFlag ? 'fuzzy' : 'exact',
      notes:                    timeGapFlag ? `Time gap ${Math.round(gapMinutes)} min` : null
    };

    matched.push(record);

    // If time gap is large, also log as exception for review
    if (timeGapFlag) {
      exceptions.push(buildException({
        exception_date:   zyn.transaction_date,
        zynatech_id:      zyn.id,
        sf_transaction_id: sfMatch.id,
        truck_number:     zyn.truck_number,
        truck_ticket:     zyn.truck_ticket,
        railcar:          zyn.railcar,
        rtu:              zyn.rtu,
        exception_type:   'time_gap_too_large',
        exception_detail: `Gap of ${Math.round(gapMinutes)} minutes between Zynatech stop and SF transaction`
      }));
    }
  });

  // Find SF transaction records with no Zynatech match
  sfTransRecords.forEach(sf => {
    if (matchedSfIds.has(sf.id)) return;
    exceptions.push(buildException({
      exception_date:   sf.transaction_datetime_utc
        ? sf.transaction_datetime_utc.split('T')[0] : null,
      sf_transaction_id: sf.id,
      truck_number:     sf.truck_number,
      truck_ticket:     sf.original_ticket,
      railcar:          sf.destination_railcar,
      exception_type:   'no_zyn_match',
      exception_detail: `SF Transaction ticket ${sf.original_ticket} truck ${sf.truck_number} not found in Zynatech`
    }));
  });

  // Batch insert results
  if (matched.length > 0) {
    batchInsert('transactions_matched', matched);
    Logger.log(`Matched: ${matched.length} records`);
  }

  if (exceptions.length > 0) {
    batchInsert('transactions_exceptions', exceptions);
    Logger.log(`Exceptions: ${exceptions.length} records`);
  }
}

// ============================================================
// RTU ACCURACY SUMMARY
// ============================================================
function updateRTUAccuracySummary() {
  const today = new Date().toISOString().split('T')[0];

  // Pull today's matched records grouped by RTU
  const records = supabaseQuery(
    'transactions_matched',
    'rtu,mass_delta_pct,volume_delta_pct,driver_vs_rtu_delta_gal,' +
    'scalehouse_override_flag,split_load_flag,mass_anomaly_flag,volume_anomaly_flag',
    `match_date=eq.${today}`
  );

  // Group by RTU
  const byRTU = {};
  records.forEach(r => {
    if (!byRTU[r.rtu]) byRTU[r.rtu] = [];
    byRTU[r.rtu].push(r);
  });

  // Pull exception counts
  const exceptions = supabaseQuery(
    'transactions_exceptions',
    'rtu',
    `exception_date=eq.${today}`
  );

  const exByRTU = {};
  exceptions.forEach(e => {
    if (!e.rtu) return;
    exByRTU[e.rtu] = (exByRTU[e.rtu] || 0) + 1;
  });

  // Build summary rows
  const summaries = Object.keys(byRTU).map(rtu => {
    const rows  = byRTU[rtu];
    const count = rows.length;
    const avg   = (arr, key) => arr.reduce((s, r) => s + (r[key] || 0), 0) / count;

    return {
      summary_date:               today,
      rtu:                        rtu,
      total_transactions:         count,
      matched_transactions:       count,
      exception_count:            exByRTU[rtu] || 0,
      split_load_count:           rows.filter(r => r.split_load_flag).length,
      scalehouse_override_count:  rows.filter(r => r.scalehouse_override_flag).length,
      avg_mass_delta_pct:         Math.round(avg(rows, 'mass_delta_pct') * 100) / 100,
      mass_anomaly_count:         rows.filter(r => r.mass_anomaly_flag).length,
      avg_volume_delta_gal:       Math.round(avg(rows, 'driver_vs_rtu_delta_gal') * 10) / 10,
      volume_anomaly_flag_count:  rows.filter(r => r.volume_anomaly_flag).length,
      avg_driver_vs_rtu_delta:    Math.round(avg(rows, 'driver_vs_rtu_delta_gal') * 10) / 10
    };
  });

  if (summaries.length > 0) {
    batchInsert('rtu_accuracy_summary', summaries);
    Logger.log('RTU summary updated for ' + summaries.length + ' RTUs');
  }
}

// ============================================================
// SUPABASE API HELPERS
// ============================================================
function supabaseQuery(table, select, filter) {
  let url = `${SUPABASE_URL}/rest/v1/${table}?select=${select}`;
  if (filter) url += `&${filter}`;

  const response = UrlFetchApp.fetch(url, {
    method: 'GET',
    headers: {
      'apikey':        SUPABASE_ANON_KEY,
      'Authorization': 'Bearer ' + SUPABASE_ANON_KEY,
      'Content-Type':  'application/json'
    },
    muteHttpExceptions: true
  });

  if (response.getResponseCode() !== 200) {
    throw new Error(`Supabase query failed on ${table}: ${response.getContentText()}`);
  }

  return JSON.parse(response.getContentText());
}

function upsertRecord(table, record, conflictColumns) {
  const url = `${SUPABASE_URL}/rest/v1/${table}`;

  const response = UrlFetchApp.fetch(url, {
    method:  'POST',
    headers: {
      'apikey':        SUPABASE_ANON_KEY,
      'Authorization': 'Bearer ' + SUPABASE_ANON_KEY,
      'Content-Type':  'application/json',
      'Prefer':        `resolution=merge-duplicates,return=minimal`
    },
    payload:            JSON.stringify(record),
    muteHttpExceptions: true
  });

  const code = response.getResponseCode();
  if (code !== 200 && code !== 201) {
    Logger.log(`Upsert failed on ${table}: ${response.getContentText()}`);
  }
}

function batchInsert(table, records) {
  // Insert in chunks of 50 to avoid payload limits
  const chunkSize = 50;
  for (let i = 0; i < records.length; i += chunkSize) {
    const chunk = records.slice(i, i + chunkSize);
    const url   = `${SUPABASE_URL}/rest/v1/${table}`;

    const response = UrlFetchApp.fetch(url, {
      method:  'POST',
      headers: {
        'apikey':        SUPABASE_ANON_KEY,
        'Authorization': 'Bearer ' + SUPABASE_ANON_KEY,
        'Content-Type':  'application/json',
        'Prefer':        'return=minimal'
      },
      payload:            JSON.stringify(chunk),
      muteHttpExceptions: true
    });

    const code = response.getResponseCode();
    if (code !== 200 && code !== 201) {
      Logger.log(`Batch insert failed on ${table}: ${response.getContentText()}`);
    }
  }
}

// ============================================================
// NORMALIZATION HELPERS
// ============================================================

// Normalize railcar ID — pad numeric portion to 6 digits, strip spaces
// PROX47217 → PROX047217, PROX 45260 → PROX045260
function normalizeRailcar(raw) {
  if (!raw) return '';
  const cleaned = String(raw).trim().replace(/\s+/g, '');
  const match   = cleaned.match(/^([A-Z]+)(\d+)$/);
  if (!match) return cleaned;
  const prefix  = match[1];
  const number  = match[2].padStart(6, '0');
  return prefix + number;
}

// Strip commas and parse as float
function parseNumeric(val) {
  if (val === null || val === undefined || val === '') return null;
  const cleaned = String(val).replace(/,/g, '').trim();
  const parsed  = parseFloat(cleaned);
  return isNaN(parsed) ? null : parsed;
}

// Look up well name in typo map cache
let _wellNameCache = null;

function lookupWellName(rawName) {
  if (!rawName) return rawName;

  // Load cache once per run
  if (!_wellNameCache) {
    const records = supabaseQuery('well_name_map', 'typo,correct');
    _wellNameCache = {};
    records.forEach(r => {
      _wellNameCache[r.typo.toUpperCase()] = r.correct;
    });
  }

  return _wellNameCache[rawName.toUpperCase()] || rawName;
}

// Normalize all exception records to same shape so batch insert works
function buildException(fields) {
  return {
    exception_date:    fields.exception_date    || null,
    zynatech_id:       fields.zynatech_id       || null,
    sf_transaction_id: fields.sf_transaction_id || null,
    sf_truck_id:       fields.sf_truck_id       || null,
    truck_number:      fields.truck_number      || null,
    truck_ticket:      fields.truck_ticket      || null,
    railcar:           fields.railcar           || null,
    rtu:               fields.rtu               || null,
    exception_type:    fields.exception_type    || null,
    exception_detail:  fields.exception_detail  || null,
    resolved:          false
  };
}

// ============================================================
// SEED FUNCTIONS — run once to populate reference tables
// ============================================================

// Run once to seed RTU config table
function seedRTUConfig() {
  const rtus = [
    { rtu_id: 'S-1195', display_name: 'Transloader S-1195', timezone_offset: 2 },
    { rtu_id: 'S-1196', display_name: 'Transloader S-1196', timezone_offset: 2 },
    { rtu_id: 'S-1626', display_name: 'Transloader S-1626', timezone_offset: 2 },
    { rtu_id: 'S-1627', display_name: 'Transloader S-1627', timezone_offset: 2 },
    { rtu_id: 'S-1701', display_name: 'Transloader S-1701', timezone_offset: 2 },
    { rtu_id: 'S-1730', display_name: 'Transloader S-1730', timezone_offset: 2 },
  ];

  rtus.forEach(rtu => upsertRecord('rtu_config', rtu, ['rtu_id']));
  Logger.log('RTU config seeded');
}

// Run once to seed well name map — delete after running
// Populate typoData from your internal TypoMap before running
// DO NOT commit real well names to public repos
function seedWellNameMap() {
  const typoData = [
    // Add entries here in format: { typo: 'INCORRECT', correct: 'CORRECT' }
    // Clear this array before committing to version control
  ];

  if (typoData.length === 0) {
    Logger.log('No typo data provided — populate typoData array before running');
    return;
  }

  typoData.forEach(row => {
    upsertRecord('well_name_map', {
      typo:    row.typo.toUpperCase().trim(),
      correct: row.correct.trim()
    }, ['typo']);
  });
  Logger.log('Well name map seeded: ' + typoData.length + ' entries');
}
