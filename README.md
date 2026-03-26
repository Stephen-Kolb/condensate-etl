# Condensate Transaction Reconciliation ETL
**Google Apps Script → Supabase pipeline for reconciling transloader RTU data against Salesforce records**

---

## The Problem

Condensate transactions flow through three separate systems that don't talk to each other:

- **Zynatech RTU meters** — measure mass and volume at the transloader during unloading
- **Salesforce (IB Condensate Trucks)** — records truck scale weights on arrival
- **Salesforce (IB Condensate Transactions/BOLs)** — records what was transferred into each railcar

Reconciling these manually in Excel was slow, error-prone, and couldn't scale. Well name typos, ticket number inconsistencies, split loads across multiple railcars, timezone mismatches, and scalehouse overrides all created noise that buried the real accuracy signal.

## The Solution

A daily automated ETL pipeline that:
- Ingests all three sources automatically each morning
- Normalizes inconsistent data (railcar IDs, well names, numeric formats, timezones)
- Matches transactions across sources using ticket number + truck number as the primary key
- Computes mass and volume deltas between RTU measurements and scale weights
- Flags anomalies, split loads, and scalehouse overrides
- Writes clean matched records and exceptions to Supabase
- Aggregates RTU accuracy metrics by transloader unit

## How It Works

```
1am — Zynatech Transaction Breakdown lands in Google Drive (emailed daily)
1am — Salesforce Cloud4J reports dump to Google Drive (IB Trucks + Transactions CSVs)
        ↓
2am — runDailyETL() triggers
        ↓
ingestZynatechFiles()       — reads Google Sheets file, normalizes, upserts to zynatech_raw
ingestSFTrucksFiles()       — reads CSV, normalizes, upserts to sf_trucks_raw
ingestSFTransactionsFiles() — reads CSV, normalizes, upserts to sf_transactions_raw
        ↓
runReconciliation()
  PRIMARY MATCH: truck_ticket + truck_number (exact)
  SECONDARY: timestamp validation (SF transaction should follow Zynatech stop by 2-12 min)
  SPLIT LOAD DETECTION: same ticket, different railcars
  DELTA COMPUTATION: mass, volume, driver-reported vs RTU
  FLAG: scalehouse_override, mass_anomaly, volume_anomaly, time_gap
        ↓
transactions_matched        — clean reconciled records
transactions_exceptions     — unmatched or flagged for review
        ↓
updateRTUAccuracySummary()  — aggregates by RTU for dashboard
```

## Data Sources

### Source 1 — Zynatech Transaction Breakdown (Google Drive, daily Excel/Sheets)
RTU meter data from transloader units. Primary mass measurement source.

| Field | Description |
|-------|-------------|
| BOL# | Bill of lading number |
| RTU | Transloader unit ID (S-1195, S-1196, S-1626, S-1627, S-1701, S-1730) |
| Trk # | Truck number |
| Trk Ticket | Ticket number — primary join key |
| Railcar # | Destination railcar |
| Well | Origin well name (requires typo normalization — internal map not published) |
| Mass Delivered | RTU mass measurement (lbs) — primary truth |
| Net Volume | RTU volume measurement (gal) |
| Start/Stop Time | Eastern time — converted to UTC (+2hrs) |

### Source 2 — IB Condensate Trucks (Salesforce CSV, daily)
Scale weight on truck arrival. Secondary mass reference.

| Field | Description |
|-------|-------------|
| Equipment ID | Truck number |
| Original Ticket # | Join key to Zynatech |
| Amount Discharged (Lbs) | Scale weight |
| Original Ticket Gallons | Driver-reported volume |
| Gross/Tare Weight | Used for tare anomaly detection |
| Destination Arrival Date | Scale-in timestamp (Central) |

### Source 3 — IB Condensate Transactions/BOLs (Salesforce CSV, daily)
Transaction records of what was loaded into each railcar.

| Field | Description |
|-------|-------------|
| Original Ticket # | Primary join key |
| Truck Number | Secondary join key |
| Destination Equipment | Railcar receiving the load |
| Amount Transferred (lbs) | SF mass record |
| Gallons Loaded | SF volume record |
| Transaction Date/Time | Central time — converted to UTC (+1hr) |

## Key Matching Logic

```
PRIMARY JOIN:
  zynatech_raw.truck_ticket = sf_transactions_raw.original_ticket
  AND zynatech_raw.truck_number = sf_transactions_raw.truck_number
  AND zynatech_raw.railcar = sf_transactions_raw.destination_railcar (normalized)

SPLIT LOAD:
  Same ticket + truck, different railcars → split_load_flag = true

SCALEHOUSE OVERRIDE:
  SF BOL gallons ≈ driver ticket gallons (not RTU) → scalehouse_override_flag = true

MASS ANOMALY:
  |mass_delta_pct| > 2% → mass_anomaly_flag = true

TIME GAP:
  |SF transaction time - Zynatech stop time| > 4 hours → time_gap_too_large exception
```

## Normalization Rules

**Railcar IDs** — Zynatech omits leading zero in numeric portion:
```
PROX47217 → PROX047217
PROX 45260 → PROX045260
```

**Well Names** — 150 known typo corrections seeded from internal TypoMap (not published)

**Timezone** — Zynatech reports Eastern, Salesforce reports Central:
```
Zynatech start/stop: +2 hours → UTC
SF transactions: +1 hour → UTC
```

**Numeric fields** — SF reports include comma formatting:
```
"51,540" → 51540
"7,980.00" → 7980.0
```

## Database Schema (Supabase / PostgreSQL)

```
well_name_map           — 150 typo corrections
rtu_config              — 6 RTU units with timezone offsets
truck_registry          — carrier info, typical tare weights
truck_weight_history    — every weigh event for tare anomaly detection

zynatech_raw            — ingested RTU data
sf_trucks_raw           — ingested scale data
sf_transactions_raw     — ingested BOL transaction data

transactions_matched    — reconciled records with all deltas and flags
transactions_exceptions — unmatched or flagged for manual review
rtu_accuracy_summary    — daily aggregates by RTU for dashboard
```

## RTU Units

| RTU | Location/Notes |
|-----|---------------|
| RTU-A through RTU-F | Six transloader units at facility |

## Configuration

All secrets stored in Google Apps Script Properties — never hardcoded:

```javascript
SUPABASE_URL      // Supabase project URL
SUPABASE_ANON_KEY // Supabase anon key
ZYN_FOLDER_ID     // Google Drive folder for Zynatech reports
SF_FOLDER_ID      // Google Drive folder for Salesforce CSVs
```

Non-secret config hardcoded at top of `Code.gs`:

```javascript
ZYN_TIMEZONE_OFFSET_HRS = 2   // Eastern → Central
SF_TIMEZONE_OFFSET_HRS  = 0   // Already Central
TIME_GAP_FLAG_HRS       = 4   // Exception threshold
```

## Setup

1. Create a Supabase project and run `schema.sql` to create all tables
2. Create a new Google Apps Script project
3. Run `setSecrets()` once to store credentials in Script Properties, then delete it
4. Run `seedRTUConfig()` once to populate RTU reference table
5. Run `seedWellNameMap()` once to load 150 well name corrections
6. Set a daily time-driven trigger on `runDailyETL()` — recommended 2am
7. Ensure Zynatech reports land in Drive as native Google Sheets (not .xlsx)
8. Ensure Salesforce Cloud4J CSVs land in the configured SF folder

## Exception Types

| Type | Meaning |
|------|---------|
| `no_sf_match` | Zynatech has a record, no matching SF transaction |
| `no_zyn_match` | SF has a transaction, no matching Zynatech record |
| `ambiguous_match` | Multiple possible matches found |
| `ticket_format_error` | Ticket number couldn't be normalized |
| `time_gap_too_large` | Timestamps more than 4 hours apart |
| `mass_delta_critical` | Mass delta exceeds critical threshold |
| `tare_anomaly` | Truck tare weight suspicious vs historical average |

## Current Performance (2026-03-25 data)

| RTU | Transactions | Avg Mass Delta | Overrides | Split Loads |
|-----|-------------|----------------|-----------|-------------|
| RTU-A | 3 | 1.72% | 0 | 3 |
| RTU-B | 17 | 0.32% | 0 | 8 |
| RTU-C | 16 | 0.58% | 1 | 5 |

**Match rate: 87% on first run (36/41 transactions matched)**

## Known Issues & Next Steps

- Volume delta null — SF gallons parsing issue in reconciliation query
- `processZynatechReportsEmail()` needs Drive API fix for auto-conversion of xlsx attachments
- Historical data load (Dec 2025 — present) pending
- Dashboard build pending

## Author

**Stephen Kolb** — Logistics & Data Analyst, Fort Worth TX
[stephen-kolb.github.io](https://stephen-kolb.github.io) · [GitHub](https://github.com/Stephen-Kolb)
