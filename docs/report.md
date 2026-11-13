# Flight Price Analysis: Pipeline Report

## 1. Overview

An Apache Airflow pipeline that ingests the *Flight Price Dataset of Bangladesh*
(57,000 bookings, 17 columns), validates and transforms it, loads it into
PostgreSQL and computes the KPIs that answer the business questions.

| Layer | Technology | Purpose |
|---|---|---|
| Orchestration | Apache Airflow 3.4 (CeleryExecutor, Docker) | Scheduling, retries, alerts |
| Staging | MySQL (`flight_staging`) | Raw copy of the CSV |
| Analytics | PostgreSQL (`flight_analytics`) | Enriched data and KPI tables |
| Processing | Python | Ingestion, validation, transformation, KPIs |
| Visualisation | Metabase | Dashboards on the KPI tables |

## 2. Architecture and execution flow

```
CSV  ->  ingest_flight_data  ->  MySQL raw_flight_prices
                                  (+ rejected_flight_rows)
                                        |
                              validate_flight_data
                                        |
                              transform_flight_data  ->  processed CSV
                                        |
                              load_postgres_data  ->  PostgreSQL transformed_flight_prices
                                        |
                              run_flight_analytics  ->  PostgreSQL kpi_* tables  ->  Metabase
```

Tasks run strictly in sequence and each passes a small result dictionary to the
next one through XCom. A later task never starts if an earlier one fails.

## 3. DAG and tasks

DAG `flight_price_pipeline`: manual trigger (`schedule=None`), one active run,
2-hour timeout, 2 retries with a 2-minute delay. After the final failed attempt a
Gmail alert is sent (optional: it is skipped when `GMAIL_ADDRESS` is not set).

| Task | What it does | Fails when |
|---|---|---|
| `ingest_flight_data` | Reads the CSV and replaces the contents of `raw_flight_prices` in batches of 1,000. Rows with missing or malformed numbers or dates are written to `rejected_flight_rows` with row number, reason and raw row. | CSV missing, a required column absent, DB error |
| `validate_flight_data` | Runs the data-quality checks (section 4) against MySQL and compares the staged count with the ingested count. | Any critical check > 0, count mismatch, > 5% rows rejected |
| `transform_flight_data` | Streams the staging table in batches, derives features, flags fare discrepancies, computes a SHA-256 `record_hash`, writes `data/processed/transformed_flight_prices.csv`. | Processed count differs from validated count |
| `load_postgres_data` | Bulk-inserts the processed CSV with `execute_values`; duplicates are skipped by `ON CONFLICT DO NOTHING`; checks that the table grew by exactly the inserted rows before committing. | Count verification fails, DB error |
| `run_flight_analytics` | Computes the KPIs and replaces the `kpi_*` tables in one transaction. | Query or storage error (previous results are kept) |

## 4. Data validation

| Check | Type | Rule |
|---|---|---|
| Required columns | critical | `airline`, `source_code`, `destination_code`, `base_fare_bdt`, `tax_surcharge_bdt`, `total_fare_bdt` exist (also enforced for the CSV at ingestion) |
| Missing values | critical | NULL/blank in the six required fields, seasonality or timestamps |
| Data types | enforced at ingestion | Non-numeric, blank or non-finite numbers and invalid dates are rejected into `rejected_flight_rows`; MySQL column types (`DECIMAL`, `DATETIME`, `INT`) enforce the rest |
| Negative fares | critical | base, tax or total < 0 |
| Durations | critical | duration <= 0 or arrival <= departure |
| Invalid cities | critical | Airport code not in the list of 20 known IATA codes, or source = destination |
| Rejected rows | warning, critical above 5% | Rows rejected at ingestion |
| Fare discrepancy | warning | `abs(total - (base + tax)) > 0.01` |
| Blank descriptive text | warning | Empty airport name, stopovers, aircraft, class or booking source |

**Handling inconsistencies.** The pipeline *flags rather than rewrites* fares. For
every row `expected_fare_bdt = base + tax`, `fare_discrepancy_bdt` and
`fare_discrepancy_flag` are stored next to the original `total_fare_bdt`.
On this dataset 2,522 rows (4.42%) have a total above base + tax; they are kept
and flagged, and the KPIs use the original `total_fare_bdt`. The
`expected_fare_bdt` column is available if you prefer the recomputed value.

Result on the supplied dataset: 57,000 rows, 0 rejected, 0 critical failures.

## 5. Transformation

`Total Fare = Base Fare + Tax & Surcharge` already exists in the CSV, so it is
verified (above) rather than recreated. Added columns:

- `departure_hour`, `departure_day_name`, `departure_month`, `departure_year`
- `is_peak_season`: true for **Eid, Hajj, Winter Holidays**; false for Regular
- `booking_lead_time_group`: 0-3, 4-7, 8-14, 15-30, 31-60, 61+ days
- `expected_fare_bdt`, `fare_discrepancy_bdt`, `fare_discrepancy_flag`
- `record_hash`: SHA-256 over the business columns (excludes the MySQL `id` and
  `ingested_at`), the stable identity of a flight

Text is trimmed and whitespace-normalised; airport codes are upper-cased.

## 6. KPIs

A *booking* is one row of `transformed_flight_prices`. Fares are `total_fare_bdt`
in BDT. Results are stored in the `flight_analytics` schema.

| KPI (spec) | Table | Logic |
|---|---|---|
| Average fare by airline | `kpi_avg_fare_by_airline` | `AVG(total_fare_bdt)` (also min, max, count) per airline |
| Seasonal fare variation | `kpi_seasonal_fare_variation`, `kpi_peak_vs_non_peak_fare` | Average fare per season (Eid, Hajj, Winter Holidays, Regular) with `% variation vs non-peak`; plus the peak vs non-peak roll-up |
| Booking count by airline | `kpi_booking_count_by_airline` | `COUNT(*)` per airline and share of all bookings |
| Most popular routes | `kpi_popular_routes` | Source-destination pairs ranked by booking count (top 15) with average fare |
| *(extra)* Fare by lead time | `kpi_fare_by_booking_lead_time` | Count and fare statistics per lead-time group |

Results on the supplied dataset (verified against an independent computation
from the raw CSV):

| Season | Bookings | Average fare (BDT) | vs non-peak |
|---|---|---|---|
| Hajj | 942 | 97,144.47 | +42.70% |
| Eid | 603 | 91,560.02 | +34.49% |
| Winter Holidays | 10,930 | 79,676.74 | +17.04% |
| Regular (non-peak) | 44,525 | 68,077.27 | baseline |
| **Peak overall** | 12,475 | 81,570.14 | **+19.82%** |

Most popular route: Rajshahi (RJH) to Singapore (SIN), 417 bookings.

The peak definition is a business rule in `src/transformation/transform.py`
(`PEAK_SEASONS`). Hajj is included because it is a major travel period in
Bangladesh; remove it there to treat only Eid and Winter Holidays as peak.

## 7. Loading and consistency

- **Bulk load.** `execute_values` sends each batch as a single statement instead
  of one round trip per row.
- **Single transaction.** The whole load commits once; any error rolls it back.
- **Re-runs are safe.** MySQL ids change on every ingestion, so they cannot
  identify a flight. `record_hash` is the identity; rows already present are skipped.
  Verified: a second load of the same 57,000 rows inserts 0 and skips 57,000.
- **Count verification.** Before committing, the loader checks
  `rows_after == rows_before + inserted`.

## 8. Challenges and resolutions

| Challenge | Resolution |
|---|---|
| The same dataset loaded twice would fail or duplicate | `record_hash` identity with `ON CONFLICT DO NOTHING`. The MySQL auto-increment id had drifted (the first id of the last run was 513,001) and was useless for deduplication. |
| `record_hash` was required by the PostgreSQL schema but missing from the loader's column mapping | Added to the mapping. |
| Reported "inserted/skipped" numbers were wrong: `rowcount` after `executemany` only reflects the last statement | Count the rows returned by `INSERT ... RETURNING` and verify against the table count. |
| One malformed value crashed the whole ingestion before validation could report it | Tolerant parsing with a `rejected_flight_rows` table and a 5% failure threshold. |
| 4.42% of rows have totals that do not equal base + tax | Flag, do not modify; keep both columns; documented. |
| KPIs were only printed, not stored | KPI tables rebuilt atomically after every run. |
| DAG failed to import without `GMAIL_ADDRESS` | Alerts are optional. |
| Pipeline not runnable from the repository alone | `.env.example`, requirements files, optional local MySQL/PostgreSQL services (`--profile local-db`), schemas auto-applied. |
| Container could not read `sql/kpi_schema.sql` | `sql/` mounted into the Airflow containers. |

## 9. How to run

1. `cp .env.example .env`, fill in the values (generate `FERNET_KEY`).
2. Ensure Airflow has the MySQL, PostgreSQL and SMTP providers
   (`requirements-airflow.txt`).
3. Databases: use your own servers (`host.docker.internal`) or
   `docker compose --profile local-db up -d` and set `MYSQL_HOST=mysql-staging`,
   `POSTGRES_HOST=analytics-db`.
   Apply `sql/mysql_schema.sql`, `sql/postgres_schema.sql`, `sql/kpi_schema.sql`
   if you use your own servers.
4. `docker compose up -d`, then create the Airflow connections
   `mysql_flight_staging`, `postgres_flight_analytics` and (optional) `smtp_default`.
5. Trigger `flight_price_pipeline` in the Airflow UI.
6. Connect Metabase (http://localhost:3000) to the analytics database and chart the `kpi_*` tables.

## 10. Testing

`python -m pytest` runs 41 unit tests (transformation, validation, ingestion,
loader idempotency, KPI logic and storage) plus a DAG import test when Airflow is
installed. The pipeline was additionally verified against throwaway MySQL and
PostgreSQL containers: clean load, malformed-file load, two consecutive loads,
and KPI results compared with an independent computation from the CSV.
