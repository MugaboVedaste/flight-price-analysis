-- KPI result tables (PostgreSQL analytics database).
-- Rebuilt on every pipeline run by src/analytics/kpi_storage.py:
-- the tables are emptied and refilled inside a single transaction,
-- so readers (e.g. Metabase) never see a half-written result.

CREATE SCHEMA IF NOT EXISTS flight_analytics;

-- KPI 1: Average fare by airline
CREATE TABLE IF NOT EXISTS flight_analytics.kpi_avg_fare_by_airline (
    airline VARCHAR(100) PRIMARY KEY,
    booking_count BIGINT NOT NULL,
    average_fare_bdt NUMERIC(18, 2),
    minimum_fare_bdt NUMERIC(18, 2),
    maximum_fare_bdt NUMERIC(18, 2),
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- KPI 2: Seasonal fare variation (one row per season)
CREATE TABLE IF NOT EXISTS flight_analytics.kpi_seasonal_fare_variation (
    season VARCHAR(50) PRIMARY KEY,
    is_peak_season BOOLEAN NOT NULL,
    booking_count BIGINT NOT NULL,
    average_fare_bdt NUMERIC(18, 2),
    variation_vs_non_peak_pct NUMERIC(10, 2),
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- KPI 2 roll-up: peak vs non-peak (single row)
CREATE TABLE IF NOT EXISTS flight_analytics.kpi_peak_vs_non_peak_fare (
    peak_booking_count BIGINT,
    non_peak_booking_count BIGINT,
    peak_average_fare_bdt NUMERIC(18, 2),
    non_peak_average_fare_bdt NUMERIC(18, 2),
    fare_difference_bdt NUMERIC(18, 2),
    percentage_difference NUMERIC(10, 2),
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- KPI 3: Booking count by airline
CREATE TABLE IF NOT EXISTS flight_analytics.kpi_booking_count_by_airline (
    airline VARCHAR(100) PRIMARY KEY,
    booking_count BIGINT NOT NULL,
    percentage_of_total NUMERIC(6, 2),
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- KPI 4: Most popular routes
CREATE TABLE IF NOT EXISTS flight_analytics.kpi_popular_routes (
    route_rank INTEGER NOT NULL,
    source_code VARCHAR(10) NOT NULL,
    source_name VARCHAR(255),
    destination_code VARCHAR(10) NOT NULL,
    destination_name VARCHAR(255),
    booking_count BIGINT NOT NULL,
    average_fare_bdt NUMERIC(18, 2),
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_code, destination_code)
);

-- Extra KPI: Fare by booking lead time
CREATE TABLE IF NOT EXISTS flight_analytics.kpi_fare_by_booking_lead_time (
    booking_lead_time_group VARCHAR(30) PRIMARY KEY,
    booking_count BIGINT NOT NULL,
    average_fare_bdt NUMERIC(18, 2),
    minimum_fare_bdt NUMERIC(18, 2),
    maximum_fare_bdt NUMERIC(18, 2),
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
