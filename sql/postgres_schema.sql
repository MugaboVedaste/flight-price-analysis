
CREATE SCHEMA IF NOT EXISTS flight_analytics;

CREATE TABLE IF NOT EXISTS flight_analytics.transformed_flight_prices (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    source_record_id BIGINT UNIQUE NOT NULL,
    record_hash CHAR(64) UNIQUE NOT NULL,

    airline VARCHAR(100),
    source_code VARCHAR(10),
    source_name VARCHAR(255),
    destination_code VARCHAR(10),
    destination_name VARCHAR(255),

    departure_datetime TIMESTAMP,
    arrival_datetime TIMESTAMP,

    duration_hours NUMERIC(12, 8),
    stopovers VARCHAR(50),
    aircraft_type VARCHAR(100),
    booking_class VARCHAR(50),
    booking_source VARCHAR(100),

    base_fare_bdt NUMERIC(18, 6),
    tax_surcharge_bdt NUMERIC(18, 6),
    total_fare_bdt NUMERIC(18, 6),

    seasonality VARCHAR(50),
    days_before_departure INTEGER,

    ingested_at TIMESTAMP,

    departure_hour SMALLINT,
    departure_day_name VARCHAR(15),
    departure_month SMALLINT,
    departure_year SMALLINT,

    is_peak_season BOOLEAN,
    booking_lead_time_group VARCHAR(30),

    expected_fare_bdt NUMERIC(18, 6),
    fare_discrepancy_bdt NUMERIC(18, 6),
    fare_discrepancy_flag BOOLEAN,

    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_flight_airline
ON flight_analytics.transformed_flight_prices (airline);

CREATE INDEX IF NOT EXISTS idx_flight_route
ON flight_analytics.transformed_flight_prices (
    source_code,
    destination_code
);

CREATE INDEX IF NOT EXISTS idx_flight_departure_date
ON flight_analytics.transformed_flight_prices (departure_datetime);

CREATE INDEX IF NOT EXISTS idx_flight_seasonality
ON flight_analytics.transformed_flight_prices (is_peak_season);
