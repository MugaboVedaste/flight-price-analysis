
CREATE DATABASE IF NOT EXISTS flight_staging;

USE flight_staging;

CREATE TABLE IF NOT EXISTS raw_flight_prices (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    airline VARCHAR(100),
    source_code VARCHAR(10),
    source_name VARCHAR(255),
    destination_code VARCHAR(10),
    destination_name VARCHAR(255),

    departure_datetime DATETIME,
    arrival_datetime DATETIME,

    duration_hours DECIMAL(12, 8),
    stopovers VARCHAR(50),
    aircraft_type VARCHAR(100),
    booking_class VARCHAR(50),
    booking_source VARCHAR(100),

    base_fare_bdt DECIMAL(18, 6),
    tax_surcharge_bdt DECIMAL(18, 6),
    total_fare_bdt DECIMAL(18, 6),

    seasonality VARCHAR(50),
    days_before_departure INT,

    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
