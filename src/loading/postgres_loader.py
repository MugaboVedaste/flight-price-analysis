
import csv
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from config.database import get_postgres_connection


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "transformed_flight_prices.csv"
)

BATCH_SIZE = 1000

COLUMN_MAPPING = {
    "id": "source_record_id",
    "airline": "airline",
    "source_code": "source_code",
    "source_name": "source_name",
    "destination_code": "destination_code",
    "destination_name": "destination_name",
    "departure_datetime": "departure_datetime",
    "arrival_datetime": "arrival_datetime",
    "duration_hours": "duration_hours",
    "stopovers": "stopovers",
    "aircraft_type": "aircraft_type",
    "booking_class": "booking_class",
    "booking_source": "booking_source",
    "base_fare_bdt": "base_fare_bdt",
    "tax_surcharge_bdt": "tax_surcharge_bdt",
    "total_fare_bdt": "total_fare_bdt",
    "seasonality": "seasonality",
    "days_before_departure": "days_before_departure",
    "ingested_at": "ingested_at",
    "departure_hour": "departure_hour",
    "departure_day_name": "departure_day_name",
    "departure_month": "departure_month",
    "departure_year": "departure_year",
    "is_peak_season": "is_peak_season",
    "booking_lead_time_group": "booking_lead_time_group",
    "expected_fare_bdt": "expected_fare_bdt",
    "fare_discrepancy_bdt": "fare_discrepancy_bdt",
    "fare_discrepancy_flag": "fare_discrepancy_flag",
}

INTEGER_COLUMNS = {
    "id",
    "days_before_departure",
    "departure_hour",
    "departure_month",
    "departure_year",
}

DECIMAL_COLUMNS = {
    "duration_hours",
    "base_fare_bdt",
    "tax_surcharge_bdt",
    "total_fare_bdt",
    "expected_fare_bdt",
    "fare_discrepancy_bdt",
}

DATETIME_COLUMNS = {
    "departure_datetime",
    "arrival_datetime",
    "ingested_at",
}

BOOLEAN_COLUMNS = {
    "is_peak_season",
    "fare_discrepancy_flag",
}


def convert_value(column, value):
    if value is None or value.strip() == "":
        return None

    if column in INTEGER_COLUMNS:
        return int(value)

    if column in DECIMAL_COLUMNS:
        try:
            return Decimal(value)
        except InvalidOperation as error:
            raise ValueError(
                f"Invalid decimal in {column}: {value}"
            ) from error

    if column in DATETIME_COLUMNS:
        return datetime.fromisoformat(value)

    if column in BOOLEAN_COLUMNS:
        normalized = value.strip().lower()

        if normalized == "true":
            return True

        if normalized == "false":
            return False

        raise ValueError(
            f"Invalid boolean in {column}: {value}"
        )

    return value.strip()


def build_insert_query():
    columns = list(COLUMN_MAPPING.values())

    column_sql = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))

    return f"""
        INSERT INTO flight_analytics.transformed_flight_prices
        ({column_sql})
        VALUES ({placeholders})
        ON CONFLICT (source_record_id) DO NOTHING
    """


def load_transformed_csv(
    csv_path=INPUT_PATH,
    batch_size=BATCH_SIZE,
    connection=None,
):
    """
    Load transformed CSV records into PostgreSQL.

    A connection can be injected by Airflow.
    If no connection is supplied, use the local database configuration.
    """

    csv_path = Path(csv_path)

    if not csv_path.exists():
        raise FileNotFoundError(
            f"Transformed CSV not found: {csv_path}"
        )

    if batch_size <= 0:
        raise ValueError(
            "Batch size must be greater than zero."
        )

    owns_connection = connection is None
    cursor = None

    total_read = 0
    total_inserted = 0
    total_skipped = 0

    insert_query = build_insert_query()

    try:
        print("Starting PostgreSQL data loading...")

        if owns_connection:
            connection = get_postgres_connection()

        print("Connected to PostgreSQL successfully.")

        cursor = connection.cursor()

        with csv_path.open(
            mode="r",
            newline="",
            encoding="utf-8-sig",
        ) as csv_file:

            reader = csv.DictReader(csv_file)

            missing_columns = (
                set(COLUMN_MAPPING) - set(reader.fieldnames or [])
            )

            if missing_columns:
                raise ValueError(
                    f"Missing CSV columns: {sorted(missing_columns)}"
                )

            batch = []

            for row in reader:
                values = tuple(
                    convert_value(column, row[column])
                    for column in COLUMN_MAPPING
                )

                batch.append(values)
                total_read += 1

                if len(batch) >= batch_size:
                    cursor.executemany(insert_query, batch)

                    inserted = cursor.rowcount
                    total_inserted += inserted
                    total_skipped += len(batch) - inserted

                    print(
                        f"Processed {total_read:,} records..."
                    )

                    batch.clear()

            if batch:
                cursor.executemany(insert_query, batch)

                inserted = cursor.rowcount
                total_inserted += inserted
                total_skipped += len(batch) - inserted

        # Commit the complete load as one transaction.
        connection.commit()

        print("\n========== POSTGRESQL LOADING REPORT ==========")
        print(f"Total records read: {total_read:,}")
        print(f"Records inserted: {total_inserted:,}")
        print(f"Duplicate records skipped: {total_skipped:,}")
        print(
            "Target table: "
            "flight_analytics.transformed_flight_prices"
        )
        print("POSTGRESQL LOADING COMPLETED SUCCESSFULLY.")

        return {
            "total_read": total_read,
            "total_inserted": total_inserted,
            "total_skipped": total_skipped,
        }

    except Exception as error:
        if connection:
            connection.rollback()

        print(f"POSTGRESQL LOADING FAILED: {error}")
        raise

    finally:
        if cursor:
            cursor.close()

        # Airflow owns connections passed into this function.
        if owns_connection and connection:
            connection.close()
            print("PostgreSQL connection closed.")


if __name__ == "__main__":
    load_transformed_csv()
