
import csv
from datetime import datetime
from decimal import Decimal, InvalidOperation
from functools import lru_cache
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
    "record_hash": "record_hash",
}

TARGET_TABLE = "flight_analytics.transformed_flight_prices"

SCHEMA_PATH = PROJECT_ROOT / "sql" / "postgres_schema.sql"

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


# PostgreSQL allows at most 65,535 bind parameters per statement.
MAX_PARAMETERS_PER_STATEMENT = 60000


@lru_cache(maxsize=None)
def build_insert_query(row_count=1):
    """
    Build a multi-row INSERT for `row_count` rows.

    Plain `cursor.execute` with a multi-row VALUES list works with both
    psycopg2 and psycopg 3 (Airflow's PostgresHook returns psycopg 3
    connections, where psycopg2.extras.execute_values does not work).

    ON CONFLICT DO NOTHING (no target) skips a row that violates ANY
    unique constraint (source_record_id or record_hash). Source record
    IDs come from the MySQL auto-increment column and change on every
    re-ingestion, so record_hash is what really identifies a flight.
    RETURNING lets us count the rows that were truly inserted.
    """
    column_sql = ", ".join(COLUMN_MAPPING.values())
    row_sql = "(" + ", ".join(["%s"] * len(COLUMN_MAPPING)) + ")"
    values_sql = ", ".join([row_sql] * row_count)

    return f"""
        INSERT INTO {TARGET_TABLE}
        ({column_sql})
        VALUES {values_sql}
        ON CONFLICT DO NOTHING
        RETURNING 1
    """


def _insert_batch(cursor, batch):
    """Insert one batch and return how many rows were really inserted."""

    rows_per_statement = max(
        1, MAX_PARAMETERS_PER_STATEMENT // len(COLUMN_MAPPING)
    )

    inserted = 0

    for start in range(0, len(batch), rows_per_statement):
        chunk = batch[start:start + rows_per_statement]
        parameters = [value for row in chunk for value in row]

        cursor.execute(build_insert_query(len(chunk)), parameters)
        inserted += len(cursor.fetchall())

    return inserted


def ensure_schema(cursor):
    """
    Create the analytics schema, table and indexes if they are missing.

    sql/postgres_schema.sql only uses CREATE ... IF NOT EXISTS, so this
    is safe on a database that is already set up, and a fresh database
    works without a manual setup step.
    """

    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(
            f"Schema file not found: {SCHEMA_PATH}. "
            "Make sure the sql/ folder is available to the application "
            "(in Docker it is mounted at /opt/airflow/sql)."
        )

    cursor.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def _count_target_rows(cursor):
    cursor.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}")
    return cursor.fetchone()[0]


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

    try:
        print("Starting PostgreSQL data loading...")

        if owns_connection:
            connection = get_postgres_connection()

        print("Connected to PostgreSQL successfully.")

        cursor = connection.cursor()

        ensure_schema(cursor)

        rows_before = _count_target_rows(cursor)

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
                    inserted = _insert_batch(cursor, batch)
                    total_inserted += inserted
                    total_skipped += len(batch) - inserted

                    print(
                        f"Processed {total_read:,} records..."
                    )

                    batch.clear()

            if batch:
                inserted = _insert_batch(cursor, batch)
                total_inserted += inserted
                total_skipped += len(batch) - inserted

        # Verify consistency inside the transaction, before committing:
        # the table must have grown by exactly the rows we inserted.
        rows_after = _count_target_rows(cursor)

        if rows_after != rows_before + total_inserted:
            raise RuntimeError(
                "PostgreSQL row-count verification failed: "
                f"before={rows_before}, inserted={total_inserted}, "
                f"after={rows_after}"
            )

        # Commit the complete load as one transaction.
        connection.commit()

        print("\n========== POSTGRESQL LOADING REPORT ==========")
        print(f"Total records read: {total_read:,}")
        print(f"Records inserted: {total_inserted:,}")
        print(f"Duplicate records skipped: {total_skipped:,}")
        print(f"Rows in target table: {rows_after:,}")
        print(
            "Target table: "
            "flight_analytics.transformed_flight_prices"
        )
        print("POSTGRESQL LOADING COMPLETED SUCCESSFULLY.")

        return {
            "total_read": total_read,
            "total_inserted": total_inserted,
            "total_skipped": total_skipped,
            "table_row_count": rows_after,
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
