
import csv
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path


# CSV column -> MySQL column
COLUMN_MAPPING = {
    "Airline": "airline",
    "Source": "source_code",
    "Source Name": "source_name",
    "Destination": "destination_code",
    "Destination Name": "destination_name",
    "Departure Date & Time": "departure_datetime",
    "Arrival Date & Time": "arrival_datetime",
    "Duration (hrs)": "duration_hours",
    "Stopovers": "stopovers",
    "Aircraft Type": "aircraft_type",
    "Class": "booking_class",
    "Booking Source": "booking_source",
    "Base Fare (BDT)": "base_fare_bdt",
    "Tax & Surcharge (BDT)": "tax_surcharge_bdt",
    "Total Fare (BDT)": "total_fare_bdt",
    "Seasonality": "seasonality",
    "Days Before Departure": "days_before_departure",
}

INSERT_COLUMNS = list(COLUMN_MAPPING.values())

INSERT_SQL = f"""
    INSERT INTO raw_flight_prices (
        {", ".join(INSERT_COLUMNS)}
    )
    VALUES ({", ".join(["%s"] * len(INSERT_COLUMNS))})
"""


REJECTED_TABLE_DDL = """
    CREATE TABLE IF NOT EXISTS rejected_flight_rows (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        csv_row_number INT NOT NULL,
        reason VARCHAR(500) NOT NULL,
        raw_row TEXT,
        rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
"""

REJECTED_INSERT_SQL = """
    INSERT INTO rejected_flight_rows (csv_row_number, reason, raw_row)
    VALUES (%s, %s, %s)
"""


def _decimal(row, column):
    """Parse a finite decimal; blank or non-numeric values are rejected."""

    raw = (row.get(column) or "").strip()

    if raw == "":
        raise ValueError(f"missing numeric value in '{column}'")

    try:
        value = Decimal(raw)
    except InvalidOperation:
        raise ValueError(f"non-numeric value in '{column}': {raw!r}")

    if not value.is_finite():
        raise ValueError(f"non-finite value in '{column}': {raw!r}")

    return value


def _datetime(row, column):
    raw = (row.get(column) or "").strip()

    if raw == "":
        raise ValueError(f"missing date/time in '{column}'")

    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        raise ValueError(f"invalid date/time in '{column}': {raw!r}")


def _integer(row, column):
    raw = (row.get(column) or "").strip()

    if raw == "":
        raise ValueError(f"missing integer value in '{column}'")

    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"non-integer value in '{column}': {raw!r}")


def _text(row, column):
    return (row.get(column) or "").strip()


def convert_row(row):
    """
    Convert CSV values into appropriate MySQL values.

    Raises ValueError (with a readable reason) when a numeric, integer
    or date/time value is missing or malformed. Text fields are kept
    as-is (possibly empty); the validation step reports those.
    """

    return (
        _text(row, "Airline"),
        _text(row, "Source"),
        _text(row, "Source Name"),
        _text(row, "Destination"),
        _text(row, "Destination Name"),
        _datetime(row, "Departure Date & Time"),
        _datetime(row, "Arrival Date & Time"),
        _decimal(row, "Duration (hrs)"),
        _text(row, "Stopovers"),
        _text(row, "Aircraft Type"),
        _text(row, "Class"),
        _text(row, "Booking Source"),
        _decimal(row, "Base Fare (BDT)"),
        _decimal(row, "Tax & Surcharge (BDT)"),
        _decimal(row, "Total Fare (BDT)"),
        _text(row, "Seasonality"),
        _integer(row, "Days Before Departure"),
    )


def load_csv_to_mysql(
    csv_path,
    connection,
    batch_size=1000,
):
    """
    Load CSV records into the MySQL raw staging table.

    Existing staging records are replaced on each successful load.

    Rows whose numeric or date values are missing/malformed are not
    allowed to crash the load: they are written to
    `rejected_flight_rows` with the reason and the raw row, and the
    validation task reports them.

    Returns the number of rows loaded into `raw_flight_prices`.
    """

    csv_path = Path(csv_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    cursor = connection.cursor()
    total_loaded = 0
    total_rejected = 0
    batch = []

    try:
        with csv_path.open(
            mode="r",
            encoding="utf-8-sig",
            newline="",
        ) as file:

            reader = csv.DictReader(file)

            missing_columns = (
                set(COLUMN_MAPPING) - set(reader.fieldnames or [])
            )

            if missing_columns:
                raise ValueError(
                    f"Missing CSV columns: {missing_columns}"
                )

            # DDL commits implicitly in MySQL, so create the table
            # before the transactional part of the load begins.
            cursor.execute(REJECTED_TABLE_DDL)

            # Replace the previous staging snapshot.
            cursor.execute("DELETE FROM raw_flight_prices")
            cursor.execute("DELETE FROM rejected_flight_rows")

            # Row 1 is the header, so data starts at row 2.
            for row_number, row in enumerate(reader, start=2):
                try:
                    converted = convert_row(row)
                except ValueError as error:
                    total_rejected += 1
                    cursor.execute(
                        REJECTED_INSERT_SQL,
                        (
                            row_number,
                            str(error)[:500],
                            json.dumps(row, ensure_ascii=False),
                        ),
                    )
                    continue

                batch.append(converted)

                if len(batch) >= batch_size:
                    cursor.executemany(INSERT_SQL, batch)
                    total_loaded += len(batch)
                    batch.clear()

            # Insert any remaining records.
            if batch:
                cursor.executemany(INSERT_SQL, batch)
                total_loaded += len(batch)

        connection.commit()

        print(f"Successfully loaded {total_loaded} records.")
        print(f"Rejected {total_rejected} malformed records.")

        return total_loaded

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()
