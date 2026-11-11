
import csv
from datetime import datetime
from decimal import Decimal
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


def convert_row(row):
    """Convert CSV values into appropriate MySQL values."""

    return (
        row["Airline"].strip(),
        row["Source"].strip(),
        row["Source Name"].strip(),
        row["Destination"].strip(),
        row["Destination Name"].strip(),
        datetime.strptime(
            row["Departure Date & Time"].strip(),
            "%Y-%m-%d %H:%M:%S",
        ),
        datetime.strptime(
            row["Arrival Date & Time"].strip(),
            "%Y-%m-%d %H:%M:%S",
        ),
        Decimal(row["Duration (hrs)"]),
        row["Stopovers"].strip(),
        row["Aircraft Type"].strip(),
        row["Class"].strip(),
        row["Booking Source"].strip(),
        Decimal(row["Base Fare (BDT)"]),
        Decimal(row["Tax & Surcharge (BDT)"]),
        Decimal(row["Total Fare (BDT)"]),
        row["Seasonality"].strip(),
        int(row["Days Before Departure"]),
    )


def load_csv_to_mysql(
    csv_path,
    connection,
    batch_size=1000,
):
    """
    Load CSV records into the MySQL raw staging table.

    Existing staging records are replaced on each successful load.
    """

    csv_path = Path(csv_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    cursor = connection.cursor()
    total_loaded = 0
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

            # Replace the previous staging snapshot.
            cursor.execute("DELETE FROM raw_flight_prices")

            for row in reader:
                batch.append(convert_row(row))

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

        return total_loaded

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()
